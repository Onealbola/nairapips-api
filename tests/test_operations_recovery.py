import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('operations_recovery', Path(__file__).resolve().parents[1] / 'website-downloads/nairapips-operations-recovery.py')
recovery = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(recovery)
FIXTURE = b'DEDICATED_DD_GUARD_OWNS_BREACH = True\ndef close_allowed(reason_type):\n    if reason_type == "pass":\n        return True\n'


class OperationsRecoveryTests(unittest.TestCase):
    def test_repair_backup_idempotence_and_unknown_file_refusal(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'engine.py'
            target.write_bytes(FIXTURE)
            fixed = FIXTURE.replace(recovery.OLD, recovery.NEW)
            with patch.object(recovery, 'ORIGINAL', hashlib.sha256(FIXTURE).hexdigest()), patch.object(recovery, 'REPAIRED', hashlib.sha256(fixed).hexdigest()):
                recovery.repair(target)
                self.assertEqual(target.read_bytes(), fixed)
                self.assertEqual(next(Path(folder).glob('*.backup-*')).read_bytes(), FIXTURE)
                recovery.repair(target)
                self.assertEqual(len(list(Path(folder).glob('*.backup-*'))), 1)
                target.write_bytes(b'uninspected-source')
                with self.assertRaises(RuntimeError):
                    recovery.repair(target)
                self.assertEqual(target.read_bytes(), b'uninspected-source')

    def test_existing_operations_process_blocks_without_stopping_anything(self):
        with patch.object(recovery.os, 'name', 'nt'), patch.object(recovery.subprocess, 'run', return_value=SimpleNamespace(stdout='123')) as run:
            with self.assertRaisesRegex(RuntimeError, 'already present'):
                recovery.check_processes()
            command = run.call_args.args[0][-1]
            self.assertNotIn('Stop-Process', command)
            self.assertNotIn('taskkill', command)

    def test_repair_does_not_launch(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(recovery, 'ENGINE', Path(folder)), patch.object(recovery, 'check_processes'), patch.object(recovery, 'checked_source', return_value=(FIXTURE, recovery.ORIGINAL)), patch.object(recovery, 'repair') as repair, patch.object(recovery.runpy, 'run_path') as run, patch.dict('sys.modules', {'requests': SimpleNamespace(), 'MetaTrader5': SimpleNamespace(initialize=lambda: None)}), patch('sys.argv', ['recovery', '--repair']):
            recovery.main()
            repair.assert_called_once()
            run.assert_not_called()

    def test_duplicate_launcher_lock(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'operations.lock'
            with recovery.launch_lock(path):
                with self.assertRaises(RuntimeError):
                    with recovery.launch_lock(path):
                        self.fail('Duplicate lock accepted')
            with recovery.launch_lock(path):
                pass
