"""Back up and repair the exact uploaded Operations engine; launch only on request.

No credentials, replacement engine source, DD changes, process kills or autostart.
"""
import argparse
import ast
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys

ENGINE = Path(r'C:\Users\Administrator\Documents\nairapips_mt5_rotation_engine (1)')
NAME = 'NAIRAPIPS_MT5_ENGINE_NON_DD_OPERATIONS_DIRECT_TELEMETRY_V3_20261004.py'
ORIGINAL = 'eb7f2799b5f949559c85d9153c2698c2a67eac327523365cb2a366233e4f0863'
REPAIRED = 'b569c12a5f964fe909deed24b850f1f70eb903a02e8e44a8573c16164abed82c'
OLD = b'if reason_type == "pass":'
NEW = b'if reason_type in ("pass", "target"):'


def checked_source(path):
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest not in (ORIGINAL, REPAIRED):
        raise RuntimeError('Engine differs from the inspected upload. No replacement or launch performed.')
    tree = ast.parse(data)
    guard = [n for n in tree.body if isinstance(n, ast.Assign)
             and any(isinstance(t, ast.Name) and t.id == 'DEDICATED_DD_GUARD_OWNS_BREACH' for t in n.targets)]
    if len(guard) != 1 or not isinstance(guard[0].value, ast.Constant) or guard[0].value.value is not True:
        raise RuntimeError('External DD ownership is not confirmed.')
    return data, digest


def repair(path):
    data, digest = checked_source(path)
    if digest == REPAIRED:
        print('Target permission fix is already installed.')
        return
    updated = data.replace(OLD, NEW)
    if data.count(OLD) != 1 or hashlib.sha256(updated).hexdigest() != REPAIRED:
        raise RuntimeError('Patch validation failed; no engine change performed.')
    compile(updated, str(path), 'exec')  # Syntax validation only; never executes MT5 code.
    backup = path.with_name(path.name + '.backup-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
    backup.write_bytes(data)
    staged = path.with_name(path.name + '.recovery-staged')
    staged.write_bytes(updated)
    os.replace(staged, path)
    print('Original engine backed up:', backup)
    print('Target/pass permission fix installed. External DD ownership retained.')


def check_processes():
    if os.name != 'nt':
        raise RuntimeError('Use this recovery tool on the Windows VPS.')
    command = ("Get-CimInstance Win32_Process | Where-Object { "
               "$_.Name -match '^python(w)?\\.exe$' -and $_.ProcessId -ne " + str(os.getpid()) +
               " -and $_.CommandLine -match 'NON_DD_OPERATIONS|MT5_ENGINE_MULTI_ACCOUNT|mt5_rotation_monitor|operations-recovery.py.*--run' "
               "} | Select-Object -ExpandProperty ProcessId | ConvertTo-Json -Compress")
    result = subprocess.run(['powershell', '-NoProfile', '-Command', command],
                            capture_output=True, text=True, timeout=30, check=True)
    if result.stdout.strip():
        pids = json.loads(result.stdout)
        if pids:
            raise RuntimeError('Operations/monitor process already present; no duplicate launched. PID(s): ' + str(pids))


@contextmanager
def launch_lock(path):
    handle = path.open('a+b')
    locked = False
    try:
        handle.seek(0, 2)
        if not handle.tell():
            handle.write(b'0')
            handle.flush()
        handle.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError:
            raise RuntimeError('Another recovery launch holds the Operations lock.')
        yield
    finally:
        if locked and os.name == 'nt':
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        handle.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--repair', action='store_true')
    mode.add_argument('--run', action='store_true')
    args = parser.parse_args()
    path = ENGINE / NAME
    with launch_lock(ENGINE / 'operations-recovery.lock'):
        check_processes()
        data, digest = checked_source(path)
        import requests
        import MetaTrader5 as mt5
        if not callable(getattr(mt5, 'initialize', None)):
            raise RuntimeError('MetaTrader5.initialize is unavailable. No engine started.')
        if args.repair:
            repair(path)
            print('Repair complete. No process was started or stopped.')
            return
        if digest != REPAIRED:
            raise RuntimeError('Run --repair before starting the engine.')
        os.chdir(ENGINE)
        print('Starting Operations/Target engine. KEEP THIS WINDOW OPEN.', flush=True)
        print('DD breach monitoring remains with the separate shard fleet.', flush=True)
        runpy.run_path(str(path), run_name='__main__')
        print('Operations engine returned. Send the preceding output for inspection.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Operations recovery stopped by user.')
    except Exception as exc:
        print('RECOVERY STOPPED:', type(exc).__name__, str(exc))
        sys.exit(1)
