import tempfile
import unittest
from unittest.mock import patch
import target_monitor_health as health


class TargetHeartbeatTests(unittest.TestCase):
    def test_no_scan_is_not_healthy(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(health.tempfile, 'gettempdir', return_value=folder):
            self.assertFalse(health.read_report()['healthy'])

    def test_parent_scan_visible_to_worker_without_customer_fields(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(health.tempfile, 'gettempdir', return_value=folder), patch.object(health.time, 'time', return_value=1000):
            with patch.object(health.os, 'getpid', return_value=100):
                health.record_scan({'checked_at': 'fixture-time', 'checked': 10, 'recorded': 0, 'errors': 0, 'private_customer': 'not-public'})
            with patch.object(health.os, 'getpid', return_value=101), patch.object(health.os, 'getppid', return_value=100):
                result = health.read_report()
                self.assertTrue(result['healthy'])
                self.assertEqual(result['last_scan']['checked'], 10)
                self.assertNotIn('private_customer', str(result))
                with patch.object(health.time, 'time', return_value=1600):
                    self.assertFalse(health.read_report()['healthy'])

    def test_errors_future_clock_and_corrupt_records_fail_closed(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(health.tempfile, 'gettempdir', return_value=folder), patch.object(health.time, 'time', return_value=1000):
            health.record_scan({'errors': 1})
            self.assertFalse(health.read_report()['healthy'])
            health.record_scan({'errors': 0})
            with patch.object(health.time, 'time', return_value=980):
                self.assertFalse(health.read_report()['healthy'])
            health._path(health.os.getpid()).write_text('broken')
            self.assertFalse(health.read_report()['healthy'])
