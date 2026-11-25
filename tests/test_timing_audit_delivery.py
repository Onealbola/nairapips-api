import unittest
from pathlib import Path
from flask import Flask
from panel_delivery import register_panel_delivery
class TimingDeliveryTests(unittest.TestCase):
 def test_audit_is_public_readonly_and_bounded(self):
  app=Flask(__name__);register_panel_delivery(app);r=app.test_client().get('/downloads/dd-timing-audit.ps1')
  self.assertEqual(r.status_code,200);self.assertIn('no-store',r.headers['Cache-Control']);source=r.data.decode();r.close()
  for forbidden in ['Stop-Process','Restart-Computer','Start-ScheduledTask','mt5.login','order_send','Set-ScheduledTask','Invoke-WebRequest','supabase','API_KEY']:
   self.assertNotIn(forbidden,source)
  for expected in ['16777216','First 8','ReadWrite','close_blocked','oldest_valid_seconds','BREACH CLOSED','ZERO EXPOSURE','REDACTED','tail_only']:
   self.assertIn(expected,source)
 def test_report_does_not_export_roster_or_latch_credentials(self):
  source=Path('website-downloads/nairapips-dd-timing-audit.ps1').read_text()
  self.assertNotIn('$matches=',source.lower());self.assertNotIn('monitorable_accounts_snapshot.json',source);self.assertNotIn('breach_latch_shard',source);self.assertIn('$evidenceFields',source);self.assertIn('[string]$ev.mt5_login -ne $Login',source);self.assertIn('positions_at_detection_count',source)
