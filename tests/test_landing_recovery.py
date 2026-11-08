import hashlib,io,unittest,zipfile
from pathlib import Path
from flask import Flask
from panel_delivery import register_panel_delivery
class HomepageRecoveryTests(unittest.TestCase):
 def test_download_contains_recovery_and_exact_selector_without_admin_entrypoint(self):
  app=Flask(__name__);register_panel_delivery(app)
  reply=app.test_client().get('/downloads/landing-recovery.zip')
  self.assertEqual(reply.status_code,200)
  self.assertIn('attachment',reply.headers['Content-Disposition'])
  with zipfile.ZipFile(io.BytesIO(reply.data)) as z:
   self.assertEqual(set(z.namelist()),{'restore-landing.php','.np-landing-files/landing-catalogue.js'})
   script=z.read('.np-landing-files/landing-catalogue.js')
   self.assertEqual(script,Path('website-downloads/account-plan-selector.js').read_bytes()+b'\n'+Path('website-downloads/landing-catalogue-sync.js').read_bytes())
   self.assertIn(hashlib.sha256(script).hexdigest().encode(),z.read('restore-landing.php'))
   self.assertNotIn(b'PAYLOAD_SHA256',z.read('restore-landing.php'))
  reply.close()
