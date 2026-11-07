import hashlib,json,unittest,zipfile
from io import BytesIO
from pathlib import Path
from flask import Flask
from panel_delivery import register_panel_delivery

class DeliveryTests(unittest.TestCase):
    def test_vps_archive_has_exact_runtime_and_check_only_installer(self):
        app=Flask(__name__);register_panel_delivery(app)
        response=app.test_client().get('/downloads/static-trailing-dd-update.zip')
        self.assertEqual(response.status_code,200)
        self.assertIn('attachment',response.headers['Content-Disposition'])
        self.assertIn('no-store',response.headers['Cache-Control'])
        with zipfile.ZipFile(BytesIO(response.data)) as z:
            self.assertEqual(z.read('equity_drawdown.py'),Path('equity_drawdown.py').read_bytes())
            manifest=json.loads(z.read('SHA256.json'))
            for name,digest in manifest.items():self.assertEqual(hashlib.sha256(z.read(name)).hexdigest(),digest)
            self.assertTrue(all('/' not in name and '..' not in name for name in z.namelist()))
            self.assertIn(b'CHECK ONLY',z.read('INSTALL.ps1'))
            self.assertIn(b'WINDOWS VPS UPDATE, NOT A CPANEL UPDATE',z.read('README.txt'))
        response.close()
