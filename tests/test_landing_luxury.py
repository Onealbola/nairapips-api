import unittest
from pathlib import Path
from flask import Flask
from panel_delivery import register_panel_delivery
from landing_bundle import build_landing_bundle
class LuxuryDeliveryTests(unittest.TestCase):
 def test_hosted_bundle_upgrades_existing_landing_before_old_guard(self):
  app=Flask(__name__);register_panel_delivery(app)
  reply=app.test_client().get('/public/landing-catalogue.js')
  self.assertEqual(reply.status_code,200)
  expected=build_landing_bundle('website-downloads')
  self.assertEqual(reply.get_data(as_text=True),expected)
  self.assertLess(expected.index('window.NPLuxuryLanding='),expected.index('if(window.NPLandingCatalogue)return'))
  self.assertIn('no-store',reply.headers['Cache-Control'])
 def test_architecture_image_is_real_png_and_cacheable(self):
  app=Flask(__name__);register_panel_delivery(app)
  reply=app.test_client().get('/public/nairapips-gold-architecture.png')
  self.assertEqual(reply.status_code,200);self.assertEqual(reply.mimetype,'image/png')
  self.assertTrue(reply.data.startswith(b'\x89PNG\r\n\x1a\n'))
  self.assertEqual(reply.data,Path('website-downloads/nairapips-gold-architecture.png').read_bytes())
  self.assertIn('max-age=86400',reply.headers['Cache-Control']);reply.close()

 def test_luxury_local_installer_archive(self):
  import zipfile,hashlib,io
  app=Flask(__name__);register_panel_delivery(app)
  response=app.test_client().get('/downloads/luxury-landing-update.zip')
  self.assertEqual(response.status_code,200)
  self.assertIn('no-store',response.headers['Cache-Control'])
  with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
   self.assertEqual(set(archive.namelist()),{'install-luxury.php','.np-luxury-files/luxury-bundle.js','.np-luxury-files/luxury-art.png','.np-luxury-files/architecture.png'})
   installer=archive.read('install-luxury.php').decode()
   for name in ['luxury-bundle.js','luxury-art.png','architecture.png']:
    self.assertIn(hashlib.sha256(archive.read('.np-luxury-files/'+name)).hexdigest(),installer)
   bundle=archive.read('.np-luxury-files/luxury-bundle.js').decode()
   self.assertIn('/.np-luxury-files/luxury-art.png',bundle)
   self.assertNotIn('https://nairapips-api.onrender.com/public/nairapips-luxury-concept.png',bundle)
   self.assertIn('np-lux-architecture',bundle)
  response.close()
