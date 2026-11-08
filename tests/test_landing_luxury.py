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
