import unittest,zipfile
from pathlib import Path
from flask import Flask,jsonify
from panel_delivery import register_panel_delivery,RELEASE

class PanelDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);register_panel_delivery(self.app)
        @self.app.get('/admin/private_fixture')
        def protected():return jsonify(error='Authentication required'),401
        self.client=self.app.test_client()
    def test_admin_aliases_serve_exact_tested_ui_without_cache(self):
        expected=Path('website-downloads/admin_clean_cpanel.html').read_bytes()
        for path in ['/control-panel','/admin/admin_clean.html']:
            r=self.client.get(path);self.assertEqual(r.status_code,200);self.assertEqual(r.data,expected)
            self.assertEqual(r.mimetype,'text/html');self.assertIn('no-store',r.headers['Cache-Control']);self.assertEqual(r.headers['X-NairaPips-Panel-Release'],RELEASE)
            self.assertNotIn('Set-Cookie',r.headers);r.close()
        self.assertEqual(self.client.get('/admin/private_fixture').status_code,401)
    def test_related_trader_preview_and_dashboard_links_resolve(self):
        for path in ['/dashboard/trader_clean.html']:
            r=self.client.get(path);self.assertEqual(r.status_code,200);self.assertEqual(r.data,Path('website-downloads/trader_clean.html').read_bytes());r.close()
    def test_entry_and_registration_have_separate_routes(self):
        for route,file in [('/dashboard/','trader_entry_redirect.html'),('/dashboard/index.html','trader_entry_redirect.html'),('/dashboard/register.html','trader_login.html')]:
            r=self.client.get(route)
            self.assertEqual(r.status_code,200)
            self.assertEqual(r.data,Path('website-downloads',file).read_bytes());r.close()
    def test_referral_portal_is_available_on_the_dashboard_host(self):
        r=self.client.get('/dashboard/referral.html')
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.data,Path('website-downloads/referral.html').read_bytes())
        self.assertIn('no-store',r.headers['Cache-Control'])
        self.assertIn(b'/np2_referral/profile',r.data)
        self.assertIn(b"Authorization:'Bearer '+token",r.data)
        self.assertNotIn('Set-Cookie',r.headers)
        r.close()
    def test_restore_archive_has_exact_filename_content_and_no_other_paths(self):
        with zipfile.ZipFile('website-downloads/admin-restore.zip') as z:
            self.assertEqual(z.namelist(),['admin/admin_clean.html'])
            self.assertEqual(z.read('admin/admin_clean.html'),Path('website-downloads/admin_clean_cpanel.html').read_bytes())
            self.assertEqual((z.getinfo('admin/admin_clean.html').external_attr>>16)&0o777,0o644)
    def test_path_traversal_is_not_a_file_server(self):
        self.assertEqual(self.client.get('/control-panel/../../app.py').status_code,404)
if __name__=='__main__':unittest.main()
