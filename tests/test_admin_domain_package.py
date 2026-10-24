import unittest,zipfile
from pathlib import Path
class AdminDomainPackageTests(unittest.TestCase):
 def test_full_admin_and_same_domain_entry(self):
  with zipfile.ZipFile('website-downloads/nairapips-admin-domain.zip') as z:
   self.assertEqual(z.namelist(),['index.html','admin_clean.html'])
   self.assertEqual(z.read('admin_clean.html'),Path('website-downloads/admin_clean_cpanel.html').read_bytes())
   entry=z.read('index.html').decode();self.assertNotIn('onrender',entry);self.assertIn("location.replace('/admin/admin_clean.html'",entry)
   self.assertIn('noindex, nofollow',entry)
