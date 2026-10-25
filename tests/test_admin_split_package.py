import re,unittest,zipfile
from pathlib import Path
class SmallAdminTests(unittest.TestCase):
 def test_all_original_scripts_preserved_exactly_and_in_order(self):
  root=Path('website-downloads');source=(root/'admin_clean_cpanel.html').read_text()
  original=[m.group(2) for m in re.finditer(r'<script\b([^>]*)>([\s\S]*?)</script>',source,re.I) if m.group(2).strip()]
  with zipfile.ZipFile(root/'nairapips-admin-small.zip') as z:
   html=z.read('admin_clean.html').decode();self.assertLess(len(html.encode()),100000)
   scripts=re.findall(r'<script[^>]*src="\./([^"]+)"',html)
   self.assertEqual([z.read(s).decode() for s in scripts],original)
   restored=html
   for name,code in zip(scripts,original):
    restored=restored.replace(' src="./'+name+'"','').replace('<script></script>','<script>'+code+'</script>',1)
   self.assertEqual(restored,source)
   self.assertTrue(all(not n.startswith('/') and '..' not in n.split('/') for n in z.namelist()))
   self.assertEqual(z.read('index.html'),(root/'admin_entry.html').read_bytes())
