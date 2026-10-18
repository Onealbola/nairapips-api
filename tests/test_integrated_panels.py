"""Shipping panel validation without executing production app.py."""
import unittest,subprocess,tempfile
from pathlib import Path
from html.parser import HTMLParser
ROOT=Path(__file__).resolve().parents[1]
class Scripts(HTMLParser):
 def __init__(self):super().__init__(convert_charrefs=False);self.inside=False;self.data=[];self.items=[]
 def handle_starttag(self,tag,attrs):
  if tag=='script':self.inside=True;self.data=[]
 def handle_data(self,data):
  if self.inside:self.data.append(data)
 def handle_endtag(self,tag):
  if tag=='script' and self.inside:self.items.append(''.join(self.data));self.inside=False
class PanelTests(unittest.TestCase):
 def test_shipping_panels_embed_the_exact_modules_tested_in_browsers(self):
  for name,js,marker in [('admin_clean.html','production-settings.js','/* Owner controls: real API persistence;'),('trader_clean.html','package-trader.js','/* New package client.')]:
   source=(ROOT/'website-downloads'/name).read_text();start=source.index(marker);end=source.index('\n</script>',start)
   self.assertEqual(source[start:end].strip(),(ROOT/'website-downloads'/js).read_text().strip())
  trader=(ROOT/'website-downloads/trader_clean.html').read_text()
  self.assertIn('np-package-builder .builder-layout',trader)
  self.assertIn('Purchases open at launch',trader)
  self.assertIn('Save my package',trader)
 def test_trader_panel_uses_current_customer_client_with_only_its_scope_wrapper_added(self):
  expected=(ROOT/'package_trader.js').read_text().replace('return `<div class="card"><h2>Build Your Package</h2>','return `<div class="np-package-builder"><div class="card"><h2>Build Your Package</h2>').replace('</button></div></div>`;\n};\nwindow.npPackageChoiceChanged','</button></div></div></div>`;\n};\nwindow.npPackageChoiceChanged')
  self.assertEqual(expected,(ROOT/'website-downloads/package-trader.js').read_text())
 def test_all_shipping_inline_scripts_are_syntactically_valid(self):
  with tempfile.TemporaryDirectory() as folder:
   for name in ['admin_clean.html','trader_clean.html']:
    scripts=Scripts();scripts.feed((ROOT/'website-downloads'/name).read_text())
    self.assertGreater(len(scripts.items),3)
    for i,script in enumerate(scripts.items):
     if not script.strip():continue
     path=Path(folder)/(name+str(i)+'.js');path.write_text(script)
     result=subprocess.run(['node','--check',str(path)],capture_output=True,text=True)
     self.assertEqual(result.returncode,0,name+' script '+str(i)+' '+result.stderr)
