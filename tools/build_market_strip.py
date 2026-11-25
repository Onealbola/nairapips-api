"""Embed the Market Strip editor in both admin editions, then rebuild deliveries."""
from pathlib import Path
import json,re,subprocess,sys
ROOT=Path(__file__).resolve().parents[1];assets=ROOT/'website-downloads'
script=(assets/'market-strip-admin.js').read_text()
block='\n<!-- NP_MARKET_STRIP_ADMIN -->\n<script>\n'+script+'\n</script>\n<!-- /NP_MARKET_STRIP_ADMIN -->\n'
for name in ['admin_clean.html','admin_clean_cpanel.html']:
 p=assets/name;s=p.read_text();s=re.sub(r'\n?<!-- NP_MARKET_STRIP_ADMIN -->[\s\S]*?<!-- /NP_MARKET_STRIP_ADMIN -->\n?','',s)
 if 'data-mod="marketStrip"' not in s:
  nav='<button class="nav" data-mod="marketStrip" onclick="setMod(\'marketStrip\')">✦ Market Strip</button>'
  s=s.replace('<button class="nav" data-mod="announcements"',nav+'<button class="nav" data-mod="announcements"',1)
 s=s.replace('marketStrip:renderMarketStrip,','marketStrip:()=>window.renderMarketStrip(),')
 if not re.search(r'marketStrip\s*:',s):
  s,n=re.subn(r'(const renderers\s*=\s*\{)',r'\1marketStrip:()=>window.renderMarketStrip(),',s,count=1)
  if n!=1:raise RuntimeError('Cannot locate admin module registry: '+name)
 p.write_text(s.rstrip()+'\n'+block)
p=assets/'staff-access.js';s=p.read_text();s=re.sub(r'const fallback=\[[^\n]*\];','const fallback='+json.dumps(json.loads((ROOT/'staff_modules.json').read_text()),separators=(',',':'))+';',s,count=1);p.write_text(s)
subprocess.run([sys.executable,str(ROOT/'tools/build_staff_access.py')],cwd=ROOT,check=True)
