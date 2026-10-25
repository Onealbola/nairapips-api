from pathlib import Path
import re,hashlib,zipfile
root=Path('website-downloads');source=(root/'admin_clean_cpanel.html').read_text();release=hashlib.sha256(source.encode()).hexdigest()[:12];assets={};i=0
def split(m):
 global i
 attrs,code=m.group(1),m.group(2)
 if not code.strip() or re.search(r'type\s*=\s*[\"\'](?:application/json|application/ld\+json)',attrs,re.I):return m.group(0)
 i+=1;name=f'admin-assets/{release}/panel-{i:02d}.js';assets[name]=code
 return '<script'+attrs+' src="./'+name+'"></script>'
small=re.sub(r'<script\b([^>]*)>([\s\S]*?)</script>',split,source,flags=re.I)
(root/'admin_clean_small.html').write_text(small)
with zipfile.ZipFile(root/'nairapips-admin-small.zip','w',zipfile.ZIP_DEFLATED) as z:
 for name,content in {'admin_clean.html':small,'index.html':(root/'admin_entry.html').read_text(),**assets}.items():
  info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,content.encode())
print('Admin HTML:',len(small.encode()),'bytes; support scripts:',len(assets),'largest:',max(len(s.encode()) for s in assets.values()),'bytes; ZIP:',(root/'nairapips-admin-small.zip').stat().st_size,'bytes')
