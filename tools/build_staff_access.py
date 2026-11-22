"""Build the authoritative Staff workspace into standalone and compact admin panels."""
from pathlib import Path
import re,subprocess,sys,zipfile
ROOT=Path(__file__).resolve().parents[1]
ASSETS=ROOT/'website-downloads'
script=(ASSETS/'staff-access.js').read_text()
block='\n<!-- NP_STAFF_ACCESS_V1 -->\n<script>\n'+script+'\n</script>\n<!-- /NP_STAFF_ACCESS_V1 -->\n'
for name in ['admin_clean.html','admin_clean_cpanel.html']:
 p=ASSETS/name;s=p.read_text();s=re.sub(r'\n?<!-- NP_STAFF_ACCESS_V1 -->[\s\S]*?<!-- /NP_STAFF_ACCESS_V1 -->\n?', '',s);p.write_text(s.rstrip()+'\n'+block)
subprocess.run([sys.executable,str(ROOT/'tools/build_small_admin.py')],cwd=ROOT,check=True)
def archive(path,entries):
 with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
  for name,content in entries.items():
   info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,content)
with zipfile.ZipFile(ASSETS/'nairapips-admin-small.zip') as z:entries={name:z.read(name) for name in z.namelist() if name!='index.html'}
for name in ['nairapips-staff-access.zip','nairapips-admin-mobile.zip']:archive(ASSETS/name,entries)
for name in ['nairapips-mobile-update.zip','nairapips-catalogue-sync.zip']:
 path=ASSETS/name
 if not path.exists():continue
 with zipfile.ZipFile(path) as z:retained={name:z.read(name) for name in z.namelist() if name!='admin/admin_clean.html' and not name.startswith('admin/admin-assets/')}
 retained.update({'admin/'+name:content for name,content in entries.items()});archive(path,retained)
archive(ASSETS/'admin-restore.zip',{'admin/admin_clean.html':(ASSETS/'admin_clean_cpanel.html').read_bytes()})
print('Staff access package ready. Extract inside public_html/admin.')
