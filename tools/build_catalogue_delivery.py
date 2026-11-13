"""Keep the inline fallback, hosted selector and cPanel delivery bundles aligned."""
from pathlib import Path
import hashlib
import re
import zipfile

root=Path(__file__).resolve().parents[1]/'website-downloads'
page=root/'trader_clean.html'
html=page.read_text()
marker=html.index('/* Saved Business Settings -> trader catalogue.')
start=html.rfind('<script>',0,marker)+len('<script>')
end=html.index('</script>',marker)
assert start>=len('<script>')
html=html[:start]+(root/'account-plan-selector.js').read_text()+'\n'+(root/'trader-catalog-sync.js').read_text()+'\n'+(root/'account-reporting.js').read_text()+'\n'+(root/'private-offer-scope.js').read_text()+html[end:]
remote='<script src="https://nairapips-api.onrender.com/public/trader-catalogue.js"></script>'
assert html.count(remote)==1
page.write_text(html)
payload=page.read_bytes()
installer=root/'apply-catalogue-update.php'
php,count=re.subn(r"(?<=!== ')[a-f0-9]{64}(?=')",hashlib.sha256(payload).hexdigest(),installer.read_text())
assert count==1
installer.write_text(php)

def archive(path,entries):
    with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
        for name,content in entries.items():
            assert not name.startswith('/') and '..' not in name.split('/')
            info=zipfile.ZipInfo(name)
            info.external_attr=0o100644<<16
            info.compress_type=zipfile.ZIP_DEFLATED
            z.writestr(info,content)

archive(root/'nairapips-complete-catalogue-update.zip',{
    'apply-catalogue-update.php':installer.read_bytes(),
    '.np-catalogue-files/trader_clean.html':payload})
with zipfile.ZipFile(root/'nairapips-admin-small.zip') as z:
    entries={'admin/'+name:z.read(name) for name in z.namelist()}
entries['dashboard/trader_clean.html']=payload
archive(root/'nairapips-catalogue-sync.zip',entries)
print('Catalogue fallback, installer checksum and both delivery ZIPs aligned.')
