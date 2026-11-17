"""Build standalone panels and the complete cPanel update with deterministic ZIPs."""
from pathlib import Path
import subprocess
import sys
import zipfile

from build_mobile_panels import build

BASE = Path(__file__).resolve().parent.parent
ASSETS = BASE / 'website-downloads'

def archive(path, entries):
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as output:
        for name, content in entries.items():
            info = zipfile.ZipInfo(name)
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            output.writestr(info, content)

if __name__ == '__main__':
    build()
    subprocess.run([sys.executable, str(BASE / 'tools/build_small_admin.py')], cwd=BASE, check=True)
    archive(ASSETS / 'admin-restore.zip', {
        'admin/admin_clean.html': (ASSETS / 'admin_clean_cpanel.html').read_bytes(),
    })
    with zipfile.ZipFile(ASSETS / 'nairapips-admin-small.zip') as compact:
        entries = {'admin/' + name: compact.read(name) for name in compact.namelist() if name != 'index.html'}
        archive(ASSETS / 'nairapips-admin-mobile.zip', {
            name: compact.read(name) for name in compact.namelist() if name != 'index.html'
        })
    archive(ASSETS / 'nairapips-trader-mobile.zip', {
        'trader_clean.html': (ASSETS / 'trader_clean.html').read_bytes(),
    })
    entries['dashboard/trader_clean.html'] = (ASSETS / 'trader_clean.html').read_bytes()
    entries['MOBILE-INSTALL.md'] = (BASE / 'MOBILE_PANELS.md').read_bytes()
    archive(ASSETS / 'nairapips-mobile-update.zip', entries)
    catalogue = ASSETS / 'nairapips-catalogue-sync.zip'
    if catalogue.exists():
        with zipfile.ZipFile(catalogue) as previous:
            retained = {name: previous.read(name) for name in previous.namelist()
                        if name not in ('admin/admin_clean.html', 'dashboard/trader_clean.html')
                        and not name.startswith('admin/admin-assets/')}
        retained.update({name: content for name, content in entries.items()
                         if name != 'MOBILE-INSTALL.md'})
        archive(catalogue, retained)
    print('Ready:', ASSETS / 'nairapips-mobile-update.zip')
