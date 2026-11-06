"""Root-safe cPanel archive: extract into public_html, never replace its index."""
from pathlib import Path
import zipfile
root=Path(__file__).resolve().parents[1]/'website-downloads'
with zipfile.ZipFile(root/'nairapips-admin-small.zip') as source, zipfile.ZipFile(root/'nairapips-business-email-update.zip','w',zipfile.ZIP_DEFLATED) as target:
    for name in source.namelist():
        if name.startswith('/') or '..' in name.split('/'):raise ValueError('Invalid archive path')
        item=zipfile.ZipInfo('admin/'+name);item.external_attr=0o100644<<16;item.compress_type=zipfile.ZIP_DEFLATED
        target.writestr(item,source.read(name))
print('Business email update: all files under admin/; extract into public_html.')
