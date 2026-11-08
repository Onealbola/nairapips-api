"""Recovery contains no root index: source is the owner's real hosting backup."""
from pathlib import Path
import hashlib,zipfile,sys
root=Path(__file__).resolve().parents[1]
assets=root/'website-downloads'
sys.path.insert(0,str(root))
from landing_bundle import build_landing_bundle
script=build_landing_bundle(assets).encode()
php=(assets/'restore-landing.php').read_bytes().replace(b'PAYLOAD_SHA256',hashlib.sha256(script).hexdigest().encode())
assert b'PAYLOAD_SHA256' not in php
entries={'restore-landing.php':php,'.np-landing-files/landing-catalogue.js':script}
with zipfile.ZipFile(assets/'nairapips-landing-recovery.zip','w',zipfile.ZIP_DEFLATED) as archive:
 for name,data in entries.items():
  info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED;archive.writestr(info,data)
print('Homepage-only recovery archive prepared; no root index or admin files included.')
