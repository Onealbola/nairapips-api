"""Self-contained cPanel visual update; keeps the owner's existing homepage and forms."""
from pathlib import Path
import hashlib,json,sys,zipfile
root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root))
from landing_bundle import build_landing_bundle
assets=root/'website-downloads'
script=build_landing_bundle(assets).replace('https://nairapips-api.onrender.com/public/nairapips-luxury-concept.png','/.np-luxury-files/luxury-art.png').replace('https://nairapips-api.onrender.com/public/nairapips-gold-architecture.png?v=luxury-20261108','/.np-luxury-files/architecture.png')
art=(assets/'nairapips-luxury-concept.png').read_bytes()
architecture=(assets/'nairapips-gold-architecture.png').read_bytes()
php=(assets/'install-luxury.php').read_text().replace('PAYLOAD_SHA256',hashlib.sha256(script.encode()).hexdigest()).replace('ART_SHA256',hashlib.sha256(art).hexdigest()).replace('ARCH_SHA256',hashlib.sha256(architecture).hexdigest())
entries={'install-luxury.php':php.encode(),'.np-luxury-files/luxury-bundle.js':script.encode(),'.np-luxury-files/luxury-art.png':art,'.np-luxury-files/architecture.png':architecture}
with zipfile.ZipFile(assets/'nairapips-luxury-landing-update.zip','w',zipfile.ZIP_DEFLATED) as z:
 for name,data in entries.items():
  info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,data)
print('Luxury landing update built with local artwork and local bundled fallback. No root index, admin files or business settings in archive.')
