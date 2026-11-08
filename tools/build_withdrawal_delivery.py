from pathlib import Path
import hashlib,json,zipfile
root=Path(__file__).resolve().parents[1];stage=root/'operations-withdrawal-release'
(stage/'withdrawal_controls.py').write_bytes((root/'withdrawal_controls.py').read_bytes())
files={n:(stage/n).read_bytes() for n in ('NAIRAPIPS_MT5_ENGINE_NON_DD_OPERATIONS_DIRECT_TELEMETRY_V3_20261004.py','withdrawal_controls.py')}
files['SHA256.json']=json.dumps({n:hashlib.sha256(b).hexdigest() for n,b in files.items()},sort_keys=True).encode()
for n in ('INSTALL.ps1','CHECK_ONLY.cmd','INSTALL_UPDATE.cmd','README.txt'):files[n]=(stage/n).read_bytes()
with zipfile.ZipFile(root/'website-downloads/nairapips-operations-withdrawal-update.zip','w',zipfile.ZIP_DEFLATED) as z:
 for name,body in files.items():
  info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,body)
print('Operations withdrawal update packaged; no VPS process was touched.')
