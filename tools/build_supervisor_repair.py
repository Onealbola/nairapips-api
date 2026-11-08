from pathlib import Path
import hashlib,json,zipfile,ast
root=Path(__file__).resolve().parents[1]
source=root/'supervisor-repair'
names=['NAIRAPIPS_DD_FLEET_SUPERVISOR_V3_20_CLEAN_LIVE_STATE_20261008.py','fleet_instance_lock.py','DD_ROSTER_WATCHER_V1_20261003.py','DD_BREACH_RELAY_V1_20261003.py']
files={name:(source/name).read_bytes() for name in names}
for name,payload in files.items():ast.parse(payload.decode())
files['SHA256.json']=json.dumps({name:hashlib.sha256(payload).hexdigest() for name,payload in files.items()},indent=2).encode()
for name in ['REPAIR_SUPERVISOR.cmd','REPAIR_SUPERVISOR.ps1','README.txt']:files[name]=(source/name).read_bytes()
with zipfile.ZipFile(root/'website-downloads'/'nairapips-dd-supervisor-repair.zip','w',zipfile.ZIP_DEFLATED) as z:
 for name,payload in files.items():
  info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,payload)
print('Built complete DD supervisor recovery ZIP.')
