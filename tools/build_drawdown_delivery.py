"""Package the reviewed broker worker, without executing any MT5 code."""
from pathlib import Path
import hashlib,json,zipfile
root=Path(__file__).resolve().parents[1]
files={name:(root/'drawdown-release'/name).read_bytes() for name in ['DD_POLICE_STATIC_TRAILING_V1_20261107.py','package_dd_runtime.py','package_account_rules.py','package_contracts.py']}
files['equity_drawdown.py']=(root/'equity_drawdown.py').read_bytes()
files['SHA256.json']=json.dumps({k:hashlib.sha256(v).hexdigest() for k,v in files.items()},indent=2).encode()
for name in ['README.txt','INSTALL.ps1','CHECK_ONLY.cmd','INSTALL_UPDATE.cmd']:files[name]=(root/'drawdown-release'/name).read_bytes()
path=root/'website-downloads'/'nairapips-static-trailing-dd-update.zip'
with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
    for name,content in files.items():
        info=zipfile.ZipInfo(name);info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED
        z.writestr(info,content)
print('Static / Equity Trailing VPS package:',path.stat().st_size,'bytes')
