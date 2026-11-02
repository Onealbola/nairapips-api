"""Install a private owner artifact after the protected-storage SQL migration.

Usage: python tools/install_package_studio_assets.py /private/path/owner-assets.json
Never commit the owner artifact: it contains private underwriting and pricing.
This command installs UI/defaults only; it never edits an existing saved draft.
"""
import json
import os
import sys
from datetime import datetime,timezone
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import requests
from package_studio import ASSET_KEY, StudioStore, validate_config


def install(path):
    asset=json.loads(Path(path).read_text())
    if set(asset)!={'defaults','html'}:
        raise ValueError('Supply only the private HTML and default configuration')
    validate_config(asset['defaults'])
    if not isinstance(asset['html'],str) or not 1000<len(asset['html'])<500000:
        raise ValueError('Invalid owner Studio HTML')
    store=StudioStore(requests,os.environ['SUPABASE_URL'],os.environ['SUPABASE_KEY'])
    # A protected-table read must succeed before any write.
    r=requests.get(store.assets.url,headers=store.assets.headers,
                   params={'key':'eq.'+ASSET_KEY,'select':'value,updated_by','limit':1},timeout=20)
    if not r.ok:
        raise RuntimeError('Private storage is not ready (HTTP '+str(r.status_code)+'). Run the SQL migration first.')
    rows=r.json();old=json.loads(rows[0]['value']) if rows else None
    if old and old['defaults']==asset['defaults'] and old['html']==asset['html']:
        print('Owner assets already installed; no write performed.');return
    raw={'value':rows[0]['value'],'updated_by':rows[0].get('updated_by')} if rows else None
    doc=dict(revision=(old['revision'] if old else 0)+1,updated_at=datetime.now(timezone.utc).isoformat(),**asset)
    confirmed=store.assets.persist(doc,raw,'owner-studio-install')
    print('Confirmed private owner assets revision',confirmed['revision'],'— checkout remains disabled.')


if __name__=='__main__':
    try:install(sys.argv[1])
    except Exception as exc:
        # Never print credential-bearing request exceptions.
        print(str(exc) if isinstance(exc,(RuntimeError,ValueError)) else 'Owner asset installation failed. Check setup and retry.',file=sys.stderr)
        sys.exit(1)
