"""Review/apply the additive package migration through Supabase Management API.

Default is read-only --plan. --apply requires SUPABASE_ACCESS_TOKEN supplied
securely in environment settings, in addition to the existing REST bindings.
No credentials or customer records are printed. Never auto-retry a DDL timeout.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit
import requests

ROOT=Path(__file__).resolve().parents[1]
MIGRATION=ROOT/'migrations/20261017_package_contracts.sql'
TABLES={'np_package_quotes','np_package_contracts','np_package_usage','np_package_entitlements','np_package_settlements','np_package_pool_verifications','np_package_notifications'}


def main():
    p=argparse.ArgumentParser();p.add_argument('--apply',action='store_true');p.add_argument('--plan',action='store_true');args=p.parse_args()
    url=os.environ.get('SUPABASE_URL','').rstrip('/');key=os.environ.get('SUPABASE_KEY')
    host=urlsplit(url).hostname or ''
    if not re.fullmatch(r'[a-z0-9]+\.supabase\.co',host) or not key:
        p.error('The existing Supabase URL/key bindings must be present and valid')
    headers={'apikey':key,'Authorization':'Bearer '+key}
    response=requests.get(url+'/rest/v1/',headers=headers,timeout=(2,8));response.raise_for_status()
    schema=response.json()
    definitions=schema.get('definitions',{})
    for name in ('traders','challenge_purchases','trader_accounts','payouts','business_settings'):
        if name not in definitions:raise RuntimeError('Expected existing production schema is incomplete')
    cfg=requests.get(url+'/rest/v1/business_settings',headers=headers,
                     params={'select':'value','key':'eq.future_package_configuration_v1','limit':'1'},timeout=(2,5))
    cfg.raise_for_status();rows=cfg.json()
    if rows and json.loads(rows[0]['value']).get('enabled') is not False:
        raise RuntimeError('Package checkout must be switched off before applying this migration')
    payload=MIGRATION.read_text();sha=hashlib.sha256(payload.encode()).hexdigest()
    print('Migration:',MIGRATION.name);print('SHA256:',sha)
    print('Existing package tables:',len(TABLES & set(definitions)),'of',len(TABLES))
    print('Plan: add package-only tables/RPCs and nullable contract bindings; no legacy row migration or checkout activation.')
    if not args.apply:
        print('Read-only plan complete; database unchanged.');return 0
    token=os.environ.get('SUPABASE_ACCESS_TOKEN')
    if not token:
        print('Blocked: SUPABASE_ACCESS_TOKEN is not available. Supply it securely in environment settings, never in chat.',file=sys.stderr);return 2
    backup=Path('/workspace/scratch')/('package-schema-before-'+sha[:12]+'.json')
    if not backup.exists():
        with backup.open('x',encoding='utf-8') as f:
            os.chmod(backup,0o600);json.dump(schema,f)
    project=host.split('.')[0]
    try:
        r=requests.post('https://api.supabase.com/v1/projects/'+project+'/database/query',
                        headers={'Authorization':'Bearer '+token},json={'query':payload},timeout=(3,40))
        r.raise_for_status()
    except Exception as exc:
        print('Migration response was not confirmed. Inspect the schema before attempting another application. No automatic retry was made.',file=sys.stderr)
        print('Failure type:',type(exc).__name__,file=sys.stderr);return 1
    print('Management API accepted the migration. Verify the REST schema and package ledger RPCs before any release.');return 0

if __name__=='__main__':raise SystemExit(main())
