"""Read-only installation readiness, recorded after an owner-verified VPS update."""
import hashlib,json,os,time
from datetime import datetime,timedelta,timezone
from pathlib import Path

KEY='withdrawal_controls_runtime_v1'
ENGINE='NAIRAPIPS_MT5_ENGINE_NON_DD_OPERATIONS_DIRECT_TELEMETRY_V3_20261004.py'
_cache=(0,False)

def engine_hash():
    return hashlib.sha256((Path(__file__).parent/'operations-withdrawal-release'/ENGINE).read_bytes()).hexdigest()

def verified_installation():
    global _cache
    url=os.environ.get('SUPABASE_URL')
    key=os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_KEY')
    if not url or not key:return False
    identity=(url,hashlib.sha256(key.encode()).digest())
    if len(_cache)==3 and _cache[2]==identity and time.monotonic()-_cache[0]<15:return _cache[1]
    ready=False
    try:
        import requests
        from withdrawal_controls import RELEASE
        response=requests.get(url.rstrip('/')+'/rest/v1/package_studio_private_settings',
            headers={'apikey':key,'Authorization':'Bearer '+key},
            params={'select':'value','key':'eq.'+KEY,'limit':'1'},timeout=(2,3))
        response.raise_for_status()
        rows=response.json()
        if len(rows)==1:
            proof=json.loads(rows[0]['value'])
            observed=datetime.fromisoformat(proof['verified_at'].replace('Z','+00:00'))
            ready=(proof.get('enabled') is True and proof.get('release')==RELEASE
                and proof.get('engine_sha256')==engine_hash() and observed.tzinfo is not None
                and observed<=datetime.now(timezone.utc)+timedelta(seconds=5))
    except Exception:
        pass
    _cache=(time.monotonic(),ready,identity)
    return ready
