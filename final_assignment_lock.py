"""Owner-requested assignment closure. Payout permissions are not changed."""
import re
from functools import wraps
LOCKED_PURCHASE='188dc023-6247-467c-ae2a-82c237a43239'
LOCKED_OWNER='793703ed-da35-46ed-9a48-4ac6ed8af748'
FINAL_ACCOUNT='18385891-48b3-4fa8-b555-97ea6f8ee96e'
MESSAGE='This purchase has received its final funded account. No further MT5 assignment is allowed. A separate new purchase is required.'

def check_assignment(trader,purchase,note,transport,url,key,approval=False):
    tid=str((trader or {}).get('id') or (trader or {}).get('trader_id') or '')
    if tid!=LOCKED_OWNER:return
    headers={'apikey':key,'Authorization':'Bearer '+key}
    def read(table,params):
        r=transport.get(url.rstrip('/')+'/rest/v1/'+table,headers=headers,params=params,timeout=(2,5));r.raise_for_status();rows=r.json()
        if not isinstance(rows,list):raise ValueError('Assignment entitlement could not be verified.')
        return rows
    pid=str((purchase or {}).get('id') or '')
    if not pid:
        match=re.search(r'source_account(?:_id)?=([0-9a-fA-F-]{36})',str(note or ''))
        if match:
            rows=read('trader_accounts',{'select':'id,trader_id,purchase_id','id':'eq.'+match.group(1),'trader_id':'eq.'+tid})
            if len(rows)==1:pid=str(rows[0].get('purchase_id') or '')
    if not pid or pid==LOCKED_PURCHASE:raise ValueError(MESSAGE)
    rows=read('challenge_purchases',{'select':'id,trader_id,status,purchase_type,plan_name','id':'eq.'+pid,'trader_id':'eq.'+tid})
    if len(rows)!=1 or str(rows[0].get('id'))!=pid or str(rows[0].get('trader_id'))!=tid:raise ValueError(MESSAGE)
    row=rows[0]
    valid_statuses={'approved','paid','approved_active','approved_waiting_mt5','assigned'}
    if approval:valid_statuses.update({'pending','pending_review','submitted','awaiting_approval'})
    if str(row.get('status') or '').lower() not in valid_statuses or 'reset' in str(row.get('purchase_type') or '').lower() or 'reset' in str(row.get('plan_name') or '').lower():raise ValueError(MESSAGE)

def install(runtime):
    base=runtime['_assign_mt5_to_trader']
    @wraps(base)
    def guarded(trader,mt5,stage,purchase=None,staff=None,note='MT5 assigned'):
        check_assignment(trader,purchase,note,runtime['requests'],runtime['SUPABASE_URL'],runtime.get('SUPABASE_SERVICE_ROLE_KEY') or runtime['SUPABASE_KEY'],approval=bool(runtime.get('_NP_APPROVAL_CONTEXT_V216') and runtime['_NP_APPROVAL_CONTEXT_V216'].get()))
        return base(trader,mt5,stage,purchase,staff,note)
    runtime['_assign_mt5_to_trader']=guarded
