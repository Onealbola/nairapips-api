"""Discover unpaid delivery obligations for finite purchased payout cycles only."""
from plan_cycles import paid_cycle_decision

def finite_renewal_owed(source,payout,transport,url,key):
    source,payout=source or {},payout or {}
    sid,tid,pid=source.get('id'),source.get('trader_id'),payout.get('id')
    purchase_id=source.get('purchase_id')
    if not all((sid,tid,pid,purchase_id)) or source.get('stage')!='funded':return False
    if payout.get('status')!='paid' or not payout.get('paid_at'):return False
    if str(payout.get('trader_id'))!=str(tid) or str(payout.get('trader_account_id'))!=str(sid):return False
    if str(source.get('mt5_login') or '')!=str(payout.get('mt5_login') or ''):return False
    if f'[NP_PAYOUT_RENEWAL_DONE_V122:{pid}:' in str(source.get('archive_reason') or ''):return False
    response=transport.get(url.rstrip('/')+'/rest/v1/challenge_purchases',headers={'apikey':key,'Authorization':'Bearer '+key},params={'select':'id,trader_id,payout_cycles,lifecycle_state,trader_account_id','id':'eq.'+str(purchase_id),'trader_id':'eq.'+str(tid)},timeout=(2,5))
    response.raise_for_status();rows=response.json()
    if not isinstance(rows,list) or len(rows)!=1:return False
    purchase=rows[0]
    if type(purchase.get('payout_cycles')) is not int or not 1<=purchase['payout_cycles']<=5:return False
    if purchase.get('lifecycle_state')!='funded_waiting_mt5' or str(purchase.get('trader_account_id'))!=str(sid):return False
    decision=paid_cycle_decision(transport,url,key,pid,complete=False)
    return (decision.get('state')=='renew' and decision.get('cycle_limit')==purchase['payout_cycles']
            and type(decision.get('paid_cycles')) is int and 0<decision['paid_cycles']<purchase['payout_cycles'])
