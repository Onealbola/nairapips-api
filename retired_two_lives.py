"""Owner-directed retirement of two-lives delivery; no payout/history writes."""
import re
from functools import wraps
from datetime import datetime,timezone

# 3 October 2026 00:00 in Nigeria (WAT).
CURRENT_PRODUCT_START=datetime(2026,10,2,23,tzinfo=timezone.utc)

def current_product(row):
    try:return datetime.fromisoformat(str((row or {}).get('created_at') or '').replace('Z','+00:00')).astimezone(timezone.utc)>=CURRENT_PRODUCT_START
    except (ValueError,TypeError):return False

REASON='two_lives_journey_retired'
MESSAGE='This two-lives journey is inactive and retired. Trading activation, payouts, resets, progression and renewals are unavailable. Contact support about the refund option, or use a separate eligible new purchase.'

def truth(value):
    return value is True or str(value).strip().lower() in {'true','1','yes'}

def marked(row):
    row=row or {}
    if truth(row.get('second_life_enabled')) or truth(row.get('second_life_used')):return True
    for key in ('lives_total','life_number'):
        try:
            if int(row.get(key) or 0)>=2:return True
        except (ValueError,TypeError):pass
    return str(row.get('second_life_status') or '').lower() in {'available_on_eligible_breach','available','life2_waiting_mt5','life2_active','life2_breached','life2_completed'}

class Retirement:
    def __init__(self,runtime):self.r=runtime
    def read(self,table,identity,owner):
        if not identity:return {}
        r=self.r['requests'].get(self.r['SUPABASE_URL'].rstrip('/')+'/rest/v1/'+table,
            headers={'apikey':self.r.get('SUPABASE_SERVICE_ROLE_KEY') or self.r['SUPABASE_KEY'],
                     'Authorization':'Bearer '+(self.r.get('SUPABASE_SERVICE_ROLE_KEY') or self.r['SUPABASE_KEY'])},
            params={'select':'*','id':'eq.'+str(identity),'trader_id':'eq.'+str(owner),'limit':'1'},timeout=(2,5))
        r.raise_for_status();rows=r.json()
        if not isinstance(rows,list) or len(rows)!=1 or str(rows[0].get('trader_id'))!=str(owner):raise ValueError('Journey lineage could not be verified')
        return rows[0]
    def blocked(self,owner,purchase=None,source=None,reason=''):
        p=purchase or {};a=source or {};seen=set()
        retired=marked(a) or marked(p)
        pid=p.get('id') or a.get('purchase_id') or a.get('challenge_purchase_id')
        # Never infer retirement from another purchase belonging to this person.
        for _ in range(12):
            if not pid:return not current_product(p or a) and (retired or str(reason or '').strip().lower() in {'second_life','free_second_life','second_life_free_reset'})
            if str(pid) in seen:raise ValueError('Cyclic journey lineage')
            seen.add(str(pid));p=self.read('challenge_purchases',pid,owner)
            retired=retired or marked(p) or p.get('lifecycle_state')=='retired_two_lives'
            marker=re.search(r'\[NP_RESET_REQUEST:([^:\]]+):([^:\]]+):(phase1|phase2|funded)\]',str(p.get('admin_note') or ''),re.I)
            parent=p.get('reset_parent_purchase_id') or p.get('parent_purchase_id')
            if marker:parent=marker.group(2)
            if not parent:return retired and not current_product(p)
            pid=parent
        raise ValueError('Journey lineage exceeds verification limit')
    def stopped(self,*args,**kwargs):
        try:return self.blocked(*args,**kwargs)
        except Exception:return True  # No delivery or stock warning from unverified lineage.

def install(runtime):
    policy=Retirement(runtime);runtime['_np_two_lives_retirement']=policy
    def owner(trader=None,purchase=None,source=None):return str((trader or {}).get('id') or (purchase or {}).get('trader_id') or (source or {}).get('trader_id') or '')
    base=runtime['_assign_mt5_to_trader']
    @wraps(base)
    def assign(trader,mt5,stage,purchase=None,staff=None,note='MT5 assigned'):
        source=None
        match=re.search(r'source_account(?:_id)?=([0-9a-fA-F-]{36})',str(note))
        if not purchase and match:source=policy.read('trader_accounts',match.group(1),owner(trader))
        try:blocked=policy.blocked(owner(trader),purchase,source)
        except Exception as exc:raise ValueError('Journey entitlement could not be verified. No MT5 assignment was authorised.') from exc
        if blocked:raise ValueError(MESSAGE)
        return base(trader,mt5,stage,purchase,staff,note)
    runtime['_assign_mt5_to_trader']=assign
    original=runtime['_np_auto_assign_waiting_stage']
    @wraps(original)
    def auto(trader,stage,purchase=None,source_account=None,reason='lifecycle_progression'):
        if policy.stopped(owner(trader,purchase,source_account),purchase,source_account,reason):return None
        return original(trader,stage,purchase,source_account,reason)
    runtime['_np_auto_assign_waiting_stage']=auto
    clean_root=runtime.get('_np_is_clean_root_v43')
    if clean_root:
        def eligible_root(purchase):
            if policy.stopped(owner(purchase=purchase),purchase=purchase):return False
            return clean_root(purchase)
        runtime['_np_is_clean_root_v43']=eligible_root
    original_source=runtime['_np_v122_source_block_reason']
    def source_block(source,payout=None):
        if policy.stopped(owner(source=source),source=source):return REASON
        return original_source(source,payout)
    runtime['_np_v122_source_block_reason']=source_block
    def wrap_simple(name,check):
        original=runtime.get(name)
        if not callable(original):return
        @wraps(original)
        def wrapped(*args,**kwargs):
            if check(*args,**kwargs):return None
            return original(*args,**kwargs)
        runtime[name]=wrapped
    wrap_simple('_np_retry_waiting_second_life_assignment',lambda *a,**k:True)
    wrap_simple('_np_auto_free_phase1_reset_after_breach',lambda trader,breached_account:policy.stopped(owner(trader,source=breached_account),source=breached_account))
    wrap_simple('_np_auto_fulfill_exact_reset_v43',lambda order:policy.stopped(owner(purchase=order),purchase=order))
    def payout_stopped(payout):
        try:
            source,_=runtime['_np_v122_load_source'](payout)
            return not source or policy.stopped(owner(source=source),source=source)
        except Exception:return True
    wrap_simple('_np_auto_post_payout_renewal',payout_stopped)
    original_reset=runtime.get('_np_reset_policy')
    if original_reset:
        def reset(account,trader=None):
            if policy.stopped(owner(trader,source=account),source=account):return {'eligible':False,'reason':REASON,'title':'Journey retired','subtitle':MESSAGE,'price':None}
            return original_reset(account,trader)
        runtime['_np_reset_policy']=reset
    original_alert=runtime['_np_inventory_wait_alert']
    def alert(stage,account_size,reason,trader=None):
        if str(reason or '').lower() in {'second_life','free_second_life','second_life_free_reset'}:return None
        payout=re.search(r'payout(?: renewal(?: retry)?)?\s+([0-9a-fA-F-]{36})',str(reason),re.I)
        if payout:
            try:
                p=runtime['_np_v122_load_payout'](payout.group(1));a,_=runtime['_np_v122_load_source'](p)
                if policy.stopped(owner(source=a),source=a):return None
            except Exception:return None
        return original_alert(stage,account_size,reason,trader)
    runtime['_np_inventory_wait_alert']=alert
    # Enforce retirement before the existing shared financial action guard can
    # claim, approve or mark a payout paid. No historic payout is edited here.
    guard=runtime.get('_np_limited_cycle_guard')
    if guard:
        original_acquire=guard.acquire
        def acquire(account,payout_id='',action='create'):
            from limited_cycle_safety import CycleBlocked
            if policy.stopped(owner(source=account),source=account):raise CycleBlocked(MESSAGE)
            return original_acquire(account,payout_id,action)
        guard.acquire=acquire
    activation=runtime.get('_np_monitoring_registry_activate')
    if activation:
        def activate(account,predecessor_account_id=None,reason='assignment_exchange'):
            if policy.stopped(owner(source=account),source=account):raise ValueError(MESSAGE)
            return activation(account,predecessor_account_id,reason)
        runtime['_np_monitoring_registry_activate']=activate
    if runtime.get('app'):
        from flask import jsonify
        @runtime['app'].get('/two_lives_retirement_version')
        def retirement_version():
            return jsonify(success=True,release='two-lives-retired-cutoff-2026-10-29',current_product_from='2026-10-03',cutoff_timezone='Africa/Lagos',new_delivery_blocked=True,payout_actions_blocked=True,monitoring_activation_blocked=True,automatic_refunds=False)
