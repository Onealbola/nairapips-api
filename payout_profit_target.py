"""Exact-account payout target gate; never changes purchased rules or balances."""
from datetime import datetime,timezone
from decimal import Decimal
from withdrawal_controls import read

def gate(account,quote):
    terms=read(account)
    q=dict(quote or {})
    if not terms or not terms['minimum_profit_enabled']:return q
    target=Decimal(str(terms['minimum_profit_percent']))
    verified=q.get('verified') is True
    met=verified and Decimal(str(q.get('start_balance') or 0))>0 and Decimal(str(q.get('verified_profit') or 0))>=Decimal(str(q['start_balance']))*target/100
    reason=('This plan requires '+str(target)+'% verified profit before payout.' if verified else 'Your profit target must be verified against a fresh account update before payout.')
    q.update(withdrawal_rules=terms,payout_target_met=bool(met),payout_target_percent=str(target))
    if not met:q.update(available_payout=0,reason=reason,payout_target_reason=reason)
    return q

def install(g):
    previous=g['_np_verified_payout_quote']
    def quote(account,*args,**kwargs):
        result=gate(account,previous(account,*args,**kwargs))
        timing=schedule(g,account)
        result.update(timing)
        if timing.get('payout_schedule_met') is False:result.update(available_payout=0,reason=timing['payout_schedule_reason'])
        return result
    g['_np_verified_payout_quote']=quote

def schedule(g,account,now=None):
    from plan_risk_controls import frozen_terms,payout_due,WAT
    terms=frozen_terms(account)
    if not terms or terms['payout_frequency']=='On Demand':return {}
    frequency=terms['payout_frequency']
    try:
        read=g['_np_query_rows_v173']
        purchases=read('challenge_purchases',filters=[('eq','id',account['purchase_id']),('eq','trader_id',account['trader_id'])],limit=1)
        if len(purchases)!=1:raise ValueError('Exact purchase missing')
        paid=read('payouts',filters=[('eq','trader_account_id',account['id']),('eq','trader_id',account['trader_id']),('eq','status','paid')],limit=1000)
        if len(paid)>=1000:raise ValueError('Payout history incomplete')
        due=payout_due(account,purchases[0],paid)
        met=(now or datetime.now(timezone.utc))>=due
        return dict(payout_frequency=frequency,payout_schedule_met=met,next_payout_at=due.isoformat(),payout_schedule_reason='' if met else 'Your '+frequency.lower()+' payout opens on '+due.astimezone(WAT).strftime('%d %b %Y at %H:%M WAT')+'.')
    except Exception:
        return dict(payout_frequency=frequency,payout_schedule_met=False,payout_schedule_reason='Your purchased payout schedule could not be verified. Please try again shortly.')

def enforce_schedule(g,account):
    timing=schedule(g,account)
    if timing.get('payout_schedule_met') is False:raise ValueError(timing['payout_schedule_reason'])
