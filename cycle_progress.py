"""Read-only lifetime cycle progress, using the same ledger verifier as payment gates."""
from collections import defaultdict
from datetime import datetime, timezone
from flask import jsonify
from limited_cycle_safety import OPEN

RELEASE='admin-cycle-progress-2026-10-22'

def progress_report(guard, purchases, accounts, payouts):
    by_purchase=defaultdict(list);by_account=defaultdict(list)
    for a in accounts:by_purchase[str(a.get('purchase_id'))].append(a)
    for p in payouts:by_account[str(p.get('trader_account_id'))].append(p)
    result=[];payout_map={}
    for purchase in purchases:
        if purchase.get('payout_cycles') in (None,0):continue
        pid=str(purchase['id']);scope=by_purchase[pid]
        ledger=[p for a in scope for p in by_account[str(a['id'])]]
        row={'purchase_id':pid,'trader_id':purchase.get('trader_id'),'customer':purchase.get('trader_name') or 'Trader',
             'plan':purchase.get('plan_name') or 'Plan','limit':purchase['payout_cycles'],'paid':None,'reserved':None,'remaining':None,
             'completed':False,'verified':False,'reason':None}
        try:
            funded=[a for a in scope if str(a.get('stage') or '').lower()=='funded']
            if not funded:
                if ledger:raise ValueError('Payout exists without a verified funded account')
                state={'limit':purchase['payout_cycles'],'paid':0,'reserved':0,'remaining':purchase['payout_cycles']}
                if type(state['limit']) is not int or not 1<=state['limit']<=5:raise ValueError('Invalid purchased cycle limit')
            else:state=guard.inspect(purchase,funded[0],accounts=scope,ledger=ledger)
            row.update(state,verified=True,completed=state['paid']>=state['limit'] or purchase.get('lifecycle_state')=='funded_cycles_completed')
            row['status']='completed' if row['completed'] else 'active' if funded else 'awaiting_funded'
        except Exception as exc:
            row.update(status='review_required',reason=str(exc))
        paid_accounts={str(p['trader_account_id']) for p in ledger if str(p.get('status') or '').lower()=='paid' or p.get('paid_at')}
        for payout in ledger:
            status=str(payout.get('status') or '').lower();reason=None
            if not row['verified']:reason='Cycle records require review'
            elif row['completed']:reason='COMPLETED — no further payout'
            elif str(payout.get('trader_account_id')) in paid_accounts:reason='This account already has a paid payout'
            elif row['paid']+row['reserved']-(1 if status in OPEN else 0)>=row['limit']:reason='Remaining cycles already reserved'
            payout_map[str(payout['id'])]={'purchase_id':pid,'blocked':bool(reason),'reason':reason}
        result.append(row)
    return {'release':RELEASE,'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'finite_purchases_only',
            'purchases':result,'payouts':payout_map,'counts':{'tracked':len(result),'completed':sum(r['completed'] for r in result),
            'review_required':sum(not r['verified'] for r in result)}}

def read_progress(guard, trader_id=None):
    params={'payout_cycles':'gt.0','select':'id,trader_id,trader_name,plan_name,payout_cycles,lifecycle_state'}
    if trader_id:params['trader_id']='eq.'+str(trader_id)
    purchases=guard.rows('challenge_purchases',params)
    if trader_id:
        purchases=[p for p in purchases if str(p.get('trader_id'))==str(trader_id)]
    accounts=[]
    for start in range(0,len(purchases),100):
        ids=','.join(str(p['id']) for p in purchases[start:start+100])
        accounts.extend(guard.rows('trader_accounts',{'purchase_id':'in.('+ids+')','select':'id,purchase_id,trader_id,stage,mt5_login'}))
    payouts=[]
    for start in range(0,len(accounts),100):
        ids=','.join(str(a['id']) for a in accounts[start:start+100])
        payouts.extend(guard.rows('payouts',{'trader_account_id':'in.('+ids+')','select':'id,trader_id,trader_account_id,mt5_login,status,paid_at'}))
    return progress_report(guard,purchases,accounts,payouts)

def register_cycle_progress(app, guard, require_admin):
    @app.get('/funded_cycle_progress_version')
    def cycle_progress_version():
        return jsonify(success=True,release=RELEASE,read_only=True,refresh_seconds=30)
    @app.get('/admin/funded_cycle_progress')
    def cycle_progress():
        _,error=require_admin()
        if error is not None:return error
        try:
            response=jsonify(success=True,data=read_progress(guard))
            response.headers['Cache-Control']='no-store'
            return response
        except Exception:
            return jsonify(success=False,error='Complete cycle progress could not be verified. Refresh before authorizing payment.'),503


def register_trader_cycle_progress(app, guard, authenticate):
    @app.get('/trader/funded_cycle_progress')
    def trader_cycle_progress():
        trader_id, error = authenticate()
        if error or not trader_id:
            return jsonify(success=False,error='Trader authentication is required'),401
        try:
            report=read_progress(guard,trader_id)
            fields=('purchase_id','plan','limit','paid','reserved','remaining','completed','verified','status')
            response=jsonify(success=True,data={'generated_at':report['generated_at'],
                'purchases':[{k:row[k] for k in fields} for row in report['purchases']]})
        except Exception:
            response=jsonify(success=False,error='Payout cycle status is temporarily unavailable. Please refresh or contact support.')
            response.status_code=503
        response.headers['Cache-Control']='no-store, private'
        return response
