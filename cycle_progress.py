"""Read-only lifetime cycle progress, using the same ledger verifier as payment gates."""
from collections import defaultdict
import re
from datetime import datetime, timezone
from flask import jsonify
from limited_cycle_safety import OPEN

RELEASE='final-funded-cycle-explicit-2026-10-25'
# Owner-requested historical display only; this does not grant or cap cycles.
LEGACY_DISPLAY_PURCHASE_IDS={'4e4c7880-5593-4ffd-8c1a-2c531635d71f'}

def progress_report(guard, purchases, accounts, payouts):
    by_purchase=defaultdict(list);by_account=defaultdict(list)
    for a in accounts:by_purchase[str(a.get('purchase_id'))].append(a)
    for p in payouts:by_account[str(p.get('trader_account_id'))].append(p)
    result=[];payout_map={}
    for purchase in purchases:
        pid=str(purchase['id']);scope=by_purchase[pid]
        ledger=[p for a in scope for p in by_account[str(a['id'])]]
        if purchase.get('payout_cycles') in (None,0):
            if pid not in LEGACY_DISPLAY_PURCHASE_IDS:continue
            relevant=[p for p in ledger if str(p.get('status') or '').lower() in OPEN|{'paid'} or p.get('paid_at')]
            if not relevant:continue
            paid=set();reserved=set();verified=True
            by_id={str(a['id']):a for a in scope}
            for payout in relevant:
                account=by_id[str(payout['trader_account_id'])]
                if str(account.get('trader_id'))!=str(purchase.get('trader_id')) or str(payout.get('trader_id'))!=str(purchase.get('trader_id')) or str(account.get('stage')).lower()!='funded' or not account.get('mt5_login') or str(account['mt5_login'])!=str(payout.get('mt5_login')):
                    verified=False;continue
                (paid if str(payout.get('status') or '').lower()=='paid' or payout.get('paid_at') else reserved).add(str(account['id']))
            row=dict(purchase_id=pid,trader_id=purchase.get('trader_id'),customer=purchase.get('trader_name') or 'Trader',plan=purchase.get('plan_name') or 'Plan',limit=purchase.get('payout_cycles'),paid=len(paid) if verified else None,reserved=len(reserved-paid) if verified else None,remaining=None,completed=False,verified=verified,status='legacy' if purchase.get('payout_cycles') is None else 'unlimited',display_only=True,reason=None)
            result.append(row)
            for payout in ledger:payout_map[str(payout['id'])]=dict(purchase_id=pid,blocked=False,reason=None,display_only=True)
            continue
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
        open_statuses={'assigned_active','active','current_active','funded_active','live','funded','approved_active','profit_protected','funded_profit_cap_reached','payout_pending','approved_payout_pending','payment_processing'}
        active_unpaid=[a for a in scope if str(a.get('trader_id'))==str(purchase.get('trader_id')) and str(a.get('stage')).lower()=='funded' and a.get('mt5_login') and str(a.get('account_status') or '').lower() in open_statuses and str(a['id']) not in paid_accounts]
        row['final_cycle_active']=bool(row['verified'] and not row['completed'] and row['remaining']==1 and len(active_unpaid)==1)
        row['final_mt5_login']=str(active_unpaid[0]['mt5_login']) if row['final_cycle_active'] else None
        for payout in ledger:
            status=str(payout.get('status') or '').lower();reason=None
            if not row['verified']:reason='Cycle records require review'
            elif row['completed']:reason='COMPLETED — no further payout'
            elif str(payout.get('trader_account_id')) in paid_accounts:reason='This account already has a paid payout'
            elif row['paid']+row['reserved']-(1 if status in OPEN else 0)>=row['limit']:reason='Remaining cycles already reserved'
            source=next((a for a in scope if str(a['id'])==str(payout.get('trader_account_id'))),{})
            receipt=re.search(r'\[NP_PAYOUT_RENEWAL_DONE_V122:'+re.escape(str(payout['id']))+r':([^:\]]+):([^\]]+)\]',str(source.get('archive_reason') or ''))
            delivered=bool(receipt and any(str(a['id'])==receipt.group(1) and str(a.get('mt5_login'))==receipt.group(2) and str(a.get('trader_id'))==str(purchase.get('trader_id')) for a in scope))
            payout_map[str(payout['id'])]={'purchase_id':pid,'blocked':bool(reason),'reason':reason,'renewal_blocked':not row['verified'] or row['completed'] or delivered,'renewal_reason':'Funded replacement already issued' if delivered else 'Purchased cycles completed' if row['completed'] else None}
        result.append(row)
    return {'release':RELEASE,'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'finite_and_recorded_legacy_history',
            'purchases':result,'payouts':payout_map,'counts':{'tracked':len(result),'completed':sum(r['completed'] for r in result),
            'review_required':sum(not r['verified'] for r in result)}}

def read_progress(guard, trader_id=None):
    params={'or':'(payout_cycles.gt.0,id.in.('+','.join(sorted(LEGACY_DISPLAY_PURCHASE_IDS))+'))','select':'id,trader_id,trader_name,plan_name,payout_cycles,lifecycle_state'}
    if trader_id:params['trader_id']='eq.'+str(trader_id)
    purchases=guard.rows('challenge_purchases',params)
    if trader_id:
        purchases=[p for p in purchases if str(p.get('trader_id'))==str(trader_id)]
    accounts=[]
    for start in range(0,len(purchases),100):
        ids=','.join(str(p['id']) for p in purchases[start:start+100])
        accounts.extend(guard.rows('trader_accounts',{'purchase_id':'in.('+ids+')','select':'id,purchase_id,trader_id,stage,mt5_login,account_status,archive_reason'}))
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
