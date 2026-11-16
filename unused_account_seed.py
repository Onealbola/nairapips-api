"""Collector-authenticated insert-only companion to verified unused recovery."""
import json
from datetime import datetime,timezone
from flask import request,jsonify
from equity_drawdown import spec,initial,fingerprint,timestamp,verify_ingest
from unused_account_recovery import money,RELEASE

def checked_seed(account,proof,now=None):
    rules=spec(account);now=now or datetime.now(timezone.utc)
    if rules['drawdown_type']!='STATIC' or money(rules.get('daily_drawdown',0))<=0:raise ValueError('Exact static daily account required')
    if account.get('breached_at') or any(s in str(account.get('account_status') or '').lower() for s in ('breach','archive','closed','passed','reset','locked')):raise ValueError('Terminal accounts cannot be restored')
    if account.get('monitoring_enabled') is not True:raise ValueError('Live monitoring enrollment required')
    for key in ('account_id','trader_id','mt5_login'):
        if str(proof.get(key))!=str(rules[key]):raise ValueError('Broker proof identity mismatch')
    if proof.get('release')!=RELEASE or proof.get('rules_hash')!=fingerprint(rules):raise ValueError('Broker proof rules mismatch')
    if not 0<=(now-timestamp(proof.get('verified_at'))).total_seconds()<=60:raise ValueError('Fresh broker proof required')
    capital=money(rules['capital'])
    for key in ('capital','balance','equity','funding_total'):
        if money(proof.get(key))!=capital:raise ValueError('Exact funded capital required')
    for key in ('profit','credit','positions','orders','trading_deals'):
        if money(proof.get(key))!=0:raise ValueError('Account must never have traded and have no exposure')
    tickets=proof.get('funding_tickets')
    if not isinstance(tickets,list) or not tickets or len(tickets)!=len(set(tickets)) or not all(str(t).isdigit() for t in tickets):raise ValueError('Exact funding tickets required')
    return dict(initial(rules),unused_account_recovery=proof)

def install(app,g):
    @app.post('/collector/unused-account-journal')
    def unused_seed():
        try:verify_ingest(request)
        except ValueError:return jsonify(error='Collector authentication required'),401
        try:
            payload=request.get_json() or {};aid=str(__import__('uuid').UUID(str(payload.get('account_id'))))
            rows=g['_np_query_rows_v173']('trader_accounts',filters=[('eq','id',aid)],limit=1)
            if len(rows)!=1:raise ValueError('Exact account unavailable')
            state=checked_seed(rows[0],payload.get('proof') or {})
            from equity_drawdown import DrawdownStateStore
            store=DrawdownStateStore(g['requests'],g['SUPABASE_URL'],g.get('SUPABASE_SERVICE_ROLE_KEY') or g['SUPABASE_KEY'])
            key='equity_drawdown_v1:'+aid
            def read():
                r=store.http.get(store.url,headers=store.headers,params={'key':'eq.'+key,'select':'value','limit':1},timeout=(2,3));r.raise_for_status();return r.json()
            existing=read()
            if not existing:
                r=store.http.post(store.url,headers=store.headers,json={'key':key,'value':json.dumps(state,sort_keys=True),'updated_at':datetime.now(timezone.utc).isoformat(),'updated_by':RELEASE},timeout=(2,3))
                if r.status_code!=409:r.raise_for_status()
                existing=read()
            if len(existing)!=1:raise ValueError('Journal creation could not be confirmed')
            saved=json.loads(existing[0]['value'])
            if saved.get('rules_hash')!=state['rules_hash'] or saved.get('breached') is not False:raise ValueError('Existing journal is terminal or has conflicting rules')
            return jsonify(state=saved)
        except (ValueError,KeyError,TypeError):return jsonify(error='Unused account proof or existing journal requires review'),409
        except Exception:return jsonify(error='Unused account recovery unavailable; no existing journal was changed'),503
