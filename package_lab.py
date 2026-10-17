"""Owner-only package rule rehearsal. Never creates financial or broker records.

Prices and rule evaluation are shared with package checkout. Rehearsal counters
are temporary and are not evidence of real payment, passing or broker coverage.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from flask import jsonify, request, make_response
from package_configuration import quote, validate
from package_contracts import make_contract, initial_state, evaluate, payout_amount, dec, money
from production_controls import ControlsUnavailable


def rehearse(config, choices, actions):
    if not isinstance(actions,list) or len(actions)>100:
        raise ValueError('A test can contain up to 100 actions')
    priced=quote(config,choices)
    contract=make_contract(priced,str(uuid4()))['document']
    state=initial_state(contract,str(uuid4()),'phase1')
    usage={'phase_resets':0,'funded_resets':0,'paid_cycles':0}
    total=dec(0);complete=False;history=[]
    now=datetime.now(timezone.utc)
    for index,action in enumerate(actions):
        if complete:raise ValueError('This test journey is complete; start a new test')
        if not isinstance(action,dict):raise ValueError('Invalid test action')
        kind=action.get('kind')
        stage=state['stage']
        if kind=='observe':
            if set(action)!={'kind','balance','equity','open_positions'}:
                raise ValueError('An observation needs balance, equity and open positions')
            if state['decision']=='breached':raise ValueError('This account breached; use an available reset before testing more trades')
            observation={k:state[k] for k in ('account_id','contract_id','trader_id','rules_hash')}
            observation.update(balance=action['balance'],equity=action['equity'],open_positions=action['open_positions'],
                sequence=index,observed_at=(now+timedelta(seconds=index)).isoformat())
            state=evaluate(contract,state,observation)
            event={'action':'observe','stage':stage,'decision':state['decision'],
                   'balance':state['balance'],'equity':state['equity'],'drawdown_floor':state['drawdown_floor']}
            if state['decision']=='passed':
                next_stage=contract['stages'][contract['stages'].index(stage)+1]
                event['next_stage']=next_stage
                state=initial_state(contract,str(uuid4()),next_stage)
        elif kind=='reset':
            if set(action)!={'kind'}:raise ValueError('Reset terms come from the package; do not supply an allowance')
            if state['decision']!='breached':raise ValueError('Only a breached test account can use an included reset')
            counter='funded_resets' if stage=='funded' else 'phase_resets'
            if usage[counter]>=contract[counter]:raise ValueError('No included '+counter.replace('_',' ')+' remain')
            usage[counter]+=1
            state=initial_state(contract,str(uuid4()),stage)
            event={'action':'reset','stage':stage,'decision':'fresh_account','allowance_used':usage[counter]}
        elif kind=='payout':
            if set(action)!={'kind'}:raise ValueError('Payout amount is computed from the test account, not supplied by the browser')
            if stage!='funded' or state['decision']=='breached':raise ValueError('A non-breached funded account is required for a test payout')
            if state.get('balance') is None or dec(state['balance'])!=dec(state['equity']):
                raise ValueError('Provide a flat funded observation with closed profit before a test payout')
            if state.get('open_positions',0)!=0:raise ValueError('Close the test positions before requesting a payout')
            amount=payout_amount(contract,dec(state['balance'])-dec(contract['capital']))
            if dec(amount)<=0:raise ValueError('No positive closed profit is available for payout')
            usage['paid_cycles']+=1;total+=dec(amount)
            complete=contract['payout_cycles'] is not None and usage['paid_cycles']>=contract['payout_cycles']
            event={'action':'payout','stage':stage,'amount':amount,'paid_cycle':usage['paid_cycles'],
                   'decision':'journey_complete' if complete else 'fresh_funded_account'}
            if not complete:state=initial_state(contract,str(uuid4()),'funded')
        else:raise ValueError('Unknown test action')
        # Keep open-position evidence with the observation state. This is test
        # state only; real payout qualification still requires broker evidence.
        if kind=='observe' and event['decision']!='passed':
            state['open_positions']=action['open_positions']
        history.append(event)
    return {'test_only':True,'financial_records_created':False,'quote':priced,'contract':contract,
            'state':state,'usage':usage,'total_test_payout':money(total),'complete':complete,'history':history}


def register_package_lab(app,store,require_admin):
    def owner():
        admin,error=require_admin()
        if error is not None:return error
        if str((admin or {}).get('role') or '').lower() not in ('owner','super_admin','superadmin'):
            return jsonify(success=False,error='Owner or super admin access required'),403
        return None

    @app.get('/package-lab')
    def package_lab_page():
        response=make_response((Path(__file__).with_name('package_lab.html')).read_text())
        response.headers['Content-Type']='text/html; charset=utf-8'
        response.headers['Cache-Control']='no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Content-Security-Policy']="default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        response.headers['Referrer-Policy']='no-referrer'
        return response

    @app.post('/admin/package_lab/rehearse')
    def package_lab_rehearse():
        error=owner()
        if error is not None:return error
        try:
            body=request.get_json(silent=True)
            if not isinstance(body,dict) or set(body)-{'choices','actions','draft'} or not {'choices','actions'}<=set(body):
                raise ValueError('Supply package choices and test actions')
            if 'draft' in body:
                config=validate(body['draft'])
            else:config,_=store.read(fresh=True)
            return jsonify(success=True,data=rehearse(config,body['choices'],body['actions']))
        except ValueError as exc:return jsonify(success=False,error=str(exc)),400
        except ControlsUnavailable as exc:return jsonify(success=False,error=str(exc)),503
