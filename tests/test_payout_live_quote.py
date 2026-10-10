"""Payout financial checks without importing production background workers."""
import ast
import copy
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from flask import Flask

TREE = ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_text())

class DB:
    def __init__(self, snap):
        self.snap=snap;self.tables={'payouts':[]};self.writes=[];self.fail_rich=False;self.lose_insert_response=False
    def table(self,name):
        self.name=name;self.filters={};self.not_filters={};self.members={};self.mode='read';return self
    def select(self,*_):return self
    def eq(self,k,v):self.filters[k]=v;return self
    def neq(self,k,v):self.not_filters[k]=v;return self
    def in_(self,k,values):self.members[k]=values;return self
    def order(self,*_,**kw):return self
    def limit(self,*_):return self
    def insert(self,row):self.mode='insert';self.payload=dict(row);return self
    def update(self,row):self.mode='update';self.payload=dict(row);return self
    def execute(self):
        if self.mode=='insert':
            if self.fail_rich and 'verified_profit' in self.payload:raise RuntimeError('legacy schema')
            if any(r.get('id')==self.payload.get('id') for r in self.tables.get(self.name,[])):raise RuntimeError('duplicate primary key')
            row=dict(self.payload);self.tables.setdefault(self.name,[]).append(row)
            self.writes.append((self.name,self.mode,dict(row)))
            if self.lose_insert_response:raise TimeoutError('response lost after commit')
            self.data=[dict(row)];return self
        rows=([self.snap] if self.snap else []) if self.name=='np_live_account_state' else self.tables.get(self.name,[])
        selected=[r for r in rows if all(str(r.get(k))==str(v) for k,v in self.filters.items())
                  and all(str(r.get(k))!=str(v) for k,v in self.not_filters.items())
                  and all(r.get(k) in values for k,values in self.members.items())]
        if self.mode=='update':
            for r in selected:r.update(self.payload)
            self.writes.append((self.name,self.mode,dict(self.payload)))
        self.data=[dict(r) for r in selected];return self

class PayoutTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__)
        self.a=dict(id='exact',trader_id='owner',mt5_login='123',stage='funded',account_status='assigned_active',start_balance=1000,current_equity=1000)
        self.snap=dict(trader_account_id='exact',trader_id='owner',mt5_login='123',balance=1200,equity=1200,observed_at=datetime.now(timezone.utc).isoformat())
        self.db=DB(self.snap)
        self.env=dict(app=self.app,supabase=self.db,datetime=datetime,timezone=timezone,json=json,
                      clean=lambda v:float(v or 0),FUNDED_MAX_GROSS_PROFIT_PERCENT=50,
                      NAIRAPIPS_V195_MT5_477365592_MANAGEMENT_VERIFIED_PAYOUT='management',
                      _np_plan_payout_split_for_account=lambda *args:60,
                      _resolve_trader_for_money_action=lambda d:{'id':'owner'},
                      _payout_eligibility=lambda *args,**kw:(True,'ok',dict(self.a)),
                      now_iso=lambda:datetime.now(timezone.utc).isoformat(),
                      email_money=lambda x:str(x),request=__import__('flask').request,
                      ok=lambda data,message='':(self.app.json.response({'data':data,'message':message}),200),
                      _staff_db=lambda:self.db,send_email_safe=lambda *args:None,send_admin_alert=lambda *args:None,
                      _audit_safe=lambda *args:None,_admin_from_payload=lambda d:{},
                      bad=lambda message,status=400:(self.app.json.response({'error':message}),status))
        def read_rows(table, select="*", filters=None, limit=1):
            q = self.db.table(table).select(select)
            for kind, col, value in filters or []:
                q = q.eq(col, value)
            return q.limit(limit).execute().data
        self.env['_np_query_rows_v173'] = read_rows
        funcs=[copy.deepcopy(n) for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'_np_verified_payout_quote','create_payout','_np_v201_bootstrap','approve_payout','cancel_payout','_np_payout_awaiting_verification','_np_v122_mark_paid_route'}]
        for f in funcs: f.decorator_list=[]
        exec(compile(ast.Module(body=funcs,type_ignores=[]),'payout','exec'),self.env)
    def quote(self,snapshot=None): return self.env['_np_verified_payout_quote'](self.a,{'id':'owner'},snapshot)
    def test_lost_insert_response_does_not_duplicate_or_unlock_request(self):
        self.db.lose_insert_response=True
        self.env['_set_funded_payout_trade_lock']=lambda *args,**kwargs:None
        self.env['_release_funded_payout_trade_lock']=lambda *args,**kwargs:self.fail('Committed payout was unlocked')
        with self.app.test_request_context('/create_payout',method='POST',json={'trader_id':'owner','trader_account_id':'exact','amount':100,'bank_name':'Bank','account_number':'123','account_name':'Owner'}):
            response=self.app.make_response(self.env['create_payout']())
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(self.db.tables['payouts']),1)
    def test_live_profit_replaces_frozen_assignment_balance(self):
        self.assertEqual(self.quote()['available_payout'],120)
        self.assertEqual(self.a['current_equity'],1000)
    def test_exact_identity_and_login_must_match(self):
        for k in ('trader_account_id','trader_id','mt5_login'):
            snap=dict(self.snap);snap[k]='other'
            self.assertFalse(self.quote(snap)['verified'])
    def test_missing_stale_future_or_unparseable_observation_unavailable(self):
        for at in (None,'invalid',(datetime.now(timezone.utc)-timedelta(minutes=4)).isoformat(),(datetime.now(timezone.utc)+timedelta(minutes=2)).isoformat()):
            self.assertFalse(self.quote(dict(self.snap,observed_at=at))['verified'])
        self.db.snap=None
        self.assertFalse(self.quote()['verified'])
    def test_profit_ceiling_and_plan_split_apply(self):
        self.env['_np_plan_payout_split_for_account']=lambda *args:80
        self.assertEqual(self.quote(dict(self.snap,equity=2000))['available_payout'],400)
    def test_nonfinite_or_missing_equity_cannot_authorize_money(self):
        for equity in (float('nan'),float('inf'),None,-1):
            self.assertFalse(self.quote(dict(self.snap,equity=equity))['verified'])
    def test_verified_no_profit_is_distinct_from_unavailable(self):
        quote=self.quote(dict(self.snap,equity=900))
        self.assertTrue(quote['verified']);self.assertEqual(quote['available_payout'],0)
    def test_request_ignores_browser_equity_and_split(self):
        with self.app.test_request_context('/create_payout',method='POST',json={'amount':121,'trader_account_id':'exact','current_equity':999999,'payout_split':100}):
            response,status=self.env['create_payout']()
        self.assertEqual(status,403);self.assertIn('120',response.get_json()['error'])
    def create_delayed(self):
        self.db.snap=None
        self.env['_set_funded_payout_trade_lock']=lambda *args,**kw:None
        with self.app.test_request_context('/create_payout',method='POST',json={'amount':100,'trader_account_id':'exact','bank_name':'bank','account_name':'name','account_number':'123'}):
            return self.env['create_payout']()
    def test_missing_update_accepts_request_awaiting_verification(self):
        response,status=self.create_delayed()
        self.assertEqual(status,200)
        p=response.get_json()['data'][0]
        self.assertEqual(p['status'],'pending')
        self.assertIn('[NP_PAYOUT_VERIFY_PENDING]',p['admin_note'])
        self.assertEqual(p['verified_profit'],0)
        self.assertIn('verifying',response.get_json()['message'])
    def test_legacy_insert_keeps_verification_marker(self):
        self.db.fail_rich=True
        response,status=self.create_delayed()
        self.assertEqual(status,200)
        self.assertIn('[NP_PAYOUT_VERIFY_PENDING]',response.get_json()['data'][0]['admin_note'])
    def test_duplicate_delayed_request_is_blocked(self):
        self.create_delayed()
        response,status=self.create_delayed()
        self.assertEqual(status,409)
        self.assertEqual(len(self.db.tables['payouts']),1)
    def prepare_admin(self):
        self.create_delayed()
        self.a['account_status']='profit_protected'
        self.env.update(get_payout_by_id=lambda pid:dict(self.db.tables['payouts'][0]),
                        _get_exact_trader_account=lambda aid:dict(self.a),
                        payout_status=lambda p:p['status'],_np_v122_s=lambda v:str(v or ''))
    def approve(self):
        with self.app.test_request_context('/approve_payout',method='POST',json={'id':self.db.tables['payouts'][0]['id']}):
            return self.env['approve_payout']()
    def test_pending_request_cannot_be_approved_without_fresh_proof(self):
        self.prepare_admin();response,status=self.approve()
        self.assertEqual(status,409)
        self.assertEqual(self.db.tables['payouts'][0]['status'],'pending')
    def test_same_request_can_be_verified_and_approved_after_updates_resume(self):
        self.prepare_admin();self.db.snap=self.snap
        response,status=self.approve()
        self.assertEqual(status,200)
        p=self.db.tables['payouts'][0]
        self.assertEqual(p['status'],'approved');self.assertEqual(p['available_payout'],120)
        self.assertNotIn('[NP_PAYOUT_VERIFY_PENDING]',p['admin_note'])
        self.assertEqual(len(self.db.tables['payouts']),1)
    def test_insufficient_profit_keeps_request_pending_for_review(self):
        self.prepare_admin();self.db.snap=dict(self.snap,equity=1050)
        response,status=self.approve()
        self.assertEqual(status,409)
        self.assertEqual(self.db.tables['payouts'][0]['status'],'pending')
    def test_mark_paid_cannot_bypass_verification_even_if_status_was_changed(self):
        self.prepare_admin();self.db.tables['payouts'][0]['status']='approved'
        with self.app.test_request_context('/mark_payout_paid',method='POST',json={'id':self.db.tables['payouts'][0]['id']}):
            response,status=self.env['_np_v122_mark_paid_route']()
        self.assertEqual(status,409);self.assertIn('cannot be paid',response.get_json()['error'])
    def test_delayed_request_can_be_cancelled_and_lock_released(self):
        self.prepare_admin();released=[]
        self.a['account_status']='assigned_active'
        self.env.update(_authenticated_trader_id_for_request=lambda tid:('owner',None),
                        get_trader_by_id=lambda tid:{'id':'owner'},
                        _release_funded_payout_trade_lock=lambda *args,**kw:released.append(True),
                        _trader_safe_account_row=lambda a:a)
        with self.app.test_request_context('/cancel_payout',method='POST',json={'id':self.db.tables['payouts'][0]['id'],'trader_id':'owner'}):
            response,status=self.env['cancel_payout']()
        self.assertEqual(status,200);self.assertEqual(released,[True])
        self.assertEqual(self.db.tables['payouts'][0]['status'],'cancelled')
    def test_stale_quote_keeps_last_verified_amount_for_display_only(self):
        snap=dict(self.snap,observed_at=(datetime.now(timezone.utc)-timedelta(minutes=4)).isoformat())
        q=self.env['_np_verified_payout_quote'](self.a,{'id':'owner'},snap,allow_stale=True)
        self.assertFalse(q['verified']);self.assertTrue(q['last_verified_available'])
        self.assertEqual(q['available_payout'],120)
        self.assertFalse(self.quote(snap)['verified'])
    def test_valid_request_reaches_existing_lock_using_live_values(self):
        seen=[]
        def lock(*args,**kw): seen.append(True);raise RuntimeError('test lock stopped')
        self.env['_set_funded_payout_trade_lock']=lock
        with self.app.test_request_context('/create_payout',method='POST',json={'amount':120,'payment_method':'bank','bank_name':'bank','account_name':'name','account_number':'123'}):
            response,status=self.env['create_payout']()
        self.assertEqual(seen,[True]);self.assertEqual(status,503)
    def test_dashboard_quote_matches_request_and_caches_repeated_account(self):
        self.env['_np_v198_is_live_row']=lambda a:a['account_status']=='assigned_active'
        payload={'data':{'trader':{'id':'owner'},'current_account':dict(self.a,latest_monitoring_snapshot=self.snap),'accounts':[dict(self.a,latest_monitoring_snapshot=self.snap)]}}
        self.env['_np_v201_bootstrap_base']=lambda:self.app.json.response(payload)
        with self.app.test_request_context('/trader_bootstrap'):
            root=self.env['_np_v201_bootstrap']().get_json()['data']
        self.assertEqual(root['current_account']['payout_quote']['available_payout'],120)
        self.assertEqual(root['accounts'][0]['payout_quote'],root['current_account']['payout_quote'])

    def test_nonfinite_request_amount_is_rejected(self):
        for amount in ('NaN', 'Infinity'):
            with self.app.test_request_context('/create_payout',method='POST',json={'amount':amount}):
                response,status=self.env['create_payout']()
            self.assertEqual(status,400)
    def test_existing_management_recovery_is_exact_account_only(self):
        self.a.update(id='55d36867-3523-46af-8193-77c946d98959',mt5_login='477365592',
                      payout_financial_authority='management',current_balance=1200,current_equity=1200)
        self.db.snap=None
        self.assertEqual(self.quote()['available_payout'],120)
        self.a['id']='other'
        self.assertFalse(self.quote()['verified'])

    def test_legacy_collector_account_id_owner_alias_uses_canonical_ownership(self):
        self.db.snap=dict(self.snap,trader_id=self.a['id'])
        quote=self.quote()
        self.assertTrue(quote['verified'])
        self.assertEqual(quote['available_payout'],120)
        self.assertEqual(quote['payout_split'],60)
        self.assertFalse(self.env['_np_verified_payout_quote'](self.a,{'id':'different_owner'})['verified'])
    def test_legacy_alias_cannot_borrow_another_account_or_login(self):
        for field in ('trader_account_id','mt5_login'):
            self.db.snap=dict(self.snap,trader_id=self.a['id'])
            self.db.snap[field]='different'
            self.assertFalse(self.quote()['verified'])
    def test_missing_quote_is_unknown_instead_of_zero_profit(self):
        self.db.snap=None
        quote=self.quote()
        self.assertIsNone(quote['available_payout'])
        self.assertIsNone(quote['verified_profit'])
        self.assertIsNone(quote['payout_split'])
