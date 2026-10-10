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
    def __init__(self, snap): self.snap = snap; self.filters = {}; self.writes = 0
    def table(self, name): self.name=name; self.filters={}; return self
    def select(self, *_): return self
    def eq(self, k, v): self.filters[k]=v; return self
    def in_(self, *_): return self
    def order(self, *_, **kw): return self
    def limit(self, *_): return self
    def execute(self):
        rows = [self.snap] if self.name == 'np_live_account_state' and self.snap else []
        self.data=[r for r in rows if all(str(r.get(k))==str(v) for k,v in self.filters.items())]
        return self

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
                      bad=lambda message,status=400:(self.app.json.response({'error':message}),status))
        funcs=[copy.deepcopy(n) for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'_np_verified_payout_quote','create_payout','_np_v201_bootstrap'}]
        for f in funcs: f.decorator_list=[]
        exec(compile(ast.Module(body=funcs,type_ignores=[]),'payout','exec'),self.env)
    def quote(self,snapshot=None): return self.env['_np_verified_payout_quote'](self.a,{'id':'owner'},snapshot)
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
    def test_missing_update_returns_temporary_error_before_lock(self):
        self.db.snap=None
        self.env['_set_funded_payout_trade_lock']=lambda *args,**kw:self.fail('must not lock')
        with self.app.test_request_context('/create_payout',method='POST',json={'amount':100,'trader_account_id':'exact'}):
            response,status=self.env['create_payout']()
        self.assertEqual(status,503);self.assertIn('temporarily unavailable',response.get_json()['error'])
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
