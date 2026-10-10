"""History responses must not erase the newest exact live observation."""
import ast
import copy
from pathlib import Path
import unittest
from flask import Flask, request

TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class TruthTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__)
        self.a=dict(id='account',trader_id='owner',mt5_login='123',stage='funded',start_balance=500000,current_equity=500000,lowest_equity=500000,highest_equity=500000,last_sync_at='2026-10-09T05:00:00+00:00')
        self.history=[dict(equity=500000,created_at=self.a['last_sync_at'])]
        self.live=dict(trader_account_id='account',trader_id='account',mt5_login='123',equity=570981.44,observed_at='2026-10-10T17:30:00+00:00')
        self.env=dict(request=request,_request_admin_auth=lambda:None,
            _authenticated_trader_id_for_request=lambda _:('owner',None),
            _np_ht_exact_account_v122=lambda *args:self.a,
            _np_ht_event_rows_v122=lambda *args:[],
            _np_ht_snapshot_rows_v122=lambda *args:copy.deepcopy(self.history),
            _np_query_rows_v173=lambda *args,**kwargs:[dict(self.live)],
            _np_ok=lambda d:self.app.json.response(d),
            _np_fail=lambda message,status:(self.app.json.response({'error':message}),status),
            NAIRAPIPS_HISTORIC_TRUTH_RELEASE_V122='test')
        funcs=[copy.deepcopy(n) for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'account_truth_v122_compat','_np_ht_num_v122','_np_ht_equity_v122'}]
        for n in funcs:n.decorator_list=[]
        exec(compile(ast.Module(body=funcs,type_ignores=[]),'truth','exec'),self.env)
    def truth(self):
        with self.app.test_request_context('/account_truth_v116?account_id=account&trader_id=owner'):
            return self.env['account_truth_v122_compat']().get_json()['truth']
    def test_newest_live_high_and_time_survive_history_response(self):
        t=self.truth()
        self.assertEqual(t['highest_equity'],570981.44)
        self.assertEqual(t['current_equity'],570981.44)
        self.assertEqual(t['highest_equity_at'],self.live['observed_at'])
        self.assertEqual(t['last_monitored_at'],self.live['observed_at'])
        self.assertEqual(t['lowest_equity'],500000)
    def test_recovery_keeps_historical_breach(self):
        self.history.append(dict(equity=350000,created_at='2026-10-09T06:00:00+00:00'))
        t=self.truth()
        self.assertEqual(t['lowest_equity'],350000)
        self.assertTrue(t['ever_crossed_dd_limit'])
        self.assertEqual(t['max_historic_dd_percent'],30)
    def test_other_account_owner_or_login_cannot_change_truth(self):
        for key in ('trader_account_id','trader_id','mt5_login'):
            saved=self.live[key];self.live[key]='other'
            self.assertEqual(self.truth()['highest_equity'],500000)
            self.live[key]=saved
