"""Exercise dashboard functions without starting database repair workers."""
import ast
import copy
import json
from pathlib import Path
import unittest
from flask import Flask

SOURCE = Path(__file__).resolve().parents[1] / 'app.py'

class Database:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []
    def table(self, name):
        self.calls.append(name)
        self.filters = {}
        return self
    def select(self, *_): return self
    def eq(self, key, value):
        self.filters[key] = value
        return self
    def limit(self, *_): return self
    def execute(self):
        self.data = [r for r in self.rows if all(str(r.get(k)) == str(v) for k, v in self.filters.items())][:1]
        return self

class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.db = Database([])
        self.scope = dict(app=self.app, supabase=self.db, clean=lambda v: float(v or 0), json=json,
                          NAIRAPIPS_V198_TRADER_BOOTSTRAP_LIVE_METRICS='live',
                          NAIRAPIPS_V200_DASHBOARD_LIVE_STATE_FINAL_BRIDGE='final')
        tree = ast.parse(SOURCE.read_text())
        names = {'_np_v198_is_live_row', '_np_v198_overlay_latest', '_np_v198_trader_bootstrap', '_np_v200_bootstrap'}
        selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == '_NP_V198_LIVE_STATUSES' for t in n.targets)]
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), 'exec'), self.scope)
    def account(self, aid='a', status='active'):
        return dict(id=aid, mt5_login='123', account_status=status, start_balance=1000, current_balance=1000)
    def snapshot(self, aid='a'):
        return dict(trader_account_id=aid, trader_id='trader', mt5_login='123', balance=1100, equity=1200)
    def test_base_table_for_each_account(self):
        self.db.rows = [self.snapshot()]
        row = self.scope['_np_v198_overlay_latest'](self.account(), 'trader')
        self.assertEqual(row['current_equity'], 1200)
        self.assertEqual(self.db.calls, ['np_live_account_state'])
    def test_no_reused_login_fallback_for_exact_account(self):
        self.db.rows = [self.snapshot('old')]
        row = self.scope['_np_v198_overlay_latest'](self.account(), 'trader')
        self.assertEqual(row['current_balance'], 1000)
        self.assertNotIn('latest_monitoring_snapshot', row)
    def test_wrappers_preserve_history_and_reuse_snapshot(self):
        self.db.rows = [self.snapshot()]
        payload = dict(data=dict(trader={'id':'trader'}, current_account=self.account(),
                                accounts=[self.account(), self.account('old', 'archived')]))
        self.scope['_np_v198_trader_bootstrap_base'] = lambda: self.app.json.response(copy.deepcopy(payload))
        self.scope['_np_v200_bootstrap_base'] = self.scope['_np_v198_trader_bootstrap']
        with self.app.test_request_context('/trader_bootstrap'):
            response = self.scope['_np_v200_bootstrap']()
            data = response.get_json()['data']
        self.assertEqual(data['trader']['current_equity'], 1200)
        self.assertEqual(data['accounts'][0]['current_equity'], 1200)
        self.assertEqual(data['accounts'][1]['current_balance'], 1000)
        self.assertNotIn('current_equity', data['accounts'][1])
        self.assertEqual(self.db.calls, ['np_live_account_state'])
    def test_database_failure_preserves_stored_metrics(self):
        def fail(_): raise TimeoutError('database unavailable')
        self.db.table = fail
        row = self.scope['_np_v198_overlay_latest'](self.account(), 'trader')
        self.assertEqual(row['current_balance'], 1000)

if __name__ == '__main__': unittest.main()
