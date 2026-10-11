"""Admin history is complete, reused per request, and fails visibly on errors."""
import ast, unittest
from pathlib import Path
from unittest.mock import Mock
from flask import Flask
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.http=Mock();self.env=dict(requests=self.http,SUPABASE_URL='https://db.example',SUPABASE_KEY='key')
        funcs=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'_np_ops_read_v208','_np_ops_tables_v208','_np_ops_accounts_v208'}]
        exec(compile(ast.Module(body=funcs,type_ignores=[]),'refresh','exec'),self.env)
        self.app=Flask(__name__)
    def test_paginated_history_not_cut_off_at_database_page_limit(self):
        first=Mock();first.json.return_value=[{'id':str(i)} for i in range(1000)]
        second=Mock();second.json.return_value=[{'id':'last'}]
        self.http.get.side_effect=[first,second]
        rows=self.env['_np_ops_read_v208']('trader_accounts',10000)
        self.assertEqual(len(rows),1001)
        self.assertEqual(self.http.get.call_args.kwargs['params']['offset'],'1000')
        self.assertEqual(self.http.get.call_args.kwargs['timeout'],(2,5))
    def test_failure_is_not_reported_as_empty_success(self):
        self.http.get.return_value.raise_for_status.side_effect=RuntimeError('unavailable')
        with self.assertRaises(RuntimeError):self.env['_np_ops_read_v208']('payouts',10000)
    def test_reconciliation_reuses_one_read_per_table_and_exact_filters(self):
        def read(name,*args):
            return [{'id':'a','purchase_id':'p','trader_id':'t'},{'id':'b','purchase_id':'q','trader_id':'t'}] if name=='trader_accounts' else []
        mocked=Mock(side_effect=read);self.env['_np_ops_read_v208']=mocked
        with self.app.test_request_context('/'):
            self.env['_np_ops_tables_v208']()
            for _ in range(10):
                self.assertEqual([r['id'] for r in self.env['_np_ops_accounts_v208']('purchase_id','p',500)],['a'])
                self.assertEqual(len(self.env['_np_ops_accounts_v208']('trader_id','t',1)),1)
            self.assertEqual(mocked.call_count,4)
        with self.app.test_request_context('/'):
            self.env['_np_ops_tables_v208']();self.assertEqual(mocked.call_count,8)
    def test_invalid_response_fails(self):
        self.http.get.return_value.json.return_value={'error':'bad'}
        with self.assertRaises(RuntimeError):self.env['_np_ops_read_v208']('payouts',10000)
