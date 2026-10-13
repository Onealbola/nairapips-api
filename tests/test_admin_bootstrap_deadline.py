"""A failing overview must not occupy a worker for repeated full refreshes."""
import ast, copy, threading, time, unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock
from flask import Flask, request, jsonify

TREE = ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_text())
NAMES = {'admin_bootstrap', '_build_admin_bootstrap_payload',
    '_np_admin_interactive_rows_v215', '_np_admin_registered_count_v215',
    '_np_admin_bootstrap_tables_v215', '_np_exact_registered_trader_count_v134',
    '_np_admin_paged_rows_v215'}

class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.http = Mock()
        response = Mock(headers={'Content-Range': '0-0/487'})
        response.json.return_value = [{'id': 'one'}]
        self.http.get.return_value = response
        self.env = dict(request=request, jsonify=jsonify, requests=self.http,
            time=time, threading=threading, ThreadPoolExecutor=ThreadPoolExecutor,
            SUPABASE_URL='https://database.example', SUPABASE_KEY='test-key',
            _NP_TRADER_COUNT_CACHE_V134={}, _ADMIN_BOOTSTRAP_CACHE={},
            _NP_ADMIN_BOOTSTRAP_EXECUTOR_V215=None,
            _NP_ADMIN_BOOTSTRAP_EXECUTOR_LOCK_V215=threading.Lock(),
            _require_admin=lambda: ({'id': 'admin'}, None), _np_ok=jsonify,
            np_cached_read=lambda key, ttl, fn: fn(),
            _quick_available_mt5=lambda rows: rows, now_iso=lambda: 'test-time')
        funcs = [copy.deepcopy(n) for n in TREE.body
                 if isinstance(n, ast.FunctionDef) and n.name in NAMES]
        for n in funcs: n.decorator_list = []
        exec(compile(ast.Module(body=funcs, type_ignores=[]), 'bootstrap', 'exec'), self.env)
    def tearDown(self):
        pool = self.env['_NP_ADMIN_BOOTSTRAP_EXECUTOR_V215']
        if pool: pool.shutdown(wait=True)
    def test_independent_reads_overlap_and_aliases_remain(self):
        def slow(*args, **kwargs):
            time.sleep(.06)
            return self.http.get.return_value
        self.http.get.side_effect = slow
        started = time.monotonic()
        out = self.env['_build_admin_bootstrap_payload']({})
        self.assertLess(time.monotonic()-started, .4) # Sequential nine reads take .54s.
        self.assertEqual(out['counts']['traders'], 487)
        self.assertEqual(out['accounts'], out['trader_accounts'])
        self.assertEqual(out['payouts'], out['all_payouts'])
        self.assertEqual(self.http.get.call_count, 9)
    def test_database_failure_is_not_zero_business_or_sdk_fallback(self):
        self.http.get.side_effect = TimeoutError('private details')
        with self.app.test_request_context('/admin_bootstrap'):
            out = self.app.make_response(self.env['admin_bootstrap']())
        self.assertEqual(out.status_code, 503)
        self.assertNotIn('traders', out.get_json())
        self.assertNotIn('private details', out.get_data(as_text=True))
        self.assertLessEqual(self.http.get.call_count, 9)
    def test_recent_good_data_survives_a_refresh_failure(self):
        saved = {'success': True, 'traders': [{'id': 'real'}], 'counts': {'traders': 487}}
        self.env['_ADMIN_BOOTSTRAP_CACHE'].update(ts=time.time(), payload=saved)
        self.http.get.side_effect = TimeoutError()
        out = self.env['_build_admin_bootstrap_payload']({})
        self.assertEqual(out['traders'], saved['traders'])
        self.assertTrue(out['degraded'])
        self.assertNotIn('degraded', saved)
    def test_expired_data_not_served_as_current(self):
        self.env['_ADMIN_BOOTSTRAP_CACHE'].update(ts=time.time()-301, payload={'success': True})
        self.http.get.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError): self.env['_build_admin_bootstrap_payload']({})
    def test_optional_history_failure_does_not_discard_core_business_data(self):
        def rows(table, *args):
            if table == 'monitoring_events': raise TimeoutError()
            return [{'id': 'real'}]
        self.env['_np_admin_interactive_rows_v215'] = rows
        out = self.env['_build_admin_bootstrap_payload']({})
        self.assertEqual(out['traders'], [{'id': 'real'}])
        self.assertEqual(out['counts']['traders'], 487)
        self.assertTrue(out['degraded'])
        self.assertEqual(out['monitoring_events'], [])
        self.assertIn('monitoring_events', out['warnings'][0])
    def test_count_requires_exact_header_and_empty_count_is_valid(self):
        self.http.get.return_value.headers = {'Content-Range': '*/0'}
        self.assertEqual(self.env['_np_admin_registered_count_v215'](), 0)
        self.http.get.return_value.headers = {'Content-Range': '0-0/*'}
        with self.assertRaises(RuntimeError): self.env['_np_admin_registered_count_v215']()
    def test_deadline_cancels_queued_work(self):
        from unittest.mock import patch
        futures = [Mock() for _ in range(9)]
        pool = Mock(); pool.submit.side_effect = futures
        self.env['_NP_ADMIN_BOOTSTRAP_EXECUTOR_V215'] = pool
        with patch('concurrent.futures.wait', return_value=(set(), {futures[0]})) as waiter:
            with self.assertRaises(TimeoutError): self.env['_np_admin_bootstrap_tables_v215']()
        self.assertEqual(waiter.call_args.kwargs['timeout'], 10)
        for future in futures: future.cancel.assert_called_once()
        self.env['_NP_ADMIN_BOOTSTRAP_EXECUTOR_V215'] = None
    def test_trader_list_keeps_pages_beyond_database_default_cap(self):
        self.http.get.return_value.json.side_effect = [[{'id': str(i)} for i in range(1000)], [{'id': 'last'}]]
        out = self.env['_np_admin_paged_rows_v215']('trader_accounts', 'updated_at', time.monotonic()+10)
        self.assertEqual(len(out), 1001)
        self.assertEqual(self.http.get.call_args.kwargs['params']['offset'], '1000')
        self.assertEqual(self.http.get.call_args.kwargs['params']['order'], 'updated_at.desc,id.desc')
    def test_trader_list_timeout_does_not_return_partial_success(self):
        self.http.get.return_value.json.side_effect = [[{'id': str(i)} for i in range(1000)], TimeoutError()]
        with self.assertRaises(TimeoutError):
            self.env['_np_admin_paged_rows_v215']('traders', 'created_at', time.monotonic()+10)
    def test_authentication_failure_never_reads_private_tables(self):
        self.env['_require_admin'] = lambda: (None, ('Unauthorized', 401))
        with self.app.test_request_context('/admin_bootstrap'):
            out = self.app.make_response(self.env['admin_bootstrap']())
        self.assertEqual(out.status_code, 401)
        self.http.get.assert_not_called()

if __name__ == '__main__': unittest.main()
