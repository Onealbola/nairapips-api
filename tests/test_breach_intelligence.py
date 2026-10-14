import ast,unittest
from pathlib import Path
from flask import Flask,request,jsonify
from breach_intelligence import build_breach_report,BreachRecordError
from breach_history import BreachReadUnavailable

class BreachEndpointTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);self.calls=[];self.failure=False
        self.tables={'trader_accounts':[{'id':'exact','trader_id':'owner','stage':'funded','account_size':1000,'dd_limit_percent':10}], 'monitoring_snapshots':[{'created_at':'2026-10-01','equity':1000,'balance':1000},{'created_at':'2026-10-02','equity':890,'balance':890,'drawdown_percent':11},{'created_at':'2026-10-03','equity':1100,'balance':1100}], 'monitoring_events':[]}
        def read(table,filters,limit=None):
            self.calls.append((table,filters))
            if self.failure:raise BreachReadUnavailable('fixture unavailable')
            return self.tables.get(table,[])
        env=dict(app=self.app,request=request,jsonify=jsonify,_require_admin=lambda:({'id':'fixture'},None) if request.headers.get('Authorization')=='Bearer fixture' else (None,(jsonify(success=False),401)),_np_breach_report_v235=build_breach_report,_NPBreachReaderV235=lambda *a:read,_NPBreachRecordErrorV235=BreachRecordError,_NPBreachUnavailableV235=BreachReadUnavailable,SUPABASE_URL='https://db.invalid',SUPABASE_KEY='fixture')
        n=next(n for n in ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='_np_breach_intelligence_v235')
        exec(compile(ast.Module(body=[n],type_ignores=[]),'isolated-main-forensic-route','exec'),env)
        self.client=self.app.test_client()
    def get(self,trader='owner',account='exact'):
        return self.client.get('/admin/breach_intelligence',query_string={'account_id':account,'trader_id':trader},headers={'Authorization':'Bearer fixture'})
    def test_exact_evidence_loaded_without_monitoring_service_and_recovery_preserves_first_breach(self):
        r=self.get();self.assertEqual(r.status_code,200)
        self.assertEqual(r.json['data']['first_crossing']['equity'],890)
        self.assertTrue(r.json['data']['recovery_after_breach']['recovered'])
        self.assertEqual(r.headers['Cache-Control'],'no-store')
    def test_missing_session_cannot_read_evidence(self):
        self.assertEqual(self.client.get('/admin/breach_intelligence?account_id=exact').status_code,401);self.assertEqual(self.calls,[])
    def test_wrong_owner_and_missing_id_do_not_read_history(self):
        self.assertEqual(self.get('other').status_code,409)
        self.assertEqual([x[0] for x in self.calls],['trader_accounts'])
        self.calls=[];self.assertEqual(self.get(account='').status_code,400);self.assertEqual(self.calls,[])
    def test_unavailable_database_is_not_legacy_or_confirmed_evidence(self):
        self.failure=True;r=self.get();self.assertEqual(r.status_code,503);self.assertFalse(r.json['success']);self.assertNotIn('data',r.json)
    def test_breach_view_has_its_own_capacity(self):
        from request_admission import RequestAdmissionMiddleware
        app=RequestAdmissionMiddleware(lambda *a:[b'ok'],8,reserved_paths=('/admin_bootstrap','/admin_operations_center','/approve_challenge_purchase','/admin/sales_outcomes','/admin/breach_intelligence'))
        held=[]
        try:
            for _ in range(3):self.assertTrue(app.slots.acquire(False));held.append(True)
            self.assertEqual(list(app({'PATH_INFO':'/admin/breach_intelligence'},lambda *a:None)),[b'ok'])
        finally:
            for _ in held:app.slots.release()

class CompleteHistoryTests(unittest.TestCase):
    def test_complete_history_reads_next_page(self):
        from unittest.mock import patch,Mock
        from breach_history import BreachReader
        pages=[Mock(json=lambda:[{'id':i} for i in range(1000)]),Mock(json=lambda:[{'id':1000}])]
        with patch('breach_history.requests.get',side_effect=pages) as get:
            rows=BreachReader('https://db.invalid','fixture')('monitoring_snapshots',{'trader_account_id':'eq.exact'})
        self.assertEqual(len(rows),1001)
        self.assertEqual(get.call_args_list[1].kwargs['params']['offset'],'1000')
    def test_failed_read_and_expired_deadline_fail_closed(self):
        from unittest.mock import patch
        from breach_history import BreachReader
        with patch('breach_history.requests.get',side_effect=TimeoutError) as get:
            with self.assertRaises(BreachReadUnavailable):BreachReader('https://db.invalid','fixture')('monitoring_events',{})
            get.reset_mock()
            with self.assertRaises(BreachReadUnavailable):BreachReader('https://db.invalid','fixture',seconds=-1)('monitoring_events',{})
            get.assert_not_called()
