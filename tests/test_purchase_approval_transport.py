"""Approval transport and single-purchase concurrency without live financial writes."""
import ast,copy,threading,time,unittest,uuid,re
from contextvars import ContextVar
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from flask import Flask,request,jsonify
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())

def extract(env,names):
    found={n.name:n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in names}
    funcs=[copy.deepcopy(n) for n in found.values()]
    for n in funcs:n.decorator_list=[]
    exec(compile(ast.Module(body=funcs,type_ignores=[]),'approval','exec'),env)

class Query:
    def __init__(self,db,table):self.db=db;self.name=table;self.filters={};self.notfilters={};self.payload=None
    def select(self,*a):return self
    def eq(self,k,v):self.filters[k]=v;return self
    def is_(self,k,v):self.filters[k]=None if v=='null' else v;return self
    def neq(self,k,v):self.notfilters[k]=v;return self
    def limit(self,*a):return self
    def update(self,p):self.payload=p;return self
    def execute(self):
        with self.db.lock:
            if self.name=='trader_accounts':
                if self.db.fail_history:raise TimeoutError()
                return SimpleNamespace(data=copy.deepcopy(self.db.history))
            p=self.db.purchase
            if any(p.get(k)!=v for k,v in self.filters.items()):return SimpleNamespace(data=[])
            if any(p.get(k)==v for k,v in self.notfilters.items()):return SimpleNamespace(data=[])
            if self.payload:p.update(self.payload)
            return SimpleNamespace(data=[copy.deepcopy(p)])
class DB:
    def __init__(self):
        self.lock=threading.Lock();self.history=[];self.fail_history=False
        self.purchase={'id':'purchase','status':'pending','admin_note':None,'updated_at':'old'}
    def table(self,name):return Query(self,name)

class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);self.db=DB();self.calls=[]
        self.transport=ContextVar('transport',default=False)
        self.context=ContextVar('approval',default=False)
        self.env=dict(request=request,time=time,uuid=uuid,
            _NP_RENEWAL_REST_V211=self.transport,_NP_APPROVAL_CONTEXT_V216=self.context,
            _np_renewal_db_v211=lambda:self.db,
            _require_admin=lambda:({'id':'admin'},None),
            _np_purchase_is_rejected_v154=lambda p,refresh=False:p['status']=='rejected',
            bad=lambda msg,status=400:(jsonify(success=False,error=str(msg)),status),
            _np_approve_purchase_v216_core=self.core)
        extract(self.env,{'_np_approve_purchase_v216'})
    def core(self):
        self.calls.append((self.transport.get(),self.context.get()))
        with self.db.lock:
            self.db.history=[{'id':'account'}]
            self.db.purchase.update(admin_note='Assigned MT5',trader_account_id='account')
        return jsonify(success=True)
    def run_approval(self,data=None):
        with self.app.test_request_context('/approve_challenge_purchase',method='POST',json=data or {'id':'purchase'}):
            return self.app.make_response(self.env['_np_approve_purchase_v216']())
    def test_approval_uses_isolated_transport_and_restores_context(self):
        self.assertEqual(self.run_approval().status_code,200)
        self.assertEqual(self.calls,[(True,True)])
        self.assertFalse(self.transport.get());self.assertFalse(self.context.get())
        self.assertEqual(self.db.purchase['admin_note'],'Assigned MT5')
    def test_repeated_approval_does_not_issue_again(self):
        self.run_approval();self.assertEqual(self.run_approval().status_code,409)
        self.assertEqual(len(self.calls),1)
    def test_unauthenticated_request_does_not_read_or_claim(self):
        self.env['_require_admin']=lambda:(None,('Unauthorized',401))
        self.assertEqual(self.run_approval().status_code,401)
        self.assertIsNone(self.db.purchase['admin_note']);self.assertFalse(self.calls)
    def test_rejected_purchase_cannot_be_claimed(self):
        self.db.purchase['status']='rejected'
        self.assertEqual(self.run_approval().status_code,409)
        self.assertIsNone(self.db.purchase['admin_note']);self.assertFalse(self.calls)
    def test_existing_claim_blocks_another_request(self):
        self.db.purchase['admin_note']='[NP_APPROVAL_CLAIM_V216:other]'
        self.assertEqual(self.run_approval().status_code,409);self.assertFalse(self.calls)
    def test_precommit_failure_releases_its_own_claim(self):
        self.env['_np_approve_purchase_v216_core']=Mock(side_effect=TimeoutError('secret'))
        r=self.run_approval();self.assertEqual(r.status_code,503)
        self.assertIsNone(self.db.purchase['admin_note'])
        self.assertNotIn('secret',r.get_data(as_text=True));self.assertFalse(self.transport.get())
    def test_partial_commit_never_unlocks_purchase_for_second_account(self):
        def partial():self.db.history=[{'id':'partial'}];raise TimeoutError()
        self.env['_np_approve_purchase_v216_core']=partial
        self.assertEqual(self.run_approval().status_code,503)
        self.assertIn('[NP_APPROVAL_CLAIM_V216:',self.db.purchase['admin_note'])
        self.assertEqual(self.run_approval().status_code,409)
    def test_failed_reconciliation_keeps_claim(self):
        def fail():self.db.fail_history=True;raise TimeoutError()
        self.env['_np_approve_purchase_v216_core']=fail
        self.assertEqual(self.run_approval().status_code,503)
        self.assertIn('[NP_APPROVAL_CLAIM_V216:',self.db.purchase['admin_note'])
    def test_reset_routing_keeps_previous_transport_semantics(self):
        self.db.purchase['purchase_type']='reset'
        self.assertEqual(self.run_approval().status_code,200)
        self.assertEqual(self.calls,[(False,False)])
        self.assertFalse(self.transport.get())
    def test_concurrent_requests_execute_core_only_once(self):
        entered,release=threading.Event(),threading.Event()
        def blocked():entered.set();release.wait(2);return self.core()
        self.env['_np_approve_purchase_v216_core']=blocked
        results=[]
        thread=threading.Thread(target=lambda:results.append(self.run_approval().status_code))
        thread.start();self.assertTrue(entered.wait(1))
        try:self.assertEqual(self.run_approval().status_code,409)
        finally:release.set();thread.join(2)
        self.assertEqual(results,[200]);self.assertEqual(len(self.calls),1)
    def test_out_of_stock_preserves_waiting_entitlement_without_stuck_claim(self):
        self.env.update(re=re,now_iso=lambda:'now',_affiliate_create_commission_from_purchase=Mock(),
            _audit_safe=Mock(),_admin_from_payload=lambda p:{'id':'admin'})
        extract(self.env,{'_np_mark_purchase_approved_waiting_v60'})
        def waiting():
            p=copy.deepcopy(self.db.purchase)
            result=self.env['_np_mark_purchase_approved_waiting_v60'](p,{})
            return jsonify(success=True,purchase=result,waiting_for_mt5=True)
        self.env['_np_approve_purchase_v216_core']=waiting
        r=self.run_approval()
        self.assertEqual(r.status_code,200)
        self.assertEqual(self.db.purchase['status'],'approved_waiting_mt5')
        self.assertIn('[NP_ENTITLEMENT:FIRST_PHASE1:',self.db.purchase['admin_note'])
        self.assertNotIn('[NP_APPROVAL_CLAIM_V216:',self.db.purchase['admin_note'])
        self.assertFalse(self.db.history)
    def test_post_success_wrapper_does_not_repeat_database_reads(self):
        core=Mock(return_value=SimpleNamespace(status_code=200))
        self.env.update(_np_post9_approve_purchase_core=core,supabase=Mock(),_np_fail=Mock())
        extract(self.env,{'_np_post9_approve_purchase_route'})
        with self.app.test_request_context('/approve_challenge_purchase',method='POST',json={'id':'purchase'}):
            self.assertIs(self.env['_np_post9_approve_purchase_route'](),core.return_value)
        self.env['supabase'].table.assert_not_called()
    def test_failed_history_read_stops_core_before_consuming_pool(self):
        self.db.fail_history=True
        self.env.update(_admin_from_payload=Mock(),_ensure_trader_for_purchase=Mock())
        extract(self.env,{'approve_purchase'})
        with self.app.test_request_context('/approve_challenge_purchase',method='POST',json={'id':'purchase','mt5_id':'pool'}):
            r=self.app.make_response(self.env['approve_purchase']())
        self.assertGreaterEqual(r.status_code,400)
        self.env['_ensure_trader_for_purchase'].assert_not_called()

class SafetyReadTests(unittest.TestCase):
    def test_approval_failure_does_not_retry_shared_sdk_or_accept_invalid_json(self):
        context=ContextVar('approval',default=True)
        http=Mock();response=Mock(status_code=200);response.json.return_value={'error':'bad payload'}
        http.get.return_value=response
        env=dict(requests=http,time=time,SUPABASE_URL='https://database.example',
            SUPABASE_SERVICE_ROLE_KEY='server-key',SUPABASE_KEY='',
            _NP_APPROVAL_CONTEXT_V216=context)
        extract(env,{'_np_rest_rows_v170'})
        with self.assertRaises(RuntimeError):env['_np_rest_rows_v170']('trader_accounts')
        self.assertEqual(http.get.call_count,1)

class AuditTransportTests(unittest.TestCase):
    def setUp(self):
        self.http=Mock();response=Mock(status_code=200)
        response.json.return_value=[{'id':42}];self.http.request.return_value=response
        self.env=dict(requests=self.http,_np_v184_rest_headers=lambda:('https://db.example',{}))
        cls=next(n for n in TREE.body if isinstance(n,ast.ClassDef) and n.name=='_NPRenewalRestQueryV211')
        exec(compile(ast.Module(body=[copy.deepcopy(cls)],type_ignores=[]),'audit-transport','exec'),self.env)
    def test_production_integer_audit_id_is_generated_by_database(self):
        row=self.env['_NPRenewalRestQueryV211']('admin_audit_logs').insert({'action':'approve'}).execute().data[0]
        self.assertEqual(row['id'],42)
        self.assertNotIn('id',self.http.request.call_args.kwargs['json'])
    def test_lost_audit_response_does_not_retry_or_query_unknown_id(self):
        self.http.request.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):
            self.env['_NPRenewalRestQueryV211']('admin_audit_logs').insert({'action':'approve'}).execute()
        self.http.get.assert_not_called();self.assertEqual(self.http.request.call_count,1)

class ProofTests(unittest.TestCase):
    def setUp(self):
        self.context=ContextVar('proof',default=False)
        self.pool={'id':'pool','mt5_login':'123','status':'available'}
        self.history=Mock(return_value=(False,''));self.query=Mock(side_effect=[[],[self.pool]])
        self.env=dict(time=time,clean=float,_normalize_lifecycle_stage=lambda x:x,
            _NP_APPROVAL_CONTEXT_V216=self.context,
            _np_mt5_auto_eligible_v216_core=lambda *a:(True,'AUTO READY'),
            _NP_V182_PICK_PROOF_TTL_SECONDS=30,
            _np_query_rows_v173=self.query,_mt5_login_has_any_history=self.history)
        extract(self.env,{'_np_mt5_auto_eligible','_np_v182_recent_picker_proof','_assert_mt5_never_used'})
    def test_server_proof_reuses_history_but_final_checks_still_run(self):
        self.context.set(True)
        m=dict(self.pool);self.env['_np_mt5_auto_eligible'](m,500000,'phase1')
        self.assertTrue(self.env['_assert_mt5_never_used'](m))
        self.assertEqual(self.query.call_count,2);self.history.assert_not_called()
    def test_fresh_proof_does_not_bypass_new_owner(self):
        self.context.set(True);m=dict(self.pool)
        self.env['_np_mt5_auto_eligible'](m,500000,'phase1')
        self.query.side_effect=[[{'id':'already-used'}]]
        with self.assertRaises(ValueError):self.env['_assert_mt5_never_used'](m)
    def test_expired_proof_requires_full_history(self):
        m=dict(self.pool,_np_v173_picker_verified=True,_np_v182_picker_verified_at=time.time()-31)
        self.history.return_value=(True,'Previously paid or used')
        with self.assertRaises(ValueError):self.env['_assert_mt5_never_used'](m)
        self.history.assert_called_once()
    def test_other_assignments_do_not_receive_approval_proof(self):
        m=dict(self.pool);self.env['_np_mt5_auto_eligible'](m,500000,'phase1')
        self.assertNotIn('_np_v173_picker_verified',m)
    def test_ineligible_inventory_never_gets_proof(self):
        self.context.set(True)
        self.env['_np_mt5_auto_eligible_v216_core']=lambda *a:(False,'Prior account')
        m=dict(self.pool);self.env['_np_mt5_auto_eligible'](m,500000,'phase1')
        self.assertNotIn('_np_v173_picker_verified',m)

if __name__=='__main__':unittest.main()
