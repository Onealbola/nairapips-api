"""Exercise the actual approval body against the deployed schema's field names.
No production import, network, credentials, notifications or real financial writes.
"""
import ast,copy,json,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from flask import Flask,request,jsonify
ROOT=Path(__file__).resolve().parents[1]
SCHEMA=set(json.loads((ROOT/'tests/fixtures/trader_account_columns.json').read_text()))
TREE=ast.parse((ROOT/'app.py').read_text())

class Query:
    def __init__(self,db,name):self.db,self.name=db,name
    def select(self,fields):
        if self.name=='trader_accounts' and fields!='*':
            missing=set(fields.split(','))-SCHEMA
            if missing:raise RuntimeError('Database rejected nonexistent column '+','.join(sorted(missing)))
        self.db.reads.append((self.name,fields));return self
    def eq(self,*args):return self
    def limit(self,*args):return self
    def order(self,*args,**kwargs):return self
    def execute(self):
        if self.name=='trader_accounts':
            if self.db.history_error:raise TimeoutError('history unavailable')
            return SimpleNamespace(data=copy.deepcopy(self.db.history))
        return SimpleNamespace(data=[copy.deepcopy(self.db.purchase if self.name=='challenge_purchases' else self.db.pool)])
class DB:
    def __init__(self):
        self.purchase={'id':'purchase','trader_id':'owner','account_size':1000,'status':'pending','payment_status':'pending'}
        self.pool={'id':'pool','mt5_login':'private-fixture-login','status':'available','account_size':1000}
        self.history=[];self.history_error=False;self.reads=[]
    def table(self,name):return Query(self,name)

class ApprovalSchemaTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);self.db=DB();self.assign=Mock(return_value=({'id':'new-account'},{'id':'owner'}))
        self.env=dict(request=request,supabase=self.db,_np_renewal_db_v211=lambda:self.db,
            _np_purchase_is_rejected_v154=lambda p,refresh=False:p['status']=='rejected',
            _second_life_bool=bool,_np_mt5_auto_eligible=lambda *args:(True,'verified'),
            _np_assert_mt5_pool_matches_stage=lambda *args:None,clean=float,
            _admin_from_payload=lambda p:{'id':'private-test-admin'},
            _ensure_trader_for_purchase=lambda p:{'id':'owner'},_assign_mt5_to_trader=self.assign,
            _affiliate_create_commission_from_purchase=Mock(),_audit_safe=Mock(),np_invalidate_admin_bootstrap=Mock(),
            ok=lambda data,message='':jsonify(success=True,data=data,message=message),
            bad=lambda error,status=400:(jsonify(success=False,error=str(error)),status),
            _require_admin=lambda:({'id':'private-test-admin'},None))
        for name in ('approve_purchase','admin_purchase_assignment_status_v147'):
            node=copy.deepcopy(next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name==name));node.decorator_list=[]
            exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-approval-body','exec'),self.env)
    def run_approval(self):
        with self.app.test_request_context('/approve_challenge_purchase',method='POST',json={'id':'purchase','mt5_id':'pool'}):
            return self.app.make_response(self.env['approve_purchase']())
    def test_fresh_purchase_reaches_assigner_instead_of_failing_history_schema(self):
        result=self.run_approval()
        self.assertEqual(result.status_code,200,result.get_json())
        self.assign.assert_called_once()
        self.assertIn(('trader_accounts','id,stage,account_status,mt5_login'),self.db.reads)
    def test_existing_account_still_blocks_second_assignment(self):
        self.db.history=[{'id':'existing','stage':'phase1','account_status':'assigned_active','mt5_login':'used'}]
        self.assertEqual(self.run_approval().status_code,409);self.assign.assert_not_called()
    def test_history_failure_still_stops_assignment(self):
        self.db.history_error=True
        self.assertFalse(self.run_approval().get_json()['success']);self.assign.assert_not_called()
    def test_rejected_purchase_still_stops_assignment(self):
        self.db.purchase['status']='rejected'
        self.assertEqual(self.run_approval().status_code,409);self.assign.assert_not_called()
    def test_assignment_status_read_uses_supported_fields(self):
        self.db.history=[{'id':'existing','stage':'phase1','account_status':'assigned_active','mt5_login':'used'}]
        with self.app.test_request_context('/admin/purchase_assignment_status_v147?id=purchase'):
            result=self.app.make_response(self.env['admin_purchase_assignment_status_v147']())
        self.assertEqual(result.status_code,200,result.get_json())
        self.assertTrue(result.get_json()['data']['assigned'])
    def test_all_literal_trader_account_projections_match_live_schema(self):
        for node in ast.walk(TREE):
            if not isinstance(node,ast.Call) or not isinstance(node.func,ast.Attribute) or node.func.attr!='select' or not node.args:continue
            if not any(isinstance(c,ast.Constant) and c.value=='trader_accounts' for c in ast.walk(node.func.value)):continue
            try:fields=ast.literal_eval(node.args[0])
            except (ValueError,TypeError):continue
            if fields=='*' or not isinstance(fields,str):continue
            self.assertFalse(set(fields.split(','))-SCHEMA,('line',node.lineno,fields))

if __name__=='__main__':unittest.main()
