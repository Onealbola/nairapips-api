"""Cross-worker compare-and-set guards, authenticated actions and paid terminality."""
import ast,copy,threading,unittest
from pathlib import Path
from flask import Flask,request
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class Store:
    def __init__(self):
        self.lock=threading.Lock();self.accounts=[dict(id='account',trader_id='owner',archive_reason=None,mt5_login='123',mt5_server='Broker1',created_at='2026-01-01')];self.payouts=[]
    def table(self,name):return Query(self,name)
    def rows(self,name,filters):
        with self.lock:
            return [copy.deepcopy(r) for r in getattr(self,'accounts' if name=='trader_accounts' else 'payouts') if all((str(r.get(k)) in [str(x) for x in v]) if kind=='in' else str(r.get(k))==str(v) for kind,k,v in filters)]
class Query:
    def __init__(self,store,name):self.store=store;self.name=name;self.filters=[];self.payload={}
    def update(self,payload):self.payload=payload;return self
    def eq(self,k,v):self.filters.append((k,v));return self
    def is_(self,k,v):self.filters.append((k,None));return self
    def execute(self):
        with self.store.lock:
            rows=getattr(self.store,'accounts' if self.name=='trader_accounts' else 'payouts')
            selected=[r for r in rows if all(r.get(k)==v for k,v in self.filters)]
            for r in selected:r.update(self.payload)
            return type('Result',(),{'data':copy.deepcopy(selected)})()
class GuardTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);self.db=Store();self.auth=('owner',None);self.admin=True
        self.env=dict(request=request,_staff_db=lambda:self.db,_np_query_rows_v173=lambda table,filters=None,**kw:self.db.rows(table,filters or []),
            _authenticated_trader_id_for_request=lambda _:self.auth,
            _require_admin=lambda:({},None) if self.admin else (None,(self.app.json.response({'error':'auth'}),401)),
            get_trader_by_id=lambda tid:{'id':tid},_payout_eligibility=lambda *args,**kw:(True,'ok',copy.deepcopy(self.db.accounts[0])),
            payout_status=lambda r:str(r.get('status') or 'pending').lower(),bad=lambda msg,status=400:(self.app.json.response({'error':msg}),status))
        names={'_np_v206_claim_payout_action','_np_v206_release_payout_action','_np_v206_payout_guard','_np_v206_broker_scope','_np_v206_broker_ledger'}
        exec(compile(ast.Module(body=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),'guard','exec'),self.env)
    def invoke(self,handler,payload):
        with self.app.test_request_context('/action',method='POST',json=payload):
            return self.app.make_response(handler()).status_code
    def handler(self,action,base):return self.env['_np_v206_payout_guard'](base,action)
    def test_only_one_concurrent_claim_in_database(self):
        barrier=threading.Barrier(3);results=[]
        def take():
            a=copy.deepcopy(self.db.accounts[0]);barrier.wait();results.append(self.env['_np_v206_claim_payout_action'](a))
        ts=[threading.Thread(target=take) for _ in range(2)]
        for t in ts:t.start()
        barrier.wait()
        for t in ts:t.join()
        self.assertEqual(sum(bool(x) for x in results),1)
    def test_concurrent_creates_and_retry_produce_one_request(self):
        entered=threading.Event();finish=threading.Event();calls=[]
        def base():
            calls.append(1);entered.set();finish.wait(2)
            self.db.payouts.append(dict(id='new',trader_id='owner',trader_account_id='account',status='pending'))
            return self.app.json.response({'success':True})
        h=self.handler('create',base);first=[]
        t=threading.Thread(target=lambda:first.append(self.invoke(h,{'trader_id':'owner'})));t.start();self.assertTrue(entered.wait(2))
        self.assertEqual(self.invoke(h,{'trader_id':'owner'}),409)
        finish.set();t.join()
        self.assertEqual(first,[200]);self.assertEqual(self.invoke(h,{'trader_id':'owner','id':'new'}),409)
        self.assertEqual(calls,[1]);self.assertIsNone(self.db.accounts[0]['archive_reason'])
    def test_paid_account_blocks_new_request_approval_and_payment(self):
        self.db.payouts=[dict(id='old',trader_account_id='account',trader_id='owner',status='paid',paid_at='today'),dict(id='new',trader_account_id='account',trader_id='owner',status='approved')]
        for action in ('create','approve','paid'):
            h=self.handler(action,lambda:self.fail('Paid account reached mutation'))
            self.assertEqual(self.invoke(h,{'trader_id':'owner','id':'new'}),409)
        self.db.payouts[0]['status']='cancelled'
        self.assertEqual(self.invoke(self.handler('create',lambda:self.fail('Payment evidence erased')),{'trader_id':'owner'}),409)
    def test_repeat_payment_has_one_completion(self):
        row=dict(id='request',trader_account_id='account',trader_id='owner',status='approved');self.db.payouts=[row];calls=[]
        def base():
            calls.append(1);row.update(status='paid',paid_at='today');return self.app.json.response({'success':True})
        h=self.handler('paid',base)
        self.assertEqual(self.invoke(h,{'id':'request'}),200)
        self.assertEqual(self.invoke(h,{'id':'request'}),409);self.assertEqual(calls,[1])
    def test_authentication_required_for_both_money_roles(self):
        self.auth=('', 'Trader authentication required')
        self.assertEqual(self.invoke(self.handler('create',lambda:self.fail()),{}),401)
        self.admin=False
        self.assertEqual(self.invoke(self.handler('paid',lambda:self.fail()),{}),401)
    def test_paid_broker_login_blocks_duplicate_internal_account(self):
        self.db.accounts.append(dict(id='old',trader_id='owner',archive_reason=None,mt5_login='123',mt5_server='Broker1',created_at='2025-01-01'))
        self.db.payouts=[dict(id='paid',trader_id='owner',trader_account_id='old',status='paid',paid_at='yesterday')]
        self.assertEqual(self.invoke(self.handler('create',lambda:self.fail('Clone bypassed paid guard')),{'trader_id':'owner'}),409)
    def test_same_login_on_different_broker_is_independent(self):
        self.db.accounts.append(dict(id='other',trader_id='owner',archive_reason=None,mt5_login='123',mt5_server='Broker2',created_at='2025-01-01'))
        self.db.payouts=[dict(id='paid',trader_id='owner',trader_account_id='other',status='paid',paid_at='yesterday')]
        self.assertEqual(self.invoke(self.handler('create',lambda:self.app.json.response({'success':True})),{'trader_id':'owner'}),200)
    def test_duplicate_records_share_the_database_action_guard(self):
        self.db.accounts.append(dict(id='clone',trader_id='owner',archive_reason=None,mt5_login='123',mt5_server='Broker1',created_at='2026-02-01'))
        first,ids=self.env['_np_v206_broker_scope'](self.db.accounts[0])
        second,ids=self.env['_np_v206_broker_scope'](self.db.accounts[1])
        self.assertEqual(first['id'],second['id'])
    def test_release_preserves_renewal_audit_and_other_writes(self):
        a=copy.deepcopy(self.db.accounts[0]);claim=self.env['_np_v206_claim_payout_action'](a)
        self.db.accounts[0]['archive_reason']+=' [paid:request]'
        self.env['_np_v206_release_payout_action'](a,claim)
        self.assertEqual(self.db.accounts[0]['archive_reason'],'[paid:request]')
