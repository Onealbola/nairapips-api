import ast,unittest
from pathlib import Path
from flask import Flask,request,jsonify
from types import SimpleNamespace

class PoolCreateTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);self.existing=[];self.insertions=[];self.failure=False;self.scheduled=0
        test=self
        class Query:
            def __init__(self,table):test.assertEqual(table,'mt5_pool');self.payload=None
            def select(self,*a):return self
            def eq(self,*a):return self
            def limit(self,*a):return self
            def insert(self,payload):self.payload=payload;return self
            def execute(self):
                if test.failure:raise TimeoutError('fixture database unavailable')
                if self.payload:
                    test.insertions.append(self.payload);return SimpleNamespace(data=[{'id':'new',**self.payload}])
                return SimpleNamespace(data=test.existing)
        def schedule():self.scheduled+=1;return True
        ns=dict(app=self.app,request=request,_require_admin=lambda:({'id':'fixture'},None) if request.headers.get('Authorization')=='Bearer fixture' else (None,(jsonify(success=False),401)),_NPRenewalRestQueryV211=Query,_np_normalize_pool_class=lambda value:value,clean=float,now_iso=lambda:'2026-10-14',_np_schedule_inventory_retry_v236=schedule,ok=lambda data,message:jsonify(success=True,data=data),bad=lambda message,status=400:(jsonify(success=False,error=str(message)),status))
        n=next(n for n in ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='create_mt5')
        exec(compile(ast.Module(body=[n],type_ignores=[]),'pool-fixture','exec'),ns)
        self.client=self.app.test_client();self.payload=dict(mt5_login='123456',mt5_server='fixture',mt5_master_password='private-fixture-master',mt5_investor_password='private-fixture-view',account_size=750000,pool_class='phase')
    def send(self):return self.client.post('/create_mt5_account',json=self.payload,headers={'Authorization':'Bearer fixture'})
    def test_save_returns_exact_record_without_waiting_for_automation(self):
        r=self.send();self.assertEqual(r.status_code,200);self.assertEqual(r.json['data']['mt5'][0]['mt5_login'],'123456');self.assertEqual(r.json['data']['mt5_login'],'123456');self.assertEqual(r.json['data']['id'],'new');self.assertEqual(self.scheduled,1);self.assertEqual(len(self.insertions),1)
    def test_duplicate_auth_and_read_failures_start_no_insert(self):
        self.existing=[{'id':'old'}];self.assertEqual(self.send().status_code,409);self.assertEqual(self.insertions,[]);self.assertEqual(self.scheduled,0)
        self.assertEqual(self.client.post('/create_mt5_account',json=self.payload).status_code,401)
        self.failure=True;self.assertFalse(self.send().json['success']);self.assertEqual(self.insertions,[])

class InventoryWakeTests(unittest.TestCase):
    def test_only_one_background_wake_runs_and_lock_is_released(self):
        import threading
        started=[];retried=[]
        class Thread:
            def __init__(self,**kw):self.kw=kw
            def start(self):started.append(self.kw)
        env=dict(threading=SimpleNamespace(Thread=Thread),_NP_INVENTORY_RETRY_LOCK_V236=threading.Lock(),_np_maybe_run_verified_retry_v19=lambda trigger:retried.append(trigger))
        n=next(n for n in ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='_np_schedule_inventory_retry_v236')
        exec(compile(ast.Module(body=[n],type_ignores=[]),'inventory-wake-fixture','exec'),env)
        self.assertTrue(env['_np_schedule_inventory_retry_v236']())
        self.assertFalse(env['_np_schedule_inventory_retry_v236']())
        self.assertEqual(retried,[]);self.assertTrue(started[0]['daemon'])
        started[0]['target']();self.assertEqual(retried,['mt5_inventory_added'])
        self.assertTrue(env['_np_schedule_inventory_retry_v236']())
        started[1]['target']()
