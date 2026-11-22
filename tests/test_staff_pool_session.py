"""Exercise real signed staff tokens + real pool create handler without production writes."""
import ast,base64,copy,hashlib,hmac,time,unittest
from pathlib import Path
from types import SimpleNamespace
from flask import Flask,request,jsonify
from staff_access import install
from tests.test_staff_access import MemoryStore
TREE=ast.parse(Path('app.py').read_bytes())
class StaffPoolSessionTests(unittest.TestCase):
 def setUp(self):
  self.app=Flask(__name__);self.store=MemoryStore();self.store.rows[1]['permissions']={'mt5pool':'edit'};self.saved=[]
  case=self
  class Query:
   def __init__(self,table):case.assertEqual(table,'mt5_pool');self.row=None
   def select(self,*a):return self
   def eq(self,*a):return self
   def limit(self,*a):return self
   def insert(self,row):self.row=row;return self
   def execute(self):
    if self.row:case.saved.append(self.row);return SimpleNamespace(data=[dict(id='fixture-pool-row',**self.row)])
    return SimpleNamespace(data=[])
  self.ns=dict(app=self.app,request=request,jsonify=jsonify,base64=base64,hashlib=hashlib,hmac=hmac,time=time,ADMIN_AUTH_TOKEN_TTL_SECONDS=43200,_trader_auth_secret=lambda:b'isolated-token-secret',_require_admin=lambda:(None,None),_NPRenewalRestQueryV211=Query,_np_normalize_pool_class=lambda x:x,clean=float,now_iso=lambda:'fixture-time',_np_schedule_inventory_retry_v236=lambda:False,ok=lambda data,message:jsonify(success=True,data=data),bad=lambda error,status=400:(jsonify(success=False,error=str(error)),status))
  funcs=[copy.deepcopy(n) for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'_make_admin_auth_token','_admin_auth_from_token','_request_admin_auth','create_mt5'}]
  exec(compile(ast.Module(body=funcs,type_ignores=[]),'signed-staff-pool','exec'),self.ns);install(self.app,self.ns,self.store);self.client=self.app.test_client();self.token=self.ns['_make_admin_auth_token'](self.store.rows[1]);self.payload=dict(mt5_login='fixture-login',mt5_server='fixture-server',mt5_master_password='fixture-master',mt5_investor_password='fixture-investor',account_size=500000,pool_class='funded')
 def post(self,token=None):return self.client.post('/create_mt5_account',json=self.payload,headers={'Authorization':'Bearer '+(token or self.token)})
 def test_ordinary_staff_with_work_access_adds_funded_pool(self):
  r=self.post();self.assertEqual(r.status_code,200,r.json);self.assertEqual(len(self.saved),1);self.assertEqual(self.saved[0]['pool_class'],'funded');self.assertEqual(self.store.rows[1]['role'],'support')
 def test_view_only_or_revoked_access_never_inserts(self):
  for permissions in [{'mt5pool':'view'},{}]:
   self.store.rows[1]['permissions']=permissions;self.assertEqual(self.post().status_code,403);self.assertEqual(self.saved,[])
 def test_expired_session_returns_compatible_relogin_message_and_never_inserts(self):
  body='admin:1:support:'+str(int(time.time())-43201);sig=hmac.new(b'isolated-token-secret',body.encode(),hashlib.sha256).hexdigest();expired=base64.urlsafe_b64encode((body+':'+sig).encode()).decode();r=self.post(expired);self.assertEqual(r.status_code,401);self.assertIn('Admin authentication is required',r.json['error']);self.assertEqual(self.saved,[])
 def test_inactive_staff_redirects_to_login_without_inserting(self):
  self.store.rows[1]['status']='inactive';r=self.post();self.assertEqual(r.status_code,401);self.assertIn('Admin authentication is required',r.json['error']);self.assertEqual(self.saved,[])
if __name__=='__main__':unittest.main()
