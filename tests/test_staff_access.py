"""Isolated permission boundary tests: no app import, production database or workers."""
import copy,unittest
from unittest.mock import Mock
from flask import Flask,request,jsonify
from staff_access import install,permissions,StaffStore,StaffUnavailable,StaffConflict,MODULES,module_for

class MemoryStore:
 def __init__(self):self.rows={1:dict(id=1,username='member',name='Member',email='',role='support',status='active',permissions={'support':'edit'})};self.calls=[];self.unavailable=False
 def read(self,id=None):
  if self.unavailable:raise StaffUnavailable()
  return copy.deepcopy(self.rows.get(int(id))) if id is not None else copy.deepcopy(list(self.rows.values()))
 def write(self,method,payload,id=None,expected=None):
  self.calls.append((method,copy.deepcopy(payload),id,expected));i=int(id or 2)
  if expected is not None and expected!=self.rows.get(i):raise StaffConflict('Concurrent edit')
  if method=='delete':return self.rows.pop(i)
  row=self.rows.setdefault(i,dict(id=i));row.update(payload);return {k:v for k,v in row.items() if k!='password'}
class AccessTests(unittest.TestCase):
 def setUp(self):
  self.app=Flask(__name__);self.store=MemoryStore()
  self.ns={'_require_admin':lambda:None,'_request_admin_auth':lambda:({'id':'bootstrap-super-admin','role':'super_admin'} if request.headers.get('Authorization')=='owner' else {'id':'1','role':'admin'} if request.headers.get('Authorization')=='staff' else None),'_audit_safe':Mock()}
  for path,endpoint,methods in [('/staff_members','staff_members',['GET']),('/staff_members','create_staff_member',['POST']),('/staff_members/update','update_staff_member',['POST']),('/staff_members/status','staff_member_status',['POST']),('/staff_members/password','staff_member_password',['POST']),('/staff_members/delete','staff_member_delete',['POST'])]:self.app.add_url_rule(path,endpoint,lambda:jsonify(old=True),methods=methods)
  for path in ['/support_tickets','/reply_support_ticket','/delete_trader','/traders','/admin/account_state','/admin/unknown','/admin_bootstrap','/challenge_plans','/announcements','/trader_accounts','/engine_ingest']:
   self.app.add_url_rule(path,path,lambda:jsonify(success=True,traders=[{'id':'customer'}],users=[1],data=[1],accounts=[1],trader_accounts=[1],staff_members=[1],challenge_purchases=[1],counts={'traders':1,'purchases':1}),methods=['GET','POST'])
  install(self.app,self.ns,self.store);self.client=self.app.test_client()
 def call(self,path,method='GET',who='staff',**kwargs):return self.client.open(path,method=method,headers={'Authorization':who} if who else {},**kwargs)
 def test_revocation_affects_existing_token_on_next_request(self):
  self.assertEqual(self.call('/support_tickets').status_code,200);self.store.rows[1]['permissions']={};self.assertEqual(self.call('/support_tickets').status_code,403)
 def test_disabled_or_deleted_staff_cannot_use_old_token(self):
  self.store.rows[1]['status']='inactive';self.assertEqual(self.call('/admin/staff_session').status_code,401);self.store.rows.clear();self.assertEqual(self.call('/support_tickets').status_code,401)
 def test_token_role_cannot_escalate_current_database_role(self):self.assertEqual(self.call('/staff_members').status_code,403)
 def test_view_can_read_but_not_write_or_other_module(self):
  self.store.rows[1]['permissions']={'support':'view'};self.assertEqual(self.call('/support_tickets').status_code,200);self.assertEqual(self.call('/reply_support_ticket','POST').status_code,403);self.assertEqual(self.call('/admin/unknown').status_code,403)
 def test_edit_cannot_delete(self):
  self.store.rows[1]['permissions']={'traders':'edit'};self.assertEqual(self.call('/delete_trader','POST').status_code,403)
 def test_owner_can_clear_all_and_save_inactive(self):
  r=self.call('/staff_members/update','POST','owner',json={'id':1,'permissions':{},'status':'inactive','expected_permissions':{'support':'edit'},'expected_status':'active'});self.assertEqual(r.status_code,200,r.json);self.assertEqual(self.store.rows[1]['permissions'],{});self.assertEqual(self.store.rows[1]['status'],'inactive')
 def test_permission_stale_save_conflict_keeps_existing_record(self):
  r=self.call('/staff_members/update','POST','owner',json={'id':1,'permissions':{},'expected_permissions':{}});self.assertEqual(r.status_code,409);self.assertEqual(self.store.calls,[])
 def test_owner_account_protected(self):
  self.store.rows[1]['role']='super_admin';self.assertEqual(self.call('/staff_members/delete','POST','owner',json={'id':1}).status_code,403)
 def test_owner_role_or_staff_module_cannot_be_assigned(self):
  for patch in [{'role':'owner'},{'permissions':{'staff':'manage'}},{'permissions':{'unknown':'edit'}}]:self.assertEqual(self.call('/staff_members/update','POST','owner',json=dict(id=1,**patch)).status_code,400)
 def test_create_safe_response_and_new_password_alias(self):
  r=self.call('/staff_members','POST','owner',json=dict(username='new',name='New',password='fixture-password',permissions={'support':'view'}));self.assertEqual(r.status_code,200);self.assertNotIn('password',r.json['data'][0]);r=self.call('/staff_members/password','POST','owner',json=dict(id=1,new_password='changed-fixture'));self.assertEqual(r.status_code,200);self.assertEqual(self.store.rows[1]['password'],'changed-fixture')
 def test_unverified_database_fails_closed_owner_bootstrap_survives(self):
  self.store.unavailable=True;self.assertEqual(self.call('/support_tickets').status_code,503);self.assertEqual(self.call('/admin/staff_session',who='owner').status_code,200)
 def test_bootstrap_removes_every_unauthorized_alias(self):
  self.store.rows[1]['permissions']={}
  body=self.call('/admin_bootstrap').json
  for key in ['traders','data','users','accounts','trader_accounts','staff_members','challenge_purchases']:self.assertEqual(body[key],[],key)
  self.assertEqual(body['counts']['traders'],0)
 def test_shared_read_grants_never_authorize_writes(self):
  self.store.rows[1]['permissions']={'funded':'view'};self.assertEqual(self.call('/admin/account_state').status_code,200);self.assertEqual(self.call('/admin/account_state','POST').status_code,403)
 def test_no_token_cannot_bypass_admin_collections_or_mutations(self):
  for path in ['/staff_members','/admin/unknown','/traders','/reply_support_ticket','/admin_bootstrap']:self.assertEqual(self.call(path,who='').status_code,401,path)
 def test_public_and_trader_scoped_paths_keep_existing_auth(self):
  for path in ['/challenge_plans','/announcements','/trader_accounts?trader_id=fixture','/engine_ingest']:self.assertEqual(self.call(path,who='').status_code,200,path)
  self.assertEqual(self.call('/trader_accounts',who='').status_code,401)
 def test_catalog_includes_actual_sidebar_modules(self):
  self.assertTrue({'actions','league','salesOutcomes','blackBook','accountLinks','staff'}.issubset({m['id'] for m in MODULES}));self.assertEqual(self.call('/admin/staff_module_catalog').status_code,200)
 def test_create_trader_module_grants_create_without_editing_existing_traders(self):
  self.store.rows[1]['permissions']={'addTrader':'edit'};self.assertEqual(self.call('/traders','POST').status_code,200);self.assertEqual(self.call('/delete_trader','POST').status_code,403)
 def test_fresh_auth_helpers_never_return_stale_token_role(self):
  with self.app.test_request_context('/support_tickets',headers={'Authorization':'staff'}):self.assertEqual(self.ns['_request_admin_auth']()['role'],'support')
 def test_catalog_covers_every_sidebar_module(self):
  import re
  from pathlib import Path
  source=Path('website-downloads/admin_clean.html').read_text();sidebar=source[source.index('function renderApp'):source.index('function setMod')]
  ids=set(re.findall(r'data-mod="([^"]+)"',sidebar));self.assertTrue(ids.issubset({m['id'] for m in MODULES}),ids)
 def test_legacy_permission_normalization(self):
  self.assertEqual(permissions('["accounts"]'),{'accounts':'edit'});self.assertEqual(permissions({'modules':{'phaseassignment':True}}),{'phaseAssign':'edit'});self.assertNotIn('staff',permissions('all'))
class StoreTests(unittest.TestCase):
 def test_transport_safe_select_timeout_and_compare_and_swap(self):
  http=Mock();http.get.return_value.json.return_value=[{'id':1}];store=StaffStore(http,'https://fixture.invalid','fixture-key');store.read(1);self.assertNotIn('password',http.get.call_args.kwargs['params']['select']);self.assertEqual(http.get.call_args.kwargs['timeout'],(2,4))
  http.patch.return_value.json.return_value=[]
  with self.assertRaises(StaffConflict):store.write('patch',{'permissions':{}},1,expected={'permissions':{'support':'view'},'status':'active','role':'support'})
  self.assertEqual(http.patch.call_args.kwargs['params']['permissions'],'eq.{"support":"view"}')
 def test_missing_service_credentials_fail_closed(self):
  store=StaffStore(Mock(),'https://fixture.invalid','')
  with self.assertRaises(StaffUnavailable):store.read(1)
  with self.assertRaises(StaffUnavailable):store.write('post',{})
 def test_response_does_not_expose_password(self):
  http=Mock();http.post.return_value.json.return_value=[{'id':1,'password':'secret','permissions':{}}];row=StaffStore(http,'https://fixture.invalid','fixture-key').write('post',{});self.assertNotIn('password',row)
if __name__=='__main__':unittest.main()
