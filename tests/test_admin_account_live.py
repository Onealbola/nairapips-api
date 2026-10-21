import ast,copy,json,unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import Mock
from flask import Flask
from admin_account_live import overlay_account_lists

class AdminAccountLiveTests(unittest.TestCase):
 def setUp(self):
  self.a={'id':'11111111-1111-4111-8111-111111111111','trader_id':'owner','mt5_login':'123','account_status':'assigned_active','stage':'funded','start_balance':500000,'current_equity':500000}
  self.snap={'trader_account_id':self.a['id'],'trader_id':'owner','mt5_login':'123','balance':512000,'equity':510000,'observed_at':datetime.now(timezone.utc).isoformat(),'dd_used_percent':4}
  self.http=Mock();self.http.get.return_value.json.side_effect=lambda:[self.snap]
  self.payload={key:[copy.deepcopy(self.a)] for key in ['data','accounts','trader_accounts']}
 def decorate(self):return overlay_account_lists(self.payload,self.http,'https://db.test','key',lambda a:a['account_status']=='assigned_active')
 def test_aliases_share_verified_metrics_in_one_batch(self):
  result=self.decorate()
  self.assertEqual(self.http.get.call_count,1)
  for key in self.payload:
   if key=='live_data_read_failed':continue
   self.assertEqual(result[key][0]['current_equity'],510000)
   self.assertEqual(result[key][0]['profit'],10000)
   self.assertEqual(result[key][0]['live_data_status'],'live')
 def test_zero_equity_is_not_replaced_with_starting_capital(self):
  self.snap['balance']=self.snap['equity']=0
  a=self.decorate()['accounts'][0];self.assertEqual(a['current_equity'],0);self.assertEqual(a['profit'],-500000)
 def test_foreign_owner_wrong_login_and_missing_are_unverified(self):
  for field,value in [('trader_id','foreign'),('mt5_login','different')]:
   original=self.snap[field];self.snap[field]=value
   self.assertEqual(self.decorate()['accounts'][0]['live_data_status'],'missing');self.snap[field]=original
 def test_stale_and_failed_feed_are_visible(self):
  self.snap['observed_at']='2026-01-01T00:00:00+00:00'
  self.assertEqual(self.decorate()['accounts'][0]['live_data_status'],'stale')
  self.http.get.side_effect=RuntimeError('offline')
  self.assertEqual(self.decorate()['accounts'][0]['live_data_status'],'unavailable')
 def test_actual_response_wrapper_updates_browser_alias(self):
  tree=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes())
  node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v204_live_refresh_response')
  app=Flask(__name__);env={'app':app,'json':json,'requests':self.http,'SUPABASE_URL':'https://db.test','SUPABASE_KEY':'key','SUPABASE_SERVICE_ROLE_KEY':'private','_np_v198_is_live_row':lambda a:True}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'real-admin-wrapper','exec'),env)
  with app.app_context():
   response=env['_np_v204_live_refresh_response'](app.json.response(self.payload))
   self.assertEqual(response.json['accounts'][0]['current_equity'],510000)
   self.assertEqual(response.headers['Cache-Control'],'no-store')
   self.assertEqual(self.http.get.call_args.kwargs['headers']['apikey'],'private')
if __name__=='__main__':unittest.main()

class CompleteFleetTests(unittest.TestCase):
 def fixture(self,authorized=True,fail_second=False):
  from admin_account_live import register_admin_account_state
  app=Flask(__name__);http=Mock();calls=[]
  def get(url,**kwargs):
   table=url.rsplit('/',1)[-1];params=kwargs['params'];calls.append((table,params))
   if table=='trader_accounts':
    if params['offset']=='0':rows=[{'id':str(i),'trader_id':'t','stage':'funded','account_status':'archived','mt5_password':'must-not-leak'} for i in range(1000)]
    elif fail_second:raise RuntimeError('database failed')
    else:rows=[{'id':'last','trader_id':'t','stage':'funded','account_status':'archived'}]
   elif table=='traders':rows=[{'id':'t','name':'Trader'}]
   else:rows=[]
   response=Mock();response.json.return_value=rows;return response
  http.get.side_effect=get
  def require():return ({'role':'admin'},None) if authorized else (None,(app.json.response({'error':'Admin authentication is required'}),401))
  register_admin_account_state(app,http,'https://db.test','private',require,lambda r:False)
  return app.test_client(),calls
 def test_complete_fleet_pages_names_and_secret_redaction(self):
  client,calls=self.fixture();response=client.get('/admin/account_state')
  self.assertEqual(response.status_code,200);self.assertEqual(len(response.json['accounts']),1001)
  self.assertTrue(response.json['complete']);self.assertEqual(response.json['accounts'][-1]['trader_name'],'Trader')
  self.assertNotIn('must-not-leak',response.get_data(as_text=True))
  self.assertEqual([p['offset'] for t,p in calls if t=='trader_accounts'],['0','1000'])
 def test_failure_never_presents_partial_totals(self):
  client,_=self.fixture(fail_second=True);response=client.get('/admin/account_state')
  self.assertEqual(response.status_code,503);self.assertNotIn('accounts',response.json)
 def test_authentication_before_database(self):
  client,calls=self.fixture(authorized=False)
  self.assertEqual(client.get('/admin/account_state').status_code,401);self.assertEqual(calls,[])
