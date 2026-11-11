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
  self.purchases=[]
  self.http=Mock()
  def read(url,**kwargs):
   response=Mock();response.json.return_value=[self.a] if url.endswith('/trader_accounts') else self.purchases if url.endswith('/challenge_purchases') else [self.snap];return response
  self.http.get.side_effect=read
  self.payload={key:[copy.deepcopy(self.a)] for key in ['data','accounts','trader_accounts']}
 def decorate(self):return overlay_account_lists(self.payload,self.http,'https://db.test','key',lambda a:a['account_status']=='assigned_active')
 def test_aliases_share_verified_metrics_in_one_batch(self):
  result=self.decorate()
  self.assertEqual(self.http.get.call_count,2)
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
 def test_canonical_breach_overrides_stale_active_aliases(self):
  self.a.update(account_status='breached_archived',breached_at='2026-11-11T07:25:20Z',archive_reason='MAX_DRAWDOWN_BREACH')
  for key in ('data','accounts','trader_accounts'):
   out=self.decorate()[key][0]
   self.assertEqual(out['account_status'],'breached_archived')
   self.assertEqual(out['risk_zone'],'breached')
   self.assertTrue(out['recorded_breach'])
 def test_breached_account_removed_from_active_collection_but_retained_in_history(self):
  self.payload['active_accounts']=[copy.deepcopy(self.a)]
  self.a.update(account_status='breached_archived',breached_at='2026-11-11T07:25:20Z')
  result=self.decorate()
  self.assertEqual(result['active_accounts'],[])
  self.assertEqual(len(result['accounts']),1)
 def test_report_separates_capital_loss_and_budget_usage(self):
  self.a['start_balance']=5000000;self.a['dd_limit_percent']=20
  for rows in self.payload.values():rows[0].update(start_balance=5000000,dd_limit_percent=20)
  self.snap.update(balance=4769271,equity=4289477,dd_used_percent=71.0523)
  out=self.decorate()['accounts'][0]
  self.assertAlmostEqual(out['capital_drawdown_percent'],14.21046)
  self.assertAlmostEqual(out['loss_allowance_used_percent'],71.0523)
 def test_recovered_daily_breach_is_still_breached_below_overall_limit(self):
  self.a.update(account_status='breached_archived',breached_at='2026-11-11T07:00:00Z',archive_reason='DAILY_DRAWDOWN_BREACH')
  self.snap.update(equity=510000,balance=510000,risk_zone='safe')
  out=self.decorate()['accounts'][0]
  self.assertTrue(out['recorded_breach']);self.assertEqual(out['risk_zone'],'breached')
 def test_lost_purchased_rule_is_flagged_not_silently_reported_safe(self):
  from plan_risk_controls import freeze
  self.a.update(journey_source='post9_purchase_root_immutable',purchase_id='exact-purchase')
  self.purchases=[dict(id='exact-purchase',trader_id='owner',drawdown_type='STATIC',journey_source=freeze('purchase',dict(daily_drawdown='5',payout_frequency='Weekly')))]
  out=self.decorate()['accounts'][0]
  self.assertEqual(out['rule_snapshot_status'],'repair_required')
  self.assertEqual(out['reporting_status'],'RULE_REVIEW_REQUIRED')
  self.assertFalse(out['recorded_breach'])
  # A different purchaser cannot supply the missing rule evidence.
  self.purchases[0]['trader_id']='foreign'
  self.assertEqual(self.decorate()['accounts'][0]['rule_snapshot_status'],'unverified')
 def test_legacy_ownership_label_without_opt_in_terms_is_not_a_rule_mismatch(self):
  self.a.update(journey_source='post9_purchase_root_immutable',purchase_id='exact-purchase')
  self.purchases=[dict(id='exact-purchase',trader_id='owner',drawdown_type='STATIC',journey_source='legacy')]
  self.assertEqual(self.decorate()['accounts'][0]['rule_snapshot_status'],'verified')
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
