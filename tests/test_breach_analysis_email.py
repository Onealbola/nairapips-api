import copy,json,unittest,uuid
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
from unittest.mock import Mock,patch
from breach_analysis_email import evidence,render,Notifier,PREFIX,CONTROL,now
from equity_drawdown import spec,initial,evaluate
from plan_risk_controls import freeze
from flask import Flask,jsonify
A=str(uuid.UUID(int=1));T=str(uuid.UUID(int=2));P=str(uuid.UUID(int=3))
class AnalysisTests(unittest.TestCase):
 def setUp(self):
  self.a=dict(id=A,trader_id=T,purchase_id=P,mt5_login='486193843',stage='phase2',account_size=2500000,start_balance=2500000,dd_limit_percent=10,drawdown_type='TRAILING',created_at='2026-11-13T15:44:47+00:00',breached_at='2026-11-13T16:13:01.377228+00:00',account_status='breached_archived')
  self.p=dict(id=P,trader_id=T,payment_status='approved',account_size=2500000,max_drawdown=10,drawdown_type='TRAILING')
  rules=spec(self.a,self.p);prior=initial(rules);prior['highest_equity']='2656814.14'
  self.state=evaluate(rules,prior,'2402036.35','2026-11-13T16:12:57.171769+00:00',balance='2509337.27')
  self.trades=[];self.events=[];self.snapshots=[]
 def read(self,table,params):
  if table=='challenge_purchases':return [copy.deepcopy(self.p)]
  if table=='package_studio_private_settings':return [dict(value=json.dumps(self.state))] if self.state else []
  if table=='trader_trades':return copy.deepcopy(self.trades)
  if table=='monitoring_events':return copy.deepcopy(self.events)
  if table=='monitoring_snapshots':return copy.deepcopy(self.snapshots)
  raise AssertionError(table)
 def test_exact_requested_trailing_breakdown_and_timing(self):
  c=evidence(self.read,self.a);subject,content,text=render(c,'Solomon')
  self.assertIn('486193843',subject)
  for value in ['₦2,656,814.14','₦250,000.00','₦2,406,814.14','₦2,402,036.35','₦2,509,337.27','₦4,777.79','4.2 seconds','16:12:57 UTC','16:13:01 UTC']:
   self.assertIn(value,text)
  self.assertIn('<table',content);self.assertIn('Floating profits raise',text)
  self.assertNotIn('2,405,933.03',text);self.assertNotIn('screenshot',text);self.assertNotIn('final balance',text.lower())
 def test_recovery_does_not_replace_original_trigger_or_unbreach(self):
  self.a.update(current_balance=2700000,current_equity=2700000)
  text=render(evidence(self.read,self.a),'Trader')[2]
  self.assertIn('₦2,402,036.35',text);self.assertIn('remains breached',text);self.assertNotIn('₦2,700,000',text)
 def test_final_balance_requires_exact_flat_broker_snapshot_after_detection(self):
  self.snapshots=[dict(trader_account_id=A,trader_id=T,balance='2405933.03',raw_data=dict(mt5_login='486193843',source='dd_police_daily_v1_shard_3_of_4',open_positions=0,observed_at='2026-11-13T16:12:58Z',balance='2405933.03',equity='2405933.03'))]
  text=render(evidence(self.read,self.a),'Trader')[2]
  self.assertIn('₦2,405,933.03',text);self.assertIn('₦881.11',text)
  for field,value in [('mt5_login','other'),('open_positions',1),('source','browser'),('observed_at','2026-11-13T15:00:00Z')]:
   original=self.snapshots[0]['raw_data'][field];self.snapshots[0]['raw_data'][field]=value
   self.assertNotIn('₦2,405,933.03',render(evidence(self.read,self.a),'Trader')[2]);self.snapshots[0]['raw_data'][field]=original
 def test_wrong_rules_missing_peak_and_false_trigger_are_not_emailed(self):
  for change in ['owner','max','hash','floor','missing']:
   test=AnalysisTests();test.setUp()
   if change=='owner':test.p['trader_id']='other'
   elif change=='max':test.p['max_drawdown']=20
   elif change=='hash':test.state['rules_hash']='foreign'
   elif change=='floor':test.state['first_breach']['floor']='2600000'
   else:test.state=None
   with self.assertRaises((ValueError,KeyError)):evidence(test.read,test.a)
 def daily(self,realised,eq,balance):
  source=freeze('snapshot',dict(daily_drawdown='5',payout_frequency='Weekly'))
  self.a.update(account_size=1500000,start_balance=1500000,dd_limit_percent=20,drawdown_type='STATIC',journey_source=source,created_at='2026-11-13T09:00:00Z')
  self.p.update(account_size=1500000,max_drawdown=20,drawdown_type='STATIC',journey_source=source)
  r=spec(self.a,self.p);self.state=evaluate(r,initial(r),eq,'2026-11-13T16:12:57.171769+00:00',balance=balance,daily_realised=realised)
 def test_genuine_daily_breach_has_its_own_allowance_and_rule(self):
  self.daily(0,'1420000','1500000')
  c=evidence(self.read,self.a);text=render(c,'Trader')[2]
  self.assertEqual(c['kind'],'daily');self.assertIn('₦75,000.00',text);self.assertIn('₦1,425,000.00',text);self.assertNotIn('highest equity',text)
 def test_false_daily_realised_omission_is_held_for_review(self):
  self.daily(0,'1484783.01','1564920.76')
  self.trades=[dict(ticket='closed-profit',profit=64920.76,closed_at='2026-11-13T10:00:00Z')]
  with self.assertRaisesRegex(ValueError,'daily input'):evidence(self.read,self.a)
 def test_legacy_static_uses_verified_event_without_inventing_observation_time(self):
  self.p['drawdown_type']='STATIC';self.a['drawdown_type']='STATIC';self.state=None
  self.events=[dict(trader_account_id=A,trader_id=T,balance=2200000,equity=2200000)]
  c=evidence(self.read,self.a);text=render(c,'Trader')[2]
  self.assertEqual(c['kind'],'maximum');self.assertIn('₦2,250,000.00',text);self.assertIn('exact broker observation time was not retained',text)
 def test_html_escapes_recipient_and_zero_equity_remains_zero(self):
  r=spec(self.a,self.p);self.state=evaluate(r,initial(r),0,'2026-11-13T16:12:57.171769+00:00',balance=2500000)
  c=evidence(self.read,self.a);content=render(c,'<img src=x onerror=evil()>')[1]
  self.assertNotIn('<img src=x',content);self.assertIn('&lt;img',content);self.assertIn('₦0.00',content)

class StoreNotifier(Notifier):
 def __init__(self,fixture):
  super().__init__({'requests':Mock(),'SUPABASE_URL':'https://db.test','SUPABASE_KEY':'key','BREVO_API_KEY':'fixture','FROM_EMAIL':'ops@example.test'})
  self.fixture=fixture;self.store={};self.accounts=[fixture.a];self.people=[dict(id=T,name='Trader',email='trader@example.test')]
  self.http.post.return_value=SimpleNamespace(status_code=201,json=lambda:{'messageId':'accepted-id'})
 def read(self,table,params):
  if table=='trader_accounts':return copy.deepcopy(self.accounts)
  if table=='traders':return copy.deepcopy(self.people)
  if table=='package_studio_private_settings' and params['key'].startswith('eq.equity_'):return self.fixture.read(table,params)
  if table=='package_studio_private_settings':
   key=params['key'];return copy.deepcopy([r for k,r in self.store.items() if k==key[3:]]) if key.startswith('eq.') else copy.deepcopy(list(self.store.values()))
  return self.fixture.read(table,params)
 def insert(self,key,value):
  if key in self.store:return False
  self.store[key]=dict(key=key,value=json.dumps(value,sort_keys=True));return True
 def save(self,stored,value):
  if self.store[stored['key']]['value']!=stored['value']:return False
  self.store[stored['key']]['value']=json.dumps(value,sort_keys=True);stored['value']=self.store[stored['key']]['value'];return True

class DeliveryTests(unittest.TestCase):
 def setUp(self):self.f=AnalysisTests();self.f.setUp();self.s=StoreNotifier(self.f)
 def case(self):return copy.deepcopy(self.s.store[PREFIX+A])
 def test_one_email_per_exact_breached_account_and_one_private_recipient(self):
  self.assertTrue(self.s.enqueue(self.f.a));self.assertFalse(self.s.enqueue(self.f.a))
  self.assertTrue(self.s.deliver(self.case()));self.assertFalse(self.s.deliver(self.case()))
  self.s.http.post.assert_called_once();body=self.s.http.post.call_args.kwargs['json'];self.assertEqual(body['to'],[{'email':'trader@example.test'}]);self.assertNotIn('cc',body);self.assertIn('idempotencyKey',body['headers']);self.assertIn('₦2,406,814.14',body['textContent'])
  self.assertEqual(json.loads(self.case()['value'])['provider_id'],'accepted-id')
 def test_lost_claim_cannot_send_duplicate(self):
  self.s.enqueue(self.f.a);old=self.case();self.s.save(self.case(),dict(json.loads(old['value']),state='sending'))
  self.assertFalse(self.s.deliver(old));self.s.http.post.assert_not_called()
 def test_uncertain_provider_acceptance_is_not_resent(self):
  self.s.enqueue(self.f.a);self.s.http.post.side_effect=TimeoutError()
  self.assertFalse(self.s.deliver(self.case()));self.assertEqual(json.loads(self.case()['value'])['state'],'unconfirmed')
  self.s.deliver(self.case());self.s.http.post.assert_called_once()
 def test_correction_or_owner_mismatch_prevents_email(self):
  self.s.enqueue(self.f.a);self.f.a['breached_at']=None
  self.s.deliver(self.case());self.s.http.post.assert_not_called()
 def test_activation_does_not_bulk_email_old_breaches(self):
  self.s.collect();self.s.http.post.assert_not_called();self.assertNotIn(PREFIX+A,self.s.store)
  control=json.loads(self.s.store[CONTROL]['value']);self.assertGreater(datetime.fromisoformat(control['activated_at']),datetime.fromisoformat(self.f.a['breached_at']))
 def test_evidence_lag_retries_reads_without_sending_an_unverified_notice(self):
  self.s.enqueue(self.f.a);self.f.state=None
  self.s.deliver(self.case());self.s.http.post.assert_not_called();case=json.loads(self.case()['value']);self.assertEqual(case['state'],'pending');self.assertEqual(case['evidence_checks'],1)
 def test_provider_quota_rejection_remains_pending(self):
  self.s.enqueue(self.f.a);self.s.http.post.return_value=SimpleNamespace(status_code=429)
  self.s.deliver(self.case());case=json.loads(self.case()['value']);self.assertEqual(case['state'],'pending');self.assertIn('retry_after',case)
 def test_status_email_hook_queues_breach_and_keeps_other_email_types(self):
  from breach_analysis_email import install
  app=Flask(__name__);app.config['NP_BREACH_EMAIL_WORKER']=False;original=Mock(return_value=True)
  g=dict(self.s.g,send_account_status_email=original,_require_admin=lambda:({'role':'admin'},None))
  with patch('breach_analysis_email.Notifier',return_value=self.s):install(app,g)
  self.assertTrue(g['send_account_status_email']({'id':T},'Phase passed','Congratulations'));original.assert_called_once()
  self.assertTrue(g['send_account_status_email']({'id':T,'mt5_login':'486193843'},'Account breached','Breach'))
  self.assertEqual(json.loads(self.case()['value'])['state'],'pending');self.s.http.post.assert_not_called()
  self.assertEqual(app.test_client().get('/admin/breach_analysis_emails?account_id=invalid').status_code,400)
 def test_admin_email_receipts_require_authentication(self):
  from breach_analysis_email import install
  app=Flask(__name__);app.config['NP_BREACH_EMAIL_WORKER']=False
  g=dict(self.s.g,_require_admin=lambda:(None,(jsonify(success=False),401)))
  with patch('breach_analysis_email.Notifier',return_value=self.s):install(app,g)
  self.assertEqual(app.test_client().get('/admin/breach_analysis_emails').status_code,401)
if __name__=='__main__':unittest.main()
