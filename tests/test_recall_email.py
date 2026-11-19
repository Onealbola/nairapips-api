import copy,json,unittest
from unittest.mock import Mock,patch
from datetime import datetime,timezone,timedelta
from flask import Flask
from recall_email import Notifier,render,install,PREFIX,CONTROL
AID='11111111-1111-4111-8111-111111111111';TID='22222222-2222-4222-8222-222222222222'
class MemoryNotifier(Notifier):
 def __init__(self):
  super().__init__({'requests':Mock(),'SUPABASE_URL':'https://db.invalid','SUPABASE_KEY':'fixture','BREVO_API_KEY':'test','FROM_EMAIL':'admin@example.invalid','ADMIN_ALERT_EMAIL':'admin@example.invalid','OWNER_ALERT_EMAIL':'ADMIN@example.invalid'})
  self.docs={};self.fail_claim=False;self.account=dict(id=AID,trader_id=TID,mt5_login='123456789',account_status='archived',archived_at=(datetime.now(timezone.utc)+timedelta(seconds=1)).isoformat(),archive_reason='[NP_MANAGEMENT_HOLD:ACTIVE] [NP_MH_ACCOUNT:'+AID+']')
  self.person=dict(id=TID,name='Amina <Trader>',email='trader@example.invalid')
  self.http.post.return_value=Mock(status_code=201,json=lambda:{'messageId':'provider-123'})
 def read(self,table,params):
  if table=='trader_accounts':return [copy.deepcopy(self.account)]
  if table=='traders':return [copy.deepcopy(self.person)]
  key=params.get('key','')
  if key.startswith('eq.'):return [dict(key=key[3:],value=self.docs[key[3:]])] if key[3:] in self.docs else []
  return []
 def insert(self,key,value):
  if key in self.docs:return False
  self.docs[key]=json.dumps(value,sort_keys=True);return True
 def save(self,stored,value):
  if self.fail_claim and value.get('state')=='sending':return False
  if self.docs[stored['key']]!=stored['value']:return False
  self.docs[stored['key']]=json.dumps(value,sort_keys=True);stored['value']=self.docs[stored['key']];return True
 def stored(self):return dict(key=PREFIX+AID,value=self.docs[PREFIX+AID])
class RecallEmailTests(unittest.TestCase):
 def setUp(self):self.service=MemoryNotifier()
 def enqueue(self):return self.service.enqueue(self.service.account,self.service.started_at)
 def test_wording_is_account_specific_branded_and_contains_no_passwords(self):
  subject,html,text=render('123456789','Amina <Trader>')
  self.assertIn('123456789',subject);self.assertIn('ignore the login credentials',text);self.assertIn('wait for further instructions',text)
  self.assertIn('no longer authorised for trading and must not be used again',text)
  self.assertIn('Amina &lt;Trader&gt;',html);self.assertNotIn('master_password',text)
  with self.assertRaises(ValueError):render('<script>','Trader')
 def test_one_receipt_for_each_exact_account_and_only_future_confirmed_recalls(self):
  self.assertTrue(self.enqueue());self.assertFalse(self.enqueue())
  saved=json.loads(self.service.docs[PREFIX+AID]);self.assertEqual(saved['trader_id'],TID);self.assertNotIn('password',json.dumps(saved))
  for patch in [dict(account_status='assigned_active'),dict(archived_at='2020-01-01T00:00:00Z'),dict(archive_reason='ordinary archive')]:
   s=MemoryNotifier();s.account.update(patch);self.assertFalse(s.enqueue(s.account,s.started_at))
 def test_success_and_repeated_delivery_send_only_once(self):
  self.enqueue();stored=self.service.stored();self.assertTrue(self.service.deliver(stored));self.assertFalse(self.service.deliver(stored));self.service.http.post.assert_called_once()
  payload=self.service.http.post.call_args.kwargs['json'];self.assertEqual(payload['to'],[{'email':'trader@example.invalid'}]);self.assertEqual(payload['tags'],['account_recall'])
  self.assertIn('idempotencyKey',payload['headers']);self.assertEqual(json.loads(stored['value'])['state'],'accepted')
 def test_lost_cross_worker_claim_never_sends(self):
  self.enqueue();self.service.fail_claim=True;self.assertFalse(self.service.deliver(self.service.stored()));self.service.http.post.assert_not_called()
 def test_restored_account_and_wrong_recipient_do_not_send(self):
  for field,value in [('account_status','assigned_active'),('trader_id','33333333-3333-4333-8333-333333333333')]:
   s=MemoryNotifier();s.enqueue(s.account,s.started_at);s.account[field]=value;self.assertFalse(s.deliver(s.stored()));s.http.post.assert_not_called()
 def test_recheck_after_claim_prevents_email_after_admin_return(self):
  self.enqueue();original=self.service.prepare;calls=[]
  def prepare(case):
   calls.append(1)
   if len(calls)==2:self.service.account['account_status']='assigned_active'
   return original(case)
  self.service.prepare=prepare;self.assertFalse(self.service.deliver(self.service.stored()));self.service.http.post.assert_not_called()
 def test_ambiguous_provider_failure_is_not_replayed(self):
  for failure in [TimeoutError(),Mock(status_code=503)]:
   s=MemoryNotifier();s.enqueue(s.account,s.started_at)
   if isinstance(failure,Exception):s.http.post.side_effect=failure
   else:s.http.post.return_value=failure
   stored=s.stored();self.assertFalse(s.deliver(stored));self.assertEqual(json.loads(stored['value'])['state'],'unconfirmed');self.assertFalse(s.deliver(stored));s.http.post.assert_called_once()
 def test_definitive_rate_limit_waits_before_safe_retry(self):
  self.enqueue();self.service.http.post.return_value=Mock(status_code=429);stored=self.service.stored();self.assertFalse(self.service.deliver(stored));self.assertEqual(json.loads(stored['value'])['state'],'pending');self.assertFalse(self.service.deliver(stored));self.service.http.post.assert_called_once()
 def test_activation_excludes_old_history_and_does_not_advance_past_enqueue_error(self):
  self.service.activation();self.service.account['archived_at']='2020-01-01T00:00:00Z';self.service.collect();self.assertNotIn(PREFIX+AID,self.service.docs)
  self.service=MemoryNotifier();self.service.activation();before=self.service.docs[CONTROL]
  self.service.enqueue=Mock(side_effect=RuntimeError('storage unavailable'))
  with self.assertRaises(RuntimeError):self.service.collect()
  self.assertEqual(self.service.docs[CONTROL],before)
 def test_hooks_queue_only_after_success_and_never_send_on_recall_thread(self):
  core=Mock(return_value=('marker','hold-id'));other=Mock(return_value='retired');g={'requests':Mock(),'SUPABASE_URL':'https://db.invalid','SUPABASE_KEY':'fixture','_np_mh_quarantine':core,'_np_ur_v87_retire_exact':other}
  with patch.object(Notifier,'start'):
   service=install(Flask(__name__),g)
  self.assertEqual(g['_np_mh_quarantine'](self.service.account,{},'reason'),('marker','hold-id'));self.assertEqual(service.queue.get_nowait(),(AID,TID));g['requests'].post.assert_not_called()
  other.side_effect=RuntimeError('recall failed')
  with self.assertRaises(RuntimeError):g['_np_ur_v87_retire_exact'](self.service.account)
  self.assertTrue(service.queue.empty())
 def test_universal_recall_has_same_notification_protection(self):
  self.service.account['archive_reason']='[NP_UNIVERSAL_RECALL:v] [NP_RECALL_ORIGINAL_ACCOUNT:'+AID+']'
  self.assertTrue(self.enqueue());self.assertTrue(self.service.deliver(self.service.stored()))

 def test_admin_copy_is_in_same_duplicate_protected_send_and_receipt(self):
  self.enqueue();stored=self.service.stored();self.service.deliver(stored)
  payload=self.service.http.post.call_args.kwargs['json'];self.assertEqual(payload['bcc'],[{'email':'admin@example.invalid'}])
  self.assertEqual(json.loads(stored['value'])['admin_copies'],['admin@example.invalid'])
  self.assertFalse(self.service.deliver(stored));self.service.http.post.assert_called_once()
 def test_trader_who_is_also_admin_is_not_duplicated(self):
  self.service.person['email']='ADMIN@example.invalid';self.enqueue();self.service.deliver(self.service.stored())
  self.assertNotIn('bcc',self.service.http.post.call_args.kwargs['json'])
 def test_missing_admin_copy_configuration_does_not_silently_send_without_copy(self):
  self.service.g.update(ADMIN_ALERT_EMAIL='',OWNER_ALERT_EMAIL='invalid');self.enqueue();self.assertFalse(self.service.deliver(self.service.stored()));self.service.http.post.assert_not_called()
 def test_owner_and_admin_get_separate_valid_addresses_and_audit_entries(self):
  self.service.g['OWNER_ALERT_EMAIL']='owner@example.invalid';self.service.g['_log_email_bank']=Mock();self.enqueue();self.service.deliver(self.service.stored())
  payload=self.service.http.post.call_args.kwargs['json'];self.assertEqual(payload['bcc'],[{'email':'admin@example.invalid'},{'email':'owner@example.invalid'}]);self.assertEqual(self.service.g['_log_email_bank'].call_count,3)
