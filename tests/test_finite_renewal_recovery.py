import unittest
from unittest.mock import Mock
from finite_renewal_recovery import finite_renewal_owed
class RecoveryTests(unittest.TestCase):
 def setUp(self):
  self.source=dict(id='a',trader_id='t',purchase_id='p',stage='funded',mt5_login='123')
  self.payout=dict(id='paid',trader_id='t',trader_account_id='a',status='paid',paid_at='date',mt5_login=123)
  self.purchase=dict(id='p',trader_id='t',payout_cycles=3,lifecycle_state='funded_waiting_mt5',trader_account_id='a')
  self.http=Mock();self.http.get.return_value.json.return_value=[self.purchase];self.http.post.return_value.json.return_value=dict(state='renew',cycle_limit=3,paid_cycles=2)
 def call(self):return finite_renewal_owed(self.source,self.payout,self.http,'https://fixture','key')
 def test_exact_waiting_purchase_recovers_even_if_trader_has_other_accounts(self):
  self.assertTrue(self.call());self.assertFalse(self.http.post.call_args.kwargs['json']['p_complete'])
 def test_completed_legacy_and_unlimited_are_not_new_entitlements(self):
  for cap in (None,0,2):
   self.purchase['payout_cycles']=cap;self.assertFalse(self.call())
 def test_different_source_owner_or_delivery_are_not_reissued(self):
  for field,value in [('trader_id','other'),('trader_account_id','other'),('status','pending')]:
   old=self.payout[field];self.payout[field]=value;self.assertFalse(self.call());self.payout[field]=old
  self.purchase['trader_account_id']='successor';self.assertFalse(self.call())
 def test_already_delivered_does_not_read_or_queue(self):
  self.source['archive_reason']='[NP_PAYOUT_RENEWAL_DONE_V122:paid:child:456]';self.assertFalse(self.call());self.http.get.assert_not_called()
 def test_failed_worker_start_does_not_leave_permanent_running_flag(self):
  import ast,threading
  from pathlib import Path
  from types import SimpleNamespace
  tree=ast.parse(Path('app.py').read_text());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v122_start')
  thread=Mock();thread.return_value.start.side_effect=RuntimeError('Resource temporarily unavailable')
  runtime={'_np_v122_s':str,'_NP_V122_LOCK':threading.Lock(),'_NP_V122_RUNNING':set(),'_np_v122_result':lambda pid,**kw:kw,'threading':SimpleNamespace(Thread=thread),'_np_v122_worker':lambda *a:None}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'isolated','exec'),runtime)
  self.assertEqual(runtime['_np_v122_start']('paid')['state'],'retry_required');self.assertEqual(runtime['_NP_V122_RUNNING'],set())
