import ast,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from limited_cycle_safety import LimitedCycleSafety,CycleBlocked
from cycle_progress import progress_report
from test_limited_cycle_safety import DB
class FinalCycleTests(unittest.TestCase):
 def setUp(self):
  self.db=DB();self.purchase=self.db.tables['challenge_purchases'][0];self.purchase['payout_cycles']=3
  self.accounts=self.db.tables['trader_accounts'];self.accounts[2]['account_status']='assigned_active'
  self.accounts[1]['archive_reason']='[NP_PAYOUT_RENEWAL_DONE_V122:second:c:333]'
  self.payouts=self.db.tables['payouts']
  for a,pid in zip(self.accounts[:2],['first','second']):self.payouts.append(dict(id=pid,trader_id='owner',trader_account_id=a['id'],mt5_login=a['mt5_login'],status='paid',paid_at='date'))
  self.guard=LimitedCycleSafety(self.db,'https://fixture','key')
 def test_final_account_shown_as_final_without_pretending_third_payout_paid(self):
  r=progress_report(self.guard,[self.purchase],self.accounts,self.payouts);p=r['purchases'][0]
  self.assertEqual((p['paid'],p['remaining'],p['completed']),(2,1,False));self.assertTrue(p['final_cycle_active']);self.assertEqual(p['final_mt5_login'],'333');self.assertTrue(r['payouts']['second']['renewal_blocked'])
 def test_after_third_payment_every_further_payment_checkpoint_is_blocked(self):
  self.payouts.append(dict(id='third',trader_id='owner',trader_account_id='c',mt5_login='333',status='paid',paid_at='date'))
  fourth=dict(self.accounts[2],id='fourth',mt5_login='444');self.accounts.append(fourth)
  for action in ['create','approve','paid']:
   with self.assertRaisesRegex(CycleBlocked,'PURCHASE COMPLETED'):self.guard.acquire(fourth,'fourth-payment',action)
  p=progress_report(self.guard,[self.purchase],self.accounts,self.payouts)['purchases'][0];self.assertTrue(p['completed']);self.assertFalse(p['final_cycle_active']);self.assertEqual(p['remaining'],0)
 def test_native_renewal_never_issues_another_account_for_delivered_or_completed_cycle(self):
  nodes=[n for n in ast.parse(Path('app.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='_np_v122_process'];node=nodes[0]
  payout=dict(self.payouts[1]);source=dict(self.accounts[1]);source.update(trader_id='owner',id='b')
  assign=Mock(side_effect=AssertionError('Fourth account must never be issued'))
  env=dict(_np_v122_s=str,_np_v122_load_payout=lambda pid:payout,payout_status=lambda p:p['status'],_np_v122_load_source=lambda p:(source,'ok'),requests=Mock(),SUPABASE_URL='fixture',SUPABASE_SERVICE_ROLE_KEY='',SUPABASE_KEY='key',_np_v122_source_block_reason=lambda *a:'',_np_v122_existing_delivery=lambda *a:self.accounts[2],_np_v122_finalize=Mock(),_np_v122_result=lambda pid,**k:k,_assign_mt5_to_trader=assign)
  exec(compile(ast.Module(body=[node],type_ignores=[]),'isolated','exec'),env)
  with patch('plan_cycles.paid_cycle_decision',return_value=dict(state='renew',paid_cycles=2,cycle_limit=3)):
   self.assertEqual(env['_np_v122_process']('second')['state'],'fulfilled')
  with patch('plan_cycles.paid_cycle_decision',return_value=dict(state='completed',paid_cycles=3,cycle_limit=3)):
   self.assertEqual(env['_np_v122_process']('second')['state'],'completed')
  assign.assert_not_called()
