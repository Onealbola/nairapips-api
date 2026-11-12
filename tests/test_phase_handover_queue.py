import copy,unittest
from unittest.mock import Mock
from phase_handover_queue import retry_waiting

class HandoverTests(unittest.TestCase):
 def setUp(self):
  self.trader=dict(id='owner',current_account_id='source',challenge_state='phase2_waiting_mt5')
  self.source=dict(id='source',trader_id='owner',purchase_id='purchase',stage='phase1',account_status='archived_phase1',passed_at='2026-11-12T00:11:21Z')
  self.purchase=dict(id='purchase',trader_id='owner',trader_account_id='source')
  self.assign=Mock(return_value={'account':{'id':'next'}})
  self.clean=Mock(return_value=True)
 def read(self,table,params):return [self.trader] if table=='traders' else [self.source] if table=='trader_accounts' else [self.purchase]
 def run_queue(self):return retry_waiting(self.read,self.assign,self.clean)
 def test_exact_recorded_phase1_pass_uses_existing_phase2_authority(self):
  result=self.run_queue();self.assertEqual(result['assigned'],1)
  self.assign.assert_called_once_with(self.trader,'phase2',self.purchase,self.source,'lifecycle_progression')
 def test_terminal_foreign_wrong_stage_and_package_sources_never_enter_assignment(self):
  original=copy.deepcopy(self.source)
  for patch in ({'breached_at':'time'},{'account_status':'breached_archived'},{'passed_at':None},{'trader_id':'foreign'},{'id':'foreign'},{'stage':'funded'},{'package_contract_id':'separate-authority'}):
   self.source={**original,**patch};self.assign.reset_mock();self.run_queue();self.assign.assert_not_called()
 def test_purchase_pointer_changed_foreign_owner_and_legacy_are_not_reassigned(self):
  for field in ('id','trader_id','trader_account_id'):
   old=self.purchase[field];self.purchase[field]='other';self.run_queue();self.assign.assert_not_called();self.purchase[field]=old
  self.clean.return_value=False;self.run_queue();self.assign.assert_not_called()
 def test_existing_successor_response_remains_consumed(self):
  self.assign.return_value={'already_fulfilled':True,'account':{'id':'existing'}}
  result=self.run_queue();self.assertEqual(result['assigned'],0);self.assertEqual(result['already_fulfilled'],1)
 def test_waiting_inventory_does_not_make_or_fake_an_account(self):
  self.assign.return_value=None;result=self.run_queue();self.assertEqual(result['waiting'],1);self.assertEqual(result['assigned'],0)
 def test_phase2_pass_uses_funded_pool_via_existing_authority(self):
  self.trader['challenge_state']='funded_waiting_mt5';self.source.update(stage='phase2',account_status='archived_phase2')
  self.run_queue();self.assertEqual(self.assign.call_args.args[1],'funded')
 def test_independent_item_failure_is_counted(self):
  self.assign.side_effect=RuntimeError('verification unavailable');result=self.run_queue();self.assertEqual(result['errors'],1);self.assertEqual(result['assigned'],0)
if __name__=='__main__':unittest.main()
