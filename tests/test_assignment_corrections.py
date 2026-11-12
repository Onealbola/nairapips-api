import copy,json,unittest
from unittest.mock import Mock
from assignment_corrections import marker,voided,trader_view,retry_pending,PREFIX
PID='11111111-1111-4111-8111-111111111111';OWNER='22222222-2222-4222-8222-222222222222'
class CorrectionTests(unittest.TestCase):
 def setUp(self):
  self.old=dict(id='old',purchase_id=PID,trader_id=OWNER,mt5_login='wrong',account_status='archived',archive_reason=marker(PID,OWNER))
  self.case=dict(purchase_id=PID,trader_id=OWNER,target_stage='phase1',authorized_by='owner_instruction',state='waiting_inventory',account_size='5000000',voided_account_ids=['old'])
  self.saved=Mock();self.pick=Mock(return_value={'mt5_login':'fresh'});self.assign=Mock(return_value=({'id':'fresh-account','mt5_login':'fresh'},{}));self.chain=[self.old]
 def read(self,t,p):
  if t=='package_studio_private_settings':return [dict(key=PREFIX+PID,value=json.dumps(self.case))]
  if t=='challenge_purchases':return [dict(id=PID,trader_id=OWNER,account_size=5000000,payment_status='approved',status='approved_waiting_mt5')]
  if t=='traders':return [dict(id=OWNER)]
  return copy.deepcopy(self.chain)
 def run_case(self):return retry_pending(self.read,self.saved,self.pick,self.assign)
 def test_marker_binds_exact_purchase_and_owner_and_does_not_hide_ordinary_history(self):
  self.assertTrue(voided(self.old))
  for patch in ({'purchase_id':'other'},{'trader_id':'other'},{'account_status':'assigned_active'},{'archive_reason':'ordinary archive'}):self.assertFalse(voided({**self.old,**patch}))
 def test_trader_history_excludes_only_voided_company_errors(self):
  regular=dict(id='valid-history',account_status='archived')
  r=trader_view({'all_accounts':[self.old,regular],'current_account':self.old})
  self.assertEqual(r['all_accounts'],[regular]);self.assertIsNone(r['current_account']);self.assertEqual(r['correction_excluded_account_ids'],['old'])
 def test_authorized_case_assigns_fresh_phase1_without_new_purchase(self):
  result=self.run_case();self.assertEqual(result['assigned'],1);self.pick.assert_called_once_with(5000000.0,'phase1');self.assertEqual(self.assign.call_args.args[2],'phase1');self.assertEqual(self.assign.call_args.args[3]['id'],PID);self.assertEqual(self.saved.call_args.args[1]['replacement_account_id'],'fresh-account')
 def test_missing_inventory_never_uses_a_wrong_vault_or_fake_account(self):
  self.pick.return_value=None;r=self.run_case();self.assertEqual(r['waiting_inventory'],1);self.assign.assert_not_called();self.saved.assert_not_called()
 def test_missing_void_marker_or_changed_capital_blocks(self):
  self.old['archive_reason']='ordinary archive';r=self.run_case();self.assertEqual(r['errors'],1);self.assign.assert_not_called()
  self.old['archive_reason']=marker(PID,OWNER);self.case['account_size']='50000000';self.run_case();self.assign.assert_not_called()
 def test_changed_purchase_payment_or_current_pointer_blocks(self):
  original=self.read
  for patch in ({'payment_status':'pending'},{'status':'rejected'},{'trader_account_id':'another-current'}):
   def changed(table,params):
    rows=original(table,params)
    if table=='challenge_purchases':rows[0].update(patch)
    return rows
   self.read=changed;self.run_case();self.assign.assert_not_called();self.read=original
 def test_progress_assignment_allows_only_exact_voided_first_deliveries(self):
  from test_progress_plans import LifecycleTests
  test=LifecycleTests();test.setUp()
  test.purchase.update(id=PID,trader_id=OWNER)
  test.chain=[dict(self.old,stage='phase1')]
  result=test.g['_assign_mt5_to_trader']({'id':OWNER},{'id':'fresh'},'phase1',test.purchase)
  self.assertEqual(result,('account','trader'))
  test.chain[0]['archive_reason']='ordinary archive'
  with self.assertRaises(ValueError):test.g['_assign_mt5_to_trader']({'id':OWNER},{'id':'fresh'},'phase1',test.purchase)
 def test_lost_case_claim_never_assigns(self):
  self.saved.return_value=False;self.run_case();self.assign.assert_not_called()
 def test_uncertain_assignment_is_not_retried_automatically(self):
  self.assign.side_effect=RuntimeError('uncertain');r=self.run_case();self.assertEqual(r['errors'],1);self.assertEqual(self.saved.call_args.args[1]['state'],'needs_review')
 def test_existing_fresh_successor_is_reconciled_without_assigning_again(self):
  self.chain.append(dict(id='already-created',mt5_login='fresh',stage='phase1'))
  r=self.run_case();self.assertEqual(r['already_fulfilled'],1);self.assign.assert_not_called()
 def test_unrelated_successor_or_missing_original_blocks_another_assignment(self):
  self.chain.append(dict(id='unexpected',mt5_login='other',stage='phase2'));r=self.run_case();self.assertEqual(r['errors'],1);self.assign.assert_not_called()
if __name__=='__main__':unittest.main()
