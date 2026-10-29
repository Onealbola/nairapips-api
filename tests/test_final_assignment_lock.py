import unittest
from unittest.mock import Mock
from final_assignment_lock import check_assignment,install,LOCKED_OWNER,LOCKED_PURCHASE
class LockTests(unittest.TestCase):
 def setUp(self):self.http=Mock();self.trader={'id':LOCKED_OWNER}
 def check(self,purchase=None,note=''):return check_assignment(self.trader,purchase,note,self.http,'https://fixture','key')
 def test_closed_purchase_and_unlinked_manual_assignment_block_without_query(self):
  for p in (None,{'id':LOCKED_PURCHASE}):
   with self.assertRaisesRegex(ValueError,'new purchase'):self.check(p)
  self.http.get.assert_not_called()
 def test_renewal_from_old_source_blocked(self):
  self.http.get.return_value.json.return_value=[dict(id='source',trader_id=LOCKED_OWNER,purchase_id=LOCKED_PURCHASE)]
  with self.assertRaises(ValueError):self.check(note='source_account=d52cc711-b554-4d29-9ee7-865c78d2494a; exact_once=true')
 def test_separate_new_purchase_allowed_after_database_owner_verification(self):
  self.http.get.return_value.json.return_value=[dict(id='new',trader_id=LOCKED_OWNER,status='approved',purchase_type='challenge',plan_name='Legend')];self.check({'id':'new'})
 def test_real_schema_new_purchase_approval_does_not_require_purchase_type(self):
  def get(url,**kwargs):
   params=kwargs['params'];self.assertEqual(params['select'],'*');self.assertEqual(params['id'],'eq.new');self.assertEqual(params['trader_id'],'eq.'+LOCKED_OWNER);self.assertEqual(params['limit'],'1')
   response=Mock();response.json.return_value=[dict(id='new',trader_id=LOCKED_OWNER,status='pending_review',plan_name='Starter')];return response
  self.http.get.side_effect=get
  check_assignment(self.trader,{'id':'new'},'',self.http,'https://fixture','key',approval=True)
  with self.assertRaises(ValueError):self.check({'id':'new'})
 def test_database_failure_fails_closed_without_exposing_internal_url(self):
  self.http.get.side_effect=RuntimeError('https://private-database/rest/v1/challenge_purchases')
  with self.assertRaises(ValueError) as error:self.check({'id':'new'})
  self.assertNotIn('https://',str(error.exception));self.assertIn('could not be verified',str(error.exception))
 def test_any_trader_can_buy_again_after_a_different_purchase_completes(self):
  self.http.get.return_value.json.return_value=[dict(id='new',trader_id='another-owner',status='pending_review',plan_name='Starter')]
  check_assignment({'id':'another-owner'},{'id':'new'},'',self.http,'fixture','key',approval=True)
 def test_any_completed_purchase_and_exact_final_assignment_marker_block(self):
  for fields in [dict(lifecycle_state='funded_cycles_completed'),dict(admin_note='[NP_NO_MORE_MT5:old:final_mt5=123]')]:
   self.http.get.return_value.json.return_value=[dict(id='old',trader_id='another-owner',status='approved',**fields)]
   with self.assertRaises(ValueError):check_assignment({'id':'another-owner'},{'id':'old'},'',self.http,'fixture','key')
 def test_global_ownership_and_database_failures_block_before_assignment(self):
  self.http.get.return_value.json.return_value=[dict(id='new',trader_id='different-owner')]
  with self.assertRaises(ValueError):check_assignment({'id':'another-owner'},{'id':'new'},'',self.http,'fixture','key')
 def test_completed_renewal_resolves_source_purchase_for_any_trader(self):
  self.http.get.return_value.json.side_effect=[[dict(purchase_id='old')],[dict(id='old',trader_id='another-owner',lifecycle_state='funded_cycles_completed')]]
  with self.assertRaises(ValueError):check_assignment({'id':'another-owner'},None,'source_account=d52cc711-b554-4d29-9ee7-865c78d2494a',self.http,'fixture','key')
 def test_rejected_reset_and_other_owner_cannot_bypass(self):
  for row in [dict(id='new',trader_id='other',status='approved'),dict(id='new',trader_id=LOCKED_OWNER,status='rejected'),dict(id='new',trader_id=LOCKED_OWNER,status='approved',purchase_type='funded_reset')]:
   self.http.get.return_value.json.return_value=[row]
   with self.assertRaises(ValueError):self.check({'id':'new'})
 def test_other_traders_unaffected_and_payout_routes_not_wrapped(self):
  check_assignment({'id':'other'},None,'',self.http,'fixture','key');self.http.get.assert_not_called()
  base=Mock();runtime=dict(_assign_mt5_to_trader=base,requests=self.http,SUPABASE_URL='fixture',SUPABASE_KEY='key',mark_paid=Mock());pay=runtime['mark_paid'];install(runtime)
  with self.assertRaises(ValueError):runtime['_assign_mt5_to_trader'](self.trader,{},'funded',{'id':LOCKED_PURCHASE})
  base.assert_not_called();self.assertIs(runtime['mark_paid'],pay)
