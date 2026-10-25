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
 def test_rejected_reset_and_other_owner_cannot_bypass(self):
  for row in [dict(id='new',trader_id='other',status='approved'),dict(id='new',trader_id=LOCKED_OWNER,status='rejected'),dict(id='new',trader_id=LOCKED_OWNER,status='approved',purchase_type='funded_reset')]:
   self.http.get.return_value.json.return_value=[row]
   with self.assertRaises(ValueError):self.check({'id':'new'})
 def test_other_traders_unaffected_and_payout_routes_not_wrapped(self):
  check_assignment({'id':'other'},None,'',self.http,'fixture','key');self.http.get.assert_not_called()
  base=Mock();runtime=dict(_assign_mt5_to_trader=base,requests=self.http,SUPABASE_URL='fixture',SUPABASE_KEY='key',mark_paid=Mock());pay=runtime['mark_paid'];install(runtime)
  with self.assertRaises(ValueError):runtime['_assign_mt5_to_trader'](self.trader,{},'funded',{'id':LOCKED_PURCHASE})
  base.assert_not_called();self.assertIs(runtime['mark_paid'],pay)
