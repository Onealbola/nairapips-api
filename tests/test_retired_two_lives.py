import unittest
from unittest.mock import Mock
from retired_two_lives import Retirement,install,marked,REASON

class RetirementTests(unittest.TestCase):
 def setUp(self):
  self.rows={'old':dict(id='old',trader_id='owner',second_life_enabled=True,life_number=1),'new':dict(id='new',trader_id='owner',second_life_enabled=False,lives_total=1),'reset':dict(id='reset',trader_id='owner',admin_note='[NP_RESET_REQUEST:source:old:funded]')}
  self.http=Mock()
  def get(url,**kw):
   p=kw['params'];self.assertEqual(p['select'],'*');r=Mock();r.json.return_value=[self.rows[p['id'][3:]]];return r
  self.http.get.side_effect=get
  self.runtime=dict(requests=self.http,SUPABASE_URL='https://fixture',SUPABASE_KEY='key')
  for name in ['_assign_mt5_to_trader','_np_auto_assign_waiting_stage','_np_v122_source_block_reason','_np_retry_waiting_second_life_assignment','_np_auto_free_phase1_reset_after_breach','_np_auto_fulfill_exact_reset_v43','_np_reset_policy','_np_inventory_wait_alert','_np_auto_post_payout_renewal']:self.runtime[name]=Mock(return_value='called')
  self.original=dict(self.runtime);self.runtime['_np_v122_load_source']=Mock(return_value=({'trader_id':'owner','purchase_id':'old'},''));self.runtime['_np_v122_load_payout']=Mock(return_value={'id':'payout'})
  self.runtime['_np_limited_cycle_guard']=Mock();self.runtime['_np_monitoring_registry_activate']=Mock()
  install(self.runtime);self.policy=self.runtime['_np_two_lives_retirement']
 def test_first_life_in_two_lives_purchase_is_retired(self):self.assertTrue(self.policy.blocked('owner',{'id':'old'}))
 def test_string_false_is_not_retired(self):self.assertFalse(marked({'second_life_enabled':'false','lives_total':1,'second_life_status':'not_included'}))
 def test_life_two_and_total_two_are_retired(self):
  for p in [{'life_number':2},{'lives_total':'2'},{'second_life_used':'true'}]:self.assertTrue(marked(p))
 def test_new_purchase_for_same_person_unaffected(self):self.assertFalse(self.policy.blocked('owner',{'id':'new'}))
 def test_reset_follows_exact_parent(self):self.assertTrue(self.policy.blocked('owner',{'id':'reset'}))
 def test_cycle_and_missing_lineage_fail_closed(self):
  self.rows['reset']['admin_note']='[NP_RESET_REQUEST:source:reset:funded]'
  self.assertTrue(self.policy.stopped('owner',{'id':'reset'}));self.assertTrue(self.policy.stopped('owner',{'id':'missing'}))
 def test_final_assignment_blocks_old_but_allows_new(self):
  fn=self.runtime['_assign_mt5_to_trader']
  with self.assertRaisesRegex(ValueError,'retired'):fn({'id':'owner'},{},'funded',{'id':'old'})
  self.original['_assign_mt5_to_trader'].assert_not_called();self.assertEqual(fn({'id':'owner'},{},'phase1',{'id':'new'}),'called')
 def test_progression_is_stopped_before_picker_and_stock_alert(self):
  self.assertIsNone(self.runtime['_np_auto_assign_waiting_stage']({'id':'owner'},'funded',{'id':'old'}));self.original['_np_auto_assign_waiting_stage'].assert_not_called()
 def test_second_life_retry_is_disabled(self):
  self.assertIsNone(self.runtime['_np_retry_waiting_second_life_assignment']({'id':'old'},'owner'));self.original['_np_retry_waiting_second_life_assignment'].assert_not_called()
 def test_paid_payout_is_not_changed_and_renewal_blocked(self):
  p={'id':'paid','status':'paid'};self.assertIsNone(self.runtime['_np_auto_post_payout_renewal'](p));self.assertEqual(p['status'],'paid');self.assertEqual(self.runtime['_np_v122_source_block_reason']({'trader_id':'owner','purchase_id':'old'},p),REASON)
 def test_both_reset_paths_and_policy_block(self):
  a={'trader_id':'owner','purchase_id':'old'}
  self.assertIsNone(self.runtime['_np_auto_free_phase1_reset_after_breach']({'id':'owner'},a));self.assertIsNone(self.runtime['_np_auto_fulfill_exact_reset_v43'](self.rows['reset']));self.assertFalse(self.runtime['_np_reset_policy'](a)['eligible'])
 def test_retired_stock_emails_suppressed_but_other_stock_alerts_unchanged(self):
  alert=self.runtime['_np_inventory_wait_alert'];alert('phase1',2000000,'second_life',{})
  alert('funded',2000000,'V122 paid payout 03c5ac9a-1fc6-4875-b3ad-c61c3026bb22',{})
  self.original['_np_inventory_wait_alert'].assert_not_called();alert('phase1',250000,'new purchase',{});self.original['_np_inventory_wait_alert'].assert_called_once()
 def test_payout_actions_and_monitoring_reactivation_are_blocked(self):
  a={'trader_id':'owner','purchase_id':'old'}
  for action in ['create','approve','paid']:
   with self.assertRaisesRegex(ValueError,'inactive'):self.runtime['_np_limited_cycle_guard'].acquire(a,'payout',action)
  with self.assertRaisesRegex(ValueError,'inactive'):self.runtime['_np_monitoring_registry_activate'](a)
 def test_independent_new_product_financial_and_activation_paths_delegate(self):
  a={'trader_id':'owner','purchase_id':'new'}
  self.runtime['_np_limited_cycle_guard'].acquire(a,'payout','create');self.runtime['_np_monitoring_registry_activate'](a)
