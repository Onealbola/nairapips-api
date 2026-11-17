import copy,unittest
from plan_commercial import included_unlimited,purchase_plan,purchase_catalog,priced_plan
class OpenCyclesTests(unittest.TestCase):
 def plan(self,size=500000):return dict(id='plan',account_size=size,fee=20000,product_category='standard',payout_cycles=0,challenge_journey=['phase1','funded'])
 def test_all_requested_sizes_include_unlimited_at_base_price_and_old_client_cannot_force_three(self):
  prices={'0':99999,'3':None,'4':None,'5':None}
  for size in [500000,1000000,2000000]:
   p=self.plan(size);before=copy.deepcopy(p)
   for requested in [None,0,1,2,3,4,5]:
    q=purchase_plan(p,prices,requested);self.assertEqual((q['payout_cycles'],q['fee'],q['payout_cycle_extra']),(0,20000,0))
   q=purchase_catalog(p,prices);self.assertEqual(q['default_cycles'],0);self.assertEqual(len(q['cycle_options']),1);self.assertEqual(q['cycle_options'][0]['cycles'],0)
   self.assertEqual(p,before)
 def test_owner_can_reenable_limits_for_future_sales_without_changing_sold_snapshot(self):
  p=self.plan();sold=purchase_plan(p,{'0':10000},3)
  p['payout_cycles']=3
  future=purchase_plan(p,{'0':10000},3)
  self.assertEqual(future['payout_cycles'],3);self.assertEqual(sold['payout_cycles'],0)
  upgrade=purchase_plan(p,{'0':10000},0);self.assertEqual(upgrade['fee'],30000)
 def test_other_sizes_models_and_markets_keep_paid_upgrade_rules(self):
  for change in [dict(account_size=2500000),dict(challenge_journey=['phase1','phase2','funded']),dict(challenge_journey=['funded']),dict(currency='USD')]:
   p=dict(self.plan(),**change);self.assertFalse(included_unlimited(p))
   self.assertEqual(priced_plan(p,{'0':5000})['fee'],25000)

 def test_existing_unlimited_purchase_progress_shows_no_remaining_cap(self):
  from cycle_progress import progress_report
  from unittest.mock import Mock
  p=dict(self.plan(),id='purchase',trader_id='owner',plan_name='LEGEND')
  result=progress_report(Mock(),[p],[],[])
  self.assertEqual(result['purchases'][0]['status'],'unlimited')
  self.assertEqual(result['purchases'][0]['limit'],0);self.assertIsNone(result['purchases'][0]['remaining']);self.assertFalse(result['purchases'][0]['completed'])
 def test_real_checkout_stores_unlimited_even_when_older_page_submits_three(self):
  from test_buyer_cycle_selection import BuyerCycles
  h=BuyerCycles();h.setUp()
  p=h.db.rows['challenge_plans'][0];p.update(account_size=500000,challenge_journey=['phase1','funded'],payout_cycles=0)
  for requested in [3,0]:
   r=h.client.post('/create_challenge_purchase',json=dict(plan_id='p',payout_cycles=requested,fee=20000))
   self.assertEqual(r.status_code,200,r.json);self.assertEqual(h.actions[-1]['payout_cycles'],0);self.assertEqual(h.actions[-1]['fee'],20000)
