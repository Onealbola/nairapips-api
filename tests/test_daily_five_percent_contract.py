"""The owner's daily limit is a permanent breach, independent of overall DD."""
from datetime import datetime,timezone,timedelta
from decimal import Decimal
import copy,unittest
from uuid import uuid4
from equity_drawdown import spec,initial,evaluate
from plan_risk_controls import freeze,inherit,assert_assignment_snapshot

class DailyContractTests(unittest.TestCase):
 def fixture(self,capital=5000000,mode='STATIC',daily='5'):
  now=datetime(2026,11,11,12,tzinfo=timezone.utc)
  purchase=dict(id=str(uuid4()),trader_id=str(uuid4()),drawdown_type=mode,
   journey_source=freeze('purchase',dict(daily_drawdown=daily,payout_frequency='Weekly')))
  account=dict(id=str(uuid4()),purchase_id=purchase['id'],trader_id=purchase['trader_id'],
   mt5_login='123',stage='phase1',start_balance=capital,dd_limit_percent=20,
   journey_source=inherit(('equity_trailing_v1:' if mode=='TRAILING' else '')+'assigned_account',purchase))
  assert_assignment_snapshot(account,purchase)
  return now,purchase,account,spec(account)
 def test_every_capital_and_mode_breaches_at_daily_five_without_waiting_for_twenty(self):
  for capital in (1500000,2500000,5000000):
   for mode in ('STATIC','TRAILING'):
    with self.subTest(capital=capital,mode=mode):
     now,p,a,r=self.fixture(capital,mode)
     floor=Decimal(capital)*Decimal('.95')
     state=evaluate(r,initial(r),floor+Decimal('.01'),now.isoformat(),balance=capital,daily_realised=0)
     self.assertFalse(state['breached'])
     hit=evaluate(r,state,floor,(now+timedelta(seconds=1)).isoformat(),balance=capital,daily_realised=0)
     self.assertTrue(hit['breached'])
     self.assertEqual(Decimal(hit['daily_loss_percent']),5)
     self.assertEqual(Decimal(hit['overall_floor']),Decimal(capital)*Decimal('.8'))
     self.assertEqual(evaluate(r,hit,capital,(now+timedelta(days=1)).isoformat(),balance=capital,daily_realised=0),hit)
 def test_realised_and_floating_losses_combine_for_five_million_plan(self):
  now,p,a,r=self.fixture()
  hit=evaluate(r,initial(r),4750000,now.isoformat(),balance=4850000,daily_realised=-150000)
  self.assertTrue(hit['breached'])
  self.assertEqual(Decimal(hit['daily_loss_percent']),5)
 def test_four_percent_loss_can_continue_next_day_but_next_daily_hit_is_terminal(self):
  now,p,a,r=self.fixture()
  first=evaluate(r,initial(r),4800000,now.isoformat(),balance=4800000,daily_realised=-200000)
  self.assertFalse(first['breached'])
  nextday=evaluate(r,first,4800000,(now+timedelta(days=1)).isoformat(),balance=4800000,daily_realised=0)
  self.assertFalse(nextday['breached'])
  self.assertEqual(Decimal(nextday['daily_loss_percent']),0)
  hit=evaluate(r,nextday,4550000,(now+timedelta(days=1,seconds=1)).isoformat(),balance=4550000,daily_realised=-250000)
  self.assertTrue(hit['breached'])
  self.assertEqual(Decimal(hit['drawdown_percent']),9)
 def test_daily_disabled_plan_does_not_acquire_another_plans_daily_limit(self):
  now,p,a,r=self.fixture(daily='0')
  state=evaluate(r,initial(r),4750000,now.isoformat(),balance=4750000)
  self.assertFalse(state['breached'])
 def test_assignment_rejects_erased_snapshot_wrong_daily_and_wrong_owner(self):
  now,p,a,r=self.fixture()
  bad=copy.deepcopy(a);bad['journey_source']='post9_purchase_root_immutable'
  with self.assertRaisesRegex(ValueError,'rules were not preserved'):assert_assignment_snapshot(bad,p)
  bad['journey_source']=freeze('account',dict(daily_drawdown='0',payout_frequency='Weekly'))
  with self.assertRaises(ValueError):assert_assignment_snapshot(bad,p)
  with self.assertRaises(ValueError):assert_assignment_snapshot(dict(a,trader_id='foreign'),p)
 def test_assignment_rejects_lost_trailing_prefix(self):
  now,p,a,r=self.fixture(mode='TRAILING')
  with self.assertRaisesRegex(ValueError,'drawdown mode'):assert_assignment_snapshot(dict(a,journey_source=a['journey_source'].removeprefix('equity_trailing_v1:')),p)
