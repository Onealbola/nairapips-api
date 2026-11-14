import unittest
import withdrawal_controls as w
from payout_profit_target import gate,install
class TargetTests(unittest.TestCase):
 def setUp(self):
  self.a=w.edit({'id':'exact'},dict(minimum_profit_enabled=True,minimum_profit_percent=5))
 def test_target_boundary_and_stale_verification(self):
  for profit,verified,allowed in [(49999.99,True,False),(50000,True,True),(50000.01,True,True),(100000,False,False)]:
   q=dict(verified=verified,start_balance=1000000,verified_profit=profit,available_payout=30000,trader_account_id='exact')
   result=gate(self.a,q);self.assertEqual(result['payout_target_met'],allowed)
   self.assertEqual(result['available_payout'],30000 if allowed else 0);self.assertEqual(q['available_payout'],30000)
 def test_optional_targets_do_not_change_other_plans(self):
  q=dict(verified=False,available_payout=123)
  self.assertEqual(gate({},q),q)
  self.assertEqual(gate(w.edit({},dict(minimum_profit_enabled=False)),q),q)
 def test_real_request_approval_payment_reject_below_target_and_unverified_before_mutation(self):
  from test_payout_duplicate_guard import GuardTests
  for action in ('create','approve','paid'):
   for verified,profit in [(True,49999.99),(False,100000)]:
    h=GuardTests();h.setUp();h.db.accounts[0].update(w.edit(h.db.accounts[0],dict(minimum_profit_enabled=True,minimum_profit_percent=5)))
    h.db.payouts=[] if action=='create' else [dict(id='p',trader_id='owner',trader_account_id='account',status='pending',amount=100)]
    h.env['_np_verified_payout_quote']=lambda *args:dict(verified=verified,start_balance=1000000,verified_profit=profit,available_payout=60000)
    code=h.invoke(h.handler(action,lambda:self.fail('Unmet target reached payout mutation')),dict(id='p',amount=100,trader_id='owner'))
    self.assertEqual(code,409,(action,verified,profit))
 def test_shared_quote_is_decorated_for_bootstrap_refresh_and_request(self):
  g={'_np_verified_payout_quote':lambda *a,**kw:dict(verified=True,start_balance=1000000,verified_profit=20000,available_payout=12000)}
  install(g);self.assertFalse(g['_np_verified_payout_quote'](self.a)['payout_target_met'])
 def test_daily_weekly_biweekly_and_monthly_schedule_exact_boundary_and_wrong_purchase(self):
  from datetime import datetime,timezone,timedelta
  from plan_risk_controls import freeze
  from payout_profit_target import schedule,enforce_schedule
  start=datetime(2026,11,1,12,tzinfo=timezone.utc)
  for frequency,days in [('Daily',1),('Weekly',7),('Biweekly',14),('Monthly',30)]:
   terms=dict(daily_drawdown='0',payout_frequency=frequency)
   source=freeze('test',terms)
   account=dict(id='a',trader_id='owner',purchase_id='p',created_at=start.isoformat(),journey_source=source)
   purchase=dict(id='p',trader_id='owner',journey_source=source)
   g={'_np_query_rows_v173':lambda table,**kw:[purchase] if table=='challenge_purchases' else []}
   due=start+timedelta(days=days)
   self.assertFalse(schedule(g,account,due-timedelta(microseconds=1))['payout_schedule_met'])
   self.assertTrue(schedule(g,account,due)['payout_schedule_met'])
   purchase['trader_id']='other';self.assertFalse(schedule(g,account,due)['payout_schedule_met'])
   with self.assertRaises(ValueError):enforce_schedule(g,account)
 def test_schedule_blocks_actual_mutation_for_create_approve_paid(self):
  from test_payout_duplicate_guard import GuardTests
  from plan_risk_controls import freeze
  from datetime import datetime,timezone
  source=freeze('test',dict(daily_drawdown='0',payout_frequency='Biweekly'))
  for action in ('create','approve','paid'):
   h=GuardTests();h.setUp();h.db.accounts[0].update(purchase_id='p',journey_source=source,created_at=datetime.now(timezone.utc).isoformat())
   h.env['_np_query_rows_v173']=lambda table,filters=None,**kw:[dict(id='p',trader_id='owner',journey_source=source)] if table=='challenge_purchases' else h.db.rows(table,filters or [])
   h.db.payouts=[] if action=='create' else [dict(id='payout',trader_id='owner',trader_account_id='account',status='pending',amount=100)]
   self.assertEqual(h.invoke(h.handler(action,lambda:self.fail('Early scheduled payout reached mutation')),dict(id='payout',amount=100,trader_id='owner')),409)
