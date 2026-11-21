"""Prospective terms, exact ledger replay and percentage trailing regressions."""
import copy,json,unittest
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import Mock,patch
from uuid import uuid4
import balance_trailing as bt
from equity_drawdown import spec,initial,evaluate,fingerprint
from plan_risk_controls import freeze,frozen_terms,inherit

class BalanceTrailingTests(unittest.TestCase):
 def setUp(self):
  self.now=datetime.now(timezone.utc).replace(microsecond=0)
  self.plan={'drawdown_type':'TRAILING','journey_source':freeze('plan',dict(daily_drawdown='0',payout_frequency='Weekly',resets={'phase1':'5000'}))}
  self.row=dict(bt.future_plan(self.plan),id=str(uuid4()),trader_id=str(uuid4()),purchase_id=str(uuid4()),mt5_login='123',stage='funded',start_balance=1000000,dd_limit_percent=10,started_at=(self.now-timedelta(seconds=20)).isoformat())
  self.rules=spec(self.row)
 def tick(self,state,equity,balance,peak,seconds=0):
  return evaluate(self.rules,state,equity,(self.now+timedelta(seconds=seconds)).isoformat(),balance=balance,peak_balance=peak)
 def test_all_four_selected_examples(self):
  for peak,floor in [(1000000,900000),(1100000,990000),(1200000,1080000),(1500000,1350000)]:
   with self.subTest(peak=peak):
    s=self.tick(initial(self.rules),peak,peak,peak)
    self.assertEqual(Decimal(s['drawdown_floor']),floor)
    self.assertTrue(self.tick(s,floor,peak,peak,1)['breached'])
 def test_unrealised_profit_never_raises_floor_but_floating_loss_breaches(self):
  s=self.tick(initial(self.rules),1500000,1000000,1000000)
  self.assertEqual(Decimal(s['drawdown_floor']),900000)
  self.assertFalse(self.tick(s,950000,1000000,1000000,1)['breached'])
  s=self.tick(s,900000,1000000,1000000,2)
  self.assertTrue(s['breached']);self.assertEqual(self.tick(s,2000000,2000000,2000000,3),s)
 def test_peak_never_falls_after_closed_loss_or_withdrawal(self):
  s=self.tick(initial(self.rules),1500000,1500000,1500000)
  s=self.tick(s,1400000,1400000,1400000,1)
  self.assertEqual(Decimal(s['drawdown_floor']),1350000)
  self.assertAlmostEqual(float(s['drawdown_percent']),100000/1500000*100)
 def test_no_guessed_peak_when_ledger_missing(self):
  with self.assertRaises(ValueError):self.tick(initial(self.rules),1100000,1100000,None)
 def test_future_snapshot_copies_and_preserves_resets_and_all_stages(self):
  before=copy.deepcopy(self.plan);new=bt.future_plan(self.plan)
  self.assertEqual(self.plan,before)
  for lifecycle in ['phase1','phase2','reset','funded','funded_reset','payout_renewal']:
   row={'journey_source':inherit(lifecycle,new)}
   self.assertEqual(frozen_terms(row),dict(frozen_terms(before),trailing_formula=bt.FORMULA))
  static=dict(self.plan,drawdown_type='STATIC');self.assertEqual(bt.future_plan(static),static)
 def test_old_terms_fingerprint_and_arithmetic_remain_identical(self):
  old=dict(self.row,journey_source=self.plan['journey_source']);rules=spec(old)
  self.assertNotIn('trailing_formula',rules)
  s=evaluate(rules,initial(rules),1200000,self.now.isoformat())
  self.assertNotIn('highest_balance',s);self.assertEqual(Decimal(s['drawdown_floor']),1100000)
  with self.assertRaises(ValueError):evaluate(self.rules,initial(rules),1200000,self.now.isoformat(),balance=1200000,peak_balance=1200000)
 def deals(self,entries):
  return [NS(ticket=i+1,type=kind,time=int((self.now-timedelta(seconds=19-i)).timestamp()),profit=net,commission=0,swap=0,fee=0) for i,(kind,net) in enumerate(entries)]
 def broker(self,entries):
  deals=self.deals(entries);balance=sum(d.profit for d in deals if d.type!=3)
  mt=NS(account_info=Mock(return_value=NS(login=123,balance=balance)),history_deals_get=Mock(return_value=deals))
  return mt
 def test_ledger_realised_peak_includes_fees_and_ignores_cash_in(self):
  mt=self.broker([(2,1000000),(0,100000),(2,500000),(1,100000),(1,-50000),(2,-10000)])
  self.assertEqual(bt.broker_balance_peak(mt,self.row,self.now.isoformat()),1200000)
  mt.history_deals_get.return_value[3].commission=-1000
  mt.account_info.return_value.balance-=1000
  self.assertEqual(bt.broker_balance_peak(mt,self.row,self.now.isoformat()),1199000)
 def test_ledger_rejects_missing_duplicates_reused_identity_and_unreconciled_data(self):
  for change in ['empty','duplicate','reused','identity','balance']:
   mt=self.broker([(2,1000000),(0,100000)])
   if change=='empty':mt.history_deals_get.return_value=[]
   if change=='duplicate':mt.history_deals_get.return_value.append(mt.history_deals_get.return_value[-1])
   if change=='reused':mt.history_deals_get.return_value[-1].time=int((self.now-timedelta(days=1)).timestamp())
   if change=='identity':mt.account_info.return_value.login=456
   if change=='balance':mt.account_info.return_value.balance+=100
   with self.subTest(change=change),self.assertRaises(ValueError):bt.broker_balance_peak(mt,self.row,self.now.isoformat())
 def test_future_purchase_requires_actual_worker_verification(self):
  with patch.object(bt,'runtime_ready',return_value=False):
   with self.assertRaises(ValueError):bt.future_plan(self.plan,require_ready=True)
 def test_runtime_needs_four_new_probes_and_blocks_mixed_or_rollback(self):
  for sources,confirmed,expected in [([bt.SOURCE_PREFIX+str(n)+'_of_4' for n in range(1,5)],False,True),([bt.SOURCE_PREFIX+'1_of_4'],False,False),([],True,True),(['dd_police_daily_v1_shard_1_of_4'],True,False)]:
   def response(data):
    r=Mock(status_code=200);r.json.return_value=data;return r
   records=[{'value':json.dumps({'release':bt.RELEASE})}]
   with self.subTest(sources=sources),patch.dict('os.environ',SUPABASE_URL='https://test.invalid',SUPABASE_KEY='test'),patch('requests.get',side_effect=[response([{'source':s} for s in sources]),response(records if confirmed else [])]),patch('requests.post',return_value=response(records)):
    bt._ready_cache=(0,False);self.assertEqual(bt.runtime_ready(),expected)
  bt._ready_cache=(0,False)

class PackageBalanceTests(unittest.TestCase):
 def test_new_contract_all_stages_and_durable_worker_journal(self):
  from test_package_account_rules import BindingTests
  from package_contracts import digest,initial_state,evaluate
  from package_account_rules import bind_package_account
  from tempfile import TemporaryDirectory
  import importlib.util
  from pathlib import Path
  fixture=BindingTests();fixture.setUp();doc=fixture.contract['document']
  for rule in doc['stage_rules'].values():rule['trailing_formula']=bt.FORMULA
  fingerprint=digest(doc);fixture.contract['rules_hash']=fingerprint;fixture.account['package_rules_hash']=fingerprint
  fixture.account['started_at']=datetime.now(timezone.utc).isoformat()
  self.assertEqual(bind_package_account(fixture.account,fixture.contract)['trailing_formula'],bt.FORMULA)
  for stage in doc['stages']:
   state=initial_state(doc,fixture.account['id'],stage)
   obs={k:state[k] for k in ('account_id','contract_id','trader_id','rules_hash')}
   obs.update(sequence=1,observed_at=datetime.now(timezone.utc).isoformat(),balance=600000,equity=800000,trailing_highest_balance=600000,open_positions=1)
   state=evaluate(doc,state,obs);self.assertEqual(state['drawdown_floor'],'540000.00')
  module_spec=importlib.util.spec_from_file_location('balance_package_journal',Path('drawdown-release/package_dd_runtime.py'))
  module=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(module)
  row=dict(fixture.account,package_contract=fixture.contract)
  with TemporaryDirectory() as directory:
   worker=module.PackageDDJournal(directory)
   state=worker.assess(row,600000,800000,datetime.now(timezone.utc).isoformat(),peak_balance=600000)
   self.assertEqual(state['breach_equity_level'],540000)
   state=module.PackageDDJournal(directory).assess(row,550000,550000,datetime.now(timezone.utc).isoformat(),peak_balance=550000)
   self.assertEqual(state['breach_equity_level'],540000)
   self.assertTrue(worker.assess(row,550000,540000,datetime.now(timezone.utc).isoformat(),peak_balance=600000)['breached'])
 def test_new_trailing_quote_frozen_and_not_orderable_without_verified_worker(self):
  from test_package_checkout import PackageCheckoutTests
  fixture=PackageCheckoutTests();fixture.setUp();fixture.choices['drawdown_type']='trailing'
  with patch('balance_trailing.runtime_ready',return_value=False):
   self.assertEqual(fixture.client.post('/trader/package_quote',json={'choices':fixture.choices}).status_code,409)
   self.assertFalse(fixture.transport.quotes)
  with patch('balance_trailing.runtime_ready',return_value=True):q=fixture.quote()
  self.assertTrue(all(rule['trailing_formula']==bt.FORMULA for rule in q['document']['stage_rules'].values()))
  with patch('balance_trailing.runtime_ready',return_value=False):
   self.assertEqual(fixture.client.post('/trader/package_checkout',json=fixture.checkout_body(q)).status_code,409)

if __name__=='__main__':unittest.main()
