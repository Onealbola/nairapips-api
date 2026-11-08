import ast,copy,unittest,zipfile,hashlib,json
from pathlib import Path
from decimal import Decimal
from unittest.mock import patch
import withdrawal_controls as w
from plan_risk_controls import freeze as risk_freeze,frozen_terms,inherit
from package_plan_matrix import make_plan,validate_config,defaults
from progress_plan_bridge import plan_row,is_managed,managed_id
from progress_lifecycle import is_progress_purchase
class WithdrawalTests(unittest.TestCase):
 def policy(self,**kw):return w.selected(dict(minimum_withdrawal_enabled=False,minimum_withdrawal=0,minimum_profit_enabled=False,minimum_profit_percent=0,profit_cap_enabled=True,profit_cap=15,**kw))
 def test_independent_switches_and_invalid_numbers(self):
  p=self.policy();self.assertFalse(p['minimum_withdrawal_enabled']);self.assertEqual(w.cap_percent({}),15)
  for field in ('minimum_withdrawal','minimum_profit_percent','profit_cap'):
   for value in (True,-1,float('nan'),float('inf'),'no',1.001):
    with self.assertRaises(ValueError):w.selected({field:value})
  with self.assertRaises(ValueError):w.selected(dict(minimum_profit_enabled=True,minimum_profit_percent=0))
 def test_impossible_minimum_target_or_amount_is_rejected(self):
  with self.assertRaises(ValueError):w.selected(dict(minimum_profit_enabled=True,minimum_profit_percent=20,profit_cap=15))
  with self.assertRaises(ValueError):w.selected(dict(account_size=1000000,payout_split=60,profit_cap=10,minimum_withdrawal_enabled=True,minimum_withdrawal=60001))
 def test_both_metadata_versions_round_trip_without_changing_risk_terms(self):
  risk={'daily_drawdown':'2','payout_frequency':'Weekly'}
  policy=w.selected(dict(minimum_withdrawal_enabled=True,minimum_withdrawal=5000))
  source=w.freeze(risk_freeze('progress:x:basic',risk),policy)
  row={'journey_source':source}
  self.assertEqual(frozen_terms(row),risk);self.assertEqual(w.read(row),policy)
  nextrow={'journey_source':inherit('new_funded_assignment',row)}
  self.assertEqual(w.read(nextrow),policy);self.assertEqual(frozen_terms(nextrow),risk)
  self.assertEqual(managed_id(source),managed_id('progress:x:basic'))
 def test_stage_identity_and_immutable_purchase_survive_plan_edits(self):
  p=make_plan('x',1000000,20000);x=p['packages'][0]
  x.update(minimum_profit_enabled=True,minimum_profit_percent=5)
  before=plan_row(p,x);self.assertTrue(is_managed(before));self.assertTrue(is_progress_purchase(dict(plan_id=before['id'],journey_source=before['journey_source'])))
  x['minimum_profit_percent']=7;after=plan_row(p,x)
  self.assertEqual(before['id'],after['id']);self.assertEqual(w.read(before)['minimum_profit_percent'],'5');self.assertEqual(w.read(after)['minimum_profit_percent'],'7')
 def test_minimum_amount_and_percentage_boundaries(self):
  row=w.edit({},dict(minimum_withdrawal_enabled=True,minimum_withdrawal=5000,minimum_profit_enabled=True,minimum_profit_percent=5))
  q=dict(verified=True,start_balance=1000000,verified_profit=50000,available_payout=30000)
  w.validate_request(row,5000,q)
  with self.assertRaises(ValueError):w.validate_request(row,4999.99,q)
  with self.assertRaises(ValueError):w.validate_request(row,5000,dict(q,verified_profit=49999.99))
  with self.assertRaises(ValueError):w.validate_request(row,30000.01,q)
  with self.assertRaises(ValueError):w.validate_request(row,5000,{'verified':False},require_verified=True)
  w.validate_request(row,5000,{'verified':False}) # receivable, pending proof only
 def test_unmarked_legacy_custom_cap_gate_and_no_cap(self):
  self.assertEqual(w.cap_percent({}),15)
  with patch.dict('os.environ',{},clear=True):
   self.assertFalse(w.ready())
   self.assertTrue(w.needs_broker_update(w.edit({},dict(profit_cap_enabled=False))))
  self.assertIsNone(w.cap_percent(w.edit({},dict(profit_cap_enabled=False))))
 def test_custom_and_disabled_caps_affect_actual_verified_quote(self):
  from test_payout_live_quote import PayoutTests
  h=PayoutTests();h.setUp()
  h.a.update(w.edit(h.a,dict(profit_cap=5,profit_cap_enabled=True)))
  self.assertEqual(h.quote()['available_payout'],30)
  h.a.update(w.edit(h.a,dict(profit_cap_enabled=False)))
  self.assertEqual(h.quote()['available_payout'],120)
 def test_request_approval_and_payment_all_enforce_minimum_before_mutation(self):
  from test_payout_duplicate_guard import GuardTests
  for action in ('create','approve','paid'):
   h=GuardTests();h.setUp();h.db.accounts[0].update(w.edit(h.db.accounts[0],dict(minimum_withdrawal_enabled=True,minimum_withdrawal=5000)))
   h.db.payouts=[] if action=='create' else [dict(id='p',trader_id='owner',trader_account_id='account',status='pending',amount=4999)]
   h.env['_np_verified_payout_quote']=lambda *a:dict(verified=True,start_balance=1000000,verified_profit=100000,available_payout=60000)
   code=h.invoke(h.handler(action,lambda:self.fail('Below minimum reached payout mutation')),dict(id='p',amount=4999,trader_id='owner'))
   self.assertEqual(code,409,action)
 def test_operations_uses_per_account_cap_and_never_closes_when_cap_disabled(self):
  path=Path('operations-withdrawal-release/NAIRAPIPS_MT5_ENGINE_NON_DD_OPERATIONS_DIRECT_TELEMETRY_V3_20261004.py')
  tree=ast.parse(path.read_text());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='funded_hard_cap_hit');ns={};exec(compile(ast.Module(body=[node],type_ignores=[]),'ops-fixture','exec'),ns)
  f=ns['funded_hard_cap_hit'];self.assertFalse(f(1000,4.99,4.99,'funded',5));self.assertTrue(f(1000,4,5,'funded',5));self.assertFalse(f(1000,100,100,'funded',None));self.assertFalse(f(1000,20,20,'phase1',5));self.assertTrue(f(1000,15,15,'funded'))
 def test_operations_download_is_exact_checked_code_and_never_auto_restarts(self):
  from flask import Flask
  from panel_delivery import register_panel_delivery
  import io
  app=Flask(__name__);register_panel_delivery(app)
  response=app.test_client().get('/downloads/operations-withdrawal-update.zip');self.assertEqual(response.status_code,200)
  with zipfile.ZipFile(io.BytesIO(response.data)) as z:
   manifest=json.loads(z.read('SHA256.json'))
   for name,digest in manifest.items():self.assertEqual(hashlib.sha256(z.read(name)).hexdigest(),digest)
   self.assertEqual(z.read('withdrawal_controls.py'),Path('withdrawal_controls.py').read_bytes())
   installer=z.read('INSTALL.ps1');self.assertNotIn(b'Stop-Process',installer);self.assertNotIn(b'Start-Process',installer)
   self.assertNotIn('index.html',z.namelist())
  response.close()
 def test_disabled_cap_has_no_draft_cap_limit_and_minimum_controls_use_metadata_only(self):
  from catalog_controls import validated_plan
  p=defaults()['config'];x=p['plans'][0]['packages'][0];x.update(profit_cap_enabled=False,profit_cap=0)
  self.assertEqual(validate_config(p),p)
  patch=validated_plan(dict(status='inactive',phase2_target=0,account_size=1000000,payout_split=60),dict(minimum_withdrawal_enabled=True,minimum_withdrawal=5000))
  self.assertEqual(set(patch),{'journey_source'})
  self.assertTrue(w.read(patch)['minimum_withdrawal_enabled'])
 def test_unchecked_blank_minimums_do_not_require_a_withdrawal_value(self):
  for blank in (None,'','   '):
   row=w.selected(dict(minimum_withdrawal_enabled=False,minimum_withdrawal=blank,minimum_profit_enabled=False,minimum_profit_percent=blank,profit_cap=15))
   self.assertEqual(row['minimum_withdrawal'],'0');self.assertEqual(row['minimum_profit_percent'],'0')
   with self.assertRaisesRegex(ValueError,'Minimum withdrawal amount'):
    w.selected(dict(minimum_withdrawal_enabled=True,minimum_withdrawal=blank))
 def test_another_packages_invalid_active_value_names_its_account_and_field(self):
  config=defaults()['config'];package=config['plans'][1]['packages'][2]
  package.update(minimum_withdrawal_enabled=True,minimum_withdrawal=None)
  with self.assertRaisesRegex(ValueError,'NGN 1,000,000 / More Share.*Minimum withdrawal amount'):
   validate_config(config)
 def test_disabled_null_draft_values_are_normalized_without_mutating_unsaved_input(self):
  config=defaults()['config'];package=config['plans'][1]['packages'][2]
  package.update(minimum_withdrawal_enabled=False,minimum_withdrawal=None,minimum_profit_enabled=False,minimum_profit_percent=None)
  saved=validate_config(config);stored=saved['plans'][1]['packages'][2]
  self.assertEqual(stored['minimum_withdrawal'],0);self.assertEqual(stored['minimum_profit_percent'],0)
  self.assertIsNone(package['minimum_withdrawal'])
