import ast,unittest
from pathlib import Path
from catalog_controls import validated_plan
from funded_profit_policy import maximum_payout
class ShareTests(unittest.TestCase):
 def test_authorized_shares_create_and_edit(self):
  for share in [50,60,70]:
   self.assertEqual(validated_plan({}, {'payout_split':share})['payout_split'],share)
 def test_other_values_stay_blocked_pending_authorization(self):
  for share in [0,-1,101,80,'NaN',float('inf'),True]:
   with self.assertRaises(ValueError):validated_plan({}, {'payout_split':share})
 def test_saved_share_is_used_without_changing_profit_cap(self):
  node=next(n for n in ast.parse(Path('app.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='_effective_payout_split')
  env={'DEFAULT_PAYOUT_PROFIT_SHARE_PERCENT':60};exec(compile(ast.Module(body=[node],type_ignores=[]),'isolated','exec'),env)
  for share,expected in [(50,37500),(60,45000),(70,52500)]:
   self.assertEqual(env['_effective_payout_split'](share),share)
   self.assertEqual(maximum_payout(500000,share),expected)
  self.assertEqual(env['_effective_payout_split'](None),60)
 def test_database_guard_retains_fifteen_percent_and_purchase_share(self):
  s=Path('migrations/20261024_editable_profit_share.sql').read_text()
  self.assertIn('share NOT IN (50,60,70)',s);self.assertIn('capital*0.15*share/100',s);self.assertIn('SELECT c.payout_split INTO share',s)
