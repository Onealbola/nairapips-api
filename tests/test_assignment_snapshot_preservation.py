import ast,copy,unittest
from pathlib import Path
from unittest.mock import Mock
from plan_risk_controls import freeze,frozen_terms

class AssignmentSnapshotTests(unittest.TestCase):
 def test_ownership_wrapper_preserves_exact_purchased_risk_and_trailing_mode(self):
  tree=ast.parse(Path('app.py').read_bytes())
  node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_assign_mt5_to_trader' and 30000<n.lineno<31000)
  purchase=dict(id='p',trader_id='t',journey_source=freeze('purchase',dict(daily_drawdown='5',payout_frequency='Weekly',resets={'phase1':'35000'})))
  account=dict(id='a',trader_id='t',purchase_id='p',journey_source=freeze('equity_trailing_v1:purchase',frozen_terms(purchase)))
  db=Mock();db.table.return_value.update.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value.data=[account]
  env={'_np_post9_root_purchase':lambda p:True,'_np_post9_assert_purchase_account_ownership':Mock(),
       '_np_assign_mt5_to_trader_pre_root_isolation':lambda *a:(copy.deepcopy(account),{'id':'t'}),
       '_np_renewal_db_v211':lambda:db,'now_iso':lambda:'now'}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'assignment-wrapper','exec'),env)
  out,_=env['_assign_mt5_to_trader']({'id':'t'},{},'phase1',purchase)
  db.table.assert_not_called()
  self.assertEqual(frozen_terms(out),frozen_terms(purchase))
  self.assertEqual(out['journey_source'],account['journey_source'])
  self.assertTrue(out['journey_source'].startswith('equity_trailing_v1:'))
