import ast, unittest
from pathlib import Path
from unittest.mock import Mock
from plan_cycles import cycle_limit, paid_cycle_decision


class PlanCycleTests(unittest.TestCase):
    def test_all_choices_and_invalid_inputs(self):
        for n in range(6):self.assertEqual(cycle_limit(str(n)), n)
        self.assertIsNone(cycle_limit(None))
        for value in (True, False, -1, 6, 2.5, 'NaN', 'unlimited', '', '2.0'):
            with self.assertRaises(ValueError):cycle_limit(value)

    def test_cycle_read_fails_closed_without_repeating_mutation(self):
        http=Mock();http.post.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):paid_cycle_decision(http,'https://db.invalid','fixture','payout')
        self.assertEqual(http.post.call_count,1)

    def test_successful_cycle_response_is_checked(self):
        http=Mock();http.post.return_value.json.return_value={'state':'completed','paid_cycles':2,'cycle_limit':2}
        self.assertEqual(paid_cycle_decision(http,'https://db.invalid','fixture','id')['state'],'completed')
        http.post.return_value.json.return_value={'state':'maybe'}
        with self.assertRaises(RuntimeError):paid_cycle_decision(http,'https://db.invalid','fixture','id')

    def test_actual_purchase_snapshot_uses_three_by_default_and_freezes_chosen_count(self):
        tree=ast.parse(Path('app.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_np_plan_rule_snapshot_v156')
        env={'_np_plan_category_v156':lambda x:x,'_effective_payout_split':lambda x:x,
             'clean':lambda x:float(x or 0),'now_iso':lambda:'fixture-time'}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'snapshot','exec'),env)
        snap=env[node.name]
        self.assertEqual(snap({})['payout_cycles'],3)
        self.assertEqual(snap({'payout_cycles':None})['payout_cycles'],3)
        for n in range(6):self.assertEqual(snap({'payout_cycles':n})['payout_cycles'],n)
        plan={'payout_cycles':3};purchase=snap(plan);plan['payout_cycles']=5
        self.assertEqual(purchase['payout_cycles'],3)

    def test_real_renewal_checks_cycles_before_inventory_assignment(self):
        tree=ast.parse(Path('app.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v122_process')
        source=ast.unparse(node)
        self.assertLess(source.index('paid_cycle_decision('),source.index('_np_v122_claim_source('))
        self.assertLess(source.index('paid_cycle_decision('),source.index('_np_pick_fresh_mt5('))
