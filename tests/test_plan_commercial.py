import ast
import copy
import unittest
from pathlib import Path
from flask import Flask, jsonify
from plan_commercial import (CyclePriceStore, defaults, priced_plan, reset_price,
                             register_cycle_prices, money)
from production_controls import register_controls, ControlsStore, Conflict
from test_production_controls import Database


class CommercialTests(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.store = CyclePriceStore(self.db, 'https://db.invalid', 'fixture')
        self.app = Flask(__name__)
        self.role = 'owner'
        self.actions = []
        def auth():
            if self.role is None:
                return None, (jsonify(success=False), 401)
            return {'id': 'owner', 'role': self.role}, None
        register_cycle_prices(self.app, self.store, auth, lambda *a: None, lambda *a: None)
        register_controls(self.app, ControlsStore(self.db, 'https://db.invalid', 'fixture'), auth,
                          lambda *a: None, lambda *a: None)
        self.app.config['NP_STANDARD_PLAN_PRICER'] = lambda p: priced_plan(p, self.store.read()[0]['prices'])
        @self.app.post('/create_challenge_purchase')
        def purchase():
            from flask import request
            self.actions.append(copy.deepcopy(request.environ['np_verified_checkout_plan_v221']))
            return jsonify(success=True)
        self.client = self.app.test_client()
        self.prices = {'3': 5000, '4': 9000, '5': 12000, '0': 20000}

    def test_distinct_upgrade_prices_and_free_one_two_and_default(self):
        for n, extra in [(None, 0), (1, 0), (2, 0), (3, 5000), (4, 9000), (5, 12000), (0, 20000)]:
            source = dict(fee=40000, payout_cycles=n)
            result = priced_plan(source, self.prices)
            self.assertEqual(result['fee'], 40000 + extra)
            self.assertEqual(result['base_fee'], 40000)
            self.assertEqual(result['payout_cycle_extra'], extra)
            self.assertEqual(source['fee'], 40000)
            self.assertEqual(priced_plan(result, self.prices)['fee'], result['fee'])
        self.assertEqual(priced_plan(dict(fee=40000, product_category='premium', payout_cycles=5), {})['fee'], 40000)

    def test_checkout_rejects_base_only_stale_and_client_forged_prices(self):
        self.store.save(dict(revision=0, prices=self.prices), 'owner')
        plan = self.db.rows['challenge_plans'][0]
        plan['payout_cycles'] = 3
        self.assertEqual(self.client.post('/create_challenge_purchase', json=dict(plan_id='p', fee=20000, payout_cycle_extra=0)).status_code, 409)
        self.assertEqual(self.client.post('/create_challenge_purchase', json=dict(plan_id='p', fee=25000)).status_code, 200)
        self.assertEqual(self.actions[0]['fee'], 25000)
        self.assertEqual(self.actions[0]['payout_cycles'], 3)
        self.store.save(dict(revision=1, prices={**self.prices, '3': 6000}), 'owner')
        self.assertEqual(self.client.post('/create_challenge_purchase', json=dict(plan_id='p', fee=25000)).status_code, 409)
        self.assertEqual(len(self.actions), 1)

    def test_unpriced_upgrades_unavailable_and_saving_does_not_edit_purchases(self):
        self.db.rows['challenge_plans'][0]['payout_cycles'] = 4
        self.db.rows['challenge_purchases'] = [dict(id='sold', fee=20000, payout_cycles=2)]
        self.assertEqual(self.client.post('/create_challenge_purchase', json=dict(plan_id='p', fee=20000)).status_code, 409)
        saved = self.client.post('/admin/standard_cycle_prices', json=dict(revision=0, prices=self.prices))
        self.assertEqual(saved.status_code, 200, saved.json)
        self.assertEqual(self.db.rows['challenge_purchases'], [dict(id='sold', fee=20000, payout_cycles=2)])
        other = CyclePriceStore(self.db, 'https://db.invalid', 'fixture')
        self.assertEqual(other.read()[0]['prices'], self.prices)
        with self.assertRaises(Conflict):
            other.save(dict(revision=0, prices=self.prices), 'owner')

    def test_prices_require_owner_and_valid_positive_extra_cost(self):
        for role, code in [(None, 401), ('admin', 403), ('trader', 403)]:
            self.role = role
            self.assertEqual(self.client.get('/admin/standard_cycle_prices').status_code, code)
            self.assertEqual(self.client.post('/admin/standard_cycle_prices', json={}).status_code, code)
        self.role = 'owner'
        for value in [0, -1, True, 'NaN', 'Infinity', '1.001']:
            response = self.client.post('/admin/standard_cycle_prices', json=dict(revision=0, prices={**self.prices, '3': value}))
            self.assertEqual(response.status_code, 400, response.json)
        self.assertFalse(self.db.writes)

    def test_not_available_is_not_free_and_custom_survives_fee_change(self):
        for stage in ['phase1', 'phase2', 'funded']:
            self.assertEqual(reset_price(dict(fee=40000, reset_fee=0, funded_reset_fee=0), stage), 0)
        plan = dict(fee=40000, reset_fee=7500, funded_reset_fee=23000)
        self.assertEqual(reset_price(plan, 'phase1'), 7500)
        self.assertEqual(reset_price(plan, 'funded'), 23000)
        plan['fee'] = 90000
        self.assertEqual(reset_price(plan, 'funded'), 23000)
        plan['funded_reset_fee'] = None
        self.assertEqual(reset_price(plan, 'funded'), 180000)

    def test_actual_phase_policy_blocks_unavailable_but_preserves_open_order(self):
        tree = ast.parse(Path('app.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_np_reset_policy')
        env = dict(_np_reset_account_is_breached=lambda a: True,
                   _np_reset_stage=lambda a: 'phase1',
                   _np_reset_purchase_for_account=lambda a: {'id': 'sold'},
                   _np_reset_current_plan_for=lambda a, p: {'reset_fee': 0},
                   _np_reset_price=reset_price, clean=lambda v: float(v or 0),
                   _second_life_bool=bool, _np_reset_open_order=lambda a: None)
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'policy', 'exec'), env)
        result = env['_np_reset_policy']({'id': 'account'})
        self.assertFalse(result['eligible'])
        self.assertEqual(result['reason'], 'reset_not_available')
        env['_np_reset_open_order'] = lambda a: {'id': 'paid', 'payment_status': 'approved', 'fee': 5000}
        self.assertEqual(env['_np_reset_policy']({'id': 'account'})['kind'], 'waiting')

    def test_actual_funded_policy_blocks_unavailable_and_keeps_paid_order(self):
        tree = ast.parse(Path('app.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_np_v62_direct_funded_reset_policy')
        env = dict(_np_reset_account_is_breached=lambda a: True,
                   _normalize_lifecycle_stage=lambda s: s,
                   _np_reset_purchase_for_account=lambda a: {'id': 'sold'},
                   _np_reset_open_order=lambda a: None,
                   _np_v62_exact_funded_reset_consumed=lambda a, p: (False, ''),
                   _np_v61_reset_price=lambda a, p: (0, {'funded_reset_fee': 0}, 'admin'),
                   clean=lambda v: float(v or 0))
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'funded-policy', 'exec'), env)
        result = env[node.name]({'id': 'account', 'stage': 'funded'})
        self.assertFalse(result['eligible'])
        self.assertEqual(result['reason'], 'reset_not_available')
        env['_np_reset_open_order'] = lambda a: {'id': 'paid', 'payment_status': 'approved', 'fee': 12000}
        self.assertEqual(env[node.name]({'id': 'account', 'stage': 'funded'})['kind'], 'waiting')

    def test_public_prices_refresh_even_with_cached_product_rows(self):
        from types import SimpleNamespace
        tree = ast.parse(Path('app.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_np_public_plan_snapshot_rows')
        rows = [dict(id='p', product_category='standard', status='active', fee=40000, payout_cycles=3)]
        prices = dict(self.prices)
        env = dict(np_cached_read=lambda *a: rows, _np_priced_plan=priced_plan,
                   _np_cycle_price_store=SimpleNamespace(read=lambda: ({'prices': prices}, None)))
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'public-prices', 'exec'), env)
        self.assertEqual(env[node.name]()[0]['fee'], 45000)
        prices['3'] = 8000
        self.assertEqual(env[node.name]()[0]['fee'], 48000)
        self.assertEqual(rows[0]['fee'], 40000)

if __name__ == '__main__':
    unittest.main()
