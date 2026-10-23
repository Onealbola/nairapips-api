"""Buyer-selected entitlements and prices, exercising the real middleware/endpoints."""
import ast
import unittest
from pathlib import Path
from flask import request
from plan_commercial import purchase_plan, purchase_catalog
from test_plan_commercial import CommercialTests

class BuyerCycles(CommercialTests):
    def setUp(self):
        super().setUp()
        self.app.config['NP_PURCHASE_PLAN_PRICER'] = lambda p, cycles: purchase_plan(p, (self.premium if p.get('product_category')=='premium' else self.store).read(fresh=True)[0]['prices'], cycles)

    # Legacy fixed-preset expectations are covered by CommercialTests, not the buyer flow.
    test_checkout_rejects_base_only_stale_and_client_forged_prices = None
    test_unpriced_upgrades_unavailable_and_saving_does_not_edit_purchases = None
    test_premium_prices_are_independent_and_cannot_bypass_checkout = None

    def test_buyer_selection_overrides_admin_preset_and_free_default(self):
        self.db.rows['challenge_plans'][0]['payout_cycles']=5
        self.store.save(dict(revision=0, prices=self.prices), 'owner')
        for cycles, extra in [(1,0),(2,0),(3,0),(4,9000),(5,5000),(0,20000)]:
            r=self.client.post('/create_challenge_purchase', json=dict(plan_id='p',payout_cycles=cycles,fee=20000+extra))
            self.assertEqual(r.status_code,200,r.json)
            self.assertEqual(self.actions[-1]['payout_cycles'],cycles)
            self.assertEqual(self.actions[-1]['fee'],20000+extra)
        self.assertEqual(self.client.post('/create_challenge_purchase',json=dict(plan_id='p',fee=20000)).status_code,200)
        self.assertEqual(self.actions[-1]['payout_cycles'],3)
        self.assertEqual(self.db.rows['challenge_plans'][0]['payout_cycles'],5)

    def test_unconfigured_unlimited_and_invalid_selection_cannot_be_sold(self):
        for cycle in [0,4,5,True,-1,6,'2.0','nan',{},[]]:
            r=self.client.post('/create_challenge_purchase',json=dict(plan_id='p',payout_cycles=cycle,fee=20000))
            self.assertEqual(r.status_code,409,r.json)
        self.assertFalse(self.actions)
        r=self.client.get('/plan_purchase_options?plan_id=p')
        self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(r.json['data']['payout_cycles'],3)
        self.assertEqual([o['cycles'] for o in r.json['data']['cycle_options'] if o['available']],[1,2,3])
        self.assertEqual(r.headers['Cache-Control'],'no-store')
        self.assertFalse(self.db.writes)

    def test_market_prices_and_stale_fee_enforced_for_buyer(self):
        self.store.save(dict(revision=0,prices=self.prices),'owner')
        self.premium.save(dict(revision=0,prices={**self.prices,'5':18000}),'owner')
        self.db.rows['challenge_plans'][0]['product_category']='premium'
        r=self.client.get('/plan_purchase_options?plan_id=p&payout_cycles=5')
        self.assertEqual(r.json['data']['fee'],38000)
        for fee in [20000,25000]:
            self.assertEqual(self.client.post('/create_challenge_purchase',json=dict(plan_id='p',payout_cycles=5,fee=fee)).status_code,409)
        self.assertEqual(self.client.post('/create_challenge_purchase',json=dict(plan_id='p',payout_cycles=5,fee=38000)).status_code,200)
        self.premium.save(dict(revision=1,prices={**self.prices,'5':19000}),'owner')
        self.assertEqual(self.client.post('/create_challenge_purchase',json=dict(plan_id='p',payout_cycles=5,fee=38000)).status_code,409)
        self.assertEqual(self.actions[0]['fee'],38000)
        self.assertEqual(self.actions[0]['payout_cycles'],5)

    def test_real_purchase_snapshot_keeps_buyer_cycles_and_base_reset_rule(self):
        tree=ast.parse(Path('app.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_np_plan_rule_snapshot_v156')
        env={'_np_plan_category_v156':lambda x:x or 'standard','_effective_payout_split':lambda x:x,'clean':lambda x:float(x or 0),'now_iso':lambda:'fixture'}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'snapshot','exec'),env)
        plan=purchase_plan(dict(fee=20000,payout_cycles=2),self.prices,5)
        snap=env[node.name](plan)
        self.assertEqual(snap['payout_cycles'],5)
        self.assertEqual(snap['funded_reset_fee'],40000)

    def test_real_promo_quote_uses_server_selected_total(self):
        tree=ast.parse(Path('app.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='quote_challenge_price')
        node.decorator_list=[]
        self.store.save(dict(revision=0,prices=self.prices),'owner')
        from flask import jsonify
        from plan_commercial import resolve_purchase_plan
        env=dict(request=request,jsonify=jsonify,clean=lambda x:float(x or 0),bad=lambda e,c=400:(jsonify(error=str(e)),c),_np_market_cycle_stores={'standard':self.store,'premium':self.premium},_np_resolve_purchase_plan=resolve_purchase_plan,_affiliate_quote_details=lambda d,fee:dict(valid=True,original_fee=fee,final_fee=fee*.9))
        exec(compile(ast.Module(body=[node],type_ignores=[]),'promo','exec'),env)
        with self.app.test_request_context('/quote_challenge_price',method='POST',json=dict(plan_id='p',payout_cycles=5,fee=1)):
            result=env[node.name]()
            self.assertEqual(result.json['data']['original_fee'],25000)
            self.assertEqual(result.json['data']['final_fee'],22500)

if __name__=='__main__':unittest.main()
