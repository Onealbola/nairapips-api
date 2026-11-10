import copy
import unittest
from flask import Flask,jsonify
from package_plan_matrix import (PlanMatrixStore,defaults,make_plan,validate_config,
    register_plan_matrix,availability,KEY)
from test_package_studio import StudioDatabase


class MatrixTests(unittest.TestCase):
    def setUp(self):
        self.db=StudioDatabase();self.db.rows['package_studio_private_settings']=[]
        self.store=PlanMatrixStore(self.db,'https://fixture.invalid','fixture')
        self.app=Flask(__name__);self.role='owner';self.audit=[]
        def owner():
            if self.role is None:return None,(jsonify(success=False),401)
            return dict(id='fixture-owner',role=self.role),None
        register_plan_matrix(self.app,self.store,owner,lambda *a:self.audit.append(a))
        self.client=self.app.test_client()

    def save(self,c=None,revision=0,**kwargs):
        return self.client.post('/admin/package_studio/plan_matrix',json={
            'revision':revision,'config':c or defaults()['config'],**kwargs})

    def test_disabled_blank_daily_limit_saves_without_enabling_it(self):
        from progress_plan_bridge import plan_row
        for blank in (None,'','   '):
            with self.subTest(blank=blank):
                config=defaults()['config'];package=config['plans'][2]['packages'][2]
                package.update(daily_drawdown_enabled=False,daily_drawdown=blank,
                    drawdown_type='TRAILING',drawdown=10,profit_share=70,
                    minimum_withdrawal_enabled=False,minimum_withdrawal=10,
                    minimum_profit_enabled=True,minimum_profit_percent=10,
                    profit_cap_enabled=True,profit_cap=10,
                    payout_schedule_enabled=True,payout_frequency='Daily')
                normalized=validate_config(config)
                stored=normalized['plans'][2]['packages'][2]
                self.assertFalse(stored['daily_drawdown_enabled'])
                self.assertEqual(stored['daily_drawdown'],0)
                self.assertEqual(stored['payout_frequency'],'Daily')
                self.assertEqual(stored['minimum_profit_percent'],10)
                self.assertEqual(stored['drawdown_type'],'TRAILING')
                self.assertEqual(package['daily_drawdown'],blank)
                self.assertEqual(plan_row(normalized['plans'][2],stored)['daily_drawdown'],0)
        config=defaults()['config'];config['plans'][2]['packages'][2].update(daily_drawdown_enabled=False,daily_drawdown=None)
        response=self.save(config)
        self.assertEqual(response.status_code,200,response.json)
        stored=self.store.read(fresh=True)[0]['config']['plans'][2]['packages'][2]
        self.assertEqual(stored['daily_drawdown'],0)
        self.assertFalse(stored['daily_drawdown_enabled'])

    def test_enabled_blank_daily_limit_is_rejected_and_off_value_is_remembered(self):
        config=defaults()['config'];package=config['plans'][2]['packages'][2]
        package.update(daily_drawdown_enabled=True,daily_drawdown=None)
        response=self.save(config)
        self.assertEqual(response.status_code,400)
        self.assertIn('More Share',response.json['error'])
        self.assertIn('daily drawdown',response.json['error'])
        self.assertEqual(self.db.rows['package_studio_private_settings'],[])
        package.update(daily_drawdown_enabled=False,daily_drawdown=5)
        stored=validate_config(config)['plans'][2]['packages'][2]
        self.assertEqual(stored['daily_drawdown'],5)
        self.assertFalse(stored['daily_drawdown_enabled'])

    def test_daily_schedule_survives_save_reload_and_public_plan_translation(self):
        from progress_plan_bridge import plan_row,commercial_plan
        from plan_risk_controls import frozen_terms
        config=defaults()['config'];package=config['plans'][0]['packages'][1]
        package.update(payout_schedule_enabled=True,payout_frequency='Daily')
        response=self.save(config)
        self.assertEqual(response.status_code,200,response.json)
        stored=self.store.read(fresh=True)[0]['config']['plans'][0]
        wanted=stored['packages'][1]
        self.assertTrue(wanted['payout_schedule_enabled'])
        self.assertEqual(wanted['payout_frequency'],'Daily')
        row=plan_row(stored,wanted)
        self.assertEqual(frozen_terms(row)['payout_frequency'],'Daily')
        self.assertEqual(commercial_plan(row)['payout_frequency'],'Daily')
        self.assertEqual(plan_row(stored,stored['packages'][0])['payout_frequency'],'On Demand')

    def test_enabled_phase1_reset_saves_with_unchecked_blank_prices(self):
        config=defaults()['config'];package=config['plans'][0]['packages'][3]
        package.update(phase1_reset_enabled=True,phase1_reset_fee=14000,
                       phase2_reset_enabled=False,phase2_reset_fee='',
                       funded_reset_enabled=False,funded_reset_fee=None,
                       minimum_profit_enabled=True,minimum_profit_percent=10,
                       profit_cap_enabled=True,profit_cap=15,
                       payout_schedule_enabled=True,payout_frequency='Weekly')
        response=self.save(config)
        self.assertEqual(response.status_code,200,response.json)
        stored=self.store.read(fresh=True)[0]['config']['plans'][0]['packages'][3]
        self.assertTrue(stored['phase1_reset_enabled'])
        self.assertEqual(stored['phase1_reset_fee'],14000)
        for stage in ('phase2','funded'):
            self.assertFalse(stored[stage+'_reset_enabled'])
            self.assertEqual(stored[stage+'_reset_fee'],0)
        self.assertEqual(stored['minimum_profit_percent'],10)
        self.assertEqual(stored['profit_cap'],15)
        self.assertEqual(stored['payout_frequency'],'Weekly')
        self.assertIsNone(package['funded_reset_fee'])

    def test_all_disabled_blank_resets_save_without_enabling_them(self):
        config=defaults()['config']
        for plan in config['plans']:
            for package in plan['packages']:
                for stage in ('phase1','phase2','funded'):
                    package[stage+'_reset_enabled']=False
                    package[stage+'_reset_fee']=None
        response=self.save(config)
        self.assertEqual(response.status_code,200,response.json)
        for plan in self.store.read(fresh=True)[0]['config']['plans']:
            for package in plan['packages']:
                for stage in ('phase1','phase2','funded'):
                    self.assertFalse(package[stage+'_reset_enabled'])
                    self.assertEqual(package[stage+'_reset_fee'],0)

    def test_reset_validation_names_enabled_stage_and_preserves_inactive_price(self):
        config=defaults()['config'];package=config['plans'][0]['packages'][3]
        package.update(phase1_reset_enabled=True,phase1_reset_fee=14000,
                       funded_reset_enabled=True,funded_reset_fee=None)
        response=self.save(config)
        self.assertEqual(response.status_code,400)
        self.assertIn('Both benefits',response.json['error'])
        self.assertIn('Funded reset price',response.json['error'])
        self.assertEqual(self.db.rows['package_studio_private_settings'],[])
        package.update(funded_reset_enabled=False,funded_reset_fee=7000)
        response=self.save(config)
        self.assertEqual(response.status_code,200,response.json)
        stored=self.store.read(fresh=True)[0]['config']['plans'][0]['packages'][3]
        self.assertEqual(stored['funded_reset_fee'],7000)
        self.assertFalse(stored['funded_reset_enabled'])

    def test_cap_and_weekly_payout_save_with_both_minimums_off_and_blank(self):
        config=defaults()['config'];package=config['plans'][0]['packages'][0]
        package.update(minimum_withdrawal_enabled=False,minimum_withdrawal=None,
                       minimum_profit_enabled=False,minimum_profit_percent=None,
                       profit_cap_enabled=True,profit_cap=15,
                       payout_schedule_enabled=True,payout_frequency='Weekly')
        response=self.save(config)
        self.assertEqual(response.status_code,200,response.json)
        stored=self.store.read(fresh=True)[0]['config']['plans'][0]['packages'][0]
        self.assertFalse(stored['minimum_withdrawal_enabled'])
        self.assertFalse(stored['minimum_profit_enabled'])
        self.assertEqual(stored['minimum_withdrawal'],0)
        self.assertEqual(stored['minimum_profit_percent'],0)
        self.assertEqual(stored['profit_cap'],15)
        self.assertEqual(stored['payout_frequency'],'Weekly')
        self.assertIsNone(package['minimum_withdrawal'])

    def test_hidden_package_validation_identifies_account_and_enabled_field(self):
        config=defaults()['config'];package=config['plans'][2]['packages'][3]
        package.update(minimum_withdrawal_enabled=True,minimum_withdrawal=None)
        response=self.save(config)
        self.assertEqual(response.status_code,400)
        self.assertIn('NGN 2,000,000 / Both benefits',response.json['error'])
        self.assertIn('Minimum withdrawal amount',response.json['error'])
        self.assertEqual(self.db.rows['package_studio_private_settings'],[])

    def test_drawdown_type_round_trips_and_invalid_modes_are_rejected(self):
        c=defaults()['config'];c['plans'][0]['packages'][0]['drawdown_type']='TRAILING'
        r=self.save(c);self.assertEqual(r.status_code,200,r.json)
        confirmed=PlanMatrixStore(self.db,'https://fixture.invalid','fixture').read(fresh=True)[0]
        self.assertEqual(confirmed['config']['plans'][0]['packages'][0]['drawdown_type'],'TRAILING')
        c['plans'][0]['packages'][0]['drawdown_type']='PEAK_BALANCE'
        self.assertEqual(self.save(c,revision=1).status_code,400)
        self.assertEqual(self.db.rows['challenge_purchases'],[])

    def test_all_future_models_and_new_sizes_offer_both_drawdown_systems(self):
        from progress_plan_bridge import plan_row
        for model in ('one_phase','two_phase','instant'):
            config=defaults()['config']
            plan=make_plan('future-'+model,5000000,100000,model=model)
            self.assertTrue(all(x['drawdown_type']=='STATIC' for x in plan['packages']))
            for package in plan['packages']:package['drawdown_type']='TRAILING'
            config['plans'].append(plan)
            checked=validate_config(config)
            for package in checked['plans'][-1]['packages']:
                self.assertEqual(plan_row(plan,package)['drawdown_type'],'TRAILING')

    def test_three_sizes_have_the_agreed_four_complete_prices(self):
        d=self.client.get('/admin/package_studio/plan_matrix').json
        self.assertFalse(d['checkout_enabled'])
        plans=d['data']['config']['plans']
        self.assertEqual([p['capital'] for p in plans],[500000,1000000,2000000])
        self.assertEqual([[x['price'] for x in p['packages']] for p in plans],
            [[10000,14000,12000,16000],[20000,28000,24000,32000],[40000,56000,48000,64000]])
        self.assertTrue(all(x['payout_mode']=='continuing' for p in plans for x in p['packages']))
        self.assertEqual(self.db.writes,[])

    def test_names_prices_benefits_and_future_allocations_persist(self):
        c=defaults()['config'];p=c['plans'][0];p['name']='My 500k'
        p['packages'][0].update(name='Custom Basic',price=11000,phase1_target=8,phase2_target=5)
        p['packages'][1].update(drawdown=15,daily_drawdown=10)
        p['limits']['max_drawdown']=15
        p['packages'][3]['enabled']=False
        new=make_plan('future-5m',5000000,100000);new['enabled']=False;c['plans'].append(new)
        response=self.save(c);self.assertEqual(response.status_code,200,response.json)
        confirmed=PlanMatrixStore(self.db,'https://fixture.invalid','fixture').read(fresh=True)[0]
        self.assertEqual(confirmed['config'],c)
        self.assertEqual(confirmed['revision'],1)
        self.assertFalse(confirmed['checkout_enabled'])
        self.assertEqual(self.db.rows['challenge_purchases'],[])
        self.assertEqual(len(self.db.rows['challenge_plans']),1)
        self.assertEqual({x['key'] for x in self.db.rows['package_studio_private_settings']},{KEY})
        self.assertFalse(response.json['availability'][p['id']]['both']['available'])
        self.assertFalse(response.json['availability']['future-5m']['basic']['available'])

    def test_package_terms_are_independent_of_legacy_account_caps_and_siblings(self):
        c=defaults()['config'];p=c['plans'][2]
        p['limits'].update(max_drawdown=1,max_profit_share=1,max_profit_cap=1)
        original=copy.deepcopy(p['packages'][0])
        p['packages'][3].update(drawdown=20,profit_share=70,profit_cap=15)
        r=self.save(c);self.assertEqual(r.status_code,200,r.json)
        for key in ('basic','room','share','both'):
            self.assertTrue(r.json['availability'][p['id']][key]['available'])
        stored=self.store.read(fresh=True)[0]['config']['plans'][2]
        self.assertEqual(stored['packages'][0],original)
        self.assertEqual(stored['packages'][3]['drawdown'],20)
        self.assertEqual(stored['packages'][3]['profit_share'],70)
        self.assertEqual(stored['limits'],p['limits'])
        p['packages'][3]['enabled']=False
        r=self.save(c,revision=1)
        self.assertFalse(r.json['availability'][p['id']]['both']['available'])
        self.assertTrue(r.json['availability'][p['id']]['basic']['available'])

    def test_unsupported_targets_daily_rules_and_duplicate_sizes_rejected(self):
        mutators=[lambda c:c['plans'][0]['packages'][0].update(phase1_target=10,phase2_target=5),
            lambda c:c['plans'][0]['packages'][0].update(drawdown=30,daily_drawdown=-1),
            lambda c:c['plans'][0]['packages'][0].update(profit_share=True),
            lambda c:c['plans'][0]['packages'][0].update(price=-1),
            lambda c:c['plans'][1].update(capital=500000),
            lambda c:c['plans'][1].update(id=c['plans'][0]['id']),
            lambda c:c['plans'][0]['packages'].pop(),
            lambda c:c['plans'][0]['packages'][0].update(payout_mode='unlimited-checkout')]
        for change in mutators:
            c=defaults()['config'];change(c)
            self.assertEqual(self.save(c).status_code,400)
        self.assertEqual(self.db.writes,[])

    def test_wider_drawdown_visibility_uses_own_rules_not_account_cap(self):
        c=defaults()['config'];p=c['plans'][0];x=p['packages'][1]
        x.update(drawdown=30,daily_drawdown=10)
        r=self.save(c);self.assertEqual(r.status_code,200)
        self.assertTrue(r.json['availability'][p['id']]['room']['available'])
        p['limits']['max_drawdown']=30
        r=self.save(c,revision=1);self.assertTrue(r.json['availability'][p['id']]['room']['available'])

    def test_conflicts_auth_and_release_attempts_do_not_change_production(self):
        for role,code in [(None,401),('admin',403),('trader',403)]:
            self.role=role
            self.assertEqual(self.save().status_code,code)
            self.assertEqual(self.client.get('/admin/package_studio/plan_matrix').status_code,code)
            self.assertEqual(self.client.get('/admin/package_studio/plan_matrix/view').status_code,code)
        self.role='owner';self.assertEqual(self.save().status_code,200)
        self.assertEqual(self.save().status_code,409)
        self.assertEqual(self.save(revision=1,checkout_enabled=True).status_code,400)
        self.db.race=True;self.assertEqual(self.save(revision=1).status_code,409)

    def test_owner_page_has_cache_and_frame_protection(self):
        r=self.client.get('/admin/package_studio/plan_matrix/view')
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.headers['Cache-Control'],'no-store')
        self.assertEqual(r.headers['X-Frame-Options'],'DENY')
        self.assertIn(b'Add account size',r.data)

    def test_database_failure_does_not_return_default_success(self):
        self.db.fail=True
        self.assertEqual(self.client.get('/admin/package_studio/plan_matrix').status_code,503)
        self.assertEqual(self.save().status_code,503)


if __name__=='__main__':unittest.main()
