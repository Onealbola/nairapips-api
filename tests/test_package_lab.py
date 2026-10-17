import copy
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock
from flask import Flask
from package_configuration import defaults
from package_lab import rehearse,register_package_lab


class LabTests(unittest.TestCase):
    def setUp(self):
        self.config=defaults();self.config['minimum_price']=5000
        for key,rows in self.config['options'].items():
            for row in rows:row.update(enabled=True,price=5000 if key=='capital' else 0)
        self.choices=dict(capital='500000',phases='two',drawdown='10',drawdown_type='static',phase1_target='10',phase2_target='8',profit_share='70',profit_cap='20',payout_cycles='1',phase_resets='1',funded_resets='1')
        self.store=Mock();self.store.read.return_value=(self.config,None)
        self.admin={'id':'fixture','role':'super_admin'};self.auth_error=None
        self.app=Flask(__name__)
        register_package_lab(self.app,self.store,lambda:(self.admin,self.auth_error))
        self.client=self.app.test_client()

    def observe(self,value,opened=0):return dict(kind='observe',balance=value,equity=value,open_positions=opened)
    def run_test(self,actions,choices=None):return rehearse(self.config,choices or self.choices,actions)

    def test_full_two_phase_journey_obeys_frozen_share_cap_and_last_cycle(self):
        result=self.run_test([self.observe(550000),self.observe(540000),self.observe(700000),{'kind':'payout'}])
        self.assertEqual([e.get('next_stage') for e in result['history'][:2]],['phase2','funded'])
        self.assertEqual(result['total_test_payout'],'70000.00')
        self.assertTrue(result['complete']);self.assertEqual(result['usage']['paid_cycles'],1)
        self.assertFalse(result['financial_records_created'])

    def test_cycles_renew_without_spending_reset_allowance(self):
        choices={**self.choices,'payout_cycles':'3'}
        result=self.run_test([self.observe(550000),self.observe(540000),self.observe(525000),{'kind':'payout'},self.observe(525000),{'kind':'payout'}],choices)
        self.assertEqual(result['usage'],{'phase_resets':0,'funded_resets':0,'paid_cycles':2})
        self.assertEqual(result['total_test_payout'],'35000.00')
        self.assertFalse(result['complete']);self.assertIsNone(result['state']['last_observed_at'])

    def test_phase_reset_budget_shared_across_both_phases(self):
        with self.assertRaisesRegex(ValueError,'No included phase resets'):
            self.run_test([self.observe(450000),{'kind':'reset'},self.observe(550000),self.observe(450000),{'kind':'reset'}])

    def test_flat_positions_required_and_trailing_floor_latches(self):
        result=self.run_test([self.observe(560000,1)])
        self.assertEqual(result['state']['stage'],'phase1');self.assertEqual(result['state']['decision'],'active')
        choices={**self.choices,'drawdown_type':'trailing'}
        result=self.run_test([self.observe(560000,1),self.observe(510000)],choices)
        self.assertEqual(result['state']['decision'],'breached')
        self.assertEqual(result['state']['drawdown_floor'],'510000.00')

    def test_browser_amounts_and_extra_allowances_rejected(self):
        with self.assertRaises(ValueError):self.run_test([{'kind':'payout','amount':1000000}])
        with self.assertRaises(ValueError):self.run_test([{'kind':'reset','allowance':100}])
        with self.assertRaises(ValueError):self.run_test([self.observe(500000)]*101)

    def test_owner_draft_is_ephemeral_and_no_database_or_financial_write_occurs(self):
        draft=copy.deepcopy(self.config)
        r=self.client.post('/admin/package_lab/rehearse',json={'draft':draft,'choices':self.choices,'actions':[]})
        self.assertEqual(r.status_code,200,r.json)
        self.assertTrue(r.json['data']['test_only'])
        self.assertEqual(r.json['data']['quote']['total'],5000)
        self.store.read.assert_not_called();self.store.persist.assert_not_called()
        self.assertEqual(draft,self.config)

    def test_nonowner_and_unauthenticated_requests_never_read_catalog(self):
        self.admin['role']='admin'
        self.assertEqual(self.client.post('/admin/package_lab/rehearse',json={}).status_code,403)
        self.auth_error=({'success':False},401)
        self.assertEqual(self.client.post('/admin/package_lab/rehearse',json={}).status_code,401)
        self.store.read.assert_not_called()

    def test_page_served_without_saved_credentials_and_disables_caching_and_framing(self):
        r=self.client.get('/package-lab')
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.headers['Cache-Control'],'no-store')
        self.assertIn("frame-ancestors 'none'",r.headers['Content-Security-Policy'])
        self.assertNotIn(b'localStorage',r.data);self.assertNotIn(b'sessionStorage',r.data)

    def test_production_registration_adds_only_owner_test_and_settings_routes(self):
        app=Flask('registration-fixture')
        app.add_url_rule('/legacy-purchase',view_func=lambda:'legacy unchanged')
        transport=Mock()
        source=(Path(__file__).resolve().parents[1]/'app.py').read_text()
        tail=source[source.index('# V224: private owner test page'):]
        ast.parse(tail)
        scope={'app':app,'requests':transport,'SUPABASE_URL':'https://fixture.invalid',
               'SUPABASE_KEY':'fixture-only','_require_admin':lambda:({'role':'owner'},None),
               '_audit_safe':lambda *args:None}
        exec(compile(tail,'owner-lab-registration','exec'),scope)
        self.assertEqual(transport.mock_calls,[],'Registration must not start external work')
        paths={rule.rule for rule in app.url_map.iter_rules()}
        self.assertIn('/package-lab',paths)
        self.assertIn('/admin/package_configuration',paths)
        self.assertNotIn('/trader/package_checkout',paths)
        self.assertNotIn('/approve_challenge_purchase',paths)
        self.assertEqual(app.test_client().get('/legacy-purchase').data,b'legacy unchanged')


if __name__=='__main__':unittest.main()
