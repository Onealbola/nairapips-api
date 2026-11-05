import copy
import unittest
from types import SimpleNamespace
from flask import Flask,jsonify
from catalog_publication import PublicationStore,SharedCatalog,register_catalog_publication,KEY
from package_plan_matrix import PlanMatrixStore
from production_controls import ControlsStore
from test_package_studio import StudioDatabase


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.db=StudioDatabase();self.db.rows['package_studio_private_settings']=[]
        self.db.rows['challenge_plans'][0].update(product_category='standard',payout_cycles=3,
            private_internal_note='do-not-expose',description='Public plan description')
        self.db.rows['challenge_plans'].append({**self.db.rows['challenge_plans'][0],
            'id':'premium','name':'Premium','product_category':'premium','status':'inactive'})
        self.app=Flask(__name__);self.role='owner';self.trader='trader-fixture';self.actions=[]
        self.store=PublicationStore(self.db,'https://db.invalid','fixture')
        self.matrix=PlanMatrixStore(self.db,'https://db.invalid','fixture')
        self.controls=ControlsStore(self.db,'https://db.invalid','fixture')
        self.custom=SimpleNamespace(read=lambda **kw:({'enabled':False},None),readiness=lambda:{'ready':False})
        self.catalog=SharedCatalog(self.db,'https://db.invalid','fixture',self.matrix,self.controls,self.custom,self.app)
        def owner():
            if self.role is None:return None,(jsonify(success=False),401)
            return dict(id='owner',role=self.role),None
        register_catalog_publication(self.app,self.store,self.catalog,owner,
            lambda _:(self.trader,None) if self.trader else ('','missing'),lambda *a:None)
        for path in ['/create_challenge_purchase','/quote_challenge_price','/trader/package_quote']:
            self.app.add_url_rule(path,path,lambda:self.actions.append(1) or jsonify(success=True),methods=['POST','GET'])
        self.client=self.app.test_client()

    def load(self):return self.client.get('/trader/challenge_catalog')
    def save(self,products,revision=0):return self.client.post('/admin/catalog_publication',json=dict(revision=revision,products=products))
    def invalidate(self):
        self.catalog.plans_cache=None
        for s in [self.store,self.matrix,self.controls]:s.cached=None

    def test_public_projection_contains_current_prices_without_private_settings(self):
        r=self.load();self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(len(r.json['products']),13)
        standard=next(p for p in r.json['products'] if p['family']=='standard')
        self.assertEqual(standard['plan']['fee'],20000)
        self.assertTrue(standard['purchases_open'])
        progress=next(p for p in r.json['products'] if p['family']=='progress')
        self.assertFalse(progress['purchases_open']);self.assertTrue(progress['proposal'])
        text=r.get_data(as_text=True)
        for private in ['private_internal_note','do-not-expose','max_profit_share','release_reason','updated_by','_package']:
            self.assertNotIn(private,text)
        self.assertEqual(r.headers['Cache-Control'],'no-store')

    def test_saved_matrix_edits_and_deactivation_reach_public_catalogue(self):
        d,_=self.matrix.read();c=d['config'];c['plans'][0]['packages'][0]['price']=13500
        c['plans'][0]['packages'][3]['enabled']=False
        self.matrix.save_draft(dict(revision=0,config=c),'owner')
        self.invalidate();rows=self.load().json['products']
        self.assertEqual(next(p['package']['price'] for p in rows if p['id']=='progress:ngn-500000:basic'),13500)
        self.assertNotIn('progress:ngn-500000:both',{p['id'] for p in rows})
        self.assertEqual(self.db.rows['challenge_purchases'],[])

    def test_saved_standard_and_premium_edits_reach_public_catalogue(self):
        self.db.rows['challenge_plans'][0]['fee']=22000
        self.db.rows['challenge_plans'][1]['status']='active'
        self.invalidate();rows=self.load().json['products']
        self.assertEqual(next(p['plan']['fee'] for p in rows if p['id']=='plan:p'),22000)
        self.assertIn('plan:premium',{p['id'] for p in rows})
        self.db.rows['challenge_plans'][0]['status']='inactive';self.invalidate()
        self.assertNotIn('plan:p',{p['id'] for p in self.load().json['products']})

    def test_existing_commercial_snapshot_is_used_when_connected(self):
        snapshot={**self.db.rows['challenge_plans'][0],'fee':24000,'base_fee':24000,
            'payout_cycles':3,'cycle_options':[dict(cycles=4,available=True,fee=28000,extra=4000)]}
        self.app.config['NP_SHARED_CATALOG_PLAN_READER']=lambda:[snapshot]
        self.invalidate()
        plan=next(p['plan'] for p in self.load().json['products'] if p['id']=='plan:p')
        self.assertEqual(plan['fee'],24000)
        self.assertEqual(plan['cycle_options'][0]['fee'],28000)
        self.assertNotIn('private_internal_note',plan)

    def test_purchase_switch_is_enforced_on_direct_submission_and_quotes(self):
        self.assertEqual(self.save({'plan:p':{'purchases_open':False}}).status_code,200)
        self.assertFalse(next(p for p in self.load().json['products'] if p['id']=='plan:p')['purchases_open'])
        for path in ['/create_challenge_purchase','/quote_challenge_price']:
            r=self.client.post(path,json={'plan_id':'p','fee':20000})
            self.assertEqual(r.status_code,403);self.assertFalse(r.json['operation_started'])
        self.assertEqual(self.actions,[])
        self.assertEqual(self.save({'plan:p':{'purchases_open':True}},revision=1).status_code,200)
        self.assertEqual(self.client.post('/create_challenge_purchase',json={'plan_id':'p'}).status_code,200)

    def test_unsupported_progress_cannot_be_released_or_bought_by_forged_request(self):
        identity='progress:ngn-500000:basic'
        r=self.save({identity:{'purchases_open':True}})
        self.assertEqual(r.status_code,409);self.assertIn('remain locked',r.json['error'])
        r=self.client.post('/trader/catalog/checkout',json={'product_id':identity,'price':1})
        self.assertEqual(r.status_code,403);self.assertFalse(r.json['operation_started'])
        self.assertEqual(self.db.writes,[])
        self.trader=None
        self.assertEqual(self.client.post('/trader/catalog/checkout',json={'product_id':identity}).status_code,401)

    def test_global_pause_still_overrides_open_product_switch(self):
        d,_=self.controls.read();d['features']['challenge_sales']=False
        self.controls.save(d['features'],0,'owner');self.invalidate()
        self.assertFalse(next(p for p in self.load().json['products'] if p['id']=='plan:p')['purchases_open'])
        self.assertEqual(self.client.post('/create_challenge_purchase',json={'plan_id':'p'}).status_code,403)

    def test_another_worker_pause_is_checked_fresh_before_new_submission(self):
        self.load()  # Warm this worker's empty/default control cache.
        other=PublicationStore(self.db,'https://db.invalid','fixture')
        other.save_switches({'revision':0,'products':{'plan:p':{'purchases_open':False}}},
            'other-owner',self.catalog.products())
        self.assertEqual(self.client.post('/create_challenge_purchase',json={'plan_id':'p'}).status_code,403)
        self.assertEqual(self.actions,[])

    def test_owner_auth_conflicts_and_fail_closed_reads(self):
        for role,code in [(None,401),('trader',403),('admin',403)]:
            self.role=role
            self.assertEqual(self.client.get('/admin/catalog_publication').status_code,code)
            self.assertEqual(self.client.get('/admin/catalog_publication/view').status_code,code)
            self.assertEqual(self.save({}).status_code,code)
        self.role='owner';self.assertEqual(self.save({}).status_code,200)
        self.assertEqual(self.save({}).status_code,409)
        self.db.fail=True;self.invalidate()
        self.assertEqual(self.load().status_code,503)
        self.assertEqual(self.client.post('/create_challenge_purchase',json={'plan_id':'p'}).status_code,503)

    def test_no_financial_or_plan_writes_when_switches_save(self):
        before=copy.deepcopy(self.db.rows['challenge_plans'])
        self.assertEqual(self.save({'plan:p':{'purchases_open':False}}).status_code,200)
        self.assertEqual(self.db.rows['challenge_plans'],before)
        self.assertEqual(self.db.rows['challenge_purchases'],[])
        self.assertEqual({row['key'] for row in self.db.rows['package_studio_private_settings']},{KEY})


if __name__=='__main__':unittest.main()
