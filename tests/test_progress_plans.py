import copy
import unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import Mock
from flask import jsonify
from test_catalog_publication import CatalogTests
from progress_plan_bridge import ProgressPlanBridge,plan_row,is_managed,readiness,offer_version
from progress_lifecycle import install,ROUTE,is_progress_purchase
from plan_commercial import purchase_plan,purchase_catalog,reset_price
from production_controls import register_controls
from package_plan_matrix import register_plan_matrix
from funded_profit_policy import validate_payout
from test_global_live_pass import extract


class ProgressTests(unittest.TestCase):
    load=CatalogTests.load
    save=CatalogTests.save
    invalidate=CatalogTests.invalidate

    def setUp(self):
        CatalogTests.setUp(self)
        self.bridge=ProgressPlanBridge(self.db,'https://db.invalid','fixture',self.matrix,self.store,self.invalidate)
        self.app.config['NP_PROGRESS_PLAN_BRIDGE']=self.bridge
        self.app.config['NP_PROGRESS_SHARED_CATALOG']=self.catalog
        self.app.config['NP_PURCHASE_PLAN_PRICER']=lambda p,n:purchase_plan(p,{},n)
        register_controls(self.app,self.controls,lambda:({'id':'owner','role':self.role},None),lambda *a:None,lambda *a:None)
        register_plan_matrix(self.app,self.matrix,lambda:({'id':'owner','role':self.role},None),lambda *a:None)
        self.identity='progress:ngn-500000:basic'
        doc,_=self.matrix.read();self.allocation=doc['config']['plans'][0];self.package=self.allocation['packages'][0]
        self.pid=plan_row(self.allocation,self.package)['id']
        self.legacy=copy.deepcopy(self.db.rows['challenge_plans'])

    def test_all_default_packages_ready_but_closed_without_customer_mutations(self):
        rows=self.client.get('/admin/catalog_publication').json['products']
        rows=[r for r in rows if r['family']=='progress']
        self.assertEqual(len(rows),12)
        self.assertTrue(all(r['checkout_supported'] and not r['purchases_open'] for r in rows))
        self.assertEqual(self.db.writes,[])

    def test_owner_on_off_gates_both_public_identity_and_direct_plan_alias(self):
        self.assertEqual(self.save({self.identity:{'purchases_open':True}}).status_code,200)
        physical=self.bridge.find(self.pid)
        self.assertTrue(is_managed(physical));self.assertEqual(physical['status'],'active')
        self.assertEqual(self.db.rows['challenge_plans'][:2],self.legacy)
        row=next(p for p in self.load().json['products'] if p['id']==self.identity)
        self.assertTrue(row['purchases_open']);self.assertEqual(row['plan']['default_cycles'],0)
        for path in ['/quote_challenge_price','/create_challenge_purchase']:
            r=self.client.post(path,json={'plan_id':self.pid,'fee':10000,'payout_cycles':0,'plan_offer_version':offer_version(physical)})
            self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(self.save({self.identity:{'purchases_open':False}},1).status_code,200)
        for path in ['/quote_challenge_price','/create_challenge_purchase']:
            self.assertEqual(self.client.post(path,json={'plan_id':self.pid,'fee':10000,'payout_cycles':0,'plan_offer_version':offer_version(physical)}).status_code,403)
        self.assertEqual(self.client.post('/trader/catalog/checkout',json={'product_id':self.identity}).status_code,403)
        self.assertEqual(self.db.rows['challenge_purchases'],[])

    def test_complete_price_is_not_surcharged_or_changed_to_three_cycles(self):
        row=plan_row(self.allocation,self.package,True)
        for cycles in (0,3,None,'0','3'):
            quote=purchase_plan(row,{'0':999999},cycles)
            self.assertEqual((quote['fee'],quote['payout_cycles'],quote['payout_cycle_extra']),(10000,0,0))
        for bad in (True,1,2,4,5,-1,{},'NaN'):
            with self.assertRaises(ValueError):purchase_plan(row,{},bad)
        options=purchase_catalog(row,{})
        self.assertEqual(len(options['cycle_options']),1)
        self.assertEqual(options['default_cycles'],0)
        for stage in ROUTE:self.assertEqual(reset_price(row,stage),0)
        self.save({self.identity:{'purchases_open':True}})
        for fee,cycles,expected in [(1,0,409),(10000,4,409),(10000,0,200)]:
            r=self.client.post('/create_challenge_purchase',json=dict(plan_id=self.pid,fee=fee,payout_cycles=cycles,plan_offer_version=offer_version(self.bridge.find(self.pid))))
            self.assertEqual(r.status_code,expected,r.json)

    def test_matrix_changes_sync_future_offer_without_editing_issued_purchase(self):
        self.save({self.identity:{'purchases_open':True}})
        issued=dict(id='issued',plan_id=self.pid,fee=10000,payout_cycles=0,max_drawdown=5,payout_split=50)
        self.db.rows['challenge_purchases'].append(copy.deepcopy(issued))
        doc,_=self.matrix.read();doc['config']['plans'][0]['packages'][0]['price']=12500
        r=self.client.post('/admin/package_studio/plan_matrix',json=dict(revision=doc['revision'],config=doc['config']))
        self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(self.bridge.find(self.pid)['fee'],12500)
        self.assertEqual(self.db.rows['challenge_purchases'],[issued])
        self.assertEqual(self.client.post('/create_challenge_purchase',json=dict(plan_id=self.pid,fee=10000,payout_cycles=0)).status_code,409)
        self.assertEqual(self.client.post('/create_challenge_purchase',json=dict(plan_id=self.pid,fee=12500,payout_cycles=0,plan_offer_version=offer_version(self.bridge.find(self.pid)))).status_code,200)

    def test_incompatible_draft_edit_closes_sales_and_can_be_reverted(self):
        self.save({self.identity:{'purchases_open':True}})
        doc,_=self.matrix.read();x=doc['config']['plans'][0]['packages'][0];x['daily_drawdown']=10;x['daily_drawdown_enabled']=True
        r=self.client.post('/admin/package_studio/plan_matrix',json=dict(revision=0,config=doc['config']))
        self.assertEqual(r.status_code,200,r.json)
        self.assertFalse(next(p for p in self.load().json['products'] if p['id']==self.identity)['purchases_open'])
        self.assertEqual(self.bridge.find(self.pid)['status'],'inactive')
        self.assertEqual(self.client.post('/create_challenge_purchase',json=dict(plan_id=self.pid,fee=10000)).status_code,403)
        self.assertFalse(readiness(self.allocation,dict(self.package,profit_cap=20))[0])
        x['daily_drawdown']=0;x['daily_drawdown_enabled']=False
        self.assertEqual(self.client.post('/admin/package_studio/plan_matrix',json=dict(revision=1,config=doc['config'])).status_code,200)
        self.assertEqual(self.bridge.find(self.pid)['status'],'active')

    def test_changed_benefits_at_the_same_price_require_new_checkout_acceptance(self):
        self.save({self.identity:{'purchases_open':True}})
        version=offer_version(self.bridge.find(self.pid))
        doc,_=self.matrix.read();doc['config']['plans'][0]['packages'][0]['profit_share']=55
        self.client.post('/admin/package_studio/plan_matrix',json=dict(revision=0,config=doc['config']))
        body=dict(plan_id=self.pid,fee=10000,payout_cycles=0,plan_offer_version=version)
        r=self.client.post('/create_challenge_purchase',json=body)
        self.assertEqual(r.status_code,409);self.assertEqual(r.json['code'],'PACKAGE_OFFER_CHANGED')
        body['plan_offer_version']=offer_version(self.bridge.find(self.pid))
        self.assertEqual(self.client.post('/create_challenge_purchase',json=body).status_code,200)

    def test_database_text_percentages_compare_identically_to_editor_numbers(self):
        from progress_plan_bridge import terms_match
        wanted=plan_row(self.allocation,self.package)
        actual=dict(wanted,payout_split='50',daily_drawdown='0')
        self.assertTrue(terms_match(actual,wanted))
        self.assertEqual(offer_version(actual),offer_version(wanted))
        actual['payout_split']='60'
        self.assertFalse(terms_match(actual,wanted))
        self.assertNotEqual(offer_version(actual),offer_version(wanted))

    def test_every_public_page_uses_current_saved_matrix_not_stale_physical_rows(self):
        doc,_=self.matrix.read()
        p=doc['config']['plans'][0]
        p['packages'][0]['profit_share']=80
        p['packages'][3]['drawdown']=15
        p['packages'][1].update(phase1_target=8,phase2_target=5,price=15500)
        self.matrix.save_draft(dict(revision=0,config=doc['config']),'owner')
        # Simulate the failing database trigger leaving physical rows stale.
        for package in self.allocation['packages']:
            self.db.rows['challenge_plans'].append(plan_row(self.allocation,package))
        self.invalidate()
        for path in ['/public_plan_snapshot','/challenge_plans','/plans']:
            r=self.client.get(path);self.assertEqual(r.status_code,200,r.json)
            rows=r.json['plans'] if isinstance(r.json,dict) else r.json
            basic=next(x for x in rows if x['catalog_product_id']==self.identity)
            both=next(x for x in rows if x['catalog_product_id']=='progress:ngn-500000:both')
            room=next(x for x in rows if x['catalog_product_id']=='progress:ngn-500000:room')
            self.assertEqual(basic['payout_split'],80)
            self.assertEqual(both['max_drawdown'],15)
            self.assertEqual((room['phase1_target'],room['phase2_target'],room['fee']),(8,5,15500))
            self.assertFalse(basic['purchases_open']);self.assertEqual(basic['reset_fee'],0)
            self.assertEqual(len([x for x in rows if x.get('managed_progress')]),12)
            self.assertEqual(r.headers['Cache-Control'],'no-store')
        products=self.load().json['products']
        self.assertEqual(len([x for x in products if x['family']=='progress']),12)
        self.assertFalse(next(x for x in products if x['id']==self.identity)['purchases_open'])

    def test_failed_plan_trigger_reports_saved_edits_and_keeps_checkout_blocked(self):
        from test_production_controls import Response
        self.db.rows['challenge_plans'].append(plan_row(self.allocation,self.package))
        patch=self.db.patch
        self.db.patch=lambda url,**kwargs:Response([],404) if url.endswith('/challenge_plans') else patch(url,**kwargs)
        doc,_=self.matrix.read();doc['config']['plans'][0]['packages'][0]['profit_share']=80
        r=self.client.post('/admin/package_studio/plan_matrix',json=dict(revision=0,config=doc['config']))
        self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(r.json['data']['config']['plans'][0]['packages'][0]['profit_share'],80)
        self.assertIn('database repair',r.json['connection_warning'])
        row=next(p for p in self.load().json['products'] if p['id']==self.identity)
        self.assertEqual(row['package']['profit_share'],80);self.assertFalse(row['purchases_open'])
        self.assertEqual(self.db.rows['challenge_purchases'],[])

    def test_missing_writes_conflicts_and_collisions_never_open_sales(self):
        from test_production_controls import Response
        realpost=self.db.post
        def lost(url,**kw):
            if url.endswith('/challenge_plans'):return Response([],201)
            return realpost(url,**kw)
        self.db.post=lost
        r=self.save({self.identity:{'purchases_open':True}})
        self.assertEqual(r.status_code,503,r.json)
        self.assertEqual(self.store.read(fresh=True)[0]['revision'],0)
        self.db.post=realpost
        self.db.rows['challenge_plans'].append(dict(id=self.pid,name='collision',status='inactive'))
        r=self.save({self.identity:{'purchases_open':True}})
        self.assertEqual(r.status_code,503)
        self.assertEqual(self.db.rows['challenge_plans'][-1]['name'],'collision')
        self.assertFalse(self.store.read(fresh=True)[0]['products'])


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        from package_plan_matrix import defaults
        p=defaults()['config']['plans'][0];row=plan_row(p,p['packages'][0],True)
        self.purchase=dict(row,id='purchase',plan_id=row['id'],trader_id='owner',status='approved_active')
        self.chain=[];self.assigned=Mock(return_value=('account','trader'))
        self.g=dict(_journey_for_lifecycle=lambda *a:('phase1','funded'),
            _next_stage_for_lifecycle=lambda stage,*a:'funded' if stage=='phase1' else None,
            _journey_source_value=lambda *a:['phase1','funded'],_assign_mt5_to_trader=self.assigned,
            _normalize_lifecycle_stage=lambda s:s,
            _np_v214_rows=lambda table,params:copy.deepcopy(self.chain) if table=='trader_accounts' else [self.purchase])
        install(self.g)

    def call(self,stage,p=None):
        return self.g['_assign_mt5_to_trader']({'id':'owner'},{'id':'fresh'},stage,self.purchase if p is None else p)

    def passed(self,stage):
        return dict(id=stage,mt5_login=stage,purchase_id='purchase',trader_id='owner',stage=stage,
            challenge_journey=list(ROUTE),account_status='archived_'+stage,phase_pass_status=stage+'_passed',
            passed_at='2026-11-05T10:00:00+00:00',archive_reason='[NP_LIVE_PASS_V214]')

    def test_two_phases_cannot_be_skipped_and_no_repeat_phase_or_reset(self):
        self.assertTrue(is_progress_purchase(self.purchase))
        self.assertEqual(self.g['_journey_for_lifecycle'](None,None,plan_row({'id':'a','name':'A','capital':500000},{'key':'basic','name':'B','price':10000,'drawdown':5,'profit_share':50,'phase1_target':10,'phase2_target':8})),ROUTE)
        self.call('phase1')
        with self.assertRaises(ValueError):self.call('funded')
        with self.assertRaises(ValueError):self.call('phase2')
        self.chain=[self.passed('phase1')]
        self.assertEqual(self.g['_next_stage_for_lifecycle']('phase1',self.chain[0],self.purchase),'phase2')
        self.call('phase2')
        with self.assertRaises(ValueError):self.call('funded')
        self.chain.append(self.passed('phase2'))
        self.assertEqual(self.g['_next_stage_for_lifecycle']('phase2',self.chain[-1],self.purchase),'funded')
        self.call('funded')
        with self.assertRaises(ValueError):self.call('phase1')
        with self.assertRaises(ValueError):self.call('phase2')
        # Funded renewal still uses the existing payment/owner/MT5 firewalls.
        for _ in range(5):self.call('funded')
        self.assertEqual(self.purchase['payout_cycles'],0)

    def test_breach_forged_pass_and_cross_owner_rejected(self):
        for bad in [dict(self.passed('phase1'),breached_at='now'),dict(self.passed('phase1'),archive_reason='passed')]:
            self.chain=[bad]
            with self.assertRaises(ValueError):self.call('phase2')
        with self.assertRaises(ValueError):self.call('phase1',dict(self.purchase,trader_id='other'))
        self.assigned.assert_not_called()

    def test_existing_legacy_route_untouched_even_with_old_phase2_metadata(self):
        old=dict(self.purchase,journey_source='purchase_plan_snapshot',challenge_journey=list(ROUTE))
        self.assertEqual(self.g['_journey_for_lifecycle'](None,old),('phase1','funded'))
        self.assertEqual(self.g['_next_stage_for_lifecycle']('phase1',None,old),'funded')
        self.call('funded',old)

    def test_production_snapshot_and_live_proof_use_exact_new_terms(self):
        ns=dict(_np_plan_category_v156=lambda v:v,_effective_payout_split=float,clean=lambda v:float(v or 0),now_iso=lambda:'now')
        extract(['_np_plan_rule_snapshot_v156','_np_live_pass_proof_v214'],ns)
        frozen=ns['_np_plan_rule_snapshot_v156'](self.purchase)
        self.assertEqual((frozen['payout_cycles'],frozen['max_drawdown'],frozen['payout_split']),(0,5,50))
        now=datetime.now(timezone.utc)
        for stage,target in [('phase1',10),('phase2',8)]:
            a=dict(id='a',purchase_id='purchase',trader_id='owner',stage=stage,account_status='assigned_active',
                monitoring_enabled=True,mt5_login='123',account_size=500000,start_balance=500000,
                created_at=(now-timedelta(days=1)).isoformat())
            live=dict(trader_account_id='a',trader_id='owner',stage=stage,mt5_login='123',account_size=500000,
                balance=500000*(1+target/100),equity=500000*(1+target/100),dd_used_percent=0,observed_at=now.isoformat())
            self.assertEqual(ns['_np_live_pass_proof_v214'](a,live,self.purchase,now)['target'],target)
            live['equity']-=100
            self.assertIsNone(ns['_np_live_pass_proof_v214'](a,live,self.purchase,now))
        validate_payout(37500,500000,50)
        with self.assertRaises(ValueError):validate_payout(37501,500000,50)
