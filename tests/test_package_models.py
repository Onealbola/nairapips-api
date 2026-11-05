"""Account models retain exact prices, checkout gates and immutable purchased routes."""
import copy
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from contextvars import ContextVar
from unittest.mock import Mock
from package_plan_matrix import defaults,make_plan,validate_config
from progress_plan_bridge import plan_row,offer_version
from progress_lifecycle import install,initial_stage,purchase_route
from test_progress_plans import ProgressTests,LifecycleTests

class ModelConfigTests(unittest.TestCase):
    def test_same_allocation_has_independent_model_prices_and_targets(self):
        config=defaults()['config']
        for model,base in [('one_phase',20000),('instant',60000)]:
            p=make_plan(model+'-500k',500000,base,model);p['enabled']=False
            config['plans'].append(p)
        self.assertEqual(validate_config(config),config)
        one,instant=config['plans'][-2:]
        self.assertEqual([x['phase2_target'] for x in one['packages']],[0]*4)
        self.assertEqual([(x['phase1_target'],x['phase2_target']) for x in instant['packages']],[(0,0)]*4)
        for p,route in [(one,['phase1','funded']),(instant,['funded'])]:
            row=plan_row(p,p['packages'][0]);self.assertEqual(row['challenge_journey'],route)
            self.assertEqual(row['status'],'inactive');self.assertEqual(row['payout_cycles'],0)
            changed=copy.deepcopy(row);changed['fee']+=1000
            self.assertNotEqual(offer_version(row),offer_version(changed))
    def test_models_and_targets_are_validated(self):
        for model,targets in [('one_phase',(0,0)),('one_phase',(10,8)),('instant',(10,0)),('two_phase',(10,0))]:
            c=defaults()['config'];p=make_plan('new',500000,50000,model)
            p['packages'][0].update(phase1_target=targets[0],phase2_target=targets[1]);c['plans'].append(p)
            with self.assertRaises(ValueError):validate_config(c)
        c=defaults()['config'];c['plans'][0]['model']='unknown'
        with self.assertRaises(ValueError):validate_config(c)

class ModelCheckoutTests(ProgressTests):
    # Exercise actual owner switch -> physical row -> public catalogue -> checkout gate.
    def test_one_phase_and_instant_publication_and_pause(self):
        config=defaults()['config']
        for model,fee,route in [('one_phase',20000,['phase1','funded']),('instant',60000,['funded'])]:
            config['plans'].append(make_plan('new-'+model,500000,fee,model))
        r=self.client.post('/admin/package_studio/plan_matrix',json={'revision':0,'config':config})
        self.assertEqual(r.status_code,200,r.json)
        switches={f'progress:new-{m}:basic':{'purchases_open':True} for m in ['one_phase','instant']}
        self.assertEqual(self.save(switches).status_code,200)
        for model,fee,route in [('one_phase',20000,['phase1','funded']),('instant',60000,['funded'])]:
            pid=f'progress:new-{model}:basic';product=next(p for p in self.load().json['products'] if p['id']==pid)
            self.assertTrue(product['purchases_open']);row=product['plan']
            self.assertEqual(row['challenge_journey'],route);self.assertEqual(row['fee'],fee)
            response=self.client.post('/create_challenge_purchase',json={'plan_id':row['id'],'fee':fee,'payout_cycles':0,'plan_offer_version':row['offer_version']})
            self.assertEqual(response.status_code,200,response.json)
        self.assertEqual(self.save({k:{'purchases_open':False} for k in switches},1).status_code,200)
        for pid in switches:
            response=self.client.post('/trader/catalog/checkout',json={'product_id':pid})
            self.assertEqual(response.status_code,403)

class AssignmentSnapshotTests(unittest.TestCase):
    def test_real_assignment_preserves_managed_identity_and_route(self):
        from progress_plan_bridge import is_managed
        tree=ast.parse(Path('app.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_assign_mt5_to_trader')
        node.decorator_list=[]
        class StopAfterWrites(Exception):pass
        for model,route in [('two_phase',['phase1','phase2','funded']),('one_phase',['phase1','funded']),('instant',['funded'])]:
            p=make_plan('snapshot-'+model,500000,25000,model);row=plan_row(p,p['packages'][0],True)
            purchase=dict(row,id='purchase',plan_id=row['id'],trader_id='owner')
            writes=[]
            class Query:
                def __init__(self,table):self.table=table;self.payload=None
                def insert(self,data):self.payload=dict(data,id='account');return self
                def update(self,data):self.payload=data;return self
                def eq(self,*a):return self
                def execute(self):writes.append((self.table,copy.deepcopy(self.payload)));return SimpleNamespace(data=[self.payload])
            db=SimpleNamespace(table=Query)
            def stop(*a):raise StopAfterWrites()
            ns=dict(_np_purchase_is_rejected_v154=lambda *a,**kw:False,_np_registry_predecessor_id=lambda p:None,
                ACCOUNT_STAGES=['phase1','phase2','funded'],_np_assert_mt5_pool_matches_stage=Mock(),_assert_mt5_never_used=Mock(),
                clean=lambda v:float(v or 0),_np_query_rows_v173=lambda *a,**kw:[],now_iso=lambda:'now',
                _safe_plan_for_purchase=lambda p:row,_journey_for_lifecycle=lambda *a:tuple(route),_journey_source_value=lambda r:list(r),
                _np_plan_category_v156=lambda v:v or 'standard',_effective_target_percent=lambda stage,*a:0 if stage=='funded' else 10,
                _np_renewal_db_v211=lambda:db,_active_state_for_stage=lambda stage:stage+'_active',
                _np_is_managed_progress_plan=is_managed,_update_trader_lifecycle=stop)
            exec(compile(ast.Module(body=[node],type_ignores=[]),'assignment','exec'),ns)
            with self.assertRaises(StopAfterWrites):
                ns['_assign_mt5_to_trader']({'id':'owner'},{'id':'pool','mt5_login':'123','account_size':500000},route[0],purchase)
            updated=next(data for table,data in writes if table=='challenge_purchases')
            self.assertEqual(updated['journey_source'],purchase['journey_source'])
            self.assertEqual(updated['challenge_journey'],route)
            account=next(data for table,data in writes if table=='trader_accounts')
            self.assertEqual(account['stage'],route[0]);self.assertEqual(account['purchase_id'],'purchase')

class ModelLifecycleTests(LifecycleTests):
    def set_model(self,model):
        plan=make_plan('new-'+model,500000,30000,model);row=plan_row(plan,plan['packages'][0],True)
        self.purchase=dict(row,id='purchase',plan_id=row['id'],trader_id='owner',payment_status='approved',status='approved_active')
        return row
    def test_one_phase_requires_exact_verified_predecessor(self):
        row=self.set_model('one_phase')
        self.assertEqual(initial_stage(self.purchase),'phase1')
        self.assertEqual(self.g['_journey_for_lifecycle'](None,self.purchase),('phase1','funded'))
        self.assertEqual(self.g['_next_stage_for_lifecycle']('phase1',None,self.purchase),'funded')
        with self.assertRaises(ValueError):self.call('phase2')
        with self.assertRaises(ValueError):self.call('funded')
        self.chain=[self.passed('phase1')];self.call('funded')
        # Editing the current plan cannot rewrite an issued purchase route.
        row['challenge_journey']=['funded']
        self.assertEqual(self.g['_journey_for_lifecycle'](None,self.purchase,row),('phase1','funded'))
        self.assertFalse(self.g['_np_progress_phase2_entitlement'](self.chain[0]))
    def test_instant_requires_approval_and_never_enters_challenge_phases(self):
        self.set_model('instant');self.assertEqual(initial_stage(self.purchase),'funded')
        self.assertEqual(self.g['_journey_for_lifecycle'](None,self.purchase),('funded',))
        self.assertIsNone(self.g['_next_stage_for_lifecycle']('funded',None,self.purchase))
        self.assertEqual(self.g['_journey_source_value'](('funded',)),['funded'])
        self.call('funded')
        for stage in ['phase1','phase2']:
            with self.assertRaises(ValueError):self.call(stage)
        self.purchase['payment_status']='pending'
        with self.assertRaises(ValueError):self.call('funded')
        self.g['_NP_APPROVAL_CONTEXT_V216']=ContextVar('instant-approval',default=True)
        self.call('funded')
        self.chain=[dict(id='first',mt5_login='123',stage='funded',account_status='assigned_active')]
        with self.assertRaises(ValueError):self.call('funded')
    def test_invalid_route_fails_closed(self):
        self.set_model('instant');self.purchase['challenge_journey']=['phase2','funded']
        with self.assertRaises(ValueError):purchase_route(self.purchase)
        with self.assertRaises(ValueError):self.call('funded')

if __name__=='__main__':unittest.main()
