import copy
import unittest
from flask import Flask, jsonify
from production_controls import ControlsStore, FEATURES, SETTING_KEY, Conflict, ControlsUnavailable, defaults, register_controls
from catalog_controls import register_catalog_controls
from package_configuration import PackageStore, defaults as package_defaults, quote, register_package_configuration

class Response:
    def __init__(self, rows, status=200):self.rows,self.status_code=copy.deepcopy(rows),status
    def json(self):return copy.deepcopy(self.rows)
    def raise_for_status(self):
        if self.status_code>=400:raise RuntimeError('HTTP '+str(self.status_code))

class Database:
    def __init__(self):
        self.rows={'business_settings':[], 'challenge_plans':[dict(id='p',name='Original',status='active',updated_at='v1',fee=20000,account_size=500000,phase1_target=10,phase2_target=0,max_drawdown=20,payout_split=60,reset_fee=10000,challenge_journey='one_phase',drawdown_type='STATIC')], 'challenge_purchases':[]}
        self.writes=[];self.fail=False;self.race=False
    def matches(self,row,params):
        for k,v in (params or {}).items():
            if k in ('select','order','limit','offset'):continue
            if v=='is.null':
                if row.get(k) is not None:return False
            elif str(row.get(k))!=str(v)[3:]:return False
        return True
    def get(self,url,headers=None,params=None,timeout=None):
        if self.fail:raise TimeoutError()
        table=url.rsplit('/',1)[-1]
        return Response([r for r in self.rows[table] if self.matches(r,params)])
    def post(self,url,headers=None,json=None,timeout=None):
        table=url.rsplit('/',1)[-1];self.writes.append((table,copy.deepcopy(json)))
        unique='key' if table=='business_settings' else 'id'
        if any(r.get(unique)==json.get(unique) for r in self.rows[table]):return Response([],409)
        self.rows[table].append(copy.deepcopy(json));return Response([json],201)
    def patch(self,url,headers=None,params=None,json=None,timeout=None):
        table=url.rsplit('/',1)[-1]
        if self.race:return Response([])
        updated=[]
        for row in self.rows[table]:
            if self.matches(row,params):row.update(copy.deepcopy(json));updated.append(row)
        self.writes.append((table,copy.deepcopy(json)));return Response(updated)

class ControlsTests(unittest.TestCase):
    def setUp(self):
        self.db=Database();self.store=ControlsStore(self.db,'https://db.invalid','not-secret')
        self.app=Flask(__name__);self.role='owner';self.audit=[];self.actions=[]
        def admin():
            if self.role is None:return None,(jsonify(success=False),401)
            return {'id':'owner-id','role':self.role},None
        register_controls(self.app,self.store,admin,lambda *a:None,lambda *a:self.audit.append(a))
        register_catalog_controls(self.app,self.db,'https://db.invalid','not-secret',admin,lambda *a:None,lambda *a:self.audit.append(a))
        self.packages=PackageStore(self.db,'https://db.invalid','not-secret')
        register_package_configuration(self.app,self.packages,admin,lambda *a:self.audit.append(a))
        for path in ['/create_challenge_purchase','/create_payout','/mark_payout_paid','/monitoring_update','/create_support_ticket','/update_challenge_plan','/delete_challenge_plan']:
            self.app.add_url_rule(path,path,lambda:self.actions.append(1) or jsonify(success=True),methods=['POST','OPTIONS'])
        self.client=self.app.test_client()
    def test_every_switch_blocks_its_registered_actions(self):
        flags=defaults()['features']
        for key,(_,_,paths) in FEATURES.items():
            flags[key]=False
        self.store.save(flags,0,'owner')
        for key,(_,_,paths) in FEATURES.items():
            for path in paths:
                method=self.client.get if key=='announcements' else self.client.post
                response=method(path)
                self.assertEqual(response.status_code,403,(key,path,response.get_json()))
                self.assertFalse(response.get_json()['operation_started'])
        self.assertEqual(self.actions,[])

    def test_old_settings_gain_new_controls_without_reopening_disabled_services(self):
        import json
        doc=defaults();doc['features']={'registrations':False}
        self.db.rows['business_settings']=[{'key':SETTING_KEY,'value':json.dumps(doc)}]
        loaded=self.store.read(fresh=True)[0]
        self.assertFalse(loaded['features']['registrations'])
        self.assertEqual(set(loaded['features']),set(FEATURES))

    def test_owner_auth_required_for_reads_and_writes(self):
        for role,expected in [(None,401),('admin',403),('trader',403)]:
            self.role=role
            for path in ['/admin/production_controls','/admin/production_catalog','/admin/package_configuration']:
                self.assertEqual(self.client.get(path).status_code,expected)
                self.assertEqual(self.client.post(path,json={}).status_code,expected)
        self.assertFalse(self.db.writes)
    def test_persist_readback_and_restart_keeps_disabled_feature(self):
        flags=defaults()['features'];flags['payout_requests']=False
        r=self.client.post('/admin/production_controls',json={'revision':0,'features':flags})
        self.assertEqual(r.status_code,200,r.json)
        other=ControlsStore(self.db,'https://db.invalid','not-secret')
        self.assertFalse(other.read()[0]['features']['payout_requests'])
        self.assertEqual(self.client.post('/create_payout',json={}).status_code,403)
        self.assertFalse(self.actions)
        for path in ['/mark_payout_paid','/monitoring_update']:
            self.assertEqual(self.client.post(path,json={}).status_code,200)
        self.assertEqual(len(self.audit),1)
    def test_unknown_feature_and_string_boolean_cannot_enable_anything(self):
        flags=defaults()['features'];flags['payout_requests']='false'
        self.assertEqual(self.client.post('/admin/production_controls',json={'revision':0,'features':flags}).status_code,400)
        flags=defaults()['features'];flags['package_builder']=True
        self.assertEqual(self.client.post('/admin/production_controls',json={'revision':0,'features':flags}).status_code,400)
        self.assertFalse(self.db.writes)
    def test_stale_revision_and_compare_swap_do_not_overwrite(self):
        flags=defaults()['features'];self.store.save(flags,0,'owner')
        with self.assertRaises(Conflict):self.store.save(flags,0,'other')
        self.db.race=True
        with self.assertRaises(Conflict):self.store.save(flags,1,'other')
        self.assertEqual(self.store.read(fresh=True)[0]['revision'],1)
    def test_compact_write_token_keeps_large_tariffs_out_of_request_urls(self):
        flags=defaults()['features'];self.store.save(flags,0,'owner')
        row=self.db.rows['business_settings'][0]
        self.assertTrue(row['updated_by'].startswith('np-config-write:'))
        self.store.save(flags,1,'owner')
        self.assertEqual(self.store.read(fresh=True)[0]['revision'],2)
        self.assertEqual(self.store.read(fresh=True)[0]['updated_by'],'owner')

    def test_storage_failure_stops_new_request_but_not_monitoring_or_paid_settlement(self):
        self.db.fail=True
        r=self.client.post('/create_payout',json={})
        self.assertEqual(r.status_code,503);self.assertFalse(r.json['operation_started'])
        self.assertEqual(self.client.post('/monitoring_update',json={}).status_code,200)
        self.assertEqual(self.client.post('/mark_payout_paid',json={}).status_code,200)
    def test_inactive_product_cannot_be_purchased_by_direct_request(self):
        self.db.rows['challenge_plans'][0]['status']='inactive'
        r=self.client.post('/create_challenge_purchase',json={'plan_id':'p'})
        self.assertEqual(r.status_code,403);self.assertFalse(r.json['operation_started'])
        self.assertFalse(self.actions)
    def test_inactive_unsupported_draft_cannot_be_released_by_status_only(self):
        row=self.db.rows['challenge_plans'][0];row.update(status='inactive',drawdown_type='TRAILING')
        r=self.client.post('/admin/production_catalog',json={'id':'p','updated_at':'v1','changes':{'status':'active'}})
        self.assertEqual(r.status_code,400,r.json)
        self.assertFalse(self.db.writes)

    def test_stale_or_forged_browser_price_is_rejected(self):
        r=self.client.post('/create_challenge_purchase',json={'plan_id':'p','fee':1})
        self.assertEqual(r.status_code,409);self.assertFalse(self.actions)
        self.assertEqual(self.client.post('/create_challenge_purchase',json={'plan_id':'p','fee':20000}).status_code,200)

    def test_product_can_be_paused_and_reopened_without_deleting_history(self):
        self.db.rows['challenge_purchases']=[{'id':'old','plan_id':'p'}]
        old=copy.deepcopy(self.db.rows['challenge_purchases'])
        r=self.client.post('/admin/production_catalog',json={'id':'p','updated_at':'v1','changes':{'status':'inactive'}})
        self.assertEqual(r.status_code,200,r.json)
        r=self.client.post('/admin/production_catalog',json={'id':'p','updated_at':r.json['plan']['updated_at'],'changes':{'status':'active'}})
        self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(self.db.rows['challenge_purchases'],old)
    def test_sold_rules_cannot_be_changed_create_new_inactive_version(self):
        self.db.rows['challenge_purchases']=[{'id':'old','plan_id':'p'}]
        for field in ['fee','phase1_target','payout_split']:
            value=70 if field=='payout_split' else 15
            r=self.client.post('/admin/production_catalog',json={'id':'p','updated_at':'v1','changes':{field:value}})
            self.assertEqual(r.status_code,409,r.json)
        r=self.client.post('/admin/production_catalog/clone',json={'id':'p','name':'Next product'})
        self.assertEqual(r.status_code,200,r.json)
        clone=self.db.rows['challenge_plans'][-1]
        self.assertNotEqual(clone['id'],'p');self.assertEqual(clone['status'],'inactive')
        self.assertEqual(self.db.rows['challenge_plans'][0]['fee'],20000)
        self.assertTrue(all(table=='challenge_plans' for table,_ in self.db.writes))
    def test_plan_validation_rejects_unsupported_trailing_nan_and_conflicting_routes(self):
        for changes in [{'drawdown_type':'TRAILING'},{'payout_split':80},{'fee':'NaN'},{'max_drawdown':0},{'phase2_target':8},{'fee':True}]:
            r=self.client.post('/admin/production_catalog',json={'id':'p','updated_at':'v1','changes':changes})
            self.assertEqual(r.status_code,400,r.json)
        self.assertFalse(self.db.writes)
    def test_old_editor_cannot_bypass_owner_and_sold_product_protection(self):
        self.role=None
        self.assertEqual(self.client.post('/update_challenge_plan',json={'id':'p','fee':1}).status_code,401)
        self.role='owner';self.db.rows['challenge_purchases']=[{'id':'old','plan_id':'p'}]
        self.assertEqual(self.client.post('/update_challenge_plan',json={'id':'p','fee':1}).status_code,409)
        self.assertEqual(self.client.post('/delete_challenge_plan',json={'id':'p'}).status_code,409)
        self.assertFalse(self.actions)

    def test_new_unsold_product_saves_zero_second_phase_and_real_fee(self):
        r=self.client.post('/admin/production_catalog',json={'id':'p','updated_at':'v1','changes':{'fee':25000,'phase2_target':0,'reset_fee':0}})
        self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(self.db.rows['challenge_plans'][0]['funded_reset_fee'],50000)
        self.assertEqual(self.db.rows['challenge_plans'][0]['phase2_target'],0)

class PackageTests(unittest.TestCase):
    setUp = ControlsTests.setUp
    def configured(self):
        d=package_defaults();d['minimum_price']=5000
        for key,rows in d['options'].items():
            for r in rows:r.update(enabled=True,price=10000 if key=='capital' else 0)
        choices=dict(capital='250000',phases='one',drawdown='20',drawdown_type='static',phase1_target='8',phase2_target=None,profit_share='70',profit_cap='none',payout_cycles='5',phase_resets='0',funded_resets='0')
        return d,choices
    def test_real_saved_configuration_survives_restart_and_does_not_release(self):
        d,_=self.configured();d.pop('updated_at');d.pop('updated_by')
        r=self.client.post('/admin/package_configuration',json=d)
        self.assertEqual(r.status_code,200,r.json)
        restarted=PackageStore(self.db,'https://db.invalid','not-secret')
        self.assertEqual(restarted.read()[0]['minimum_price'],5000)
        self.assertFalse(r.json['release_ready']);self.assertFalse(r.json['data']['enabled'])
    def test_unpriced_enabled_choice_and_unverified_launch_rejected(self):
        d,_=self.configured();d.pop('updated_at');d.pop('updated_by')
        d['enabled']=True
        self.assertEqual(self.client.post('/admin/package_configuration',json=d).status_code,409)
        d['enabled']=False;d['options']['capital'][0]['price']=None
        self.assertEqual(self.client.post('/admin/package_configuration',json=d).status_code,400)
        self.assertFalse(self.db.writes)
    def test_price_all_facilities_floor_rounding_and_disabled_choice(self):
        d,choices=self.configured()
        for key,value,price in [('phases','one',20),('drawdown','20',30),('phase1_target','8',20),('profit_share','70',15),('profit_cap','none',40),('payout_cycles','5',25)]:
            next(r for r in d['options'][key] if r['value']==value)['price']=price
        frozen=copy.deepcopy(d)
        result=quote(d,choices)
        self.assertEqual(result['total'],25000);self.assertFalse(result['checkout_available'])
        self.assertEqual(d,frozen)
        next(r for r in d['options']['profit_share'] if r['value']=='70')['enabled']=False
        with self.assertRaises(ValueError):quote(d,choices)
        d=frozen;d['minimum_price']=27000.01
        self.assertEqual(quote(d,choices)['total'],27000.01)
    def test_owner_can_add_real_capital_and_percentage_choices(self):
        d,choices=self.configured()
        d['options']['capital'].append(dict(value='3000000',enabled=True,price=120000))
        d['options']['phase1_target'].append(dict(value='7.5',enabled=True,price=12))
        result=quote(d,{**choices,'capital':'3000000','phase1_target':'7.5'})
        self.assertEqual(result['total'],134400)
        d['options']['phase1_target'].append(dict(value='7.50',enabled=True,price=12))
        with self.assertRaises(ValueError):quote(d,choices)

    def test_preview_rejects_browser_price_and_phase2_on_one_phase(self):
        d,choices=self.configured()
        with self.assertRaises(ValueError):quote(d,{**choices,'total':1})
        with self.assertRaises(ValueError):quote(d,{**choices,'phase2_target':'8'})
        with self.assertRaises(ValueError):quote(d,{**choices,'profit_share':'99'})


class LegacyPlanCreationTests(unittest.TestCase):
    def test_real_legacy_create_preserves_one_phase_zero_and_starts_unreleased(self):
        import ast
        from pathlib import Path
        from types import SimpleNamespace
        from flask import request
        node=next(n for n in ast.parse(Path('app.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='create_plan')
        node.decorator_list=[]
        rows=[]
        class Query:
            def insert(self,row):rows.append(row);return self
            def execute(self):return SimpleNamespace(data=rows)
        scope=dict(request=request,clean=float,now_iso=lambda:'now',
                   _np_plan_category_v156=lambda v:v or 'standard',
                   _journey_source_value=lambda *v:next(x for x in v if x),
                   _effective_payout_split=lambda v:v or 60,
                   supabase=SimpleNamespace(table=lambda table:Query()),np_invalidate=lambda *a:None,
                   ok=lambda data,message:dict(success=True,data=data),bad=lambda error:dict(success=False,error=str(error)))
        exec(compile(ast.Module(body=[node],type_ignores=[]),'app.py','exec'),scope)
        app=Flask(__name__)
        with app.test_request_context(json=dict(name='Future one phase',account_size=500000,fee=20000,phase2_target=0,reset_fee=10000)):
            response=scope['create_plan']()
        self.assertTrue(response['success'],response)
        self.assertEqual(rows[0]['phase2_target'],0)
        self.assertEqual(rows[0]['challenge_journey'],'one_phase')
        self.assertEqual(rows[0]['status'],'inactive')

if __name__=='__main__':unittest.main()
