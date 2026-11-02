import copy
import unittest
from flask import Flask, jsonify
from package_studio import (StudioStore, register_package_studio, defaults,
                            validate_config, KEY, ROOT, TEMPLATE, ASSET_KEY)
from test_production_controls import Database, Response


class StudioDatabase(Database):
    def post(self,url,headers=None,json=None,timeout=None):
        table=url.rsplit('/',1)[-1]
        if table!='package_studio_private_settings':
            return super().post(url,headers,json,timeout)
        if any(row['key']==json['key'] for row in self.rows[table]):
            return Response([],409)
        self.writes.append((table,copy.deepcopy(json)))
        self.rows[table].append(copy.deepcopy(json))
        return Response([json],201)


def sample_config():
    c=copy.deepcopy(TEMPLATE)
    c.update(revision=1,phase1=8,phase2=5,grossCap=15,rounding=50,dailyEnabled=True)
    c['base']={k:10000 for k in c['base']}
    for a in c['availability'].values():
        a['active']=True
        for group in a:
            if group!='active':a[group]={k:True for k in a[group]}
    for p in c['riskPolicy'].values():
        p.update(maxShare=50,maxCycles=3,maxCap=15,maxDD=5,maxResets=0,minTarget=8,dailyAboveDD=0,maxPayout=100000,maxPayoutToFee=10)
    return c


class StudioTests(unittest.TestCase):
    def setUp(self):
        self.db=StudioDatabase();self.db.rows['package_studio_private_settings']=[]
        import json
        self.html='<html><body>Owner-only protected draft fixture</body></html>'+(' '*1200)
        self.db.rows['package_studio_private_settings'].append(dict(key=ASSET_KEY,value=json.dumps(dict(revision=1,defaults=sample_config(),html=self.html,updated_at=None)),updated_by='fixture'))
        self.store=StudioStore(self.db,'https://db.invalid','fixture')
        self.role='owner';self.audits=[]
        self.app=Flask(__name__)
        def auth():
            if self.role is None:return None,(jsonify(success=False),401)
            return {'id':'owner-fixture','role':self.role},None
        register_package_studio(self.app,self.store,auth,lambda *a:self.audits.append(a))
        self.client=self.app.test_client()

    def save(self,config=None,revision=0,**extra):
        return self.client.post('/admin/package_studio/draft',json={
            'revision':revision,'config':config or sample_config(),**extra})

    def test_authentication_and_roles_protect_formulas_and_writes(self):
        for role,code in [(None,401),('trader',403),('admin',403),('manager',403)]:
            self.role=role
            for path in ['/admin/package_studio/view','/admin/package_studio/draft']:
                self.assertEqual(self.client.get(path).status_code,code)
            self.assertEqual(self.save().status_code,code)
        self.assertEqual(self.db.writes,[])
        public=self.client.get('/package-studio').get_data(as_text=True)
        self.assertNotIn('maxPayoutToFee',public)
        self.assertNotIn('riskPolicy',public)

    def test_saved_draft_is_shared_and_has_no_commercial_effect(self):
        before=copy.deepcopy(self.db.rows)
        config=sample_config();config['base']['500000']=10000
        config['availability']['500000']['profitCap']['30']=False
        config['riskPolicy']['1000000']['maxDD']=10
        r=self.save(config)
        self.assertEqual(r.status_code,200,r.get_json())
        self.assertFalse(r.json['checkout_enabled'])
        doc=StudioStore(self.db,'https://db.invalid','fixture').read(fresh=True)[0]
        self.assertEqual(doc['config'],config)
        self.assertEqual(doc['revision'],1)
        self.assertFalse(doc['checkout_enabled'])
        self.assertEqual(doc['mode'],'draft')
        self.assertEqual(self.db.rows['challenge_plans'],before['challenge_plans'])
        self.assertEqual(self.db.rows['challenge_purchases'],[])
        self.assertEqual({row['key'] for row in self.db.rows['package_studio_private_settings']},{KEY,ASSET_KEY})
        self.assertEqual(len(self.audits),1)

    def test_stale_revision_and_release_attempt_do_not_write(self):
        self.assertEqual(self.save().status_code,200)
        writes=len(self.db.writes)
        self.assertEqual(self.save().status_code,409)
        self.assertEqual(self.save(revision=1,enabled=True).status_code,400)
        self.assertEqual(self.save(revision=1,checkout_enabled=True).status_code,400)
        self.assertEqual(len(self.db.writes),writes)

    def test_racing_admin_write_is_not_silently_overwritten(self):
        self.assertEqual(self.save().status_code,200)
        self.db.race=True
        self.assertEqual(self.save(revision=1).status_code,409)

    def test_unavailable_database_does_not_fall_back_to_fake_success(self):
        self.db.fail=True
        self.assertEqual(self.client.get('/admin/package_studio/draft').status_code,503)
        self.assertEqual(self.save().status_code,503)

    def test_missing_private_assets_blocks_view_and_draft(self):
        self.db.rows['package_studio_private_settings']=[]
        self.assertEqual(self.client.get('/admin/package_studio/view').status_code,503)
        self.assertEqual(self.client.get('/admin/package_studio/draft').status_code,503)
        self.assertEqual(self.db.writes,[])

    def test_bad_prices_targets_and_shapes_are_rejected(self):
        for mutate in [lambda c:c.update(phase2=8),
                       lambda c:c['base'].update({'500000':-1}),
                       lambda c:c['share'].update({'50':10}),
                       lambda c:c['availability']['500000'].update(active='true'),
                       lambda c:c['riskPolicy']['1000000'].update(maxDD=99),
                       lambda c:c.update(blockedCombinations=[{'capital':500000}]),
                       lambda c:c.update(extra='unrecognised')]:
            config=sample_config();mutate(config)
            self.assertEqual(self.save(config).status_code,400)
        self.assertEqual(self.db.writes,[])

    def test_private_view_is_not_cached_and_uses_hosted_adapter(self):
        r=self.client.get('/admin/package_studio/view')
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.headers['Cache-Control'],'no-store')
        text=r.get_data(as_text=True)
        self.assertEqual(text,self.html)


if __name__=='__main__':unittest.main()
