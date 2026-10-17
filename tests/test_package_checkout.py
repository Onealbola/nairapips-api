import copy
import unittest
from types import SimpleNamespace
from uuid import uuid4
from flask import Flask
from package_configuration import defaults
from package_checkout import PackageOrderStore,register_package_checkout

class Reply:
    def __init__(self,value,code=200):self.value=value;self.status_code=code;self.ok=code<400
    def json(self):return copy.deepcopy(self.value)
    def raise_for_status(self):
        if not self.ok:raise RuntimeError('Fixture HTTP failure')
class Transport:
    def __init__(self):self.quotes=[];self.calls=[];self.timeout=False;self.contracts=[]
    def get(self,url,headers=None,params=None,timeout=None):
        self.calls.append(('GET',url,copy.deepcopy(params)))
        if url.endswith('/np_package_quotes'):
            rows=[r for r in self.quotes if r['id']==params['id'][3:] and r['trader_id']==params['trader_id'][3:]]
            return Reply(rows)
        if url.endswith('/np_package_contracts'):
            return Reply([r for r in self.contracts if r['document']['trader_id']==params['trader_id'][3:]])
        if url.endswith('/rpc/np_package_schema_version'):return Reply(dict(schema_version='NP_PACKAGE_SCHEMA_1',ready=True))
        raise AssertionError(url)
    def post(self,url,headers=None,json=None,timeout=None):
        self.calls.append(('POST',url,copy.deepcopy(json)))
        if url.endswith('/np_package_quotes'):
            self.quotes.append(copy.deepcopy(json));return Reply([json],201)
        if url.endswith('/rpc/np_package_checkout'):
            if self.timeout:raise TimeoutError('Lost response after possible commit')
            return Reply(dict(purchase_id=json['p_quote_id'],contract_id=self.quotes[0]['document']['contract_id'],replayed=False))
        raise AssertionError(url)

class PackageCheckoutTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);self.transport=Transport();self.tid=str(uuid4());self.authed=True;self.ready=True
        self.cfg=defaults();self.cfg.update(enabled=True,minimum_price=5000,revision=3)
        for key,rows in self.cfg['options'].items():
            for row in rows:row.update(enabled=True,price=10000 if key=='capital' else 0)
        self.flags={'features':{'challenge_sales':True}}
        self.choices=dict(capital='500000',phases='one',drawdown='10',drawdown_type='static',phase1_target='10',phase2_target=None,profit_share='70',profit_cap='none',payout_cycles='3',phase_resets='0',funded_resets='0')
        self.store=PackageOrderStore(self.transport,'https://db.invalid','fixture-key')
        register_package_checkout(self.app,self.store,SimpleNamespace(read=lambda **kw:(self.cfg,None)),
            SimpleNamespace(read=lambda **kw:(self.flags,None)),
            lambda _: (self.tid,None) if self.authed else ('','Authentication required'),
            lambda:dict(ready=self.ready,blockers=[] if self.ready else ['DD fleet']))
        self.client=self.app.test_client()
    def quote(self):
        response=self.client.post('/trader/package_quote',json={'choices':self.choices})
        self.assertEqual(response.status_code,200,response.json)
        return response.json['quote']
    def checkout_body(self,q):
        return dict(quote_id=q['id'],checkout_key=str(uuid4()),payment_proof_url='https://proof.invalid/receipt',accepted_rules_hash=q['rules_hash'])
    def test_auth_required_on_all_customer_package_routes(self):
        self.authed=False
        for method,path in [('GET','/trader/package_catalog'),('GET','/trader/package_contracts'),('POST','/trader/package_quote'),('POST','/trader/package_checkout')]:
            self.assertEqual(self.client.open(path,method=method,json={}).status_code,401)
        self.assertFalse(self.transport.calls)
    def test_unreleased_catalog_and_unverified_fleet_do_not_expose_choices(self):
        self.ready=False
        r=self.client.get('/trader/package_catalog');self.assertFalse(r.json['available']);self.assertEqual(r.json['options'],{})
        self.assertEqual(self.client.post('/trader/package_quote',json={'choices':self.choices}).status_code,409)
        self.assertFalse(self.transport.calls)
    def test_quote_owner_and_price_are_server_owned_and_frozen(self):
        q=self.quote();self.assertEqual(q['document']['trader_id'],self.tid);self.assertEqual(q['price'],10000)
        self.cfg['minimum_price']=99999
        self.assertEqual(self.transport.quotes[0]['price'],10000)
        bad={'choices':self.choices,'trader_id':str(uuid4()),'price':1}
        self.assertEqual(self.client.post('/trader/package_quote',json=bad).status_code,400)
        self.assertEqual(len(self.transport.quotes),1)
    def test_checkout_uses_owned_quote_hash_and_same_key(self):
        q=self.quote();body=self.checkout_body(q)
        r=self.client.post('/trader/package_checkout',json=body)
        self.assertEqual(r.status_code,200,r.json)
        call=self.transport.calls[-1][2]
        self.assertEqual(call['p_trader_id'],self.tid);self.assertEqual(call['p_checkout_key'],body['checkout_key'])
        self.assertEqual(call['p_quote_id'],q['id']);self.assertNotIn('price',call)
    def test_tampered_hash_foreign_quote_and_browser_amount_never_reach_rpc(self):
        q=self.quote();body=self.checkout_body(q)
        self.assertEqual(self.client.post('/trader/package_checkout',json={**body,'accepted_rules_hash':'wrong'}).status_code,409)
        self.assertEqual(self.client.post('/trader/package_checkout',json={**body,'amount':1}).status_code,400)
        self.tid=str(uuid4())
        self.assertEqual(self.client.post('/trader/package_checkout',json=body).status_code,409)
        self.assertFalse(any('/rpc/np_package_checkout' in c[1] for c in self.transport.calls))
    def test_paused_new_order_rejected_but_accepted_purchase_can_recover(self):
        q=self.quote();body=self.checkout_body(q);self.flags['features']['challenge_sales']=False
        self.assertEqual(self.client.post('/trader/package_checkout',json=body).status_code,409)
        self.transport.quotes[0]['consumed_purchase_id']=q['id']
        self.assertEqual(self.client.post('/trader/package_checkout',json=body).status_code,200)
    def test_unconfirmed_timeout_never_claims_no_purchase_or_retries_automatically(self):
        q=self.quote();body=self.checkout_body(q);self.transport.timeout=True
        r=self.client.post('/trader/package_checkout',json=body)
        self.assertEqual(r.status_code,503);self.assertEqual(r.json['purchase_status'],'unconfirmed')
        self.assertNotIn('operation_started',r.json)
        self.assertEqual(sum('/rpc/np_package_checkout' in c[1] for c in self.transport.calls),1)
    def test_history_is_owned_paginated_and_available_after_sales_pause(self):
        q=self.quote();self.cfg['enabled']=False
        self.transport.contracts=[dict(id=q['document']['contract_id'],document=q['document'],rules_hash=q['rules_hash'])]
        r=self.client.get('/trader/package_contracts?page=2&trader_id=someone-else')
        self.assertEqual(r.status_code,200,r.json);self.assertEqual(len(r.json['contracts']),1)
        self.assertEqual(self.transport.calls[-1][2]['trader_id'],'eq.'+self.tid)
        self.assertEqual(self.transport.calls[-1][2]['offset'],'20')
        self.assertEqual(self.client.get('/trader/package_contracts?page=0').status_code,400)
    def test_non_https_credentials_in_proof_and_malformed_ids_rejected(self):
        q=self.quote();body=self.checkout_body(q)
        for changes in [dict(payment_proof_url='http://proof.invalid'),dict(payment_proof_url='https://user:password@proof.invalid'),dict(quote_id='not-uuid')]:
            self.assertEqual(self.client.post('/trader/package_checkout',json={**body,**changes}).status_code,400)

if __name__=='__main__':unittest.main()
