import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4
from flask import Flask
from package_fulfillment import PackageFulfillmentStore, register_package_fulfillment
from package_checkout import PackageOrderUnconfirmed


class FulfillmentTests(unittest.TestCase):
    def setUp(self):
        self.tid,self.cid,self.aid,self.eid=[str(uuid4()) for _ in range(4)]
        self.orders=SimpleNamespace(read=Mock())
        self.approval=SimpleNamespace(orders=self.orders,rpc=Mock())
        self.store=PackageFulfillmentStore(self.approval)
        self.app=Flask(__name__)
        self.admin_error=None
        register_package_fulfillment(self.app,self.store,lambda:({'id':'admin'},self.admin_error),lambda _:(self.tid,None))
        self.client=self.app.test_client()

    def test_reset_uses_signed_owner_and_exact_reserved_entitlement(self):
        self.orders.read.return_value=[{'id':self.aid,'package_contract_id':self.cid,'stage':'funded'}]
        self.approval.rpc.side_effect=[{'id':self.eid},{'entitlement_id':self.eid,'waiting':True}]
        r=self.client.post('/trader/package_included_reset',json={'account_id':self.aid})
        self.assertEqual(r.status_code,200,r.json)
        self.assertEqual(self.orders.read.call_args.args[1]['trader_id'],'eq.'+self.tid)
        self.assertEqual(self.approval.rpc.call_args_list[0].args[1],{'p_contract_id':self.cid,'p_source_account_id':self.aid,'p_kind':'funded_reset'})
        self.assertEqual(self.approval.rpc.call_args_list[1].args[1],{'p_entitlement_id':self.eid})

    def test_foreign_or_legacy_account_cannot_consume_allowance(self):
        self.orders.read.return_value=[]
        self.assertEqual(self.client.post('/trader/package_included_reset',json={'account_id':self.aid}).status_code,409)
        self.approval.rpc.assert_not_called()

    def test_browser_cannot_supply_contract_stage_or_budget(self):
        r=self.client.post('/trader/package_included_reset',json={'account_id':self.aid,'funded_resets':99})
        self.assertEqual(r.status_code,400);self.orders.read.assert_not_called()

    def test_admin_retry_is_authenticated_and_pagination_bounded(self):
        self.admin_error=({'success':False},401)
        self.assertEqual(self.client.post('/admin/package_fulfillment/retry',json={'entitlement_id':self.eid}).status_code,401)
        self.approval.rpc.assert_not_called()
        self.admin_error=None;self.orders.read.return_value=[{'id':self.eid}]*21
        r=self.client.get('/admin/package_fulfillment?page=2')
        self.assertEqual(len(r.json['entitlements']),20);self.assertTrue(r.json['has_more'])
        self.assertEqual(self.orders.read.call_args.args[1]['offset'],'20')

    def test_cycle_stops_on_uncertain_mutation_without_retrying(self):
        self.orders.read.return_value=[{'id':self.eid},{'id':str(uuid4())}]
        self.approval.rpc.side_effect=PackageOrderUnconfirmed('fixture')
        result=self.store.cycle()
        self.assertEqual(self.approval.rpc.call_count,1)
        self.assertEqual(result[0]['state'],'unconfirmed')


if __name__=='__main__':unittest.main()
