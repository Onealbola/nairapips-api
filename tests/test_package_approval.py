import unittest
import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4
from flask import Flask
from package_approval import PackageApprovalStore, register_package_approval
from package_checkout import PackageOrderUnconfirmed


class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.pid, self.cid, self.eid, self.aid = [str(uuid4()) for _ in range(4)]
        self.transport = Mock()
        self.orders = SimpleNamespace(base='https://fixture.invalid/rest/v1/', headers={}, transport=self.transport,
                                     read=Mock(return_value=[{'id': self.pid, 'package_contract_id': self.cid}]))
        self.store = PackageApprovalStore(self.orders)
        self.app = Flask(__name__)
        self.legacy = Mock(return_value=({'success': True, 'legacy': True}, 200))
        self.app.add_url_rule('/approve_purchase', 'approve_purchase', lambda: self.legacy(), methods=['POST', 'OPTIONS'])
        self.admin = {'id': 'fixture-admin'}
        self.auth_error = None
        register_package_approval(self.app, self.store, lambda: (self.admin, self.auth_error))
        self.client = self.app.test_client()

    def reply(self, value):
        return SimpleNamespace(ok=True, json=lambda: value)

    def receipts(self, waiting=False):
        self.transport.post.side_effect = [
            self.reply({'purchase_id': self.pid, 'approved': True, 'entitlement_id': self.eid}),
            self.reply({'entitlement_id': self.eid, 'account_id': None if waiting else self.aid, 'waiting': waiting})]

    def test_approval_routes_exact_purchase_to_atomic_ledger(self):
        self.receipts()
        r=self.client.post('/approve_purchase',json={'id':self.pid})
        self.assertEqual(r.status_code,200,r.json)
        self.assertFalse(self.legacy.called)
        self.assertEqual(self.transport.post.call_args_list[0].kwargs['json'],{'p_purchase_id':self.pid,'p_actor':'fixture-admin'})
        self.assertEqual(r.json['data']['account_id'],self.aid)

    def test_unavailable_verified_stock_is_truthful_waiting_not_fake_assignment(self):
        self.receipts(waiting=True)
        r=self.client.post('/approve_purchase',json={'id':self.pid})
        self.assertTrue(r.json['data']['waiting'])
        self.assertIsNone(r.json['data']['account_id'])

    def test_uncertain_delivery_is_not_automatically_retried(self):
        self.transport.post.side_effect=[self.reply({'purchase_id':self.pid,'approved':True,'entitlement_id':self.eid}),TimeoutError()]
        r=self.client.post('/approve_purchase',json={'id':self.pid})
        self.assertEqual(r.status_code,503)
        self.assertEqual(r.json['purchase_status'],'unconfirmed')
        self.assertEqual(self.transport.post.call_count,2)

    def test_legacy_purchase_keeps_existing_approval_path(self):
        self.orders.read.return_value=[{'id':self.pid,'package_contract_id':None}]
        r=self.client.post('/approve_purchase',json={'id':self.pid})
        self.assertTrue(r.json['legacy']);self.legacy.assert_called_once()
        self.transport.post.assert_not_called()

    def test_unauthenticated_and_manual_pool_bypass_rejected(self):
        self.auth_error=({'success':False},401)
        self.assertEqual(self.client.post('/approve_purchase',json={'id':self.pid}).status_code,401)
        self.orders.read.assert_not_called()
        self.auth_error=None
        self.assertEqual(self.client.post('/approve_purchase',json={'id':self.pid,'mt5_id':str(uuid4())}).status_code,409)
        self.transport.post.assert_not_called()

    def test_database_rejection_not_misreported_as_invalid_browser_input(self):
        self.transport.post.return_value=SimpleNamespace(ok=False,json=lambda:{'code':'P0001'})
        self.assertEqual(self.client.post('/approve_purchase',json={'id':self.pid}).status_code,409)

    def test_legacy_assignment_cannot_bypass_package_entitlements(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes())
        node=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_assign_mt5_to_trader'][-1]
        scope={}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'assignment_guard','exec'),scope)
        with self.assertRaisesRegex(RuntimeError,'immutable entitlement ledger'):
            scope['_assign_mt5_to_trader']({}, {}, 'phase1', purchase={'package_contract_id':self.cid})


if __name__=='__main__':unittest.main()
