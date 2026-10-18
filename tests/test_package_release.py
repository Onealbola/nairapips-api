import unittest
from unittest.mock import Mock
from types import SimpleNamespace
from flask import Flask,jsonify
from package_release import register_package_production
from package_customer import register_package_customer

class ReleaseTests(unittest.TestCase):
    def test_actual_app_registration_keeps_startup_free_of_external_work(self):
        from pathlib import Path
        app=Flask('actual-registration')
        app.add_url_rule('/approve_challenge_purchase','approve_purchase',lambda:jsonify(success=True),methods=['POST'])
        transport=Mock()
        source=(Path(__file__).resolve().parents[1]/'app.py').read_text()
        tail=source[source.index('# V224: private owner test page'):]
        scope={'app':app,'requests':transport,'SUPABASE_URL':'https://fixture.invalid',
               'SUPABASE_SERVICE_ROLE_KEY':'private-server-fixture','SUPABASE_KEY':'fixture','_require_admin':lambda:({'id':'owner','role':'owner'},None),
               '_authenticated_trader_id_for_request':lambda _:('', 'Authentication required'),
               '_audit_safe':lambda *args:None,'np_invalidate':lambda *args:None}
        exec(compile(tail,'actual-package-registration','exec'),scope)
        self.assertEqual(transport.mock_calls,[])
        client=app.test_client()
        self.assertEqual(client.get('/build-package').status_code,200)
        self.assertEqual(client.post('/trader/package_price',json={}).status_code,401)
        self.assertEqual(transport.mock_calls,[])

    def test_registration_has_no_io_and_runtime_blockers_cannot_be_enabled_by_checkbox(self):
        app=Flask(__name__);app.add_url_rule('/approve_challenge_purchase','approve_purchase',lambda:jsonify(success=True),methods=['POST'])
        transport=Mock();transport.get.return_value=SimpleNamespace(ok=True,json=lambda:{'schema_version':'NP_PACKAGE_SCHEMA_1','ready':True})
        config=SimpleNamespace();controls=Mock();role={'role':'owner'}
        register_package_production(app,transport,'https://fixture.invalid','fixture',config,controls,lambda:(role,None),lambda _:('', 'Sign in required'),{})
        register_package_customer(app)
        self.assertEqual(transport.mock_calls,[])
        client=app.test_client()
        response=client.get('/admin/package_readiness')
        self.assertEqual(response.status_code,200)
        self.assertFalse(response.json['ready'])
        self.assertIn('Verified running monitoring and DD fleet',response.json['blockers'])
        self.assertIn('Payout verification',response.json['blockers'])
        transport.reset_mock()
        for path in ['/trader/package_quote','/trader/package_price','/trader/package_checkout']:
            self.assertEqual(client.post(path,json={}).status_code,401)
        self.assertEqual(transport.mock_calls,[])
        role['role']='admin'
        self.assertEqual(client.get('/admin/package_readiness').status_code,403)
        response=client.get('/build-package')
        self.assertEqual(response.status_code,200)
        self.assertIn("frame-ancestors 'none'",response.headers['Content-Security-Policy'])
        self.assertNotIn(b'fixture',response.data)
        self.assertNotIn(b'localStorage',response.data)
        self.assertEqual(client.get('/package-trader.js').headers['Cache-Control'],'no-store')
