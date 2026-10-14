import ast,base64,hashlib,hmac,time,unittest
from pathlib import Path
from flask import Flask,request,jsonify

class AdminProbeTests(unittest.TestCase):
    def setUp(self):
        app=Flask(__name__)
        names={'_make_admin_auth_token','_admin_auth_from_token','_request_admin_auth','_require_admin','_np_admin_auth_check_v234'}
        nodes=[n for n in ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name in names]
        self.env=dict(app=app,request=request,jsonify=jsonify,base64=base64,hashlib=hashlib,hmac=hmac,time=time,ADMIN_AUTH_TOKEN_TTL_SECONDS=60,_trader_auth_secret=lambda:b'private-fixture-secret',_np_fail=lambda message,status:(jsonify(success=False,error=message),status))
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'auth-probe-fixture','exec'),self.env)
        self.client=app.test_client()
    def test_valid_session_requires_no_database_and_returns_no_staff_details(self):
        token=self.env['_make_admin_auth_token']({'id':'private-fixture','role':'admin'})
        r=self.client.get('/admin/auth_check',headers={'Authorization':'Bearer '+token})
        self.assertEqual(r.status_code,200);self.assertEqual(r.json,{'success':True})
        self.assertEqual(r.headers['Cache-Control'],'no-store')
    def test_missing_and_forged_tokens_fail_closed(self):
        for token in ('','invalid'):
            self.assertEqual(self.client.get('/admin/auth_check',headers={'Authorization':'Bearer '+token}).status_code,401)
    def test_probe_has_reserved_authentication_capacity(self):
        from request_admission import RequestAdmissionMiddleware
        self.assertIn('/admin/auth_check',RequestAdmissionMiddleware.PRIORITY_PATHS)
