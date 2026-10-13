"""Authenticate without the shared client; timeouts must never bypass credentials."""
import ast,copy,hmac,unittest
from contextvars import ContextVar
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from flask import Flask,request,jsonify
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class LoginTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__);self.http=Mock();self.response=Mock()
        self.rows=[dict(id='staff',username='member',password='correct',role='admin',status='active')]
        self.response.json.side_effect=lambda:self.rows
        self.http.get.return_value=self.response
        self.env=dict(request=request,jsonify=jsonify,hmac=hmac,requests=self.http,
            os=SimpleNamespace(getenv=lambda key,default=None:{'NAIRAPIPS_BOOTSTRAP_ADMIN_USERNAME':'root','NAIRAPIPS_BOOTSTRAP_ADMIN_PASSWORD':'root-secret'}.get(key,default)),
            SUPABASE_URL='https://database.example',SUPABASE_SERVICE_ROLE_KEY='server-key',SUPABASE_KEY='',
            _make_admin_auth_token=lambda staff:'signed-test-token',
            threading=SimpleNamespace(Thread=lambda **kw:SimpleNamespace(start=lambda:None)))
        funcs=[copy.deepcopy(n) for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'staff_login','_np_staff_login_rows_v207'}]
        for n in funcs:n.decorator_list=[]
        exec(compile(ast.Module(body=funcs,type_ignores=[]),'login','exec'),self.env)
    def login(self,user='member',password='correct'):
        with self.app.test_request_context('/staff_login',method='POST',json={'username':user,'password':password}):
            return self.app.make_response(self.env['staff_login']())
    def test_correct_staff_credentials_get_signed_token_without_password(self):
        r=self.login();self.assertEqual(r.status_code,200)
        self.assertEqual(r.get_json()['token'],'signed-test-token')
        self.assertNotIn('password',r.get_json()['staff'])
        call=self.http.get.call_args
        self.assertEqual(call.kwargs['timeout'],(2,4))
        self.assertNotIn('password',call.kwargs['params'])
    def test_wrong_password_does_not_authenticate(self):
        r=self.login(password='wrong');self.assertEqual(r.status_code,401)
        self.assertNotIn('token',r.get_json())
    def test_bootstrap_admin_does_not_wait_for_database(self):
        r=self.login('root','root-secret');self.assertEqual(r.status_code,200)
        self.http.get.assert_not_called()
    def test_database_timeout_does_not_authenticate_or_expose_exception(self):
        self.http.get.side_effect=TimeoutError('sensitive database details')
        r=self.login();self.assertEqual(r.status_code,503)
        self.assertNotIn('sensitive',r.get_data(as_text=True));self.assertNotIn('token',r.get_json())
    def test_inactive_staff_cannot_sign_in(self):
        self.rows[0]['status']='disabled';self.assertEqual(self.login().status_code,403)
    def test_background_login_audit_uses_bounded_transport_and_restores_context(self):
        capture={}
        def thread(**kw):
            capture.update(kw)
            return SimpleNamespace(start=lambda:None)
        context=ContextVar('staff-audit',default=False)
        db=Mock();seen=[]
        self.env.update(threading=SimpleNamespace(Thread=thread),
            _NP_RENEWAL_REST_V211=context,_np_renewal_db_v211=lambda:db,
            now_iso=lambda:'now',audit_log=lambda *args:seen.append(context.get()))
        self.assertEqual(self.login().status_code,200)
        capture['target'](*capture['args'])
        db.table.assert_called_once_with('admin_staff_members')
        self.assertEqual(seen,[True]);self.assertFalse(context.get())
    def test_background_audit_failure_does_not_leak_transport_context(self):
        capture={}
        def thread(**kw):
            capture.update(kw)
            return SimpleNamespace(start=lambda:None)
        context=ContextVar('staff-audit-failure',default=False)
        self.env.update(threading=SimpleNamespace(Thread=thread),
            _NP_RENEWAL_REST_V211=context,_np_renewal_db_v211=Mock(side_effect=TimeoutError()),
            audit_log=Mock(side_effect=TimeoutError()))
        self.assertEqual(self.login().status_code,200)
        capture['target'](*capture['args'])
        self.assertFalse(context.get())
    def test_email_fallback_and_ambiguous_accounts(self):
        self.response.json.side_effect=[[],[dict(self.rows[0],email='member@example.com')]]
        self.assertEqual(self.login('Member@example.com').status_code,200)
        self.assertEqual(self.http.get.call_args.kwargs['params']['email'],'eq.member@example.com')
        self.response.json.side_effect=lambda:self.rows*2
        self.assertEqual(self.login().status_code,401)
