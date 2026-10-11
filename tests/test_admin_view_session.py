"""Exact Admin View cannot hang on the SDK or switch trader identity."""
import ast,copy,unittest,time
from pathlib import Path
from unittest.mock import Mock
from flask import Flask,request,jsonify
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class ViewTests(unittest.TestCase):
 def setUp(self):
  self.app=Flask(__name__);self.http=Mock();self.http.get.return_value.json.return_value=[{'id':'exact','email':'test@example.invalid'}]
  self.auth=Mock(return_value=True);self.token=Mock(return_value='signed');self.legacy=Mock(side_effect=AssertionError('Legacy lookup forbidden'))
  self.env=dict(request=request,requests=self.http,time=time,SUPABASE_URL='https://db.example',SUPABASE_KEY='key',_admin_view_request_ok=self.auth,_make_trader_auth_token=self.token,_latest_trader_for_lookup=self.legacy,ok=lambda data,message:jsonify(success=True,data=data),bad=lambda msg,code:(jsonify(success=False,error=str(msg)),code),_np_ok=lambda data:jsonify(data))
  funcs=[copy.deepcopy(n) for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'admin_view_trader_token','_np_admin_view_trader_v209'}]
  for n in funcs:n.decorator_list=[]
  exec(compile(ast.Module(body=funcs,type_ignores=[]),'view','exec'),self.env)
 def call(self):
  with self.app.test_request_context('/',method='POST',json={'trader_id':'exact','lookup':'other@example.invalid'}):return self.app.make_response(self.env['admin_view_trader_token']())
 def test_exact_session(self):
  r=self.call();self.assertEqual(r.status_code,200);self.token.assert_called_once_with('exact');self.legacy.assert_not_called();self.assertEqual(self.http.get.call_args.kwargs['timeout'],(2,4))
 def test_timeout_no_token(self):
  self.http.get.side_effect=TimeoutError();self.assertEqual(self.call().status_code,503);self.token.assert_not_called();self.legacy.assert_not_called()
 def test_missing_exact_trader_no_fallback(self):
  self.http.get.return_value.json.return_value=[];self.assertEqual(self.call().status_code,404);self.token.assert_not_called();self.legacy.assert_not_called()
 def test_wrong_identity_no_token(self):
  self.http.get.return_value.json.return_value=[{'id':'other'}];self.assertEqual(self.call().status_code,503);self.token.assert_not_called()
 def test_unauthorized_no_database_read(self):
  self.auth.return_value=False;self.assertEqual(self.call().status_code,401);self.http.get.assert_not_called();self.token.assert_not_called()
