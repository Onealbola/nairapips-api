"""The deployed trader client and authenticated announcement feed must agree."""
import ast,copy,unittest
from pathlib import Path
from flask import Flask,request,jsonify

class AnnouncementFeedTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__)
        self.rows=[];self.fail=False
        test=self
        class Query:
            def select(self,*args):return self
            def eq(self,*args):return self
            def order(self,*args,**kw):return self
            def limit(self,*args):return self
            def execute(self):
                if test.fail:raise TimeoutError('fixture outage')
                return type('Result',(),{'data':test.rows})()
        class DB:
            def table(self,name):
                test.assertEqual(name,'announcements');return Query()
        def auth(requested):
            return ('owner',None) if request.headers.get('Authorization')=='Bearer fixture-session' and requested=='owner' else ('','Trader authentication is required')
        env=dict(app=self.app,request=request,jsonify=jsonify,supabase=DB(),
            _authenticated_trader_id_for_request=auth,
            _get_trader_by_id=lambda tid:{'email':'owner@example.invalid'},
            _np_offer_merge_meta=lambda row:dict(row),_trader_safe_offer_row=lambda row:dict(row),
            _np_ok=lambda body:jsonify(body),
            _np_fail=lambda message,status:(jsonify(success=False,error=message),status))
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='trader_announcements')
        exec(compile(ast.Module(body=[copy.deepcopy(node)],type_ignores=[]),'announcement-fixture','exec'),env)
        self.client=self.app.test_client()
    def test_versioned_route_delivers_same_owned_and_public_notices(self):
        self.rows=[{'id':'public'}, {'id':'owned','target_trader_id':'owner'},
            {'id':'email','target_email':'owner@example.invalid'},
            {'id':'other','target_trader_id':'other'},
            {'id':'other-email','target_email':'other@example.invalid'},
            {'id':'hidden','show_on_dashboard':False}]
        for path in ('/trader_announcements','/trader_announcements_v159'):
            r=self.client.get(path+'?trader_id=owner',headers={'Authorization':'Bearer fixture-session'})
            self.assertEqual(r.status_code,200)
            self.assertEqual([x['id'] for x in r.json],['public','owned','email'])
    def test_missing_session_and_wrong_owner_are_rejected_on_both_routes(self):
        for path in ('/trader_announcements','/trader_announcements_v159'):
            self.assertEqual(self.client.get(path+'?trader_id=owner').status_code,401)
            self.assertEqual(self.client.get(path+'?trader_id=other',headers={'Authorization':'Bearer fixture-session'}).status_code,401)
    def test_failed_read_is_an_error_not_an_empty_success(self):
        self.fail=True
        r=self.client.get('/trader_announcements_v159?trader_id=owner',headers={'Authorization':'Bearer fixture-session'})
        self.assertEqual(r.status_code,500);self.assertFalse(r.json['success'])
    def test_realtime_preflight_route_exists(self):
        r=self.client.options('/trader_announcements_v159')
        self.assertEqual(r.status_code,200)
