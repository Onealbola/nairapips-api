import ast,unittest
from pathlib import Path
from flask import Flask,request
class ActivityTests(unittest.TestCase):
 def setUp(self):
  self.app=Flask(__name__);self.calls=[];self.fail=False
  node=next(n for n in ast.parse(Path('app.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='np2_referral_activity');node.decorator_list=[]
  def rows(table,params):
   self.calls.append((table,params))
   if self.fail:raise OSError(11,'Resource temporarily unavailable')
   if table.endswith('registrations'):return [{'id':'r','referred_email':'foo@example.com'}]
   if table.endswith('commissions'):return [{'registration_id':'r','status':'approved','commission_amount':2000}]
   return [{'registration_id':'r'}]
  self.ns={'request':request,'_np_ok':lambda d:d,'_np_fail':lambda msg,status:({'success':False,'error':msg},status),'_np2_ref_authenticated_trader':lambda _:({'id':'t'},None),'_np2_ref_ensure_profile':lambda t:{'id':'profile','code':'CODE'},'_np_limited_cycle_guard':type('Guard',(),{'rows':staticmethod(rows)})(),'clean':lambda v:float(v or 0),'NP2_REFERRAL_RELEASE':'test'}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'isolated','exec'),self.ns)
 def call(self):
  with self.app.test_request_context('/np2_referral/activity'):return self.ns['np2_referral_activity']()
 def test_complete_scoped_ledger(self):
  d=self.call();self.assertEqual(d['registered_count'],1);self.assertEqual(d['approved_commission'],2000);self.assertEqual(len(self.calls),3)
  self.assertTrue(all(p['profile_id']=='eq.profile' for _,p in self.calls))
 def test_failure_is_not_zero_and_hides_internal_error(self):
  self.fail=True;d,status=self.call();self.assertEqual(status,503);self.assertNotIn('registered_count',d);self.assertNotIn('Errno',d['error'])
 def test_authentication_precedes_reads(self):
  self.ns['_np2_ref_authenticated_trader']=lambda _:(None,({'error':'Unauthorized'},401));self.assertEqual(self.call()[1],401);self.assertEqual(self.calls,[])
