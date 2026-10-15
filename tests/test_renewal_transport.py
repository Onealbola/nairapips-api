import ast,unittest,threading
from pathlib import Path
from contextvars import ContextVar
from unittest.mock import Mock
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class TransportTests(unittest.TestCase):
 def setUp(self):
  self.http=Mock();self.sdk=object();self.ctx=ContextVar('test',default=False)
  self.ns=dict(requests=self.http,supabase=self.sdk,_NP_RENEWAL_REST_V211=self.ctx,_np_v184_rest_headers=lambda:('https://db.example',{'apikey':'key'}))
  nodes=[n for n in TREE.body if isinstance(n,(ast.ClassDef,ast.FunctionDef)) and n.name in {'_NPRenewalRestQueryV211','_np_renewal_db_v211'}]
  exec(compile(ast.Module(body=nodes,type_ignores=[]),'transport','exec'),self.ns)
 def query(self):return self.ns['_NPRenewalRestQueryV211']('trader_accounts')
 def test_compare_and_set_filters_preserved(self):
  self.http.request.return_value.status_code=200;self.http.request.return_value.json.return_value=[]
  self.query().update({'archive_reason':'claimed'}).eq('id','a').eq('archive_reason','old').is_('paid_at','null').execute()
  kw=self.http.request.call_args.kwargs;self.assertEqual(kw['params'],{'id':'eq.a','archive_reason':'eq.old','paid_at':'is.null'});self.assertEqual(kw['timeout'],(2,7));self.assertEqual(self.http.request.call_args.args[0],'PATCH')
 def test_lost_insert_recovers_exact_uuid_without_second_post(self):
  self.http.request.side_effect=TimeoutError();q=self.query().insert({'trader_id':'t','mt5_login':'123','stage':'funded'})
  self.http.get.return_value.json.return_value=[dict(q.payload)]
  self.assertEqual(q.execute().data[0]['id'],q.payload['id']);self.assertEqual(self.http.request.call_count,1)
 def test_lost_insert_wrong_identity_fails_closed(self):
  self.http.request.side_effect=TimeoutError();q=self.query().insert({'trader_id':'t','mt5_login':'123'})
  self.http.get.return_value.json.return_value=[dict(q.payload,trader_id='other')]
  with self.assertRaises(RuntimeError):q.execute()
  self.assertEqual(self.http.request.call_count,1)
 def test_explicit_insert_rejection_preserves_optional_column_error(self):
  self.http.request.return_value.status_code=400
  self.http.request.return_value.text="Could not find the 'phase' column"
  with self.assertRaisesRegex(RuntimeError,"Could not find the 'phase' column"):
   self.query().insert({'phase':'phase1'}).execute()
  self.http.get.assert_not_called()
 def test_explicit_server_rejection_does_not_retry_insert(self):
  self.http.request.return_value.status_code=503
  self.http.request.return_value.text='database busy'
  with self.assertRaisesRegex(RuntimeError,'database busy'):
   self.query().insert({'trader_id':'t'}).execute()
  self.assertEqual(self.http.request.call_count,1);self.http.get.assert_not_called()
 def test_context_isolated_from_other_threads_and_default_paths(self):
  helper=self.ns['_np_renewal_db_v211'];self.assertIs(helper(),self.sdk)
  token=self.ctx.set(True)
  try:
   self.assertIsNot(helper(),self.sdk);seen=[];t=threading.Thread(target=lambda:seen.append(helper()));t.start();t.join();self.assertIs(seen[0],self.sdk)
  finally:self.ctx.reset(token)
  self.assertIs(helper(),self.sdk)

class RecoveryContextTests(unittest.TestCase):
 def test_startup_recovery_enters_and_restores_isolated_context(self):
  ctx=ContextVar('recovery',default=False);observed=[]
  ns={'_NP_RENEWAL_REST_V211':ctx,'_np_v122_repair_v212_core':lambda limit:observed.append((ctx.get(),limit)) or {'queued':1}}
  nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v122_repair_outstanding_current_paid']
  exec(compile(ast.Module(body=[nodes[-1]],type_ignores=[]),'startup','exec'),ns)
  self.assertEqual(ns['_np_v122_repair_outstanding_current_paid'](80),{'queued':1});self.assertEqual(observed,[(True,80)]);self.assertFalse(ctx.get())
