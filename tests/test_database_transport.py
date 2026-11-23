import unittest,httpx
from database_transport import create_database_client
class DatabaseTransportTests(unittest.TestCase):
 def test_queries_keep_exact_key_filters_and_timeouts(self):
  for key in ('fixture-public','fixture-service'):
   requests=[]
   def handler(request):requests.append(request);return httpx.Response(200,json=[{'id':'fixture'}])
   c=create_database_client('https://database.invalid',key,transport=httpx.MockTransport(handler))
   try:
    rows=c.table('traders').select('id').eq('id','fixture').limit(1).execute().data
    self.assertEqual(rows,[{'id':'fixture'}]);r=requests[0]
    self.assertEqual(r.headers['apikey'],key);self.assertEqual(r.headers['Authorization'],'Bearer '+key)
    self.assertEqual(r.url.params['id'],'eq.fixture');self.assertEqual(r.extensions['timeout'],{'connect':2,'read':8,'write':8,'pool':2})
   finally:c.postgrest.session.close()
 def test_rpc_keeps_payload_and_role_without_retries(self):
  calls=[]
  def handler(r):calls.append(r);return httpx.Response(503,json={'message':'fixture unavailable'})
  c=create_database_client('https://database.invalid','fixture',transport=httpx.MockTransport(handler))
  try:
   with self.assertRaises(Exception):c.rpc('fixture_read',{'owner_id':'one'}).execute()
   self.assertEqual(len(calls),1);self.assertEqual(calls[0].method,'POST');self.assertIn(b'owner_id',calls[0].content)
  finally:c.postgrest.session.close()
 def test_real_transport_does_not_enable_http2(self):
  c=create_database_client('https://database.invalid','fixture')
  try:self.assertFalse(c.postgrest.session._transport._pool._http2)
  finally:c.postgrest.session.close()
