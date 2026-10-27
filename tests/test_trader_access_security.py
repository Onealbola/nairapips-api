import ipaddress,unittest,uuid
from unittest.mock import Mock
from flask import Flask,jsonify
from trader_access_security import client_ip,register_trader_access_security,TRUSTED_DEFAULT
NETWORKS=[ipaddress.ip_network(x) for x in TRUSTED_DEFAULT.split(',')]
class IPTests(unittest.TestCase):
 def test_direct_client_cannot_spoof_forwarded_banned_ip(self):self.assertEqual(client_ip('8.8.8.8','1.1.1.1',NETWORKS),('8.8.8.8','direct_peer'))
 def test_proxy_chain_uses_nearest_untrusted_hop_not_forged_first_value(self):self.assertEqual(client_ip('10.0.0.1','1.1.1.1, 8.8.8.8',NETWORKS),('8.8.8.8','trusted_proxy_chain'))
 def test_private_missing_and_invalid_addresses_never_become_shared_ip_flags(self):
  for peer,forwarded in [('10.0.0.1',''),('10.0.0.1','bad'),('10.0.0.1','192.168.1.1'),('bad','8.8.8.8')]:self.assertEqual(client_ip(peer,forwarded,NETWORKS),(None,None))
 def test_ipv6_normalization(self):self.assertEqual(client_ip('::ffff:8.8.8.8','',NETWORKS)[0],'8.8.8.8')
class AccessTests(unittest.TestCase):
 def setUp(self):
  self.app=Flask(__name__);self.http=Mock();self.role='super_admin';self.authorized=True;self.owner=str(uuid.uuid4());self.audit=Mock();self.success=True;self.token='server-signed-token'
  @self.app.post('/login_trader')
  @self.app.post('/register_trader')
  def login():return jsonify(success=self.success,data={'id':'client-supplied-id-is-not-trusted','auth_token':self.token})
  @self.app.post('/admin_view_trader_token')
  def view():return jsonify(success=True,data={'auth_token':self.token})
  register_trader_access_security(self.app,dict(requests=self.http,SUPABASE_URL='https://db',SUPABASE_KEY='key',_trader_id_from_auth_token=lambda t:self.owner if t=='server-signed-token' else '',_require_admin=lambda:({'role':self.role},None) if self.authorized else (None,(jsonify(error='Unauthorized'),401)),_audit_safe=self.audit))
  self.client=self.app.test_client();self.store=self.app._np_security_log;self.store.put=Mock()
 def test_successful_login_records_server_verified_identity_not_request_identity(self):
  r=self.client.post('/login_trader',json={'trader_id':'victim','password':'never store'},headers={'X-Forwarded-For':'1.1.1.1,8.8.8.8','User-Agent':'Browser'},environ_overrides={'REMOTE_ADDR':'10.0.0.1'})
  self.assertEqual(r.status_code,200);record=self.store.put.call_args.args[0];self.assertEqual(record['trader_id'],self.owner);self.assertEqual(record['ip_address'],'8.8.8.8');self.assertEqual(record['user_agent'],'Browser');self.assertNotIn('password',record);self.assertNotIn('token',record)
 def test_failed_login_and_admin_previews_do_not_poison_trader_history(self):
  self.success=False;self.client.post('/login_trader');self.success=True;self.client.post('/admin_view_trader_token');self.store.put.assert_not_called()
 def test_public_and_unprivileged_staff_cannot_read_search_or_status(self):
  for path in ['/admin/trader_access/status','/admin/trader_access/alerts','/admin/trader_access/search?q=ab','/admin/trader_access/review?trader_id='+self.owner]:
   self.authorized=False;self.assertEqual(self.client.get(path).status_code,401)
   self.authorized=True;self.role='marketer';self.assertEqual(self.client.get(path).status_code,403)
  self.http.get.assert_not_called();self.http.post.assert_not_called()
 def test_review_is_private_read_only_and_requests_no_automatic_ban(self):
  self.http.post.return_value.json.return_value={'events':[],'matches':[],'automatic_ban':False}
  r=self.client.get('/admin/trader_access/review?trader_id='+self.owner+'&days=30');self.assertEqual(r.status_code,200);self.assertIn('no-store',r.headers['Cache-Control']);self.assertFalse(r.json['data']['automatic_ban']);self.http.patch.assert_not_called();self.http.delete.assert_not_called();self.audit.assert_called_once()
 def test_setup_failure_is_reported_without_fake_empty_history(self):
  self.http.get.side_effect=RuntimeError('not installed');r=self.client.get('/admin/trader_access/status');self.assertTrue(r.json['setup_required']);self.assertFalse(r.json['ready'])

 def test_registration_and_forged_response_token(self):
  self.client.post('/register_trader',environ_overrides={'REMOTE_ADDR':'8.8.8.8'});self.assertEqual(self.store.put.call_args.args[0]['event_type'],'registration')
  self.store.put.reset_mock();self.token='invalid';self.client.post('/login_trader',environ_overrides={'REMOTE_ADDR':'8.8.8.8'});self.store.put.assert_not_called()
 def test_automatic_review_list_never_restricts_anyone(self):
  self.http.post.return_value.json.return_value={'alerts':[],'total':0,'automatic_ban':False}
  r=self.client.get('/admin/trader_access/alerts?days=30');self.assertEqual(r.status_code,200);self.assertFalse(r.json['data']['automatic_ban']);self.http.patch.assert_not_called();self.http.delete.assert_not_called()
