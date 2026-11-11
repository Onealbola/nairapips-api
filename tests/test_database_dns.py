import json,socket,subprocess,time,unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from database_dns import DatabaseDNSGuard,install_database_dns_guard

class DatabaseDNSTests(unittest.TestCase):
 def guard(self,ttl=60):
  self.original=Mock(return_value=['original resolver'])
  self.runner=Mock(return_value=SimpleNamespace(stdout=json.dumps({'rows':[[2,1,6,'',['127.0.0.1',443]],[10,1,6,'',['::1',443,0,0]]]})))
  return DatabaseDNSGuard('database.example',self.original,timeout=.2,ttl=ttl,runner=self.runner)
 def test_unrelated_hosts_use_the_original_resolver(self):
  g=self.guard();self.assertEqual(g('email.example',465,0,1),['original resolver']);self.runner.assert_not_called()
  self.original.assert_called_once_with('email.example',465,0,1,0,0)
 def test_resolution_preserves_addresses_and_socket_options_and_caches(self):
  g=self.guard();rows=g('database.example',443,type=socket.SOCK_STREAM)
  self.assertEqual(rows[0],(2,1,6,'',('127.0.0.1',443)))
  self.assertEqual(rows[1],(10,1,6,'',('::1',443,0,0)))
  self.assertEqual(g('database.example',443,type=socket.SOCK_STREAM),rows);self.assertEqual(self.runner.call_count,1)
  request=json.loads(self.runner.call_args.kwargs['input']);self.assertEqual(request['host'],'database.example.');self.assertEqual(request['type'],1)
  self.assertLessEqual(self.runner.call_args.kwargs['timeout'],.2)
 def test_expired_addresses_are_not_reused_when_resolution_fails(self):
  g=self.guard(ttl=0);g('database.example',443)
  self.runner.side_effect=subprocess.TimeoutExpired('resolver',.2)
  with self.assertRaises(socket.gaierror):g('database.example',443)
 def test_timeout_is_retryable_and_does_not_strand_future_refreshes(self):
  g=self.guard();self.runner.side_effect=subprocess.TimeoutExpired('resolver',.2)
  with self.assertRaises(socket.gaierror):g('database.example',443)
  self.runner.side_effect=None
  self.assertTrue(g('database.example',443));self.assertEqual(self.runner.call_count,2)
 def test_contended_lookup_has_a_bounded_wait(self):
  g=self.guard();g.timeout=.02;g.lock.acquire();started=time.monotonic()
  try:
   with self.assertRaises(socket.gaierror):g('database.example',443)
   self.assertLess(time.monotonic()-started,.2)
  finally:g.lock.release()
 def test_resolver_errors_never_return_empty_or_invented_addresses(self):
  g=self.guard();self.runner.return_value.stdout=json.dumps({'errno':socket.EAI_NONAME,'error':'missing'})
  with self.assertRaises(socket.gaierror):g('database.example',443)
 def test_real_hung_child_is_terminated_then_the_next_lookup_recovers(self):
  g=DatabaseDNSGuard('localhost',socket.getaddrinfo,timeout=.15)
  with patch('database_dns._RESOLVE','import time; time.sleep(10)'):
   started=time.monotonic()
   with self.assertRaises(socket.gaierror):g('localhost',443,type=socket.SOCK_STREAM)
   self.assertLess(time.monotonic()-started,1)
  g.timeout=1
  self.assertTrue(g('localhost',443,type=socket.SOCK_STREAM))
 def test_installation_is_idempotent_and_keeps_original_hostname(self):
  original=socket.getaddrinfo
  try:
   g=install_database_dns_guard('https://database.example')
   self.assertIs(g,install_database_dns_guard('https://database.example'))
   self.assertIs(g.original,original)
  finally:socket.getaddrinfo=original
