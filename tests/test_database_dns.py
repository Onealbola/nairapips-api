import socket,time,unittest
from threading import Event
from unittest.mock import Mock
from database_dns import DatabaseDNSGuard,install_database_dns_guard,MAX_PENDING_LOOKUPS
ROWS=[(2,1,6,'',('127.0.0.1',443)),(10,1,6,'',('::1',443,0,0))]
class DatabaseDNSTests(unittest.TestCase):
 def guard(self,ttl=60):
  self.original=Mock(return_value=ROWS)
  return DatabaseDNSGuard('database.example',self.original,timeout=.2,ttl=ttl)
 def test_unrelated_hosts_use_the_original_resolver(self):
  g=self.guard();self.assertEqual(g('email.example',465,0,1),ROWS)
  self.original.assert_called_once_with('email.example',465,0,1,0,0)
 def test_resolution_preserves_addresses_and_socket_options_and_caches(self):
  g=self.guard();rows=g('database.example',443,type=socket.SOCK_STREAM)
  self.assertEqual(rows,ROWS);self.assertEqual(g('database.example',443,type=socket.SOCK_STREAM),rows)
  self.original.assert_called_once_with('database.example.',443,0,socket.SOCK_STREAM,0,0)
 def test_expired_addresses_are_not_reused_when_resolution_fails(self):
  g=self.guard(ttl=0);g('database.example',443)
  self.original.side_effect=socket.gaierror(socket.EAI_AGAIN,'fixture')
  with self.assertRaises(socket.gaierror):g('database.example',443)
 def test_slow_lookup_is_shared_and_later_result_recovers_without_new_threads(self):
  release=Event();started=Event()
  def resolve(*args):started.set();release.wait(3);return ROWS
  g=DatabaseDNSGuard('database.example',resolve,timeout=.03)
  try:
   for _ in range(3):
    begin=time.monotonic()
    with self.assertRaises(socket.gaierror):g('database.example',443)
    self.assertLess(time.monotonic()-begin,.2)
   self.assertEqual(g.lookups,1);self.assertEqual(len(g.pending),1)
  finally:release.set()
  deadline=time.monotonic()+1
  while g.pending and time.monotonic()<deadline:time.sleep(.005)
  self.assertEqual(g('database.example',443),ROWS);self.assertEqual(g.lookups,1)
 def test_contended_lookup_has_a_bounded_wait(self):
  g=self.guard();g.timeout=.02;g.lock.acquire();started=time.monotonic()
  try:
   with self.assertRaises(socket.gaierror):g('database.example',443)
   self.assertLess(time.monotonic()-started,.2)
  finally:g.lock.release()
 def test_failed_lookup_can_be_retried(self):
  g=self.guard();self.original.side_effect=socket.gaierror(socket.EAI_NONAME,'fixture')
  with self.assertRaises(socket.gaierror):g('database.example',443)
  self.original.side_effect=None;self.assertEqual(g('database.example',443),ROWS)
 def test_pending_pool_is_bounded_across_distinct_socket_options(self):
  release=Event();g=DatabaseDNSGuard('database.example',lambda *args:(release.wait(3) and ROWS),timeout=.01)
  try:
   for port in range(443,443+MAX_PENDING_LOOKUPS+2):
    with self.assertRaises(socket.gaierror):g('database.example',port)
   self.assertEqual(len(g.pending),MAX_PENDING_LOOKUPS)
  finally:release.set()
 def test_installation_is_idempotent_and_keeps_original_hostname(self):
  original=socket.getaddrinfo
  try:
   g=install_database_dns_guard('https://database.example')
   self.assertIs(g,install_database_dns_guard('https://database.example'))
   self.assertIs(g.original,original)
  finally:socket.getaddrinfo=original
