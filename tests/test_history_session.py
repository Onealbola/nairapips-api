import ast,pathlib,unittest,time
from types import SimpleNamespace
from unittest.mock import Mock,patch
from decimal import Decimal
ROOT=pathlib.Path(__file__).resolve().parents[1]
class SessionTests(unittest.TestCase):
 def setUp(self):
  tree=ast.parse((ROOT/'drawdown-release/DD_POLICE_STATIC_TRAILING_V1_20261107.py').read_text());self.fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='authenticate')
  self.mt5=Mock();self.mt5.login.return_value=True;self.mt5.terminal_info.return_value=SimpleNamespace(connected=True)
  self.info=SimpleNamespace(login=123,server='Trial9',balance=4702014.48,equity=4702014.48,profit=0,credit=0)
  self.mt5.account_info.return_value=self.info
  self.ns=dict(norm=lambda x:str(x or '').strip(),time=SimpleNamespace(monotonic=lambda:1000,sleep=lambda n:None),auth_quarantine_until={},auth_fail_count={},row_master=lambda r:'master',pool_master=lambda l:'master',row_investor=lambda r:'investor',pool_investor=lambda l:'investor',mt5=self.mt5,verified_live_info=lambda *args:self.info,log=Mock(),now_iso=lambda:'2026-11-12T16:00:00+00:00',AUTH_QUARANTINE_BASE_SECONDS=15,AUTH_QUARANTINE_MAX_SECONDS=120)
  exec(compile(ast.Module(body=[self.fn],type_ignores=[]),'<worker-auth>','exec'),self.ns)
 def call(self,master=False):return self.ns['authenticate']({'mt5_login':'123','mt5_server':'Trial9'},master)
 def test_missing_investor_history_falls_through_to_valid_master(self):
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(5)),patch('plan_risk_controls.broker_daily_realised',side_effect=[ValueError('empty'),ValueError('empty'),ValueError('empty'),Decimal('-107795.68')]):
   result=self.call()
  self.assertEqual(result['source'],'row-master-fallback');self.assertEqual([c.kwargs['password'] for c in self.mt5.login.call_args_list],['investor','master'])
 def test_history_can_finish_loading_without_password_switch(self):
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(5)),patch('plan_risk_controls.broker_daily_realised',side_effect=[ValueError('empty'),Decimal(0)]):result=self.call()
  self.assertEqual(result['source'],'row-investor');self.mt5.login.assert_called_once()
 def test_cached_disconnected_account_and_wrong_server_are_rejected(self):
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(0)):
   self.mt5.terminal_info.return_value=SimpleNamespace(connected=False);self.assertIsNone(self.call())
   self.ns['auth_quarantine_until'].clear();self.mt5.terminal_info.return_value=SimpleNamespace(connected=True);self.info.server='different';self.assertIsNone(self.call())
 def test_cached_zero_equity_is_rejected_but_genuine_total_loss_allows_emergency_auth(self):
  self.info.equity=0
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(5)),patch('plan_risk_controls.broker_daily_realised') as history:
   self.assertIsNone(self.call(True));history.assert_not_called()
   self.info.profit=-self.info.balance
   result=self.call(True);self.assertEqual(result['source'],'row-master');history.assert_not_called()
 def test_missing_history_preserves_exact_connected_session_for_overall_only(self):
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(5)),patch('plan_risk_controls.broker_daily_realised',side_effect=ValueError('incomplete')):
   result=self.call()
  self.assertTrue(result['daily_history_unavailable']);self.assertEqual(result['info'].login,123)
 def test_preferred_history_credential_is_reauthenticated_and_rechecked(self):
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(5)),patch('plan_risk_controls.broker_daily_realised',side_effect=[ValueError('empty'),ValueError('empty'),ValueError('empty'),Decimal(-1),Decimal(-2)]) as history:
   self.call();self.ns['row_master']=lambda r:'changed-master';result=self.call()
  self.assertEqual(result['source'],'row-master-fallback');self.assertEqual(self.mt5.login.call_args.kwargs['password'],'changed-master');self.assertEqual(history.call_count,5)
 def test_nonfinite_financial_snapshots_are_never_used(self):
  self.info.equity=float('nan')
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(0)):self.assertIsNone(self.call())
 def test_valid_dd_coverage_is_marked_only_after_daily_journal_passes(self):
  tree=ast.parse((ROOT/'drawdown-release/DD_POLICE_STATIC_TRAILING_V1_20261107.py').read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='inspect');source=ast.unparse(fn)
  self.assertGreater(source.index('verified.add(login)'),source.index('_equity_dd_journal.assess('))
  self.assertGreater(source.index('last_valid_dd_read[login] ='),source.index('_equity_dd_journal.assess('))

class SnapshotConsistencyTests(unittest.TestCase):
 def test_closing_trade_is_not_counted_as_both_old_floating_and_new_realised_loss(self):
  tree=ast.parse((ROOT/'drawdown-release/DD_POLICE_STATIC_TRAILING_V1_20261107.py').read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='inspect')
  before=SimpleNamespace(balance=5000000,equity=4850000)
  after=SimpleNamespace(login=123,server='Trial9',balance=4850000,equity=4850000,profit=0,credit=0)
  journal=Mock();terminal=Mock();terminal.account_info.return_value=after;terminal.terminal_info.return_value=SimpleNamespace(connected=True)
  ns=dict(_package_dd_journal=None,_equity_dd_journal=journal,_daily_probe_ready=True,norm=lambda v:str(v or '').strip(),os=__import__('os'),LIVE_STATE_KEY='configured',log=Mock(),dd_rule=lambda row:(True,20,'test'),first_attempted=set(),last_attempted_at={},time=SimpleNamespace(monotonic=lambda:1000),authenticate=lambda row:{'info':before},verified=set(),last_ok={},starting_capital=lambda *args:5000000,invalid_live_capital={},position_evidence=lambda:[],now_iso=lambda:'2026-11-12T16:00:00+00:00',mt5=terminal,AUTH_RETRY_SECONDS=2)
  exec(compile(ast.Module(body=[fn],type_ignores=[]),'<worker-inspect>','exec'),ns)
  with patch('plan_risk_controls.daily_percent',return_value=Decimal(5)),patch('plan_risk_controls.broker_daily_realised',return_value=Decimal(-150000)):
   result=ns['inspect']({'mt5_login':'123','mt5_server':'Trial9'})
  self.assertEqual(result,2);journal.assess.assert_not_called();self.assertEqual(ns['verified'],set())

class SessionDeliveryTests(unittest.TestCase):
 def test_exact_worker_patch_is_backed_up_and_idempotent(self):
  import importlib.util,tempfile
  spec=importlib.util.spec_from_file_location('session_patch',ROOT/'dd-session-history-release/patch_session.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  fixed=(ROOT/'drawdown-release/DD_POLICE_STATIC_TRAILING_V1_20261107.py').read_text();p=m.PARTS
  coverage='    # '+m.MARKER+': coverage follows usable rule evidence.\n'+p['old_verified']+p['old_coverage']+p['anchor']
  original=fixed.replace(p['new_auth'],p['old_auth']).replace(coverage,p['anchor']).replace('    info=a["info"]\n',p['old_verified']+'    info=a["info"]\n').replace('    if daily_enabled or not _daily_probe_ready:\n',p['old_coverage']+'    if daily_enabled or not _daily_probe_ready:\n').replace(p['new_daily'],p['old_daily'])
  self.assertEqual(m.patched(original),fixed)
  with tempfile.TemporaryDirectory() as d:
   path=pathlib.Path(d)/'worker.py';helper=pathlib.Path(d)/'plan_risk_controls.py';module=(ROOT/'plan_risk_controls.py').read_text();helper.write_text(module.replace(m.HELPERS['new_helper'],m.HELPERS['old_helper']));path.write_bytes(original.replace('\n','\r\n').encode());before=path.read_bytes();m.install(path);after=path.read_bytes();m.install(path);self.assertEqual(path.read_bytes(),after);self.assertEqual(helper.read_text(),module);self.assertIn(before,[x.read_bytes() for x in pathlib.Path(d).glob('*.bak')])
  script=(ROOT/'website-downloads/nairapips-dd-history-session.ps1').read_text();self.assertIn((ROOT/'dd-session-history-release/patch_session.py').read_text(),script)
