import ast,importlib.util,pathlib,unittest
from types import SimpleNamespace
from datetime import datetime,timezone
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('daily_audit',ROOT/'dd-daily-history-release/audit_daily.py');audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
class AuditTests(unittest.TestCase):
 def test_report_distinguishes_initial_funding_from_post_assignment_ledger(self):
  assigned=datetime(2026,11,10,11,tzinfo=timezone.utc);observed=datetime(2026,11,12,16,tzinfo=timezone.utc)
  d=[SimpleNamespace(ticket=1,time=int(assigned.timestamp())-60,type=2,profit=5000000),SimpleNamespace(ticket=2,time=int(datetime(2026,11,11,20,tzinfo=timezone.utc).timestamp()),type=1,profit=-190189.84),SimpleNamespace(ticket=3,time=int(datetime(2026,11,12,7,tzinfo=timezone.utc).timestamp()),type=1,profit=-107795.68)]
  full=audit.summarise(d,observed,assigned);post=audit.summarise(d[1:],observed,assigned)
  self.assertEqual(full['balance_ledger_net'],'4702014.48');self.assertEqual(post['balance_ledger_net'],'-297985.52');self.assertEqual(full['today_trading_net'],'-107795.68');self.assertTrue(full['funding_sample'][0]['before_assignment'])
 def test_collector_sends_no_login_trading_or_write_commands(self):
  tree=ast.parse((ROOT/'dd-daily-history-release/audit_daily.py').read_text())
  calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Name) and n.func.value.id=='mt5'}
  self.assertEqual(calls,{'initialize','account_info','history_deals_get','shutdown'})
  script=(ROOT/'website-downloads/nairapips-dd-daily-audit.ps1').read_text();self.assertIn((ROOT/'dd-daily-history-release/audit_daily.py').read_text(),script)
  self.assertNotIn('Stop-Process',script);self.assertIn('$terminals.Count -ne 1',script)
