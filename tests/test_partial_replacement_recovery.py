import ast,re,unittest
from pathlib import Path
from unittest.mock import Mock,MagicMock
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class PartialTests(unittest.TestCase):
 def test_monitoring_failure_cannot_finalize_or_close_source(self):
  monitor=Mock(side_effect=RuntimeError('monitoring unavailable'));pointer=Mock();db=Mock()
  ns={'_np_v122_s':lambda v:str(v or ''),'now_iso':lambda:'now','_np_v122_done_marker':lambda *a:'receipt','_np_monitoring_registry_activate':monitor,'_np_update_trader_current_pointer_v59':pointer,'_np_renewal_db_v211':lambda:db}
  node=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v122_finalize');exec(compile(ast.Module(body=[node],type_ignores=[]),'finalize','exec'),ns)
  with self.assertRaises(RuntimeError):ns['_np_v122_finalize']({'id':'p','trader_id':'t'},{'id':'s'},{'id':'c','mt5_login':'123'})
  pointer.assert_not_called();db.table.assert_not_called()
 def test_startup_completes_partial_child_but_skips_receipted_delivery(self):
  source={'id':'s','archive_reason':'paid:p'};child={'id':'c','mt5_login':'123'};p={'id':'p','trader_id':'t','trader_account_id':'s'}
  query=MagicMock();query.select.return_value=query;query.eq.return_value=query;query.order.return_value=query;query.limit.return_value=query;query.execute.return_value.data=[p]
  start=Mock(return_value={'state':'queued'})
  ns={'_np_renewal_db_v211':lambda:Mock(table=lambda name:query),'now_iso':lambda:'now','_np_v122_s':lambda v:str(v or ''),'_np_v122_load_source':lambda p:(source,'ok'),'_np_v122_paid_marker':lambda pid:'paid:'+pid,'_np_v122_existing_delivery':lambda p,s:child,'_np_v122_done_marker':lambda *args:'done:p:c:123','_np_v122_start':start}
  node=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v122_repair_outstanding_current_paid');exec(compile(ast.Module(body=[node],type_ignores=[]),'repair','exec'),ns)
  result=ns['_np_v122_repair_outstanding_current_paid']();self.assertEqual(result['queued'],1);start.assert_called_once_with('p')
  start.reset_mock();source['archive_reason']+=' done:p:c:123';result=ns['_np_v122_repair_outstanding_current_paid']();self.assertEqual(result['queued'],0);start.assert_not_called()
