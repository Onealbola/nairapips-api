import copy,unittest
from unittest.mock import Mock
from phase_handover_queue import retry_waiting

class HandoverTests(unittest.TestCase):
 def setUp(self):
  self.trader=dict(id='owner',current_account_id='source',challenge_state='phase2_waiting_mt5')
  self.source=dict(id='source',trader_id='owner',purchase_id='purchase',stage='phase1',account_status='archived_phase1',passed_at='2026-11-12T00:11:21Z')
  self.purchase=dict(id='purchase',trader_id='owner',trader_account_id='source')
  self.assign=Mock(return_value={'account':{'id':'next'}})
  self.clean=Mock(return_value=True)
 def read(self,table,params):return [self.trader] if table=='traders' else [self.source] if table=='trader_accounts' else [self.purchase]
 def run_queue(self):return retry_waiting(self.read,self.assign,self.clean)
 def test_exact_recorded_phase1_pass_uses_existing_phase2_authority(self):
  result=self.run_queue();self.assertEqual(result['assigned'],1)
  self.assign.assert_called_once_with(self.trader,'phase2',self.purchase,self.source,'lifecycle_progression')
 def test_terminal_foreign_wrong_stage_and_package_sources_never_enter_assignment(self):
  original=copy.deepcopy(self.source)
  for patch in ({'breached_at':'time'},{'account_status':'breached_archived'},{'passed_at':None},{'trader_id':'foreign'},{'id':'foreign'},{'stage':'funded'},{'package_contract_id':'separate-authority'}):
   self.source={**original,**patch};self.assign.reset_mock();self.run_queue();self.assign.assert_not_called()
 def test_purchase_pointer_changed_foreign_owner_and_legacy_are_not_reassigned(self):
  for field in ('id','trader_id','trader_account_id'):
   old=self.purchase[field];self.purchase[field]='other';self.run_queue();self.assign.assert_not_called();self.purchase[field]=old
  self.clean.return_value=False;self.run_queue();self.assign.assert_not_called()
 def test_existing_successor_response_remains_consumed(self):
  self.assign.return_value={'already_fulfilled':True,'account':{'id':'existing'}}
  result=self.run_queue();self.assertEqual(result['assigned'],0);self.assertEqual(result['already_fulfilled'],1)
 def test_waiting_inventory_does_not_make_or_fake_an_account(self):
  self.assign.return_value=None;result=self.run_queue();self.assertEqual(result['waiting'],1);self.assertEqual(result['assigned'],0)
 def test_phase2_pass_uses_funded_pool_via_existing_authority(self):
  self.trader['challenge_state']='funded_waiting_mt5';self.source.update(stage='phase2',account_status='archived_phase2')
  self.run_queue();self.assertEqual(self.assign.call_args.args[1],'funded')
 def test_independent_item_failure_is_counted(self):
  self.assign.side_effect=RuntimeError('verification unavailable');result=self.run_queue();self.assertEqual(result['errors'],1);self.assertEqual(result['assigned'],0)
if __name__=='__main__':unittest.main()

class StartupOrderingTests(unittest.TestCase):
 def test_assignment_workers_start_once_after_all_final_guards(self):
  import ast
  from pathlib import Path
  tree=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes())
  final_definition=max(n.end_lineno for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)))
  for name in ('_np_start_lifecycle_worker_v40','_np_start_reset_autofire_v43'):
   calls=[n for n in tree.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id==name]
   self.assertEqual(len(calls),1);self.assertGreater(calls[0].lineno,final_definition)
 def test_pending_handover_precedes_live_and_broad_scans_under_the_existing_lock(self):
  import ast
  from pathlib import Path
  from unittest.mock import patch
  node=[n for n in ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='_np_run_verified_lifecycle_cycle_v40'][-1]
  order=[];ns={'_np_v74_try_file_lock':lambda:True,'_np_v74_release_file_lock':lambda lock:order.append('unlock'),'_np_v214_rows':Mock(),'_np_auto_assign_waiting_stage':Mock(),'_np_is_clean_root_v43':Mock(),'_np_record_live_passes_v214':lambda:order.append('monitor') or {},'_np_retry_clean_pass_funded_v19':lambda limit:order.append('broad-passes') or {},'_np_v214_cycle_core':lambda trigger:order.append('legacy') or {}}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-priority-cycle','exec'),ns)
  with patch('phase_handover_queue.retry_waiting',side_effect=lambda *args:order.append('waiting') or {'assigned':1}):ns['_np_run_verified_lifecycle_cycle_v40']()
  self.assertEqual(order,['waiting','unlock','monitor','broad-passes','unlock','legacy'])
