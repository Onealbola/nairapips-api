import ast,copy,unittest
from pathlib import Path
from datetime import datetime,timezone,timedelta
from contextvars import ContextVar
from unittest.mock import Mock
from types import SimpleNamespace
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
def extract(names,ns):
 nodes=[]
 for name in names:nodes.append(copy.deepcopy([n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name==name][-1]))
 exec(compile(ast.Module(body=nodes,type_ignores=[]),'passes','exec'),ns)
class ProofTests(unittest.TestCase):
 def setUp(self):
  self.now=datetime.now(timezone.utc);self.a=dict(id='a',trader_id='t',mt5_login='123',stage='phase1',account_status='assigned_active',monitoring_enabled=True,purchase_id='p',account_size=500000,start_balance=500000,target_percent=15,dd_limit_percent=20,created_at=(self.now-timedelta(days=2)).isoformat())
  self.p=dict(id='p',trader_id='t',status='approved_active',phase1_target=10,max_drawdown=20)
  self.live=dict(trader_account_id='a',trader_id='t',mt5_login='123',stage='phase1',account_size=500000,balance=551000,equity=551000,dd_used_percent=0,observed_at=self.now.isoformat())
  self.ns={};extract(['_np_live_pass_proof_v214'],self.ns)
 def proof(self):return self.ns['_np_live_pass_proof_v214'](self.a,self.live,self.p,self.now)
 def test_purchase_frozen_ten_percent_overrides_old_account_fifteen(self):self.assertEqual(self.proof()['target'],10)
 def test_below_exact_target_does_not_pass(self):
  self.live['equity']=549999;self.assertIsNone(self.proof())
 def test_breach_and_historical_low_take_priority_over_high_profit(self):
  original=copy.deepcopy(self.a)
  for key,value in [('breached_at','now'),('breach_reason','DD'),('lowest_equity',399999),('worst_dd_used_percent',100)]:
   self.a=dict(original,**{key:value});self.assertIsNone(self.proof(),key)
 def test_wrong_owner_login_stage_or_account_rejected(self):
  original=copy.deepcopy(self.live)
  for key,value in [('trader_id','other'),('trader_account_id','other'),('mt5_login','other'),('stage','funded')]:
   self.live=dict(original,**{key:value});self.assertIsNone(self.proof(),key)
 def test_legacy_snapshot_owner_allowed_only_on_exact_account(self):
  self.live['trader_id']='a';self.assertIsNotNone(self.proof());self.live['trader_account_id']='other';self.assertIsNone(self.proof())
 def test_old_future_or_pre_assignment_observations_rejected(self):
  for delta in [timedelta(hours=-1),timedelta(minutes=2),timedelta(days=-3)]:
   self.live['observed_at']=(self.now+delta).isoformat();self.assertIsNone(self.proof())
 def test_nonfinite_or_missing_metrics_rejected(self):
  for value in [float('nan'),float('inf'),None]:
   self.live['equity']=value;self.assertIsNone(self.proof())
 def test_rejected_purchase_cannot_pass(self):
  self.p['status']='rejected';self.assertIsNone(self.proof())
 def test_phase_two_uses_its_own_target(self):
  self.a['stage']=self.live['stage']='phase2';self.p['phase2_target']=8;self.assertEqual(self.proof()['target'],8)
 def test_wrong_capital_snapshot_cannot_pass(self):
  self.live['account_size']=1000000;self.assertIsNone(self.proof())
 def test_funded_accounts_never_enter_pass_assignment(self):
  self.a['stage']=self.live['stage']='funded';self.assertIsNone(self.proof())
 def test_dd_rule_and_closed_balance_checked_even_with_target_equity(self):
  self.live['balance']=399999;self.assertIsNone(self.proof());self.live['balance']=551000;self.live['dd_used_percent']=100;self.assertIsNone(self.proof())
class AssignmentTests(unittest.TestCase):
 def setUp(self):
  self.ctx=ContextVar('phase',default=False);self.source=dict(id='s',trader_id='t',purchase_id='p',stage='phase1',account_status='archived_phase1',archive_reason='pass',created_at='2026-10-01T00:00:00+00:00')
  self.child=dict(id='c',trader_id='t',purchase_id='p',stage='funded',account_status='breached_archived',mt5_login='456',created_at='2026-10-02T00:00:00+00:00')
  self.core=Mock();self.monitor=Mock();self.pointer=Mock()
  self.ns={'_NP_RENEWAL_REST_V211':self.ctx,'_np_auto_assign_v214_core':self.core,'_np_v214_rows':lambda table,p: [self.source] if 'id' in p else [self.source,self.child], '_np_mh_blocked':lambda *a:False,'_next_stage_for_lifecycle':lambda *a:'funded','_np_parse_dt_safe':lambda v:datetime.fromisoformat(v) if v else None,'_np_monitoring_registry_activate':self.monitor,'_np_update_trader_current_pointer_v59':self.pointer}
  extract(['_np_auto_assign_waiting_stage'],self.ns)
 def call(self):return self.ns['_np_auto_assign_waiting_stage']({'id':'t'},'funded',{'id':'p','trader_id':'t'},self.source)
 def test_late_pass_record_does_not_issue_second_funded_after_first_breached(self):
  self.source['passed_at']='2026-10-12T00:00:00+00:00';self.assertTrue(self.call()['already_fulfilled']);self.core.assert_not_called();self.monitor.assert_not_called();self.pointer.assert_not_called();self.assertFalse(self.ctx.get())
 def test_existing_active_child_completed_instead_of_second_insert(self):
  self.child['account_status']='assigned_active';self.assertTrue(self.call()['already_fulfilled']);self.monitor.assert_called_once();self.pointer.assert_called_once();self.core.assert_not_called()
 def test_breached_source_cannot_assign(self):
  self.source['breached_at']='now';self.assertIsNone(self.call());self.core.assert_not_called()
 def test_hold_and_ownership_mismatch_fail_closed(self):
  self.ns['_np_mh_blocked']=lambda *a:True;self.assertIsNone(self.call());self.core.assert_not_called()
  self.ns['_np_mh_blocked']=lambda *a:False;self.source['purchase_id']='other';self.assertIsNone(self.call());self.core.assert_not_called()

class RecordingTests(unittest.TestCase):
 def setUp(self):
  ProofTests.setUp(self);self.ctx=ContextVar('recording',default=False);self.rpc=Mock();self.writes=[];state=self.a;writes=self.writes
  class Query:
   def __init__(self):self.filters={};self.payload={}
   def update(self,payload):self.payload=payload;return self
   def eq(self,k,v):self.filters[k]=v;return self
   def is_(self,k,v):self.filters[k]=None;return self
   def execute(self):
    if any(state.get(k)!=v for k,v in self.filters.items()):return SimpleNamespace(data=[])
    writes.append((dict(self.filters),dict(self.payload)));state.update(self.payload);return SimpleNamespace(data=[copy.deepcopy(state)])
  def rows(table,params):
   if table=='np_live_account_state':return [copy.deepcopy(self.live)]
   if table=='trader_accounts':return [copy.deepcopy(self.a)]
   if table=='challenge_purchases':return [copy.deepcopy(self.p)]
   raise AssertionError(table)
  self.ns.update(_NP_RENEWAL_REST_V211=self.ctx,_NP_V214_LIVE_CURSOR=0,_np_v214_rows=rows,_np_mh_blocked=lambda *args:False,_next_stage_for_lifecycle=lambda *args:'funded',_np_renewal_db_v211=lambda:SimpleNamespace(table=lambda t:Query()),now_iso=lambda:self.now.isoformat(),_np_v184_rpc=self.rpc,_invalidate_trader_bootstrap_cache=Mock(),np_invalidate_admin_bootstrap=Mock())
  extract(['_np_record_live_passes_v214'],self.ns)
 def record(self):return self.ns['_np_record_live_passes_v214']()
 def test_durable_pass_recorded_once_with_metrics_and_exact_cas(self):
  first=self.record();self.assertEqual(first['recorded'],1);self.assertEqual(self.a['account_status'],'archived_phase1');self.assertFalse(self.a['monitoring_enabled']);self.assertEqual(self.a['profit'],51000);self.assertEqual(self.writes[0][0]['account_status'],'assigned_active');self.assertEqual(self.writes[0][0]['trader_id'],'t');self.rpc.assert_called_once();self.assertEqual(self.record()['recorded'],0);self.assertFalse(self.ctx.get())
 def test_hold_does_not_record_pass(self):
  self.ns['_np_mh_blocked']=lambda *args:True;self.assertEqual(self.record()['recorded'],0);self.assertEqual(self.writes,[]);self.rpc.assert_not_called()
 def test_retire_failure_preserves_recorded_pass_for_assignment_retry(self):
  self.rpc.side_effect=RuntimeError('unavailable');result=self.record();self.assertEqual(result['recorded'],1);self.assertEqual(result['errors'],1);self.assertEqual(self.a['account_status'],'archived_phase1');self.assertFalse(self.ctx.get())
 def test_failed_database_read_does_not_invent_passes(self):
  self.ns['_np_v214_rows']=Mock(side_effect=RuntimeError('read failed'));self.assertEqual(self.record()['recorded'],0);self.assertEqual(self.writes,[]);self.assertFalse(self.ctx.get())

class ClaimTests(unittest.TestCase):
 setUp = AssignmentTests.setUp
 call = AssignmentTests.call
 def test_busy_exact_source_never_enters_assignment(self):
  self.ns['_np_v214_rows']=lambda table,p:[self.source]
  self.source['archive_reason']='[NP_PHASE_ASSIGNMENT_CLAIM_V214:busy]'
  self.assertIsNone(self.call());self.core.assert_not_called();self.assertFalse(self.ctx.get())
 def test_lost_database_claim_never_enters_assignment(self):
  from unittest.mock import MagicMock
  self.ns['_np_v214_rows']=lambda table,p:[self.source]
  q=MagicMock();q.update.return_value=q;q.eq.return_value=q;q.is_.return_value=q;q.execute.return_value.data=[]
  self.ns['_np_renewal_db_v211']=lambda:SimpleNamespace(table=lambda table:q)
  self.assertIsNone(self.call());self.core.assert_not_called();self.assertFalse(self.ctx.get())

class CycleTests(unittest.TestCase):
 def test_passes_reconciled_and_assigned_before_legacy_scans(self):
  order=[]
  ns={'_np_record_live_passes_v214':lambda:order.append('record') or {'recorded':1},'_np_v74_try_file_lock':lambda:True,'_np_v74_release_file_lock':lambda lock:order.append('unlock'),'_np_retry_clean_pass_funded_v19':lambda limit:order.append('assign') or {'assigned':1},'_np_v214_cycle_core':lambda trigger:order.append('legacy') or {}}
  extract(['_np_run_verified_lifecycle_cycle_v40'],ns);r=ns['_np_run_verified_lifecycle_cycle_v40']();self.assertEqual(order,['record','assign','unlock','legacy']);self.assertEqual(r['verified_pass_priority_recovery']['assigned'],1)
 def test_cross_process_busy_defers_assignment(self):
  retry=Mock();ns={'_np_record_live_passes_v214':lambda:{'recorded':0},'_np_v74_try_file_lock':lambda:None,'_np_retry_clean_pass_funded_v19':retry,'_np_v214_cycle_core':lambda trigger:{}}
  extract(['_np_run_verified_lifecycle_cycle_v40'],ns);ns['_np_run_verified_lifecycle_cycle_v40']();retry.assert_not_called()
