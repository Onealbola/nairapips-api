import ast,copy,unittest
from pathlib import Path
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
class ExceptionTests(unittest.TestCase):
 def setUp(self):
  ns={'_np_v122_s':lambda v:str(v or '').strip()}
  nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in {'_np_paid_replacement_exception_v210','_np_v122_source_block_reason'}]
  exec(compile(ast.Module(body=nodes,type_ignores=[]),'exception','exec'),ns);self.ns=ns
  marker='[NP_PAID_REPLACEMENT_EXCEPTION_V210:p]'
  self.s=dict(id='s',trader_id='t',stage='funded',mt5_login='123',breached_at='historical',archive_reason='MAX_DRAWDOWN_BREACH | '+marker)
  self.p=dict(id='p',trader_id='t',trader_account_id='s',mt5_login='123',status='paid',paid_at='recorded',admin_note=marker)
 def block(self):return self.ns['_np_v122_source_block_reason'](self.s,self.p)
 def test_exact_paid_exception_only(self):self.assertEqual(self.block(),'');self.assertEqual(self.s['breached_at'],'historical')
 def test_no_exception_breach_remains_blocked(self):
  self.s['archive_reason']='MAX_DRAWDOWN_BREACH';self.assertEqual(self.block(),'source_breached')
 def test_unpaid_missing_payment_or_wrong_identity_blocked(self):
  original=copy.deepcopy(self.p)
  for key,value in [('status','pending'),('paid_at',None),('trader_id','other'),('trader_account_id','other'),('mt5_login','other'),('id','other'),('admin_note','')]:
   self.p=dict(original,**{key:value});self.assertEqual(self.block(),'source_breached',key)
 def test_exception_never_overrides_reset_or_recall(self):
  self.s['reset_at']='historical';self.assertEqual(self.block(),'source_already_reset_or_replaced')
  self.s.pop('reset_at');self.s['archive_reason']+=' wrong_assignment';self.assertEqual(self.block(),'source_recalled_or_wrong_assignment')
 def test_phase1_cannot_receive_paid_exception(self):
  self.s['stage']='phase1';self.assertEqual(self.block(),'source_breached')
