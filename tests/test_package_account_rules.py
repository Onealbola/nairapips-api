import copy,unittest
from uuid import uuid4
from package_account_rules import bind_package_account,load_package_rules,PackageBindingError
from package_contracts import make_contract
from package_configuration import defaults,quote

class BindingTests(unittest.TestCase):
 def setUp(self):
  cfg=defaults();cfg['minimum_price']=5000
  for key,rows in cfg['options'].items():
   for row in rows:row.update(enabled=True,price=5000 if key=='capital' else 0)
  self.choices=dict(capital='500000',phases='two',drawdown='10',drawdown_type='trailing',phase1_target='10',phase2_target='8',profit_share='80',profit_cap='20',payout_cycles='3',phase_resets='1',funded_resets='2')
  made=make_contract(quote(cfg,self.choices),str(uuid4()));doc=made['document']
  self.contract=dict(id=doc['contract_id'],trader_id=doc['trader_id'],purchase_id=str(uuid4()),rules_hash=made['rules_hash'],document=doc)
  self.account=dict(id=str(uuid4()),trader_id=doc['trader_id'],purchase_id=self.contract['purchase_id'],package_contract_id=doc['contract_id'],package_rules_hash=made['rules_hash'],stage='phase2',mt5_login='123456',account_size=500000,start_balance=500000,target_percent=99,dd_limit_percent=80)
 def test_purchase_contract_overrides_mutable_account_fields(self):
  r=bind_package_account(self.account,self.contract)
  self.assertEqual(r['target_percent'],8);self.assertEqual(r['dd_limit_percent'],10)
  self.assertEqual(r['drawdown_type'],'trailing');self.assertEqual(r['profit_share_percent'],80)
  self.assertEqual((r['phase_resets'],r['funded_resets'],r['payout_cycles']),(1,2,3))
 def test_other_customer_purchase_hash_stage_and_capital_are_rejected(self):
  for key,value in [('trader_id',str(uuid4())),('purchase_id',str(uuid4())),('package_rules_hash','0'*64),('package_contract_id',str(uuid4())),('stage','unknown'),('account_size',100000)]:
   with self.subTest(key=key),self.assertRaises(PackageBindingError):bind_package_account({**self.account,key:value},self.contract)
  changed=copy.deepcopy(self.contract);changed['document']['profit_share_percent']='60'
  with self.assertRaises(PackageBindingError):bind_package_account(self.account,changed)
 def test_exact_contract_read_and_no_plan_fallback_on_failure(self):
  calls=[]
  def read(table,filters,limit):calls.append((table,filters,limit));return [self.contract]
  self.assertEqual(load_package_rules(self.account,read)['target_percent'],8)
  self.assertEqual(calls,[('np_package_contracts',{'id':'eq.'+self.account['package_contract_id']},1)])
  for reader in [lambda *a,**kw:[],lambda *a,**kw:(_ for _ in ()).throw(TimeoutError())]:
   with self.assertRaises(PackageBindingError):load_package_rules(self.account,reader)
  with self.assertRaises(PackageBindingError):load_package_rules(self.account,read,stage='phase1')
 def test_legacy_rules_are_not_read_or_changed(self):
  def forbidden(*a,**kw):raise AssertionError('Legacy must not query package storage')
  self.assertIsNone(load_package_rules({'id':'legacy'},forbidden))
