import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock
from flask import Flask
from first_purchase_promo import quote,requested,CODE

class WelcomeTests(unittest.TestCase):
 def setUp(self):
  self.app=Flask(__name__);self.db=Mock();self.db.rpc.return_value.execute.return_value=NS(data=True)
  self.auth=Mock(return_value=('registered-trader',None));self.body={'trader_id':'registered-trader','plan_id':'plan','promo_code':'welcome20'}
 def q(self,body=None,price=20000,path='/quote_challenge_price'):
  with self.app.test_request_context(path):return quote(body or self.body,price,self.db,self.auth)
 def test_exact_twenty_percent_naira_and_half_kobo_rounding(self):
  for price,discount,total in [(20000,4000,16000),(100,20,80),(9999.99,2000,7999.99),('0.03',0.01,0.02)]:
   q=self.q(price=price);self.assertTrue(q['valid']);self.assertEqual((q['discount_percent'],q['discount_amount'],q['final_fee']),(20,discount,total));self.assertEqual(q['commission_amount'],0)
 def test_existing_pending_and_legacy_account_history_denied_by_fresh_database_check(self):
  self.db.rpc.return_value.execute.return_value=NS(data=False)
  q=self.q();self.assertFalse(q['valid']);self.assertEqual(q['discount_amount'],0)
  self.db.rpc.assert_called_once_with('np_first_purchase_promo_eligible_v1',{'p_trader_id':'registered-trader'})
 def test_database_outage_or_unexpected_response_never_grants_discount(self):
  self.db.rpc.side_effect=TimeoutError();self.assertFalse(self.q()['valid'])
  self.db.rpc.side_effect=None
  for value in ['true',1,None,[],{}]:
   self.db.rpc.return_value.execute.return_value=NS(data=value);self.assertFalse(self.q()['valid'])
 def test_spoofed_trader_is_rejected_and_payload_contacts_are_not_authority(self):
  self.auth.return_value=('', 'Token owner mismatch');self.assertFalse(self.q()['valid']);self.db.rpc.assert_not_called()
 def test_no_stacking_or_discount_for_reset_renewal_upgrade_payout_routes(self):
  for path in ['/create_reset_request','/funded_renewal','/create_payout','/upgrade','/trader/package_checkout']:
   self.assertFalse(self.q(path=path)['valid'])
  for key in ['reset_id','renewal_id','source_account_id','trader_account_id','purchase_id']:
   self.assertFalse(self.q({**self.body,key:'existing'})['valid'])
  self.assertFalse(self.q({**self.body,'affiliate_code':'OTHER'})['valid']);self.db.rpc.assert_not_called()
 def test_only_initial_order_submit_and_authenticated_price_preview(self):
  self.assertTrue(self.q(path='/create_challenge_purchase')['valid'])
  self.assertFalse(self.q({**self.body,'plan_id':''})['valid'])
 def test_unrelated_codes_leave_existing_pricing_untouched(self):
  self.assertFalse(requested({'code':'OTHER'}))
  with self.app.test_request_context('/quote_challenge_price'):self.assertIsNone(quote({'code':'OTHER'},100,self.db,self.auth))
  self.db.rpc.assert_not_called();self.auth.assert_not_called()

if __name__=='__main__':unittest.main()

class CheckoutIntegrationTests(unittest.TestCase):
 def test_actual_checkout_quote_and_saved_fields_keep_20_percent_with_no_commission(self):
  import ast,copy
  from pathlib import Path
  tree=ast.parse(Path('app.py').read_text())
  nodes=[copy.deepcopy(n) for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('_affiliate_quote_details','_affiliate_purchase_fields')]
  db=Mock();db.rpc.return_value.execute.return_value=NS(data=True)
  scope=dict(clean=lambda x:float(x or 0),supabase=db,supabase_admin=db,_authenticated_trader_id_for_request=lambda x:('customer',None))
  exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-checkout-promo','exec'),scope)
  with Flask(__name__).test_request_context('/create_challenge_purchase'):
   fields=scope['_affiliate_purchase_fields']({'code':'WELCOME20','plan_id':'plan','trader_id':'customer'},20000)
  self.assertEqual(fields['fee'],16000);self.assertEqual(fields['original_fee'],20000);self.assertEqual(fields['discount_amount'],4000)
  self.assertEqual(fields['campaign_type'],'first_purchase_welcome20');self.assertEqual(fields['promo_code'],'WELCOME20');self.assertEqual(fields['commission_amount'],0)
