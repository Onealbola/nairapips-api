import unittest
from flask import Flask,request
from funded_profit_policy import validate_payout,register_profit_policy
class PolicyTests(unittest.TestCase):
 def test_current_fifteen_percent_for_both_splits(self):
  self.assertEqual(validate_payout(45000,500000,60),45000)
  self.assertEqual(validate_payout(52500,500000,70),52500)
  for n in [45000.01,70430,150000]:
   with self.assertRaises(ValueError):validate_payout(n,500000,60)
 def test_create_approve_paid_block_legacy_excess_without_calling_mutations(self):
  app=Flask(__name__);calls=[]
  for endpoint in ['create_payout','approve_payout','mark_paid']:
   app.add_url_rule('/'+endpoint,endpoint,lambda:calls.append('mutation') or {'success':True},methods=['POST'])
  account={'id':'a','trader_id':'t','start_balance':500000,'payout_cycles':None}
  payout={'id':'p','trader_id':'t','trader_account_id':'a','amount':70430,'status':'pending'}
  runtime={'bad':lambda msg,code:(app.json.response(error=msg),code),'_require_admin':lambda:({},None),'_authenticated_trader_id_for_request':lambda x:('t',None),'get_trader_by_id':lambda x:{'id':x},'_payout_eligibility':lambda *a,**k:(True,'ok',account),'_np_plan_payout_split_for_account':lambda *a:60,'_np_query_rows_v173':lambda table,**kw:[payout] if table=='payouts' else [account]}
  register_profit_policy(app,runtime)
  with app.test_client() as c:
   for endpoint in ['create_payout','approve_payout','mark_paid']:
    r=c.post('/'+endpoint,json={'id':'p','amount':70430,'trader_id':'t','trader_account_id':'a'});self.assertEqual(r.status_code,409);self.assertIn('45000',r.json['error'])
   self.assertEqual(calls,[])
   payout['amount']=45000;self.assertEqual(c.post('/approve_payout',json={'id':'p'}).status_code,200)
if __name__=='__main__':unittest.main()
