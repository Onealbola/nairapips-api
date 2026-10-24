import unittest
from flask import Flask,jsonify
from finance_source import register_finance_source
class FinanceTests(unittest.TestCase):
 def test_auth_full_read_secret_stripping_and_failure(self):
  class Guard:
   calls=[]
   def rows(self,table,params):
    self.calls.append(table);return [dict(id=table,fee=100,amount=10,status='paid',mt5_master_password='secret',payment_proof_url='private',created_at='2026-10-01')]
  app=Flask(__name__);g=Guard();auth=[False]
  register_finance_source(app,g,lambda:({},None) if auth[0] else (None,(jsonify(success=False),401)))
  with app.test_client() as c:
   self.assertEqual(c.get('/admin/finance_source').status_code,401);self.assertEqual(g.calls,[])
   auth[0]=True;r=c.get('/admin/finance_source');self.assertEqual(r.status_code,200);self.assertEqual(g.calls,['challenge_purchases','payouts']);self.assertNotIn('secret',r.text);self.assertNotIn('private',r.text);self.assertIn('no-store',r.headers['Cache-Control'])
   def fail(*args):raise TimeoutError()
   g.rows=fail;r=c.get('/admin/finance_source');self.assertEqual(r.status_code,503);self.assertNotIn('purchases',r.json)
if __name__=='__main__':unittest.main()
