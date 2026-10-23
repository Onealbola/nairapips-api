import unittest
from flask import Flask,jsonify
from purchase_alert import needs_review,register_purchase_alert

class PurchaseAlertTests(unittest.TestCase):
 def test_review_states_and_final_payment_precedence(self):
  for status in ['pending','pending_review','awaiting_review','submitted','awaiting_verification','payment_pending']:
   self.assertTrue(needs_review(dict(id='p',status=status)))
   self.assertFalse(needs_review(dict(status=status,payment_status='approved')))
  for status in ['approved','approved_active','assigned','completed','approved_waiting_mt5','rejected','cancelled']:
   self.assertFalse(needs_review(dict(status=status,payment_status='pending')))
  self.assertFalse(needs_review(dict(status='pending',mark_as_test=True)))
  self.assertFalse(needs_review(dict(status='pending',payment_status='refunded')))
  self.assertFalse(needs_review(dict(status='unknown')))
 def test_auth_count_dedup_and_failure_not_zero(self):
  class Guard:
   def rows(self,*args):return [dict(id='p',status='pending'),dict(id='p',status='pending'),dict(id='q',status='approved')]
  g=Guard();auth=[False];app=Flask(__name__)
  register_purchase_alert(app,g,lambda:({},None) if auth[0] else (None,(jsonify(success=False),401)))
  with app.test_client() as c:
   self.assertEqual(c.get('/admin/purchase_alert').status_code,401)
   auth[0]=True;r=c.get('/admin/purchase_alert');self.assertEqual(r.json['pending'],1);self.assertIn('no-store',r.headers['Cache-Control'])
   def fail(*args):raise TimeoutError()
   g.rows=fail;r=c.get('/admin/purchase_alert');self.assertEqual(r.status_code,503);self.assertNotIn('pending',r.json)
if __name__=='__main__':unittest.main()
