import unittest
from datetime import datetime,timezone,timedelta
from flask import Flask,jsonify
from purchase_alert import needs_review,register_purchase_alert

class PurchaseAlertTests(unittest.TestCase):
 def test_pending_is_separate_from_arrival(self):
  self.assertTrue(needs_review(dict(status='pending')))
  self.assertFalse(needs_review(dict(status='pending',payment_status='approved')))
 def test_auth_baseline_old_pending_and_only_new_ids(self):
  now=datetime.now(timezone.utc)
  class Guard:
   calls=0
   def rows(self,*args):
    self.calls+=1
    return [dict(id='old',created_at=(now-timedelta(days=90)).isoformat(),status='pending'),dict(id='new',created_at=(now-timedelta(seconds=2)).isoformat(),status='pending'),dict(id='new',created_at=(now-timedelta(seconds=2)).isoformat()),dict(id='test',created_at=(now-timedelta(seconds=1)).isoformat(),mark_as_test=True)]
  g=Guard();auth=[False];app=Flask(__name__)
  register_purchase_alert(app,g,lambda:({},None) if auth[0] else (None,(jsonify(success=False),401)))
  with app.test_client() as c:
   self.assertEqual(c.get('/admin/purchase_alert').status_code,401);self.assertEqual(g.calls,0)
   auth[0]=True;r=c.get('/admin/purchase_alert');self.assertEqual(r.json['new_count'],0);self.assertEqual(g.calls,0)
   r=c.get('/admin/purchase_alert',query_string={'since':(now-timedelta(seconds=10)).isoformat()});self.assertEqual(r.json['new_ids'],['new']);self.assertEqual(r.json['new_count'],1);self.assertIn('no-store',r.headers['Cache-Control'])
   self.assertEqual(c.get('/admin/purchase_alert?since=bad').status_code,400)
   self.assertEqual(c.get('/admin/purchase_alert',query_string={'since':(now+timedelta(days=1)).isoformat()}).status_code,400)
   def fail(*args):raise TimeoutError()
   g.rows=fail;r=c.get('/admin/purchase_alert',query_string={'since':(now-timedelta(seconds=10)).isoformat()});self.assertEqual(r.status_code,503);self.assertNotIn('new_count',r.json)
if __name__=='__main__':unittest.main()
