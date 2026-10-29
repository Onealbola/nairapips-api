import unittest
from flask import Flask,jsonify
from limited_cycle_safety import LimitedCycleSafety
from cycle_progress import progress_report,read_progress,register_cycle_progress
from test_limited_cycle_safety import DB

class ProgressTests(unittest.TestCase):
    def test_retired_purchase_has_no_remaining_entitlement_or_renewal(self):
        self.p['lifecycle_state']='retired_two_lives';self.payout('a','paid')
        r=self.report();self.assertEqual(r['purchases'][0]['remaining'],0);self.assertEqual(r['purchases'][0]['status'],'inactive');self.assertTrue(r['payouts']['paid']['renewal_blocked']);self.assertEqual(self.db.writes,[])
    def setUp(self):
        self.db=DB();self.guard=LimitedCycleSafety(self.db,'https://fixture.invalid','fixture')
        self.p=self.db.tables['challenge_purchases'][0];self.p.update(trader_name='Fixture',plan_name='Legend')
    def report(self):return progress_report(self.guard,self.db.tables['challenge_purchases'],self.db.tables['trader_accounts'],self.db.tables['payouts'])
    def payout(self,aid,pid,status='paid'):
        a=next(a for a in self.db.tables['trader_accounts'] if a['id']==aid)
        p=dict(id=pid,trader_id=a['trader_id'],trader_account_id=aid,mt5_login=a['mt5_login'],status=status,paid_at='now' if status=='paid' else None);self.db.tables['payouts'].append(p);return p
    def test_real_counter_zero_first_second_and_completed(self):
        row=self.report()['purchases'][0];self.assertEqual((row['paid'],row['remaining']),(0,2))
        self.payout('a','first');row=self.report()['purchases'][0];self.assertEqual((row['paid'],row['remaining'],row['completed']),(1,1,False))
        self.payout('b','second');r=self.report();row=r['purchases'][0]
        self.assertEqual((row['paid'],row['remaining'],row['completed']),(2,0,True))
        self.assertTrue(r['payouts']['second']['blocked']);self.assertEqual(r['counts']['completed'],1)
        self.assertEqual(self.db.writes,[])
    def test_pending_approved_reserve_without_incrementing_paid(self):
        self.payout('a','pending','pending');self.payout('b','approved','approved')
        r=self.report();p=r['purchases'][0];self.assertEqual((p['paid'],p['reserved'],p['remaining']),(0,2,2))
        self.assertFalse(r['payouts']['pending']['blocked']);self.assertFalse(r['payouts']['approved']['blocked'])
    def test_rejected_does_not_consume_cycle_and_paid_timestamp_cannot_be_hidden(self):
        self.payout('a','rejected','rejected');self.assertEqual(self.report()['purchases'][0]['paid'],0)
        self.db.tables['payouts'][0]['paid_at']='now';self.assertEqual(self.report()['purchases'][0]['paid'],1)
    def test_invalid_identity_requires_review_instead_of_zero(self):
        self.payout('a','bad')['mt5_login']='other'
        r=self.report();p=r['purchases'][0];self.assertFalse(p['verified']);self.assertIsNone(p['paid']);self.assertIsNone(p['remaining']);self.assertTrue(r['payouts']['bad']['blocked'])
    def test_legacy_and_unlimited_not_in_progress_or_modified(self):
        for cap in (None,0):
            self.p['payout_cycles']=cap;self.assertEqual(self.report()['purchases'],[])
        self.assertEqual(self.db.writes,[])
    def test_exact_purchase_scope_does_not_mix_same_owner_purchases(self):
        self.db.tables['challenge_purchases'].append(dict(self.p,id='other'))
        self.payout('a','first');r=self.report();self.assertEqual([p['paid'] for p in r['purchases']],[1,0])
    def test_authentication_before_read_and_unavailable_does_not_return_counts(self):
        app=Flask(__name__);authorized=[False];self.db.fail=True
        register_cycle_progress(app,self.guard,lambda:({},None) if authorized[0] else (None,(jsonify(success=False),401)))
        with app.test_client() as c:
            self.assertEqual(c.get('/admin/funded_cycle_progress').status_code,401)
            authorized[0]=True;r=c.get('/admin/funded_cycle_progress');self.assertEqual(r.status_code,503);self.assertNotIn('data',r.json)
    def test_no_funded_account_is_zero_paid_not_completed(self):
        self.db.tables['trader_accounts']=[];p=self.report()['purchases'][0];self.assertEqual(p['status'],'awaiting_funded');self.assertEqual(p['paid'],0);self.assertFalse(p['completed'])
    def test_requested_legacy_display_keeps_terms_and_actions_unchanged(self):
        self.p['id']='4e4c7880-5593-4ffd-8c1a-2c531635d71f';self.p['payout_cycles']=None
        for a in self.db.tables['trader_accounts']:a['purchase_id']=self.p['id']
        self.payout('a','one');self.payout('b','two')
        r=self.report();p=r['purchases'][0]
        self.assertEqual(p['paid'],2);self.assertIsNone(p['limit']);self.assertIsNone(p['remaining']);self.assertTrue(p['display_only'])
        self.assertFalse(r['payouts']['two']['blocked']);self.assertEqual(self.db.writes,[])
        self.p['id']='unrelated';self.assertEqual(self.report()['purchases'],[])
if __name__=='__main__':unittest.main()
