import unittest
from flask import Flask
from cycle_progress import register_trader_cycle_progress
from test_cycle_progress import ProgressTests

class TraderNoticeTests(ProgressTests):
    def setUp(self):
        super().setUp()
        original=self.db.matches
        self.db.matches=lambda row,params: (row.get('payout_cycles',0) or 0)>0 and original(row,{k:v for k,v in params.items() if v!='gt.0' and k!='or'}) if 'payout_cycles' in params or 'or' in params else original(row,params)
        self.auth=['owner',None]
        self.app=Flask(__name__)
        register_trader_cycle_progress(self.app,self.guard,lambda:tuple(self.auth))
        self.client=self.app.test_client()
    def test_owner_only_no_id_override_or_other_purchase_disclosure(self):
        self.db.tables['challenge_purchases'].append(dict(self.p,id='secret',trader_id='other'))
        r=self.client.get('/trader/funded_cycle_progress?trader_id=other')
        self.assertEqual(r.status_code,200)
        rows=r.json['data']['purchases'];self.assertEqual([p['purchase_id'] for p in rows],['purchase'])
        self.assertNotIn('trader_id',rows[0]);self.assertNotIn('payouts',r.json['data'])
        self.assertIn('no-store',r.headers['Cache-Control']);self.assertEqual(self.db.writes,[])
    def test_authentication_required_and_no_read(self):
        self.auth[:]=['','missing'];self.db.fail=True
        self.assertEqual(self.client.get('/trader/funded_cycle_progress').status_code,401)
    def test_final_payout_and_pending_are_distinct(self):
        self.p['payout_cycles']=3
        self.payout('a','one');self.payout('b','two');self.payout('c','pending','pending')
        p=self.client.get('/trader/funded_cycle_progress').json['data']['purchases'][0]
        self.assertEqual((p['paid'],p['remaining'],p['reserved'],p['completed']),(2,1,1,False))
        self.db.tables['payouts'][-1].update(status='paid',paid_at='now')
        p=self.client.get('/trader/funded_cycle_progress').json['data']['purchases'][0]
        self.assertEqual((p['paid'],p['remaining'],p['completed']),(3,0,True))
    def test_failure_does_not_invent_remaining_cycles(self):
        self.db.fail=True;r=self.client.get('/trader/funded_cycle_progress');self.assertEqual(r.status_code,503);self.assertNotIn('data',r.json)
if __name__=='__main__':unittest.main()
