import copy, threading, unittest
from limited_cycle_safety import LimitedCycleSafety, CycleBlocked

class Response:
    def __init__(self,rows):self.data=copy.deepcopy(rows)
    def raise_for_status(self):pass
    def json(self):return copy.deepcopy(self.data)

class DB:
    def __init__(self):
        self.lock=threading.Lock();self.fail=False;self.writes=[]
        self.tables={'challenge_purchases':[dict(id='purchase',trader_id='owner',payout_cycles=2,account_size=500000,payout_split=60,archive_reason=None)],
          'trader_accounts':[dict(id='a',trader_id='owner',purchase_id='purchase',stage='funded',mt5_login='111'),dict(id='b',trader_id='owner',purchase_id='purchase',stage='funded',mt5_login='222'),dict(id='c',trader_id='owner',purchase_id='purchase',stage='funded',mt5_login='333')], 'payouts':[]}
    def matches(self,row,params):
        for k,v in params.items():
            if k in {'select','limit','offset','order'}:continue
            if v=='is.null':
                if row.get(k) is not None:return False
            elif v.startswith('eq.'):
                if str(row.get(k))!=v[3:]:return False
            elif v.startswith('in.('):
                if str(row.get(k)) not in v[4:-1].split(','):return False
            else:raise AssertionError(v)
        return True
    def get(self,url,params,**kw):
        if self.fail:raise TimeoutError()
        with self.lock:
            rows=[r for r in self.tables[url.rsplit('/',1)[-1]] if self.matches(r,params)]
            rows=sorted(rows,key=lambda r:r['id']);offset=int(params.get('offset',0));return Response(rows[offset:offset+int(params.get('limit',500))])
    def patch(self,url,params,json,**kw):
        if self.fail:raise TimeoutError()
        with self.lock:
            rows=[r for r in self.tables[url.rsplit('/',1)[-1]] if self.matches(r,params)]
            for r in rows:r.update(json)
            if rows:self.writes.append(copy.deepcopy(json))
            return Response(rows)

class SafetyTests(unittest.TestCase):
    def setUp(self):
        self.db=DB();self.g=LimitedCycleSafety(self.db,'https://fixture.invalid','fixture')
        self.p=self.db.tables['challenge_purchases'][0];self.a=self.db.tables['trader_accounts'][0];self.b=self.db.tables['trader_accounts'][1]
    def paid(self,account,pid='paid'):
        p=dict(id=pid,trader_id='owner',trader_account_id=account['id'],mt5_login=account['mt5_login'],status='paid',paid_at='2026-10-22')
        self.db.tables['payouts'].append(p);return p
    def quote(self,**kw):return dict(verified=True,start_balance=500000,payout_split=60,available_payout=45000,**kw)
    def test_final_cycle_allows_exactly_second_payment_then_blocks_all_checkpoints(self):
        self.paid(self.a)
        claim=self.g.acquire(self.b,'second','paid');self.g.check_amount(claim,45000,self.quote());self.paid(self.b,'second');self.g.release(claim)
        for action in ['create','approve','paid']:
            with self.assertRaisesRegex(CycleBlocked,'PURCHASE COMPLETED'):self.g.acquire(self.db.tables['trader_accounts'][2],'third',action)
        self.assertIsNone(self.p['archive_reason'])
        self.assertEqual(self.g.inspect(self.p,self.b)['remaining'],0)
    def test_cross_account_concurrent_actions_share_one_purchase_lock(self):
        barrier=threading.Barrier(3);finish=threading.Event();results=[]
        def attempt(a):
            barrier.wait()
            try:
                c=self.g.acquire(a);results.append('won');finish.wait(1);self.g.release(c)
            except CycleBlocked:results.append('blocked')
        ts=[threading.Thread(target=attempt,args=(a,)) for a in [self.a,self.b]]
        for t in ts:t.start()
        barrier.wait()
        for t in ts:t.join()
        self.assertEqual(sorted(results),['blocked','won'])
    def test_open_request_reserves_final_slot_but_can_itself_be_paid(self):
        self.paid(self.a)
        self.db.tables['payouts'].append(dict(id='pending',trader_id='owner',trader_account_id='b',mt5_login='222',status='approved',paid_at=None))
        with self.assertRaisesRegex(CycleBlocked,'already have payout requests'):self.g.acquire(self.db.tables['trader_accounts'][2])
        c=self.g.acquire(self.b,'pending','paid');self.g.release(c)
    def test_legacy_and_unlimited_are_never_written_or_reinterpreted(self):
        for cap in [None,0]:
            self.p['payout_cycles']=cap
            for action in ['create','approve','paid']:self.assertIsNone(self.g.acquire(self.a,action=action))
        self.assertEqual(self.db.writes,[])
    def test_stored_paid_date_cannot_be_hidden_by_cancelled_status(self):
        self.paid(self.a)['status']='cancelled';self.paid(self.b,'second')['status']='cancelled'
        with self.assertRaisesRegex(CycleBlocked,'PURCHASE COMPLETED'):self.g.acquire(self.b)
    def test_changed_owner_or_login_requires_review(self):
        for field in ['trader_id','mt5_login']:
            self.db.tables['payouts']=[];self.paid(self.a)[field]='wrong'
            with self.assertRaisesRegex(CycleBlocked,'identity mismatch'):self.g.acquire(self.b)
            self.assertIsNone(self.p['archive_reason'])
    def test_contract_ceiling_and_fresh_quote_both_required(self):
        c=self.g.acquire(self.a)
        try:
            self.g.check_amount(c,45000,self.quote())
            for value,q in [(150001,None),(45001,self.quote()),(1,{**self.quote(),'verified':False}),(1,{**self.quote(),'payout_split':80}),(1,{**self.quote(),'start_balance':1000000}),('NaN',None),(True,None),(-1,None),('1.001',None)]:
                with self.assertRaises(CycleBlocked):self.g.check_amount(c,value,q)
        finally:self.g.release(c)
    def test_purchase_lock_release_preserves_completion_marker(self):
        c=self.g.acquire(self.a);self.p['archive_reason']+=' [NP_PLAN_CYCLES_COMPLETED:purchase]';self.g.release(c)
        self.assertEqual(self.p['archive_reason'],'[NP_PLAN_CYCLES_COMPLETED:purchase]')
    def test_database_read_failure_never_authorizes_payout(self):
        self.db.fail=True
        with self.assertRaises(TimeoutError):self.g.acquire(self.a)
        self.assertEqual(self.db.writes,[])
    def test_cap_is_frozen_purchase_not_current_plan_or_browser_input(self):
        self.p['payout_cycles']=1;self.paid(self.a)
        with self.assertRaisesRegex(CycleBlocked,'PURCHASE COMPLETED'):self.g.acquire(self.b)
    def test_unrelated_purchase_can_act_while_another_is_locked(self):
        c=self.g.acquire(self.a)
        self.db.tables['challenge_purchases'].append(dict(self.p,id='other',archive_reason=None))
        a=dict(self.a,id='other-a',purchase_id='other');self.db.tables['trader_accounts'].append(a)
        other=self.g.acquire(a);self.g.release(other);self.g.release(c)

    def test_four_cycle_purchase_stops_after_four_and_old_two_cycle_test_stays_valid(self):
        self.p['payout_cycles']=4
        accounts=self.db.tables['trader_accounts']
        accounts.extend([dict(self.a,id='d',mt5_login='444'),dict(self.a,id='e',mt5_login='555')])
        for i,account in enumerate(accounts[:4]):
            c=self.g.acquire(account,action='paid')
            self.assertEqual(c['cycles']['remaining'],4-i)
            self.paid(account,'paid-'+str(i));self.g.release(c)
        for action in ('create','approve','paid'):
            with self.assertRaisesRegex(CycleBlocked,'PURCHASE COMPLETED'):self.g.acquire(accounts[4],action=action)
        self.assertEqual(self.g.inspect(self.p,accounts[0])['remaining'],0)

if __name__=='__main__':unittest.main()
