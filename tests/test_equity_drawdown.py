import copy
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4
from equity_drawdown import spec, initial, evaluate, resolve_mode, EquityDrawdownJournal

class DrawdownTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.account = dict(id=str(uuid4()), trader_id=str(uuid4()), purchase_id=str(uuid4()),
            mt5_login='123456', stage='phase1', start_balance=1000000, dd_limit_percent=10,
            drawdown_type='TRAILING', started_at=self.now.isoformat())
        self.rules = spec(self.account)
    def tick(self, state, equity, seconds=1):
        return evaluate(self.rules, state, equity, (self.now+timedelta(seconds=seconds)).isoformat())
    def test_equivalent_database_and_api_numeric_forms_have_identical_frozen_terms(self):
        self.assertEqual(self.rules,spec({**self.account,'start_balance':1000000.0,'dd_limit_percent':10.0}))
        self.assertEqual(self.rules,spec({**self.account,'start_balance':'1000000.00','dd_limit_percent':'10.00'}))

    def test_floating_profit_raises_floor_by_fixed_initial_allowance(self):
        s=self.tick(initial(self.rules),1200000)
        self.assertEqual(float(s['drawdown_floor']),1100000)
        s=self.tick(s,1150000,2)
        self.assertEqual(float(s['used_percent']),50)
        self.assertEqual(float(s['drawdown_floor']),1100000)
        self.assertTrue(self.tick(s,1100000,3)['breached'])
    def test_static_floor_never_moves_with_profit(self):
        self.rules['drawdown_type']='STATIC'
        s=self.tick(initial(self.rules),1200000)
        self.assertEqual(float(s['drawdown_floor']),900000)
        self.assertFalse(self.tick(s,900000.01,2)['breached'])
        self.assertTrue(self.tick(s,900000,2)['breached'])
    def test_low_before_new_peak_does_not_manufacture_historical_breach(self):
        s=self.tick(initial(self.rules),950000)
        s=self.tick(s,1200000,2)
        self.assertFalse(s['breached'])
        self.assertEqual(float(s['worst_used_percent']),50)
    def test_out_of_order_duplicate_and_recovery_never_reset_state(self):
        s=self.tick(initial(self.rules),1200000,3)
        self.assertEqual(self.tick(s,500000,2),s)
        self.assertEqual(self.tick(s,1300000,3),s)
        s=self.tick(s,1000000,4)
        self.assertTrue(s['breached'])
        self.assertEqual(self.tick(s,1500000,5),s)
    def test_cash_withdrawal_does_not_lower_floor_or_allowance(self):
        s=self.tick(initial(self.rules),1200000)
        self.assertTrue(self.tick(s,1099999,2)['breached'])
    def test_cross_account_stage_or_terms_cannot_reuse_watermark(self):
        s=initial(self.rules)
        for field,value in [('account_id',str(uuid4())),('stage','funded'),('limit','20'),('drawdown_type','STATIC')]:
            changed={**self.rules,field:value}
            with self.assertRaises(ValueError):evaluate(changed,s,1200000,self.now.isoformat())
    def test_invalid_equity_rejected_and_negative_equity_breaches(self):
        for equity in [True,None,'NaN','Infinity']:
            with self.assertRaises(ValueError):self.tick(initial(self.rules),equity)
        self.assertTrue(self.tick(initial(self.rules),0)['breached'])
        self.assertTrue(self.tick(initial(self.rules),-1)['breached'])
    def test_frozen_purchase_and_legacy_defaults(self):
        a={k:v for k,v in self.account.items() if k!='drawdown_type'}
        p=dict(id=a['purchase_id'],trader_id=a['trader_id'],drawdown_type='TRAILING')
        self.assertEqual(resolve_mode(a,p),'TRAILING')
        self.assertEqual(resolve_mode(a),'STATIC')
        with self.assertRaises(ValueError):resolve_mode(a,{**p,'trader_id':'other'})
    def test_atomic_journal_survives_restart_and_rejects_missing_or_corrupt_history(self):
        row={**self.account,'drawdown_rules':self.rules}
        with TemporaryDirectory() as directory:
            j=EquityDrawdownJournal(directory)
            j.assess(row,1200000,self.now.isoformat(),self.now)
            s=EquityDrawdownJournal(directory).assess(row,1150000,(self.now+timedelta(seconds=1)).isoformat(),self.now+timedelta(seconds=1))
            self.assertEqual(float(s['drawdown_floor']),1100000)
            path=Path(directory)/(self.account['id']+'.json')
            path.write_text('broken')
            with self.assertRaises(ValueError):j.assess(row,1150000,self.now.isoformat(),self.now)
            self.assertEqual(path.read_text(),'broken')
        with TemporaryDirectory() as directory:
            row['started_at']=(self.now-timedelta(days=1)).isoformat()
            with self.assertRaises(ValueError):EquityDrawdownJournal(directory).assess(row,1150000,self.now.isoformat(),self.now)

if __name__=='__main__': unittest.main()

class DurableStoreTests(DrawdownTests):
    def test_cas_conflict_never_loses_a_newer_higher_watermark(self):
        from types import SimpleNamespace
        from equity_drawdown import DrawdownStateStore, verify_ingest, runtime_ready, RELEASE
        from unittest.mock import patch
        rules=self.rules
        row={**self.account}
        class Transport:
            raw=None
            writes=0
            conflict=False
            def response(self, rows, status=200):
                return SimpleNamespace(status_code=status,raise_for_status=lambda:None,json=lambda:copy.deepcopy(rows))
            def get(self,*args,**kwargs):
                return self.response([] if self.raw is None else [{'value':self.raw}])
            def post(self,*args,**kwargs):
                self.raw=kwargs['json']['value'];self.writes+=1
                return self.response([{'value':self.raw}])
            def patch(self,*args,**kwargs):
                if self.conflict:
                    self.conflict=False
                    state=evaluate(rules,json.loads(self.raw),1300000,(self_now+timedelta(seconds=2)).isoformat())
                    self.raw=json.dumps(state)
                    return self.response([])
                self.assert_filter=kwargs['params']['value']=='eq.'+self.raw
                self.raw=kwargs['json']['value'];self.writes+=1
                return self.response([{'value':self.raw}])
        self_now=self.now
        http=Transport();store=DrawdownStateStore(http,'https://fixture.invalid','fixture')
        a=store.assess(row,{},1200000,self.now.isoformat(),self.now)
        http.conflict=True
        later=self.now+timedelta(seconds=3)
        b=store.assess(row,{},1250000,later.isoformat(),later)
        self.assertEqual(float(b['highest_equity']),1300000)
        self.assertEqual(float(b['drawdown_floor']),1200000)
        self.assertFalse(b['breached']);self.assertTrue(http.assert_filter)
        writes=http.writes
        store.assess(row,{},500000,self.now.isoformat(),self.now)
        self.assertEqual(http.writes,writes)
        with patch.dict('os.environ',{},clear=True):
            self.assertFalse(runtime_ready())
            with self.assertRaises(ValueError):verify_ingest(SimpleNamespace(headers={}))
        with patch.dict('os.environ',{'NP_EQUITY_TRAILING_VERIFIED':RELEASE,'NP_EQUITY_TRAILING_INGEST_KEY':'fixture'}):
            self.assertTrue(runtime_ready())
            verify_ingest(SimpleNamespace(headers={'X-NP-Trailing-Key':'fixture'}))
            with self.assertRaises(ValueError):verify_ingest(SimpleNamespace(headers={'X-NP-Trailing-Key':'wrong'}))
