import copy
import unittest
from uuid import uuid4
from package_configuration import defaults,quote
from package_contracts import make_contract,validate_contract,digest,initial_state,evaluate,payout_amount

class PackageContractTests(unittest.TestCase):
    def setUp(self):
        self.config=defaults();self.config['minimum_price']=5000
        for key,rows in self.config['options'].items():
            for row in rows:row.update(enabled=True,price=5000 if key=='capital' else 0)
        self.choices=dict(capital='500000',phases='two',drawdown='10',drawdown_type='static',phase1_target='10',phase2_target='8',profit_share='70',profit_cap='20',payout_cycles='3',phase_resets='1',funded_resets='2')
        self.doc=make_contract(quote(self.config,self.choices),str(uuid4()))['document']
        self.aid=str(uuid4())
    def observation(self,state,equity,sequence=1,**changes):
        return {**{k:state[k] for k in ('account_id','contract_id','trader_id','rules_hash')},'equity':equity,'balance':equity,'sequence':sequence,'observed_at':f'2026-10-17T10:{sequence:02}:00Z','open_positions':0,**changes}
    def test_contract_survives_tariff_edits_and_detects_rule_tampering(self):
        frozen=copy.deepcopy(self.doc);hash=digest(frozen)
        self.config['minimum_price']=99999;self.choices['profit_share']='80'
        self.assertEqual(self.doc,frozen);validate_contract(self.doc,hash)
        changed=copy.deepcopy(self.doc);changed['profit_share_percent']='80'
        with self.assertRaisesRegex(ValueError,'hash'):validate_contract(changed,hash)
    def test_static_drawdown_pass_flat_positions_and_exact_boundary(self):
        state=initial_state(self.doc,self.aid,'phase1')
        self.assertEqual(evaluate(self.doc,state,self.observation(state,550000))['decision'],'passed')
        self.assertEqual(evaluate(self.doc,state,self.observation(state,550000,open_positions=1))['decision'],'active')
        self.assertEqual(evaluate(self.doc,state,self.observation(state,450000))['decision'],'breached')
        self.assertEqual(evaluate(self.doc,state,self.observation(state,-1))['decision'],'breached')
    def test_stage_specific_target_never_borrows_phase1_rules(self):
        state=initial_state(self.doc,self.aid,'phase2')
        self.assertEqual(evaluate(self.doc,state,self.observation(state,540000))['decision'],'passed')
        state=initial_state(self.doc,self.aid,'phase1')
        self.assertEqual(evaluate(self.doc,state,self.observation(state,540000))['decision'],'active')
    def test_trailing_floor_persists_high_water_and_cannot_be_reset_by_low_tick(self):
        doc=make_contract(quote(self.config,{**self.choices,'drawdown_type':'trailing','profit_cap':'none'}),self.doc['trader_id'])['document']
        state=initial_state(doc,self.aid,'funded')
        state=evaluate(doc,state,self.observation(state,600000))
        self.assertEqual(state['drawdown_floor'],'550000.00')
        state=evaluate(doc,state,self.observation(state,550000,sequence=2))
        self.assertEqual(state['decision'],'breached');self.assertEqual(state['highest_equity'],'600000.00')
    def test_exact_identity_rejects_old_account_owner_and_hash(self):
        state=initial_state(self.doc,self.aid,'phase1')
        for key in ('account_id','contract_id','trader_id','rules_hash'):
            with self.assertRaises(ValueError):evaluate(self.doc,state,self.observation(state,560000,**{key:'wrong'}))
    def test_stale_equal_time_and_replayed_broker_readings_cannot_replace_state(self):
        state=initial_state(self.doc,self.aid,'phase1')
        state=evaluate(self.doc,state,self.observation(state,510000))
        for obs in [self.observation(state,440000),self.observation(state,440000,sequence=2,observed_at=state['last_observed_at'])]:
            self.assertEqual(evaluate(self.doc,state,obs),state)
        passed=evaluate(self.doc,state,self.observation(state,560000,sequence=2))
        self.assertEqual(evaluate(self.doc,passed,self.observation(passed,440000,sequence=3)),passed)
    def test_funded_profit_cap_still_allows_dd_watchdog_after_trading_lock(self):
        state=initial_state(self.doc,self.aid,'funded')
        state=evaluate(self.doc,state,self.observation(state,600000))
        self.assertEqual(state['decision'],'profit_cap_reached')
        state=evaluate(self.doc,state,self.observation(state,440000,sequence=2))
        self.assertEqual(state['decision'],'breached')
    def test_closed_profit_share_cap_and_decimal_rounding(self):
        self.assertEqual(payout_amount(self.doc,200000),'70000.00')
        self.assertEqual(payout_amount(self.doc,'123.45'),'86.42')
        self.assertEqual(payout_amount(self.doc,-100),'0.00')
    def test_replacement_state_starts_fresh_without_altering_predecessor_history(self):
        old=initial_state(self.doc,self.aid,'phase1');old=evaluate(self.doc,old,self.observation(old,440000))
        new=initial_state(self.doc,str(uuid4()),'phase1')
        self.assertEqual(old['decision'],'breached');self.assertEqual(new['decision'],'active')
        self.assertEqual(new['highest_equity'],'500000.00');self.assertNotEqual(new['account_id'],old['account_id'])
    def test_invalid_values_missing_rules_and_naive_time_rejected(self):
        state=initial_state(self.doc,self.aid,'phase1')
        for value in ('NaN','Infinity',None,True):
            with self.assertRaises(ValueError):evaluate(self.doc,state,self.observation(state,value))
        with self.assertRaises(ValueError):evaluate(self.doc,state,self.observation(state,500000,observed_at='2026-10-17T10:00:00'))
        bad=copy.deepcopy(self.doc);del bad['stage_rules']['phase2']
        with self.assertRaises(ValueError):validate_contract(bad)
