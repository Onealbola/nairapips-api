import unittest,copy
from recorded_phase_pass import verify
class RecordedPassTests(unittest.TestCase):
 def setUp(self):
  self.a=dict(id='11111111-1111-4111-8111-111111111111',trader_id='22222222-2222-4222-8222-222222222222',purchase_id='p',mt5_login='123',stage='phase1',account_status='archived_phase1',phase_pass_status='phase1_passed',passed_at='2026-11-12T00:11:21Z',account_size=1500000,start_balance=1500000,dd_limit_percent=5,lowest_equity=1496633.82,worst_dd_used_percent=4.49)
  self.p=dict(id='p',trader_id=self.a['trader_id'],account_size=1500000,max_drawdown=5,phase1_target=10,phase2_target=8,drawdown_type='STATIC')
  self.live=dict(trader_account_id=self.a['id'],trader_id=self.a['trader_id'],mt5_login='123',stage='phase1',account_size=1500000,balance=1650204.2,equity=1650204.2,observed_at='2026-11-12T00:11:22Z',dd_used_percent=0,risk_zone='live')
 def test_exact_saved_monitor_pass_is_valid_without_an_alternate_writer_marker(self):self.assertTrue(verify(self.a,self.p,self.live))
 def test_below_target_foreign_or_unbound_observations_are_rejected(self):
  for patch in ({'equity':1649999.99},{'trader_account_id':'other'},{'trader_id':'other'},{'mt5_login':'other'},{'stage':'phase2'},{'account_size':500000},{'observed_at':'2026-11-12T05:00:00Z'},{'dd_used_percent':100},{'risk_zone':'breached'},{'equity':'NaN'}):
   self.assertFalse(verify(self.a,self.p,{**self.live,**patch}),patch)
 def test_permanent_and_historical_breach_evidence_always_blocks_pass(self):
  for patch in ({'breached_at':'now'},{'account_status':'breached_archived'},{'lowest_equity':1425000},{'worst_dd_used_percent':100},{'phase_pass_status':'unknown'}):self.assertFalse(verify({**self.a,**patch},self.p,self.live),patch)
 def test_daily_or_trailing_requires_a_matching_non_breached_durable_journal(self):
  from plan_risk_controls import freeze
  from equity_drawdown import spec,fingerprint
  self.a['journey_source']=freeze('purchase',dict(daily_drawdown='5',payout_frequency='Weekly'))
  self.p['journey_source']=self.a['journey_source']
  self.assertFalse(verify(self.a,self.p,self.live))
  journal=dict(rules_hash=fingerprint(spec(self.a,self.p)),breached=False,worst_used_percent='20')
  self.assertTrue(verify(self.a,self.p,self.live,journal));journal['breached']=True;self.assertFalse(verify(self.a,self.p,self.live,journal))
if __name__=='__main__':unittest.main()
