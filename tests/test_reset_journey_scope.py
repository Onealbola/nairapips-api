import copy,unittest
from reset_journey_scope import install
class ScopeTests(unittest.TestCase):
 def test_canonical_archived_breach_excludes_unrelated_second_life_without_mutation(self):
  trader={'id':'owner','current_account_id':'new'}
  accounts=[dict(id='new',trader_id='owner',purchase_id='newpurchase',account_status='breached_archived'),dict(id='old',trader_id='owner',purchase_id='oldpurchase')]
  purchases=[{'id':'newpurchase'},{'id':'oldpurchase','second_life_used':True}]
  before=copy.deepcopy((trader,accounts,purchases))
  def original(t,a,p,plans):return {'journey':'second_life' if any(x.get('second_life_used') for x in p) else 'legacy'}
  g={'_np_bootstrap_lifecycle_authority':original};install(g)
  result=g['_np_bootstrap_lifecycle_authority'](trader,accounts,purchases,{})
  self.assertEqual(result['journey'],'legacy');self.assertEqual(result['focus_account_id'],'new');self.assertEqual((trader,accounts,purchases),before)
  trader['current_account_id']='old';self.assertEqual(g['_np_bootstrap_lifecycle_authority'](trader,accounts,purchases,{})['journey'],'second_life')

 def test_bootstrap_includes_current_challenge_action_without_optional_endpoint(self):
  from reset_journey_scope import reset_actions
  from test_phase2_resets import Phase2ResetTests
  fixture=Phase2ResetTests();fixture.setUp();fixture.a['stage']='phase1';fixture.package.update(phase1_reset_enabled=True,phase1_reset_fee=6000)
  source=fixture.a
  def action(a,t):
   policy=fixture.g['_np_reset_policy'](a,t)
   return dict(account_id=a['id'],eligible=policy['eligible'],price=policy.get('price'),kind=policy['kind'])
  g=dict(fixture.g,_np_v69_previous_reset_action=action)
  result=reset_actions(g,{'id':'owner'},[source],[fixture.p],[{'account_id':'old','price':1000}])
  self.assertEqual(next(x for x in result if x['account_id']=='source')['price'],6000)
  self.assertEqual(len(result),2)
  source['reset_consumed_at']='now'
  result=reset_actions(g,{'id':'owner'},[source],[fixture.p],result)
  self.assertFalse(next(x for x in result if x['account_id']=='source')['eligible'])
