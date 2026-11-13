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
