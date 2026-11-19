import copy,unittest,ast
from pathlib import Path
from recalled_account_visibility import recalled,trader_view

class RecallVisibilityTests(unittest.TestCase):
 def setUp(self):
  self.held=dict(id='held',trader_id='owner',account_status='archived',mt5_login='477761875',mt5_master_password='never-expose',archive_reason='[NP_MANAGEMENT_HOLD:ACTIVE] [NP_MH_ACCOUNT:held]')
  self.fresh=dict(id='fresh',trader_id='owner',account_status='assigned_active',mt5_login='fresh-login',admin_note='[NP_UNIVERSAL_RECALL:v] [NP_RECALL_ORIGINAL_ACCOUNT:held]')
 def test_both_workflows_hide_only_the_exact_terminal_delivery(self):
  self.assertTrue(recalled(self.held))
  self.assertTrue(recalled(dict(self.held,archive_reason='[NP_UNIVERSAL_RECALL:v] [NP_RECALL_ORIGINAL_ACCOUNT:held]')))
  self.assertTrue(recalled(dict(self.held,archive_reason=self.held['archive_reason']+' [NP_MANAGEMENT_HOLD:RETURNED]')))
  self.assertTrue(recalled(dict(self.held,archive_reason='',recalled_delivery=True)))
  for row in [self.fresh,dict(self.held,account_status='assigned_active'),dict(self.held,id='different'),dict(self.held,archive_reason='ordinary archive'),dict(self.held,account_status='breached_archived'),dict(self.held,account_status='archived_phase1')]:self.assertFalse(recalled(row),row)
 def test_every_list_single_and_stale_credential_mirror_is_cleaned_without_mutating_input_rows(self):
  old=copy.deepcopy(self.held);history=dict(id='history',account_status='breached_archived')
  payload=dict(data=[self.held,self.fresh,history],accounts=[self.held,self.fresh,history],all_accounts=[self.held],account=self.held,current_account=self.fresh,count=3,trader=dict(current_account_id='held',mt5_login='477761875',mt5_master_password='stale'),previous_reset_actions=[dict(account_id='held'),dict(account_id='history')],latest_trades=[dict(trader_account_id='held')])
  result=trader_view(payload)
  self.assertEqual(result['data'],[self.fresh,history]);self.assertEqual(result['accounts'],[self.fresh,history]);self.assertEqual(result['count'],2)
  self.assertIsNone(result['account']);self.assertEqual(result['current_account'],self.fresh)
  self.assertEqual(result['trader']['current_account_id'],'fresh');self.assertIsNone(result['trader']['mt5_master_password'])
  self.assertEqual(result['previous_reset_actions'],[dict(account_id='history')]);self.assertEqual(result['latest_trades'],[])
  self.assertEqual(result['recalled_excluded_account_ids'],['held']);self.assertEqual(self.held,old)
 def test_ordinary_pass_breach_reset_and_archived_history_remain(self):
  rows=[dict(id=s,account_status=s) for s in ['archived','archived_phase1','archived_phase2','breached_archived','archived_reset']]
  root={'accounts':rows};self.assertEqual(trader_view(root),{'accounts':rows})
 def test_production_wrapper_filters_trader_responses_and_preserves_admin_audit(self):
  import json
  from flask import Flask,request
  from unittest.mock import patch
  tree=ast.parse(Path('app.py').read_bytes());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v204_live_refresh_response')
  app=Flask(__name__);env=dict(app=app,request=request,json=json,requests=object(),SUPABASE_URL='fixture',SUPABASE_SERVICE_ROLE_KEY='fixture',SUPABASE_KEY='fixture',_np_v198_is_live_row=lambda r:False)
  exec(compile(ast.Module(body=[node],type_ignores=[]),'refresh','exec'),env)
  for endpoint,trader_id,hidden in [('trader_bootstrap',False,True),('admin_trader_accounts_feed',True,True),('admin_trader_accounts_feed',False,False)]:
   @app.route('/'+endpoint+str(trader_id),endpoint=endpoint+str(trader_id))
   def unused():return ''
   with app.test_request_context('/'+endpoint+str(trader_id)+('?trader_id=owner' if trader_id else '')):
    request.url_rule.endpoint=endpoint
    payload=dict(accounts=[self.held,self.fresh],data=[self.held,self.fresh])
    with patch('admin_account_live.overlay_account_lists',side_effect=lambda aliases,*a:aliases):
     result=env['_np_v204_live_refresh_response'](app.json.response(payload)).get_json()
    self.assertEqual(any(r['id']=='held' for r in result['accounts']),not hidden)

 def test_safe_row_keeps_visibility_flag_without_exposing_internal_markers(self):
  tree=ast.parse(Path('app.py').read_bytes());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_trader_safe_account_row')
  env=dict(_trader_account_progression_metadata=lambda row:{},_only_fields=lambda row,fields:{k:v for k,v in row.items() if k in fields},_TRADER_ACCOUNT_RESPONSE_FIELDS={'id','account_status'},_trader_safe_monitoring_row=lambda r:r)
  exec(compile(ast.Module(body=[node],type_ignores=[]),'safe_row','exec'),env)
  safe=env['_trader_safe_account_row'](self.held)
  self.assertTrue(safe['recalled_delivery']);self.assertNotIn('archive_reason',safe)
  self.assertTrue(recalled(safe));self.assertFalse(env['_trader_safe_account_row'](self.fresh)['recalled_delivery'])

 def test_purchase_mirrors_lose_only_recalled_credentials_not_purchase_terms(self):
  purchase=dict(id='purchase',trader_account_id='held',mt5_login='477761875',mt5_master_password='revoked',max_drawdown=5,fee=35000)
  out=trader_view(dict(accounts=[self.held,self.fresh],purchases=[purchase]))
  self.assertIsNone(out['purchases'][0]['mt5_master_password']);self.assertIsNone(out['purchases'][0]['mt5_login'])
  self.assertEqual(out['purchases'][0]['max_drawdown'],5);self.assertEqual(out['purchases'][0]['fee'],35000)
  self.assertEqual(purchase['mt5_master_password'],'revoked')
