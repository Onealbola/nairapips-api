import copy,json,unittest
from unittest.mock import Mock
from decimal import Decimal
from plan_risk_controls import freeze
from equity_drawdown import spec,initial,evaluate
from daily_breach_corrections import PREFIX,marker,voided,evidence,retry_pending
PID='11111111-1111-4111-8111-111111111111';TID='22222222-2222-4222-8222-222222222222';AID='33333333-3333-4333-8333-333333333333';PARENT='44444444-4444-4444-8444-444444444444'
class CorrectionTests(unittest.TestCase):
 def setUp(self):
  source=freeze('terms',dict(daily_drawdown='5',payout_frequency='Weekly'))
  self.purchase=dict(id=PID,trader_id=TID,account_size=1500000,payment_status='approved',status='approved_waiting_mt5',trader_account_id=None,journey_source=source,drawdown_type='STATIC')
  self.trader=dict(id=TID,current_account_id=None)
  self.account=dict(id=AID,purchase_id=PID,trader_id=TID,stage='phase2',account_size=1500000,start_balance=1500000,dd_limit_percent=20,created_at='2026-11-12T09:19:48+00:00',mt5_login='old',account_status='archived',archive_reason=marker(PID,TID,AID),journey_source=source)
  self.parent=dict(id=PARENT,purchase_id=PID,trader_id=TID,stage='phase1',account_status='archived_phase1',passed_at='2026-11-12T09:19:23+00:00',mt5_login='parent')
  rules=spec(self.account,self.purchase);self.journal=evaluate(rules,initial(rules),Decimal('1484783.01'),'2026-11-12T14:42:50+00:00',balance=Decimal('1564920.76'),daily_realised=0)
  self.trade=dict(ticket='H-profit',trader_id=TID,trader_account_id=AID,profit=64920.76,closed_at='2026-11-12T10:21:41+00:00')
  self.case=dict(source_account_id=AID,purchase_id=PID,trader_id=TID,target_stage='phase2',account_size='1500000',parent_account_id=PARENT,authorized_by='owner_instruction',state='waiting_inventory')
  self.chain=[self.parent,self.account];self.payouts=[];self.save=Mock(return_value=True);self.pick=Mock(return_value={'mt5_login':'fresh'});self.assign=Mock(return_value=({'id':'new','mt5_login':'fresh'},{}))
 def read(self,table,params):
  if table=='package_studio_private_settings':
   return [dict(key=PREFIX+AID,value=json.dumps(self.case))] if params['key'].startswith('like.') else [dict(value=json.dumps(self.journal))]
  if table=='challenge_purchases':return [copy.deepcopy(self.purchase)]
  if table=='traders':return [copy.deepcopy(self.trader)]
  if table=='trader_accounts':return copy.deepcopy(self.chain)
  if table=='trader_trades':return [copy.deepcopy(self.trade),copy.deepcopy(self.trade)]
  if table=='payouts':return self.payouts
  raise AssertionError(table)
 def run_case(self):return retry_pending(self.read,self.save,self.pick,self.assign)
 def test_replay_uses_exact_profit_and_excludes_replayed_trade_duplicates(self):
  e=evidence(self.read,self.account,self.purchase);self.assertEqual(e['closed_profit'],'64920.76');self.assertEqual(Decimal(e['correct_daily_loss_percent']),Decimal('1.014466'))
 def test_only_exact_audited_delivery_is_hidden_and_phase2_is_retained_while_waiting(self):
  from assignment_corrections import trader_view
  self.assertTrue(voided(self.account));self.assertFalse(voided(dict(self.account,id=PARENT)))
  root=trader_view(dict(all_accounts=self.chain,current_account=self.account,trader={'current_account_id':AID,'phase':'phase2'}))
  self.assertEqual(root['all_accounts'],[self.parent]);self.assertEqual(root['trader']['phase'],'phase2');self.assertEqual(root['trader']['challenge_state'],'phase2_waiting_mt5')
 def test_exact_owner_approved_case_uses_existing_phase2_assigner_once(self):
  result=self.run_case();self.assertEqual(result['assigned'],1);self.pick.assert_called_once_with(1500000.0,'phase2');self.assertEqual(self.assign.call_args.args[2],'phase2');self.assertEqual(self.assign.call_args.args[3]['id'],PID);self.assertEqual(self.save.call_args.args[1]['replacement_account_id'],'new')
 def test_missing_stock_does_not_use_funded_stock_or_issue_any_account(self):
  self.pick.return_value=None;self.assertEqual(self.run_case()['waiting_inventory'],1);self.assign.assert_not_called()
 def test_genuine_breach_missing_profit_and_wrong_owner_block(self):
  for change in ['no_profit','wrong_owner','funded','genuine']:
   test=CorrectionTests();test.setUp()
   if change=='no_profit':test.trade['profit']=0
   if change=='wrong_owner':test.trade['trader_id']=PARENT
   if change=='funded':test.account['stage']='funded'
   if change=='genuine':test.journal['first_breach']['equity']='1200000'
   self.assertEqual(test.run_case()['errors'],1);test.assign.assert_not_called()
 def test_unexpected_live_successor_or_changed_payment_blocks(self):
  self.chain.append(dict(self.parent,id='other',stage='funded'));self.assertEqual(self.run_case()['errors'],1);self.assign.assert_not_called()
  self.chain.pop();self.purchase['payment_status']='pending';self.assertEqual(self.run_case()['errors'],1);self.assign.assert_not_called()
 def test_lost_claim_and_uncertain_writes_never_issue_duplicate(self):
  self.save.return_value=False;self.run_case();self.assign.assert_not_called()
  self.save.return_value=True;self.assign.side_effect=RuntimeError('uncertain');self.assertEqual(self.run_case()['errors'],1);self.assertEqual(self.save.call_args.args[1]['state'],'needs_review')
 def test_existing_replacement_is_reconciled_without_reallocation(self):
  self.chain.append(dict(self.parent,id='new',stage='phase2',account_status='assigned_active'))
  self.assertEqual(self.run_case()['already_fulfilled'],1);self.assign.assert_not_called()
 def test_payout_link_requires_manual_financial_review(self):
  self.payouts=[{'id':'payout'}];self.assertEqual(self.run_case()['errors'],1);self.assign.assert_not_called()
