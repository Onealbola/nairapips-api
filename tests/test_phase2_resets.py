import ast,copy,re,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from decimal import Decimal
from datetime import datetime,timezone
from flask import Flask,request,jsonify
from package_plan_matrix import defaults,validate_config
from progress_plan_bridge import plan_row,commercial_plan
from progress_lifecycle import install as install_progress
from phase2_resets import install,RELEASE
from plan_commercial import reset_price
from plan_risk_controls import frozen_terms
TREE=ast.parse(Path('app.py').read_bytes())
def extract(name,g,first=False):
 nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name==name]
 node=copy.deepcopy(nodes[0] if first else nodes[-1]);node.decorator_list=[]
 exec(compile(ast.Module(body=[node],type_ignores=[]),'production-reset-functions','exec'),g)
 return g[name]
class Query:
 def __init__(self,db,table):self.db=db;self.table=table;self.filters=[]
 def select(self,*a):return self
 def eq(self,k,v):self.filters.append((k,v));return self
 def limit(self,*a):return self
 def update(self,data):self.change=data;return self
 def insert(self,data):self.add=data;return self
 def execute(self):
  if hasattr(self,'add'):
   row=dict(self.add,id='order');self.db[self.table].append(row);return SimpleNamespace(data=[copy.deepcopy(row)])
  rows=[x for x in self.db[self.table] if all(x.get(k)==v for k,v in self.filters)]
  if hasattr(self,'change'):
   for x in rows:x.update(copy.deepcopy(self.change))
  return SimpleNamespace(data=copy.deepcopy(rows))
class Phase2ResetTests(unittest.TestCase):
 def setUp(self):
  self.doc=defaults();self.plan=self.doc['config']['plans'][0];self.package=self.plan['packages'][0]
  row=plan_row(self.plan,self.package,True)
  self.p=dict(row,id='purchase',plan_id=row['id'],trader_id='owner',payment_status='approved',status='approved_active')
  self.parent=dict(id='phase1',trader_id='owner',purchase_id='purchase',stage='phase1',mt5_login='first',account_status='archived_phase1',phase_pass_status='phase1_passed',passed_at='2026-11-12T09:00:00Z',archive_reason='[NP_LIVE_PASS_V214]')
  self.a=dict(id='source',trader_id='owner',purchase_id='purchase',stage='phase2',account_size=500000,mt5_login='old',account_status='breached_archived',breached_at='2026-11-12T10:00:00Z',archive_reason='DAILY_DRAWDOWN_BREACH')
  self.chain=[self.parent,self.a];self.order=None;self.child=None;self.base_assign=Mock(return_value=({'id':'fresh','stage':'phase2'},{}));self.old_policy=Mock(return_value={'eligible':False,'reason':'legacy'})
  self.db={'trader_accounts':self.chain,'challenge_purchases':[self.p]}
  self.g=dict(re=re,clean=lambda x:float(x or 0),_normalize_lifecycle_stage=lambda x:x,_np_reset_stage=lambda a:a['stage'],
   _np_reset_account_is_breached=lambda a:bool(a.get('breached_at')),_np_reset_purchase_for_account=lambda a:copy.deepcopy(self.p),
   _np_reset_current_plan_for=lambda *a:None,_np_reset_price=reset_price,_np_reset_bool=bool,_second_life_bool=bool,
   _np_reset_open_order=lambda a:copy.deepcopy(self.order),_np_reset_policy=self.old_policy,
   _np_funded_reset_child_already_used=lambda a:False,
   _np_plan_matrix_store=SimpleNamespace(read=lambda **kw:(copy.deepcopy(self.doc),None)),
   _journey_for_lifecycle=lambda *a:('phase1','funded'),_next_stage_for_lifecycle=lambda *a:'funded',
   _journey_source_value=lambda *a:[],_assign_mt5_to_trader=self.base_assign,
   _np_v214_rows=self.read,_np_reset_existing_child_v43=lambda *a:self.child,
   supabase=SimpleNamespace(table=lambda t:Query(self.db,t)),get_trader_by_id=lambda t:{'id':t},
   _np_v71_nonempty_proof=lambda v:bool(v),_np_kill_blob=lambda a:str(a.get('archive_reason') or '').lower())
  self.g['_np_reset_policy_v60_core']=extract('_np_reset_policy',self.g,first=True)
  extract('_np_verify_exact_paid_reset_order',self.g);extract('_np_v71_paid_reset_order_firewall',self.g)
  self.g['_np_reset_order_refs_v43']=lambda o:dict(source_account_id='source',parent_purchase_id='purchase',stage='phase2')
  install_progress(self.g);install(self.g)
 def read(self,table,p):
  if table=='trader_accounts':return copy.deepcopy(self.chain)
  return [copy.deepcopy(x) for x in self.db['challenge_purchases'] if ('id' not in p or p['id']=='eq.'+x['id']) and ('trader_id' not in p or p['trader_id']=='eq.'+x['trader_id'])]
 def policy(self):return self.g['_np_reset_policy'](self.a,{'id':'owner'})
 def enable(self,price=8750):self.package.update(phase2_reset_enabled=True,phase2_reset_fee=price)
 def paid(self):
  self.order=dict(id='order',trader_id='owner',payment_status='approved',status='approved_reset_waiting_mt5',approved_at='now',fee=8750,payment_proof_url='proof',admin_note='[NP_RESET_REQUEST:source:purchase:phase2] [NP_ENTITLEMENT:challenge_reset_paid:order]')
  self.a.update(account_status='archived_reset_phase2',archive_reason=self.a['archive_reason']+' [NP_ENTITLEMENT:challenge_reset_paid:order]')
  self.db['challenge_purchases'].append(self.order)
 def assign(self):return self.g['_assign_mt5_to_trader']({'id':'owner'},{'id':'new','pool_class':'phase'},'phase2',self.p,{'role':'system'},'PHASE2 RESET APPROVED order=order source=source')
 def test_owner_price_enables_exact_current_offer_without_changing_issued_terms(self):
  before=copy.deepcopy(self.p);self.assertFalse(self.policy()['eligible']);self.enable();policy=self.policy()
  self.assertEqual((policy['kind'],policy['price'],policy['stage']),('paid_challenge',8750,'phase2'));self.assertEqual(policy['authority'],RELEASE)
  self.assertEqual(self.p,before);self.enable(9500);self.assertEqual(self.policy()['price'],9500)
  self.package['phase2_reset_enabled']=False;self.assertFalse(self.policy()['eligible'])
 def test_same_price_is_in_saved_catalogue_and_phase2_is_not_phase1_price(self):
  self.enable();self.package.update(phase1_reset_enabled=True,phase1_reset_fee=4000)
  saved=validate_config(copy.deepcopy(self.doc['config']));p=saved['plans'][0];row=plan_row(p,p['packages'][0],True)
  self.assertEqual(commercial_plan(row)['reset_stage_prices'],{'phase1':'4E+3','phase2':'8.75E+3'})
  self.assertEqual(reset_price(row,'phase2'),8750);self.assertEqual(reset_price(row,'phase1'),4000)
 def test_healthy_foreign_and_used_sources_never_get_a_paid_reset(self):
  self.enable();self.a['breached_at']=None;self.assertFalse(self.policy()['eligible']);self.a['breached_at']='now'
  self.p['challenge_reset_used']=True;self.assertFalse(self.policy()['eligible']);self.p.pop('challenge_reset_used')
  with self.assertRaises(ValueError):self.g['_np_reset_policy'](self.a,{'id':'foreign'})
  self.p['trader_id']='foreign'
  with self.assertRaises(ValueError):self.policy()
 def test_exact_offer_only_no_borrowing_from_sibling_or_changed_model(self):
  self.enable();self.package['key']='removed';self.assertFalse(self.policy()['eligible'])
  self.package['key']='basic';self.plan['capital']=1000000
  with self.assertRaises(ValueError):self.policy()
 def test_pending_proof_and_approved_waiting_keep_original_quote(self):
  self.enable();self.order=dict(id='order',payment_status='pending',fee=8000)
  self.assertEqual((self.policy()['kind'],self.policy()['price']),('payment_pending',8000))
  self.order['payment_status']='approved';self.assertEqual(self.policy()['kind'],'waiting');self.assertEqual(self.policy()['price'],8000)
 def test_exact_approved_proof_allows_same_stage_replacement_then_blocks_replay(self):
  self.enable();before=copy.deepcopy(self.p);self.paid();self.assign();self.base_assign.assert_called_once();self.assertEqual(self.p,before)
  self.assertEqual(self.a['breached_at'],'2026-11-12T10:00:00Z')
  self.child={'id':'fresh','mt5_login':'new'}
  with self.assertRaises(ValueError):self.assign()
 def test_unpaid_missing_proof_wrong_source_and_later_funded_cannot_assign(self):
  self.enable()
  with self.assertRaises(ValueError):self.assign()
  self.paid()
  for field,value in [('payment_status','pending'),('payment_proof_url',None),('approved_at',None),('status','completed'),('trader_id','foreign')]:
   old=self.order[field];self.order[field]=value
   with self.assertRaises(ValueError):self.assign()
   self.order[field]=old
  self.chain.append(dict(self.a,id='funded',stage='funded'))
  with self.assertRaises(ValueError):self.assign()
  self.base_assign.assert_not_called()
 def test_phase1_configured_reset_requires_exact_paid_proof_and_never_reopens_source(self):
  self.a['stage']='phase1';self.chain.remove(self.parent)
  self.package.update(phase1_reset_enabled=True,phase1_reset_fee=6000)
  self.assertEqual((self.policy()['kind'],self.policy()['price']),('paid_challenge',6000))
  self.g['_np_reset_order_refs_v43']=lambda o:dict(source_account_id='source',parent_purchase_id='purchase',stage='phase1')
  def assign():return self.g['_assign_mt5_to_trader']({'id':'owner'},{'id':'new','pool_class':'phase'},'phase1',self.p,{'role':'system'},'paid reset')
  with self.assertRaises(ValueError):assign()
  self.paid();self.order.update(fee=6000,admin_note='[NP_RESET_REQUEST:source:purchase:phase1] [NP_ENTITLEMENT:challenge_reset_paid:order]')
  assign();self.base_assign.assert_called_once();self.assertTrue(self.a['breached_at'])
  self.child={'id':'fresh','mt5_login':'new'}
  with self.assertRaises(ValueError):assign()
 def test_consumed_source_blocks_assignment_even_when_child_lookup_or_mirror_fields_are_missing(self):
  self.enable();self.paid()
  self.a['archive_reason']+=' | reset_consumed replacement_account_id=already-issued replacement_mt5=123'
  self.child=None
  with self.assertRaises(ValueError):self.assign()
  self.base_assign.assert_not_called()
 def test_legacy_routes_keep_their_existing_policy(self):
  self.p['journey_source']='legacy';self.assertEqual(self.policy()['reason'],'reset_not_available')
 def test_real_payment_creation_approval_inventory_retry_and_consumption_flow(self):
  self.enable();app=Flask(__name__);g=self.g
  g.update(request=request,datetime=datetime,timezone=timezone,now_iso=lambda:datetime.now(timezone.utc).isoformat(),
   month=lambda:'2026-11',year=lambda:2026,_authenticated_trader_id_for_request=lambda t:('owner',None),
   _np_ok=lambda d,*a:jsonify(dict(success=True,**{k:v for k,v in d.items() if k!='success'})),ok=lambda d,*a:jsonify(dict(success=True,**{k:v for k,v in d.items() if k!='success'})),
   _np_fail=lambda e,status=400:(jsonify(success=False,error=str(e)),status),bad=lambda e,status=400:(jsonify(success=False,error=str(e)),status),
   _audit_safe=Mock(),send_admin_alert=Mock(),send_email_safe=Mock(),send_telegram=Mock(),
   _admin_from_payload=lambda d:{'role':'admin'},np_invalidate_admin_bootstrap=Mock(),email_money=str)
  g['_np_reset_open_order']=lambda aid:next((copy.deepcopy(x) for x in self.db['challenge_purchases'] if '[NP_RESET_REQUEST:'+aid+':' in str(x.get('admin_note')) and x.get('status') not in ('rejected','cancelled','completed')),None)
  create=extract('create_reset_purchase',g,first=True)
  with app.test_request_context(json={'trader_id':'owner','trader_account_id':'source','payment_proof_url':'proof'}):
   result=create();self.assertFalse(isinstance(result,tuple),result[0].json if isinstance(result,tuple) else '');self.assertTrue(result.json['success'])
  self.order=self.db['challenge_purchases'][-1]
  self.assertEqual((self.order['fee'],self.order['payment_status']),(8750,'pending'));self.base_assign.assert_not_called()
  with app.test_request_context(json={'trader_id':'owner','trader_account_id':'source','payment_proof_url':'proof'}):
   self.assertEqual(create()[1],409)
  approve=extract('_np_approve_reset_purchase_payment',g,first=True)
  with app.app_context():
   response=approve(dict(self.order,reset_source_account_id='source',reset_parent_purchase_id='purchase'),{})
   self.assertTrue(response.json['success'])
  self.assertEqual(self.order['status'],'approved_reset_waiting_mt5');self.assertTrue(self.p['challenge_reset_used']);self.base_assign.assert_not_called()
  g.update(_np_pick_fresh_mt5=Mock(return_value=None),_np_assert_mt5_pool_matches_stage=Mock(),_np_mt5_auto_eligible=lambda *a:(True,'fresh'))
  extract('_np_mark_reset_consumed_v43',g)
  core=extract('_np_auto_fulfill_exact_reset_v43',g,first=True);g['_np_auto_fulfill_exact_reset_v71_core']=core
  worker=extract('_np_auto_fulfill_exact_reset_v43',g)
  self.assertEqual(worker(copy.deepcopy(self.order))['status'],'waiting_inventory');self.base_assign.assert_not_called()
  self.assertEqual(self.order['status'],'approved_reset_waiting_mt5')
  g['_np_pick_fresh_mt5'].return_value={'id':'fresh-pool','mt5_login':'fresh','pool_class':'phase'}
  def assigned(t,m,stage,p,*rest):
   self.assertEqual(stage,'phase2');self.assertEqual(p['id'],'purchase');self.child=dict(self.a,id='fresh',mt5_login='fresh',account_status='assigned_active',breached_at=None);self.chain.append(self.child);return self.child,t
  self.base_assign.side_effect=assigned
  self.assertEqual(worker(copy.deepcopy(self.order))['status'],'assigned');self.base_assign.assert_called_once()
  self.assertEqual(g['_np_pick_fresh_mt5'].call_args.args,(500000.0,'phase2'))
  self.assertEqual(self.order['status'],'completed');self.assertEqual(self.a['reset_replacement_account_id'],'fresh')
  self.assertTrue(self.a['breached_at']);self.assertEqual(self.parent['account_status'],'archived_phase1')
  self.assertEqual(worker(copy.deepcopy(self.order))['status'],'blocked');self.base_assign.assert_called_once()
if __name__=='__main__':unittest.main()
