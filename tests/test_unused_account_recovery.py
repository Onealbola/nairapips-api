import unittest,copy,tempfile,os,json
from unittest.mock import Mock,patch
from types import SimpleNamespace as N
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unused_account_recovery import prove,coherent,install
from equity_drawdown import EquityDrawdownJournal,spec,initial
from plan_risk_controls import freeze
class RecoveryTests(unittest.TestCase):
 def setUp(self):
  self.row=dict(id='0f2140b0-cfbf-4f4b-a722-5e1b67ed5632',trader_id='f1ab5917-30c7-4473-985d-c37776b8c59a',purchase_id='p',mt5_login='486190789',mt5_server='Exness-MT5Trial10',stage='phase1',account_status='assigned_active',start_balance=1500000,dd_limit_percent=20,drawdown_type='STATIC',started_at=(datetime.now(timezone.utc)-timedelta(hours=3)).isoformat(),journey_source=freeze('test',dict(daily_drawdown='5',payout_frequency='Weekly')))
  self.row['drawdown_rules']=spec(self.row)
  self.info=N(login=486190789,server='Exness-MT5Trial10',balance=1500000,equity=1500000,profit=0,credit=0)
  self.deal=N(ticket=1,type=2,profit=1500000,commission=0,swap=0,fee=0,time_msc=1,time=0)
  self.mt5=Mock();self.mt5.account_info.return_value=self.info;self.mt5.terminal_info.return_value=N(connected=True);self.mt5.history_deals_get.return_value=[self.deal];self.mt5.positions_get.return_value=();self.mt5.orders_get.return_value=()
 def test_zero_equity_against_funded_flat_balance_is_incoherent(self):
  self.info.equity=0;self.assertFalse(coherent(self.info))
  with self.assertRaises(ValueError):prove(self.mt5,self.row)
 def test_only_exact_never_traded_static_daily_funding_is_accepted(self):
  self.assertEqual(prove(self.mt5,self.row)['trading_deals'],0)
  for kind in (0,1,3,4,6):
   self.deal.type=kind
   with self.assertRaises(ValueError):prove(self.mt5,self.row)
 def test_wrong_identity_unavailable_history_open_exposure_and_terminal_accounts_refused(self):
  for change in ['login','history','positions','orders','funding','breach','trailing']:
   self.setUp()
   if change=='login':self.info.login=1
   if change=='history':self.mt5.history_deals_get.return_value=None
   if change=='positions':self.mt5.positions_get.return_value=(N(ticket=9),)
   if change=='orders':self.mt5.orders_get.return_value=None
   if change=='funding':self.deal.profit=1400000
   if change=='breach':self.row['breached_at']='recorded'
   if change=='trailing':self.row['drawdown_rules']['drawdown_type']='TRAILING'
   with self.assertRaises(ValueError,msg=change):prove(self.mt5,self.row)
 def test_recovers_missing_unused_journal_only_and_keeps_confirmed_breach_terminal(self):
  g=dict(verified_live_info=lambda *a:self.info,EquityDrawdownJournal=EquityDrawdownJournal,mt5=self.mt5,log=Mock(),_np_unused_cloud_seed=lambda g,row,proof:dict(initial(row['drawdown_rules']),unused_account_recovery=proof));install(g)
  with tempfile.TemporaryDirectory() as folder,patch.dict(os.environ,{'NAIRAPIPS_DD_LATCH_ROOT':folder}):
   j=g['EquityDrawdownJournal'](folder);at=datetime.now(timezone.utc)
   state=j.assess(self.row,1500000,at.isoformat(),now=at,balance=1500000,daily_realised=0)
   self.assertFalse(state['breached']);self.assertEqual(state['unused_account_recovery']['trading_deals'],0)
   at+=timedelta(seconds=1);state=j.assess(self.row,1420000,at.isoformat(),now=at,balance=1500000,daily_realised=0);self.assertTrue(state['breached'])
   at+=timedelta(seconds=1);state=j.assess(self.row,1500000,at.isoformat(),now=at,balance=1500000,daily_realised=0);self.assertTrue(state['breached'])
 def test_latch_or_existing_corrupt_journal_is_never_overwritten(self):
  g=dict(verified_live_info=lambda *a:self.info,EquityDrawdownJournal=EquityDrawdownJournal,mt5=self.mt5,log=Mock(),_np_unused_cloud_seed=lambda g,row,proof:dict(initial(row['drawdown_rules']),unused_account_recovery=proof));install(g)
  for kind in ['latch','corrupt']:
   with tempfile.TemporaryDirectory() as folder,patch.dict(os.environ,{'NAIRAPIPS_DD_LATCH_ROOT':folder}):
    p=Path(folder)/(self.row['id']+'.json')
    if kind=='corrupt':p.write_text('broken')
    else:(Path(folder)/'breach_latch_shard_01.json').write_text(json.dumps({self.row['mt5_login']:True}))
    with self.assertRaises(ValueError):g['EquityDrawdownJournal'](folder).assess(self.row,1500000,datetime.now(timezone.utc).isoformat(),balance=1500000,daily_realised=0)
    if kind=='corrupt':self.assertEqual(p.read_text(),'broken')
    else:self.assertFalse(p.exists())

 def test_incoherent_authentication_is_rejected_for_next_credential_fallback(self):
  g=dict(verified_live_info=lambda *a:self.info,EquityDrawdownJournal=EquityDrawdownJournal,mt5=self.mt5,log=Mock(),_np_unused_cloud_seed=lambda g,row,proof:dict(initial(row['drawdown_rules']),unused_account_recovery=proof));install(g)
  self.info.equity=0;self.assertIsNone(g['verified_live_info']('486190789','Exness-MT5Trial10','unused',self.row))
  self.info.equity=1500000;self.assertIs(g['verified_live_info']('486190789','Exness-MT5Trial10','unused',self.row),self.info)
 def test_no_journal_is_created_from_incoherent_assessment_even_if_later_proof_is_valid(self):
  g=dict(verified_live_info=lambda *a:self.info,EquityDrawdownJournal=EquityDrawdownJournal,mt5=self.mt5,log=Mock(),_np_unused_cloud_seed=lambda g,row,proof:dict(initial(row['drawdown_rules']),unused_account_recovery=proof));install(g)
  with tempfile.TemporaryDirectory() as folder,patch.dict(os.environ,{'NAIRAPIPS_DD_LATCH_ROOT':folder}):
   with self.assertRaises(ValueError):g['EquityDrawdownJournal'](folder).assess(self.row,0,datetime.now(timezone.utc).isoformat(),balance=1500000,daily_realised=0)
   self.assertFalse((Path(folder)/(self.row['id']+'.json')).exists())
 def test_download_has_exact_module_hash_and_only_restarts_worker_one(self):
  import hashlib
  from flask import Flask
  from panel_delivery import register_panel_delivery
  app=Flask(__name__);register_panel_delivery(app)
  client=app.test_client();response=client.get('/downloads/unused-account-recovery.py');self.assertEqual(response.status_code,200);data=response.data;response.close()
  response=client.get('/downloads/unused-account-recovery.ps1');self.assertEqual(response.status_code,200);text=response.get_data(as_text=True);response.close()
  self.assertIn(hashlib.sha256(data).hexdigest(),text);self.assertIn('--shard\\s+1',text);self.assertNotIn('Remove-Item',text)
 def test_cloud_seed_validates_exact_collector_proof_and_refuses_terminal_or_stale(self):
  from unused_account_seed import checked_seed
  self.row['monitoring_enabled']=True
  proof=prove(self.mt5,self.row)
  self.assertEqual(checked_seed(self.row,proof)['rules_hash'],self.row['drawdown_rules'] and spec(self.row) and proof['rules_hash'])
  for key,value in [('account_id','other'),('equity','0'),('trading_deals',1),('positions',1),('rules_hash','wrong'),('funding_tickets',[])]:
   altered=dict(proof,**{key:value})
   with self.assertRaises(ValueError):checked_seed(self.row,altered)
  with self.assertRaises(ValueError):checked_seed(dict(self.row,breached_at='recorded'),proof)
  with self.assertRaises(ValueError):checked_seed(self.row,proof,datetime.now(timezone.utc)+timedelta(minutes=2))
 def test_cloud_route_requires_collector_auth_and_preserves_existing_terminal_journal(self):
  from unused_account_seed import install as install_seed
  from flask import Flask
  self.row['monitoring_enabled']=True;proof=prove(self.mt5,self.row);app=Flask(__name__)
  http=Mock();response=http.get.return_value;response.json.return_value=[{'value':json.dumps(dict(initial(spec(self.row)),breached=True))}]
  g=dict(_np_query_rows_v173=lambda *a,**kw:[self.row],requests=http,SUPABASE_URL='https://fixture.invalid',SUPABASE_KEY='fixture')
  install_seed(app,g);client=app.test_client()
  with patch('unused_account_seed.verify_ingest',side_effect=ValueError('auth')):self.assertEqual(client.post('/collector/unused-account-journal',json={'account_id':self.row['id'],'proof':proof}).status_code,401)
  with patch('unused_account_seed.verify_ingest'):
   self.assertEqual(client.post('/collector/unused-account-journal',json={'account_id':self.row['id'],'proof':proof}).status_code,409)
  http.post.assert_not_called();http.patch.assert_not_called()
