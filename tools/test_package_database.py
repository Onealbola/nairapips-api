"""Real PostgreSQL transaction tests. Only an isolated labelled Docker DB is used.

Run: python tools/test_package_database.py
No production URL, credentials, customer records or broker operations are used.
"""
import json
import pathlib
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from package_configuration import defaults,quote
from package_contracts import make_contract
IMAGE='postgres@sha256:0ea6700a3b4f0ae6ce746519073558aed4d88a79d8d07622a9a644946c7319c4'


def literal(value):
    return "'"+str(value).replace("'","''")+"'"

class PackageDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container='np-package-test-'+uuid4().hex[:10]
        subprocess.run(['docker','run','--rm','-d','--name',cls.container,'--label','nairapips.test=package-contracts',
                        '--network','none','--memory','512m','--cpus','1','--tmpfs','/var/lib/postgresql/data:rw,size=256m',
                        '-e','POSTGRES_HOST_AUTH_METHOD=trust',IMAGE],check=True,capture_output=True,text=True)
        try:
            for _ in range(150):
                logs=subprocess.run(['docker','logs',cls.container],capture_output=True,text=True)
                initialized='PostgreSQL init process complete; ready for start up.' in logs.stdout+logs.stderr
                ready=subprocess.run(['docker','exec',cls.container,'pg_isready','-U','postgres'],capture_output=True)
                if initialized and ready.returncode==0:break
                time.sleep(.1)
            else:raise RuntimeError('Isolated database did not become ready')
            cls.sql((ROOT/'tests/fixtures/package_schema.sql').read_text())
            migration=(ROOT/'migrations/20261017_package_contracts.sql').read_text()
            cls.sql(migration);cls.sql(migration) # Reapplication must preserve data/schema.
        except BaseException:
            subprocess.run(['docker','stop',cls.container],capture_output=True)
            raise
    @classmethod
    def tearDownClass(cls):
        subprocess.run(['docker','stop',cls.container],check=True,capture_output=True)
    @classmethod
    def sql(cls,statement,error=None):
        result=subprocess.run(['docker','exec','-i',cls.container,'psql','-X','-qAt','-U','postgres','-v','ON_ERROR_STOP=1'],
                              input=statement,text=True,capture_output=True)
        if error is not None:
            if result.returncode==0 or error not in result.stderr:raise AssertionError((statement,result.stdout,result.stderr))
            return None
        if result.returncode:raise AssertionError((statement,result.stderr))
        return result.stdout.strip()
    def setUp(self):
        self.sql("update mt5_pool set status='unavailable' where status='available';")
    def prepare(self,cycles='3',phase_resets='1',funded_resets='2',approve=True):
        cfg=defaults();cfg['minimum_price']=5000;cfg['revision']=7;cfg['enabled']=True
        for key,rows in cfg['options'].items():
            for row in rows:row.update(enabled=True,price=5000 if key=='capital' else 0)
        tid,qid,key=str(uuid4()),str(uuid4()),str(uuid4())
        choices=dict(capital='500000',phases='two',drawdown='10',drawdown_type='static',phase1_target='10',phase2_target='8',profit_share='70',profit_cap='20',payout_cycles=cycles,phase_resets=phase_resets,funded_resets=funded_resets)
        contract=make_contract(quote(cfg,choices),tid);cid=contract['document']['contract_id']
        self.sql(f"insert into traders(id,name,email,phone) values({literal(tid)},'Isolated fixture','fixture@example.invalid','');")
        self.sql(f"insert into business_settings(key,value) values('future_package_configuration_v1',{literal(json.dumps(cfg))}) on conflict(key) do update set value=excluded.value;")
        self.sql(f"insert into np_package_quotes(id,trader_id,catalog_revision,document,rules_hash,price,expires_at) values({literal(qid)},{literal(tid)},7,{literal(json.dumps(contract['document']))}::jsonb,{literal(contract['rules_hash'])},5000,clock_timestamp()+interval '10 minutes');")
        self.sql("delete from business_settings where key='production_controls_v1';")
        fixture=dict(tid=tid,qid=qid,key=key,cid=cid,hash=contract['rules_hash'])
        if approve:
            self.checkout(fixture)
            self.sql(f"update challenge_purchases set status='approved_active',payment_status='approved',approved_at=clock_timestamp() where id={literal(qid)};")
        return fixture
    def checkout(self,f):
        return json.loads(self.sql(f"select np_package_checkout({literal(f['tid'])},{literal(f['qid'])},{literal(f['key'])},'https://proof.invalid/fixture');"))
    def account(self,f,stage='phase1',breached=False,passed=False):
        aid=str(uuid4())
        self.sql(f"insert into trader_accounts(id,trader_id,purchase_id,stage,account_status,package_contract_id,package_rules_hash,breached_at,passed_at,phase_pass_status) values({literal(aid)},{literal(f['tid'])},{literal(f['qid'])},{literal(stage)},{literal('breached_archived' if breached else 'assigned_active')},{literal(f['cid'])},{literal(f['hash'])},{'clock_timestamp()' if breached else 'null'},{'clock_timestamp()' if passed else 'null'},{literal('passed') if passed else 'null'}); update np_package_usage set current_account_id={literal(aid)} where contract_id={literal(f['cid'])};")
        return aid
    def reserve(self,f,aid,kind,error=None):
        raw=self.sql(f"select np_package_reserve_action({literal(f['cid'])},{literal(aid) if aid else 'null'},{literal(kind)});",error)
        return json.loads(raw) if raw else None
    def payout(self,f,aid,status='paid',amount=70000):
        pid=str(uuid4())
        self.sql(f"insert into payouts(id,trader_account_id,trader_id,status,paid_at,verified_profit,amount,package_contract_id) values({literal(pid)},{literal(aid)},{literal(f['tid'])},{literal(status) if status else 'null'},clock_timestamp(),200000,{amount},{literal(f['cid'])});")
        return pid
    def record(self,pid,error=None):
        raw=self.sql(f"select np_package_record_paid({literal(pid)});",error)
        return json.loads(raw) if raw else None
    def test_checkout_retries_and_simultaneous_calls_create_one_purchase(self):
        f=self.prepare(approve=False)
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:self.checkout(f),range(2)))
        self.assertEqual(sum(not r['replayed'] for r in results),1)
        self.assertEqual(self.sql(f"select count(*) from challenge_purchases where id={literal(f['qid'])};"),'1')
        self.assertEqual(self.sql(f"select payment_status from challenge_purchases where id={literal(f['qid'])};"),'pending')
    def test_expired_repriced_and_paused_quotes_cannot_create_orders(self):
        f=self.prepare(approve=False)
        self.sql(f"update np_package_quotes set expires_at=created_at+interval '1 microsecond' where id={literal(f['qid'])};")
        self.sql(f"select np_package_checkout({literal(f['tid'])},{literal(f['qid'])},{literal(f['key'])},'proof');",'Quote expired')
        f=self.prepare(approve=False)
        self.sql("update business_settings set value=jsonb_set(value::jsonb,'{revision}','8')::text where key='future_package_configuration_v1';")
        self.sql(f"select np_package_checkout({literal(f['tid'])},{literal(f['qid'])},{literal(f['key'])},'proof');",'Catalog changed')
        f=self.prepare(approve=False)
        self.sql("insert into business_settings(key,value) values('production_controls_v1','{\"features\":{\"challenge_sales\":false}}');")
        self.sql(f"select np_package_checkout({literal(f['tid'])},{literal(f['qid'])},{literal(f['key'])},'proof');",'purchases are paused')
        self.assertEqual(self.sql(f"select count(*) from challenge_purchases where id={literal(f['qid'])};"),'0')

    def test_missing_head_with_existing_account_never_issues_another_first_account(self):
        f=self.prepare();self.account(f)
        self.sql(f"update np_package_usage set current_account_id=null where contract_id={literal(f['cid'])};")
        self.reserve(f,None,'first_account',error='Initial account already exists')

    def test_frozen_contract_and_client_permissions(self):
        f=self.prepare()
        self.sql(f"update np_package_contracts set catalog_revision=99 where id={literal(f['cid'])};",'immutable')
        self.sql(f"set role anon; select np_package_reserve_action({literal(f['cid'])},null,'first_account');",'permission denied')
        self.sql("set role authenticated; select * from np_package_contracts;",'permission denied')
        self.sql(f"set role service_role; update np_package_usage set paid_cycles=999 where contract_id={literal(f['cid'])};",'permission denied')
    def test_payment_not_approved_cannot_issue_first_entitlement(self):
        f=self.prepare(approve=False);self.checkout(f)
        self.reserve(f,None,'first_account',error='Approved package purchase is required')
    def test_initial_entitlement_is_idempotent(self):
        f=self.prepare();first=self.reserve(f,None,'first_account');again=self.reserve(f,None,'first_account')
        self.assertEqual(first['id'],again['id']);self.assertTrue(again['replayed'])
    def test_phase_reset_budget_shared_across_evaluation_stages_and_retries(self):
        f=self.prepare();a=self.account(f,breached=True)
        with ThreadPoolExecutor(max_workers=2) as pool:events=list(pool.map(lambda _:self.reserve(f,a,'phase_reset'),range(2)))
        self.assertEqual(events[0]['id'],events[1]['id'])
        a2=self.account(f,stage='phase2',breached=True)
        self.reserve(f,a2,'phase_reset',error='No included phase resets remain')
        self.assertEqual(self.sql(f"select phase_resets_reserved from np_package_usage where contract_id={literal(f['cid'])};"),'1')
        af=self.account(f,stage='funded',breached=True);self.reserve(f,af,'funded_reset')
        self.assertEqual(self.sql(f"select funded_resets_reserved from np_package_usage where contract_id={literal(f['cid'])};"),'1')
    def test_wrong_owner_and_missing_recorded_pass_are_not_promoted(self):
        f=self.prepare();a=self.account(f)
        self.reserve(f,a,'phase_pass',error='Recorded exact-account pass is required')
        self.sql(f"update trader_accounts set trader_id={literal(str(uuid4()))},passed_at=clock_timestamp(),phase_pass_status='passed' where id={literal(a)};")
        self.reserve(f,a,'phase_pass',error='contract mismatch')
    def test_paid_cycle_is_once_only_and_renewal_does_not_spend_reset_budget(self):
        f=self.prepare();a=self.account(f,'funded');pid=self.payout(f,a)
        with ThreadPoolExecutor(max_workers=2) as pool:events=list(pool.map(lambda _:self.record(pid),range(2)))
        self.assertEqual(sum(not e['replayed'] for e in events),1)
        self.assertEqual(self.sql(f"select paid_cycles||','||funded_resets_reserved from np_package_usage where contract_id={literal(f['cid'])};"),'1,0')
        self.assertEqual(self.sql(f"select count(*) from np_package_entitlements where source_account_id={literal(a)} and kind='payout_renewal';"),'1')
        self.record(self.payout(f,a),error='unique constraint')
        self.assertEqual(self.sql(f"select paid_cycles from np_package_usage where contract_id={literal(f['cid'])};"),'1')
    def pool(self,stage='phase',currency='NGN',age='0 seconds'):
        pid=str(uuid4());login=str(uuid4().int)[:12]
        self.sql(f"insert into mt5_pool(id,account_size,mt5_login,mt5_server,mt5_master_password,mt5_investor_password,pool_class,status,created_at) values({literal(pid)},500000,{literal(login)},'fixture-server','fixture-only','fixture-only',{literal(stage)},'available',clock_timestamp());")
        if currency=='NGN':
            self.sql(f"insert into np_package_pool_verifications values({literal(pid)},{literal(login)},'fixture-server','NGN',500000,500000,0,0,clock_timestamp()-interval {literal(age)},'fixture-collector');")
        return pid
    def assign(self,eid,error=None):
        raw=self.sql(f"select np_package_assign_entitlement({literal(eid)});",error)
        return json.loads(raw) if raw else None
    def test_assignment_is_atomic_and_retry_does_not_issue_another_account(self):
        f=self.prepare();e=self.reserve(f,None,'first_account');poolid=self.pool()
        with ThreadPoolExecutor(max_workers=2) as workers:results=list(workers.map(lambda _:self.assign(e['id']),range(2)))
        self.assertEqual(results[0]['account_id'],results[1]['account_id'])
        aid=results[0]['account_id']
        self.assertEqual(self.sql(f"select count(*) from trader_accounts where package_contract_id={literal(f['cid'])};"),'1')
        self.assertEqual(self.sql(f"select status from mt5_pool where id={literal(poolid)};"),'assigned')
        self.assertEqual(self.sql(f"select active from monitoring_registry where trader_account_id={literal(aid)};"),'t')
        self.assertEqual(self.sql(f"select current_balance is null and current_equity is null and last_sync_at is null from trader_accounts where id={literal(aid)};"),'t')
    def test_no_assignment_from_unverified_or_stale_currency_pool(self):
        f=self.prepare();e=self.reserve(f,None,'first_account')
        # Quarantine other fixtures' available stock so this check is deterministic.
        self.sql("update mt5_pool set status='unavailable' where status='available';")
        self.pool(currency='USD');self.pool(age='10 minutes')
        result=self.assign(e['id']);self.assertTrue(result['waiting'])
        self.assertEqual(self.sql(f"select count(*) from trader_accounts where package_contract_id={literal(f['cid'])};"),'0')
    def test_new_purchase_does_not_displace_independent_legacy_funded_account(self):
        f=self.prepare();e=self.reserve(f,None,'first_account');self.pool()
        legacy=str(uuid4())
        self.sql(f"insert into trader_accounts(id,trader_id,stage,account_status) values({literal(legacy)},{literal(f['tid'])},'funded','profit_protected'); update traders set current_account_id={literal(legacy)} where id={literal(f['tid'])}; insert into monitoring_registry values({literal(legacy)},true,'WATCHDOG');")
        result=self.assign(e['id'])
        self.assertEqual(self.sql(f"select current_account_id from traders where id={literal(f['tid'])};"),legacy)
        self.assertEqual(self.sql(f"select trader_account_id from challenge_purchases where id={literal(f['qid'])};"),result['account_id'])
        self.assertEqual(self.sql(f"select active from monitoring_registry where trader_account_id={literal(legacy)};"),'t')

    def test_registry_failure_rolls_back_pool_account_and_purchase_pointer(self):
        f=self.prepare();e=self.reserve(f,None,'first_account');pid=self.pool()
        self.sql('update test_monitoring_failure set enabled=true;')
        try:self.assign(e['id'],error='Fixture registry failure')
        finally:self.sql('update test_monitoring_failure set enabled=false;')
        self.assertEqual(self.sql(f"select status from mt5_pool where id={literal(pid)};"),'available')
        self.assertEqual(self.sql(f"select count(*) from trader_accounts where package_contract_id={literal(f['cid'])};"),'0')
        self.assertEqual(self.sql(f"select state from np_package_entitlements where id={literal(e['id'])};"),'reserved')
        self.assertEqual(self.sql(f"select trader_account_id is null from challenge_purchases where id={literal(f['qid'])};"),'t')

    def test_last_paid_cycle_closes_journey_without_renewal(self):
        f=self.prepare(cycles='1');a=self.account(f,'funded');r=self.record(self.payout(f,a))
        self.assertTrue(r['completed']);self.assertIsNone(r['renewal_entitlement_id'])
        self.assertEqual(self.sql(f"select count(*) from np_package_entitlements where contract_id={literal(f['cid'])};"),'0')
    def test_pending_null_status_and_wrong_paid_amount_do_not_count(self):
        f=self.prepare();a=self.account(f,'funded')
        for status in ('pending',None):self.record(self.payout(f,a,status),error='recorded paid payout')
        self.record(self.payout(f,a,amount=80000),error='purchased share and cap')
        self.assertEqual(self.sql(f"select paid_cycles from np_package_usage where contract_id={literal(f['cid'])};"),'0')

if __name__=='__main__':unittest.main(verbosity=2)
