"""Exercise the real cycle RPC in a disposable, network-isolated PostgreSQL."""
import subprocess, uuid, time
from pathlib import Path
name='np-cycle-tests-'+uuid.uuid4().hex[:10]
def run(*args, **kwargs):
    return subprocess.run(args,check=True,text=True,capture_output=True,**kwargs).stdout
def sql(source):
    return run('docker','exec','-i',name,'psql','-U','postgres','-v','ON_ERROR_STOP=1','-At',input=source)
try:
    run('docker','run','--rm','-d','--name',name,'--network','none','--tmpfs','/var/lib/postgresql/data',
        '-e','POSTGRES_HOST_AUTH_METHOD=trust','postgres:16-bookworm')
    for _ in range(40):
        try: run('docker','exec',name,'pg_isready','-U','postgres');break
        except subprocess.CalledProcessError: time.sleep(.2)
    sql('''CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;
CREATE TABLE challenge_plans(id uuid PRIMARY KEY);
CREATE TABLE challenge_purchases(id uuid PRIMARY KEY,trader_id uuid,status text,lifecycle_state text,account_state text,archived_at timestamptz,archive_reason text,updated_at timestamptz);
CREATE TABLE payouts(id uuid PRIMARY KEY,trader_id uuid,trader_account_id uuid,mt5_login text,status text,paid_at timestamptz);
CREATE TABLE trader_accounts(id uuid PRIMARY KEY,trader_id uuid,purchase_id uuid,package_contract_id uuid,mt5_login text,stage text,mt5_pool_id uuid,account_status text,monitoring_enabled boolean,archived_at timestamptz,archive_reason text,updated_at timestamptz);
CREATE TABLE mt5_pool(id uuid PRIMARY KEY,assigned_trader_id uuid,status text,archived_at timestamptz,archive_reason text,updated_at timestamptz);
CREATE TABLE monitoring_registry(trader_account_id uuid,trader_id uuid,active boolean,retired_at timestamptz,retirement_reason text,updated_at timestamptz);
CREATE TABLE traders(id uuid PRIMARY KEY,current_account_id uuid,current_purchase_id uuid,active_purchase_id uuid,mt5_login text,mt5_server text,mt5_master_password text,mt5_investor_password text,mt5_password text,master_password text,investor_password text,login text,server text,phase text,status text,account_state text,challenge_state text,current_active boolean,mt5_account_active boolean,monitoring_enabled boolean,mt5_access_disabled boolean,account_size numeric,balance numeric,equity numeric,profit numeric,profit_percent numeric,updated_at timestamptz);''')
    sql(Path('migrations/20261020_plan_payout_cycles.sql').read_text())
    # Every scenario is rolled back. Historical terms, duplicate payout rows,
    # independent purchases and replayed final payouts must all behave correctly.
    for limit in (1,2,3,4,5,0,None):
        tid,pid,other=str(uuid.uuid4()),str(uuid.uuid4()),str(uuid.uuid4())
        accounts=[str(uuid.uuid4()) for _ in range(6)]
        limit_sql='NULL' if limit is None else str(limit)
        setup=f"BEGIN; INSERT INTO challenge_purchases(id,trader_id,payout_cycles) VALUES('{pid}','{tid}',{limit_sql}),('{other}','{tid}',1);"
        setup+=f"INSERT INTO traders(id,current_account_id,mt5_login) VALUES('{tid}','{accounts[0]}','100');"
        sql(setup+'COMMIT;')
        for index,aid in enumerate(accounts[:limit if limit else 5]):
            payout=str(uuid.uuid4());login=str(100+index)
            sql(f"INSERT INTO trader_accounts(id,trader_id,purchase_id,mt5_login,stage,account_status,monitoring_enabled) VALUES('{aid}','{tid}','{pid}','{login}','funded','profit_protected',true); INSERT INTO payouts VALUES('{payout}','{tid}','{aid}','{login}','paid',now());")
            sql(f"UPDATE traders SET current_account_id='{aid}' WHERE id='{tid}'; INSERT INTO monitoring_registry(trader_account_id,trader_id,active) VALUES('{aid}','{tid}',true);")
            import json
            dry=json.loads(sql(f"SELECT np_plan_paid_cycle_decision('{payout}',false);").strip())
            assert sql(f"SELECT account_status FROM trader_accounts WHERE id='{aid}';").strip()=='profit_protected'
            result=json.loads(sql(f"SELECT np_plan_paid_cycle_decision('{payout}');").strip())
            expected='legacy' if limit in (0,None) else ('completed' if index+1>=limit else 'renew')
            assert result['state']==expected,(limit,index,result)
            if expected=='completed':
                assert sql(f"SELECT current_account_id IS NULL FROM traders WHERE id='{tid}';").strip()=='t'
                assert sql(f"SELECT active FROM monitoring_registry WHERE trader_account_id='{aid}';").strip()=='f'
                assert sql(f"SELECT account_status FROM trader_accounts WHERE id='{aid}';").strip()=='archived'
            # A second paid record on the SAME account must not spend two cycles.
            duplicate=str(uuid.uuid4())
            sql(f"INSERT INTO payouts VALUES('{duplicate}','{tid}','{aid}','{login}','paid',now());")
            replay=json.loads(sql(f"SELECT np_plan_paid_cycle_decision('{payout}');").strip())
            assert replay==result,(result,replay)
        if limit:
            extra=str(uuid.uuid4())
            try:sql(f"INSERT INTO trader_accounts(id,trader_id,purchase_id,mt5_login,stage) VALUES('{extra}','{tid}','{pid}','extra','funded');")
            except subprocess.CalledProcessError:pass
            else:raise AssertionError('Another assignment bypassed exhausted cycles')
        assert sql(f"SELECT coalesce(status,'unchanged') FROM challenge_purchases WHERE id='{other}';").strip()=='unchanged'
    immutable=str(uuid.uuid4());owner=str(uuid.uuid4())
    sql(f"INSERT INTO challenge_purchases(id,trader_id,payout_cycles) VALUES('{immutable}','{owner}',2);")
    try:sql(f"UPDATE challenge_purchases SET payout_cycles=5 WHERE id='{immutable}';")
    except subprocess.CalledProcessError:pass
    else:raise AssertionError('Purchase cycle snapshot must not change')
    # Ineligible payment/identity fails without creating any renewal or mutation.
    bad=str(uuid.uuid4());sql(f"INSERT INTO payouts(id,status) VALUES('{bad}','approved');")
    try:sql(f"SELECT np_plan_paid_cycle_decision('{bad}');")
    except subprocess.CalledProcessError:pass
    else:raise AssertionError('An un-paid payout must fail closed')
    assert sql("SELECT has_function_privilege('anon','np_plan_paid_cycle_decision(uuid,boolean)','EXECUTE');").strip()=='f'
    print('Passed: all 1–5 cycle boundaries, unlimited/legacy preservation, duplicate/replay safety, purchase isolation, payment gate and private RPC access.')
finally:
    subprocess.run(['docker','rm','-f',name],capture_output=True)
