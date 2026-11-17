"""Validate the reviewed benefit upgrade in disposable PostgreSQL, never production."""
import ast,subprocess,uuid,time,json
from pathlib import Path
name='np-open-cycle-test-'+uuid.uuid4().hex[:8]
def run(*a,**kw):return subprocess.run(a,check=True,text=True,capture_output=True,**kw).stdout
def sql(q):return run('docker','exec','-i',name,'psql','-U','postgres','-v','ON_ERROR_STOP=1','-At',input=q)
try:
 run('docker','run','--rm','-d','--name',name,'--network','none','--tmpfs','/var/lib/postgresql/data','-e','POSTGRES_HOST_AUTH_METHOD=trust','postgres:16-bookworm')
 for _ in range(50):
  try:run('docker','exec',name,'pg_isready','-U','postgres');break
  except subprocess.CalledProcessError:time.sleep(.2)
 tree=ast.parse(Path('tools/test_plan_cycles_database.py').read_text())
 schema=next(n.args[0].value for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='sql' and n.args and isinstance(n.args[0],ast.Constant) and str(n.args[0].value).startswith('CREATE ROLE'))
 sql(schema);sql(Path('migrations/20261020_plan_payout_cycles.sql').read_text())
 sql('''ALTER TABLE challenge_plans ADD name text,ADD account_size numeric,ADD challenge_journey jsonb,ADD fee numeric,ADD updated_at timestamptz;
 ALTER TABLE challenge_purchases ADD account_size numeric,ADD challenge_journey jsonb,ADD payment_status text,ADD package_contract_id uuid,ADD admin_note text,ADD fee numeric;
 CREATE TABLE package_studio_private_settings(key text PRIMARY KEY,value text,updated_at timestamptz,updated_by text);''')
 owner=str(uuid.uuid4());ids=[str(uuid.uuid4()) for _ in range(9)];planids=[str(uuid.uuid4()) for _ in range(5)]
 for pid,size,route in zip(planids,[500000,1000000,2000000,2500000,1000000],[['phase1','funded']]*4+[['phase1','phase2','funded']]):
  sql(f"INSERT INTO challenge_plans VALUES('{pid}',3,'plan',{size},'{json.dumps(route)}',40000,now());")
 for i,pid in enumerate(ids):
  route=['phase1','phase2','funded'] if i==4 else ['phase1','funded'];payment='pending' if i==5 else 'approved';contract="'"+str(uuid.uuid4())+"'" if i==6 else 'NULL';note='[NP_RESET_REQUEST:x:y:z]' if i==7 else '';status='breached' if i==2 else 'funded_cycles_completed' if i==1 else 'phase1_active';cap='NULL' if i==8 else '3';size=2500000 if i==3 else 500000
  sql(f"INSERT INTO challenge_purchases(id,trader_id,payout_cycles,account_size,challenge_journey,payment_status,package_contract_id,admin_note,fee,lifecycle_state,status,archive_reason) VALUES('{pid}','{owner}',{cap},{size},'{json.dumps(route)}','{payment}',{contract},'{note}',20000,'{status}','approved_active','[NP_PLAN_CYCLES_COMPLETED:{pid}]');")
 aid=str(uuid.uuid4());payout=str(uuid.uuid4())
 sql(f"INSERT INTO trader_accounts(id,trader_id,purchase_id,mt5_login,stage,account_status) VALUES('{aid}','{owner}','{ids[1]}','123','funded','archived');INSERT INTO payouts(id,trader_id,trader_account_id,mt5_login,status,paid_at) VALUES('{payout}','{owner}','{aid}','123','paid',now());")
 before=sql('SELECT row_to_json(p)::text FROM payouts p;SELECT row_to_json(a)::text FROM trader_accounts a;')
 migration=Path('migrations/20261117_open_one_phase_cycles.sql').read_text();sql(migration)
 assert sql("SELECT count(*) FROM challenge_plans WHERE payout_cycles=0;").strip()=='3'
 assert sql("SELECT count(*) FROM challenge_purchases WHERE payout_cycles=0;").strip()=='4'
 assert sql(f"SELECT lifecycle_state FROM challenge_purchases WHERE id='{ids[1]}';").strip()=='funded_waiting_mt5'
 assert sql(f"SELECT lifecycle_state FROM challenge_purchases WHERE id='{ids[2]}';").strip()=='breached'
 assert sql("SELECT tgenabled FROM pg_trigger WHERE tgname='np_keep_purchase_cycles';").strip()=='O'
 assert sql('SELECT row_to_json(p)::text FROM payouts p;SELECT row_to_json(a)::text FROM trader_accounts a;')==before
 assert json.loads(sql(f"SELECT np_plan_paid_cycle_decision('{payout}',false);"))['state']=='legacy'
 sql(migration);assert sql('SELECT count(*) FROM package_studio_private_settings;').strip()=='1'
 try:sql(f"UPDATE challenge_purchases SET payout_cycles=3 WHERE id='{ids[0]}';");raise AssertionError('Purchase guard bypassed')
 except subprocess.CalledProcessError:pass
 sql(f"UPDATE challenge_plans SET payout_cycles=3 WHERE id='{planids[0]}';")
 assert sql(f"SELECT payout_cycles FROM challenge_purchases WHERE id='{ids[0]}';").strip()=='0'
 print('PASS: scoped unlimited upgrade, immutable snapshot restoration, paid renewal decision, exact audit, replay safety, reactivation for future plans, and unchanged payout/account history.')
finally:subprocess.run(['docker','rm','-f',name],capture_output=True)
