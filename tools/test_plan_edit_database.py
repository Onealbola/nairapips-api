"""Test actual plan-edit snapshot trigger in disposable PostgreSQL, no prod data."""
import subprocess,uuid,time
from pathlib import Path
name='np-plan-edit-'+uuid.uuid4().hex[:10]
def run(*args,**kwargs):return subprocess.run(args,check=True,text=True,capture_output=True,**kwargs).stdout
def sql(text):return run('docker','exec','-i',name,'psql','-U','postgres','-v','ON_ERROR_STOP=1','-At',input=text)
try:
 run('docker','run','--rm','-d','--name',name,'--network','none','--tmpfs','/var/lib/postgresql/data','-e','POSTGRES_HOST_AUTH_METHOD=trust','postgres:16-bookworm')
 for _ in range(40):
  try:run('docker','exec',name,'pg_isready','-U','postgres');break
  except subprocess.CalledProcessError:time.sleep(.2)
 common='account_size numeric,fee numeric,phase1_target numeric,phase2_target numeric,max_drawdown numeric,payout_split numeric,reset_fee numeric,funded_reset_fee numeric,challenge_journey jsonb,drawdown_type text,payout_frequency text,product_category text,payout_cycles integer,second_life_enabled boolean,lives_total integer'
 plan_columns=common.replace('payout_split numeric','payout_split text')
 sql(f'CREATE TABLE challenge_plans(id int PRIMARY KEY,{plan_columns}); CREATE TABLE challenge_purchases(id int PRIMARY KEY,plan_id int,plan_snapshot_at timestamptz,{common});')
 sql('''CREATE FUNCTION freeze_cycles() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.payout_cycles IS DISTINCT FROM OLD.payout_cycles THEN RAISE EXCEPTION 'immutable cycle snapshot'; END IF; RETURN NEW; END$$;
 CREATE TRIGGER freeze_cycles BEFORE UPDATE ON challenge_purchases FOR EACH ROW EXECUTE FUNCTION freeze_cycles();''')
 sql(Path('migrations/20261021_edit_current_plans.sql').read_text())
 sql("""INSERT INTO challenge_plans VALUES(1,1000000,40000,15,0,10,60,5000,80000,'["phase1","funded"]','STATIC','Daily','premium',2,true,2);
 INSERT INTO challenge_plans SELECT 2,account_size,fee,phase1_target,phase2_target,max_drawdown,payout_split,reset_fee,funded_reset_fee,challenge_journey,drawdown_type,payout_frequency,'standard',payout_cycles,second_life_enabled,lives_total FROM challenge_plans WHERE id=1;
 INSERT INTO challenge_purchases(id,plan_id,fee,payout_cycles) VALUES(1,1,35000,NULL),(2,1,39000,2),(3,2,40000,NULL);
 UPDATE challenge_purchases SET max_drawdown=12 WHERE id=2;
 UPDATE challenge_plans SET max_drawdown=20,fee=50000,payout_cycles=5,second_life_enabled=false,lives_total=1,phase1_target=20,phase2_target=8,challenge_journey='["phase1","phase2","funded"]' WHERE id=1;""")
 assert sql('SELECT max_drawdown FROM challenge_purchases WHERE id=1;').strip()=='10'
 assert sql('SELECT max_drawdown FROM challenge_purchases WHERE id=2;').strip()=='12'
 assert sql('SELECT max_drawdown IS NULL FROM challenge_purchases WHERE id=3;').strip()=='t'
 assert sql('SELECT fee FROM challenge_purchases WHERE id=1;').strip()=='35000'
 assert sql('SELECT payout_cycles IS NULL FROM challenge_purchases WHERE id=1;').strip()=='t'
 assert sql('SELECT payout_cycles FROM challenge_purchases WHERE id=2;').strip()=='2'
 assert sql('SELECT phase1_target FROM challenge_purchases WHERE id=1;').strip()=='15'
 assert sql('SELECT second_life_enabled FROM challenge_purchases WHERE id=1;').strip()=='t'
 assert sql('SELECT lives_total FROM challenge_purchases WHERE id=1;').strip()=='2'
 assert sql('SELECT phase2_target FROM challenge_purchases WHERE id=1;').strip()=='0'
 assert sql("SELECT challenge_journey::text FROM challenge_purchases WHERE id=1;").strip()=='["phase1", "funded"]'
 sql('UPDATE challenge_plans SET max_drawdown=25 WHERE id=1;')
 assert sql('SELECT max_drawdown FROM challenge_purchases WHERE id=1;').strip()=='10'
 sql('UPDATE challenge_plans SET max_drawdown=30 WHERE id=2;')
 assert sql('SELECT max_drawdown FROM challenge_purchases WHERE id=3;').strip()=='10'
 # A brand-new purchase uses the edited terms, not the old snapshot.
 sql('INSERT INTO challenge_purchases(id,plan_id,max_drawdown,payout_cycles) SELECT 4,id,max_drawdown,payout_cycles FROM challenge_plans WHERE id=1;')
 assert sql('SELECT max_drawdown||\':\'||payout_cycles FROM challenge_purchases WHERE id=4;').strip()=='25:5'
 print('Passed: sold standard/premium edits preserve historical DD, targets, route, cycles and paid amounts; new purchases receive new terms; independent plans stay unchanged.')
finally:subprocess.run(['docker','rm','-f',name],capture_output=True)
