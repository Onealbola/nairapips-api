import ast,importlib.util,pathlib,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('patch_retry',ROOT/'dd-login-retry-release/patch_retry.py');patch=importlib.util.module_from_spec(spec);spec.loader.exec_module(patch)
WORKER=ROOT/'drawdown-release/DD_POLICE_STATIC_TRAILING_V1_20261107.py'
class RetryTests(unittest.TestCase):
 def test_real_scheduler_does_not_starve_failed_login_behind_always_due_healthy_accounts(self):
  tree=ast.parse(WORKER.read_text());main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main');main.decorator_list=[]
  captured=[]
  class Clock:
   def monotonic(self):return 1000
   def sleep(self,n):pass
  def inspect(row):captured.append(row['login']);raise KeyboardInterrupt
  ns=dict(configure_worker=lambda:None,_load_latches=lambda:None,log=lambda *a:None,RELEASE='test',SHARD_ID=2,TOTAL_SHARDS=4,MT5_PATH='test',refresh_population=lambda *a:None,refresh_pool=lambda *a:None,health=lambda:None,initialize_mt5=lambda:True,time=Clock(),rows={x:{'login':x} for x in ['healthy','desmond']},next_due={'healthy':0,'desmond':0},verified={'healthy'},first_attempted={'healthy','desmond'},invalid_live_capital={},last_valid_dd_read={'healthy':995},last_attempted_at={'healthy':995,'desmond':10},auth_quarantine_until={'desmond':25},MAX_VALID_REVISIT_SECONDS=60,MAX_COVERAGE_RESCUE_STREAK=1,risk_priority={'healthy':3},MAX_CONSECUTIVE_RISK_TURNS=2,inspect=inspect,mt5=type('MT5',(),{'shutdown':staticmethod(lambda:None)})())
  exec(compile(ast.Module(body=[main],type_ignores=[]),'<real-worker-main>','exec'),ns)
  ns['main']();self.assertEqual(captured,['desmond'])
  captured.clear();ns['auth_quarantine_until']['desmond']=1100;ns['main']();self.assertEqual(captured,['healthy'])
  captured.clear();ns['auth_quarantine_until']['desmond']=0;ns['last_attempted_at']['desmond']=950;ns['main']();self.assertEqual(captured,['healthy'])
 def test_only_scheduler_changes_and_install_is_idempotent_with_backup(self):
  fixed=WORKER.read_text();old=fixed.replace(patch.NEW,patch.OLD).replace(patch.NEW_SORT,patch.OLD_SORT)
  self.assertEqual(patch.patched(old),fixed)
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d)/'worker.py';p.write_bytes(old.replace('\n','\r\n').encode());original=p.read_bytes();patch.install(p);updated=p.read_bytes();patch.install(p)
   self.assertEqual(p.read_bytes(),updated);backups=list(pathlib.Path(d).glob('*.bak'));self.assertEqual(len(backups),1);self.assertEqual(backups[0].read_bytes(),original)
  with self.assertRaises(ValueError):patch.patched('unknown scheduler')
 def test_installer_embeds_exact_reviewed_patch_and_has_process_guards(self):
  script=(ROOT/'website-downloads/nairapips-dd-login-retry.ps1').read_text()
  self.assertIn((ROOT/'dd-login-retry-release/patch_retry.py').read_text(),script)
  self.assertIn('$owned.Count -ne 4',script);self.assertIn('$live.CreationDate -ne $before.CreationDate',script)
  self.assertNotIn('terminal64.exe -Force',script)
if __name__=='__main__':unittest.main()
