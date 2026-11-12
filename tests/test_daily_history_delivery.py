import importlib.util,pathlib,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('patch_daily',ROOT/'dd-daily-history-release/patch_daily.py');patch=importlib.util.module_from_spec(spec);spec.loader.exec_module(patch)
class DailyDeliveryTests(unittest.TestCase):
 def test_surgical_patch_preserves_unrelated_rules_and_makes_one_backup(self):
  current=(ROOT/'plan_risk_controls.py').read_text();a=current.index('def broker_daily_realised(');b=current.index('\n\n_ready_cache',a);source=current[:a]+patch.NEW+current[b:];old=current[:a]+patch.OLD+current[b:]
  self.assertNotEqual(source,old);self.assertEqual(patch.patched(old),source)
  with tempfile.TemporaryDirectory() as d:
   helper=pathlib.Path(d)/'plan_risk_controls.py';helper.write_bytes(old.replace('\n','\r\n').encode());before=helper.read_bytes()
   patch.install(pathlib.Path(d)/'worker.py');after=helper.read_bytes();patch.install(pathlib.Path(d)/'worker.py')
   self.assertEqual(helper.read_bytes(),after);backups=list(pathlib.Path(d).glob('*.bak'));self.assertEqual(len(backups),1);self.assertEqual(backups[0].read_bytes(),before)
  with self.assertRaises(ValueError):patch.patched(source.replace(patch.NEW,'def broker_daily_realised():\n    return 0'))
 def test_installer_embeds_tested_patch_and_keeps_fleet_identity_guards(self):
  script=(ROOT/'website-downloads/nairapips-dd-daily-history.ps1').read_text()
  self.assertIn((ROOT/'dd-daily-history-release/patch_daily.py').read_text(),script)
  self.assertIn('$owned.Count -ne 4',script);self.assertIn('$live.CreationDate -ne $before.CreationDate',script)
  self.assertIn('daily-history',script.lower());self.assertNotIn('Remove-Item -Recurse',script)
