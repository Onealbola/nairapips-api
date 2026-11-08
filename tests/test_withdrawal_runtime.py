import json,unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import Mock,patch
import withdrawal_runtime as runtime
from withdrawal_controls import RELEASE,ready
class RuntimeTests(unittest.TestCase):
 def setUp(self):
  runtime._cache=(0,False)
  self.env=patch.dict('os.environ',{'SUPABASE_URL':'https://fixture.invalid','SUPABASE_KEY':'fixture'},clear=True);self.env.start();self.addCleanup(self.env.stop)
  self.proof={'enabled':True,'release':RELEASE,'engine_sha256':runtime.engine_hash(),'verified_at':datetime.now(timezone.utc).isoformat()}
 def probe(self,rows):
  response=Mock();response.json.return_value=rows
  with patch('requests.get',return_value=response) as get:
   result=ready();self.assertEqual(get.call_args.kwargs['params']['key'],'eq.'+runtime.KEY)
  return result
 def test_verified_exact_installed_engine_is_ready_without_render_flag(self):
  self.assertTrue(self.probe([{'value':json.dumps(self.proof)}]))
 def test_missing_revoked_wrong_engine_and_invalid_proofs_stay_closed(self):
  for proof in (None,dict(self.proof,enabled=False),dict(self.proof,release='old'),dict(self.proof,engine_sha256='wrong'),dict(self.proof,verified_at=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat())):
   runtime._cache=(0,False)
   self.assertFalse(self.probe([] if proof is None else [{'value':json.dumps(proof)}]))
 def test_read_failure_is_closed_and_never_writes(self):
  with patch('requests.get',side_effect=TimeoutError()),patch('requests.post') as post,patch('requests.patch') as update:
   self.assertFalse(ready());post.assert_not_called();update.assert_not_called()
 def test_explicit_render_verification_remains_compatible(self):
  with patch.dict('os.environ',{'NP_PLAN_WITHDRAWAL_VERIFIED':RELEASE}),patch('requests.get') as get:
   self.assertTrue(ready());get.assert_not_called()

 def test_verified_cache_cannot_cross_credentials_or_environment(self):
  self.assertTrue(self.probe([{'value':json.dumps(self.proof)}]))
  with patch.dict('os.environ',{},clear=True):self.assertFalse(ready())
  with patch.dict('os.environ',{'SUPABASE_URL':'https://other.invalid'}),patch('requests.get',side_effect=TimeoutError()):self.assertFalse(ready())
