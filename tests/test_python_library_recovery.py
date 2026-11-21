import hashlib,io,json,os,subprocess,sys,tempfile,unittest,zipfile
from pathlib import Path,PurePosixPath
from flask import Flask
from panel_delivery import register_panel_delivery
ROOT=Path(__file__).resolve().parents[1]
ZIP=ROOT/'website-downloads/nairapips-python311-library-recovery.zip'

class LibraryRecoveryTests(unittest.TestCase):
 def test_direct_download_contains_only_verified_standard_library_and_add_only_helper(self):
  app=Flask(__name__);register_panel_delivery(app)
  response=app.test_client().get('/downloads/python311-library-recovery.zip')
  self.assertEqual(response.status_code,200);self.assertIn('attachment',response.headers['Content-Disposition']);self.assertIn('no-store',response.headers['Cache-Control'])
  self.assertEqual(response.data,ZIP.read_bytes())
  with zipfile.ZipFile(io.BytesIO(response.data)) as z:
   manifest=json.loads(z.read('LIBRARY_SHA256.json'))
   self.assertGreater(len(manifest),600)
   for name,digest in manifest.items():
    self.assertTrue(name.startswith('Lib/'));self.assertNotIn('..',PurePosixPath(name).parts);self.assertNotIn('site-packages',name);self.assertTrue(name.endswith(('.py','.whl')))
    self.assertEqual(hashlib.sha256(z.read(name)).hexdigest(),digest)
   self.assertIn('Lib/encodings/__init__.py',manifest);self.assertIn('Lib/os.py',manifest)
   self.assertEqual(z.read('RESTORE_MISSING_LIBRARY.ps1'),(ROOT/'python-library-recovery/RESTORE_MISSING_LIBRARY.ps1').read_bytes())
   self.assertIn(b'github.com/python/cpython/archive/refs/tags/v3.11.0.zip',z.read('SOURCE.txt'))
   self.assertIn(b'PYTHON SOFTWARE FOUNDATION LICENSE',z.read('PYTHON_LICENSE.txt'))
  response.close()
 def test_package_restores_enough_missing_library_to_boot_an_isolated_python_311(self):
  if sys.version_info[:2]!=(3,11):self.skipTest('Bootstrap check requires a Python 3.11 runtime')
  # This exercises the real official library, independent of the host stdlib.
  # Windows native imports still require the existing matching Windows binaries.
  with tempfile.TemporaryDirectory() as folder,zipfile.ZipFile(ZIP) as z:
   library=Path(folder)/'lib/python3.11';library.mkdir(parents=True)
   for name in json.loads(z.read('LIBRARY_SHA256.json')):
    target=library/name.removeprefix('Lib/');target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(z.read(name))
   env={k:v for k,v in os.environ.items() if k not in ('PYTHONPATH','PYTHONHOME')};env['PYTHONHOME']=folder
   result=subprocess.run([sys.executable,'-S','-c',"import encodings, json, ast, os; print('RECOVERED LIBRARY BOOTS'); print(encodings.__file__)"],env=env,cwd=folder,capture_output=True,text=True,timeout=15)
   self.assertEqual(result.returncode,0,result.stderr);self.assertIn('RECOVERED LIBRARY BOOTS',result.stdout);self.assertIn(folder,result.stdout)
 def test_recovery_cannot_overwrite_existing_code_or_restart_monitoring(self):
  script=(ROOT/'python-library-recovery/RESTORE_MISSING_LIBRARY.ps1').read_text()
  self.assertIn('[IO.File]::Move($temporary,$entry.Destination)',script)
  self.assertIn('[IO.File]::Copy($entry.Source,$temporary,$false)',script)
  for forbidden in ['Stop-Process','Restart-Computer','shutdown','Start-ScheduledTask','Set-ScheduledTask','initialize(','login(','Copy-Item','Move-Item','site-packages']:
   self.assertNotIn(forbidden,script)
  self.assertLess(script.index('Library checksum failed'),script.index('[IO.File]::Copy'))
  self.assertIn("^3\\.11\\.0",script)

if __name__=='__main__':unittest.main()
