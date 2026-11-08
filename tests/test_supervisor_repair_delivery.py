import ast,hashlib,json,unittest,zipfile
from io import BytesIO
from pathlib import Path
from flask import Flask
from panel_delivery import register_panel_delivery

class SupervisorRepairTests(unittest.TestCase):
    def test_recovery_contains_complete_entrypoint_and_no_worker_replacement(self):
        app=Flask(__name__);register_panel_delivery(app)
        r=app.test_client().get('/downloads/dd-supervisor-repair.zip')
        self.assertEqual(r.status_code,200)
        self.assertIn('attachment',r.headers['Content-Disposition'])
        with zipfile.ZipFile(BytesIO(r.data)) as z:
            names=z.namelist()
            self.assertTrue(all('/' not in name and '..' not in name for name in names))
            self.assertFalse(any(name.startswith('DD_POLICE_') for name in names))
            manifest=json.loads(z.read('SHA256.json'))
            for name,digest in manifest.items():self.assertEqual(hashlib.sha256(z.read(name)).hexdigest(),digest)
            tree=ast.parse(z.read('NAIRAPIPS_DD_FLEET_SUPERVISOR_V3_20_CLEAN_LIVE_STATE_20261008.py').decode())
            self.assertTrue(any(isinstance(n,ast.FunctionDef) and n.name=='main' for n in tree.body))
            self.assertTrue(any(isinstance(n,ast.If) and '__name__' in ast.unparse(n.test) for n in tree.body))
            script=z.read('REPAIR_SUPERVISOR.ps1').decode()
            self.assertLess(script.index('if ($ActiveDD.Count -gt 0)'),script.index('New-Item -ItemType Directory'))
            self.assertIn('$current.CreationDate -eq $process.CreationDate',script)
            self.assertIn('$current.CommandLine -eq $process.CommandLine',script)
            self.assertNotIn('taskkill',script.lower())
            self.assertIn('No new supervisor was started automatically',script)
        r.close()
