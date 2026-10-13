"""Instantiate the actual SDK clients without network or production credentials."""
import ast,unittest
from pathlib import Path
from supabase import create_client,ClientOptions
TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())

class DatabaseDeadlineTests(unittest.TestCase):
    def test_runtime_clients_have_bounded_pool_connect_and_read_waits(self):
        nodes=[n for n in TREE.body if isinstance(n,ast.Assign) and
               any(isinstance(t,ast.Name) and t.id in {'supabase','supabase_admin'} for t in n.targets)]
        env=dict(create_client=create_client,_NPClientOptionsV218=ClientOptions,
            SUPABASE_URL='https://database.example',SUPABASE_KEY='private-fixture-key',
            SUPABASE_SERVICE_ROLE_KEY='private-fixture-service-key')
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'sdk-init','exec'),env)
        for name in ('supabase','supabase_admin'):
            session=env[name].postgrest.session
            try:
                self.assertEqual(session.timeout.pool,8)
                self.assertEqual(session.timeout.connect,8)
                self.assertEqual(session.timeout.read,8)
            finally:session.close()
    def test_missing_service_role_does_not_create_privileged_client(self):
        from unittest.mock import Mock
        nodes=[n for n in TREE.body if isinstance(n,ast.Assign) and
               any(isinstance(t,ast.Name) and t.id=='supabase_admin' for t in n.targets)]
        factory=Mock()
        env=dict(create_client=factory,_NPClientOptionsV218=ClientOptions,
            SUPABASE_URL='https://database.example',SUPABASE_SERVICE_ROLE_KEY=None)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'sdk-role','exec'),env)
        self.assertIsNone(env['supabase_admin']);factory.assert_not_called()

if __name__=='__main__':unittest.main()
