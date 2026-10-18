"""Check actual app wiring without importing financial workers."""
import ast
from pathlib import Path
import unittest


class DatabaseCredentialTests(unittest.TestCase):
    def test_protected_package_stores_prefer_server_credential(self):
        tree = ast.parse(Path('app.py').read_text())
        targets = {'_NPLabPackageStoreV224': 2, '_NPControlsStoreV225': 2,
                   '_NPRegisterCatalogV225': 3, '_NPRegisterProductionPackagesV226': 3}
        found = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in targets:
                value = node.args[targets[node.func.id]]
                self.assertIsInstance(value, ast.BoolOp)
                self.assertIsInstance(value.op, ast.Or)
                self.assertEqual([v.id for v in value.values],
                                 ['SUPABASE_SERVICE_ROLE_KEY', 'SUPABASE_KEY'])
                found.add(node.func.id)
        self.assertEqual(found, set(targets))
