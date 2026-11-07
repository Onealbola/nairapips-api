"""A hung transport must not occupy an overview request or grow its queue."""
import ast
from pathlib import Path
from threading import Event
from time import monotonic
import unittest
from unittest.mock import patch
import business_overview as overview


class ReadDeadlineTests(unittest.TestCase):
    def test_stalled_read_is_bounded_and_cannot_queue_another_batch(self):
        release = Event()
        class Transport:
            def get(self, *args, **kwargs):
                release.wait(3)
                class Response:
                    def raise_for_status(self): pass
                    def json(self): return []
                return Response()
        try:
            with patch.object(overview, 'READ_DEADLINE_SECONDS', .05):
                started = monotonic()
                with self.assertRaises(TimeoutError):
                    overview.read_sources(Transport(), 'https://example.invalid', 'test')
                self.assertLess(monotonic() - started, .5)
                pending = set(overview._reader_pending)
                with self.assertRaises(TimeoutError):
                    overview.read_sources(Transport(), 'https://example.invalid', 'test')
                self.assertTrue(overview._reader_pending.issubset(pending))
        finally:
            release.set()
            overview.wait(overview._reader_pending, timeout=4)
        result = overview.read_sources(Transport(), 'https://example.invalid', 'test')
        self.assertEqual(result['customers'], [])

    def test_netrc_is_loaded_before_importing_threaded_dependencies(self):
        tree = ast.parse(Path('app.py').read_text())
        imports = [alias.name for node in tree.body if isinstance(node, ast.Import) for alias in node.names]
        self.assertEqual(imports[0], 'netrc')
