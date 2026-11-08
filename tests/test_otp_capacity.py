import threading
import time
import unittest
import ast
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from flask import Flask, jsonify, request
from otp_capacity import bounded


class OTPCapacityTests(unittest.TestCase):
    def test_stuck_work_times_out_and_retries_cannot_queue_duplicates(self):
        app = Flask(__name__)
        release = threading.Event()
        started = threading.Event()
        calls = []

        def otp():
            calls.append(request.json['fixture'])
            started.set()
            release.wait(2)
            return jsonify(success=True)

        app.add_url_rule('/otp', view_func=bounded(otp, timeout=.03), methods=['POST'])
        app.add_url_rule('/catalogue', view_func=lambda: jsonify(success=True))
        client = app.test_client()
        try:
            before = time.monotonic()
            response = client.post('/otp', json={'fixture': 'no-email-sent'})
            self.assertEqual(response.status_code, 503)
            self.assertLess(time.monotonic()-before, .5)
            self.assertTrue(started.is_set())
            self.assertEqual(client.post('/otp', json={'fixture': 'duplicate'}).status_code, 503)
            self.assertEqual(calls, ['no-email-sent'])
            self.assertEqual(client.get('/catalogue').status_code, 200)
        finally:
            release.set()

    def test_success_preserves_response_and_request_context(self):
        app = Flask(__name__)
        app.add_url_rule('/otp', view_func=bounded(lambda: (jsonify(success=True, fixture=request.json['fixture']), 201)), methods=['POST'])
        response = app.test_client().post('/otp', json={'fixture': 'fixture-only'})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json, {'success': True, 'fixture': 'fixture-only'})

    def test_failure_releases_capacity_without_disclosing_exception(self):
        app = Flask(__name__)

        def otp():
            raise RuntimeError('private fixture information')

        app.add_url_rule('/otp', view_func=bounded(otp), methods=['POST'])
        for unused in range(2):
            response = app.test_client().post('/otp')
            self.assertEqual(response.status_code, 503)
            self.assertNotIn('private fixture', response.get_data(as_text=True))


class OTPStorageTests(unittest.TestCase):
    def setUp(self):
        app = Flask(__name__)
        self.post = Mock(return_value=SimpleNamespace(raise_for_status=lambda: None))
        self.send = Mock(return_value=True)
        node = next(n for n in ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_bytes()).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'send_email_otp')
        env = dict(app=app, request=request, datetime=datetime, timezone=timezone, time=time,
                   SUPABASE_URL='https://fixture.invalid', SUPABASE_KEY='fixture-only',
                   requests=SimpleNamespace(post=self.post), send_email_safe=self.send,
                   _valid_email=lambda value: '@' in value, _otp_digits=lambda: '000000',
                   now_iso=lambda: datetime.now(timezone.utc).isoformat(),
                   ok=lambda data, message: jsonify(success=True, **data),
                   bad=lambda message, status=400: (jsonify(success=False), status))
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'isolated-otp-handler', 'exec'), env)
        self.client = app.test_client()

    def test_insert_uses_isolated_transport_with_explicit_timeout(self):
        response = self.client.post('/send_email_otp', json={'email': 'fixture@example.invalid'})
        self.assertEqual(response.status_code, 200)
        self.post.assert_called_once()
        self.assertEqual(self.post.call_args.kwargs['timeout'], (2, 4))
        self.send.assert_called_once()

    def test_storage_failure_does_not_attempt_email(self):
        self.post.side_effect = TimeoutError('fixture timeout')
        response = self.client.post('/send_email_otp', json={'email': 'fixture@example.invalid'})
        self.assertEqual(response.status_code, 500)
        self.send.assert_not_called()
