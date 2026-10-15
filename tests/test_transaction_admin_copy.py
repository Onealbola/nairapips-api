"""Run real notification helpers/paid route with fake delivery and database only."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

TREE = ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_bytes())
def load(names, env):
    nodes = [n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name in names]
    for n in nodes:
        n.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'notification-extract', 'exec'), env)

class AdminCopyTests(unittest.TestCase):
    def setUp(self):
        self.trader = Mock(return_value=True)
        self.admin = Mock(return_value=True)
        self.env = dict(send_email_safe=self.trader, send_admin_alert=self.admin, print=Mock())
        load({'send_transaction_email_with_owner_copy'}, self.env)
        self.send = self.env['send_transaction_email_with_owner_copy']

    def test_exact_reference_and_original_notice_copied_independently(self):
        self.assertTrue(self.send('trader@example.test', 'Payout completed', 'Amount: N500', 'Payout ID: paid-1 | MT5: 123'))
        self.trader.assert_called_once_with('trader@example.test', 'Payout completed', 'Amount: N500')
        subject, body = self.admin.call_args.args
        self.assertEqual(subject, 'ADMIN COPY — Payout completed')
        for value in ['trader@example.test', 'Payout ID: paid-1 | MT5: 123', 'Amount: N500']:
            self.assertIn(value, body)

    def test_trader_rejection_or_exception_never_suppresses_owner(self):
        for failure in [False, RuntimeError('provider unavailable')]:
            with self.subTest(failure=type(failure).__name__):
                self.admin.reset_mock()
                self.trader.side_effect = failure if isinstance(failure, Exception) else None
                self.trader.return_value = False
                self.assertFalse(self.send('trader@example.test', 'Transaction', 'Notice'))
                self.admin.assert_called_once()

    def test_admin_failure_cannot_reverse_successful_trader_notification(self):
        self.admin.side_effect = RuntimeError('provider unavailable')
        self.assertTrue(self.send('trader@example.test', 'Transaction', 'Notice'))
        self.trader.assert_called_once()
        self.admin.assert_called_once()

    def test_existing_recipient_deduplication_preserved(self):
        env = dict(ADMIN_ALERT_EMAIL='owner@example.test', OWNER_ALERT_EMAIL='OWNER@example.test',
                   send_email_safe=Mock(return_value=True))
        load({'send_admin_alert'}, env)
        self.assertTrue(env['send_admin_alert']('Subject', 'Body'))
        env['send_email_safe'].assert_called_once_with('owner@example.test', 'Subject', 'Body')

    def test_all_missing_transaction_paths_use_independent_copy(self):
        wanted = {'NairaPips payout approved', 'NairaPips payout received - your certificate is ready', 'NairaPips Second Life activated'}
        counts = dict.fromkeys(wanted, 0)
        for node in ast.walk(TREE):
            if isinstance(node, ast.Call) and len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and node.args[1].value in wanted:
                self.assertEqual(node.func.id, 'send_transaction_email_with_owner_copy')
                counts[node.args[1].value] += 1
        self.assertEqual(sorted(counts.values()), [1, 2, 4])

    def test_paid_retry_cannot_send_again_or_repeat_renewal(self):
        row = dict(id='paid-1', status='approved', email='trader@example.test', amount=500, mt5_login='123')
        class Query:
            def update(self, payload): self.payload = payload; return self
            def eq(self, *args): return self
            def is_(self, *args): return self
            def execute(self): row.update(self.payload); return SimpleNamespace(data=[dict(row)])
        env = dict(self.env, request=SimpleNamespace(get_json=lambda **kw: {'id':'paid-1'}),
                   _np_v122_s=lambda x: str(x or ''), get_payout_by_id=lambda _: dict(row),
                   payout_status=lambda p:p['status'], _np_payout_awaiting_verification=lambda _:False,
                   now_iso=lambda:'2026-10-15T08:00:00Z', supabase=SimpleNamespace(table=lambda _:Query()),
                   email_money=lambda x:str(x), _np_v122_start=Mock(return_value={'state':'queued'}),
                   _audit_safe=Mock(), _admin_from_payload=lambda _:{}, np_invalidate_admin_bootstrap=Mock(),
                   NAIRAPIPS_PAYOUT_RENEWAL_RELEASE_V122='test', ok=lambda x,*args:x,
                   bad=lambda msg,status=400:{'error':msg,'status':status})
        load({'_np_v122_mark_paid_route'}, env)
        self.assertIn('payout', env['_np_v122_mark_paid_route']())
        self.assertEqual(env['_np_v122_mark_paid_route']()['status'], 409)
        self.trader.assert_called_once()
        self.admin.assert_called_once()
        env['_np_v122_start'].assert_called_once()

if __name__ == '__main__': unittest.main()
