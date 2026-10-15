"""Recall must prove exact non-use despite a broken shared client."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

TREE = ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_text())


def load(ns, names):
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            exec(compile(ast.Module(body=[node], type_ignores=[]), 'recall', 'exec'), ns)


class Query:
    def __init__(self, table, answers, calls):
        self.table, self.answers, self.calls = table, answers, calls
        self.filters = {}
    def select(self, columns): return self
    def eq(self, column, value):
        self.filters[column] = value
        return self
    def limit(self, value): return self
    def execute(self):
        self.calls.append((self.table, dict(self.filters)))
        result = self.answers.pop(0)
        if isinstance(result, Exception): raise result
        return SimpleNamespace(data=result)


class RecallTransportTests(unittest.TestCase):
    def check(self, answers):
        calls = []
        broken = Mock()
        broken.table.side_effect = BlockingIOError(11, 'Resource temporarily unavailable')
        ns = {'supabase': broken,
              '_NPRenewalRestQueryV211': lambda table: Query(table, answers, calls)}
        load(ns, {'_np_ur_v87_db', '_np_ur_v87_s', '_np_ur_v87_use_check'})
        result = ns['_np_ur_v87_use_check']({'id': 'account', 'trader_id': 'trader', 'mt5_login': '477559795'})
        broken.table.assert_not_called()
        return result, calls

    def test_unused_checks_both_exact_and_legacy_identity_without_shared_client(self):
        result, calls = self.check([[], [], [], []])
        self.assertTrue(result['safe'])
        self.assertEqual(calls, [
            ('trader_trades', {'trader_account_id': 'account'}),
            ('trader_trades', {'trader_id': 'trader', 'mt5_login': '477559795'}),
            ('payouts', {'trader_account_id': 'account'}),
            ('payouts', {'trader_id': 'trader', 'mt5_login': '477559795'})])

    def test_trade_or_payout_at_either_identity_blocks_recall(self):
        for answers, reason in [
            ([[{'id':'trade'}]], 'account_has_trade_history'),
            ([[], [{'id':'trade'}]], 'account_has_trade_history'),
            ([[], [], [{'id':'paid'}]], 'account_has_payout_history'),
            ([[], [], [], [{'id':'paid'}]], 'account_has_payout_history')]:
            with self.subTest(reason=reason, queries=len(answers)):
                result, _ = self.check(answers)
                self.assertFalse(result['safe'])
                self.assertEqual(result['reason'], reason)

    def test_read_failure_never_becomes_unused(self):
        for preceding in range(4):
            with self.subTest(query=preceding):
                result, _ = self.check([[] for _ in range(preceding)] + [BlockingIOError(11, 'busy')])
                self.assertFalse(result['safe'])
                self.assertIn('verification_failed:', result['reason'])
                self.assertFalse(result['trade_found'])
                self.assertFalse(result['payout_found'])

    def test_pointer_transport_context_restored_on_success_and_failure(self):
        from contextvars import ContextVar
        for fails in [False, True]:
            with self.subTest(fails=fails):
                context = ContextVar('recall-pointer', default=False)
                def persist(*args):
                    self.assertTrue(context.get())
                    if fails: raise RuntimeError('unavailable')
                    return 'persisted'
                ns = {'_NP_RENEWAL_REST_V211':context, '_np_update_trader_current_pointer_v59':persist}
                load(ns, {'_np_ur_v87_set_pointer'})
                if fails:
                    with self.assertRaises(RuntimeError): ns['_np_ur_v87_set_pointer']('t',{'id':'a'},'phase1')
                else:self.assertEqual(ns['_np_ur_v87_set_pointer']('t',{'id':'a'},'phase1'),'persisted')
                self.assertFalse(context.get())

    def test_recall_helpers_do_not_use_shared_transport(self):
        for node in TREE.body:
            if isinstance(node, ast.FunctionDef) and node.name.startswith(('_np_ur_v87_', '_np_ur_v89_')):
                with self.subTest(helper=node.name):
                    self.assertFalse(any(isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                        and n.value.id == 'supabase' for n in ast.walk(node)))


class RecallExecutionTests(unittest.TestCase):
    def route(self, use):
        from flask import Flask, request
        account = {'id':'old', 'trader_id':'owner', 'mt5_login':'477559795'}
        target = {'ok':True, 'target_trader_id':'owner', 'target_stage':'phase1', 'target_size':500000, 'purchase_id':'purchase'}
        retire = Mock(return_value='marker')
        create = Mock(return_value={'id':'new', 'mt5_login':'81832920'})
        pointers = Mock()
        ns = {'request':request, '_require_admin':lambda:({'name':'admin'},None),
              '_np_ok':lambda body, status=200:(body,status),
              '_np_fail':lambda message,status=400:({'error':message},status),
              '_NP_UR_V87_REASON_LABELS':{'invalid_login':'Invalid login'},
              'NAIRAPIPS_UNIVERSAL_RECALL_RELEASE_V87':'test',
              '_np_ur_v87_account_by_id':lambda aid:account,
              '_np_ur_v87_status_blob':lambda row:'',
              '_np_ur_v87_preview':lambda *a:{'ok':True,'action':'REPLACE_EXACT_DELIVERY'},
              '_np_ur_v87_target':lambda *a:target,
              '_np_ur_v87_active_sibling':lambda *a:None,
              '_np_ur_v89_pick_replacement':lambda *a:({'id':'pool'},{}),
              '_np_ur_v87_use_check':lambda a:use,
              '_np_ur_v87_retire_exact':retire,
              '_np_ur_v87_create_replacement':create,
              '_np_ur_v87_update_pointers':pointers,
              '_np_ur_v87_send_replacement_email':lambda *a:True,
              '_audit_safe':lambda *a:None,
              'np_invalidate_admin_bootstrap':lambda *a:None,
              '_invalidate_trader_bootstrap_cache':lambda *a:None}
        load(ns, {'_np_ur_v87_s','_np_ur_v87_low'})
        node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name=='admin_universal_recall_v87_execute')
        node = __import__('copy').deepcopy(node); node.decorator_list=[]
        exec(compile(ast.Module(body=[node],type_ignores=[]),'execute','exec'),ns)
        with Flask(__name__).test_request_context('/execute',method='POST',json={
                'account_id':'old','expected_trader_id':'owner','reason_code':'invalid_login'}):
            result=ns[node.name]()
        return result, retire, create, pointers

    def test_late_history_or_database_failure_cannot_start_replacement(self):
        for reason in ['account_has_trade_history','account_has_payout_history','trade_verification_failed:busy','payout_verification_failed:busy']:
            with self.subTest(reason=reason):
                (body,status),retire,create,pointers=self.route({'safe':False,'reason':reason})
                self.assertEqual(status,409)
                retire.assert_not_called();create.assert_not_called();pointers.assert_not_called()
                if 'verification_failed:' in reason:
                    self.assertIn('verification is temporarily unavailable',body['message'])
                else:self.assertIn('recorded trades or payouts',body['message'])

    def test_confirmed_unused_delivery_preserves_exact_journey(self):
        (body,status),retire,create,pointers=self.route({'safe':True})
        self.assertEqual(status,200);self.assertEqual(body['status'],'REPLACED')
        self.assertTrue(body['journey_unchanged'])
        for event in ['pass_replayed','payout_replayed','reset_replayed','second_life_replayed']:
            self.assertFalse(body[event])
        create.assert_called_once();pointers.assert_called_once()
        self.assertEqual(retire.call_count,2)


if __name__ == '__main__': unittest.main()
