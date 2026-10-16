"""Exact completion evidence survives safe API projection without private notes."""
import ast
from pathlib import Path
import unittest

TREE=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text())
SOURCE='2f434b2c-5ae7-4b24-b93b-5233ef2318b2'
CHILD='2e748398-a9ee-4249-86b4-00d7001cff5f'

class HistoryMetadataTests(unittest.TestCase):
    def setUp(self):
        fields=next(ast.literal_eval(n.value) for n in TREE.body if isinstance(n,ast.Assign)
                    and any(isinstance(t,ast.Name) and t.id=='_TRADER_ACCOUNT_RESPONSE_FIELDS' for t in n.targets))
        self.ns={'_TRADER_ACCOUNT_RESPONSE_FIELDS':fields,
                 '_only_fields':lambda row,fields:{k:v for k,v in (row or {}).items() if k in fields},
                 '_trader_safe_monitoring_row':lambda row:{k:v for k,v in row.items() if k=='trader_account_id'}}
        for n in TREE.body:
            if isinstance(n,ast.FunctionDef) and n.name in {'_trader_account_progression_metadata','_trader_safe_account_row'}:
                exec(compile(ast.Module(body=[n],type_ignores=[]),'history','exec'),self.ns)
        self.safe=self.ns['_trader_safe_account_row']
        self.row={'id':SOURCE,'mt5_login':'477299291','stage':'phase1','account_status':'archived_phase1',
                  'phase_pass_status':'phase1_passed','updated_at':'2026-10-16T01:00:00Z',
                  'archive_reason':'PHASE1 PASSED | [NP_CONSUMED:PHASE_PASS:'+SOURCE+'] replacement_account_id='+CHILD+' replacement_mt5=477389747',
                  'admin_note':'private operational details','current_equity':1305775.61}

    def test_exact_completed_source_keeps_safe_successor_evidence(self):
        out=self.safe(self.row)
        self.assertTrue(out['progression_completed']);self.assertTrue(out['phase_pass_recorded'])
        self.assertEqual(out['successor_account_id'],CHILD);self.assertEqual(out['successor_mt5_login'],'477389747')
        self.assertEqual(out['phase_pass_status'],'phase1_passed')
        self.assertNotIn('archive_reason',out);self.assertNotIn('admin_note',out)
        self.assertEqual(out['current_equity'],1305775.61)

    def test_unrelated_receipt_cannot_complete_this_account(self):
        out=self.safe(dict(self.row,archive_reason=self.row['archive_reason'].replace(SOURCE,CHILD)))
        self.assertFalse(out['progression_completed']);self.assertNotIn('successor_account_id',out)

    def test_profit_above_target_is_not_a_recorded_pass(self):
        out=self.safe({'id':'new','stage':'phase1','mt5_login':'477500736','account_status':'assigned_active',
                       'current_equity':562150.65,'profit_percent':12.43,'target_percent':10})
        self.assertFalse(out['phase_pass_recorded']);self.assertFalse(out['progression_completed'])
        self.assertEqual(out['current_equity'],562150.65)

    def test_safe_projection_is_idempotent(self):
        first=self.safe(self.row);self.assertEqual(self.safe(first),first)

    def test_self_successor_or_invalid_receipt_is_not_completion(self):
        for successor in [SOURCE,'not-a-valid-successor']:
            with self.subTest(successor=successor):
                row=dict(self.row,archive_reason=self.row['archive_reason'].replace(CHILD,successor))
                self.assertFalse(self.safe(row)['progression_completed'])

    def test_nested_private_monitoring_data_stays_filtered(self):
        out=self.safe(dict(self.row,latest_monitoring_event={'trader_account_id':SOURCE,'admin_note':'private'}))
        self.assertEqual(out['latest_monitoring_event'],{'trader_account_id':SOURCE})

if __name__=='__main__':unittest.main()
