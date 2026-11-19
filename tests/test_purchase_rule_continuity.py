import ast
import copy
import re
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from purchase_rule_continuity import (account_fields, assert_bound, assignment_purchase,
                                       paid_renewal_rules)
from plan_risk_controls import freeze, frozen_terms
from withdrawal_controls import freeze as freeze_withdrawal, read as withdrawal_terms


class ContinuityTests(unittest.TestCase):
    def setUp(self):
        source = freeze_withdrawal('purchase', dict(profit_cap_enabled=True, profit_cap='12',
            minimum_profit_enabled=True, minimum_profit_percent='2',
            minimum_withdrawal_enabled=False, minimum_withdrawal='0'))
        source = freeze(source, dict(daily_drawdown='1.37', payout_frequency='Monthly', resets={'phase1':'35000'}))
        self.purchase = dict(id='purchase', trader_id='owner', account_size=5000000,
            max_drawdown=8, phase1_target=6, phase2_target=4,
            challenge_journey=['phase1','phase2','funded'], drawdown_type='TRAILING', journey_source=source)
        self.source = dict(id='source', trader_id='owner', purchase_id='purchase', stage='funded',
            start_balance=5000000, account_size=5000000)
        self.payout = dict(id='paid', trader_id='owner', trader_account_id='source', status='paid')
        self.account = dict(id='child', trader_id='owner', purchase_id='purchase', stage='funded',
            start_balance=5000000, account_size=5000000)

    def test_every_phase_uses_exact_original_terms_and_correct_phase_target(self):
        for journey in (['phase1','funded'],['phase1','phase2','funded'],['funded']):
            for risk_mode in ('STATIC','TRAILING'):
                purchase=dict(self.purchase,challenge_journey=journey,drawdown_type=risk_mode)
                before=copy.deepcopy(purchase)
                for stage in journey:
                    account=dict(self.account,stage=stage)
                    account.update(account_fields(account,purchase))
                    assert_bound(account,purchase)
                    self.assertEqual(account['dd_limit_percent'],8)
                    self.assertEqual(frozen_terms(account),frozen_terms(purchase))
                    self.assertEqual(withdrawal_terms(account),withdrawal_terms(purchase))
                    self.assertEqual(account['challenge_journey'],journey)
                    self.assertEqual(account['journey_source'].startswith('equity_trailing_v1:'),risk_mode=='TRAILING')
                self.assertEqual(purchase,before)

    def test_unrelated_plan_edits_and_risk_defaults_have_no_input(self):
        fields=account_fields(self.account,self.purchase)
        self.purchase['plan_id']='mutable-plan'
        self.assertEqual(account_fields(self.account,self.purchase),fields)
        self.assertEqual(fields['dd_limit_percent'],8)
        self.assertEqual(frozen_terms(fields)['daily_drawdown'],'1.37')

    def test_paid_renewal_context_is_exact_and_thread_local(self):
        trader={'id':'owner'};mt5={'account_size':5000000}
        self.assertEqual(assignment_purchase(trader,mt5,'funded',None),(None,None))
        with paid_renewal_rules(self.payout,self.source,self.purchase):
            purchase,predecessor=assignment_purchase(trader,mt5,'funded',None)
            self.assertEqual(purchase,self.purchase);self.assertEqual(predecessor,'source')
            seen=[]
            thread=threading.Thread(target=lambda:seen.append(assignment_purchase(trader,mt5,'funded',None)))
            thread.start();thread.join();self.assertEqual(seen,[(None,None)])
            with self.assertRaises(ValueError):assignment_purchase(trader,mt5,'phase1',None)
            with self.assertRaises(ValueError):assignment_purchase({'id':'other'},mt5,'funded',None)
        self.assertEqual(assignment_purchase(trader,mt5,'funded',None),(None,None))

    def test_missing_cross_purchase_and_package_rules_fail_before_assignment(self):
        for purchase in (None,dict(self.purchase,trader_id='other'),dict(self.purchase,max_drawdown=None),
                         dict(self.purchase,package_contract_id='contract'),dict(self.purchase,account_size=1000000)):
            with self.assertRaises(ValueError):
                with paid_renewal_rules(self.payout,self.source,purchase):pass
        for payout in (dict(self.payout,status='approved'),dict(self.payout,trader_account_id='other')):
            with self.assertRaises(ValueError):
                with paid_renewal_rules(payout,self.source,self.purchase):pass
        with self.assertRaises(ValueError):account_fields(dict(self.account,purchase_id='other'),self.purchase)

    def test_repair_changes_no_money_lifecycle_or_breach_fields(self):
        account=dict(self.account,current_equity=4400997.04,account_status='assigned_active',breached_at=None)
        before=copy.deepcopy(account);fields=account_fields(account,self.purchase,recovered=True)
        self.assertEqual(account,before)
        self.assertFalse({'current_equity','account_status','breached_at','profit','balance'} & fields.keys())
        self.assertIn('purchase_rule_continuity_recovered',fields['journey_source'])
        self.assertEqual(frozen_terms(fields),frozen_terms(self.purchase))

    def test_low_level_insert_has_purchased_rules_before_any_account_is_created(self):
        tree=ast.parse(Path('app.py').read_bytes())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_assign_mt5_to_trader')
        captured=[]
        class Captured(RuntimeError):pass
        db=Mock()
        def insert(row):captured.append(copy.deepcopy(row));raise Captured()
        db.table.return_value.insert.side_effect=insert
        reads=Mock(return_value=[])
        env=dict(ACCOUNT_STAGES=['phase1','phase2','funded'],clean=float,
            _np_purchase_is_rejected_v154=lambda *a,**kw:False,_np_registry_predecessor_id=lambda p:'source',
            _np_assert_mt5_pool_matches_stage=lambda *a:None,_assert_mt5_never_used=lambda *a:None,
            _np_query_rows_v173=reads,now_iso=lambda:'now',_safe_plan_for_purchase=lambda p:{'max_drawdown':20},
            _journey_for_lifecycle=lambda *a:self.purchase['challenge_journey'],_journey_source_value=lambda v:v,
            _np_plan_category_v156=lambda *a:'standard',_effective_target_percent=lambda *a:999,
            _np_renewal_db_v211=lambda:db)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'assignment','exec'),env)
        with patch('plan_risk_controls.daily_runtime_ready',return_value=True),patch('withdrawal_controls.ready',return_value=True),patch('equity_drawdown.runtime_ready',return_value=True):
            with paid_renewal_rules(self.payout,self.source,self.purchase):
                with self.assertRaises(Captured):
                    env['_assign_mt5_to_trader']({'id':'owner'},{'id':'stock','mt5_login':'123','account_size':5000000},'funded')
        self.assertEqual(len(captured),1)
        assert_bound(captured[0],self.purchase)
        self.assertIn(('neq','id','source'),reads.call_args_list[0].kwargs['filters'])

    def test_renewal_finalizer_preserves_the_already_verified_snapshot(self):
        tree=ast.parse(Path('app.py').read_bytes())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_np_v122_finalize')
        child=dict(self.account,mt5_login='123');child.update(account_fields(child,self.purchase))
        db=Mock();db.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data=[]
        env=dict(_np_v122_s=lambda v:str(v or ''),now_iso=lambda:'now',
            re=re,
            _np_v122_load_payout=lambda *a:self.payout,_np_v122_done_marker=lambda *a:'done',_np_v122_purchase_hint=lambda s:(self.purchase,'exact'),
            _np_monitoring_registry_activate=Mock(),_np_update_trader_current_pointer_v59=Mock(),
            _np_renewal_db_v211=lambda:db,_np_v122_paid_marker=lambda *a:'paid',
            _np_v122_append=lambda *a:' '.join(a),np_invalidate_admin_bootstrap=Mock(),_invalidate_trader_bootstrap_cache=Mock())
        exec(compile(ast.Module(body=[node],type_ignores=[]),'finalize','exec'),env)
        env['_np_v122_finalize'](self.payout,self.source,child)
        writes=[call.args[0] for call in db.table.return_value.update.call_args_list]
        lineage=next(w for w in writes if 'previous_trader_account_id' in w)
        self.assertEqual(lineage['journey_source'],child['journey_source'])
        env['_np_monitoring_registry_activate'].assert_called_once()

    def test_active_account_with_all_markers_lost_is_still_audited(self):
        from account_reporting import refresh_lifecycle
        account=dict(self.account,account_status='assigned_active',mt5_login='123',dd_limit_percent=20,
                     challenge_journey=['phase1','funded'],journey_source='manual_unknown_legacy_default_review')
        http=Mock();http.get.return_value.json.return_value=[self.purchase]
        rows,failed=refresh_lifecycle({'child':account},http,'https://db.test','key',fresh_rows=[account])
        self.assertFalse(failed)
        self.assertEqual(rows['child']['trading_rule_snapshot_status'],'repair_required')
        self.assertEqual(rows['child']['snapshot_repair_scope'],'trading_rules')

    def test_metadata_repair_does_not_claim_unverified_broker_history_is_healthy(self):
        from account_reporting import refresh_lifecycle
        from equity_drawdown import spec,fingerprint
        from datetime import datetime,timezone
        import json
        purchase=dict(self.purchase,id='33333333-3333-4333-8333-333333333333',trader_id='22222222-2222-4222-8222-222222222222')
        account=dict(self.account,id='11111111-1111-4111-8111-111111111111',trader_id=purchase['trader_id'],purchase_id=purchase['id'],mt5_login='123',account_status='assigned_active')
        account.update(account_fields(account,purchase,recovered=True))
        journals=[];http=Mock()
        def read(url,**kwargs):
            response=Mock();response.json.return_value=journals if url.endswith('/package_studio_private_settings') else [purchase];return response
        http.get.side_effect=read
        rows,_=refresh_lifecycle({account['id']:account},http,'https://db.test','key',fresh_rows=[account])
        self.assertEqual(rows[account['id']]['snapshot_repair_scope'],'broker_history_verification')
        self.assertEqual(rows[account['id']]['trading_rule_snapshot_status'],'repair_required')
        state=dict(rules_hash=fingerprint(spec(account)),last_observed_at=datetime.now(timezone.utc).isoformat(),breached=False)
        journals.append(dict(key='equity_drawdown_v1:'+account['id'],value=json.dumps(state)))
        rows,_=refresh_lifecycle({account['id']:account},http,'https://db.test','key',fresh_rows=[account])
        self.assertEqual(rows[account['id']]['trading_rule_snapshot_status'],'verified')
        self.assertEqual(rows[account['id']]['rule_enforcement_status'],'verified')
        state['breached']=True;journals[0]['value']=json.dumps(state)
        rows,_=refresh_lifecycle({account['id']:account},http,'https://db.test','key',fresh_rows=[account])
        self.assertEqual(rows[account['id']]['trading_rule_snapshot_status'],'repair_required')

    def test_package_reporting_uses_immutable_stage_contract_not_generic_purchase(self):
        from account_reporting import refresh_lifecycle
        from test_package_account_rules import BindingTests
        fixture=BindingTests();fixture.setUp()
        account=dict(fixture.account,account_status='assigned_active',target_percent=8,dd_limit_percent=10)
        purchase=dict(id=account['purchase_id'],trader_id=account['trader_id'],
                      package_contract_id=account['package_contract_id'],max_drawdown=20)
        http=Mock()
        http.get.side_effect=lambda url,**kw: Mock(json=lambda:[fixture.contract] if url.endswith('np_package_contracts') else [purchase])
        rows,_=refresh_lifecycle({account['id']:account},http,'https://db.test','key',fresh_rows=[account])
        self.assertEqual(rows[account['id']]['trading_rule_snapshot_status'],'verified')
        self.assertEqual(rows[account['id']]['rule_authority'],'immutable_package_contract')
        account['dd_limit_percent']=20
        rows,_=refresh_lifecycle({account['id']:account},http,'https://db.test','key',fresh_rows=[account])
        self.assertEqual(rows[account['id']]['trading_rule_snapshot_status'],'repair_required')

    def test_missing_purchase_evidence_never_labels_an_active_account_safe(self):
        from account_reporting import refresh_lifecycle
        account=dict(self.account,account_status='assigned_active',mt5_login='123')
        http=Mock();http.get.side_effect=TimeoutError()
        rows,_=refresh_lifecycle({'child':account},http,'https://db.test','key',fresh_rows=[account])
        self.assertEqual(rows['child']['trading_rule_snapshot_status'],'repair_required')
        self.assertEqual(rows['child']['snapshot_repair_scope'],'purchase_evidence_unavailable')


if __name__=='__main__':unittest.main()
