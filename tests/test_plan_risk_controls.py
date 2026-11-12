import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

from plan_risk_controls import (selected_terms, freeze, frozen_terms, inherit,
    daily_percent, payout_due, broker_daily_realised, daily_breach_proof, WAT)
from equity_drawdown import spec, initial, evaluate
from package_plan_matrix import defaults, validate_config
from progress_plan_bridge import plan_row, is_managed, catalog_identity, offer_version


class PlanRiskControlsTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 1, 31, 12, tzinfo=timezone.utc)
        self.terms = dict(daily_drawdown='2.5', payout_frequency='Weekly')
        self.purchase = dict(id=str(uuid4()), trader_id=str(uuid4()),
            journey_source=freeze('purchase_plan_snapshot',self.terms))
        self.account = dict(id=str(uuid4()), trader_id=self.purchase['trader_id'],
            purchase_id=self.purchase['id'], mt5_login='123', stage='funded',
            dd_limit_percent=20, start_balance=1000000,
            started_at=self.start.isoformat(), journey_source=inherit('account', self.purchase))

    def test_any_finite_percentage_and_independent_switches(self):
        for limit in (0.01, 1.25, 5, 12.5, 80, 100):
            terms = selected_terms(dict(daily_drawdown=limit,daily_drawdown_enabled=True,
                payout_schedule_enabled=True,payout_frequency='Biweekly'))
            self.assertEqual(Decimal(terms['daily_drawdown']),Decimal(str(limit)))
            self.assertEqual(terms['payout_frequency'],'Biweekly')
            terms = selected_terms(dict(daily_drawdown=limit,daily_drawdown_enabled=False))
            self.assertEqual(terms,dict(daily_drawdown='0',payout_frequency='On Demand'))
        for value in (-1,101,float('nan'),float('inf'),True):
            with self.assertRaises(ValueError):selected_terms(dict(daily_drawdown=value))

    def test_all_models_keep_stable_product_identity_with_different_risk_modes(self):
        from package_plan_matrix import make_plan
        for model in ('one_phase','two_phase','instant'):
            allocation = make_plan('fixture-'+model,1000000,20000,model=model)
            original = plan_row(allocation,allocation['packages'][0])
            package = allocation['packages'][0]
            package.update(daily_drawdown=3.75,daily_drawdown_enabled=True,
                payout_schedule_enabled=True,payout_frequency='Monthly',drawdown_type='TRAILING')
            changed = plan_row(allocation,package)
            self.assertEqual(changed['id'],original['id'])
            self.assertTrue(is_managed(changed))
            self.assertEqual(catalog_identity(changed),'progress:fixture-'+model+':basic')
            self.assertNotEqual(offer_version(changed),offer_version(original))
            self.assertEqual(changed['payout_frequency'],'Monthly')
            self.assertEqual(changed['daily_drawdown'],3.75)
            self.assertEqual(changed['drawdown_type'],'TRAILING')
            self.assertEqual(plan_row(allocation,allocation['packages'][1])['drawdown_type'],'STATIC')

    def test_custom_daily_values_round_trip_without_changing_old_saved_shape(self):
        config = defaults()['config']
        package = config['plans'][0]['packages'][0]
        package.update(daily_drawdown=1.37,daily_drawdown_enabled=True,
            payout_schedule_enabled=True,payout_frequency='Weekly')
        self.assertEqual(validate_config(config)['plans'][0]['packages'][0],package)
        for allocation in config['plans']:
            for p in allocation['packages']:
                for key in ('daily_drawdown_enabled','payout_schedule_enabled','payout_frequency'):
                    p.pop(key,None)
        self.assertEqual(validate_config(config),config)

    def test_issued_rules_are_independent_of_later_plan_edits(self):
        before = copy.deepcopy(self.account)
        changed = dict(daily_drawdown='20',payout_frequency='Monthly')
        future_plan = dict(journey_source=freeze('plan',changed))
        self.assertEqual(frozen_terms(self.account),self.terms)
        self.assertEqual(self.account,before)
        self.assertEqual(daily_percent(future_plan),20)
        self.assertEqual(daily_percent(dict(journey_source='legacy',daily_drawdown=20)),0)
        self.assertIsNone(frozen_terms(dict(journey_source=inherit('account',{}))))

    def test_first_weekly_payout_and_paid_exact_account_anchor(self):
        due = payout_due(self.account,self.purchase)
        self.assertEqual(due,self.start+timedelta(days=7))
        paid = dict(trader_account_id=self.account['id'],trader_id=self.account['trader_id'],
            status='paid',created_at=(self.start+timedelta(days=9)).isoformat())
        self.assertEqual(payout_due(self.account,self.purchase,[paid]),self.start+timedelta(days=16))
        for key,value in [('trader_account_id','another-account'),('trader_id','another-trader'),('status','approved')]:
            self.assertEqual(payout_due(self.account,self.purchase,[dict(paid,**{key:value})]),due)

    def test_calendar_month_and_biweekly_are_distinct_from_arbitrary_fixed_days(self):
        for frequency,expected in [('Monthly',datetime(2026,2,28,12,tzinfo=timezone.utc)),
                                   ('Biweekly',self.start+timedelta(days=14)),
                                   ('Daily',self.start+timedelta(days=1))]:
            terms = dict(self.terms,payout_frequency=frequency)
            purchase = dict(self.purchase,journey_source=freeze('purchase',terms))
            account = dict(self.account,journey_source=inherit('account',purchase))
            self.assertEqual(payout_due(account,purchase),expected)

    def test_legacy_and_on_demand_do_not_gain_a_waiting_period(self):
        self.assertIsNone(payout_due(dict(self.account,journey_source='legacy'),{}))
        terms = dict(self.terms,payout_frequency='On Demand')
        self.assertIsNone(payout_due(dict(self.account,journey_source=freeze('account',terms)),{}))
        with self.assertRaises(ValueError):payout_due(self.account,dict(self.purchase,id='wrong'))

    def test_broker_day_net_excludes_transfers_and_includes_fees(self):
        terminal = Mock()
        terminal.account_info.return_value=SimpleNamespace(login=123,balance=1999463)
        terminal.history_deals_get.return_value=[
            SimpleNamespace(ticket=1,time=int(datetime(2026,2,2,10,tzinfo=timezone.utc).timestamp()),type=0,profit=-1000,commission=-20,swap=-5,fee=-2),
            SimpleNamespace(ticket=2,time=int(datetime(2026,2,2,11,tzinfo=timezone.utc).timestamp()),type=1,profit=500,commission=-10,swap=0,fee=0),
            SimpleNamespace(ticket=3,time=int(datetime(2026,2,2,9,tzinfo=timezone.utc).timestamp()),type=2,profit=1000000)]
        observed = datetime(2026,2,2,12,tzinfo=timezone.utc)
        self.assertEqual(broker_daily_realised(terminal,self.account,observed.isoformat()),Decimal('-537'))
        self.assertEqual(terminal.history_deals_get.call_args.args[0],self.start)
        terminal.history_deals_get.return_value=None
        with self.assertRaises(ValueError):broker_daily_realised(terminal,self.account,observed.isoformat())

    def test_daily_history_reconciles_prior_day_losses_without_counting_them_today(self):
        terminal=Mock();terminal.account_info.return_value=SimpleNamespace(login=123,balance=4702014.48)
        account=dict(self.account,start_balance=5000000,started_at='2026-11-10T11:09:45+00:00')
        yesterday=datetime(2026,11,11,20,tzinfo=timezone.utc);today=datetime(2026,11,12,7,25,12,tzinfo=timezone.utc)
        terminal.history_deals_get.return_value=[SimpleNamespace(ticket=1,time=int(yesterday.timestamp()),type=1,profit=-190189.84),SimpleNamespace(ticket=2,time=int(today.timestamp()),type=1,profit=-107795.68)]
        observed=datetime(2026,11,12,15,tzinfo=timezone.utc)
        self.assertEqual(broker_daily_realised(terminal,account,observed.isoformat()),Decimal('-107795.68'))
        # A genuine empty trading day is accepted only with the complete prior ledger.
        self.assertEqual(broker_daily_realised(terminal,account,(observed+timedelta(days=1)).isoformat()),Decimal(0))
        # A stale empty broker cache cannot turn known balance losses into zero DD.
        terminal.history_deals_get.return_value=[]
        with self.assertRaisesRegex(ValueError,'does not reconcile'):broker_daily_realised(terminal,account,observed.isoformat())

    def test_empty_day_history_needs_real_funding_evidence_even_at_starting_balance(self):
        terminal=Mock();terminal.account_info.return_value=SimpleNamespace(login=123,balance=1000000)
        observed=datetime(2026,2,2,12,tzinfo=timezone.utc)
        terminal.history_deals_get.return_value=[]
        with self.assertRaisesRegex(ValueError,'does not reconcile'):broker_daily_realised(terminal,self.account,observed.isoformat())
        funding=SimpleNamespace(ticket=1,time=int((self.start-timedelta(minutes=1)).timestamp()),type=2,profit=1000000)
        terminal.history_deals_get.side_effect=[[],[funding]]
        self.assertEqual(broker_daily_realised(terminal,self.account,observed.isoformat()),Decimal(0))

    def test_full_ledger_verifies_initial_funding_after_delivery_without_double_count(self):
        terminal=Mock();terminal.account_info.return_value=SimpleNamespace(login=123,balance=4702014.48)
        account=dict(self.account,start_balance=5000000,started_at='2026-11-10T11:09:45+00:00')
        observed=datetime(2026,11,12,15,tzinfo=timezone.utc)
        full=[SimpleNamespace(ticket=1,time=int(datetime(2026,11,10,11,10,tzinfo=timezone.utc).timestamp()),type=2,profit=5000000),SimpleNamespace(ticket=2,time=int(datetime(2026,11,11,20,tzinfo=timezone.utc).timestamp()),type=1,profit=-190189.84),SimpleNamespace(ticket=3,time=int(datetime(2026,11,12,7,tzinfo=timezone.utc).timestamp()),type=1,profit=-107795.68)]
        for first in (full,[]):
            terminal.history_deals_get.side_effect=[first,full]
            self.assertEqual(broker_daily_realised(terminal,account,observed.isoformat()),Decimal('-107795.68'))
        # A complete ledger from an incorrectly capitalised pool account is rejected.
        full[0].profit=50000000;terminal.account_info.return_value.balance=49702014.48
        terminal.history_deals_get.side_effect=[full,full]
        with self.assertRaisesRegex(ValueError,'does not reconcile'):broker_daily_realised(terminal,account,observed.isoformat())

    def test_transfers_are_reconciled_but_do_not_disguise_daily_breach(self):
        terminal=Mock();terminal.account_info.return_value=SimpleNamespace(login=123,balance=4900000)
        observed=datetime(2026,2,2,12,tzinfo=timezone.utc);stamp=int((observed-timedelta(hours=1)).timestamp())
        terminal.history_deals_get.return_value=[SimpleNamespace(ticket=1,time=stamp,type=2,profit=200000),SimpleNamespace(ticket=2,time=stamp,type=2,profit=-50000),SimpleNamespace(ticket=3,time=stamp,type=0,profit=-250000)]
        account=dict(self.account,start_balance=5000000,journey_source=freeze('test',dict(daily_drawdown='5',payout_frequency='Weekly')))
        realised=broker_daily_realised(terminal,account,observed.isoformat())
        self.assertEqual(realised,Decimal('-250000'))
        rules=spec(account,self.purchase);state=evaluate(rules,initial(rules),4900000,observed.isoformat(),balance=4900000,daily_realised=realised)
        self.assertTrue(state['breached']);self.assertEqual(Decimal(state['daily_used_percent']),Decimal(100))
        recovered=evaluate(rules,state,5000000,(observed+timedelta(days=1)).isoformat(),balance=5000000,daily_realised=0)
        self.assertEqual(recovered,state)

    def test_history_identity_conflicts_and_incomplete_pages_fail_closed(self):
        terminal=Mock();observed=datetime(2026,2,2,12,tzinfo=timezone.utc)
        terminal.account_info.return_value=SimpleNamespace(login=123,balance=1000000)
        deal=SimpleNamespace(ticket=1,time=int(observed.timestamp()),type=0,profit=0)
        terminal.history_deals_get.return_value=[deal,deal]
        with self.assertRaisesRegex(ValueError,'duplicated'):broker_daily_realised(terminal,self.account,observed.isoformat())
        terminal.history_deals_get.return_value=[SimpleNamespace(ticket=1,type=0,profit=0)]
        with self.assertRaisesRegex(ValueError,'outside'):broker_daily_realised(terminal,self.account,observed.isoformat())
        terminal.history_deals_get.return_value=[]
        terminal.account_info.side_effect=[SimpleNamespace(login=123,balance=1000000),SimpleNamespace(login=999,balance=1000000)]
        with self.assertRaisesRegex(ValueError,'changed'):broker_daily_realised(terminal,self.account,observed.isoformat())

    def test_daily_limit_counts_realised_and_floating_loss_and_is_latched(self):
        rules = spec(self.account)
        observed = self.start+timedelta(seconds=1)
        state = evaluate(rules,initial(rules),976000,observed.isoformat(),balance=990000,daily_realised=-10000)
        self.assertFalse(state['breached'])
        self.assertEqual(Decimal(state['daily_used_percent']),Decimal(96))
        self.assertEqual(Decimal(state['drawdown_floor']),Decimal(975000))
        state = evaluate(rules,state,975000,(observed+timedelta(seconds=1)).isoformat(),balance=990000,daily_realised=-10000)
        self.assertTrue(state['breached'])
        self.assertEqual(evaluate(rules,state,1200000,(observed+timedelta(days=1)).isoformat(),balance=1200000,daily_realised=0),state)

    def test_midnight_changes_day_without_reusing_previous_daily_trading_loss(self):
        rules = spec(self.account)
        before=datetime(2026,2,1,22,59,tzinfo=timezone.utc)
        state=evaluate(rules,initial(rules),980000,before.isoformat(),balance=980000,daily_realised=-20000)
        after=before+timedelta(minutes=2)
        state=evaluate(rules,state,980000,after.isoformat(),balance=980000,daily_realised=0)
        self.assertEqual(Decimal(state['daily_used_percent']),0)
        self.assertEqual(state['daily_day'],'2026-02-02')
        self.assertFalse(state['breached'])

    def test_missing_history_cannot_bypass_existing_overall_breach(self):
        rules=spec(self.account)
        with self.assertRaises(ValueError):evaluate(rules,initial(rules),990000,self.start.isoformat(),balance=1000000)
        self.assertTrue(evaluate(rules,initial(rules),800000,self.start.isoformat(),balance=1000000)['breached'])

    def test_static_and_trailing_daily_limits_coexist(self):
        static=spec(self.account)
        trailing=spec(dict(self.account,journey_source='equity_trailing_v1:'+self.account['journey_source']))
        for rules in (static,trailing):
            state=evaluate(rules,initial(rules),1100000,self.start.isoformat(),balance=1100000,daily_realised=100000)
            state=evaluate(rules,state,975000,(self.start+timedelta(seconds=1)).isoformat(),balance=1000000,daily_realised=0)
            self.assertTrue(state['breached'])

    def test_authenticated_historic_daily_breach_math_is_bound_to_account_start(self):
        data=dict(timestamp=self.start.isoformat(),current_equity=975000,current_balance=990000,daily_realised=-10000)
        self.assertTrue(daily_breach_proof(self.account,data,now=self.start+timedelta(days=3))['breached'])
        with self.assertRaises(ValueError):daily_breach_proof(self.account,dict(data,current_equity=999000),now=self.start)
        with self.assertRaises(ValueError):daily_breach_proof(self.account,dict(data,timestamp=(self.start-timedelta(days=1)).isoformat()),now=self.start)

    def test_floating_recovery_offsets_today_realised_loss_for_daily_limit(self):
        from equity_drawdown import initial,evaluate
        rules={'account_id':'a','trader_id':'t','mt5_login':'1','stage':'phase1',
               'capital':'1000000','limit':'20','drawdown_type':'STATIC','daily_drawdown':'5'}
        state=evaluate(rules,initial(rules),1000000,self.start.isoformat(),balance=940000,daily_realised=-60000)
        self.assertFalse(state['breached'])
        self.assertEqual(float(state['daily_loss_percent']),0)

    def test_legacy_disabled_daily_text_loads_and_stage_reset_price_is_preserved(self):
        from plan_risk_controls import plan_controls_view,edit_plan_controls
        row={'daily_drawdown':'None','reset_fee':0,'funded_reset_fee':0}
        self.assertEqual(plan_controls_view(row)['daily_drawdown'],0)
        row=edit_plan_controls(row,{'phase2_reset_enabled':True,'phase2_reset_fee':8750.5})
        row=edit_plan_controls(row,{'daily_drawdown_enabled':False,'payout_schedule_enabled':True,'payout_frequency':'Weekly','reset_fee':0})
        self.assertEqual(frozen_terms(row)['resets'],{'phase2':'8750.5'})

    def test_paid_resets_are_opt_in_and_independent_for_every_stage(self):
        from plan_commercial import reset_price
        from package_plan_matrix import make_plan
        allocation=make_plan('resets',1000000,20000)
        package=allocation['packages'][0]
        self.assertEqual(reset_price(plan_row(allocation,package),'funded'),0)
        package.update(phase2_reset_enabled=True,phase2_reset_fee=8750.50,
                       funded_reset_enabled=True,funded_reset_fee=25000)
        selected=plan_row(allocation,package)
        self.assertEqual(reset_price(selected,'phase1'),0)
        self.assertEqual(reset_price(selected,'phase2'),8750.50)
        self.assertEqual(reset_price(selected,'funded'),25000)
        # A paid reset order freezes its quoted price; editing the offer does not
        # mutate the selected quote or purchased DD terms.
        quoted=copy.deepcopy(selected)
        package['funded_reset_fee']=31000
        self.assertEqual(reset_price(plan_row(allocation,package),'funded'),31000)
        self.assertEqual(reset_price(quoted,'funded'),25000)
        self.assertEqual(selected['drawdown_type'],'STATIC')
        self.assertEqual(plan_row(allocation,allocation['packages'][1])['reset_fee'],0)

    def test_enabled_reset_needs_a_real_price_and_never_enables_other_stages(self):
        for value in (0,-1,True,float('nan'),1.234):
            with self.assertRaises(ValueError):selected_terms(dict(phase1_reset_enabled=True,phase1_reset_fee=value))
        selected=selected_terms(dict(phase1_reset_enabled=True,phase1_reset_fee=12500))
        self.assertEqual(selected['resets'],{'phase1':'1.25E+4'})
        self.assertEqual(frozen_terms(dict(journey_source=freeze('plan',selected))),selected)


class RuntimeCapabilityTests(unittest.TestCase):
    def response(self, rows):
        import json
        result=Mock(status_code=200)
        result.json.return_value=rows
        return result

    def test_four_broker_probes_record_only_technical_capability(self):
        import plan_risk_controls as controls
        import json
        sources=[{'source':'dd_police_daily_v1_shard_%d_of_4'%n} for n in range(1,5)]
        saved=[{'value':json.dumps({'release':controls.RELEASE})}]
        with patch.dict('os.environ',{'SUPABASE_URL':'https://fixture.invalid','SUPABASE_KEY':'fixture-only'},clear=True), patch.object(controls,'_ready_cache',(-float('inf'),False)), patch('requests.get',side_effect=[self.response([]),self.response(sources)]), patch('requests.post',return_value=self.response(saved)) as post:
            self.assertTrue(controls.daily_runtime_ready())
            self.assertEqual(post.call_args.kwargs['json']['key'],'plan_risk_controls_runtime_v1')
            self.assertNotIn('trader',str(post.call_args.kwargs['json']))

    def test_verified_installation_survives_empty_roster_but_detects_rollback(self):
        import plan_risk_controls as controls
        import json
        saved=[{'value':json.dumps({'release':controls.RELEASE})}]
        for sources,expected in [([],True),([{'source':'dd_police_live_mt5_v320'}],False)]:
            with patch.dict('os.environ',{'SUPABASE_URL':'https://fixture.invalid','SUPABASE_KEY':'fixture-only'},clear=True), patch.object(controls,'_ready_cache',(-float('inf'),False)), patch('requests.get',side_effect=[self.response(saved),self.response(sources)]), patch('requests.post') as post:
                self.assertEqual(controls.daily_runtime_ready(),expected)
                post.assert_not_called()

    def test_partial_shards_never_enable_daily_controls(self):
        import plan_risk_controls as controls
        with patch.dict('os.environ',{'SUPABASE_URL':'https://fixture.invalid','SUPABASE_KEY':'fixture-only'},clear=True), patch.object(controls,'_ready_cache',(-float('inf'),False)), patch('requests.get',side_effect=[self.response([]),self.response([{'source':'dd_police_daily_v1_shard_1_of_4'}])]), patch('requests.post') as post:
            self.assertFalse(controls.daily_runtime_ready())
            post.assert_not_called()
