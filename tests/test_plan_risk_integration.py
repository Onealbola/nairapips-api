"""Execute changed integration handlers only; never import production workers."""
import ast, copy, unittest
from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import Mock
from plan_risk_controls import freeze
TREE=ast.parse(Path('app.py').read_text())
def extract(name,ns):
    node=copy.deepcopy([n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name==name][-1])
    exec(compile(ast.Module(body=[node],type_ignores=[]),'risk-integration','exec'),ns)
    return ns[name]
class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.source=freeze('plan',dict(daily_drawdown='0',payout_frequency='Weekly'))
        self.account=dict(id='a',purchase_id='p',trader_id='t',started_at=datetime.now(timezone.utc).isoformat(),journey_source=self.source)
        self.purchase=dict(id='p',trader_id='t',journey_source=self.source)
    def handler(self,eligible=True,reason='eligible'):
        self.query=Mock(side_effect=lambda table,**kw:[self.purchase] if table=='challenge_purchases' else [])
        ns=dict(_np_payout_eligibility_risk_controls_base=lambda *a,**kw:(eligible,reason,self.account),
                _np_query_rows_v173=self.query,datetime=datetime,timezone=timezone)
        return extract('_payout_eligibility',ns)
    def test_scheduled_handler_blocks_early_payout_and_exposes_date(self):
        allowed,reason,account=self.handler()({'id':'t'},requested_account_id='a')
        self.assertFalse(allowed);self.assertIn('next payout',reason)
        self.assertIn('next_payout_at',account)
        self.assertEqual(self.query.call_args_list[1].kwargs['filters'][0],('eq','trader_account_id','a'))
    def test_legacy_handler_adds_no_queries(self):
        self.account.pop('journey_source')
        self.assertTrue(self.handler()({'id':'t'})[0]);self.query.assert_not_called()
    def test_schedule_never_overrides_base_ineligibility(self):
        self.account['started_at']=(datetime.now(timezone.utc)-timedelta(days=8)).isoformat()
        self.assertEqual(self.handler(False,'Original restriction')({'id':'t'})[:2],(False,'Original restriction'))
    def test_purchase_mismatch_fails_closed(self):
        self.purchase['trader_id']='other'
        self.assertFalse(self.handler()({'id':'t'})[0])
    def test_new_reset_offer_cannot_fall_back_to_another_plan(self):
        class Query:
            def table(self,*a):return self
            def select(self,*a):return self
            def eq(self,*a):return self
            def execute(self):return type('Reply',(),{'data':[dict(id='other',status='active',fee=1,reset_fee=1)]})()
        func=extract('_np_reset_current_plan_for',dict(clean=lambda x:float(x or 0),supabase=Query()))
        self.assertIsNone(func(dict(account_size=500000,journey_source=self.source),dict(plan_id='chosen')))
    def test_daily_reset_configuration_survives_purchase_snapshot(self):
        func=extract('_np_plan_rule_snapshot_v156',dict(_np_plan_category_v156=lambda x:x,_effective_payout_split=lambda x:60,clean=lambda x:float(x or 0),now_iso=lambda:'now'))
        terms=dict(daily_drawdown='2',payout_frequency='Weekly',resets={'phase2':'8750'})
        plan=dict(journey_source=freeze('chosen',terms),fee=20000,payout_cycles=3)
        result=func(plan)
        self.assertEqual(result['journey_source'],plan['journey_source'])
