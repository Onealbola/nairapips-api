import unittest
from sales_outcomes import build_report

class SalesOutcomesTests(unittest.TestCase):
    def p(self, id='p', **extra):
        return dict(dict(id=id, trader_id='t', created_at='2026-10-01T23:30:00Z', status='approved', plan_name='Legend'), **extra)
    def a(self,id='a',**extra):
        return dict(dict(id=id,trader_id='t',purchase_id='p',mt5_login='123',stage='phase1'),**extra)
    def test_overlap_and_no_duplicate_sales(self):
        rows=[self.a(passed_at='2026-10-03'),self.a('b',stage='funded',breached_at='2026-10-05')]
        r=build_report([self.p(),self.p()],rows,'2026-10-02','2026-10-02')
        self.assertEqual((r['summary']['sold'],r['summary']['passed'],r['summary']['funded'],r['summary']['breached']),(1,1,1,1))
    def test_exclusions(self):
        purchases=[self.p(),self.p('reset',purchase_type='funded_reset'),dict(self.p('pending'),status='pending'),dict(self.p('reject'),status='rejected',payment_status='approved')]
        self.assertEqual(build_report(purchases,[],'','')['summary']['sold'],1)
    def test_date_and_waiting(self):
        r=build_report([self.p()],[],'2026-10-01','2026-10-01')
        self.assertEqual(r['summary']['sold'],0)
        self.assertEqual(build_report([self.p()],[],'','')['summary']['awaiting_assignment'],1)
    def test_source_link_and_owner(self):
        child=dict(self.a('b',stage='funded'),purchase_id=None,source_account_id='a')
        wrong=dict(child,id='c',trader_id='other',breached_at='today')
        r=build_report([self.p()],[self.a(),child,wrong],'','')['summary']
        self.assertEqual(r['funded'],1);self.assertEqual(r['breached'],0)
    def test_invalid_dates(self):
        for start,end in [('2026-10-04','2026-10-01'),('bad','')]:
            with self.assertRaises(ValueError):build_report([],[],start,end)
    def test_no_funded_placeholder_or_archival_false_pass(self):
        a=dict(self.a(),stage='funded',mt5_login='',status='archived')
        r=build_report([self.p()],[a],'','')['summary']
        self.assertEqual(r['funded'],0);self.assertEqual(r['passed'],0)

    def test_paid_then_funded_breach_keeps_all_milestones(self):
        account=self.a(stage='funded',breached_at='2026-10-10')
        paid=dict(id='paid',trader_account_id='a',trader_id='t',status='paid',amount='42000.25',paid_at='2026-10-20')
        pending=dict(paid,id='pending',status='pending',paid_at=None,amount=99000)
        approved=dict(pending,id='approved',status='approved',paid_at='old-timestamp')
        r=build_report([self.p()],[account],'2026-10-02','2026-10-02',[paid,paid,pending,approved])
        t=r['summary']
        self.assertEqual((t['funded'],t['funded_breached'],t['collected_payout'],t['paid_payout_count']),(1,1,1,1))
        self.assertEqual(t['payout_amount'],42000.25)
    def test_wrong_owner_and_missing_account_are_not_guessed(self):
        paid=dict(id='wrong',trader_account_id='a',trader_id='other',status='paid',amount=100)
        unknown=dict(paid,id='unknown',trader_account_id='missing',trader_id='t')
        r=build_report([self.p()],[self.a()],'','',[paid,unknown])
        self.assertEqual(r['summary']['collected_payout'],0)
        self.assertEqual(r['unlinked_paid_records'],2)
    def test_multiple_paid_payments_count_purchase_once_and_sum_money(self):
        payouts=[dict(id=str(i),trader_account_id='a',trader_id='t',status='paid',amount='0.10') for i in range(3)]
        r=build_report([self.p()],[self.a()],'','',payouts)['summary']
        self.assertEqual(r['collected_payout'],1)
        self.assertEqual(r['paid_payout_count'],3)
        self.assertEqual(r['payout_amount'],0.3)
    def test_evaluation_breach_is_not_funded_breach(self):
        r=build_report([self.p()],[self.a(breached_at='today')],'','')['summary']
        self.assertEqual(r['breached'],1);self.assertEqual(r['funded_breached'],0)
    def test_invalid_money_does_not_silently_zero_total(self):
        for amount in ('NaN','Infinity','-1','bad'):
            with self.assertRaises(ValueError):
                build_report([self.p()],[self.a()],'','',[dict(id='paid',trader_account_id='a',trader_id='t',status='paid',amount=amount)])

    def test_exclusive_outcomes_reconcile_and_do_not_call_survivors_funded(self):
        purchases=[];accounts=[];payouts=[]
        specs=[('awaiting_account',None,None),
               ('no_recorded_result',dict(stage='phase1'),None),
               ('passed_not_funded',dict(stage='phase1',passed_at='today'),None),
               ('failed_before_funded',dict(stage='phase2',breached_at='today'),None),
               ('funded_no_payout',dict(stage='funded'),None),
               ('funded_breached_no_payout',dict(stage='funded',breached_at='today'),None),
               ('paid_no_funded_breach',dict(stage='funded'),True),
               ('paid_and_funded_breached',dict(stage='funded',breached_at='today'),True),
               ('needs_review',dict(stage='phase1'),True)]
        for key,a,paid in specs:
            purchases.append(self.p(key))
            if a:accounts.append(dict(self.a(key,**a),purchase_id=key))
            if paid:payouts.append(dict(id='payout-'+key,trader_id='t',trader_account_id=key,status='paid',amount=100))
        r=build_report(purchases,accounts,'','',payouts);t=r['summary']
        self.assertEqual(t['sold'],9)
        self.assertEqual(set(t['outcomes'].values()),{1})
        self.assertEqual(t['reconciliation_total'],9)
        self.assertEqual(t['failed_before_funded']+t['not_funded_yet']+t['funded']+t['needs_review'],9)
        self.assertEqual(t['not_funded_yet'],3)
        self.assertEqual(t['funded'],4)
        self.assertEqual(t['collected_payout'],3)
        for plan in r['plans']:self.assertEqual(plan['reconciliation_total'],plan['sold'])
    def test_earlier_evaluation_breach_does_not_double_count_recovered_funded_sale(self):
        accounts=[self.a('old',breached_at='old'),self.a('new',stage='funded')]
        r=build_report([self.p()],accounts,'','')
        self.assertEqual(r['summary']['failed_before_funded'],0)
        self.assertEqual(r['rows'][0]['outcome'],'funded_no_payout')

    def test_paid_then_breached_closed_requires_recorded_order_in_exact_journey(self):
        account=self.a(stage='funded',account_status='breached',breached_at='2026-10-10T12:00:00Z')
        payout=dict(id='paid',trader_id='t',trader_account_id='a',status='paid',paid_at='2026-10-09T12:00:00Z',amount=100)
        r=build_report([self.p()],[account],'','',[payout])
        self.assertTrue(r['rows'][0]['paid_then_breached_closed'])
        self.assertEqual(r['summary']['paid_then_breached_closed'],1)
        for date in ('2026-10-11T12:00:00Z','2026-10-10T12:00:00Z',None):
            r=build_report([self.p()],[account],'','',[dict(payout,paid_at=date)])
            self.assertFalse(r['rows'][0]['paid_then_breached_closed'])
            self.assertEqual(r['summary']['collected_payout'],1)
        active=dict(account,account_status='assigned_active')
        self.assertFalse(build_report([self.p()],[active],'','',[payout])['rows'][0]['paid_then_breached_closed'])
        other=self.a('other',stage='funded',account_status='breached',breached_at='2026-10-10T12:00:00Z')
        original=self.a(stage='funded',account_status='archived_reset_funded')
        self.assertTrue(build_report([self.p()],[original,other],'','',[payout])['rows'][0]['paid_then_breached_closed'])
        unrelated=dict(other,purchase_id='another-purchase')
        self.assertFalse(build_report([self.p()],[original,unrelated],'','',[payout])['rows'][0]['paid_then_breached_closed'])

class SalesOutcomesRouteTests(unittest.TestCase):
    def route(self, auth):
        import ast
        from pathlib import Path
        from flask import Flask, request, jsonify
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_np_sales_outcomes_v219')
        node.decorator_list = []
        app = Flask(__name__)
        ns = {'request': request, 'jsonify': jsonify, '_require_admin': auth,
              '_np_build_sales_outcomes_v219': build_report}
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'route-test', 'exec'), ns)
        app.add_url_rule('/report', view_func=ns['_np_sales_outcomes_v219'])
        return app.test_client()
    def test_unauthorized_does_not_read(self):
        from flask import jsonify
        client = self.route(lambda: (None, (jsonify({'success':False}),401)))
        self.assertEqual(client.get('/report').status_code,401)
    def test_invalid_dates_do_not_read(self):
        client = self.route(lambda: ({'role':'admin'},None))
        self.assertEqual(client.get('/report?from=bad').status_code,400)

if __name__=='__main__':unittest.main()
