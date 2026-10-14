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
