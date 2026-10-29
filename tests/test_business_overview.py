import copy, unittest
from flask import Flask, jsonify
from business_overview import build_overview, register_business_overview
from unittest.mock import Mock, patch

NOW='2026-10-22T10:00:00+00:00'
def fixture():
    return dict(generated_at=NOW,customers=[dict(id='t',name='Trader',email='t@example.invalid')],accounts=[],purchases=[],payouts=[],stock=[],tickets=[])
def account(**kw):
    return dict(id='a',trader_id='t',purchase_id='p',mt5_login='123',mt5_server='Broker',stage='funded',account_status='assigned_active',created_at=NOW,observed_at=NOW,**kw)
def purchase(**kw):
    return dict(id='p',trader_id='t',payment_status='approved',status='approved_active',fee=40000,approved_at=NOW,created_at=NOW,**kw)

class OverviewTests(unittest.TestCase):
    def test_retired_product_has_no_work_but_preserves_recorded_money(self):
        s=fixture();s['purchases']=[{**purchase(),'lifecycle_state':'retired_two_lives'},{**purchase(),'id':'pending','payment_status':'pending','has_proof':True,'lifecycle_state':'retired_two_lives'}];s['accounts']=[account()]
        s['payouts']=[dict(id='paid',trader_id='t',trader_account_id='a',status='paid',paid_at=NOW,payout_amount=1000),dict(id='pending',trader_id='t',trader_account_id='a',status='pending',payout_amount=2000)]
        d=build_overview(s);self.assertEqual(d['counts']['work_waiting'],0);self.assertEqual(d['counts'].get('open_accounts',0),0);self.assertEqual(d['amounts']['approved_receipts'],40000);self.assertEqual(d['amounts']['paid_payouts'],1000)
    def test_history_does_not_inflate_open_accounts_or_work(self):
        s=fixture();s['accounts']=[account()]+[{**account(), 'id':str(i),'account_status':'funded_profit_cap_reached','archived_at':NOW} for i in range(500)]
        d=build_overview(s)
        self.assertEqual(d['counts']['open_accounts'],1)
        self.assertEqual(d['counts']['account_history'],500)
        self.assertEqual(d['counts']['work_waiting'],0)
        self.assertEqual(d['counts']['telemetry_fresh'],1)
    def test_open_duplicates_count_once_and_conflicting_owners_excluded(self):
        s=fixture();s['accounts']=[account(),{**account(),'id':'b'}]
        d=build_overview(s);self.assertEqual(d['counts']['open_accounts'],1);self.assertEqual(d['counts']['record_checks'],2)
        s['customers'].append(dict(id='other'));s['accounts'][1]['trader_id']='other'
        d=build_overview(s);self.assertEqual(d['counts'].get('open_accounts',0),0);self.assertEqual(d['counts']['record_checks'],2)
    def test_terminal_timestamps_win_and_protected_is_separate(self):
        s=fixture();s['accounts']=[{**account(),'account_status':'profit_protected'}, {**account(),'id':'b','mt5_login':'2','breached_at':NOW}, {**account(),'id':'c','mt5_login':'3','passed_at':NOW}]
        d=build_overview(s)
        self.assertEqual(d['counts']['protected_accounts'],1);self.assertEqual(d['counts']['open_accounts'],1)
        self.assertEqual(d['counts'].get('funded_accounts',0),0)
    def test_freshness_uses_matching_observation_not_old_balance_fields(self):
        s=fixture();s['accounts']=[{**account(),'observed_at':'2026-10-22T09:56:59Z','current_equity':999999999}]
        d=build_overview(s);self.assertEqual(d['counts']['telemetry_attention'],1)
        self.assertNotIn('current_equity',d['records']['account:a'])
    def test_paid_or_rejected_payout_is_not_pending(self):
        s=fixture();s['payouts']=[dict(id='paid',trader_id='t',status='pending',paid_at=NOW,payout_amount=1000),dict(id='rejected',trader_id='t',status='pending',rejected_at=NOW,payout_amount=2000)]
        d=build_overview(s);self.assertEqual(d['counts'].get('payout_reviews',0),0)
        self.assertEqual(d['amounts']['paid_payouts'],1000)
    def test_paid_without_date_and_conflicts_are_checks_not_cash(self):
        s=fixture();s['payouts']=[dict(id='p',trader_id='t',status='paid',payout_amount=1000),dict(id='c',trader_id='t',status='rejected',paid_at=NOW,payout_amount=2000)]
        d=build_overview(s);self.assertEqual(d['amounts']['paid_payouts'],0);self.assertEqual(d['counts']['record_checks'],2)
    def test_final_zero_and_invalid_amount_are_not_replaced_with_original_fee(self):
        s=fixture();s['purchases']=[{**purchase(),'final_fee':0},{**purchase(),'id':'b','final_fee':'NaN'}]
        d=build_overview(s);self.assertEqual(d['amounts']['approved_receipts'],0)
        self.assertEqual(d['unknown_amounts']['approved_receipts'],1)
    def test_review_requires_proof_and_resets_not_new_plan_sales(self):
        s=fixture();s['purchases']=[{**purchase(),'payment_status':'pending','status':'pending_review','has_proof':True},{**purchase(),'id':'reset','payment_status':'pending','has_proof':True,'reset_order':True},{**purchase(),'id':'noproof','payment_status':'pending','has_proof':False}]
        d=build_overview(s);self.assertEqual(d['counts']['purchase_reviews'],1);self.assertEqual(d['counts']['reset_reviews'],1)
        self.assertEqual(d['counts']['work_waiting'],2);self.assertEqual(d['counts']['record_checks'],1)
    def test_exact_assignment_and_legacy_evidence_prevent_false_accounts_owed(self):
        s=fixture();s['purchases']=[purchase(),{**purchase(),'id':'legacy','mt5_login':'9'},{**purchase(),'id':'reset','reset_order':True}]
        s['accounts']=[{**account(),'archived_at':NOW}]
        d=build_overview(s);self.assertEqual(d['counts'].get('first_account_waiting',0),0)
        self.assertEqual(d['counts']['record_checks'],1)
        self.assertEqual(d['counts']['paying_customers'],1)
    def test_revenue_exclusions_do_not_erase_real_delivery_obligations(self):
        s=fixture();s['purchases']=[{**purchase(),'mark_as_test':True},{**purchase(),'id':'excluded','excluded_from_revenue':True}]
        d=build_overview(s);self.assertEqual(d['amounts']['approved_receipts'],0);self.assertEqual(d['counts']['excluded_transactions'],2);self.assertEqual(d['counts']['first_account_waiting'],1)
    def test_money_period_uses_business_dates_but_does_not_hide_open_work(self):
        s=fixture();s['purchases']=[{**purchase(),'approved_at':'2026-10-21T23:30:00Z'},{**purchase(),'id':'old','approved_at':'2026-10-01T10:00:00Z'}]
        d=build_overview(s);self.assertEqual(d['amounts']['approved_receipts'],40000);self.assertEqual(d['counts']['first_account_waiting'],2)
        self.assertEqual(build_overview(s,'all')['amounts']['approved_receipts'],80000)
    def test_checks_are_deduplicated_even_with_multiple_reasons(self):
        s=fixture();s['payouts']=[dict(id='p',trader_id='t',status='approved',payout_amount='NaN')]
        d=build_overview(s);self.assertEqual(d['counts']['record_checks'],1)
        self.assertIn('exact customer',d['reasons']['record_checks']['payout:p'])
        self.assertIn('no valid amount',d['reasons']['record_checks']['payout:p'])
    def test_pool_must_not_include_occupied_or_missing_broker(self):
        s=fixture();s['accounts']=[account()];s['stock']=[dict(id='occupied',mt5_login='123',mt5_server='Broker',status='available'),dict(id='good',mt5_login='456',mt5_server='Broker',status='available'),dict(id='duplicate',mt5_login='456',mt5_server='Broker',status='available')]
        d=build_overview(s);self.assertEqual(d['counts']['available_stock'],1);self.assertEqual(d['counts']['record_checks'],1)
    def test_auth_partial_data_and_failures_cannot_return_zero_totals(self):
        app=Flask(__name__);transport=Mock();allowed=[False]
        def auth():return ({'id':'owner'},None) if allowed[0] else (None,(jsonify(success=False),401))
        register_business_overview(app,transport,'https://db.invalid','fixture',auth)
        client=app.test_client();self.assertEqual(client.get('/admin/business_overview').status_code,401);transport.get.assert_not_called()
        allowed[0]=True
        with patch('business_overview.read_sources', return_value={'customers':[]}):
            r=client.get('/admin/business_overview?fresh=1');self.assertEqual(r.status_code,503);self.assertNotIn('data',r.json)
        with patch('business_overview.read_sources', return_value=fixture()):
            r=client.get('/admin/business_overview?fresh=1');self.assertEqual(r.status_code,200);self.assertEqual(r.headers['Cache-Control'],'no-store')
        self.assertEqual(client.get('/admin/business_overview?period=invalid').status_code,400)

    def test_all_pages_and_exact_live_identity_without_credentials_in_payload(self):
        from business_overview import read_sources
        from threading import Lock
        calls=[];lock=Lock()
        class Response:
            def __init__(self, rows):self.rows=rows
            def raise_for_status(self):pass
            def json(self):return copy.deepcopy(self.rows)
        class Transport:
            def get(self,url,headers,params,timeout):
                table=url.rsplit('/',1)[-1]
                with lock:calls.append((table,dict(params)))
                if params.get('id') or params.get('trader_account_id'):return Response([])
                if table=='trader_accounts':
                    row=account();row.pop('observed_at');return Response([row])
                if table=='np_live_account_state':return Response([dict(trader_account_id='a',trader_id='wrong-owner',mt5_login='123',observed_at=NOW)])
                if table=='traders':return Response([dict(id='t',name='Trader')])
                if table=='challenge_purchases':return Response([{**purchase(),'payment_proof_url':'private-proof','admin_note':'[np_reset_request:a:funded]'}])
                return Response([])
        result=read_sources(Transport(),'https://fixture.invalid','fixture-key')
        self.assertNotIn('observed_at',result['accounts'][0])
        self.assertTrue(result['purchases'][0]['reset_order'])
        self.assertTrue(result['purchases'][0]['has_proof'])
        self.assertNotIn('payment_proof_url',result['purchases'][0]);self.assertNotIn('admin_note',result['purchases'][0])
        self.assertTrue(any(table=='trader_accounts' and p.get('id')=='gt.a' for table,p in calls))
        self.assertFalse(any('password' in p['select'] or 'account_number' in p['select'] for _,p in calls))

if __name__=='__main__':unittest.main()
