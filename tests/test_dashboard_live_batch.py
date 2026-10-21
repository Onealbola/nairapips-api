import ast,copy,json,unittest,uuid
from pathlib import Path
from unittest.mock import Mock
from flask import Flask,jsonify
from dashboard_live_batch import load_observations

class DashboardBatchTests(unittest.TestCase):
    def test_repeated_accounts_use_one_read_and_never_share_by_login(self):
        owner=str(uuid.uuid4());ids=[str(uuid.uuid4()) for _ in range(74)]
        rows=[dict(id=x,trader_id=owner,mt5_login='same-login') for x in ids]
        http=Mock();snap=dict(trader_account_id=ids[0],trader_id=owner,mt5_login='same-login',balance=123)
        http.get.return_value.json.return_value=[snap]
        result=load_observations(http,'https://fixture.invalid','fixture',owner,rows*4)
        self.assertEqual(http.get.call_count,1);self.assertEqual(result,{ids[0]:snap})
        self.assertEqual(http.get.call_args.kwargs['timeout'],(2,4))

    def test_identity_mismatches_and_foreign_account_are_rejected(self):
        owner=str(uuid.uuid4());aid=str(uuid.uuid4());other=str(uuid.uuid4())
        rows=[dict(id=aid,trader_id=owner,mt5_login='123'),dict(id=other,trader_id='foreign',mt5_login='456')]
        http=Mock()
        http.get.return_value.json.return_value=[dict(trader_account_id=aid,trader_id='foreign',mt5_login='123'),dict(trader_account_id=other,trader_id=owner,mt5_login='456'),dict(trader_account_id=aid,trader_id=owner,mt5_login='wrong')]
        self.assertEqual(load_observations(http,'https://fixture.invalid','fixture',owner,rows),{})
        self.assertNotIn(other,http.get.call_args.kwargs['params']['trader_account_id'])

    def test_actual_bootstrap_and_payout_overlay_do_not_requery_missing_74_rows(self):
        tree=ast.parse(Path('app.py').read_text());app=Flask('batch-fixture');owner=str(uuid.uuid4())
        rows=[dict(id=str(uuid.uuid4()),trader_id=owner,mt5_login='same-login',stage='funded',account_status='funded_profit_cap_reached',start_balance=1000000,current_balance=1000000,current_equity=1000000) for _ in range(74)]
        root={'trader':{'id':owner},'current_account':rows[0],'active_accounts':rows,'accounts':rows,'all_accounts':rows}
        http=Mock();http.get.return_value.json.return_value=[]
        forbidden=Mock(side_effect=AssertionError('Per-account query must not run'))
        ns=dict(app=app,json=json,requests=http,SUPABASE_URL='https://fixture.invalid',SUPABASE_KEY='fixture',SUPABASE_SERVICE_ROLE_KEY='fixture-server',
                _NP_V198_LIVE_STATUSES={'funded_profit_cap_reached'},clean=lambda x:float(x or 0),
                NAIRAPIPS_V198_TRADER_BOOTSTRAP_LIVE_METRICS='fixture',
                NAIRAPIPS_V195_MT5_477365592_MANAGEMENT_VERIFIED_PAYOUT='fixture-management',
                _np_query_rows_v173=forbidden,_np_plan_payout_split_for_account=forbidden,
                _np_v198_trader_bootstrap_base=lambda:jsonify(success=True,data=copy.deepcopy(root)))
        names=['_np_v198_is_live_row','_np_v198_overlay_latest','_np_v198_trader_bootstrap','_np_verified_payout_quote','_np_v201_bootstrap']
        nodes=[next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name) for name in names]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'isolated-native-dashboard','exec'),ns)
        ns['_np_v201_bootstrap_base']=ns['_np_v198_trader_bootstrap']
        with app.test_request_context('/trader_bootstrap'):
            response=ns['_np_v201_bootstrap']();data=response.get_json()['data']
        self.assertEqual(http.get.call_count,1);forbidden.assert_not_called()
        self.assertEqual(len(data['all_accounts']),74)
        self.assertTrue(all(not q['verified'] for q in data['payout_quotes'].values()))
        self.assertTrue(all(a['current_balance']==1000000 for a in data['all_accounts']))
