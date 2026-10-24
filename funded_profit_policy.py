"""Current business-wide funded profit policy; custom-cap add-ons are unreleased."""
from decimal import Decimal,InvalidOperation,ROUND_DOWN
from functools import wraps
from flask import request,jsonify

CAP_PERCENT=15
RELEASE='global-funded-profit-cap-15-2026-10-24'

def maximum_payout(capital,split):
    try:
        c,s=Decimal(str(capital)),Decimal(str(split))
        if not c.is_finite() or not s.is_finite() or c<=0 or not 0<s<=100:raise ValueError()
        return (c*Decimal(CAP_PERCENT)/100*s/100).quantize(Decimal('.01'),rounding=ROUND_DOWN)
    except (ValueError,InvalidOperation,TypeError):raise ValueError('Funded capital and payout share must be verified before payment.')

def validate_payout(value,capital,split):
    limit=maximum_payout(capital,split)
    try:
        n=Decimal(str(value))
        if not n.is_finite() or n<=0:raise ValueError()
    except (ValueError,InvalidOperation,TypeError):raise ValueError('A valid payout amount is required.')
    if n>limit:raise ValueError('Business profit cap is 15% per funded cycle. Maximum trader payout for this account is '+format(limit,'.2f')+'. This request cannot be approved or paid above that amount.')
    return limit

def register_profit_policy(app,runtime):
    @app.get('/funded_profit_policy')
    def funded_profit_policy():
        return jsonify(success=True,release=RELEASE,gross_profit_cap_percent=CAP_PERCENT,scope='all_funded_accounts',paid_custom_caps_available=False)
    def protect(base,action):
        @wraps(base)
        def guarded(*args,**kwargs):
            if request.method=='OPTIONS':return base(*args,**kwargs)
            body=request.get_json(silent=True) or {}
            if action=='create':
                tid,error=runtime['_authenticated_trader_id_for_request'](body.get('trader_id'))
                if error:return runtime['bad'](error,401)
            else:
                _,error=runtime['_require_admin']()
                if error is not None:return error
            try:
                if action=='create':
                    trader=runtime['get_trader_by_id'](tid)
                    eligible,reason,account=runtime['_payout_eligibility'](trader,requested_account_id=str(body.get('trader_account_id') or body.get('account_id') or ''))
                    if not eligible:return runtime['bad'](reason,403)
                    value=body.get('amount')
                else:
                    pid=str(body.get('id') or body.get('payout_id') or '')
                    rows=runtime['_np_query_rows_v173']('payouts',filters=[('eq','id',pid)],limit=1)
                    if not rows:return runtime['bad']('Payout not found',404)
                    payout=rows[0];tid=str(payout.get('trader_id') or '')
                    accounts=runtime['_np_query_rows_v173']('trader_accounts',filters=[('eq','id',str(payout.get('trader_account_id') or ''))],limit=1)
                    account=accounts[0] if accounts else None;value=payout.get('amount')
                if not account or str(account.get('trader_id'))!=str(tid):raise ValueError('Exact funded account ownership must be verified.')
                split=runtime['_np_plan_payout_split_for_account'](account,{'id':tid},{})
                validate_payout(value,account.get('start_balance') or account.get('account_size'),split)
            except ValueError as exc:return runtime['bad'](str(exc),409)
            except Exception:return runtime['bad']('Business profit-cap verification is unavailable. No payout was authorized.',503)
            return base(*args,**kwargs)
        return guarded
    for endpoint,action in [('create_payout','create'),('approve_payout','approve'),('mark_paid','paid')]:
        if endpoint in app.view_functions:app.view_functions[endpoint]=protect(app.view_functions[endpoint],action)
