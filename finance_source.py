"""One complete read-only financial source for admin reports; no money/lifecycle writes."""
from datetime import datetime,timezone
from flask import jsonify

FIELDS={'id','trader_id','trader_name','name','email','phone','plan_id','plan_name','selected_plan','challenge_name','account_size','status','payment_status','created_at','payment_submitted_at','payment_date','date','paid_at','payment_confirmed_at','approved_at','requested_at','updated_at','mark_as_test','excluded_from_revenue','final_fee','amount_paid','payment_amount','amount','challenge_fee','plan_fee','fee','amount_due','price','payout_amount','profit_share_amount','available_payout','bank_name','bank','payment_method','mt5_login','trader_account_id','purchase_type','type'}

def register_finance_source(app,guard,require_admin):
    @app.get('/admin/finance_source')
    def finance_source():
        _,error=require_admin()
        if error is not None:return error
        try:
            purchases=guard.rows('challenge_purchases',{'select':'*'})
            payouts=guard.rows('payouts',{'select':'*'})
            strip=lambda rows:[{k:v for k,v in row.items() if k in FIELDS} for row in rows]
            r=jsonify(success=True,timezone='Africa/Lagos',generated_at=datetime.now(timezone.utc).isoformat(),purchases=strip(purchases),payouts=strip(payouts))
        except Exception:
            r=jsonify(success=False,error='Complete financial records could not be loaded. Totals are unavailable; no partial totals are shown.');r.status_code=503
        r.headers['Cache-Control']='no-store, private'
        return r
