"""Read-only arrival alerts. Old pending payments never become new arrivals."""
from flask import jsonify, request
from datetime import datetime, timezone

PENDING={'pending','pending_review','awaiting_review','review','payment_pending','submitted','awaiting_verification','under_review','awaiting_approval','requested'}
TERMINAL={'rejected','declined','cancelled','canceled','refunded','failed','expired'}
APPROVED={'approved','paid','completed','approved_active','assigned','approved_waiting_mt5'}

def needs_review(row):
    status=str(row.get('status') or '').strip().lower()
    payment=str(row.get('payment_status') or '').strip().lower()
    if row.get('mark_as_test') is True or status in TERMINAL or payment in TERMINAL:
        return False
    if payment in {'approved','paid'} or status in APPROVED:
        return False
    return (payment or status) in PENDING

def register_purchase_alert(app,guard,require_admin):
    @app.get('/admin/purchase_alert')
    def purchase_alert():
        _,error=require_admin()
        if error is not None:return error
        observed=datetime.now(timezone.utc)
        raw=request.args.get('since')
        try:
            since=datetime.fromisoformat(raw.replace('Z','+00:00')) if raw else None
            if since and (not since.tzinfo or since>observed):raise ValueError()
        except (ValueError,TypeError):
            return jsonify(success=False,error='Invalid new-purchase checkpoint'),400
        try:
            rows=guard.rows('challenge_purchases',{'select':'id,created_at,mark_as_test'}) if since else []
            ids=set()
            for row in rows:
                if row.get('mark_as_test') is True:continue
                try:
                    created=datetime.fromisoformat(str(row.get('created_at') or '').replace('Z','+00:00'))
                    if created.tzinfo and since<created<=observed:ids.add(str(row['id']))
                except (ValueError,TypeError):
                    raise ValueError('Purchase arrival date could not be verified')
            response=jsonify(success=True,mode='new_arrivals',new_count=len(ids),new_ids=sorted(ids),observed_at=observed.isoformat(),refresh_seconds=30)
        except Exception:
            response=jsonify(success=False,error='New purchase alerts are unavailable. Refresh purchases to check arrivals.')
            response.status_code=503
        response.headers['Cache-Control']='no-store, private'
        return response
