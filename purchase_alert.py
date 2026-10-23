"""Read-only count of original plan payments requiring admin review."""
from flask import jsonify

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
        try:
            rows=guard.rows('challenge_purchases',{'select':'id,status,payment_status,mark_as_test'})
            unique={str(row['id']):row for row in rows}
            response=jsonify(success=True,pending=sum(needs_review(r) for r in unique.values()),refresh_seconds=30)
        except Exception:
            response=jsonify(success=False,error='Purchase alert is unavailable. Refresh purchases to check payment reviews.')
            response.status_code=503
        response.headers['Cache-Control']='no-store, private'
        return response
