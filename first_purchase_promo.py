"""Server-owned first account purchase promotion. No reset/renewal pricing hooks."""
from decimal import Decimal,ROUND_HALF_UP
from flask import has_request_context,request

CODE='WELCOME20'
CAMPAIGN='first_purchase_welcome20'
CODE_FIELDS=('affiliate_code','referral_code','ref_code','partner_code','promo_code','code')
ALLOWED_PATHS={'/quote_challenge_price','/create_challenge_purchase','/validate_affiliate_code'}

def requested(body):
    return any(str((body or {}).get(k) or '').strip().upper()==CODE for k in CODE_FIELDS)

def quote(body,base_fee,db,authenticate):
    if not requested(body):return None
    base=Decimal(str(base_fee)).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
    result=dict(valid=False,code=CODE,message='Sign in to use WELCOME20 on your first account purchase.',original_fee=float(base),discount_percent=0,discount_amount=0,final_fee=float(base),fee=float(base),commission_percent=0,commission_amount=0,affiliate_owner='',campaign_type=CAMPAIGN,source=None)
    if not has_request_context() or request.path.rstrip('/') not in ALLOWED_PATHS:
        result['message']='WELCOME20 is only for your first account purchase, not resets or renewals.';return result
    if any(str(body.get(k) or '').strip().upper() not in ('',CODE) for k in CODE_FIELDS):
        result['message']='Use WELCOME20 by itself. Discounts cannot be combined.';return result
    if any(body.get(k) for k in ('reset_id','renewal_id','source_account_id','trader_account_id','purchase_id')):
        result['message']='WELCOME20 cannot be used for resets, renewals or existing accounts.';return result
    trader_id,error=authenticate(body.get('trader_id'))
    if error or not trader_id:return result
    if not body.get('plan_id') or base<=0:
        result['message']='Select a current account plan before applying WELCOME20.';return result
    try:
        eligible=db.rpc('np_first_purchase_promo_eligible_v1',{'p_trader_id':str(trader_id)}).execute().data
    except Exception:
        result['message']='First-purchase eligibility could not be verified. Please try again.';return result
    if eligible is not True:
        result['message']='WELCOME20 is only for customers with no previous purchases or existing accounts. Pending purchases also count.';return result
    discount=(base*Decimal('.20')).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
    result.update(valid=True,message='WELCOME20 applied: 20% off your first account purchase only.',discount_percent=20,discount_amount=float(discount),final_fee=float(base-discount),fee=float(base-discount))
    return result
