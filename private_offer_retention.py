"""Registered-member search and owner-bound, expiring, single-use recovery offers."""
from contextvars import ContextVar
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
from functools import wraps
import re,uuid
from flask import request,jsonify,has_request_context


def expiry(value):
    try:
        dt=datetime.fromisoformat(str(value).replace('Z','+00:00'))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except (TypeError,ValueError):return None


def expired(row,now=None):
    end=expiry(row.get('expires_at') or row.get('offer_expires_at') or row.get('expiry_date') or row.get('expires'))
    return end is None or end<=(now or datetime.now(timezone.utc))


def discount(value):
    if isinstance(value,bool):raise ValueError('Enter a discount greater than 0 and below 100%')
    try:n=Decimal(str(value))
    except InvalidOperation:raise ValueError('Enter a valid percentage')
    if not n.is_finite() or not 0<n<100:raise ValueError('Enter a discount greater than 0 and below 100%')
    return float(n)


def matches(row,trader):
    # An explicit owner ID always wins; email/phone cannot borrow another owner’s offer.
    target=str(row.get('target_trader_id') or '').strip()
    if target:return target==str(trader.get('id') or '')
    email=str(row.get('target_email') or '').strip().casefold()
    return bool(email and email==str(trader.get('email') or '').strip().casefold())


class Retention:
    def __init__(self,g):
        self.g=g;self.http=g['requests'];self.base=g['SUPABASE_URL'].rstrip('/')+'/rest/v1/'
        key=g.get('SUPABASE_SERVICE_ROLE_KEY') or g['SUPABASE_KEY']
        self.headers={'apikey':key,'Authorization':'Bearer '+key,'Prefer':'return=representation'}
        self.reservation=ContextVar('np_private_offer_reservation',default=None)
    def read(self,table,params):
        r=self.http.get(self.base+table,headers=self.headers,params=params,timeout=(2,8));r.raise_for_status();rows=r.json()
        if not isinstance(rows,list):raise ValueError('Offer records could not be verified')
        return rows
    def patch(self,identity,status,payload):
        r=self.http.patch(self.base+'announcements',headers=self.headers,params={'id':'eq.'+str(identity),'status':'eq.'+status},json=payload,timeout=(2,8));r.raise_for_status();rows=r.json()
        if not isinstance(rows,list):raise ValueError('Offer state could not be verified')
        return rows
    def insert(self,row):
        r=self.http.post(self.base+'announcements',headers=self.headers,json=row,timeout=(2,8));r.raise_for_status();rows=r.json()
        if not isinstance(rows,list) or len(rows)!=1:raise ValueError('Offer was not confirmed in storage')
        return rows
    def member(self,identity='',email=''):
        if not identity and not email:return None
        filters={'id':'eq.'+str(identity)} if identity else {'email':'eq.'+str(email).strip().lower()}
        rows=self.read('traders',{'select':'*','limit':'2',**filters})
        if not rows and not identity and email:
            try:rows=self.read('traders',{'select':'*','limit':'2','canonical_email':'eq.'+str(email).strip().lower()})
            except Exception:return None
        return rows[0] if len(rows)==1 else None
    def offers(self):
        after=None
        for _ in range(100):
            params={'select':'*','type':'eq.private_offer','order':'id.asc','limit':'200'}
            if after:params['id']='gt.'+after
            rows=self.read('announcements',params)
            for row in rows:yield self.g['_np_offer_merge_meta'](row)
            if len(rows)<200:return
            next_id=str(rows[-1]['id'])
            if next_id==after:raise ValueError('Offer pagination did not advance')
            after=next_id
        raise ValueError('Offer records exceed the verified read limit')
    def trader(self,data):
        tid,error=self.g['_authenticated_trader_id_for_request'](str(data.get('trader_id') or '') or None)
        if error or not tid:raise ValueError('Sign in as the trader receiving this offer')
        row=self.member(tid) or {}
        if str(row.get('id') or '')!=str(tid):raise ValueError('Registered trader could not be verified')
        return row
    def account_scope(self,trader,account_id,cutoff=None):
        rows=self.read('trader_accounts',{'select':'id,trader_id,purchase_id,account_size,stage,mt5_login,account_status,breached_at,created_at','id':'eq.'+str(account_id),'trader_id':'eq.'+str(trader['id']),'limit':'1'})
        if len(rows)!=1:raise ValueError('Exact breached account could not be verified')
        a=rows[0]
        if str(a.get('trader_id'))!=str(trader['id']) or not a.get('breached_at'):raise ValueError('Choose a genuinely breached account belonging to this trader')
        if cutoff and (expiry(a['breached_at']) is None or expiry(a['breached_at'])>cutoff):raise ValueError('The account was not breached when this offer was issued')
        purchases=self.read('challenge_purchases',{'select':'id,trader_id,plan_id,plan_name,account_size,payout_cycles,payment_status','id':'eq.'+str(a['purchase_id']),'trader_id':'eq.'+str(trader['id']),'limit':'1'})
        if len(purchases)!=1:raise ValueError('Exact source purchase could not be verified')
        p=purchases[0]
        if (p.get('id')!=a['purchase_id'] or str(p.get('trader_id'))!=str(trader['id']) or p.get('payment_status')!='approved' or not p.get('plan_id') or Decimal(str(p['account_size']))!=Decimal(str(a['account_size']))):raise ValueError('The breached purchase does not match this account')
        return dict(source_account_id=a['id'],source_purchase_id=p['id'],eligible_plan_id=p['plan_id'],account_size=str(p['account_size']),payout_cycles=int(p.get('payout_cycles') if p.get('payout_cycles') is not None else 3),plan_name=p.get('plan_name') or '',mt5_login=a.get('mt5_login') or '')
    def scope(self,row,trader):
        saved=row.get('recovery_scope')
        if not isinstance(saved,dict) or not saved.get('source_account_id'):raise ValueError('This recovery offer needs its exact breached account linked by admin')
        actual=self.account_scope(trader,saved['source_account_id'],expiry(row.get('created_at')))
        for key in ('source_account_id','source_purchase_id','eligible_plan_id','payout_cycles'):
            if str(saved.get(key))!=str(actual[key]):raise ValueError('Recovery offer account or purchase scope changed')
        if Decimal(str(saved.get('account_size')))!=Decimal(actual['account_size']):raise ValueError('Recovery offer account size changed')
        return actual
    def eligible(self,row,trader,data):
        scope=self.scope(row,trader)
        if str(data.get('plan_id') or '')!=str(scope['eligible_plan_id']):raise ValueError('This discount applies only to '+scope['plan_name']+' for the linked breached account')
        verified=request.environ.get('np_verified_checkout_plan_v221') if has_request_context() else None
        if verified and (str(verified.get('id'))!=str(scope['eligible_plan_id']) or Decimal(str(verified.get('account_size')))!=Decimal(scope['account_size'])):raise ValueError('Verified checkout does not match this recovery offer')
        if data.get('account_size') is not None and Decimal(str(data['account_size']))!=Decimal(scope['account_size']):raise ValueError('This discount does not apply to another account size')
        if int(data['payout_cycles'] if data.get('payout_cycles') is not None else 3)!=scope['payout_cycles']:raise ValueError('This discount applies only to the original payout-cycle option')
        return scope
    def find(self,data,code):
        held=self.reservation.get()
        if held and code==held['code']:
            trader=self.trader(data)
            if held['trader']==str(trader['id']):return held['offer'],False
        candidates=[r for r in self.offers() if self.g['_aff_code'](r.get('offer_code') or 'PHASEHELP')==code]
        if not candidates:return None,False # Ordinary affiliate codes retain their existing flow.
        trader=self.trader(data)
        own=[r for r in candidates if matches(r,trader)]
        own.sort(key=lambda r:str(r.get('created_at') or ''),reverse=True)
        return (own[0] if own else None),not bool(own)
    def quote(self,data,base,code):
        code=self.g['_aff_code'](code)
        if not code:return None
        try:
            row,foreign=self.find(data,code)
            if not row:
                return self.invalid(base,code,'This private offer belongs to another trader') if foreign else None
            if not self.g['_np_private_offer_usable'](row):return self.invalid(base,code,'Private offer expired, already used, or unavailable')
            scope=self.eligible(row,self.trader(data),data)
            pct=discount(self.g['_np_offer_discount_percent'](row,code));amount=round(float(base)*pct/100,2);fee=round(float(base)-amount,2)
            return dict(valid=True,code=code,message=f'{pct:g}% private discount applied',original_fee=float(base),discount_percent=pct,
                discount_amount=amount,final_fee=fee,fee=fee,commission_percent=0,commission_amount=0,
                affiliate_owner='NairaPips Private Offer',campaign_type='private_recovery_offer',offer_id=row['id'],expires_at=row.get('expires_at'),recovery_scope=scope,source=row)
        except ValueError as exc:return self.invalid(base,code,str(exc))
        except Exception:return self.invalid(base,code,'Private offer could not be verified. Sign in and retry.')
    @staticmethod
    def invalid(base,code,message):return dict(valid=False,code=code,message=message,original_fee=float(base),final_fee=float(base),fee=float(base),discount_percent=0,discount_amount=0)


def install(app,g):
    service=Retention(g);app.config['NP_PRIVATE_RETENTION']=service
    g['_np_find_offer_trader']=service.member
    old_expired=g['_np_private_offer_expired'];old_discount=g['_np_offer_discount_percent']
    g['_np_private_offer_expired']=lambda row: expired(row) if not row.get('message_only') else old_expired(row)
    def percentage(row,code=''):
        row=g['_np_offer_merge_meta'](row)
        for key in ('discount_percent','discount','discount_pct'):
            if row.get(key) is not None:return discount(row[key])
        return old_discount(row,code)
    g['_np_offer_discount_percent']=percentage
    g['_np_private_offer_quote']=service.quote
    def exact_match(row,trader_id='',email='',phone='',account_reference=''):
        return matches(row,{'id':trader_id,'email':email})
    g['_np_private_offer_matches']=exact_match

    @app.get('/admin/private_offer_members')
    def private_offer_members():
        _,error=g['_require_admin']()
        if error is not None:return error
        try:
            after=request.args.get('after','')
            if after:after=str(uuid.UUID(after))
            params={'select':'id,name,email,phone,account_reference','order':'id.asc','limit':'250'}
            if after:params['id']='gt.'+after
            rows=service.read('traders',params)
            r=jsonify(success=True,members=rows,next_after=str(rows[-1]['id']) if len(rows)==250 else None)
            r.headers['Cache-Control']='no-store, private';return r
        except Exception:return jsonify(success=False,error='Registered members could not be loaded. Retry.'),503

    @app.get('/admin/private_offer_accounts')
    def private_offer_accounts():
        _,error=g['_require_admin']()
        if error is not None:return error
        try:
            trader=service.member(request.args.get('trader_id',''))
            if not trader:return jsonify(success=False,error='Registered trader required'),400
            rows=service.read('trader_accounts',{'select':'id,breached_at','trader_id':'eq.'+str(trader['id']),'order':'created_at.desc','limit':'1001'})
            if len(rows)>1000:raise ValueError('Account history requires review')
            scopes=[]
            for row in rows:
                if not row.get('breached_at'):continue
                try:scopes.append(service.account_scope(trader,row['id']))
                except ValueError:continue
            response=jsonify(success=True,accounts=scopes);response.headers['Cache-Control']='no-store, private';return response
        except Exception:return jsonify(success=False,error='Breached accounts could not be verified'),503

    # Read all relevant history instead of only the latest 100 offers globally.
    for endpoint in ('private_offers_for_trader','trader_targeted_offers'):
        if endpoint not in app.view_functions:continue
        def reader(kind):
            def visible_offers():
                if request.method=='OPTIONS':return jsonify(success=True)
                try:
                    data=dict(request.args)
                    admin=g.get('_request_admin_auth',lambda:None)() if kind=='private_offers_for_trader' else None
                    trader=(g['_np_find_offer_trader'](data.get('trader_id',''),data.get('email','')) if admin else service.trader(data)) or {}
                    if not trader.get('id'):return jsonify(success=False,error='Registered trader required'),400
                    account=None;visible=[]
                    for row in service.offers():
                        if not g['_np_offer_bool'](row.get('delivery_dashboard'),g['_np_offer_bool'](row.get('show_on_dashboard'),False)):continue
                        if not g['_np_private_offer_usable'](row):continue
                        segment=str(row.get('audience_segment') or 'single').lower()
                        if segment=='single':
                            if not matches(row,trader):continue
                        else:
                            if kind!='trader_targeted_offers':continue
                            if account is None:
                                try:account=g['_get_active_account'](trader['id']) or {}
                                except Exception:account={}
                            if not g['_np_notice_audience_matches'](row,trader,account):continue
                        safe=g['_trader_safe_offer_row'](row)
                        if not row.get('message_only'):
                            try:safe['recovery_scope']=service.scope(row,trader)
                            except ValueError:continue
                        visible.append(safe)
                    visible.sort(key=lambda row:str(row.get('created_at') or ''),reverse=True)
                    r=jsonify(visible);r.headers['Cache-Control']='no-store, private';return r
                except ValueError as exc:return jsonify(success=False,error=str(exc)),401
                except Exception:return jsonify(success=False,error='Private offers could not be loaded'),503
            return visible_offers
        app.view_functions[endpoint]=reader(endpoint)

    original_create=app.view_functions['create_private_offer']
    @wraps(original_create)
    def create_offer():
        if request.method=='OPTIONS':return original_create()
        _,error=g['_require_admin']()
        if error is not None:return error
        try:
            d=dict(request.get_json(silent=True) or {})
            if not g['_np_offer_bool'](d.get('message_only'),False):
                d['discount_percent']=discount(d.get('discount_percent'))
                end=expiry(d.get('expires_at'))
                if end is None or end<=datetime.now(timezone.utc):raise ValueError('Choose a valid future expiry')
                d['expires_at']=end.isoformat()
                if str(d.get('audience_segment') or 'single')!='single':raise ValueError('Discount offers must target one registered trader')
                target=g['_np_find_offer_trader'](str(d.get('target_trader_id') or ''),str(d.get('target_email') or ''))
                if not target or (d.get('target_trader_id') and str(target['id'])!=str(d['target_trader_id'])):raise ValueError('Choose a registered member')
                d.update(target_trader_id=str(target['id']),target_email=target.get('email') or '',target_name=target.get('name') or '')
                # Older installed admin pages identify only the member. Their
                # exact current account may supply the scope only if verified
                # breached; never turn this compatibility path into a broad offer.
                source=str(d.get('source_account_id') or (d.get('recovery_scope') or {}).get('source_account_id') or target.get('current_account_id') or '')
                if not source:raise ValueError('Choose the exact breached account for this recovery discount')
                d['recovery_scope']=service.account_scope(target,source)
            request._cached_json=(d,d)
            return original_create()
        except ValueError as exc:return jsonify(success=False,error=str(exc)),400
        except Exception:return jsonify(success=False,error='Offer could not be saved. Retry.'),503
    app.view_functions['create_private_offer']=create_offer

    # Claiming never extends expiry or consumes the one discounted purchase.
    for endpoint in ('claim_private_offer','mark_private_offer_read'):
        def wrap(kind):
            def protected():
                if request.method=='OPTIONS':return jsonify(success=True)
                try:
                    d=dict(request.get_json(silent=True) or {});t=service.trader(d)
                    identity=str(d.get('offer_id') or d.get('id') or '')
                    rows=service.read('announcements',{'select':'*','id':'eq.'+identity,'type':'eq.private_offer','limit':'1'}) if identity else list(service.offers())
                    rows=[g['_np_offer_merge_meta'](r) for r in rows]
                    rows.sort(key=lambda r:str(r.get('created_at') or ''),reverse=True)
                    chosen=next((r for r in rows if matches(r,t) and g['_np_private_offer_usable'](r)),None)
                    if not chosen:return jsonify(success=False,error='Offer expired, already used, or not assigned to you'),404
                    scope=service.scope(chosen,t) if not chosen.get('message_only') else None
                    stamp=datetime.now(timezone.utc).isoformat()
                    audit={key:stamp for key in ('read_at','claimed_at') if key in chosen and (key=='read_at' or kind=='claim_private_offer')}
                    if audit:
                        try:service.patch(chosen['id'],str(chosen.get('status') or 'active'),audit)
                        except Exception:pass
                    if kind=='mark_private_offer_read':return jsonify(success=True,data={'read':True,'offer_id':chosen['id']})
                    code=g['_aff_code'](chosen.get('offer_code') or 'PHASEHELP')
                    return jsonify(success=True,data=dict(claimed=True,offer_id=chosen['id'],offer_code=code,promo_code=code,
                        discount_percent=g['_np_offer_discount_percent'](chosen,code),expires_at=chosen.get('expires_at') or chosen.get('offer_expires_at'),recovery_scope=scope,redirect='plans'))
                except ValueError as exc:return jsonify(success=False,error=str(exc)),401
                except Exception:return jsonify(success=False,error='Offer could not be verified. Retry.'),503
            return protected
        app.view_functions[endpoint]=wrap(endpoint)

    core=app.view_functions['create_purchase']
    @wraps(core)
    def purchase():
        if request.method=='OPTIONS':return core()
        d=request.get_json(silent=True) or {}
        code=g['_aff_code'](next((d.get(k) for k in ('affiliate_code','referral_code','ref_code','partner_code','promo_code','code') if d.get(k)),''))
        if not code:return core()
        try:
            offer,foreign=service.find(d,code)
            if not offer:return (jsonify(success=False,error='This private offer belongs to another trader'),403) if foreign else core()
            if not g['_np_private_offer_usable'](offer):return jsonify(success=False,error='Private offer expired or already used'),409
            trader=service.trader(d)
            try:service.eligible(offer,trader,d)
            except ValueError as exc:return jsonify(success=False,error=str(exc)),409
            token='offer_reserved_'+uuid.uuid4().hex;started=datetime.now(timezone.utc).isoformat()
            rows=service.patch(offer['id'],str(offer.get('status') or 'active'),{'status':token})
            if len(rows)!=1:return jsonify(success=False,error='This private offer is already being used'),409
        except Exception:return jsonify(success=False,error='Offer availability could not be verified. Retry.'),503
        trusted=dict(d,trader_id=str(trader['id']),email=trader.get('email') or '',trader_name=trader.get('name') or '',phone=trader.get('phone') or '')
        request._cached_json=(trusted,trusted)
        context=service.reservation.set(dict(trader=str(trader['id']),code=code,offer=offer))
        try:
            response=app.make_response(core());body=response.get_json(silent=True) or {}
            if response.status_code<300 and body.get('success') is not False:
                # Reserved rows remain blocked even if this final cosmetic status update fails.
                try:service.patch(offer['id'],token,{'status':'used','show_on_dashboard':False})
                except Exception:pass
            else:
                # Only release a failed validation after proving no purchase was created.
                try:
                    history=service.read('challenge_purchases',{'select':'id','trader_id':'eq.'+str(trader['id']),'promo_code':'eq.'+code,'created_at':'gte.'+started,'limit':'1'})
                    if not history:service.patch(offer['id'],token,{'status':offer.get('status') or 'active'})
                except Exception:pass
            return response
        finally:service.reservation.reset(context)
    app.view_functions['create_purchase']=purchase
