"""One read-only trader catalogue plus owner-operated gates for NEW purchases.

Never copies draft terms into issued plans/contracts, and never releases a new
package runtime by treating a visibility checkbox as financial authorisation.
"""
import copy
import time
from datetime import datetime,timezone
from pathlib import Path
from flask import jsonify,request,make_response
from production_controls import ControlsStore,ControlsUnavailable,Conflict
from package_plan_matrix import availability,SCHEMA
from progress_plan_bridge import is_managed, plan_row, commercial_plan, readiness, catalog_identity, offer_version

KEY='catalog_purchase_switches_v1'
RELEASE='unified-live-catalogue-2026-11-05'
PLAN_FIELDS=('id','name','description','status','fee','account_size','phase1_target','phase2_target',
             'max_drawdown','drawdown_type','payout_split','reset_fee','funded_reset_fee',
             'payout_cycles','payout_frequency','withdrawal_rules','reset_stage_prices','challenge_journey','product_category','base_fee','cycle_options','default_cycles','journey_source','managed_progress','offer_version','daily_drawdown','second_life_enabled','lives_total','public_badge')


def defaults():return dict(revision=0,products={},updated_at=None,updated_by=None)


def validate(doc):
    if not isinstance(doc,dict) or set(doc)!=set(defaults()):raise ValueError('Invalid catalogue control document')
    if type(doc['revision']) is not int or doc['revision']<0:raise ValueError('Invalid revision')
    if not isinstance(doc['products'],dict) or len(doc['products'])>1500:raise ValueError('Too many product switches')
    for identity,value in doc['products'].items():
        if not isinstance(identity,str) or not 1<=len(identity)<=160 or not isinstance(value,dict) or set(value)!={'purchases_open'} or type(value['purchases_open']) is not bool:
            raise ValueError('Each product requires an on/off purchase switch')
    return copy.deepcopy(doc)


class PublicationStore(ControlsStore):
    def __init__(self,http,url,key):
        def checked(doc):
            try:return validate(doc)
            except ValueError as exc:raise ControlsUnavailable('Saved purchase controls are invalid') from exc
        super().__init__(http,url,key,KEY,defaults,checked)
        self.url=url.rstrip('/')+'/rest/v1/package_studio_private_settings'

    def save_switches(self,body,actor,products):
        if not isinstance(body,dict) or set(body)!={'revision','products'} or type(body['revision']) is not int:
            raise ValueError('Supply the loaded revision and product switches')
        doc=validate(dict(revision=body['revision'],products=body['products'],updated_at=None,updated_by=None))
        old,raw=self.read(fresh=True)
        if old['revision']!=body['revision']:raise Conflict('Another admin changed purchase switches. Reload before saving.')
        for identity,flags in doc['products'].items():
            if identity not in products:raise Conflict('The product list changed. Reload before saving.')
            if flags['purchases_open'] and not products[identity]['checkout_supported']:
                raise Conflict('Purchases remain locked for '+products[identity]['name']+': '+products[identity]['release_reason'])
        doc.update(revision=old['revision']+1,updated_at=datetime.now(timezone.utc).isoformat(),updated_by=str(actor))
        return self.persist(doc,raw,actor)


class SharedCatalog:
    def __init__(self,http,url,key,matrix,controls,custom,app):
        self.http=http;self.base=url.rstrip('/')+'/rest/v1/'
        self.headers={'apikey':key,'Authorization':'Bearer '+key}
        self.matrix=matrix;self.controls=controls;self.custom=custom;self.app=app
        self.plans_cache=None;self.plans_cached_at=0

    def plans(self,fresh=False):
        if not fresh and self.plans_cache is not None and time.monotonic()-self.plans_cached_at<3:return copy.deepcopy(self.plans_cache)
        try:
            reader=self.app.config.get('NP_SHARED_CATALOG_PLAN_READER')
            if callable(reader):rows=reader()
            else:
                r=self.http.get(self.base+'challenge_plans',headers=self.headers,
                    params={'select':'*','order':'account_size.asc,id.asc','limit':'1001'},timeout=(2,5))
                r.raise_for_status();rows=r.json()
            if not isinstance(rows,list) or len(rows)>1000:raise ValueError('Incomplete plan list')
            self.plans_cache=copy.deepcopy(rows);self.plans_cached_at=time.monotonic();return rows
        except Exception as exc:raise ControlsUnavailable('Current challenge plans could not be loaded') from exc

    def progress_readiness(self,plan,package):
        if self.app.config.get('NP_PROGRESS_PLAN_BRIDGE') is not None:
            return readiness(plan,package)
        probe=self.app.config.get('NP_PROGRESS_PACKAGE_READINESS')
        checkout=self.app.config.get('NP_PROGRESS_PACKAGE_CHECKOUT')
        if not callable(probe) or not callable(checkout):return False,'Continuing-payout checkout and account enforcement are not connected yet'
        try:
            result=probe(copy.deepcopy(plan),copy.deepcopy(package))
            ok=isinstance(result,dict) and result.get('ready') is True and result.get('schema')==SCHEMA
            return ok,'' if ok else 'The selected package has not passed its production release checks'
        except Exception:return False,'Package release checks could not be verified'

    def products(self,fresh=False):
        result={}
        for p in self.plans(fresh=fresh):
            if is_managed(p):continue
            identity='plan:'+str(p['id']);category='premium' if str(p.get('product_category') or '').lower()=='premium' else 'standard'
            result[identity]=dict(id=identity,family=category,name=str(p.get('name') or 'Challenge'),
                visible=str(p.get('status') or '').lower()=='active',
                checkout_supported=((not __import__('withdrawal_controls').needs_broker_update(p) or __import__('withdrawal_controls').ready()) and (str(p.get('drawdown_type') or 'STATIC').upper()!='TRAILING' or __import__('equity_drawdown').runtime_ready()) and (__import__('plan_risk_controls').daily_percent(p)<=0 or __import__('plan_risk_controls').daily_runtime_ready())),
                release_reason=('Custom or disabled profit caps require the verified Operations engine update' if __import__('withdrawal_controls').needs_broker_update(p) and not __import__('withdrawal_controls').ready() else 'Daily drawdown requires four updated DD shards reporting' if __import__('plan_risk_controls').daily_percent(p)>0 and not __import__('plan_risk_controls').daily_runtime_ready() else 'Equity Trailing requires the updated broker-side DD worker before purchases can open' if str(p.get('drawdown_type') or 'STATIC').upper()=='TRAILING' else ''),
                plan_id=str(p['id']),plan={**{k:p.get(k) for k in PLAN_FIELDS},'reset_stage_prices':(__import__('plan_risk_controls').frozen_terms(p) or {}).get('resets',{}),'withdrawal_rules':__import__('withdrawal_controls').read(p)})
        matrix,_=self.matrix.read(fresh=fresh)
        for p in matrix['config']['plans']:
            for x in p['packages']:
                identity='progress:'+p['id']+':'+x['key'];ready,reason=self.progress_readiness(p,x)
                decision=availability(p,x)
                if not decision['available']:
                    ready=False;reason='; '.join(decision['reasons'])
                result[identity]=dict(id=identity,family='progress',name=p['name']+' · '+x['name'],
                    visible=bool(p['enabled'] and x['enabled']),checkout_supported=ready,release_reason=reason,
                    plan_id=plan_row(p,x)['id'],plan=commercial_plan(plan_row(p,x,True)),
                    proposal=True,package=dict(name=x['name'],allocation_name=p['name'],capital=p['capital'],
                        price=x['price'],drawdown=x['drawdown'],profit_share=x['profit_share'],profit_cap=x['profit_cap'],
                        phase1_target=x['phase1_target'],phase2_target=x['phase2_target'],daily_drawdown=x['daily_drawdown'],
                        payout_mode=x['payout_mode'],drawdown_type=x.get('drawdown_type','STATIC')),_plan=p,_package=x)
        if self.custom is not None:
            config,_=self.custom.read(fresh=fresh)
            try:ready=bool(config.get('enabled') and self.custom.readiness and self.custom.readiness().get('ready'))
            except Exception:ready=False
            result['custom']=dict(id='custom',family='custom',name='Other custom-package builder',visible=bool(config.get('enabled')),
                checkout_supported=ready,release_reason='' if ready else 'The custom-package release checks are incomplete')
        return result


def register_catalog_publication(app,store,catalog,require_admin,authenticated_trader,audit):
    def owner():
        admin,error=require_admin()
        if error is not None:return None,error
        if str((admin or {}).get('role') or '').lower() not in ('owner','super_admin','superadmin'):
            return None,(jsonify(success=False,error='Owner or super admin access required'),403)
        return admin,None

    def state(products=None,fresh=False):
        products=products if products is not None else catalog.products(fresh=fresh)
        doc,_=store.read(fresh=fresh);flags,_=catalog.controls.read(fresh=fresh)
        sales=flags['features']['challenge_sales']
        for identity,p in products.items():
            default_open=p['family'] in ('standard','premium','custom')
            p['purchase_switch']=doc['products'].get(identity,{'purchases_open':default_open})['purchases_open']
            p['purchases_open']=bool(p['visible'] and p['checkout_supported'] and p['purchase_switch'] and sales)
        return doc,products,sales

    def response(payload,status=200):
        r=jsonify(payload);r.status_code=status
        r.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','X-NairaPips-Catalog-Release':RELEASE})
        return r

    @app.get('/trader/challenge_catalog')
    def trader_challenge_catalog():
        try:
            doc,products,sales=state(fresh=True)
            public=[]
            for p in products.values():
                if not p['visible'] or p['family']=='custom':continue
                row={k:p[k] for k in ('id','family','name','purchases_open')}
                if p['family']=='progress':row.update(package=p['package'],plan=p['plan'],proposal=not p['purchases_open'])
                else:row['plan']=p['plan']
                public.append(row)
            return response(dict(success=True,products=public,global_sales_open=sales,
                controls_revision=doc['revision'],refresh_seconds=30,generated_at=datetime.now(timezone.utc).isoformat()))
        except ControlsUnavailable:return response(dict(success=False,error='Current prices and availability could not be verified. Please retry.'),503)

    def published_rows():
        _,products,_=state(fresh=True)
        return [dict(p['plan'],status='active',purchases_open=p['purchases_open'],
                    catalog_product_id=p['id'])
                for p in products.values() if p['visible'] and p['family']!='custom']

    def published_snapshot():
        try:
            rows=published_rows()
            return response(dict(success=True,plans=rows,count=len(rows),
                published_at=datetime.now(timezone.utc).isoformat()))
        except ControlsUnavailable:
            return response(dict(success=False,error='Current offer terms could not be verified'),503)

    def published_plan_list():
        try:return response(published_rows())
        except ControlsUnavailable:return response(dict(success=False,error='Current offer terms could not be verified'),503)

    # Landing, older clients and the new trader UI share one publication source.
    # This replaces HTTP projections, never the purchase-snapshot runtime reader.
    for path,endpoint,view in [('/public_plan_snapshot','public_plan_snapshot',published_snapshot),
                              ('/challenge_plans','challenge_plans',published_plan_list),
                              ('/plans','plans_alias',published_plan_list)]:
        if endpoint in app.view_functions:app.view_functions[endpoint]=view
        else:app.add_url_rule(path,endpoint,view,methods=['GET'])

    @app.get('/admin/catalog_publication/view')
    def catalog_publication_view():
        _,error=owner()
        if error is not None:return error
        r=make_response((Path(__file__).with_name('package_studio_assets')/'catalog_publication.html').read_text())
        r.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','X-Frame-Options':'DENY',
            'Content-Security-Policy':"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"})
        return r

    @app.route('/admin/catalog_publication',methods=['GET','POST'])
    def catalog_publication_settings():
        admin,error=owner()
        if error is not None:return error
        try:
            products=catalog.products(fresh=True)
            if request.method=='POST':
                body=request.get_json(silent=True)
                # Validate/CAS before staging; private switches stay authoritative
                # if a physical row write or a later CAS fails.
                if not isinstance(body,dict) or set(body)!={'revision','products'}:raise ValueError('Supply revision and product switches')
                validate(dict(body,updated_at=None,updated_by=None))
                current,_=store.read(fresh=True)
                if body['revision']!=current['revision']:raise Conflict('Reload the latest purchase switches before saving')
                for pid,flags in body['products'].items():
                    if pid not in products:raise Conflict('The product list changed; reload')
                    if flags['purchases_open'] and not products[pid]['checkout_supported']:raise Conflict('Purchases remain locked: '+products[pid]['release_reason'])
                bridge=app.config.get('NP_PROGRESS_PLAN_BRIDGE')
                if bridge is not None:bridge.sync(products,body['products'])
                store.save_switches(body,admin.get('id') or 'owner',products)
                audit('catalogue','purchase_switches_saved','New purchases only',admin,KEY)
            doc,products,sales=state(products)
            rows=[{k:v for k,v in p.items() if not k.startswith('_') and k not in ('plan','package')} for p in products.values()]
            return response(dict(success=True,data=doc,products=rows,global_sales_open=sales,refresh_seconds=30))
        except Conflict as exc:return response(dict(success=False,error=str(exc)),409)
        except ValueError as exc:return response(dict(success=False,error=str(exc)),400)
        except ControlsUnavailable as exc:return response(dict(success=False,error=str(exc)),503)

    def gate(identity):
        # Financial submissions bypass worker caches. Existing fixed-plan
        # checkout must not depend on loading an unrelated future draft.
        if identity.startswith('plan:') and app.config.get('NP_PROGRESS_PLAN_BRIDGE') is not None:
            bridge=app.config['NP_PROGRESS_PLAN_BRIDGE']
            row=bridge.find(identity[5:])
            if row is not None and is_managed(row):identity=catalog_identity(row)
        doc,_=store.read(fresh=True);flags,_=catalog.controls.read(fresh=True)
        default_open=identity.startswith('plan:') or identity=='custom'
        opened=doc['products'].get(identity,{'purchases_open':default_open})['purchases_open']
        opened=opened and flags['features']['challenge_sales']
        if opened and identity.startswith('plan:'):
            try:
                r=catalog.http.get(catalog.base+'challenge_plans',headers=catalog.headers,
                    params={'id':'eq.'+identity[5:],'select':'id,status','limit':'1'},timeout=(2,4))
                r.raise_for_status();rows=r.json()
                opened=isinstance(rows,list) and len(rows)==1 and str(rows[0].get('status') or '').lower()=='active'
            except Exception as exc:raise ControlsUnavailable('Current plan availability could not be verified') from exc
        elif opened and identity=='custom':
            config,_=catalog.custom.read(fresh=True) if catalog.custom is not None else ({},None)
            opened=bool(config.get('enabled'))
        elif opened:
            _,products,_=state(fresh=True)
            opened=bool(products.get(identity,{}).get('purchases_open'))
            bridge=app.config.get('NP_PROGRESS_PLAN_BRIDGE')
            if opened and bridge is not None:
                product=products[identity]
                physical=bridge.find(product['plan_id'])
                opened=bool(physical and bridge.verified_identity(physical)==identity)
        if not opened:
            return response(dict(success=False,code='PRODUCT_PURCHASES_CLOSED',
                error='This package is not open for new purchases. Refresh the catalogue.',operation_started=False),403)
        return None

    @app.before_request
    def enforce_catalog_purchase_switches():
        path=request.path.rstrip('/')
        if request.method=='OPTIONS':return None
        legacy=path=='/create_challenge_purchase' and request.method=='POST'
        quote=path=='/quote_challenge_price' and request.method in ('GET','POST')
        custom=path in ('/trader/package_quote','/trader/package_checkout') and request.method=='POST'
        if not (legacy or quote or custom):return None
        body=request.get_json(silent=True) or {} if request.method=='POST' else request.args
        if not isinstance(body,dict) and request.method=='POST':return None
        # Accepted custom checkout retries retain their own transactional replay
        # semantics. Its release gate must run only if creating a new order.
        if path=='/trader/package_checkout':return None
        identity='custom' if custom else 'plan:'+str(body.get('plan_id') or '')
        if not custom and not body.get('plan_id'):return None
        try:
            denied=gate(identity)
            if denied is not None:return denied
            if legacy:
                bridge=app.config.get("NP_PROGRESS_PLAN_BRIDGE")
                row=bridge.find(body["plan_id"]) if bridge is not None else None
                if row is not None and is_managed(row) and (body.get("plan_offer_version")!=offer_version(row) or (request.environ.get("np_verified_checkout_plan_v221") is not None and offer_version(request.environ["np_verified_checkout_plan_v221"])!=offer_version(row))):
                    return response(dict(success=False,code="PACKAGE_OFFER_CHANGED",error="Package terms changed. Close checkout and select the current package again.",operation_started=False),409)
            return None
        except ControlsUnavailable:return response(dict(success=False,error='Purchase availability could not be verified.',operation_started=False),503)

    # Existing custom checkout uses this only after confirming that the quote
    # has not already been consumed, so recoverable retries are not blocked.
    app.config['NP_CUSTOM_CATALOG_GATE']=lambda:gate('custom')

    @app.post('/trader/catalog/checkout')
    def progress_catalog_checkout():
        tid,error=authenticated_trader(None)
        if error:return response(dict(success=False,error='Trader authentication is required'),401)
        body=request.get_json(silent=True)
        if not isinstance(body,dict) or not isinstance(body.get('product_id'),str):return response(dict(success=False,error='Select a current package'),400)
        try:
            denied=gate(body['product_id'])
            if denied is not None:return denied
            _,products,_=state(fresh=True);product=products[body['product_id']]
            if product['family']!='progress':return response(dict(success=False,error='Use the existing plan checkout'),400)
            # A future verified connector owns signed quotes, frozen contracts,
            # idempotency, payment validation and account-runtime bindings.
            if app.config.get('NP_PROGRESS_PLAN_BRIDGE') is not None:
                return response(dict(success=True,plan=product['plan'],next_step='submit_payment_proof',purchase_endpoint='/create_challenge_purchase'))
            return app.config['NP_PROGRESS_PACKAGE_CHECKOUT'](tid,copy.deepcopy(product['_plan']),copy.deepcopy(product['_package']),body)
        except ControlsUnavailable:return response(dict(success=False,error='Package availability could not be verified.'),503)
