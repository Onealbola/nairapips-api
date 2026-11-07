"""Owner-editable future offers shared by landing and trader catalogues.

Purchase switches and immutable issued terms remain separate authorities.
"""
import copy
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from flask import jsonify, request, make_response
from production_controls import ControlsStore, ControlsUnavailable, Conflict
from package_studio import number

KEY='package_studio_plan_matrix_v1'
SCHEMA='np-progress-plan-matrix-v1'
KINDS=('basic','room','share','both')
MODELS=('two_phase','one_phase','instant')
ROUTES={'two_phase':('phase1','phase2','funded'),'one_phase':('phase1','funded'),'instant':('funded',)}

def model_of(plan):
    return plan.get('model','two_phase')

def route_for(plan):
    return ROUTES[model_of(plan)]
ROOT=Path(__file__).with_name('package_studio_assets')
RELEASE='editable-static-equity-trailing-2026-11-07'


def make_plan(identity,capital,base,model=None):
    variants=[]
    for key,name,multiple,dd,share in (
        ('basic','Basic',1,5,50),('room','More Room',1.4,10,50),
        ('share','More Share',1.2,5,60),('both','Both benefits',1.6,10,60)):
        variants.append(dict(key=key,name=name,enabled=True,price=math.ceil(base*multiple/50)*50,
            drawdown=dd,drawdown_type='STATIC',profit_share=share,profit_cap=15,daily_drawdown=0,
            phase1_target=10,phase2_target=8,payout_mode='continuing'))
    plan=dict(id=identity,name='NGN '+format(capital,','),capital=capital,enabled=True,
        limits=dict(max_drawdown=10,max_profit_share=60,max_profit_cap=15),packages=variants)
    if model is not None:
        if model not in MODELS:raise ValueError('Unknown account model')
        plan['model']=model
        for x in variants:
            if model!='two_phase':x['phase2_target']=0
            if model=='instant':x['phase1_target']=0
    return plan


def defaults():
    return dict(revision=0,mode='draft',checkout_enabled=False,updated_at=None,updated_by=None,
        config=dict(schema=SCHEMA,plans=[make_plan('ngn-'+str(cap),cap,fee)
                    for cap,fee in [(500000,10000),(1000000,20000),(2000000,40000)]]))


def text(value,limit):
    if not isinstance(value,str) or not value.strip() or len(value)>limit or any(ord(c)<32 for c in value):
        raise ValueError('Enter a valid name')


def boolean(value):
    if type(value) is not bool:raise ValueError('Availability switches must be true or false')


def fields(value,expected):
    if not isinstance(value,dict) or set(value)!=set(expected):
        raise ValueError('Missing or unsupported package fields')


def availability(plan,package):
    reasons=[]
    if not plan['enabled']:reasons.append('Account size switched off')
    if not package['enabled']:reasons.append('Package switched off')
    for key,limit,label in [('drawdown','max_drawdown','Drawdown'),('profit_share','max_profit_share','Trader share'),('profit_cap','max_profit_cap','Profit cap')]:
        if package[key]>plan['limits'][limit]:reasons.append(label+' exceeds this account size’s draft limit')
    return dict(available=not reasons,reasons=reasons)


def validate_config(config):
    fields(config,('schema','plans'))
    if config['schema']!=SCHEMA:raise ValueError('Unsupported plan-matrix version')
    if not isinstance(config['plans'],list) or not 1<=len(config['plans'])<=30:
        raise ValueError('Keep between 1 and 30 account sizes')
    seen=set();capitals=set()
    for plan in config['plans']:
        fields(plan,('id','name','capital','enabled','limits','packages')+ (('model',) if 'model' in plan else ()))
        if model_of(plan) not in MODELS:raise ValueError('Choose 1-Phase, 2-Phase or Instant Funded')
        if not isinstance(plan['id'],str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}',plan['id']) or plan['id'] in seen:
            raise ValueError('Duplicate or invalid account-size identity')
        seen.add(plan['id']);text(plan['name'],80);boolean(plan['enabled'])
        number(plan['capital'],1000,1e9)
        if type(plan['capital']) is not int or (model_of(plan),plan['capital']) in capitals:
            raise ValueError('Account sizes must be unique whole amounts within each model')
        capitals.add((model_of(plan),plan['capital']))
        fields(plan['limits'],('max_drawdown','max_profit_share','max_profit_cap'))
        for key,low,high in [('max_drawdown',.1,30),('max_profit_share',1,100),('max_profit_cap',.1,30)]:
            number(plan['limits'][key],low,high)
        if not isinstance(plan['packages'],list) or len(plan['packages'])!=4:
            raise ValueError('Each account size needs all four package configurations')
        kinds=set()
        for package in plan['packages']:
            fields(package,('key','name','enabled','price','drawdown','profit_share','profit_cap',
                            'daily_drawdown','phase1_target','phase2_target','payout_mode') + (('drawdown_type',) if 'drawdown_type' in package else ()))
            from equity_drawdown import mode
            if 'drawdown_type' in package and package['drawdown_type'] != mode(package['drawdown_type']):
                raise ValueError('Choose STATIC or TRAILING')
            if package['key'] not in KINDS or package['key'] in kinds:
                raise ValueError('Each account size needs Basic, More Room, More Share and Both exactly once')
            kinds.add(package['key']);text(package['name'],60);boolean(package['enabled'])
            number(package['price'],50,1e8)
            if round(package['price']*100,6)!=round(package['price']*100):
                raise ValueError('Prices can have at most two decimal places')
            for key,low,high in [('drawdown',.1,30),('profit_share',1,100),('profit_cap',.1,30)]:
                number(package[key],low,high)
            if type(package['daily_drawdown']) not in (int,float) or package['daily_drawdown'] not in (0,10):
                raise ValueError('Choose no separate daily rule or the 10% daily rule')
            if package['drawdown']>20 and package['daily_drawdown']!=10:
                raise ValueError('Drawdown above 20% requires the 10% daily rule')
            for key in ('phase1_target','phase2_target'):
                number(package[key],0,100)
            targets=(package['phase1_target'],package['phase2_target'])
            if model_of(plan)=='two_phase' and targets not in ((8,5),(10,8)):
                raise ValueError('Choose either 8% / 5% or 10% / 8% as a complete target pair')
            if model_of(plan)=='one_phase' and not (1<=targets[0]<=100 and targets[1]==0):
                raise ValueError('A 1-Phase package needs one positive target and no Phase 2 target')
            if model_of(plan)=='instant' and targets!=(0,0):
                raise ValueError('Instant Funded packages have no evaluation targets')
            if package['payout_mode']!='continuing':
                raise ValueError('This proposed format uses continuing eligible payouts')
    return copy.deepcopy(config)


def validate_document(doc):
    fields(doc,defaults())
    if type(doc['revision']) is not int or doc['revision']<0:raise ValueError('Invalid draft revision')
    if doc['mode']!='draft' or doc['checkout_enabled'] is not False:
        raise ValueError('This editor cannot open trader purchases')
    validate_config(doc['config'])
    for key in ('updated_at','updated_by'):
        if doc[key] is not None and (not isinstance(doc[key],str) or len(doc[key])>200):
            raise ValueError('Invalid saved metadata')
    return copy.deepcopy(doc)


class PlanMatrixStore(ControlsStore):
    def __init__(self,transport,url,key):
        def checked(doc):
            try:return validate_document(doc)
            except ValueError as exc:raise ControlsUnavailable('The saved plan matrix is invalid') from exc
        super().__init__(transport,url,key,KEY,defaults,checked)
        self.url=url.rstrip('/')+'/rest/v1/package_studio_private_settings'

    def save_draft(self,body,actor):
        fields(body,('revision','config'))
        if type(body['revision']) is not int:raise ValueError('Supply the loaded server revision')
        config=validate_config(body['config']);old,raw=self.read(fresh=True)
        if body['revision']!=old['revision']:
            raise Conflict('Another admin saved a newer version. Export your edits, then reload before saving.')
        doc=defaults();doc.update(revision=old['revision']+1,config=config,
            updated_at=datetime.now(timezone.utc).isoformat(),updated_by=str(actor))
        return self.persist(doc,raw,actor)


def register_plan_matrix(app,store,require_admin,audit):
    def owner():
        admin,error=require_admin()
        if error is not None:return None,error
        if str((admin or {}).get('role') or '').lower() not in ('owner','super_admin','superadmin'):
            return None,(jsonify(success=False,error='Owner or super admin access required'),403)
        return admin,None

    def secure(r):
        r.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff',
            'X-Frame-Options':'DENY','Referrer-Policy':'no-referrer',
            'X-NairaPips-Studio-Release':RELEASE,
            'Content-Security-Policy':"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"})
        return r

    @app.get('/admin/package_studio/plan_matrix/view')
    def plan_matrix_view():
        _,error=owner()
        if error is not None:return error
        html=(ROOT/'plan_matrix.html').read_text()
        component=(ROOT.parent/'website-downloads'/'account-plan-selector.js').read_text()
        return secure(make_response(html.replace('</head>','<script>'+component+'</script></head>',1)))

    @app.route('/admin/package_studio/plan_matrix',methods=['GET','POST'])
    def plan_matrix_settings():
        admin,error=owner()
        if error is not None:return error
        try:
            connection_warning=''
            if request.method=='POST':
                if request.content_length and request.content_length>200000:raise ValueError('Draft is too large')
                doc=store.save_draft(request.get_json(silent=True),admin.get('id') or 'owner')
                bridge=app.config.get('NP_PROGRESS_PLAN_BRIDGE')
                if bridge is not None:
                    switches,_=bridge.publication.read(fresh=True)
                    catalog=app.config.get('NP_PROGRESS_SHARED_CATALOG')
                    if catalog is not None:
                        try:bridge.sync(catalog.products(fresh=True),switches['products'])
                        except ControlsUnavailable:
                            connection_warning='Offer edits are saved and visible. Production plan synchronization needs the database repair; changed offers cannot be purchased until synchronized.'
                audit('package_studio','plan_matrix_draft_saved',str({'revision':doc['revision']}),admin,KEY)
            else:doc,_=store.read(fresh=True)
            decisions={p['id']:{x['key']:availability(p,x) for x in p['packages']} for p in doc['config']['plans']}
            return secure(jsonify(success=True,data=doc,availability=decisions,
                checkout_enabled=False,connection_warning=connection_warning,purchase_controls_connected=app.config.get('NP_PROGRESS_PLAN_BRIDGE') is not None,production_effect='future_offers_only' if app.config.get('NP_PROGRESS_PLAN_BRIDGE') is not None else 'none',release=RELEASE))
        except Conflict as exc:return secure(jsonify(success=False,error=str(exc))),409
        except ValueError as exc:return secure(jsonify(success=False,error=str(exc))),400
        except ControlsUnavailable as exc:return secure(jsonify(success=False,error=str(exc))),503
