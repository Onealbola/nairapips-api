"""Live staff permissions and owner-managed checkbox access. No financial writes."""
import json,re
from pathlib import Path
from flask import request,jsonify,g

MODULES=json.loads(Path(__file__).with_name('staff_modules.json').read_text())
IDS={m['id'] for m in MODULES}
LEVELS={'view':1,'edit':2,'manage':3}
ALIASES={'phaseassignment':'phaseAssign','phase_assignment':'phaseAssign','businessSettings':'settings'}
SAFE_FIELDS='id,username,name,email,role,permissions,status,created_at,last_login_at'
OWNERS={'super_admin','superadmin','owner'}
COMMON={'/audit_event','/admin/auth_check','/admin/staff_session','/admin/staff_module_catalog','/admin_bootstrap','/admin_fast_refresh','/admin_live_bootstrap','/admin_logout'}

ADMIN_LEGACY=['/account_truth_v116', '/activate_trader', '/admin_bootstrap', '/admin_bulk_resend_breach_emails', '/admin_bulk_resend_pass_emails', '/admin_complete_funded_payout_cycle', '/admin_email_status', '/admin_journey_authority/reconcile_due', '/admin_operations_center', '/admin_pass_funded/fire', '/admin_reset_trader_account', '/admin_reset_trader_password', '/admin_second_life/activate', '/affiliate_codes', '/affiliate_commissions', '/affiliate_partners', '/affiliate_payout_requests', '/announcements', '/approve_challenge_purchase', '/approve_payout', '/assign_golden_ticket_account', '/assign_phase_mt5', '/audit_event', '/black-book/candidates', '/black-book/entries', '/black-book/events', '/black-book/health', '/black-book/restore', '/black-book/restrict', '/challenge_plans', '/challenge_purchases', '/close_support_ticket', '/create_affiliate_code', '/create_affiliate_commission', '/create_affiliate_partner', '/create_announcement', '/create_challenge_plan', '/create_mt5_account', '/create_private_offer', '/deactivate_trader', '/delete_affiliate_code', '/delete_affiliate_partner', '/delete_challenge_plan', '/delete_mt5_account', '/delete_trader', '/disable_announcement', '/mark_payout_paid', '/mt5_pool', '/np_assignment_center', '/payouts', '/referral_settings', '/reject_challenge_purchase', '/reject_payout', '/reply_support_ticket', '/reset_trader_mt5', '/revenue_summary', '/staff_members', '/staff_members/delete', '/staff_members/password', '/staff_members/status', '/staff_members/update', '/support_tickets', '/trader_accounts', '/trader_trades', '/traders', '/update_affiliate_payout_request', '/update_challenge_plan', '/update_mt5_account', '/update_trader']

# Read feeds used by several modules. Mutations never inherit these read grants.
READ_SHARING={
 '/admin/finance_source':{'purchases','payouts','revenue','salesOutcomes'},
 '/admin/traders_count':{'overview','traders','actions'},
 '/admin/offer_performance':{'offers','offerStrategies'},

 '/admin/private_offer_members':{'offers','businessEmail'},

 '/admin/account_state':{'accounts','funded','breached','phase2','historicTruth','overview','actions'},
 '/trader_trades':{'accounts','historicTruth','traders','funded','breached'},
 '/np_assignment_center':{'phaseAssign','phase2'},

 '/trader_accounts':{'accounts','funded','breached','phase2','phaseAssign','recovery','timeline','historicTruth','overview','actions'},
 '/traders':{'traders','recovery','phaseAssign','phase2','offers','support','leads','blackBook','accountLinks'},
 '/challenge_purchases':{'purchases','revenue','recovery','salesOutcomes'},
 '/payouts':{'payouts','revenue'},
 '/challenge_plans':{'plans','purchases','recovery','addTrader','addBulk','settings'},
}

def owner(a):return str((a or {}).get('role') or '').lower().replace('-','_').replace(' ','_') in OWNERS

def permissions(raw):
    if isinstance(raw,str):
        try:raw=json.loads(raw)
        except ValueError:
            if raw!='all':raw=[x.strip() for x in raw.split(',') if x.strip()]
    if raw=='all' or (isinstance(raw,dict) and (raw.get('all') is True or raw.get('*') is True)):
        return {m['id']:'manage' for m in MODULES if not m['owner_only']}
    if isinstance(raw,dict) and 'modules' in raw:raw=raw['modules']
    if isinstance(raw,list):raw={k:'edit' for k in raw}
    if not isinstance(raw,dict):return {}
    out={}
    for key,value in raw.items():
        key=ALIASES.get(key,key)
        if value is True:value='edit'
        if key in IDS and key!='staff' and str(value).lower() in LEVELS:out[key]=str(value).lower()
    return out

def module_for(path):
    """Server-owned route policy: unknown staff operations require the owner."""
    p=path.lower()
    if p.startswith('/staff_members') or '/staff_' in p:return 'staff'
    if 'black-book' in p:return 'blackBook'
    if '/trader_access/' in p:return 'accountLinks'
    if 'operations_center' in p:return 'actions'
    if 'reset_trader_password' in p or 'view_trader_token' in p:return 'traders'
    if 'segment' in p or 'auto_trigger' in p or 'lead_follow' in p or 'cron_status' in p:return 'leads'
    if 'promo_code' in p:return 'offers'
    if 'production_catalog' in p:return 'plans'
    if any(x in p for x in ['account_links','account-links','access_review']):return 'accountLinks'
    if any(x in p for x in ['production_control','business_overview','package_','plan_matrix','cycle_price','catalog_control','business_setting','admin/settings']):return 'settings'
    if any(x in p for x in ['sales_outcome','sales-outcome']):return 'salesOutcomes'
    if 'historic' in p or 'account_truth' in p:return 'historicTruth'
    if 'resend_' in p and 'email' in p:return 'emailStatus'
    if 'business_email' in p:return 'businessEmail'
    if 'email_status' in p or 'email_log' in p:return 'emailStatus'
    if 'offer_strateg' in p:return 'offerStrategies'
    if 'offer' in p:return 'offers'
    if 'affiliate' in p or 'referral' in p or p.startswith('/np2'):return 'affiliates'
    if 'announcement' in p:return 'announcements'
    if 'support' in p or 'ticket' in p:return 'support'
    if 'payout' in p or 'withdrawal' in p:return 'payouts'
    if 'purchase' in p or 'payment_proof' in p:return 'purchases'
    if 'revenue' in p or 'finance' in p:return 'revenue'
    if 'lead' in p:return 'leads'
    if 'league' in p:return 'league'
    if 'timeline' in p or 'event' in p or 'audit_logs' in p:return 'timeline'
    if 'plan' in p:return 'plans'
    if 'mt5_account' in p and p!='/trader_mt5_accounts':return 'mt5pool'
    if 'pool' in p or p in ('/mt5_accounts','/add_mt5','/update_mt5','/delete_mt5','/bulk_add_mt5'):return 'mt5pool'
    if 'recover' in p or 'second_life' in p or 'reset' in p or 'recall' in p:return 'recovery'
    if 'phase2' in p or 'phase_2' in p:return 'phase2'
    if any(x in p for x in ['assign','phase','pass_stage','journey']):return 'phaseAssign'
    if 'breach' in p:return 'breached'
    if 'funded' in p:return 'funded'
    if 'account' in p or 'mt5_access' in p or 'monitoring_snapshot' in p:return 'accounts'
    if 'bulk' in p and 'trader' in p:return 'addBulk'
    if 'create_trader' in p or 'add_trader' in p:return 'addTrader'
    if 'trader' in p:return 'traders'
    if 'action_required' in p:return 'actions'
    if 'overview' in p or 'dashboard' in p:return 'overview'
    return None

class StaffUnavailable(RuntimeError):pass
class StaffConflict(StaffUnavailable):pass
class StaffStore:
    def __init__(self,http,url,key):
        self.http=http;self.url=url.rstrip('/')+'/rest/v1/admin_staff_members';self.headers={'apikey':key,'Authorization':'Bearer '+key,'Prefer':'return=representation'}
    def read(self,id=None):
        if not self.headers.get('apikey'):raise StaffUnavailable('Staff verification requires protected server credentials.')
        params={'select':SAFE_FIELDS,'limit':'1000','order':'created_at.desc'}
        if id is not None:params.update({'id':'eq.'+str(id),'limit':'2'})
        try:
            r=self.http.get(self.url,headers=self.headers,params=params,timeout=(2,4));r.raise_for_status();rows=r.json()
            if not isinstance(rows,list):raise ValueError('Invalid staff response')
            rows=[{k:v for k,v in row.items() if k in SAFE_FIELDS.split(',')} for row in rows]
            if id is not None and (len(rows)!=1 or str(rows[0].get('id'))!=str(id)):return None
            return rows[0] if id is not None else rows
        except Exception as exc:raise StaffUnavailable('Staff access could not be verified. Please retry.') from exc
    def write(self,method,payload,id=None,expected=None):
        if not self.headers.get('apikey'):raise StaffUnavailable('Staff changes require protected server credentials.')
        kwargs={'headers':self.headers,'timeout':(2,5)}
        if id is not None:kwargs['params']={'id':'eq.'+str(id)}
        if expected is not None:
            kwargs.setdefault('params',{}).update({'permissions':('is.null' if expected.get('permissions') is None else 'eq.'+json.dumps(expected['permissions'],sort_keys=True,separators=(',',':'))),'status':'eq.'+str(expected.get('status') or ''),'role':'eq.'+str(expected.get('role') or '')})
        if payload is not None:kwargs['json']=payload
        try:
            r=getattr(self.http,method)(self.url,**kwargs);r.raise_for_status();rows=r.json()
            if rows==[] and expected is not None:raise StaffConflict('This staff record changed in another session. Reload it before saving.')
            if not isinstance(rows,list) or len(rows)!=1:raise ValueError('Staff save not confirmed')
            return {k:v for k,v in rows[0].items() if k in SAFE_FIELDS.split(',')}
        except StaffConflict:raise
        except Exception as exc:raise StaffUnavailable('The staff change was not confirmed. Reload this staff member before retrying.') from exc

def install(app,ns,store=None):
    store=store or StaffStore(ns['requests'],ns['SUPABASE_URL'],ns.get('SUPABASE_SERVICE_ROLE_KEY') or '')
    raw_auth=ns['_request_admin_auth']
    def fail(message,status=403):return jsonify(success=False,error=message,code='STAFF_ACCESS_DENIED'),status
    def current():
        if hasattr(g,'np_staff_access'):return g.np_staff_access
        token=raw_auth()
        if not token or token.get('id') is None:return None
        if token.get('id')=='bootstrap-super-admin' and owner(token):staff=token
        else:
            staff=store.read(token.get('id'))
            if not staff or str(staff.get('status','')).lower()!='active':return None
        g.np_staff_access=staff;return staff
    def require():
        try:staff=current()
        except StaffUnavailable:return None,fail('Staff access verification is temporarily unavailable.',503)
        return (staff,None) if staff else (None,fail('Admin authentication is required: your staff session is inactive. Please sign in again.',401))
    ns['_require_admin']=require
    # Other existing admin helpers must also see the current role/status, not the token's old role.
    def fresh_auth():
        try:return current()
        except StaffUnavailable:return None
    ns['_request_admin_auth']=fresh_auth
    def gate():
        if request.method=='OPTIONS' or request.path in ('/staff_login','/healthz/http'):return None
        # Existing trader/engine authentication stays independent.
        token=raw_auth()
        if not token:
            if request.method in ('GET','HEAD') and (request.path in {'/challenge_plans','/announcements','/referral_settings'} or (request.path in {'/trader_accounts','/payouts','/trader_trades'} and request.args.get('trader_id'))):return None
            if request.path in ADMIN_LEGACY or (request.path.startswith('/admin/') and not request.path.endswith('.html') and request.path not in {'/admin/np_unified_mt5_sync'}):return fail('Admin authentication is required. Please sign in again.',401)
            return None
        staff,error=require()
        if error is not None:return error
        if owner(staff):return None
        if request.path in COMMON:return None
        if request.path=='/traders' and request.method=='POST':
            allowed=permissions(staff.get('permissions'))
            if any(LEVELS.get(allowed.get(k),0)>=2 for k in ('addTrader','addBulk','traders')):return None
            return fail('You do not have permission to create traders.')
        if request.path.startswith('/public/') or request.path.endswith('_version'):return None
        if request.method in ('GET','HEAD') and request.path in READ_SHARING and set(permissions(staff.get('permissions'))).intersection(READ_SHARING[request.path]):return None
        module=module_for(request.path)
        level=permissions(staff.get('permissions')).get(module,'none')
        required=3 if request.method=='DELETE' or re.search(r'delete|remove',request.path,re.I) else 1 if request.method in ('GET','HEAD') else 2
        if module=='staff' or LEVELS.get(level,0)<required:return fail('You do not have access to this module or action. Contact your administrator.')
    app.before_request_funcs.setdefault(None,[]).insert(0,gate)
    @app.get('/staff_access_version',endpoint='np_staff_access_version')
    def version():return jsonify(success=True,release='staff-access-20261122-v1',modules=len(MODULES),live_permissions=True)
    @app.get('/admin/staff_session',endpoint='np_staff_access_session')
    def session():
        staff,error=require()
        if error is not None:return error
        response=jsonify(success=True,staff=staff,permissions='all' if owner(staff) else permissions(staff.get('permissions')),owner=owner(staff));response.headers['Cache-Control']='no-store';return response
    @app.get('/admin/staff_module_catalog',endpoint='np_staff_access_catalog')
    def catalog():return jsonify(success=True,modules=MODULES,release='staff-access-20261122-v1')
    def only_owner():
        staff,error=require()
        if error is not None:return staff,error
        return (staff,None) if owner(staff) else (None,fail('Only the owner can manage staff access.'))
    def crud(action):
        staff,error=only_owner()
        if error is not None:return error
        try:
            if action=='list':return jsonify(store.read())
            d=request.get_json(silent=True) or {};id=d.get('id');target=None
            if action!='create':
                if not id:return fail('Select a staff member.',400)
                target=store.read(id)
                if not target:return fail('Staff member not found.',404)
                if owner(target) or str(id)==str(staff['id']):return fail('Owner accounts are protected. Their access cannot be removed here.')
            if target and 'expected_permissions' in d and permissions(target.get('permissions'))!=permissions(d['expected_permissions']):return fail('Permissions changed in another session. Reload this staff member before saving.',409)
            if target and 'expected_status' in d and target.get('status')!=d['expected_status']:return fail('Staff status changed in another session. Reload this staff member before saving.',409)
            if action=='delete':row=store.write('delete',None,id,expected=target)
            else:
                payload={}
                if action in ('create','update'):
                    for k in ('name','email','username','role','status'):
                        if k in d:payload[k]=str(d[k] or '').strip()
                    if action=='update':payload.pop('username',None)
                    if payload.get('role','support') not in ('admin','manager','admin_manager','support','viewer'):return fail('Choose a staff role; owner access cannot be assigned here.',400)
                    if 'permissions' in d:
                        raw=d['permissions']
                        if not isinstance(raw,dict) or set(raw)-IDS or any(v not in LEVELS for v in raw.values()) or 'staff' in raw:return fail('Choose valid module permissions from the checklist.',400)
                        payload['permissions']=raw
                    if 'email' in payload:payload['email']=payload['email'].lower()
                    if action=='create':
                        if not payload.get('username') or not payload.get('name'):return fail('Username and name are required.',400)
                        payload.setdefault('role','support');payload.setdefault('status','active');payload.setdefault('permissions',{})
                if action=='status':payload['status']=d.get('status')
                if 'status' in payload and payload['status'] not in ('active','inactive'):return fail('Choose Active or Inactive.',400)
                password=d.get('password') or d.get('new_password')
                if password:
                    if not isinstance(password,str) or len(password)<8:return fail('Use a password of at least 8 characters.',400)
                    payload['password']=password
                elif action in ('create','password'):return fail('A password is required.',400)
                row=store.write('post' if action=='create' else 'patch',payload,None if action=='create' else id,expected=target)
            audit=ns.get('_audit_safe')
            if audit:audit('staff',action,json.dumps({'permissions_before':permissions((target or {}).get('permissions')),'permissions_after':permissions(row.get('permissions')),'status_before':(target or {}).get('status'),'status_after':row.get('status')},sort_keys=True),staff,row.get('id',''))
            return jsonify(success=True,data=[row])
        except StaffConflict as exc:return fail(str(exc),409)
        except StaffUnavailable as exc:return fail(str(exc),503)
    for endpoint,action in [('staff_members','list'),('create_staff_member','create'),('update_staff_member','update'),('staff_member_status','status'),('staff_member_password','password'),('staff_member_delete','delete')]:
        if endpoint in app.view_functions:app.view_functions[endpoint]=(lambda action=action:crud(action))
    # Bootstrap is shared by modules; do not deliver unrelated private collections.
    fields={'staff':{'staff'},'traders':{'overview','traders','addTrader','addBulk','accounts','funded','breached','recovery','phaseAssign','phase2','purchases','payouts','offers','support','leads','salesOutcomes','blackBook','accountLinks'},'traderAccounts':{'accounts','funded','breached','historicTruth','recovery','phaseAssign','phase2','overview','actions','payouts'},'purchases':{'purchases','recovery','salesOutcomes','revenue','overview','actions'},'payouts':{'payouts','revenue','overview','actions'},'mt5Accounts':{'mt5pool','phaseAssign','phase2','recovery'},'plans':{'plans','purchases','recovery','settings'},'tickets':{'support'},'announcements':{'announcements'},'offers':{'offers','offerStrategies'},'leads':{'leads'}}
    aliases={
        'traders':['data','users','users_list','all_traders','traders_list'],
        'traderAccounts':['accounts','trader_accounts','all_accounts','trader_accounts_list','monitoring_snapshots','snapshots','trader_trades','trades'],
        'purchases':['challenge_purchases','all_purchases','purchases_list'],
        'payouts':['all_payouts','payouts_list'],
        'mt5Accounts':['mt5_pool','all_mt5','available_mt5','mt5_available','phase_assignment_queue','assignment_queue'],
        'plans':['challenge_plans','all_plans','plans_list'],
        'tickets':['support_tickets','open_tickets','tickets_list'],
        'announcements':['announcements_list'],
        'staff':['staff_members'],
    }
    fields.update({'events':{'timeline','actions','accounts','overview'},'monitoring_events':{'timeline','actions','accounts','overview'},'events_list':{'timeline','actions','accounts','overview'},'audit_logs':{'timeline'},'affiliate_partners':{'affiliates'},'affiliate_codes':{'affiliates'},'affiliate_commissions':{'affiliates'},'affiliate_summary':{'affiliates'},'referral_settings':{'affiliates'},'business_settings':{'settings'},'marketing_deleted_contacts':{'leads'}})
    for key,names in aliases.items():
        for name in names:fields[name]=fields[key]
    @app.after_request
    def filter_bootstrap(response):
        if request.path not in ('/admin_bootstrap','/admin_fast_refresh','/admin_live_bootstrap') or response.status_code!=200:return response
        staff=getattr(g,'np_staff_access',None)
        if not staff or owner(staff):return response
        body=response.get_json(silent=True)
        if not isinstance(body,dict):return response
        allowed=set(permissions(staff.get('permissions')))
        for root in [body,body.get('data')]:
            if not isinstance(root,dict):continue
            for key,modules in fields.items():
                if key in root and not allowed.intersection(modules):root[key]={} if isinstance(root[key],dict) else []
            if isinstance(root.get('counts'),dict):
                for key in root['counts']:
                    rule=fields.get(key)
                    if rule and not allowed.intersection(rule):root['counts'][key]=0
        response.set_data(app.json.dumps(body));response.headers['Cache-Control']='no-store';return response
    return store
