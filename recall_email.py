"""Transactional recall notices, with durable claims and no historical bulk send."""
import json,queue,re,threading,uuid
from datetime import datetime,timezone,timedelta
from business_email import EMAIL,render as render_business
from recalled_account_visibility import recalled

RELEASE='recall-email-20261119-v1'
PREFIX='recall_email_v1:'
CONTROL='recall_email_activation_v1'
ACCOUNT_FIELDS='id,trader_id,mt5_login,account_status,archived_at,archive_reason,admin_note'

def now():return datetime.now(timezone.utc).isoformat()
def stamp(value):
    value=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('Recall time must include timezone')
    return value.astimezone(timezone.utc)
def identity(value):return str(uuid.UUID(str(value)))
def render(login,name):
    if not re.fullmatch(r'\d{1,20}',str(login)):raise ValueError('Exact MT5 login is required')
    subject=f'MT5 account {login} recalled — await admin instructions'
    body=(f'Hello {{{{name}}}},\n\nNairaPips Admin has recalled MT5 account {login}.\n\n'
          'A recalled account is no longer authorised for trading and must not be used again. '
          'Please stop using this account and ignore the login credentials previously issued for it. '
          'The recalled account has been removed from your trader dashboard.\n\n'
          'Please wait for further instructions from NairaPips Admin. '
          'Any replacement account and its credentials will be communicated separately.\n\n'
          f'This notice applies only to recalled MT5 account {login}.\n\nNairaPips Admin')
    content,text=render_business(subject,body,name)
    return subject,content,text

class Notifier:
    def __init__(self,g):
        self.g=g;self.http=g['requests'];self.base=g['SUPABASE_URL'].rstrip('/')+'/rest/v1/'
        key=g.get('SUPABASE_SERVICE_ROLE_KEY') or g['SUPABASE_KEY']
        self.headers={'apikey':key,'Authorization':'Bearer '+key,'Prefer':'return=representation'}
        self.started_at=now();self.lock=threading.Lock();self.queue=queue.Queue();self.last={}
    def ready(self):return bool(self.g.get('BREVO_API_KEY') and self.g.get('FROM_EMAIL'))
    def admins(self):
        addresses=[]
        for value in (self.g.get('ADMIN_ALERT_EMAIL'),self.g.get('OWNER_ALERT_EMAIL')):
            address=str(value or '').strip()
            if EMAIL.fullmatch(address) and address.casefold() not in {a.casefold() for a in addresses}:addresses.append(address)
        return addresses
    def read(self,table,params):
        r=self.http.get(self.base+table,headers=self.headers,params={'select':'*',**params},timeout=(2,8));r.raise_for_status();rows=r.json()
        if not isinstance(rows,list):raise ValueError('Recall notification read unavailable')
        return rows
    def insert(self,key,value):
        r=self.http.post(self.base+'package_studio_private_settings',headers=self.headers,json=dict(key=key,value=json.dumps(value,sort_keys=True),updated_at=now(),updated_by=RELEASE),timeout=(2,8))
        if r.status_code==409:return False
        r.raise_for_status();return True
    def save(self,stored,value):
        r=self.http.patch(self.base+'package_studio_private_settings',headers=self.headers,params={'key':'eq.'+stored['key'],'value':'eq.'+stored['value']},json=dict(value=json.dumps(value,sort_keys=True),updated_at=now(),updated_by=RELEASE),timeout=(2,8));r.raise_for_status();rows=r.json()
        if len(rows)!=1:return False
        stored['value']=rows[0]['value'];return True
    def activation(self):
        rows=self.read('package_studio_private_settings',{'key':'eq.'+CONTROL,'limit':'1'})
        if not rows:
            self.insert(CONTROL,dict(release=RELEASE,activated_at=self.started_at,cursor_at=self.started_at,cursor_id='00000000-0000-0000-0000-000000000000'))
            rows=self.read('package_studio_private_settings',{'key':'eq.'+CONTROL,'limit':'1'})
        if len(rows)!=1:raise ValueError('Recall email activation unavailable')
        return rows[0]
    def exact(self,aid,tid):
        rows=self.read('trader_accounts',{'id':'eq.'+identity(aid),'trader_id':'eq.'+identity(tid),'select':ACCOUNT_FIELDS,'limit':'1'})
        if len(rows)!=1 or rows[0].get('id')!=aid or rows[0].get('trader_id')!=tid:raise ValueError('Exact recalled account unavailable')
        return rows[0]
    def enqueue(self,a,activated_at):
        if not recalled(a) or not a.get('archived_at') or stamp(a['archived_at'])<stamp(activated_at):return False
        aid=identity(a['id']);tid=identity(a['trader_id']);login=str(a.get('mt5_login') or '')
        if not re.fullmatch(r'\d{1,20}',login):return False
        case=dict(account_id=aid,trader_id=tid,mt5_login=login,recalled_at=a['archived_at'],state='pending',created_at=now(),attempt_id=str(uuid.uuid5(uuid.NAMESPACE_URL,'nairapips-recall-email:'+aid)))
        return self.insert(PREFIX+aid,case)
    def collect(self):
        stored=self.activation();control=json.loads(stored['value']);at=control['cursor_at'];aid=control['cursor_id']
        # The durable scan recovers a worker crash after recall but before its hook.
        rows=self.read('trader_accounts',{'or':'(archived_at.gt.'+at+',and(archived_at.eq.'+at+',id.gt.'+aid+'))','select':ACCOUNT_FIELDS,'order':'archived_at.asc,id.asc','limit':'25'})
        for a in rows:
            if not a.get('archived_at') or (stamp(a['archived_at']),a['id'])<=(stamp(at),aid):continue
            self.enqueue(a,control['activated_at'])
            updated=dict(control,cursor_at=a['archived_at'],cursor_id=a['id'])
            if not self.save(stored,updated):break
            control=updated
        # Hooks accelerate delivery; they perform no network activity in recall.
        for _ in range(10):
            try:account_id,trader_id=self.queue.get_nowait()
            except queue.Empty:break
            self.enqueue(self.exact(account_id,trader_id),control['activated_at'])
        return len(rows)
    def prepare(self,case):
        a=self.exact(case['account_id'],case['trader_id'])
        if not recalled(a) or str(a.get('mt5_login'))!=case['mt5_login']:return None
        people=self.read('traders',{'id':'eq.'+case['trader_id'],'select':'id,name,email','limit':'1'})
        if len(people)!=1 or people[0].get('id')!=case['trader_id']:raise ValueError('Exact recall recipient unavailable')
        trader=people[0];email=str(trader.get('email') or '').strip()
        if not EMAIL.fullmatch(email):raise ValueError('Registered recall email unavailable')
        subject,content,text=render(case['mt5_login'],trader.get('name') or 'Trader')
        admins=self.admins()
        if not admins:raise ValueError('Admin recall-copy recipient unavailable')
        return dict(recipient=email,admin_copies=[a for a in admins if a.casefold()!=email.casefold()],subject=subject,html=content,text=text)
    def deliver(self,stored):
        case=json.loads(stored['value'])
        if case.get('state')!='pending' or (case.get('retry_after') and stamp(case['retry_after'])>datetime.now(timezone.utc)):return False
        try:mail=self.prepare(case)
        except Exception:
            checks=int(case.get('evidence_checks',0))+1
            self.save(stored,dict(case,state='needs_review' if checks>=5 else 'pending',evidence_checks=checks,error='Recall or recipient could not be verified',retry_after=(datetime.now(timezone.utc)+timedelta(seconds=60)).isoformat()));return False
        if mail is None:
            self.save(stored,dict(case,state='cancelled',reason='Account is no longer recalled'));return False
        claimed=dict(case,state='sending',claimed_at=now(),recipient=mail['recipient'],admin_copies=mail['admin_copies'],subject=mail['subject'])
        if not self.save(stored,claimed):return False
        try:
            confirmed=self.prepare(case)
            if confirmed is None or confirmed['recipient']!=mail['recipient'] or confirmed['admin_copies']!=mail['admin_copies']:
                self.save(stored,dict(claimed,state='cancelled',reason='Recall or recipient changed before sending'));return False
        except Exception:
            self.save(stored,dict(claimed,state='needs_review',error='Final recall check unavailable'));return False
        try:
            payload={'sender':{'name':'NairaPips','email':self.g['FROM_EMAIL']},'to':[{'email':mail['recipient']}],'subject':mail['subject'],'htmlContent':mail['html'],'textContent':mail['text'],'tags':['account_recall'],'headers':{'idempotencyKey':case['attempt_id']}}
            if mail['admin_copies']:payload['bcc']=[{'email':address} for address in mail['admin_copies']]
            r=self.http.post('https://api.brevo.com/v3/smtp/email',headers={'api-key':self.g['BREVO_API_KEY'],'Content-Type':'application/json'},json=payload,timeout=(2,10))
            if 200<=r.status_code<300:
                try:provider_id=r.json().get('messageId')
                except Exception:provider_id=None
                result=dict(state='accepted',provider_id=provider_id,accepted_at=now())
            elif r.status_code in (401,402,403,429):result=dict(state='pending',error='Provider unavailable: HTTP '+str(r.status_code),retry_after=(datetime.now(timezone.utc)+timedelta(minutes=15)).isoformat())
            else:result=dict(state='unconfirmed' if r.status_code>=500 else 'failed',error='Provider HTTP '+str(r.status_code)+'; no automatic resend')
        except Exception:result=dict(state='unconfirmed',error='Provider acceptance unknown; no automatic resend')
        # A failed receipt write leaves `sending`, never a duplicate auto-send.
        self.save(stored,dict(claimed,**result))
        logger=self.g.get('_log_email_bank')
        if callable(logger):
            context=self.g.get('_NP_RENEWAL_REST_V211');token=None
            try:
                if context is not None:token=context.set(True)
                for recipient in [mail['recipient']]+mail['admin_copies']:
                    logger(recipient,mail['subject'],email_type='account_recall' if recipient==mail['recipient'] else 'account_recall_admin_copy',status='sent' if result['state']=='accepted' else result['state'],trader_id=case['trader_id'],message=mail['text'],provider_response=json.dumps({'messageId':result.get('provider_id'),'account_id':case['account_id']}),error=result.get('error') or '')
            except Exception:pass
            finally:
                if token is not None:context.reset(token)
        return result['state']=='accepted'
    def cycle(self):
        if not self.ready() or not self.lock.acquire(False):return
        try:
            collected=self.collect();rows=self.read('package_studio_private_settings',{'key':'like.'+PREFIX+'*','value':'like.*"state": "pending"*','order':'updated_at.asc','limit':'20'})
            accepted=0
            checked=0
            for stored in rows:
                case=json.loads(stored['value'])
                if case.get('retry_after') and stamp(case['retry_after'])>datetime.now(timezone.utc):continue
                accepted+=bool(self.deliver(stored));checked+=1
                if checked>=5:break
            self.last=dict(checked_at=now(),collected=collected,accepted=accepted)
        except Exception as exc:self.last=dict(checked_at=now(),error=type(exc).__name__)
        finally:self.lock.release()
    def start(self):
        def loop():
            import time
            time.sleep(8)
            while True:self.cycle();time.sleep(30)
        threading.Thread(target=loop,name='np-recall-email',daemon=True).start()

def install(app,g):
    if app.config.get('NP_RECALL_EMAIL') is not None:return app.config['NP_RECALL_EMAIL']
    service=Notifier(g);app.config['NP_RECALL_EMAIL']=service
    for name in ('_np_mh_quarantine','_np_ur_v87_retire_exact'):
        original=g.get(name)
        if not callable(original) or getattr(original,'_recall_email_hook',False) is True:continue
        def wrap(core):
            def hooked(account,*args,**kwargs):
                result=core(account,*args,**kwargs)
                try:service.queue.put_nowait((identity(account['id']),identity(account['trader_id'])))
                except Exception:pass
                return result
            hooked._recall_email_hook=True
            return hooked
        g[name]=wrap(original)
    service.start()
    return service
