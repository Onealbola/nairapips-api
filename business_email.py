"""Admin-authored business email with private durable drafts and recipient receipts."""
import copy,hashlib,html,json,re,threading,uuid
from datetime import datetime,timezone
from flask import jsonify,request

PREFIX='business_email_v1:'
ROLES={'owner','super_admin','superadmin','admin','admin_manager'}
EMAIL=re.compile(r'^[^\s@<>;,\r\n]+@[^\s@<>;,\r\n]+\.[^\s@<>;,\r\n]+$')
class Conflict(ValueError):pass
class Unavailable(RuntimeError):pass

def now():return datetime.now(timezone.utc).isoformat()
def render(subject,body,name):
    text=body.replace('{{name}}',name or 'Trader')
    safe=html.escape(text).replace('\n','<br>')
    return '<!doctype html><html><body style="margin:0;background:#f5f5f2;font-family:Arial,sans-serif;color:#172022"><main style="max-width:620px;margin:24px auto;background:white;padding:32px;border-top:4px solid #c6a64d"><p style="font-weight:bold;color:#8b701d">NAIRAPIPS</p><h1 style="font-size:24px">'+html.escape(subject)+'</h1><div style="line-height:1.7;font-size:16px">'+safe+'</div><hr style="border:0;border-top:1px solid #ddd;margin-top:28px"><p style="font-size:12px;color:#666">NairaPips · Business communication<br><a href="https://nairapips.com/dashboard/">Open your trader dashboard</a></p></main></body></html>',text

def summary(doc):
    states={key:sum(r['status']==key for r in doc['recipients']) for key in ('pending','sending','accepted','failed','unconfirmed')}
    return {k:doc[k] for k in ('id','subject','body','mode','status','revision','created_at','updated_at','created_by','skipped')}|dict(total=len(doc['recipients']),counts=states,pause_reason=doc.get('pause_reason'))


class BusinessEmail:
    def __init__(self,g):
        self.g=g;self.http=g['requests'];self.base=g['SUPABASE_URL'].rstrip('/')+'/rest/v1/'
        key=g.get('SUPABASE_SERVICE_ROLE_KEY') or g['SUPABASE_KEY']
        self.headers={'apikey':key,'Authorization':'Bearer '+key,'Prefer':'return=representation'}
        self.lock=threading.Lock();self.queue=[];self.worker=False
    def ready(self):return bool(self.g.get('BREVO_API_KEY') and self.g.get('FROM_EMAIL'))
    def read(self,table,params):
        try:
            r=self.http.get(self.base+table,headers=self.headers,params=params,timeout=(2,8));r.raise_for_status();rows=r.json()
            if not isinstance(rows,list):raise ValueError()
            return rows
        except Exception as exc:raise Unavailable('Email records could not be loaded. Retry.') from exc
    def members(self):
        after=''
        for _ in range(200):
            params={'select':'*','order':'id.asc','limit':'500'}
            if after:params['id']='gt.'+after
            rows=self.read('traders',params)
            yield from rows
            if len(rows)<500:return
            next_id=str(rows[-1]['id'])
            if next_id==after:raise Unavailable('Member pagination did not advance')
            after=next_id
        raise ValueError('Member list is too large for this campaign')
    def recipients(self,mode,ids):
        if mode not in ('all','selected'):raise ValueError('Choose all traders or selected traders')
        if not isinstance(ids,list) or any(not isinstance(x,str) for x in ids):raise ValueError('Select registered traders')
        selected=set(ids)
        if mode=='selected' and not selected:raise ValueError('Select at least one trader')
        members=list(self.members());known={str(t['id']) for t in members}
        if mode=='selected' and selected-known:raise ValueError('A selected member no longer exists. Reload members before sending.')
        skipped={'invalid_email':0,'duplicate_email':0,'opted_out':0};result=[];seen=set()
        for t in members:
            if mode=='selected' and str(t['id']) not in selected:continue
            if any(t.get(k) is True or str(t.get(k) or '').lower() in ('true','yes','1') for k in ('email_opt_out','email_unsubscribed','unsubscribed')):
                skipped['opted_out']+=1;continue
            email=str(t.get('email') or t.get('canonical_email') or '').strip().lower()
            if not EMAIL.fullmatch(email) or len(email)>254:skipped['invalid_email']+=1;continue
            if email in seen:skipped['duplicate_email']+=1;continue
            seen.add(email);result.append(dict(trader_id=str(t['id']),name=str(t.get('name') or 'Trader')[:200],email=email,status='pending',attempt_id=None,attempted_at=None,finished_at=None,provider_id=None,error=None))
        if not result:raise ValueError('No selected members have an eligible email address')
        return result,skipped
    def load(self,identity):
        identity=str(uuid.UUID(identity));rows=self.read('package_studio_private_settings',{'select':'value,updated_by','key':'eq.'+PREFIX+identity,'limit':'1'})
        if not rows:raise ValueError('Email campaign not found')
        try:
            doc=json.loads(rows[0]['value'])
            if doc['id']!=identity or doc['schema']!=1 or not isinstance(doc['recipients'],list):raise ValueError()
        except Exception as exc:raise Unavailable('Campaign record could not be verified') from exc
        return doc,rows[0]['updated_by']
    def save(self,doc,token=None):
        doc=copy.deepcopy(doc);doc['revision']+=1;doc['updated_at']=now();value=json.dumps(doc,separators=(',',':'),ensure_ascii=False)
        if len(value.encode())>1000000:raise ValueError('This campaign is too large. Select a smaller group.')
        marker='np-email-write:'+str(uuid.uuid4());row=dict(key=PREFIX+doc['id'],value=value,updated_at=doc['updated_at'],updated_by=marker)
        try:
            if token is None:r=self.http.post(self.base+'package_studio_private_settings',headers=self.headers,json=row,timeout=(2,8))
            else:r=self.http.patch(self.base+'package_studio_private_settings',headers=self.headers,params={'key':'eq.'+row['key'],'updated_by':'eq.'+token},json=row,timeout=(2,8))
            if r.status_code==409:raise Conflict('Campaign already exists or changed. Reload it.')
            r.raise_for_status();rows=r.json()
            if not isinstance(rows,list) or len(rows)!=1:raise Conflict('Campaign changed in another session. Reload it.')
            return doc
        except (Conflict,ValueError):raise
        except Exception as exc:raise Unavailable('Email changes were not confirmed. Reload the campaign before retrying.') from exc
    def create(self,body,actor):
        subject=body.get('subject');message=body.get('body')
        if not isinstance(subject,str) or not subject.strip() or len(subject)>200 or any(ord(c)<32 for c in subject):raise ValueError('Enter a subject of up to 200 characters on one line')
        if not isinstance(message,str) or not message.strip() or len(message)>15000:raise ValueError('Enter a message of up to 15,000 characters')
        identity=str(uuid.UUID(str(body.get('id') or '')));mode=body.get('mode');ids=body.get('trader_ids',[])
        if mode not in ('all','selected') or not isinstance(ids,list) or any(not isinstance(x,str) for x in ids):raise ValueError('Choose a valid recipient group')
        fingerprint=hashlib.sha256(json.dumps([subject.strip(),message.strip(),mode,sorted(set(ids))],sort_keys=True).encode()).hexdigest()
        existing=self.read('package_studio_private_settings',{'select':'value','key':'eq.'+PREFIX+identity,'limit':'1'})
        if existing:
            doc=json.loads(existing[0]['value'])
            if doc.get('fingerprint')!=fingerprint:raise Conflict('This draft identity has different content. Create a new preview.')
            return doc
        recipients,skipped=self.recipients(mode,ids)
        doc=dict(id=identity,schema=1,subject=subject.strip(),body=message.strip(),mode=mode,status='draft',revision=0,
            created_at=now(),updated_at=now(),created_by=str(actor),fingerprint=fingerprint,skipped=skipped,pause_reason=None,recipients=recipients)
        return self.save(doc)
    def change_status(self,identity,status,revision=None):
        for _ in range(8):
            doc,token=self.load(identity)
            if doc['status']=='completed':return doc
            if status=='queued':
                if doc['status']=='draft' and (type(revision) is not int or revision!=doc['revision']):raise Conflict('Preview this saved version before sending')
                if doc['status']=='completed':return doc
                if doc['status'] not in ('draft','paused','queued','sending'):raise ValueError('Campaign cannot be sent')
                if not self.ready():raise ValueError('Business email sender is not configured')
            elif doc['status']=='draft':raise ValueError('This draft has not been sent')
            if not any(r['status']=='pending' for r in doc['recipients']) and status=='queued':return doc
            doc['status']=status
            if status=='queued':doc['pause_reason']=None
            try:return self.save(doc,token)
            except Conflict:continue
        raise Conflict('Campaign is busy. Reload its progress.')
    def recover_stale(self,identity):
        for _ in range(8):
            doc,token=self.load(identity);changed=False
            for row in doc['recipients']:
                if row['status']!='sending':continue
                try:age=(datetime.now(timezone.utc)-datetime.fromisoformat(row['attempted_at'])).total_seconds()
                except Exception:age=121
                if age>120:
                    row.update(status='unconfirmed',finished_at=now(),error='Sending was interrupted. Not resent automatically.');changed=True
            if not changed:return doc
            if doc['status']!='paused' and not any(r['status'] in ('pending','sending') for r in doc['recipients']):doc['status']='completed'
            try:return self.save(doc,token)
            except Conflict:continue
        raise Conflict('Campaign is busy. Reload its status.')
    def claim(self,identity):
        for _ in range(12):
            doc,token=self.load(identity)
            if doc['status'] not in ('queued','sending'):return None
            if not self.ready():
                doc.update(status='paused',pause_reason='Email sender is not configured. No further requests were sent.')
                try:self.save(doc,token);return None
                except Conflict:continue
            recipient=next((r for r in doc['recipients'] if r['status']=='pending'),None)
            if recipient is None:
                if not any(r['status']=='sending' for r in doc['recipients']):
                    doc['status']='completed'
                    try:self.save(doc,token)
                    except Conflict:continue
                return None
            recipient.update(status='sending',attempt_id=str(uuid.uuid4()),attempted_at=now());doc['status']='sending'
            snapshot=copy.deepcopy(recipient)
            try:self.save(doc,token);return doc,snapshot
            except Conflict:continue
        raise Conflict('Campaign is busy')
    def deliver(self,doc,recipient):
        # One private To address per request. Never retry an uncertain provider call.
        content,text=render(doc['subject'],doc['body'],recipient['name'])
        try:
            r=self.http.post('https://api.brevo.com/v3/smtp/email',headers={'api-key':self.g['BREVO_API_KEY'],'Content-Type':'application/json'},
                json={'sender':{'name':'NairaPips','email':self.g['FROM_EMAIL']},'to':[{'email':recipient['email']}],
                    'subject':doc['subject'],'htmlContent':content,'textContent':text,'tags':['business_email'],
                    'headers':{'idempotencyKey':recipient['attempt_id']}},timeout=(2,10))
            if 200<=r.status_code<300:
                try:provider_id=r.json().get('messageId')
                except Exception:provider_id=None
                return dict(status='accepted',provider_id=provider_id,error=None)
            if r.status_code in (401,402,403,429):
                return dict(status='pending',provider_id=None,error='Email provider paused sending (HTTP '+str(r.status_code)+'). Check sender settings or quota before resuming.',pause=True)
            # A definite API rejection is kept distinct from an uncertain transport.
            return dict(status='unconfirmed' if r.status_code>=500 else 'failed',provider_id=None,
                error=('Provider acceptance is unconfirmed (HTTP '+str(r.status_code)+'). Not resent automatically.')
                    if r.status_code>=500 else 'Email provider rejected this request (HTTP '+str(r.status_code)+').',
                pause=r.status_code>=500)
        except Exception:return dict(status='unconfirmed',provider_id=None,error='Provider acceptance could not be confirmed. Not resent automatically.',pause=True)
    def finish(self,identity,recipient,result):
        for _ in range(12):
            doc,token=self.load(identity);row=next((r for r in doc['recipients'] if r['email']==recipient['email']),None)
            if not row or row['attempt_id']!=recipient['attempt_id'] or row['status']!='sending':return
            result=copy.deepcopy(result);paused=result.pop('pause',False)
            row.update(result,finished_at=now())
            if paused:doc.update(status='paused',pause_reason=result.get('error'))
            if doc['status']!='paused' and not any(r['status'] in ('pending','sending') for r in doc['recipients']):doc['status']='completed'
            try:self.save(doc,token);return
            except Conflict:continue
        raise Conflict('Email receipt is waiting for reconciliation')
    def kick(self,identity):
        with self.lock:
            if identity not in self.queue:self.queue.append(identity)
            if self.worker:return
            self.worker=True
        try:threading.Thread(target=self.drain,name='np-business-email',daemon=True).start()
        except Exception:
            with self.lock:self.worker=False
            raise Unavailable('Sender worker could not start. Resume this campaign.')
    def drain(self):
        while True:
            with self.lock:
                if not self.queue:self.worker=False;return
                identity=self.queue.pop(0)
            try:
                claimed=self.claim(identity)
                if not claimed:continue
                doc,recipient=claimed
                result=self.deliver(doc,recipient);self.finish(identity,recipient,result)
                with self.lock:
                    if identity not in self.queue:self.queue.append(identity)
            except Exception:
                # The saved per-recipient claim prevents duplicates after crashes.
                # A subsequent authenticated status read resumes pending recipients.
                continue


def install(app,g):
    service=BusinessEmail(g);app.config['NP_BUSINESS_EMAIL']=service
    def admin():
        actor,error=g['_require_admin']()
        if error is not None:return None,error
        if str((actor or {}).get('role') or '').lower() not in ROLES:return None,(jsonify(success=False,error='Admin access is required for business email'),403)
        return actor,None
    def reply(data,status=200):
        r=jsonify(data);r.status_code=status;r.headers['Cache-Control']='no-store, private';return r
    def error(exc):return reply(dict(success=False,error=str(exc)),409 if isinstance(exc,Conflict) else 400 if isinstance(exc,ValueError) else 503)
    @app.get('/admin/business_email/campaigns')
    def email_campaigns():
        _,denied=admin()
        if denied is not None:return denied
        try:
            rows=service.read('package_studio_private_settings',{'select':'value','key':'like.'+PREFIX+'*','order':'updated_at.desc','limit':'20'})
            return reply(dict(success=True,ready=service.ready(),campaigns=[summary(json.loads(r['value'])) for r in rows]))
        except Exception:return reply(dict(success=False,error='Email history could not be loaded'),503)
    @app.post('/admin/business_email/campaigns')
    def create_email_campaign():
        actor,denied=admin()
        if denied is not None:return denied
        try:
            doc=service.create(request.get_json(silent=True) or {},actor.get('id') or actor.get('username') or 'admin')
            content,_=render(doc['subject'],doc['body'],doc['recipients'][0]['name'])
            try:g['_audit_safe']('business_email','draft_created','Email draft '+doc['id'],actor,doc['id'])
            except Exception:pass
            return reply(dict(success=True,campaign=summary(doc),preview_html=content,recipients=[{k:r[k] for k in ('trader_id','name','email')} for r in doc['recipients']]))
        except Exception as exc:return error(exc)
    @app.get('/admin/business_email/campaigns/<identity>')
    def email_campaign(identity):
        _,denied=admin()
        if denied is not None:return denied
        try:
            doc=service.recover_stale(identity)
            if doc['status'] in ('queued','sending') and any(r['status']=='pending' for r in doc['recipients']):service.kick(identity)
            # Interrupted attempts are shown as unconfirmed rather than retried.
            rows=[]
            for r in doc['recipients']:
                row={k:r[k] for k in ('trader_id','name','email','status','attempted_at','finished_at','error')}
                rows.append(row)
            content,_=render(doc['subject'],doc['body'],doc['recipients'][0]['name'])
            return reply(dict(success=True,campaign=summary(doc),preview_html=content,recipients=rows))
        except Exception as exc:return error(exc)
    @app.post('/admin/business_email/campaigns/<identity>/start')
    def start_email_campaign(identity):
        actor,denied=admin()
        if denied is not None:return denied
        try:
            doc=service.change_status(identity,'queued',(request.get_json(silent=True) or {}).get('revision'))
            if any(r['status']=='pending' for r in doc['recipients']):service.kick(identity)
            try:g['_audit_safe']('business_email','send_requested','Email campaign '+doc['id']+' to '+str(len(doc['recipients']))+' unique addresses',actor,doc['id'])
            except Exception:pass
            return reply(dict(success=True,campaign=summary(doc)))
        except Exception as exc:return error(exc)
    @app.post('/admin/business_email/campaigns/<identity>/pause')
    def pause_email_campaign(identity):
        _,denied=admin()
        if denied is not None:return denied
        try:return reply(dict(success=True,campaign=summary(service.change_status(identity,'paused'))))
        except Exception as exc:return error(exc)
