"""Read-only provider event checks; never submit or retry an email."""
import copy,re,threading,time
from datetime import datetime,timezone
from functools import wraps
from flask import jsonify
from business_email import PREFIX,ROLES,Conflict,summary,now


def outcome(events,email,message_id):
    matched=[]
    for event in events:
        if str(event.get('email') or '').lower()!=email.lower():continue
        if str(event.get('messageId') or '').strip('<>')!=message_id.strip('<>'):continue
        matched.append(event)
    matched.sort(key=lambda e:str(e.get('date') or ''),reverse=True)
    for event in matched:
        kind=re.sub(r'[^a-z]','',str(event.get('event') or '').lower())
        mapping={'delivered':'delivered','opened':'delivered','uniqueopened':'delivered','proxyopen':'delivered','click':'delivered','clicks':'delivered',
            'hardbounce':'bounced','hardbounces':'bounced','softbounce':'deferred','softbounces':'deferred','deferred':'deferred',
            'blocked':'blocked','invalid':'blocked','error':'blocked','spam':'spam','unsubscribed':'unsubscribed','request':'queued','requests':'queued'}
        if kind not in mapping:continue
        status=mapping[kind]
        labels={'delivered':'Recipient mail server accepted the message. Inbox placement is not confirmed.',
            'bounced':'Recipient mail server rejected the message.','deferred':'Delivery is delayed; the provider may retry.',
            'blocked':'The email provider blocked delivery.','spam':'The recipient/provider reported this message as spam.',
            'unsubscribed':'The provider recorded an unsubscribe.','queued':'Provider accepted the request; no recipient delivery event yet.'}
        return dict(status=status,event=str(event.get('event') or ''),event_at=event.get('date'),reason=str(event.get('reason') or '')[:700],detail=labels[status])
    return dict(status='unknown',event=None,event_at=None,reason='',detail='No matching delivery event has been reported yet.')


class DeliveryChecks:
    def __init__(self,service):self.s=service;self.lock=threading.Lock();self.active=set();self.last={}
    def check(self,identity,limit=5):
        doc,_=self.s.load(identity)
        candidates=[copy.deepcopy(r) for r in doc['recipients'] if r['status']=='accepted' and r.get('provider_id')]
        candidates.sort(key=lambda r:str((r.get('delivery') or {}).get('checked_at') or ''))
        candidates=candidates[:limit]
        for recipient in candidates:
            try:
                response=self.s.http.get('https://api.brevo.com/v3/smtp/statistics/events',
                    headers={'api-key':self.s.g['BREVO_API_KEY'],'Accept':'application/json'},
                    params={'email':recipient['email'],'messageId':recipient['provider_id'],'limit':'100','sort':'desc'},timeout=(2,8))
                if response.status_code>=400:
                    delivery=dict(status='unavailable',detail='Provider delivery logs are unavailable (HTTP '+str(response.status_code)+').',reason='',event_at=None)
                else:
                    data=response.json();events=data.get('events',[])
                    if not isinstance(events,list):raise ValueError('Invalid provider events')
                    delivery=outcome(events,recipient['email'],recipient['provider_id'])
            except Exception:delivery=dict(status='unavailable',detail='Delivery events could not be checked. The email was not resent.',reason='',event_at=None)
            delivery['checked_at']=now()
            for _ in range(8):
                current,token=self.s.load(identity)
                row=next((r for r in current['recipients'] if r['email']==recipient['email'] and r.get('provider_id')==recipient['provider_id']),None)
                if row is None:break
                row['delivery']=delivery
                try:self.s.save(current,token);break
                except Conflict:continue
        return self.s.load(identity)[0]
    def kick(self,identity):
        with self.lock:
            if identity in self.active or time.monotonic()-self.last.get(identity,0)<20:return
            self.active.add(identity);self.last[identity]=time.monotonic()
        def work():
            try:self.check(identity)
            except Exception:pass
            finally:
                with self.lock:self.active.discard(identity)
        try:threading.Thread(target=work,name='np-email-delivery-read',daemon=True).start()
        except Exception:
            with self.lock:self.active.discard(identity)
    def bootstrap(self):
        # Reconcile a small recent campaign after a deployment without sending mail.
        try:
            rows=self.s.read('package_studio_private_settings',{'select':'key','key':'like.'+PREFIX+'*','order':'updated_at.desc','limit':'5'})
            for row in rows:
                identity=row['key'][len(PREFIX):];doc,_=self.s.load(identity)
                if any(r['status']=='accepted' and r.get('provider_id') for r in doc['recipients']):
                    self.kick(identity);break
        except Exception:pass


def install(app,g):
    service=app.config['NP_BUSINESS_EMAIL'];checks=DeliveryChecks(service);app.config['NP_BUSINESS_EMAIL_DELIVERY']=checks
    original=app.view_functions['email_campaign']
    @wraps(original)
    def campaign(identity):
        response=app.make_response(original(identity))
        if response.status_code!=200:return response
        try:doc,_=service.load(identity)
        except Exception:return response
        data=response.get_json(silent=True) or {}
        for exposed,row in zip(data.get('recipients',[]),doc['recipients']):
            exposed['delivery']=row.get('delivery')
        data['delivery_counts']={state:sum((r.get('delivery') or {}).get('status')==state for r in doc['recipients']) for state in ('delivered','bounced','blocked','deferred','queued','unknown','unavailable')}
        result=jsonify(data);result.headers['Cache-Control']='no-store, private';return result
    app.view_functions['email_campaign']=campaign
    @app.post('/admin/business_email/campaigns/<identity>/check-delivery')
    def email_delivery_check(identity):
        actor,error=g['_require_admin']()
        if error is not None:return error
        if str((actor or {}).get('role') or '').lower() not in ROLES:return jsonify(success=False,error='Admin access is required'),403
        try:
            service.load(identity);checks.kick(identity)
            return jsonify(success=True,message='Checking provider delivery events. No email will be resent.')
        except Exception:return jsonify(success=False,error='Campaign delivery could not be checked'),503
    if app.config.get('NP_EMAIL_DELIVERY_BOOTSTRAP',True):
        try:threading.Thread(target=checks.bootstrap,name='np-email-delivery-startup',daemon=True).start()
        except Exception:pass
