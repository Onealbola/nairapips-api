"""Evidence-based breach explanations and a durable, single-delivery outbox.

Reads saved account evidence only. Never changes trading, breach, reset or payout
state. No estimated closing balance and no historic bulk mail on activation.
"""
import copy,hashlib,html,json,threading,uuid
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
from string import Template
from flask import jsonify,request
from business_email import EMAIL

PREFIX='breach_analysis_email_v1:'
CONTROL='breach_analysis_email_activation_v1'
RELEASE='breach-explanation-20261113-v1'

def now():return datetime.now(timezone.utc).isoformat()
def decimal(value):
    n=Decimal(str(value))
    if not n.is_finite():raise ValueError('Non-finite breach evidence')
    return n

def stamp(value):
    d=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Breach time has no timezone')
    return d

def money(n):return '₦'+format(decimal(n),',.2f')

def evidence(read,account):
    from equity_drawdown import spec,fingerprint,initial,evaluate
    from plan_risk_controls import assert_assignment_snapshot,WAT
    from assignment_corrections import voided
    a=account
    if not a.get('breached_at') or voided(a):raise ValueError('No current recorded breach on this exact account')
    rows=read('challenge_purchases',{'id':'eq.'+a['purchase_id'],'trader_id':'eq.'+a['trader_id'],'limit':'1'})
    if len(rows)!=1:raise ValueError('Exact purchased rules unavailable')
    p=rows[0]
    if p['id']!=a['purchase_id'] or p['trader_id']!=a['trader_id']:raise ValueError('Breach purchase ownership mismatch')
    if p.get('payment_status')!='approved':raise ValueError('The exact issued purchase is not payment-approved')
    assert_assignment_snapshot(a,p);rules=spec(a,p)
    if p.get('max_drawdown') is not None and decimal(p['max_drawdown'])!=decimal(rules['limit']):raise ValueError('Purchased maximum and account limit conflict')
    journals=read('package_studio_private_settings',{'key':'eq.equity_drawdown_v1:'+a['id'],'limit':'1'})
    if journals:
        state=json.loads(journals[0]['value']);first=state.get('first_breach') or {}
        if not state.get('breached') or state.get('rules_hash')!=fingerprint(rules):raise ValueError('Exact terminal risk journal is not verified')
        prior=initial(rules);prior['highest_equity']=state['highest_equity']
        realised=state.get('daily_realised')
        replay=evaluate(rules,prior,decimal(first['equity']),first['observed_at'],balance=decimal(first['balance']),daily_realised=realised)
        if not replay['breached'] or decimal(replay['drawdown_floor'])!=decimal(first['floor']):raise ValueError('Saved breach does not replay under purchased rules')
        observed=first['observed_at'];peak=state['highest_equity']
        # Refuse the known false-daily-input defect rather than emailing a false
        # accusation. Saved trade replays are deduplicated by broker ticket.
        if decimal(rules.get('daily_drawdown',0))>0 and (decimal(first['equity']) if rules['drawdown_type']=='TRAILING' else min(decimal(first['equity']),decimal(first['balance'])))>decimal(replay['overall_floor']) and stamp(observed).astimezone(WAT).date()==stamp(a['created_at']).astimezone(WAT).date():
            trades=read('trader_trades',{'trader_account_id':'eq.'+a['id'],'trader_id':'eq.'+a['trader_id'],'status':'eq.closed','order':'synced_at.desc','limit':'1000'})
            if len(trades)>=1000:raise ValueError('Daily closing-history evidence is incomplete')
            unique={}
            for t in trades:
                if t.get('closed_at') and stamp(a['created_at'])<=stamp(t['closed_at'])<=stamp(observed):unique.setdefault(str(t['ticket']),t)
            net=sum((sum((decimal(t.get(k) or 0) for k in ('profit','swap','commission')),Decimal(0)) for t in unique.values()),Decimal(0))
            if abs(decimal(rules['capital'])+net-decimal(first['balance']))>Decimal('.01') or realised is None or abs(net-decimal(realised))>Decimal('.01'):
                raise ValueError('Recorded daily input needs broker-history review')
    else:
        if rules['drawdown_type']=='TRAILING' or decimal(rules.get('daily_drawdown',0))>0:raise ValueError('Required peak or daily journal is unavailable')
        events=read('monitoring_events',{'trader_account_id':'eq.'+a['id'],'trader_id':'eq.'+a['trader_id'],'event_type':'eq.breached','order':'created_at.asc','limit':'1'})
        if len(events)!=1:raise ValueError('Exact static breach observation unavailable')
        event=events[0]
        if event.get('trader_account_id')!=a['id'] or event.get('trader_id')!=a['trader_id']:raise ValueError('Static breach owner mismatch')
        floor=decimal(rules['capital'])*(1-decimal(rules['limit'])/100)
        if min(decimal(event['balance']),decimal(event['equity']))>floor:raise ValueError('Static observation does not cross its purchased limit')
        first=dict(balance=str(event['balance']),equity=str(event['equity']),floor=str(floor))
        observed=None;peak=None;replay={}
    equity=decimal(first['equity']);balance=decimal(first['balance']);floor=decimal(first['floor'])
    overall_floor=decimal(replay.get('overall_floor',decimal(peak)-decimal(rules['capital'])*decimal(rules['limit'])/100 if rules['drawdown_type']=='TRAILING' else decimal(rules['capital'])*(1-decimal(rules['limit'])/100)))
    daily_cross=bool(replay.get('daily_floor') is not None and equity<=decimal(replay['daily_floor']))
    overall_cross=(equity if rules['drawdown_type']=='TRAILING' else min(equity,balance))<=overall_floor
    kind='daily' if daily_cross and not overall_cross else 'trailing' if rules['drawdown_type']=='TRAILING' else 'maximum'
    if kind!='daily':floor=overall_floor
    percent=rules.get('daily_drawdown') if kind=='daily' else rules['limit']
    allowance=decimal(rules['capital'])*decimal(percent)/100
    values=[('Recorded highest equity',money(peak))] if kind=='trailing' else []
    if kind=='daily':values.append(('Net realised trading P&L for the day',money(replay['daily_realised'])))
    values.extend([('Allowed fall from that peak' if kind=='trailing' else 'Daily loss allowance' if kind=='daily' else 'Maximum loss allowance',money(allowance)),
        ('Trailing breach line' if kind=='trailing' else 'Daily breach line' if kind=='daily' else 'Maximum drawdown breach line',money(floor)),
        ('Equity when the breach was detected',money(equity)),('Balance when the breach was detected',money(balance))])
    settled=None
    if observed:
        # A later flat broker snapshot can supply a settled balance. Never use
        # the trigger balance, a screenshot or a capital/P&L estimate as one.
        try:
            snapshots=read('monitoring_snapshots',{'trader_account_id':'eq.'+a['id'],'trader_id':'eq.'+a['trader_id'],'order':'created_at.desc','limit':'20'})
            for snapshot in snapshots:
                raw=snapshot.get('raw_data') or {}
                if not isinstance(raw,dict):continue
                at=raw.get('observed_at') or raw.get('timestamp')
                if (str(snapshot.get('trader_account_id'))!=a['id'] or str(snapshot.get('trader_id'))!=a['trader_id'] or str(raw.get('mt5_login'))!=str(a['mt5_login'])
                        or not str(raw.get('source') or '').startswith('dd_police') or type(raw.get('open_positions')) is not int or raw['open_positions']!=0):continue
                if not at or not 0<(stamp(at)-stamp(observed)).total_seconds()<=600:continue
                closing=decimal(raw['balance'])
                if closing!=decimal(raw['equity']) or closing!=decimal(snapshot['balance']):continue
                settled=closing;values.append(('Verified balance after trades were closed',money(settled)));break
        except Exception:pass
    recorded=a['breached_at'];delay=None
    if observed:
        delta=(stamp(recorded)-stamp(observed)).total_seconds()
        if delta < -5:raise ValueError('Breach timestamps conflict')
        if delta>=0:delay=round(delta,1)
    return dict(account_id=a['id'],purchase_id=p['id'],trader_id=a['trader_id'],login=a['mt5_login'],kind=kind,percent=str(percent),capital=money(rules['capital']),rows=values,
        observed_at=observed,recorded_at=recorded,recording_delay_seconds=delay,below_line=money(floor-(equity if kind in ('trailing','daily') else min(equity,balance))),settled_below_line=money(floor-settled) if settled is not None and settled<=floor else None,rules_hash=fingerprint(rules),release=RELEASE)

def render(context,name):
    c=context;kind=c['kind'];label={'trailing':'trailing DD','daily':'daily DD','maximum':'maximum DD'}[kind]
    subject='NairaPips account '+c['login']+' — breach explanation'
    opening='The saved evidence shows that your account crossed its '+label+' limit.'
    intro='Account '+c['login']+' had '+c['percent']+'% '+label+' on '+c['capital']+'.'
    explanation=('Floating profits raise the trailing line. This explains why an account can breach while its final loss from the original deposit looks relatively small. The allowed fall is a fixed percentage of the starting account size, and the line never moves down.' if kind=='trailing' else
        'The daily limit includes realised trading P&L and floating P&L for the Nigerian trading day. A confirmed daily breach permanently ends this account.' if kind=='daily' else
        'This maximum drawdown limit is measured from the starting account size. Touching or falling below the breach line ends this account.')
    explanation+=' The recorded risk value was '+c['below_line']+' below the breach line.'
    if c.get('settled_below_line'):explanation+=' The verified balance after closing was also '+c['settled_below_line']+' below that line.'
    def display(t):return stamp(t).astimezone(timezone.utc).strftime('%d %B %Y at %H:%M:%S UTC')
    timing=('The crossing was observed on '+display(c['observed_at'])+' and recorded on '+display(c['recorded_at'])+'.' if c['observed_at'] else 'The breach was recorded on '+display(c['recorded_at'])+'. An exact broker observation time was not retained for this legacy event.')
    if c['recording_delay_seconds'] is not None:timing+=' The recording delay was about '+format(c['recording_delay_seconds'],'g')+' seconds. The saved trigger preserves the original equity observation.'
    paragraphs=['Hello '+(name or 'Trader')+',',opening,intro]
    plain='\n\n'.join(paragraphs)+'\n\nItem | Amount\n'+'\n'.join(k+' | '+v for k,v in c['rows'])+'\n\n'+explanation+'\n\n'+timing+'\n\nThis account remains breached. Later price recovery does not reverse a confirmed breach.\n\nNairaPips Team\nhttps://nairapips.com/dashboard/'
    esc=html.escape
    content=''.join('<p style="margin:0 0 16px">'+esc(p)+'</p>' for p in paragraphs)
    content+='<table cellpadding="10" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;margin:18px 0;font-size:14px"><thead><tr><th align="left" style="border-bottom:2px solid #d6b65c">Item</th><th align="right" style="border-bottom:2px solid #d6b65c">Amount</th></tr></thead><tbody>'+''.join('<tr><td style="border-bottom:1px solid #ddd">'+esc(k)+'</td><td align="right" style="border-bottom:1px solid #ddd;white-space:nowrap">'+esc(v)+'</td></tr>' for k,v in c['rows'])+'</tbody></table>'
    content+=''.join('<p style="margin:18px 0">'+esc(p)+'</p>' for p in (explanation,timing,'This account remains breached. Later price recovery does not reverse a confirmed breach.'))
    template=Template((Path(__file__).with_name('email_templates')/'business_update.html').read_text())
    return subject,template.substitute(subject=esc(subject),content=content,preheader=esc(opening),recipient=esc(name or 'Trader')).replace('MEMBER UPDATE','ACCOUNT RISK NOTICE'),plain

class Notifier:
    def __init__(self,g):
        self.g=g;self.http=g['requests'];self.base=g['SUPABASE_URL'].rstrip('/')+'/rest/v1/'
        key=g.get('SUPABASE_SERVICE_ROLE_KEY') or g['SUPABASE_KEY']
        self.headers={'apikey':key,'Authorization':'Bearer '+key,'Prefer':'return=representation'}
        self.lock=threading.Lock();self.last={};self.started_at=now()
    def ready(self):return bool(self.g.get('BREVO_API_KEY') and self.g.get('FROM_EMAIL'))
    def admins(self):
        result=[]
        for value in (self.g.get('ADMIN_ALERT_EMAIL'),self.g.get('OWNER_ALERT_EMAIL')):
            email=str(value or '').strip()
            if EMAIL.fullmatch(email) and email.casefold() not in {e.casefold() for e in result}:result.append(email)
        return result
    def read(self,table,params):
        r=self.http.get(self.base+table,headers=self.headers,params=dict(params,select='*'),timeout=(2,8));r.raise_for_status();rows=r.json()
        if not isinstance(rows,list):raise ValueError('Breach email read failed')
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
            activated=self.started_at;value=dict(activated_at=activated,cursor_at=activated,cursor_id='00000000-0000-0000-0000-000000000000',release=RELEASE)
            self.insert(CONTROL,value);rows=self.read('package_studio_private_settings',{'key':'eq.'+CONTROL,'limit':'1'})
        if len(rows)!=1:raise ValueError('Breach email activation unavailable')
        return rows[0]
    def enqueue(self,a,explicit=False,admin_only=False):
        from assignment_corrections import voided
        if not a.get('breached_at') or voided(a):return False
        suffix=':admin-copy' if admin_only else ''
        value=dict(account_id=a['id'],trader_id=a['trader_id'],breached_at=a['breached_at'],state='pending',created_at=now(),explicit=explicit,audience='admin_copy' if admin_only else 'trader',attempt_id=str(uuid.uuid5(uuid.NAMESPACE_URL,'nairapips-breach-email:'+a['id']+':'+a['breached_at']+suffix)))
        return self.insert(PREFIX+a['id']+suffix,value)
    def collect(self):
        stored=self.activation();control=json.loads(stored['value'])
        at=control['cursor_at'];aid=control['cursor_id']
        rows=self.read('trader_accounts',{'or':'(breached_at.gt.'+at+',and(breached_at.eq.'+at+',id.gt.'+aid+'))','order':'breached_at.asc,id.asc','limit':'20'})
        for a in rows:
            if not a.get('breached_at'):continue
            if stamp(a['breached_at'])<stamp(control['activated_at']) or (stamp(a['breached_at']),a['id'])<=(stamp(at),aid):continue
            self.enqueue(a)
            updated=dict(control,cursor_at=a['breached_at'],cursor_id=a['id'])
            if not self.save(stored,updated):break
            control=updated
        return len(rows)
    def prepare(self,case):
        rows=self.read('trader_accounts',{'id':'eq.'+case['account_id'],'trader_id':'eq.'+case['trader_id'],'limit':'1'})
        if len(rows)!=1:raise ValueError('Exact breach account no longer available')
        a=rows[0]
        if a['id']!=case['account_id'] or a['trader_id']!=case['trader_id'] or a.get('breached_at')!=case['breached_at']:raise ValueError('Breach identity changed')
        context=evidence(self.read,a)
        people=self.read('traders',{'id':'eq.'+case['trader_id'],'limit':'1'})
        if len(people)!=1 or people[0]['id']!=case['trader_id']:raise ValueError('Exact breach recipient unavailable')
        trader=people[0];email=str(trader.get('email') or '').strip()
        if not EMAIL.fullmatch(email):raise ValueError('Registered recipient email unavailable')
        subject,content,text=render(context,trader.get('name') or 'Trader')
        admins=self.admins()
        if case.get('audience')=='admin_copy':
            if not admins:raise ValueError('Admin copy recipient is not configured')
            return dict(context=context,recipient=admins[0],recipients=admins,admin_copies=[],subject='Admin copy — '+subject,html=content,text=text)
        return dict(context=context,recipient=email,recipients=[email],admin_copies=[a for a in admins if a.casefold()!=email.casefold()],subject=subject,html=content,text=text)
    def deliver(self,stored):
        case=json.loads(stored['value'])
        if case.get('state')!='pending':return False
        if case.get('retry_after') and stamp(case['retry_after'])>datetime.now(timezone.utc):return False
        try:mail=self.prepare(case)
        except Exception as exc:
            from datetime import timedelta
            attempts=int(case.get('evidence_checks',0))+1
            self.save(stored,dict(case,state='needs_review' if attempts>=5 else 'pending',evidence_checks=attempts,error=str(exc)[:250],checked_at=now(),retry_after=(datetime.now(timezone.utc)+timedelta(seconds=60)).isoformat()));return False
        claimed=dict(case,state='sending',claimed_at=now(),recipient=mail['recipient'],recipients=mail['recipients'],admin_copies=mail['admin_copies'],subject=mail['subject'],analysis=mail['context'])
        if not self.save(stored,claimed):return False
        # Re-read the canonical account immediately before external delivery; an
        # audited correction or a changed breach identity cancels this message.
        try:
            confirmed=self.prepare(case)
            if confirmed['recipients']!=mail['recipients'] or confirmed['admin_copies']!=mail['admin_copies'] or confirmed['context']['rules_hash']!=mail['context']['rules_hash']:
                raise ValueError('Recipient or purchased rules changed before delivery')
        except Exception as exc:
            self.save(stored,dict(claimed,state='needs_review',error=str(exc)[:250]));return False
        try:
            payload={'sender':{'name':'NairaPips','email':self.g['FROM_EMAIL']},'to':[{'email':e} for e in mail['recipients']],'subject':mail['subject'],'htmlContent':mail['html'],'textContent':mail['text'],'tags':['breach_analysis'],'headers':{'idempotencyKey':case['attempt_id']}}
            if mail['admin_copies']:payload['bcc']=[{'email':e} for e in mail['admin_copies']]
            r=self.http.post('https://api.brevo.com/v3/smtp/email',headers={'api-key':self.g['BREVO_API_KEY'],'Content-Type':'application/json'},json=payload,timeout=(2,10))
            if 200<=r.status_code<300:
                try:provider_id=r.json().get('messageId')
                except Exception:provider_id=None
                result=dict(state='accepted',provider_id=provider_id,accepted_at=now())
            elif r.status_code in (401,402,403,429):
                from datetime import timedelta
                result=dict(state='pending',error='Provider sending unavailable: HTTP '+str(r.status_code),retry_after=(datetime.now(timezone.utc)+timedelta(minutes=15)).isoformat())
            else:result=dict(state='unconfirmed' if r.status_code>=500 else 'failed',error='Provider HTTP '+str(r.status_code)+'. Not automatically resent.')
        except Exception:result=dict(state='unconfirmed',error='Provider acceptance could not be confirmed. Not automatically resent.')
        self.save(stored,dict(claimed,**result))
        logger=self.g.get('_log_email_bank')
        if callable(logger):
            context=self.g.get('_NP_RENEWAL_REST_V211');token=None
            try:
                if context is not None:token=context.set(True)
                for recipient in mail['recipients']+mail['admin_copies']:
                    logger(recipient,mail['subject'],email_type='breach_admin_copy' if case.get('audience')=='admin_copy' or recipient in mail['admin_copies'] else 'breach',status='sent' if result['state']=='accepted' else result['state'],trader_id=case['trader_id'],message=mail['text'],provider_response=json.dumps({'messageId':result.get('provider_id'),'acceptance':result['state'],'account_id':case['account_id']}),error=result.get('error') or '')
            except Exception:pass
            finally:
                if token is not None:context.reset(token)
        return result['state']=='accepted'
    def cycle(self):
        if not self.ready() or not self.lock.acquire(False):return
        try:
            collected=self.collect();rows=self.read('package_studio_private_settings',{'key':'like.'+PREFIX+'*','value':'like.*"state": "pending"*','order':'updated_at.asc','limit':'20'})
            accepted=0;checked=0
            for stored in rows:
                pending=json.loads(stored['value'])
                if pending.get('state')!='pending' or (pending.get('retry_after') and stamp(pending['retry_after'])>datetime.now(timezone.utc)):continue
                accepted+=bool(self.deliver(stored));checked+=1
                if checked>=5:break
            self.last=dict(checked_at=now(),collected=collected,checked=checked,accepted=accepted)
        except Exception as exc:self.last=dict(checked_at=now(),error=str(exc)[:180])
        finally:self.lock.release()
    def start(self):
        def loop():
            # No network or provider calls on the application import thread.
            import time
            time.sleep(8)
            while True:
                self.cycle();time.sleep(45)
        threading.Thread(target=loop,name='np-breach-analysis-email',daemon=True).start()


def install(app,g):
    service=Notifier(g);app.config['NP_BREACH_ANALYSIS_EMAIL']=service
    original=g.get('send_account_status_email')
    if callable(original):
        def status_email(trader,subject,title,details=''):
            if 'breach' not in str(subject).lower():return original(trader,subject,title,details)
            try:
                tid=str((trader or {}).get('id') or '')
                params={'trader_id':'eq.'+tid,'order':'breached_at.desc.nullslast','limit':'1'}
                if (trader or {}).get('mt5_login'):params['mt5_login']='eq.'+str(trader['mt5_login'])
                rows=service.read('trader_accounts',params)
                if not rows or rows[0].get('trader_id')!=tid:return False
                if (trader or {}).get('mt5_login') and str(rows[0].get('mt5_login'))!=str(trader['mt5_login']):return False
                return bool(service.enqueue(rows[0],explicit=True))
            except Exception:return False
        g['send_account_status_email']=status_email
    @app.get('/admin/breach_analysis_emails')
    def breach_email_status():
        _,error=g['_require_admin']()
        if error is not None:return error
        params={'key':'like.'+PREFIX+'*','order':'updated_at.desc','limit':'30'}
        try:
            if request.args.get('account_id'):params['key']='eq.'+PREFIX+str(uuid.UUID(request.args['account_id']))
            rows=service.read('package_studio_private_settings',params)
            cases=[json.loads(r['value']) for r in rows]
            response=jsonify(success=True,release=RELEASE,ready=service.ready(),last=service.last,emails=cases);response.headers['Cache-Control']='no-store, private';return response
        except ValueError:return jsonify(success=False,error='Supply a valid account identity'),400
        except Exception:return jsonify(success=False,error='Breach email receipts unavailable'),503
    if app.config.get('NP_BREACH_EMAIL_WORKER',True):service.start()
    return service
