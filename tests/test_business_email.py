import copy,json,threading,unittest,uuid
from datetime import datetime,timedelta,timezone
from flask import Flask,jsonify
from business_email import install,render,Conflict

class Response:
    def __init__(self,data,status=200):self.data=data;self.status_code=status
    def json(self):return copy.deepcopy(self.data)
    def raise_for_status(self):
        if self.status_code>=400:raise RuntimeError('Fixture request failure')
class HTTP:
    def __init__(self):self.rows=[];self.members=[];self.sent=[];self.lock=threading.Lock();self.code=201;self.timeout=False;self.fail_db=False
    def get(self,url,params,**kw):
        if self.fail_db:raise TimeoutError()
        rows=copy.deepcopy(self.members if url.endswith('/traders') else self.rows)
        for k,v in params.items():
            if k in ('select','order','limit'):continue
            if v.startswith('eq.'):rows=[r for r in rows if str(r.get(k))==v[3:]]
            elif v.startswith('gt.'):rows=[r for r in rows if str(r.get(k))>v[3:]]
            elif v.startswith('like.'):rows=[r for r in rows if str(r.get(k)).startswith(v[5:].rstrip('*'))]
        if params.get('order')=='id.asc':rows.sort(key=lambda r:r['id'])
        return Response(rows[:int(params.get('limit',1000))])
    def post(self,url,json,**kw):
        if url.startswith('https://api.brevo.com'):
            with self.lock:self.sent.append(copy.deepcopy(json))
            if self.timeout:raise TimeoutError()
            return Response({'messageId':'fixture-'+str(len(self.sent))},self.code)
        if self.fail_db:raise TimeoutError()
        with self.lock:
            if any(r['key']==json['key'] for r in self.rows):return Response([],409)
            self.rows.append(copy.deepcopy(json));return Response([json])
    def patch(self,url,params,json,**kw):
        if self.fail_db:raise TimeoutError()
        with self.lock:
            rows=[r for r in self.rows if all(str(r.get(k))==v[3:] for k,v in params.items())]
            for r in rows:r.update(copy.deepcopy(json))
            return Response(rows)

class EmailTests(unittest.TestCase):
    def setUp(self):
        self.http=HTTP();self.app=Flask(__name__);self.role='admin';self.audit=[]
        self.g=dict(requests=self.http,SUPABASE_URL='https://fixture.invalid',SUPABASE_KEY='fixture',BREVO_API_KEY='mock',FROM_EMAIL='sender@example.test',
            _require_admin=lambda:({'id':'admin','role':self.role},None) if self.role else (None,(jsonify(success=False),401)),
            _audit_safe=lambda *a:self.audit.append(a))
        install(self.app,self.g);self.s=self.app.config['NP_BUSINESS_EMAIL'];self.c=self.app.test_client();self.s.kick=lambda identity:None
        self.http.members=[dict(id=str(uuid.UUID(int=i+1)),name='Trader '+str(i),email=f'user{i}@example.test',status='breached' if i%2 else 'inactive') for i in range(3)]
    def draft(self,**extra):
        body=dict(id=str(uuid.uuid4()),mode='all',trader_ids=[],subject='Business news',body='Hello {{name}},\nYour update.');body.update(extra)
        r=self.c.post('/admin/business_email/campaigns',json=body);self.assertEqual(r.status_code,200,r.json);return r.json,body
    def start(self,data):
        return self.c.post('/admin/business_email/campaigns/'+data['campaign']['id']+'/start',json={'revision':data['campaign']['revision']})
    def send_one(self,identity):
        claimed=self.s.claim(identity)
        if claimed:
            doc,recipient=claimed;self.s.finish(identity,recipient,self.s.deliver(doc,recipient))
        return claimed
    def test_full_audience_deduplicates_emails_and_skips_invalid_and_optouts(self):
        self.http.members=[dict(id=str(uuid.UUID(int=i+1)),name='Trader',email=f'u{i}@example.test') for i in range(621)]
        self.http.members.extend([dict(id=str(uuid.uuid4()),name='Dup',email='U1@EXAMPLE.TEST'),dict(id=str(uuid.uuid4()),email='bad\n@example.test'),dict(id=str(uuid.uuid4()),email='opt@example.test',email_unsubscribed=True)])
        data,_=self.draft();self.assertEqual(data['campaign']['total'],621);self.assertEqual(data['campaign']['skipped'],dict(invalid_email=1,duplicate_email=1,opted_out=1));self.assertEqual(self.http.sent,[])
    def test_selected_recipients_frozen_preview_and_explicit_send(self):
        target=copy.deepcopy(self.http.members[1]);data,body=self.draft(mode='selected',trader_ids=[target['id']])
        self.assertEqual([r['email'] for r in data['recipients']],[target['email']]);self.assertIn('Hello Trader 1',data['preview_html']);self.assertEqual(self.http.sent,[])
        repeated=self.c.post('/admin/business_email/campaigns',json=body);self.assertEqual(repeated.status_code,200);self.assertEqual(len(self.http.rows),1)
        self.assertEqual(self.start(data).status_code,200);self.assertEqual(self.http.sent,[])
        self.http.members[1]['email']='new@example.test'
        self.send_one(data['campaign']['id']);self.assertEqual(self.http.sent[0]['to'],[{'email':target['email']}])
        self.assertNotIn('bcc',self.http.sent[0]);self.assertNotIn('cc',self.http.sent[0]);self.assertEqual(self.s.load(data['campaign']['id'])[0]['status'],'completed')
    def test_double_start_concurrent_claims_and_reload_never_resend(self):
        data,_=self.draft();self.start(data);self.assertEqual(self.start(data).status_code,200);identity=data['campaign']['id'];claims=[]
        def claim():claims.append(self.s.claim(identity))
        threads=[threading.Thread(target=claim) for _ in range(6)]
        for t in threads:t.start()
        for t in threads:t.join()
        real=[c for c in claims if c];self.assertEqual(len(real),3);self.assertEqual(len({r['email'] for _,r in real}),3)
        for doc,recipient in real:self.s.finish(identity,recipient,self.s.deliver(doc,recipient))
        self.assertIsNone(self.s.claim(identity));self.assertEqual(len(self.http.sent),3)
        result=self.c.get('/admin/business_email/campaigns/'+identity).json;self.assertEqual(result['campaign']['counts']['accepted'],3)
    def test_pause_resume_and_provider_quota_rejection(self):
        data,_=self.draft();self.start(data);identity=data['campaign']['id'];self.send_one(identity)
        self.assertEqual(self.c.post('/admin/business_email/campaigns/'+identity+'/pause',json={}).status_code,200);self.assertIsNone(self.s.claim(identity))
        current=self.c.get('/admin/business_email/campaigns/'+identity).json;self.start(current);self.http.code=429;self.send_one(identity)
        doc,_=self.s.load(identity);self.assertEqual(doc['status'],'paused');self.assertEqual(sum(r['status']=='pending' for r in doc['recipients']),2)
        self.http.code=201;self.start(self.c.get('/admin/business_email/campaigns/'+identity).json)
        self.send_one(identity);self.send_one(identity);self.assertEqual(sum(r['status']=='accepted' for r in self.s.load(identity)[0]['recipients']),3)
    def test_timeout_or_interrupted_send_is_not_automatically_retried(self):
        data,_=self.draft();self.start(data);identity=data['campaign']['id'];self.http.timeout=True;self.send_one(identity)
        doc,token=self.s.load(identity);self.assertEqual(doc['recipients'][0]['status'],'unconfirmed')
        self.start(self.c.get('/admin/business_email/campaigns/'+identity).json);claimed=self.s.claim(identity);self.assertIsNotNone(claimed);doc,token=self.s.load(identity)
        for r in doc['recipients']:
            if r['status']=='sending':r['attempted_at']=(datetime.now(timezone.utc)-timedelta(minutes=3)).isoformat()
        self.s.save(doc,token);self.s.recover_stale(identity)
        self.http.timeout=False;self.send_one(identity);self.assertEqual(len(self.http.sent),2);self.assertEqual(sum(r['status']=='unconfirmed' for r in self.s.load(identity)[0]['recipients']),2)
    def test_roles_content_validation_and_confirmed_revision(self):
        for role,code in [(None,401),('trader',403),('staff',403)]:
            self.role=role;self.assertEqual(self.c.get('/admin/business_email/campaigns').status_code,code)
        self.role='admin'
        for extra in [dict(subject='Hi\r\nBcc: other@example.test'),dict(body=''),dict(mode='x'),dict(mode='selected',trader_ids=[]),dict(mode='selected',trader_ids=['missing'])]:
            body=dict(id=str(uuid.uuid4()),mode='all',trader_ids=[],subject='Subject',body='Message');body.update(extra)
            self.assertEqual(self.c.post('/admin/business_email/campaigns',json=body).status_code,400)
        data,_=self.draft();identity=data['campaign']['id'];self.assertEqual(self.c.post('/admin/business_email/campaigns/'+identity+'/start',json={'revision':0}).status_code,409)
        self.g['BREVO_API_KEY']='';self.assertEqual(self.start(data).status_code,400);self.assertEqual(self.http.sent,[])
        self.http.fail_db=True;self.assertEqual(self.c.get('/admin/business_email/campaigns').status_code,503)
    def test_provider_outage_or_removed_configuration_pauses_remaining_recipients(self):
        data,_=self.draft();self.start(data);identity=data['campaign']['id'];self.http.code=503;self.send_one(identity)
        doc,_=self.s.load(identity);self.assertEqual(doc['status'],'paused');self.assertEqual(sum(r['status']=='pending' for r in doc['recipients']),2)
        self.assertEqual(sum(r['status']=='unconfirmed' for r in doc['recipients']),1);self.assertEqual(len(self.http.sent),1)
        self.http.code=201;self.start(self.c.get('/admin/business_email/campaigns/'+identity).json);self.g['BREVO_API_KEY']=''
        self.assertIsNone(self.s.claim(identity));self.assertEqual(self.s.load(identity)[0]['status'],'paused');self.assertEqual(len(self.http.sent),1)

    def test_html_is_escaped_and_recipient_body_personalized(self):
        content,text=render('<subject>','Hello {{name}}\n<script>alert(1)</script>','<name>')
        self.assertNotIn('<script>',content);self.assertIn('&lt;script&gt;',content);self.assertIn('Hello <name>',text);self.assertIn('&lt;name&gt;',content)
    def test_worker_drains_saved_queue_and_draft_never_sends(self):
        data,_=self.draft();identity=data['campaign']['id'];self.s.queue=[identity];self.s.worker=True;self.s.drain();self.assertEqual(self.http.sent,[])
        self.start(data);self.s.queue=[identity];self.s.worker=True;self.s.drain();self.assertFalse(self.s.worker);self.assertEqual(len(self.http.sent),3)
        self.assertEqual(self.s.load(identity)[0]['status'],'completed')

if __name__=='__main__':unittest.main()
