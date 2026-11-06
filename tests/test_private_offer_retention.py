import copy,threading,unittest,uuid,ast,json,base64
from pathlib import Path
from unittest.mock import Mock
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from flask import Flask,request,jsonify
from private_offer_retention import install,expired,discount,matches

class Response:
    def __init__(self,rows):self.rows=rows
    def json(self):return copy.deepcopy(self.rows)
    def raise_for_status(self):pass
class HTTP:
    def __init__(self):self.rows={'traders':[],'announcements':[],'challenge_purchases':[]};self.lock=threading.Lock();self.fail=False;self.fallback=False
    def get(self,url,params,**kw):
        if self.fail:raise TimeoutError()
        rows=copy.deepcopy(self.rows[url.rsplit('/',1)[1]])
        for key,value in params.items():
            if key in ('select','limit','order'):continue
            op,target=value.split('.',1)
            rows=[r for r in rows if (str(r.get(key) or '')==target if op=='eq' else str(r.get(key) or '')>target if op=='gt' else str(r.get(key) or '')>=target)]
        if params.get('order')=='id.asc':rows.sort(key=lambda r:r['id'])
        return Response(rows[:int(params.get('limit',1000))])
    def post(self,url,json,**kw):
        if self.fail:raise TimeoutError()
        if self.fallback and 'discount_percent' in json:raise ValueError('Fixture missing optional columns')
        row=dict(json,id=str(uuid.uuid4()));self.rows['announcements'].append(row);return Response([row])
    def patch(self,url,params,json,**kw):
        with self.lock:
            rows=[r for r in self.rows['announcements'] if r['id']==params['id'][3:] and r['status']==params['status'][3:]]
            for r in rows:r.update(json)
            return Response(rows)

class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.http=HTTP();self.app=Flask(__name__);self.role=True;self.owner='owner';self.created=[]
        self.t={'id':'owner','name':'Owner','email':'owner@example.test'}
        self.offer=dict(id='offer',type='private_offer',status='active',target_trader_id='owner',target_email='owner@example.test',discount_percent=37.5,offer_code='RETURN1',expires_at=(datetime.now(timezone.utc)+timedelta(days=1)).isoformat(),created_at='2026-11-06',delivery_dashboard=True)
        self.http.rows['announcements']=[self.offer];self.http.rows['traders']=[self.t]
        self.g={'requests':self.http,'SUPABASE_URL':'https://db.test','SUPABASE_KEY':'fixture','_np_offer_merge_meta':lambda r:dict(r),
            '_authenticated_trader_id_for_request':lambda requested:(self.owner,None) if self.owner and (not requested or requested==self.owner) else (None,'forbidden'),
            '_get_trader_by_id':lambda tid:next((r for r in self.http.rows['traders'] if r['id']==tid),None),
            '_require_admin':lambda:({'role':'admin'},None) if self.role else (None,(jsonify(success=False),401)),
            '_np_private_offer_expired':lambda row:False,'_np_offer_discount_percent':lambda row,code:50,
            '_np_offer_bool':lambda value,default=False:default if value is None else bool(value),'_aff_code':lambda c:str(c).upper(),
            '_np_find_offer_trader':lambda tid,email:self.t if tid=='owner' or email=='owner@example.test' else None}
        self.g.update(_request_admin_auth=lambda:None,_trader_safe_offer_row=lambda r:{k:r[k] for k in ('id','expires_at','discount_percent')},_np_notice_audience_matches=lambda *a:False)
        self.g['_np_private_offer_usable']=lambda row:row.get('status')=='active' and not self.g['_np_private_offer_expired'](row)
        @self.app.post('/create_private_offer',endpoint='create_private_offer')
        def create():self.created.append(request.get_json());return jsonify(success=True)
        @self.app.post('/claim_private_offer',endpoint='claim_private_offer')
        def claim():return jsonify(success=True,data=request.get_json())
        @self.app.post('/mark_private_offer_read',endpoint='mark_private_offer_read')
        def read():return jsonify(success=True)
        @self.app.post('/create_challenge_purchase',endpoint='create_purchase')
        def purchase():
            d=request.get_json();q=self.g['_np_private_offer_quote'](d,20000,d.get('promo_code'))
            if not q or not q['valid']:return jsonify(success=False),400
            if d.get('bad'):return jsonify(success=False),400
            self.http.rows['challenge_purchases'].append(dict(id='purchase',trader_id=d['trader_id'],promo_code=d['promo_code'],created_at=datetime.now(timezone.utc).isoformat()))
            return jsonify(success=True,data=q)
        self.app.add_url_rule('/private_offers_for_trader','private_offers_for_trader',lambda:jsonify([]),methods=['GET'])
        self.app.add_url_rule('/trader_targeted_offers','trader_targeted_offers',lambda:jsonify([]),methods=['GET'])
        install(self.app,self.g);self.service=self.app.config['NP_PRIVATE_RETENTION'];self.c=self.app.test_client()
    def test_percentage_identity_and_expiry_at_quote(self):
        with self.app.test_request_context():
            q=self.service.quote({'trader_id':'owner'},20000,'RETURN1');self.assertEqual(q['final_fee'],12500);self.assertEqual(q['discount_percent'],37.5)
            self.offer['expires_at']='broken';self.assertFalse(self.service.quote({'trader_id':'owner'},20000,'RETURN1')['valid'])
            for end in [None,'',datetime.now(timezone.utc).isoformat()]:self.assertTrue(expired({'expires_at':end}))
            self.offer['expires_at']=(datetime.now(timezone.utc)+timedelta(days=1)).isoformat()
            self.owner='other';self.http.rows['traders'].append({'id':'other','email':'owner@example.test'})
            self.assertFalse(self.service.quote({'trader_id':'other','email':'owner@example.test'},20000,'RETURN1')['valid'])
            self.owner=None;self.assertIsNone(self.service.quote({},20000,'AFFILIATE'))
    def test_single_use_at_purchase_and_claim_does_not_consume(self):
        r=self.c.post('/claim_private_offer',json={'trader_id':'owner','email':'forged@example.test'});self.assertEqual(r.status_code,200);self.assertEqual(r.json['data']['offer_id'],'offer');self.assertEqual(self.offer['status'],'active')
        r=self.c.post('/create_challenge_purchase',json={'trader_id':'owner','promo_code':'RETURN1'});self.assertEqual(r.status_code,200,r.json);self.assertEqual(r.json['data']['final_fee'],12500);self.assertEqual(self.offer['status'],'used')
        self.assertEqual(self.c.post('/create_challenge_purchase',json={'trader_id':'owner','promo_code':'RETURN1'}).status_code,409)
        self.assertEqual(len(self.http.rows['challenge_purchases']),1)
    def test_failed_validation_releases_only_when_no_purchase_exists(self):
        r=self.c.post('/create_challenge_purchase',json={'trader_id':'owner','promo_code':'RETURN1','bad':True});self.assertEqual(r.status_code,400);self.assertEqual(self.offer['status'],'active')
    def test_duplicate_concurrent_submissions_only_create_one_purchase(self):
        entered=threading.Event();release=threading.Event();base=self.app.view_functions['create_purchase'].__wrapped__
        # Pause the real purchase core after its reservation is committed.
        self.app.view_functions['create_purchase'].__wrapped__=base
        original=self.service.quote
        def blocked(*a):entered.set();release.wait(3);return original(*a)
        self.service.quote=blocked;self.g['_np_private_offer_quote']=blocked
        results=[]
        def send():
            with self.app.test_client() as c:results.append(c.post('/create_challenge_purchase',json={'trader_id':'owner','promo_code':'RETURN1'}).status_code)
        thread=threading.Thread(target=send);thread.start();self.assertTrue(entered.wait(1))
        try:self.assertEqual(self.c.post('/create_challenge_purchase',json={'trader_id':'owner','promo_code':'RETURN1'}).status_code,409)
        finally:release.set();thread.join(4)
        self.assertEqual(results,[200]);self.assertEqual(len(self.http.rows['challenge_purchases']),1)
    def test_member_pages_include_every_registered_status_and_auth(self):
        self.http.rows['traders']=[dict(id=str(uuid.UUID(int=i+1)),name=f'Member {i}',email=f'{i}@example.test',status='breached' if i%2 else 'inactive') for i in range(621)]
        rows=[];after=''
        while True:
            r=self.c.get('/admin/private_offer_members',query_string={'after':after});self.assertEqual(r.status_code,200);rows+=r.json['members'];after=r.json['next_after']
            if not after:break
        self.assertEqual(len(rows),621);self.assertEqual(len({r['id'] for r in rows}),621)
        self.role=False;self.assertEqual(self.c.get('/admin/private_offer_members').status_code,401)
    def test_create_validates_percentage_member_and_future_date(self):
        base=dict(target_trader_id='owner',discount_percent=23.5,expires_at=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat())
        for bad in [dict(discount_percent=100),dict(discount_percent=-1),dict(discount_percent=True),dict(expires_at='bad'),dict(expires_at='2020-01-01'),dict(target_trader_id='other')]:
            self.assertEqual(self.c.post('/create_private_offer',json=dict(base,**bad)).status_code,400)
        r=self.c.post('/create_private_offer',json=base);self.assertEqual(r.status_code,200);self.assertEqual(self.created[0]['discount_percent'],23.5);self.assertTrue(self.created[0]['expires_at'].endswith('+00:00'))
        self.owner=None;self.assertEqual(self.c.post('/claim_private_offer',json={'trader_id':'owner'}).status_code,401)
        self.http.fail=True;self.assertEqual(self.c.get('/admin/private_offer_members').status_code,503)
    def test_quote_finds_targeted_offer_beyond_first_page(self):
        strangers=[dict(self.offer,id=f'{i:06}',target_trader_id='other',offer_code='OTHER') for i in range(450)]
        self.http.rows['announcements']=strangers+[self.offer]
        with self.app.test_request_context():
            q=self.service.quote({'trader_id':'owner'},20000,'RETURN1')
        self.assertTrue(q['valid']);self.assertEqual(q['final_fee'],12500)

    def test_real_creation_keeps_percentage_in_schema_fallback_and_sends_only_after_save(self):
        tree=ast.parse(Path('app.py').read_text())
        names={'create_private_offer','_np_offer_meta_pack','_np_offer_meta_unpack','_np_offer_merge_meta'}
        nodes=[copy.deepcopy(n) for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
        for node in nodes:node.decorator_list=[]
        self.g.update(request=request,json=json,base64=base64,now_iso=lambda:datetime.now(timezone.utc).isoformat(),
            _np_offer_clean_str=lambda v,max_len=2000:str(v or '').strip()[:max_len],
            ok=lambda data,message:jsonify(success=True,data=data),bad=lambda error,*a:(jsonify(success=False,error=str(error)),400),
            _audit_safe=Mock(),_admin_from_payload=lambda d:{},send_email_brevo=Mock(return_value=True),_np_private_offer_html=Mock(return_value='<p>fixture</p>'))
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'real_offer','exec'),self.g)
        app=Flask('original-core');self.g['app']=app
        app.add_url_rule('/create_private_offer','create_private_offer',self.g['create_private_offer'],methods=['POST'])
        for route,endpoint in [('/claim_private_offer','claim_private_offer'),('/mark_private_offer_read','mark_private_offer_read'),('/create_challenge_purchase','create_purchase')]:
            app.add_url_rule(route,endpoint,lambda:jsonify(success=True),methods=['POST'])
        install(app,self.g);self.http.fallback=True
        payload=dict(target_trader_id='owner',discount_percent=23.5,expires_at=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat(),title='Come back',message='A second opportunity',offer_code='RETURN2',delivery_email=True)
        result=app.test_client().post('/create_private_offer',json=payload);self.assertEqual(result.status_code,200,result.json)
        raw=self.http.rows['announcements'][-1];merged=self.g['_np_offer_merge_meta'](raw)
        self.assertEqual(merged['discount_percent'],23.5);self.assertEqual(merged['target_trader_id'],'owner')
        self.assertTrue(merged['expires_at'].endswith('+00:00'));self.g['send_email_brevo'].assert_called_once()
        self.g['send_email_brevo'].reset_mock();self.http.fail=True
        result=app.test_client().post('/create_private_offer',json=payload);self.assertNotEqual(result.status_code,200)
        self.g['send_email_brevo'].assert_not_called()

    def test_stipulated_deadline_stops_claim_quote_and_purchase_without_using_offer(self):
        deadline=datetime(2026,11,10,12,30,tzinfo=timezone.utc)
        self.offer['expires_at']=deadline.isoformat()
        self.g['_np_private_offer_expired']=lambda row:expired(row,deadline-timedelta(seconds=1))
        self.assertEqual(self.c.post('/claim_private_offer',json={'trader_id':'owner','offer_id':'offer'}).status_code,200)
        self.assertEqual(len(self.c.get('/trader_targeted_offers?trader_id=owner').json),1)
        with self.app.test_request_context():self.assertTrue(self.service.quote({'trader_id':'owner'},20000,'RETURN1')['valid'])
        self.g['_np_private_offer_expired']=lambda row:expired(row,deadline)
        self.assertEqual(self.c.post('/claim_private_offer',json={'trader_id':'owner','offer_id':'offer'}).status_code,404)
        for path in ['/trader_targeted_offers','/private_offers_for_trader']:self.assertEqual(self.c.get(path+'?trader_id=owner').json,[])
        with self.app.test_request_context():self.assertFalse(self.service.quote({'trader_id':'owner'},20000,'RETURN1')['valid'])
        self.assertEqual(self.c.post('/create_challenge_purchase',json={'trader_id':'owner','promo_code':'RETURN1'}).status_code,409)
        self.assertEqual(self.http.rows['challenge_purchases'],[])
        self.assertEqual(self.offer['status'],'active') # Deadline enforces expiry without waiting for a status-update job.

    def test_invalid_numeric_and_partial_identity_never_match(self):
        for n in [0,-1,100,True,'NaN','Infinity']:
            with self.assertRaises(ValueError):discount(n)
        self.assertFalse(matches({'target_email':'abc@example.test'},{'email':'xabc@example.test'}))
        self.assertFalse(matches(self.offer,{'id':'other','email':'owner@example.test'}))

if __name__=='__main__':unittest.main()
