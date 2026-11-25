import copy,io,unittest
from datetime import datetime,timezone
from flask import Flask,jsonify
from PIL import Image
from market_strip import defaults,validate,public_items,install,ControlsUnavailable,Conflict
from staff_access import module_for
class Store:
 def __init__(self):self.doc=defaults();self.fail=False;self.writes=0
 def read(self,**kw):
  if self.fail:raise ControlsUnavailable('Unavailable')
  return copy.deepcopy(self.doc),'token'
 def persist(self,new,raw,actor):self.doc=copy.deepcopy(new);self.writes+=1;return new
class MarketTests(unittest.TestCase):
 def setUp(self):
  self.store=Store();self.actor={'id':'owner','role':'owner'};self.app=Flask(__name__)
  def admin():return (self.actor,None) if self.actor else (None,(jsonify(success=False),401))
  self.ns={'_require_admin':admin,'SUPABASE_URL':'https://test.invalid','SUPABASE_SERVICE_ROLE_KEY':None};install(self.app,self.ns,self.store);self.c=self.app.test_client()
 def body(self):return {k:copy.deepcopy(self.store.doc[k]) for k in ['revision','enabled','items']}
 def test_public_and_schedule(self):
  d=defaults();now=datetime(2026,11,25,tzinfo=timezone.utc)
  d['items'][0].update(starts_at='2026-11-25T00:00:00Z',ends_at='2026-11-26T00:00:00Z');d['items'][1]['active']=False;d['items'][2]['starts_at']='2026-11-26T00:00:00Z'
  self.assertEqual(len(public_items(d,now)),1);self.assertNotIn('starts_at',public_items(d,now)[0]);self.assertEqual(len(public_items(d,datetime(2026,11,26,tzinfo=timezone.utc))),1)
  d['enabled']=False;self.assertEqual(public_items(d,now),[])
  r=self.c.get('/public/market-strip');self.assertEqual(r.status_code,200);self.assertNotIn('updated_by',r.get_json());self.assertEqual(r.headers['Cache-Control'],'no-store')
 def test_auth_and_levels(self):
  self.actor=None;self.assertEqual(self.c.get('/admin/market-strip').status_code,401)
  self.actor={'id':'staff','role':'staff','permissions':{'marketStrip':'view'}};self.assertEqual(self.c.get('/admin/market-strip').status_code,200);self.assertEqual(self.c.post('/admin/market-strip',json=self.body()).status_code,403)
  self.actor['permissions']['marketStrip']='edit';self.assertEqual(self.c.post('/admin/market-strip',json=self.body()).status_code,200)
  self.actor['permissions']={};self.assertEqual(self.c.get('/admin/market-strip').status_code,403);self.assertEqual(module_for('/admin/market-strip/image'),'marketStrip')
 def test_save_revision_conflict(self):
  b=self.body();b['items'][0]['title']='A new chapter';r=self.c.post('/admin/market-strip',json=b);self.assertEqual(r.status_code,200);self.assertEqual(self.store.doc['revision'],1)
  self.assertEqual(self.c.post('/admin/market-strip',json=b).status_code,409);self.assertEqual(self.store.writes,1)
 def test_validation(self):
  for field,value in [('destination','javascript:alert(1)'),('image','data:image/svg+xml,bad'),('destination','https://user:pass@example.com'),('image','http://example.com/a.png'),('ends_at','not-a-date'),('active','true')]:
   with self.subTest(field=field,value=value):
    b=self.body();b['items'][0][field]=value;self.assertEqual(self.c.post('/admin/market-strip',json=b).status_code,400)
  b=self.body();b['items'][0].update(starts_at='2026-11-25T12:00:00',ends_at=None);self.assertEqual(self.c.post('/admin/market-strip',json=b).status_code,400)
  b=self.body();b['items'][0]['surprise']='x';self.assertEqual(self.c.post('/admin/market-strip',json=b).status_code,400)
  self.assertEqual(self.store.writes,0)
 def test_store_failure(self):
  self.store.fail=True;r=self.c.get('/public/market-strip');self.assertEqual(r.status_code,503);self.assertEqual(r.get_json()['items'],[])
 def test_art_and_bad_upload(self):
  r=self.c.get('/public/market-strip/art/gold.svg');self.assertEqual(r.status_code,200);r.close();self.assertEqual(self.c.get('/public/market-strip/art/bad.svg').status_code,404)
  r=self.c.post('/admin/market-strip/image',data={'image':(io.BytesIO(b'not-image'),'x.png')});self.assertEqual(r.status_code,400)
  self.actor={'id':'view','role':'staff','permissions':{'marketStrip':'view'}};self.assertEqual(self.c.post('/admin/market-strip/image',data={'image':(io.BytesIO(b'not-image'),'x.png')}).status_code,403)
 def test_upload_reencodes_image_and_uses_unique_storage_key(self):
  class Response:
   def __init__(self,status):self.status_code=status
   def raise_for_status(self):
    if self.status_code>=400:raise RuntimeError('HTTP error')
  class Transport:
   def __init__(self):self.posts=[]
   def get(self,*args,**kw):return Response(404)
   def post(self,url,**kw):self.posts.append((url,kw));return Response(201)
  http=Transport();self.ns.update(requests=http,SUPABASE_SERVICE_ROLE_KEY='fixture-key')
  image=io.BytesIO();Image.new('RGB',(100,100),'gold').save(image,'PNG');image.seek(0)
  r=self.c.post('/admin/market-strip/image',data={'image':(image,'original.png')})
  self.assertEqual(r.status_code,200,r.get_json());self.assertTrue(r.get_json()['url'].endswith('.webp'))
  self.assertEqual(http.posts[0][1]['json']['allowed_mime_types'],['image/webp'])
  final=http.posts[1][1];self.assertEqual(final['headers']['x-upsert'],'false')
  with Image.open(io.BytesIO(final['data'])) as im:self.assertEqual(im.format,'WEBP')
if __name__=='__main__':unittest.main()
