"""Scheduled editorial promotion content; never changes trading or checkout rules."""
import copy,json,re,uuid
from PIL import Image,UnidentifiedImageError
from datetime import datetime,timezone
from urllib.parse import urlsplit
from flask import request,jsonify,send_file
from pathlib import Path
from production_controls import ControlsStore,ControlsUnavailable,Conflict
KEY='market_strip_v1';BUCKET='market-strip-v1';RELEASE='market-strip-20261125-v1'
TABS={'plans','purchase','packages','announcements','offers','support','referrals','accounts','mt5vault','payouts','timeline'}
FIELDS={'id','title','subtitle','description','badge','price_label','image','cta','destination','active','starts_at','ends_at'}
def stamp(v):
 if not v:return None
 d=datetime.fromisoformat(v.replace('Z','+00:00'))
 if d.tzinfo is None:raise ValueError('Promotion dates require a time zone')
 return d.astimezone(timezone.utc)
def defaults():
 items=[]
 for id,title,subtitle,description,badge,art,cta,tab in [
 ('capital','Your next move.','New possibilities start here.','Explore the current NairaPips challenge collection. Choose the account and journey that fit your next chapter.','THE COLLECTION','gold','Explore plans','plans'),
 ('insider','Stay one step ahead.','Fresh news. New perspectives.','Visit the latest NairaPips announcements for product news, platform updates and things worth knowing.','THE INSIDE TRACK','emerald','See what’s new','announcements'),
 ('possibility','Make room for more.','A different chapter awaits.','Discover the available challenge options and review the latest terms before choosing your next account.','DISCOVER MORE','violet','Explore challenges','purchase')]:
  items.append(dict(id=id,title=title,subtitle=subtitle,description=description,badge=badge,price_label='',image='/public/market-strip/art/'+art+'.svg',cta=cta,destination=tab,active=True,starts_at=None,ends_at=None))
 return dict(revision=0,enabled=True,items=items,updated_at=None,updated_by=None)
def valid_image(url):
 return bool(re.fullmatch(r'/public/market-strip/art/(gold|emerald|violet)\.svg',url)) or valid_https(url)
def valid_https(url):
 try:
  u=urlsplit(url);return u.scheme=='https' and bool(u.hostname) and not u.username and not u.password and not re.search(r'[\s\x00-\x1f]',url)
 except ValueError:return False
def validate(doc):
 if not isinstance(doc,dict) or set(doc)!={'revision','enabled','items','updated_at','updated_by'}:raise ValueError('Invalid Market Strip document')
 if type(doc['revision']) is not int or doc['revision']<0 or type(doc['enabled']) is not bool:raise ValueError('Reload Market Strip before saving')
 if not isinstance(doc['items'],list) or len(doc['items'])>20:raise ValueError('Use up to 20 products')
 ids=set()
 for item in doc['items']:
  if not isinstance(item,dict) or set(item)!=FIELDS:raise ValueError('Invalid product fields')
  for k,n in {'id':64,'title':70,'subtitle':110,'description':1400,'badge':30,'price_label':40,'image':2048,'cta':32,'destination':2048}.items():
   if not isinstance(item[k],str) or len(item[k])>n:raise ValueError('Check the '+k.replace('_',' ')+' field length')
  if not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}',item['id']) or item['id'] in ids:raise ValueError('Product identity is missing or duplicated')
  ids.add(item['id'])
  if not item['title'].strip() or not item['cta'].strip():raise ValueError('Add a product title and button label')
  if not valid_image(item['image']):raise ValueError('Choose artwork, upload an image, or use an HTTPS image URL')
  if item['destination'] not in TABS and not valid_https(item['destination']):raise ValueError('Choose a dashboard destination or HTTPS link')
  if type(item['active']) is not bool:raise ValueError('Use the visibility checkbox')
  try:
   start,end=stamp(item['starts_at']),stamp(item['ends_at'])
   if start and end and end<=start:raise ValueError('The end date must be after the start date')
  except (TypeError,AttributeError):raise ValueError('Choose valid promotion dates')
 return copy.deepcopy(doc)
def public_items(doc,now=None):
 now=now or datetime.now(timezone.utc)
 if not doc['enabled']:return []
 return [{k:v for k,v in x.items() if k not in {'active','starts_at','ends_at'}} for x in doc['items'] if x['active'] and (not stamp(x['starts_at']) or stamp(x['starts_at'])<=now) and (not stamp(x['ends_at']) or now<stamp(x['ends_at']))]
class MarketStore(ControlsStore):
 def __init__(self,ns):
  def checked(doc):
   try:return validate(doc)
   except ValueError as e:raise ControlsUnavailable('Market Strip settings could not be verified') from e
  super().__init__(ns['requests'],ns['SUPABASE_URL'],ns.get('SUPABASE_SERVICE_ROLE_KEY') or '',KEY,defaults,checked)
  self.url=ns['SUPABASE_URL'].rstrip('/')+'/rest/v1/package_studio_private_settings'
def install(app,ns,store=None):
 store=store or MarketStore(ns);assets=Path(__file__).parent/'website-downloads'/'market-art'
 def reply(data,status=200):
  r=jsonify(data);r.status_code=status;r.headers['Cache-Control']='no-store';return r
 def admin(write=False):
  actor,error=ns['_require_admin']()
  if error is not None:return None,error
  from staff_access import owner,permissions,LEVELS
  level=LEVELS.get(permissions(actor.get('permissions')).get('marketStrip'),0)
  if not owner(actor) and level<(2 if write else 1):return None,(jsonify(success=False,error='Market Strip access is required'),403)
  return actor,None
 @app.get('/public/market-strip')
 def np_market_strip_public():
  try:
   doc,_=store.read();return reply(dict(success=True,release=RELEASE,items=public_items(doc)))
  except ControlsUnavailable:return reply(dict(success=False,error='Market Strip is temporarily unavailable',items=[]),503)
 @app.get('/public/market-strip/art/<name>')
 def np_market_strip_art(name):
  if name not in {'gold.svg','emerald.svg','violet.svg'}:return reply(dict(success=False),404)
  r=send_file(assets/name,mimetype='image/svg+xml',max_age=86400);r.headers['X-Content-Type-Options']='nosniff';return r
 @app.route('/admin/market-strip',methods=['GET','POST'])
 def np_market_strip_admin():
  actor,error=admin(request.method=='POST')
  if error is not None:return error
  try:
   doc,raw=store.read(fresh=True)
   if request.method=='GET':return reply(dict(success=True,document=doc))
   body=request.get_json(silent=True)
   if not isinstance(body,dict) or set(body)!={'revision','enabled','items'}:raise ValueError('Reload the editor and save the complete strip')
   if body['revision']!=doc['revision'] or type(body['revision']) is not int:raise Conflict('Another session changed Market Strip. Reload before saving.')
   new=validate({**body,'revision':body['revision']+1,'updated_at':datetime.now(timezone.utc).isoformat(),'updated_by':str(actor.get('id') or 'owner')})
   saved=store.persist(new,raw,actor.get('id'));return reply(dict(success=True,document=saved))
  except Conflict as e:return reply(dict(success=False,error=str(e)),409)
  except ValueError as e:return reply(dict(success=False,error=str(e)),400)
  except ControlsUnavailable as e:return reply(dict(success=False,error=str(e)),503)
 @app.post('/admin/market-strip/image')
 def np_market_strip_image():
  _,error=admin(True)
  if error is not None:return error
  if not request.content_length or request.content_length>4*1024*1024:return reply(dict(success=False,error='Choose a JPG, PNG or WebP below 3 MB'),413)
  try:
   import io
   file=request.files.get('image')
   if not file:raise ValueError('Choose an image')
   data=file.read(3*1024*1024+1)
   if len(data)>3*1024*1024:raise ValueError('Choose an image below 3 MB')
   with Image.open(io.BytesIO(data)) as image:
    if image.format not in {'JPEG','PNG','WEBP'}:raise ValueError('Unsupported image format')
    if image.width*image.height>20000000:raise ValueError('Choose an image under 20 megapixels')
    image.load();image=image.convert('RGB');image.thumbnail((1600,1200));out=io.BytesIO();image.save(out,'WEBP',quality=88);data=out.getvalue()
   key=ns.get('SUPABASE_SERVICE_ROLE_KEY')
   if not key:raise ControlsUnavailable('Image storage is unavailable')
   http=ns['requests'];base=ns['SUPABASE_URL'].rstrip('/')+'/storage/v1';headers={'apikey':key,'Authorization':'Bearer '+key}
   check=http.get(base+'/bucket/'+BUCKET,headers=headers,timeout=(2,5))
   if check.status_code==404:
    made=http.post(base+'/bucket',headers=headers,json={'id':BUCKET,'name':BUCKET,'public':True,'file_size_limit':3145728,'allowed_mime_types':['image/webp']},timeout=(2,6))
    if made.status_code!=409:made.raise_for_status()
   else:check.raise_for_status()
   path=uuid.uuid4().hex+'.webp';r=http.post(base+'/object/'+BUCKET+'/'+path,headers=dict(headers,**{'Content-Type':'image/webp','x-upsert':'false','cache-control':'31536000'}),data=data,timeout=(2,12));r.raise_for_status()
   return reply(dict(success=True,url=base+'/object/public/'+BUCKET+'/'+path))
  except (ValueError,UnidentifiedImageError,Image.DecompressionBombError) as e:return reply(dict(success=False,error='Choose a valid JPG, PNG or WebP image'),400)
  except Exception:return reply(dict(success=False,error='The image upload could not be confirmed. Please retry.'),503)
 app.config['NP_MARKET_STRIP']=store
