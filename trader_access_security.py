"""Staff-only account-link review, separate from financial and restriction decisions."""
import ipaddress,os,queue,threading,time,uuid
from datetime import datetime,timedelta,timezone
from flask import request,jsonify
ROLES={'super_admin','admin','admin_manager'}
TRUSTED_DEFAULT='127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16'

def parse_ip(value):
    try:
        value=str(value or '').strip()
        if '%' in value or '/' in value:return None
        ip=ipaddress.ip_address(value)
        return ip.ipv4_mapped if isinstance(ip,ipaddress.IPv6Address) and ip.ipv4_mapped else ip
    except ValueError:return None

def client_ip(peer,forwarded,networks):
    peer=parse_ip(peer)
    if peer is None:return None,None
    trusted=lambda ip:any(ip in network for network in networks if ip.version==network.version)
    if not trusted(peer):return (str(peer),'direct_peer') if peer.is_global else (None,None)
    parts=str(forwarded or '').split(',')
    if not forwarded or len(parts)>32 or len(str(forwarded))>2048:return None,None
    for raw in reversed(parts):
        ip=parse_ip(raw)
        if ip is None:return None,None
        if not trusted(ip):return (str(ip),'trusted_proxy_chain') if ip.is_global else (None,None)
    return None,None

class SecurityLog:
    def __init__(self,http,url,key):
        self.http=http;self.base=url.rstrip('/')+'/rest/v1/';self.headers={'apikey':key,'Authorization':'Bearer '+key}
        self.queue=queue.Queue(maxsize=128);self.lock=threading.Lock();self.running=False;self.last_error='';self.last_saved=None;self.dropped=0;self.last_purge=0
    def put(self,event):
        try:self.queue.put_nowait(event)
        except queue.Full:self.dropped+=1;self.last_error='Security logging queue is full';return
        with self.lock:
            if not self.running:
                self.running=True
                try:threading.Thread(target=self.drain,name='np-security-events',daemon=True).start()
                except Exception:self.running=False;self.last_error='Security logging worker unavailable'
    def drain(self):
        while True:
            try:event=self.queue.get(timeout=2)
            except queue.Empty:
                with self.lock:
                    if self.queue.empty():self.running=False;return
                continue
            try:
                r=self.http.post(self.base+'trader_access_events',headers={**self.headers,'Prefer':'return=minimal'},json=event,timeout=(2,5));r.raise_for_status()
                self.last_saved=event['observed_at'];self.last_error=''
                if time.monotonic()-self.last_purge>21600:
                    cutoff=(datetime.now(timezone.utc)-timedelta(days=90)).isoformat()
                    r=self.http.delete(self.base+'trader_access_events',headers=self.headers,params={'observed_at':'lt.'+cutoff},timeout=(2,5));r.raise_for_status();self.last_purge=time.monotonic()
            except Exception:self.dropped+=1;self.last_error='Access history could not be saved. Check database setup and connectivity.'
            finally:self.queue.task_done()
    def ready(self):
        r=self.http.get(self.base+'trader_access_events',headers=self.headers,params={'select':'id','limit':'1'},timeout=(2,5));r.raise_for_status()


def register_trader_access_security(app,runtime):
    store=SecurityLog(runtime['requests'],runtime['SUPABASE_URL'],runtime.get('SUPABASE_SERVICE_ROLE_KEY') or runtime['SUPABASE_KEY'])
    app._np_security_log=store
    networks=[ipaddress.ip_network(n.strip()) for n in os.getenv('SECURITY_TRUSTED_PROXY_CIDRS',TRUSTED_DEFAULT).split(',') if n.strip()]
    def require_staff():
        admin,error=runtime['_require_admin']()
        if error is not None:return None,error
        if str((admin or {}).get('role') or '').lower() not in ROLES:return None,(jsonify(success=False,error='Security review is restricted to authorised admin roles.'),403)
        return admin,None
    def reply(data,status=200):
        response=jsonify(data);response.status_code=status;response.headers['Cache-Control']='no-store, private';response.headers['Vary']='Authorization';return response
    @app.after_request
    def observe_access(response):
        if request.method!='POST' or request.path not in {'/login_trader','/register_trader'} or response.status_code>=300:return response
        try:
            payload=response.get_json(silent=True)
            if not isinstance(payload,dict) or payload.get('success') is not True:return response
            def token(data,depth=0):
                if not isinstance(data,dict) or depth>3:return ''
                for key in ('auth_token','token','trader_auth_token'):
                    if isinstance(data.get(key),str) and data[key]:return data[key]
                return token(data.get('data'),depth+1) or token(data.get('trader'),depth+1)
            tid=runtime['_trader_id_from_auth_token'](token(payload))
            if not tid:return response
            ip,source=client_ip(request.remote_addr,request.headers.get('X-Forwarded-For'),networks)
            if not ip:store.last_error='Client IP could not be verified from the proxy chain';return response
            agent=''.join(c for c in request.headers.get('User-Agent','') if c.isprintable())[:512]
            store.put({'id':str(uuid.uuid4()),'trader_id':str(uuid.UUID(tid)),'event_type':'login' if request.path=='/login_trader' else 'registration','ip_address':ip,'ip_source':source,'user_agent':agent,'observed_at':datetime.now(timezone.utc).isoformat()})
        except Exception:store.last_error='Access observation unavailable'
        return response
    @app.get('/admin/trader_access/status')
    def access_status():
        _,error=require_staff()
        if error is not None:return error
        try:store.ready();ready=True
        except Exception:ready=False
        return reply({'success':True,'ready':ready,'last_saved':store.last_saved,'pending':store.queue.qsize(),'unrecorded_events':store.dropped,'warning':store.last_error,'retention_days':90,'automatic_ban':False,'setup_required':not ready})
    @app.get('/admin/trader_access/search')
    def access_search():
        _,error=require_staff()
        if error is not None:return error
        query=''.join(c for c in request.args.get('q','').strip() if c.isalnum() or c in '@._+- ')[:100]
        if len(query)<2:return reply({'success':False,'error':'Enter at least two characters.'},400)
        try:
            r=store.http.get(store.base+'traders',headers=store.headers,params={'select':'id,name,email','or':f'(name.ilike.*{query}*,email.ilike.*{query}*)','limit':'25'},timeout=(2,5));r.raise_for_status()
            return reply({'success':True,'traders':r.json()})
        except Exception:return reply({'success':False,'error':'Trader search is unavailable.'},503)
    @app.get('/admin/trader_access/alerts')
    def access_alerts():
        _,error=require_staff()
        if error is not None:return error
        try:
            days=int(request.args.get('days',30))
            if days not in (7,30,90):raise ValueError()
        except (ValueError,TypeError):return reply({'success':False,'error':'Choose a 7, 30 or 90 day period.'},400)
        try:
            r=store.http.post(store.base+'rpc/np_trader_access_alerts',headers=store.headers,json={'p_days':days},timeout=(2,8));r.raise_for_status();data=r.json()
            if not isinstance(data,dict) or 'alerts' not in data:raise ValueError()
            return reply({'success':True,'data':data})
        except Exception:return reply({'success':False,'error':'Automatic review list is unavailable. Confirm the latest SQL setup is installed.'},503)
    @app.get('/admin/trader_access/review')
    def access_review():
        admin,error=require_staff()
        if error is not None:return error
        try:
            tid=str(uuid.UUID(request.args.get('trader_id','')));days=int(request.args.get('days',30))
            if days not in (7,30,90):raise ValueError()
        except (ValueError,TypeError):return reply({'success':False,'error':'Choose a trader and a 7, 30 or 90 day period.'},400)
        try:
            r=store.http.post(store.base+'rpc/np_trader_access_review',headers=store.headers,json={'p_trader_id':tid,'p_days':days},timeout=(2,8));r.raise_for_status();data=r.json()
            if not isinstance(data,dict) or 'events' not in data or 'matches' not in data:raise ValueError()
            try:runtime['_audit_safe']('security','view_access_review','Viewed account-link evidence for trader '+tid,admin,tid)
            except Exception:pass
            return reply({'success':True,'data':data})
        except Exception:return reply({'success':False,'error':'Security history is unavailable. Confirm database setup before relying on this review.'},503)
