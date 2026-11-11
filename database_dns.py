"""Bound database DNS waits without changing HTTP identity or other hosts.

A small daemon resolver pool lets slow OS lookups finish in the background.
HTTP callers wait at most two seconds; later requests reuse a completed result.
Pending work is shared and bounded, not abandoned or multiplied on every retry.
"""
import socket
from threading import Lock,Event,Thread
from time import monotonic
from urllib.parse import urlparse

DNS_TIMEOUT_SECONDS=2
DNS_CACHE_SECONDS=60
MAX_PENDING_LOOKUPS=4

class DatabaseDNSGuard:
    def __init__(self,hostname,original,timeout=DNS_TIMEOUT_SECONDS,ttl=DNS_CACHE_SECONDS):
        self.hostname=hostname.lower().rstrip('.')
        self.original=original
        self.timeout=timeout
        self.ttl=ttl
        self.lock=Lock()
        self.cache={}
        self.pending={}
        self.last_failure=None
        self.lookups=0

    def _resolve(self,key,host,port,family,type,proto,flags,task):
        try:
            query=host.rstrip('.')+'.' if '.' in host else host
            rows=self.original(query,port,family,type,proto,flags)
            if not rows:raise socket.gaierror(socket.EAI_NONAME,'Database DNS lookup returned no addresses')
            task['rows']=tuple(rows)
            with self.lock:
                self.cache={k:v for k,v in self.cache.items() if v[0]>monotonic()}
                self.cache[key]=(monotonic()+self.ttl,task['rows'])
                self.last_failure=None
        except Exception as exc:
            task['error']=getattr(exc,'errno',None) or socket.EAI_AGAIN
            with self.lock:self.last_failure={'kind':'resolver_error','errno':task['error']}
        finally:
            with self.lock:self.pending.pop(key,None)
            task['event'].set()

    def __call__(self,host,port,family=0,type=0,proto=0,flags=0):
        try:normalized=host.decode('ascii') if isinstance(host,bytes) else host
        except UnicodeError:return self.original(host,port,family,type,proto,flags)
        if not isinstance(normalized,str) or normalized.lower().rstrip('.')!=self.hostname:
            return self.original(host,port,family,type,proto,flags)
        key=(normalized,port,family,type,proto,flags)
        deadline=monotonic()+self.timeout
        cached=self.cache.get(key)
        if cached and cached[0]>monotonic():return list(cached[1])
        if not self.lock.acquire(timeout=max(0,deadline-monotonic())):
            raise socket.gaierror(socket.EAI_AGAIN,'Database DNS lookup deadline exceeded')
        start=False
        try:
            cached=self.cache.get(key)
            if cached and cached[0]>monotonic():return list(cached[1])
            task=self.pending.get(key)
            if task is None:
                if len(self.pending)>=MAX_PENDING_LOOKUPS:
                    raise socket.gaierror(socket.EAI_AGAIN,'Database DNS resolver is busy')
                task={'event':Event(),'rows':None,'error':None}
                self.pending[key]=task
                self.lookups+=1
                start=True
        finally:self.lock.release()
        if start:
            try:Thread(target=self._resolve,args=(key,normalized,port,family,type,proto,flags,task),name='database-dns',daemon=True).start()
            except Exception as exc:
                with self.lock:self.pending.pop(key,None)
                task['error']=socket.EAI_AGAIN;task['event'].set()
                raise socket.gaierror(socket.EAI_AGAIN,'Database DNS resolver could not start') from exc
        if not task['event'].wait(timeout=max(0,deadline-monotonic())):
            self.last_failure={'kind':'caller_deadline','seconds':self.timeout}
            raise socket.gaierror(socket.EAI_AGAIN,'Database DNS lookup deadline exceeded')
        if task['error'] is not None:raise socket.gaierror(task['error'],'Database DNS lookup failed')
        return list(task['rows'])


def install_database_dns_guard(url):
    host=urlparse(url).hostname
    if not host:raise ValueError('Database URL must contain a hostname')
    original=socket.getaddrinfo
    if isinstance(original,DatabaseDNSGuard):
        if original.hostname!=host.lower().rstrip('.'):
            raise ValueError('Database DNS guard is already configured for another host')
        return original
    guard=DatabaseDNSGuard(host,original)
    socket.getaddrinfo=guard
    return guard


def database_dns_status():
    guard=socket.getaddrinfo
    if not isinstance(guard,DatabaseDNSGuard):return {'configured':False}
    return {'configured':True,'lookup_count':guard.lookups,'pending_lookups':len(guard.pending),
        'cached_queries':sum(1 for deadline,_ in guard.cache.values() if deadline>monotonic()),
        'last_failure':guard.last_failure}
