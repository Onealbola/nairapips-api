"""Bound DNS for the configured database host, preserving normal HTTP/TLS identity.

Socket connect/read timeouts do not cover libc getaddrinfo. An isolated, short
stdlib resolver process can be terminated; an abandoned Python thread cannot.
Only the configured database hostname is handled here. Other hosts retain the
original resolver. No IP is pinned and expired resolutions are never reused.
"""
import json
import socket
import subprocess
import sys
from threading import Lock
from time import monotonic
from urllib.parse import urlparse

DNS_TIMEOUT_SECONDS = 2
DNS_CACHE_SECONDS = 60
_RESOLVE = '''import json,socket,sys
p=json.loads(sys.stdin.read(4096))
try:
 rows=socket.getaddrinfo(**p)
 print(json.dumps({'rows':rows}))
except OSError as e:
 print(json.dumps({'errno':e.errno,'error':str(e)}))
'''

class DatabaseDNSGuard:
    def __init__(self, hostname, original, timeout=DNS_TIMEOUT_SECONDS, ttl=DNS_CACHE_SECONDS, runner=None):
        self.hostname=hostname.lower().rstrip('.')
        self.original=original
        self.timeout=timeout
        self.ttl=ttl
        self.runner=runner or subprocess.run
        self.lock=Lock()
        self.cache={}
        self.last_failure=None
        self.lookups=0

    def __call__(self, host, port, family=0, type=0, proto=0, flags=0):
        normalized=host.decode('ascii') if isinstance(host,bytes) else host
        if not isinstance(normalized,str) or normalized.lower().rstrip('.')!=self.hostname:
            return self.original(host,port,family,type,proto,flags)
        key=(normalized,port,family,type,proto,flags)
        now=monotonic()
        cached=self.cache.get(key)
        if cached and cached[0]>now:return list(cached[1])
        deadline=now+self.timeout
        if not self.lock.acquire(timeout=max(0,deadline-monotonic())):
            raise socket.gaierror(socket.EAI_AGAIN,'Database DNS lookup deadline exceeded')
        try:
            cached=self.cache.get(key)
            if cached and cached[0]>monotonic():return list(cached[1])
            remaining=deadline-monotonic()
            if remaining<=0:raise socket.gaierror(socket.EAI_AGAIN,'Database DNS lookup deadline exceeded')
            # Resolve the configured public hostname as an absolute DNS name.
            # Container search domains/ndots must not multiply this lookup into
            # a long sequence of irrelevant internal-domain requests.
            query_host=normalized.rstrip('.')+'.' if '.' in normalized else normalized
            payload=dict(host=query_host,port=port,family=int(family),type=int(type),proto=int(proto),flags=int(flags))
            try:
                self.lookups+=1
                result=self.runner([sys.executable,'-c',_RESOLVE],input=json.dumps(payload),
                    capture_output=True,text=True,timeout=remaining,check=True)
                answer=json.loads(result.stdout)
            except subprocess.TimeoutExpired as exc:
                self.last_failure={'kind':'deadline','seconds':self.timeout}
                raise socket.gaierror(socket.EAI_AGAIN,'Database DNS lookup deadline exceeded') from exc
            except (subprocess.SubprocessError,ValueError) as exc:
                self.last_failure={'kind':'resolver_process','type':type(exc).__name__}
                raise socket.gaierror(socket.EAI_AGAIN,'Database DNS lookup failed') from exc
            if 'rows' not in answer:
                self.last_failure={'kind':'resolver_error','errno':answer.get('errno')}
                raise socket.gaierror(answer.get('errno') or socket.EAI_AGAIN,'Database DNS lookup failed')
            rows=[(int(r[0]),int(r[1]),int(r[2]),str(r[3]),tuple(r[4])) for r in answer['rows']]
            if not rows:raise socket.gaierror(socket.EAI_NONAME,'Database DNS lookup returned no addresses')
            # DNS still supplies every address in normal resolver order; the HTTP
            # clients retain their original hostname, certificate checks and SNI.
            self.cache={k:v for k,v in self.cache.items() if v[0]>monotonic()}
            self.cache[key]=(monotonic()+self.ttl,tuple(rows))
            self.last_failure=None
            return rows
        finally:
            self.lock.release()


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
    return {'configured':True,'lookup_count':guard.lookups,
        'cached_queries':sum(1 for deadline,_ in guard.cache.values() if deadline>monotonic()),
        'last_failure':guard.last_failure}
