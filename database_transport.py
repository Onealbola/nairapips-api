"""Bounded HTTP/1.1 SDK transport; avoid shared HTTP/2 stream receive locks."""
import httpx
from supabase import create_client,ClientOptions

def create_database_client(url,key,*,transport=None):
    # Explicit auth headers are required when supplying an SDK-owned HTTP client.
    # Retain the original key for each client: public and service roles stay separate.
    http=httpx.Client(http2=False,follow_redirects=True,
        timeout=httpx.Timeout(8,connect=2,write=8,pool=2),
        limits=httpx.Limits(max_connections=20,max_keepalive_connections=8,keepalive_expiry=15),
        headers={'apikey':key,'Authorization':'Bearer '+key},transport=transport)
    return create_client(url,key,options=ClientOptions(httpx_client=http,postgrest_client_timeout=8))
