BEGIN;
CREATE TABLE IF NOT EXISTS public.trader_access_events (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 trader_id uuid NOT NULL REFERENCES public.traders(id) ON DELETE CASCADE,
 event_type text NOT NULL CHECK(event_type IN ('registration','login')),
 ip_address inet NOT NULL,
 ip_source text NOT NULL CHECK(ip_source IN ('direct_peer','trusted_proxy_chain')),
 user_agent text NOT NULL DEFAULT '' CHECK(length(user_agent)<=512),
 observed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS trader_access_owner_time ON public.trader_access_events(trader_id,observed_at DESC);
CREATE INDEX IF NOT EXISTS trader_access_ip_time ON public.trader_access_events(ip_address,observed_at DESC);
CREATE INDEX IF NOT EXISTS trader_access_retention ON public.trader_access_events(observed_at);
ALTER TABLE public.trader_access_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.trader_access_events FORCE ROW LEVEL SECURITY;
REVOKE ALL ON public.trader_access_events FROM PUBLIC,anon,authenticated,service_role;
GRANT SELECT,INSERT,DELETE ON public.trader_access_events TO service_role;
CREATE OR REPLACE FUNCTION public.np_security_parse_ip(value text) RETURNS inet
LANGUAGE plpgsql IMMUTABLE SET search_path=public,pg_temp AS $f$
BEGIN RETURN nullif(btrim(value),'')::inet; EXCEPTION WHEN OTHERS THEN RETURN NULL; END $f$;
REVOKE ALL ON FUNCTION public.np_security_parse_ip(text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.np_security_parse_ip(text) TO service_role;
CREATE OR REPLACE FUNCTION public.np_trader_access_review(p_trader_id uuid,p_days integer DEFAULT 30)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $f$
DECLARE result jsonb; cutoff timestamptz:=now()-make_interval(days=>greatest(1,least(coalesce(p_days,30),90)));
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.traders WHERE id=p_trader_id) THEN RAISE EXCEPTION 'Trader not found'; END IF;
 WITH mine AS (
  SELECT ip_address,min(observed_at) AS first_seen,max(observed_at) AS last_seen
  FROM public.trader_access_events WHERE trader_id=p_trader_id AND observed_at>=cutoff GROUP BY ip_address
 ), banned AS (
  SELECT DISTINCT t.id,t.name,t.email,coalesce(public.np_security_parse_ip(to_jsonb(t)->>'registration_ip'),public.np_security_parse_ip(to_jsonb(t)->>'ip_address')) AS historical_ip
  FROM public.black_book_entries b JOIN public.traders t ON t.id::text=btrim(lower(b.trader_id::text)) WHERE b.status='restricted' AND t.id<>p_trader_id
 ), matches AS (
  SELECT b.id AS banned_trader_id,b.name AS banned_name,b.email AS banned_email,host(m.ip_address) AS ip,m.first_seen,m.last_seen,
   max(e.observed_at) AS banned_last_seen,'Recorded access IP match'::text AS evidence
  FROM mine m JOIN public.trader_access_events e ON e.ip_address=m.ip_address AND e.observed_at>=cutoff
  JOIN banned b ON b.id=e.trader_id GROUP BY b.id,b.name,b.email,m.ip_address,m.first_seen,m.last_seen
  UNION ALL
  SELECT b.id,b.name,b.email,host(m.ip_address),m.first_seen,m.last_seen,NULL::timestamptz,
   'Historical registration IP match — older proxy provenance unverified'::text
  FROM mine m JOIN banned b ON b.historical_ip=m.ip_address
  WHERE NOT EXISTS(SELECT 1 FROM public.trader_access_events e WHERE e.trader_id=b.id AND e.ip_address=m.ip_address AND e.observed_at>=cutoff)
 ), recent AS (
  SELECT id,event_type,host(ip_address) AS ip,ip_source,user_agent,observed_at
  FROM public.trader_access_events WHERE trader_id=p_trader_id AND observed_at>=cutoff ORDER BY observed_at DESC,id DESC LIMIT 100
 )
 SELECT jsonb_build_object(
  'trader',(SELECT jsonb_build_object('id',id,'name',name,'email',email) FROM public.traders WHERE id=p_trader_id),
  'since',cutoff,'generated_at',now(),'retention_days',90,
  'events',coalesce((SELECT jsonb_agg(to_jsonb(r) ORDER BY observed_at DESC) FROM recent r),'[]'::jsonb),
  'events_total',(SELECT count(*) FROM public.trader_access_events WHERE trader_id=p_trader_id AND observed_at>=cutoff),
  'matches',coalesce((SELECT jsonb_agg(to_jsonb(x)) FROM (SELECT * FROM matches ORDER BY last_seen DESC LIMIT 100) x),'[]'::jsonb),
  'matches_total',(SELECT count(*) FROM matches),
  'review_required',true,'automatic_ban',false
 ) INTO result;
 RETURN result;
END $f$;
REVOKE ALL ON FUNCTION public.np_trader_access_review(uuid,integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.np_trader_access_review(uuid,integer) TO service_role;
CREATE OR REPLACE FUNCTION public.np_trader_access_alerts(p_days integer DEFAULT 30)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $f$
DECLARE result jsonb; cutoff timestamptz:=now()-make_interval(days=>greatest(1,least(coalesce(p_days,30),90)));
BEGIN
 WITH banned AS (
  SELECT DISTINCT t.id,coalesce(public.np_security_parse_ip(to_jsonb(t)->>'registration_ip'),public.np_security_parse_ip(to_jsonb(t)->>'ip_address')) AS historical_ip
  FROM public.black_book_entries b JOIN public.traders t ON t.id::text=btrim(lower(b.trader_id::text)) WHERE b.status='restricted'
 ), banned_ips AS (
  SELECT b.id AS banned_id,e.ip_address FROM banned b JOIN public.trader_access_events e ON e.trader_id=b.id AND e.observed_at>=cutoff
  UNION SELECT id,historical_ip FROM banned WHERE historical_ip IS NOT NULL
 ), links AS (
  SELECT e.trader_id,e.ip_address,b.banned_id,max(e.observed_at) AS last_seen
  FROM public.trader_access_events e JOIN banned_ips b ON b.ip_address=e.ip_address AND b.banned_id<>e.trader_id
  WHERE e.observed_at>=cutoff AND NOT EXISTS(SELECT 1 FROM banned x WHERE x.id=e.trader_id)
  GROUP BY e.trader_id,e.ip_address,b.banned_id
 ), alerts AS (
  SELECT t.id AS trader_id,t.name,t.email,max(l.last_seen) AS last_seen,count(DISTINCT l.banned_id) AS matched_banned_accounts,
   jsonb_agg(DISTINCT host(l.ip_address)) AS shared_ips
  FROM links l JOIN public.traders t ON t.id=l.trader_id GROUP BY t.id,t.name,t.email
 )
 SELECT jsonb_build_object('generated_at',now(),'since',cutoff,'total',(SELECT count(*) FROM alerts),
  'alerts',coalesce((SELECT jsonb_agg(to_jsonb(a)) FROM (SELECT * FROM alerts ORDER BY last_seen DESC LIMIT 100) a),'[]'::jsonb),'automatic_ban',false)
 INTO result;
 RETURN result;
END $f$;
REVOKE ALL ON FUNCTION public.np_trader_access_alerts(integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.np_trader_access_alerts(integer) TO service_role;
COMMENT ON TABLE public.trader_access_events IS 'Server-recorded successful trader login/registration observations. Staff review only; IP matches are not identity proof. Application retention: 90 days.';
NOTIFY pgrst,'reload schema';
COMMIT;
