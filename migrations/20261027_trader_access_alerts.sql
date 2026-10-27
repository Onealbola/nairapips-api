BEGIN;
CREATE OR REPLACE FUNCTION public.np_trader_access_alerts(p_days integer DEFAULT 30)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $f$
DECLARE result jsonb; cutoff timestamptz:=now()-make_interval(days=>greatest(1,least(coalesce(p_days,30),90)));
BEGIN
 WITH banned AS (
  SELECT DISTINCT t.id,coalesce(public.np_security_parse_ip(to_jsonb(t)->>'registration_ip'),public.np_security_parse_ip(to_jsonb(t)->>'ip_address')) AS historical_ip
  FROM public.black_book_entries b JOIN public.traders t ON t.id=b.trader_id WHERE b.status='restricted'
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
NOTIFY pgrst,'reload schema';
COMMIT;
