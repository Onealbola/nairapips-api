-- Fix only the invalid text/numeric expression in the existing snapshot trigger.
-- No purchase, account, payout, price, availability or limit is changed here.
BEGIN;
DO $repair$
DECLARE body text; broken text; fixed text;
BEGIN
 body := pg_get_functiondef('public.np_preserve_sold_plan_terms()'::regprocedure);
 broken := 'CASE WHEN OLD.payout_split>=0.01 AND OLD.payout_split<=100 THEN OLD.payout_split ELSE 60 END';
 fixed := $expression$CASE WHEN btrim(OLD.payout_split::text) ~ '^[0-9]+([.][0-9]+)?$' THEN CASE WHEN btrim(OLD.payout_split::text)::numeric BETWEEN 0.01 AND 100 THEN btrim(OLD.payout_split::text)::numeric ELSE 60 END ELSE 60 END$expression$;
 IF strpos(body, broken) > 0 THEN
   EXECUTE replace(body, broken, fixed);
 ELSIF strpos(body, fixed) = 0 THEN
   RAISE EXCEPTION 'Snapshot trigger differs from the expected version. No change was made.';
 END IF;
END $repair$;
NOTIFY pgrst, 'reload schema';
COMMIT;
