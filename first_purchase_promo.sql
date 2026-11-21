-- Prospective WELCOME20 only. No existing financial rows are rewritten.
CREATE OR REPLACE FUNCTION public.np_promo_phone_v1(value text)
RETURNS text LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path='' AS $$
 WITH d AS (SELECT regexp_replace(coalesce(value,''),'[^0-9]','','g') AS v), n AS (SELECT CASE WHEN v LIKE '00%' THEN substr(v,3) ELSE v END AS v FROM d)
 SELECT CASE WHEN length(v)=11 AND left(v,1)='0' THEN '234'||substr(v,2) ELSE v END FROM n;
$$;
CREATE TABLE IF NOT EXISTS public.np_first_purchase_promo_claims_v1 (
 identity_key text PRIMARY KEY, purchase_id uuid NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.np_first_purchase_promo_claims_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.np_first_purchase_promo_claims_v1 FROM PUBLIC,anon,authenticated;
GRANT SELECT,INSERT ON public.np_first_purchase_promo_claims_v1 TO service_role;
CREATE OR REPLACE FUNCTION public.np_first_purchase_promo_keys_v1(p_trader_id uuid)
RETURNS SETOF text LANGUAGE sql STABLE SECURITY INVOKER SET search_path='' AS $$
 SELECT encode(sha256(convert_to(k,'UTF8')),'hex') FROM (
  SELECT 'trader:'||id::text AS k FROM public.traders WHERE id=p_trader_id
  UNION ALL SELECT 'email:'||lower(trim(email)) FROM public.traders WHERE id=p_trader_id AND trim(coalesce(email,''))<>''
  UNION ALL SELECT 'phone:'||public.np_promo_phone_v1(phone) FROM public.traders WHERE id=p_trader_id AND length(public.np_promo_phone_v1(phone))>=8
 ) identities;
$$;
CREATE OR REPLACE FUNCTION public.np_first_purchase_promo_eligible_v1(p_trader_id uuid)
RETURNS boolean LANGUAGE sql VOLATILE SECURITY INVOKER SET search_path='' AS $$
 WITH customer AS (SELECT id,lower(trim(email)) AS email,public.np_promo_phone_v1(phone) AS phone FROM public.traders WHERE id=p_trader_id),
 linked AS (SELECT t.id FROM public.traders t CROSS JOIN customer c WHERE t.id=c.id OR lower(trim(t.email))=c.email OR (length(c.phone)>=8 AND public.np_promo_phone_v1(t.phone)=c.phone))
 SELECT EXISTS(SELECT 1 FROM customer WHERE email<>'')
 AND NOT EXISTS(SELECT 1 FROM public.np_first_purchase_promo_claims_v1 WHERE identity_key IN (SELECT public.np_first_purchase_promo_keys_v1(p_trader_id)))
 AND NOT EXISTS(SELECT 1 FROM public.challenge_purchases p CROSS JOIN customer c WHERE p.trader_id IN (SELECT id FROM linked) OR lower(trim(p.email))=c.email OR (length(c.phone)>=8 AND public.np_promo_phone_v1(p.phone)=c.phone))
 AND NOT EXISTS(SELECT 1 FROM public.trader_accounts a WHERE a.trader_id IN (SELECT id FROM linked));
$$;
CREATE OR REPLACE FUNCTION public.np_guard_first_purchase_promo_v1()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path='' AS $$
DECLARE is_promo boolean; key_value text;
BEGIN
 -- All purchase inserts share this short transaction lock, including normal-price
 -- orders. A normal checkout and a promo checkout cannot both be the first order.
 PERFORM pg_advisory_xact_lock(723119,20261121);
 is_promo := 'WELCOME20'=ANY(ARRAY[upper(trim(coalesce(NEW.promo_code,''))),upper(trim(coalesce(NEW.affiliate_code,''))),upper(trim(coalesce(NEW.referral_code,''))),upper(trim(coalesce(NEW.partner_code,'')))]);
 IF NOT is_promo THEN
  -- Retain prospective purchase evidence even if an order is later removed.
  -- Existing public/legacy writers are not forced to access the private ledger.
  IF current_user IN ('postgres','service_role','supabase_admin') THEN
   FOR key_value IN SELECT public.np_first_purchase_promo_keys_v1(NEW.trader_id) LOOP
    INSERT INTO public.np_first_purchase_promo_claims_v1(identity_key,purchase_id) VALUES(key_value,NEW.id) ON CONFLICT(identity_key) DO NOTHING;
   END LOOP;
  END IF;
  RETURN NEW;
 END IF;
 IF current_user NOT IN ('postgres','service_role','supabase_admin') THEN RAISE EXCEPTION 'WELCOME20 requires authenticated server checkout'; END IF;
 IF NOT public.np_first_purchase_promo_eligible_v1(NEW.trader_id) THEN RAISE EXCEPTION 'WELCOME20 is only for a first account purchase. Previous or pending purchases and existing accounts are not eligible.'; END IF;
 IF NEW.plan_id IS NULL OR NEW.original_fee IS NULL OR NEW.original_fee<=0 OR NEW.discount_percent IS DISTINCT FROM 20::numeric
 OR NEW.discount_amount IS DISTINCT FROM round(NEW.original_fee*0.20,2)
 OR NEW.fee IS DISTINCT FROM NEW.original_fee-round(NEW.original_fee*0.20,2)
 OR NEW.final_fee IS DISTINCT FROM NEW.fee OR NEW.amount_due IS DISTINCT FROM NEW.fee
 OR coalesce(NEW.commission_amount,0)<>0 OR coalesce(NEW.commission_percent,0)<>0
 OR NEW.campaign_type IS DISTINCT FROM 'first_purchase_welcome20' THEN RAISE EXCEPTION 'Invalid WELCOME20 first-purchase price'; END IF;
 IF EXISTS(SELECT 1 FROM unnest(ARRAY[NEW.promo_code,NEW.affiliate_code,NEW.referral_code,NEW.partner_code]) code WHERE trim(coalesce(code,''))<>'' AND upper(trim(code))<>'WELCOME20') THEN RAISE EXCEPTION 'WELCOME20 cannot be combined with another discount'; END IF;
 -- Trusted registered profile wins over customer-supplied checkout identity fields.
 SELECT email,phone INTO NEW.email,NEW.phone FROM public.traders WHERE id=NEW.trader_id;
 FOR key_value IN SELECT public.np_first_purchase_promo_keys_v1(NEW.trader_id) LOOP
  INSERT INTO public.np_first_purchase_promo_claims_v1(identity_key,purchase_id) VALUES(key_value,NEW.id);
 END LOOP;
 RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION public.np_promo_phone_v1(text),public.np_first_purchase_promo_keys_v1(uuid),public.np_first_purchase_promo_eligible_v1(uuid),public.np_guard_first_purchase_promo_v1() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.np_promo_phone_v1(text),public.np_first_purchase_promo_keys_v1(uuid),public.np_first_purchase_promo_eligible_v1(uuid),public.np_guard_first_purchase_promo_v1() TO service_role;
DROP TRIGGER IF EXISTS np_first_purchase_promo_v1 ON public.challenge_purchases;
CREATE TRIGGER np_first_purchase_promo_v1 BEFORE INSERT ON public.challenge_purchases FOR EACH ROW EXECUTE FUNCTION public.np_guard_first_purchase_promo_v1();
-- Adding the code to an old invoice, or repricing an accepted promo invoice,
-- cannot turn a reset/renewal/second purchase into a discounted first purchase.
CREATE OR REPLACE FUNCTION public.np_guard_first_purchase_promo_update_v1()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path='' AS $$
BEGIN
 IF 'WELCOME20'=ANY(ARRAY[upper(trim(coalesce(NEW.promo_code,''))),upper(trim(coalesce(NEW.affiliate_code,''))),upper(trim(coalesce(NEW.referral_code,''))),upper(trim(coalesce(NEW.partner_code,''))),upper(trim(coalesce(OLD.promo_code,''))),upper(trim(coalesce(OLD.affiliate_code,''))),upper(trim(coalesce(OLD.referral_code,''))),upper(trim(coalesce(OLD.partner_code,'')))])
 AND ROW(NEW.trader_id,NEW.plan_id,NEW.original_fee,NEW.discount_percent,NEW.discount_amount,NEW.fee,NEW.final_fee,NEW.amount_due,NEW.promo_code,NEW.affiliate_code,NEW.referral_code,NEW.partner_code,NEW.campaign_type)
 IS DISTINCT FROM ROW(OLD.trader_id,OLD.plan_id,OLD.original_fee,OLD.discount_percent,OLD.discount_amount,OLD.fee,OLD.final_fee,OLD.amount_due,OLD.promo_code,OLD.affiliate_code,OLD.referral_code,OLD.partner_code,OLD.campaign_type)
 THEN RAISE EXCEPTION 'WELCOME20 is only applied when creating an eligible first account purchase; its accepted price cannot be reused or changed.'; END IF;
 RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION public.np_guard_first_purchase_promo_update_v1() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.np_guard_first_purchase_promo_update_v1() TO service_role;
DROP TRIGGER IF EXISTS np_first_purchase_promo_update_v1 ON public.challenge_purchases;
CREATE TRIGGER np_first_purchase_promo_update_v1 BEFORE UPDATE OF trader_id,plan_id,original_fee,discount_percent,discount_amount,fee,final_fee,amount_due,promo_code,affiliate_code,referral_code,partner_code,campaign_type ON public.challenge_purchases FOR EACH ROW EXECUTE FUNCTION public.np_guard_first_purchase_promo_update_v1();
