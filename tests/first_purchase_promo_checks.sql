CREATE TEMP TABLE traders(id uuid PRIMARY KEY,email text,phone text);
CREATE TEMP TABLE trader_accounts(trader_id uuid);
CREATE TEMP TABLE np_first_purchase_promo_claims_v1(identity_key text PRIMARY KEY,purchase_id uuid,created_at timestamptz DEFAULT now());
CREATE TEMP TABLE challenge_purchases(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),trader_id uuid,plan_id uuid,email text,phone text,promo_code text,affiliate_code text,referral_code text,partner_code text,original_fee numeric,discount_percent numeric,discount_amount numeric,fee numeric,final_fee numeric,amount_due numeric,commission_percent numeric,commission_amount numeric,campaign_type text);
CREATE OR REPLACE FUNCTION pg_temp.np_promo_phone_v1(value text)
RETURNS text LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path='' AS $$
 WITH d AS (SELECT regexp_replace(coalesce(value,''),'[^0-9]','','g') AS v), n AS (SELECT CASE WHEN v LIKE '00%' THEN substr(v,3) ELSE v END AS v FROM d)
 SELECT CASE WHEN length(v)=11 AND left(v,1)='0' THEN '234'||substr(v,2) ELSE v END FROM n;
$$;
CREATE OR REPLACE FUNCTION pg_temp.np_first_purchase_promo_keys_v1(p_trader_id uuid)
RETURNS SETOF text LANGUAGE sql STABLE SECURITY INVOKER SET search_path='' AS $$
 SELECT encode(sha256(convert_to(k,'UTF8')),'hex') FROM (
  SELECT 'trader:'||id::text AS k FROM pg_temp.traders WHERE id=p_trader_id
  UNION ALL SELECT 'email:'||lower(trim(email)) FROM pg_temp.traders WHERE id=p_trader_id AND trim(coalesce(email,''))<>''
  UNION ALL SELECT 'phone:'||pg_temp.np_promo_phone_v1(phone) FROM pg_temp.traders WHERE id=p_trader_id AND length(pg_temp.np_promo_phone_v1(phone))>=8
 ) identities;
$$;
CREATE OR REPLACE FUNCTION pg_temp.np_first_purchase_promo_eligible_v1(p_trader_id uuid)
RETURNS boolean LANGUAGE sql VOLATILE SECURITY INVOKER SET search_path='' AS $$
 WITH customer AS (SELECT id,lower(trim(email)) AS email,pg_temp.np_promo_phone_v1(phone) AS phone FROM pg_temp.traders WHERE id=p_trader_id),
 linked AS (SELECT t.id FROM pg_temp.traders t CROSS JOIN customer c WHERE t.id=c.id OR lower(trim(t.email))=c.email OR (length(c.phone)>=8 AND pg_temp.np_promo_phone_v1(t.phone)=c.phone))
 SELECT EXISTS(SELECT 1 FROM customer WHERE email<>'')
 AND NOT EXISTS(SELECT 1 FROM pg_temp.np_first_purchase_promo_claims_v1 WHERE identity_key IN (SELECT pg_temp.np_first_purchase_promo_keys_v1(p_trader_id)))
 AND NOT EXISTS(SELECT 1 FROM pg_temp.challenge_purchases p CROSS JOIN customer c WHERE p.trader_id IN (SELECT id FROM linked) OR lower(trim(p.email))=c.email OR (length(c.phone)>=8 AND pg_temp.np_promo_phone_v1(p.phone)=c.phone))
 AND NOT EXISTS(SELECT 1 FROM pg_temp.trader_accounts a WHERE a.trader_id IN (SELECT id FROM linked));
$$;
CREATE OR REPLACE FUNCTION pg_temp.np_guard_first_purchase_promo_v1()
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
   FOR key_value IN SELECT pg_temp.np_first_purchase_promo_keys_v1(NEW.trader_id) LOOP
    INSERT INTO pg_temp.np_first_purchase_promo_claims_v1(identity_key,purchase_id) VALUES(key_value,NEW.id) ON CONFLICT(identity_key) DO NOTHING;
   END LOOP;
  END IF;
  RETURN NEW;
 END IF;
 IF current_user NOT IN ('postgres','service_role','supabase_admin') THEN RAISE EXCEPTION 'WELCOME20 requires authenticated server checkout'; END IF;
 IF NOT pg_temp.np_first_purchase_promo_eligible_v1(NEW.trader_id) THEN RAISE EXCEPTION 'WELCOME20 is only for a first account purchase. Previous or pending purchases and existing accounts are not eligible.'; END IF;
 IF NEW.plan_id IS NULL OR NEW.original_fee IS NULL OR NEW.original_fee<=0 OR NEW.discount_percent IS DISTINCT FROM 20::numeric
 OR NEW.discount_amount IS DISTINCT FROM round(NEW.original_fee*0.20,2)
 OR NEW.fee IS DISTINCT FROM NEW.original_fee-round(NEW.original_fee*0.20,2)
 OR NEW.final_fee IS DISTINCT FROM NEW.fee OR NEW.amount_due IS DISTINCT FROM NEW.fee
 OR coalesce(NEW.commission_amount,0)<>0 OR coalesce(NEW.commission_percent,0)<>0
 OR NEW.campaign_type IS DISTINCT FROM 'first_purchase_welcome20' THEN RAISE EXCEPTION 'Invalid WELCOME20 first-purchase price'; END IF;
 IF EXISTS(SELECT 1 FROM unnest(ARRAY[NEW.promo_code,NEW.affiliate_code,NEW.referral_code,NEW.partner_code]) code WHERE trim(coalesce(code,''))<>'' AND upper(trim(code))<>'WELCOME20') THEN RAISE EXCEPTION 'WELCOME20 cannot be combined with another discount'; END IF;
 -- Trusted registered profile wins over customer-supplied checkout identity fields.
 SELECT email,phone INTO NEW.email,NEW.phone FROM pg_temp.traders WHERE id=NEW.trader_id;
 FOR key_value IN SELECT pg_temp.np_first_purchase_promo_keys_v1(NEW.trader_id) LOOP
  INSERT INTO pg_temp.np_first_purchase_promo_claims_v1(identity_key,purchase_id) VALUES(key_value,NEW.id);
 END LOOP;
 RETURN NEW;
END;
$$;
CREATE OR REPLACE FUNCTION pg_temp.np_guard_first_purchase_promo_update_v1()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path='' AS $$
BEGIN
 IF 'WELCOME20'=ANY(ARRAY[upper(trim(coalesce(NEW.promo_code,''))),upper(trim(coalesce(NEW.affiliate_code,''))),upper(trim(coalesce(NEW.referral_code,''))),upper(trim(coalesce(NEW.partner_code,''))),upper(trim(coalesce(OLD.promo_code,''))),upper(trim(coalesce(OLD.affiliate_code,''))),upper(trim(coalesce(OLD.referral_code,''))),upper(trim(coalesce(OLD.partner_code,'')))])
 AND ROW(NEW.trader_id,NEW.plan_id,NEW.original_fee,NEW.discount_percent,NEW.discount_amount,NEW.fee,NEW.final_fee,NEW.amount_due,NEW.promo_code,NEW.affiliate_code,NEW.referral_code,NEW.partner_code,NEW.campaign_type)
 IS DISTINCT FROM ROW(OLD.trader_id,OLD.plan_id,OLD.original_fee,OLD.discount_percent,OLD.discount_amount,OLD.fee,OLD.final_fee,OLD.amount_due,OLD.promo_code,OLD.affiliate_code,OLD.referral_code,OLD.partner_code,OLD.campaign_type)
 THEN RAISE EXCEPTION 'WELCOME20 is only applied when creating an eligible first account purchase; its accepted price cannot be reused or changed.'; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER test_welcome_guard BEFORE INSERT ON pg_temp.challenge_purchases FOR EACH ROW EXECUTE FUNCTION pg_temp.np_guard_first_purchase_promo_v1();
CREATE TRIGGER test_welcome_update BEFORE UPDATE ON pg_temp.challenge_purchases FOR EACH ROW EXECUTE FUNCTION pg_temp.np_guard_first_purchase_promo_update_v1();
DO $test$
DECLARE a uuid:=gen_random_uuid();b uuid:=gen_random_uuid();c uuid:=gen_random_uuid();d uuid:=gen_random_uuid();e uuid:=gen_random_uuid();p uuid:=gen_random_uuid();blocked boolean;
BEGIN
 INSERT INTO pg_temp.traders VALUES(a,'new@fixture.invalid','08012345678'),(b,' NEW@fixture.invalid ','+2348012345678'),(c,'other@fixture.invalid','08099999999'),(d,'normal@fixture.invalid','08011111111'),(e,'legacy@fixture.invalid','08022222222');
 ASSERT pg_temp.np_first_purchase_promo_eligible_v1(a),'Fresh customer eligible';
 ASSERT pg_temp.np_promo_phone_v1('08012345678')=pg_temp.np_promo_phone_v1('+2348012345678'),'Local/international phone match';
 INSERT INTO pg_temp.trader_accounts VALUES(e);
 ASSERT NOT pg_temp.np_first_purchase_promo_eligible_v1(e),'Legacy account disqualifies';
 -- A malformed discounted price never consumes the benefit.
 blocked:=false;
 BEGIN
  INSERT INTO pg_temp.challenge_purchases(trader_id,plan_id,promo_code,original_fee,discount_percent,discount_amount,fee,final_fee,amount_due,campaign_type) VALUES(a,p,'WELCOME20',20000,20,4000,1,1,1,'first_purchase_welcome20');
 EXCEPTION WHEN raise_exception THEN blocked:=true; END;
 ASSERT blocked,'Forged price rejected';ASSERT pg_temp.np_first_purchase_promo_eligible_v1(a),'Failed insert does not reserve';
 INSERT INTO pg_temp.challenge_purchases(id,trader_id,plan_id,email,phone,promo_code,original_fee,discount_percent,discount_amount,fee,final_fee,amount_due,campaign_type) VALUES(p,a,p,'spoof@fixture.invalid','fake','WELCOME20',20000,20,4000,16000,16000,16000,'first_purchase_welcome20');
 ASSERT (SELECT email='new@fixture.invalid' AND phone='08012345678' AND fee=16000 FROM pg_temp.challenge_purchases WHERE id=p),'Trusted identity and 20 percent price';
 blocked:=false;
 BEGIN UPDATE pg_temp.challenge_purchases SET fee=1 WHERE id=p;
 EXCEPTION WHEN raise_exception THEN blocked:=true; END;
 ASSERT blocked,'Accepted promo price cannot be altered';
 ASSERT NOT pg_temp.np_first_purchase_promo_eligible_v1(a),'Pending first order consumes';
 ASSERT NOT pg_temp.np_first_purchase_promo_eligible_v1(b),'Matching alternate email/phone cannot reuse';
 blocked:=false;
 BEGIN
  INSERT INTO pg_temp.challenge_purchases(trader_id,plan_id,promo_code,original_fee,discount_percent,discount_amount,fee,final_fee,amount_due,campaign_type) VALUES(a,p,'WELCOME20',20000,20,4000,16000,16000,16000,'first_purchase_welcome20');
 EXCEPTION WHEN raise_exception THEN blocked:=true; END;
 ASSERT blocked,'Second discounted order rejected';
 DELETE FROM pg_temp.challenge_purchases WHERE id=p;
 ASSERT NOT pg_temp.np_first_purchase_promo_eligible_v1(a),'Deletion does not reset redemption';
 INSERT INTO pg_temp.challenge_purchases(trader_id,plan_id,email,phone,fee) VALUES(d,p,'normal@fixture.invalid','08011111111',20000);
 ASSERT NOT pg_temp.np_first_purchase_promo_eligible_v1(d),'Normal first order disqualifies';
 DELETE FROM pg_temp.challenge_purchases WHERE trader_id=d;
 ASSERT NOT pg_temp.np_first_purchase_promo_eligible_v1(d),'Deleted prospective normal order still counts';
 blocked:=false;
 BEGIN
  INSERT INTO pg_temp.challenge_purchases(trader_id,plan_id,promo_code,affiliate_code,original_fee,discount_percent,discount_amount,fee,final_fee,amount_due,campaign_type) VALUES(c,p,'WELCOME20','OTHER',20000,20,4000,16000,16000,16000,'first_purchase_welcome20');
 EXCEPTION WHEN raise_exception THEN blocked:=true; END;
 ASSERT blocked,'Stacking rejected';
 ASSERT pg_temp.np_first_purchase_promo_eligible_v1(c),'Another customer keeps own eligibility';
END $test$;
