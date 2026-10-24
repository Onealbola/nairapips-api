-- User explicitly authorized admin shares from 0.01 to 100 percent.
BEGIN;
CREATE OR REPLACE FUNCTION public.np_guard_funded_payout_15() RETURNS trigger
LANGUAGE plpgsql SET search_path=public,pg_temp AS $fn$
DECLARE a public.trader_accounts%ROWTYPE; capital numeric; share numeric; maximum numeric;
BEGIN
 IF lower(coalesce(NEW.status,'')) NOT IN ('pending','requested','submitted','under_review','pending_review','awaiting_review','approved','processing','payment_processing','paid') THEN RETURN NEW; END IF;
 SELECT * INTO a FROM public.trader_accounts WHERE id=NEW.trader_account_id;
 IF NOT FOUND OR a.trader_id IS DISTINCT FROM NEW.trader_id OR a.mt5_login::text IS DISTINCT FROM NEW.mt5_login::text OR a.stage IS DISTINCT FROM 'funded' THEN RAISE EXCEPTION 'Exact funded account identity required for payout'; END IF;
 capital=coalesce(nullif(a.start_balance,0),a.account_size);
 SELECT c.payout_split INTO share FROM public.challenge_purchases c WHERE c.id=a.purchase_id AND c.trader_id=a.trader_id;
 share=coalesce(share,NEW.payout_split,60);
 IF capital IS NULL OR capital<=0 OR share IS NULL OR share<0.01 OR share>100 OR share::text IN ('NaN','Infinity','-Infinity') THEN RAISE EXCEPTION 'Funded capital and payout split require verification'; END IF;
 maximum=trunc(capital*0.15*share/100,2);
 IF NEW.amount IS NULL OR NEW.amount<=0 OR NEW.amount>maximum THEN RAISE EXCEPTION 'Business profit cap is 15 percent; maximum trader payout is %',maximum; END IF;
 RETURN NEW;
END $fn$;
CREATE OR REPLACE FUNCTION public.np_preserve_sold_plan_terms() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $freeze$
BEGIN
 IF ROW(NEW.phase1_target,NEW.phase2_target,NEW.max_drawdown,NEW.payout_split,
        NEW.reset_fee,NEW.funded_reset_fee,NEW.challenge_journey,NEW.drawdown_type,
        NEW.payout_frequency,NEW.product_category,NEW.payout_cycles,NEW.fee,NEW.account_size,NEW.second_life_enabled,NEW.lives_total)
 IS NOT DISTINCT FROM
    ROW(OLD.phase1_target,OLD.phase2_target,OLD.max_drawdown,OLD.payout_split,
        OLD.reset_fee,OLD.funded_reset_fee,OLD.challenge_journey,OLD.drawdown_type,
        OLD.payout_frequency,OLD.product_category,OLD.payout_cycles,OLD.fee,OLD.account_size,OLD.second_life_enabled,OLD.lives_total)
 THEN RETURN NEW; END IF;
 UPDATE public.challenge_purchases p SET
   second_life_enabled=coalesce(p.second_life_enabled,OLD.second_life_enabled,false),
   lives_total=coalesce(p.lives_total,OLD.lives_total,1),
   account_size=coalesce(p.account_size,OLD.account_size),
   phase1_target=coalesce(p.phase1_target,OLD.phase1_target,10),
   phase2_target=coalesce(p.phase2_target,OLD.phase2_target,0),
   max_drawdown=coalesce(p.max_drawdown,OLD.max_drawdown,20),
   payout_split=coalesce(p.payout_split,CASE WHEN OLD.payout_split>=0.01 AND OLD.payout_split<=100 THEN OLD.payout_split ELSE 60 END),
   reset_fee=coalesce(p.reset_fee,OLD.reset_fee,0),
   funded_reset_fee=coalesce(p.funded_reset_fee,OLD.funded_reset_fee,0),
   challenge_journey=coalesce(p.challenge_journey,OLD.challenge_journey,
     CASE WHEN coalesce(OLD.phase2_target,0)>0 THEN '["phase1","phase2","funded"]'::jsonb ELSE '["phase1","funded"]'::jsonb END),
   drawdown_type=coalesce(nullif(p.drawdown_type,''),nullif(OLD.drawdown_type,''),'STATIC'),
   payout_frequency=coalesce(nullif(p.payout_frequency,''),nullif(OLD.payout_frequency,''),'Daily'),
   product_category=coalesce(p.product_category,OLD.product_category,'standard'),
   plan_snapshot_at=coalesce(p.plan_snapshot_at,now())
 WHERE p.plan_id=OLD.id AND (p.account_size IS NULL OR p.phase1_target IS NULL OR p.phase2_target IS NULL
   OR p.max_drawdown IS NULL OR p.payout_split IS NULL OR p.reset_fee IS NULL
   OR p.funded_reset_fee IS NULL OR p.challenge_journey IS NULL OR p.plan_snapshot_at IS NULL
   OR p.product_category IS NULL OR p.second_life_enabled IS NULL OR p.lives_total IS NULL OR nullif(p.drawdown_type,'') IS NULL OR nullif(p.payout_frequency,'') IS NULL);
 -- NULL historical cycle limits deliberately remain legacy/unlimited. The
 -- existing immutable-cycle trigger protects every non-NULL purchased limit.
 RETURN NEW;
END $freeze$;
NOTIFY pgrst, 'reload schema';
COMMIT;
