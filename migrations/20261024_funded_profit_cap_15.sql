BEGIN;
CREATE OR REPLACE FUNCTION public.np_lock_funded_profit_at_15() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $fn$
DECLARE a public.trader_accounts%ROWTYPE; capital numeric;
BEGIN
 IF NEW.observed_at IS NULL OR NEW.observed_at<now()-interval '180 seconds' OR NEW.observed_at>now()+interval '60 seconds' THEN RETURN NEW; END IF;
 SELECT * INTO a FROM public.trader_accounts WHERE id=NEW.trader_account_id;
 IF NOT FOUND OR a.stage IS DISTINCT FROM 'funded' OR a.mt5_login::text IS DISTINCT FROM NEW.mt5_login::text OR a.account_status NOT IN ('assigned_active','active','current_active','funded_active','live','funded','approved_active','profit_protected','payout_pending','approved_payout_pending','payment_processing') OR a.archived_at IS NOT NULL OR a.breached_at IS NOT NULL THEN RETURN NEW; END IF;
 capital=coalesce(nullif(a.start_balance,0),a.account_size);
 IF capital>0 AND NEW.equity>=capital*1.15 THEN
   UPDATE public.trader_accounts SET account_status='funded_profit_cap_reached',monitoring_enabled=true,locked_reason='Business-wide 15% funded profit cap reached. Payout share applies only to capped profit.',updated_at=now() WHERE id=a.id AND trader_id=a.trader_id AND account_status=a.account_status;
 END IF;
 RETURN NEW;
END $fn$;
REVOKE ALL ON FUNCTION public.np_lock_funded_profit_at_15() FROM PUBLIC,anon,authenticated;
DROP TRIGGER IF EXISTS np_lock_funded_profit_at_15 ON public.np_live_account_state;
CREATE TRIGGER np_lock_funded_profit_at_15 AFTER INSERT OR UPDATE ON public.np_live_account_state FOR EACH ROW EXECUTE FUNCTION public.np_lock_funded_profit_at_15();
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
 IF capital IS NULL OR capital<=0 OR share NOT IN (60,70) THEN RAISE EXCEPTION 'Funded capital and payout split require verification'; END IF;
 maximum=trunc(capital*0.15*share/100,2);
 IF NEW.amount IS NULL OR NEW.amount<=0 OR NEW.amount>maximum THEN RAISE EXCEPTION 'Business profit cap is 15 percent; maximum trader payout is %',maximum; END IF;
 RETURN NEW;
END $fn$;
DROP TRIGGER IF EXISTS np_guard_funded_payout_15 ON public.payouts;
CREATE TRIGGER np_guard_funded_payout_15 BEFORE INSERT OR UPDATE OF status,amount,approved_at,paid_at ON public.payouts FOR EACH ROW EXECUTE FUNCTION public.np_guard_funded_payout_15();
COMMIT;
