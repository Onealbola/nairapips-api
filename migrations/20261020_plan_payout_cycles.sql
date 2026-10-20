BEGIN;
ALTER TABLE public.challenge_plans ADD COLUMN IF NOT EXISTS payout_cycles integer CHECK (payout_cycles BETWEEN 0 AND 5);
ALTER TABLE public.challenge_purchases ADD COLUMN IF NOT EXISTS payout_cycles integer CHECK (payout_cycles BETWEEN 0 AND 5);
-- Deliberately no backfill/default: existing commercial entitlements are unchanged.
CREATE OR REPLACE FUNCTION public.np_keep_purchase_cycles() RETURNS trigger
LANGUAGE plpgsql SET search_path=public,pg_temp AS $guard$
BEGIN
 IF NEW.payout_cycles IS DISTINCT FROM OLD.payout_cycles THEN
   RAISE EXCEPTION 'Purchased cycle limit is immutable; create a new purchase';
 END IF;
 RETURN NEW;
END $guard$;
DROP TRIGGER IF EXISTS np_keep_purchase_cycles ON public.challenge_purchases;
CREATE TRIGGER np_keep_purchase_cycles BEFORE UPDATE ON public.challenge_purchases
FOR EACH ROW EXECUTE FUNCTION public.np_keep_purchase_cycles();

CREATE OR REPLACE FUNCTION public.np_plan_paid_cycle_decision(p_payout_id uuid, p_complete boolean DEFAULT true)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $fn$
DECLARE p public.payouts%ROWTYPE; a public.trader_accounts%ROWTYPE; purchase public.challenge_purchases%ROWTYPE;
 used integer; marker text;
BEGIN
 SELECT * INTO STRICT p FROM public.payouts WHERE id=p_payout_id;
 IF p.status IS DISTINCT FROM 'paid' OR p.paid_at IS NULL OR p.trader_account_id IS NULL THEN RAISE EXCEPTION 'Exact paid payout required'; END IF;
 SELECT * INTO STRICT a FROM public.trader_accounts WHERE id=p.trader_account_id;
 IF a.trader_id IS DISTINCT FROM p.trader_id OR a.mt5_login::text IS DISTINCT FROM p.mt5_login::text OR a.stage IS DISTINCT FROM 'funded' THEN RAISE EXCEPTION 'Payout account identity mismatch'; END IF;
 IF a.purchase_id IS NULL OR a.package_contract_id IS NOT NULL THEN RETURN jsonb_build_object('state','legacy'); END IF;
 SELECT * INTO STRICT purchase FROM public.challenge_purchases WHERE id=a.purchase_id FOR UPDATE;
 IF purchase.trader_id IS DISTINCT FROM p.trader_id THEN RAISE EXCEPTION 'Purchase identity mismatch'; END IF;
 IF purchase.payout_cycles IS NULL OR purchase.payout_cycles=0 THEN RETURN jsonb_build_object('state','legacy'); END IF;
 SELECT count(DISTINCT x.trader_account_id) INTO used FROM public.payouts x
 JOIN public.trader_accounts child ON child.id=x.trader_account_id
 WHERE child.purchase_id=purchase.id AND child.trader_id=p.trader_id AND child.stage='funded'
 AND x.trader_id=p.trader_id AND x.mt5_login::text=child.mt5_login::text AND x.status='paid' AND x.paid_at IS NOT NULL;
 IF used < purchase.payout_cycles THEN
   RETURN jsonb_build_object('state','renew','paid_cycles',used,'cycle_limit',purchase.payout_cycles);
 END IF;
 IF NOT p_complete THEN RETURN jsonb_build_object('state','completed','paid_cycles',used,'cycle_limit',purchase.payout_cycles); END IF;
 -- Retire only the exact final paid source. Never alter another purchase/account.
 PERFORM 1 FROM public.trader_accounts WHERE id=a.id FOR UPDATE;
 marker := '[NP_PLAN_CYCLES_COMPLETED:'||purchase.id::text||']';
 IF coalesce(a.archive_reason,'') NOT LIKE '%'||marker||'%' THEN
   UPDATE public.trader_accounts SET account_status='archived',monitoring_enabled=false,archived_at=coalesce(archived_at,now()),
     archive_reason=concat_ws(' | ',nullif(archive_reason,''),marker||' Closed: purchased funded payout cycles completed.'),updated_at=now() WHERE id=a.id;
   UPDATE public.mt5_pool SET status='archived',archived_at=coalesce(archived_at,now()),updated_at=now(),
     archive_reason=concat_ws(' | ',nullif(archive_reason,''),marker) WHERE id=a.mt5_pool_id AND assigned_trader_id=p.trader_id;
   UPDATE public.monitoring_registry SET active=false,retired_at=coalesce(retired_at,now()),retirement_reason=marker,updated_at=now() WHERE trader_account_id=a.id AND trader_id=p.trader_id;
   UPDATE public.challenge_purchases SET status='completed',lifecycle_state='funded_cycles_completed',account_state='archived',
     archived_at=coalesce(archived_at,now()),archive_reason=concat_ws(' | ',nullif(archive_reason,''),marker),updated_at=now() WHERE id=purchase.id;
   UPDATE public.traders SET current_account_id=NULL,current_purchase_id=NULL,active_purchase_id=NULL,
     mt5_login=NULL,mt5_server=NULL,mt5_master_password=NULL,mt5_investor_password=NULL,
     mt5_password=NULL,master_password=NULL,investor_password=NULL,login=NULL,server=NULL,
     phase='registered',status='registered',account_state='no_active_account',challenge_state='no_active_account',
     current_active=false,mt5_account_active=false,monitoring_enabled=false,mt5_access_disabled=true,
     account_size=0,balance=0,equity=0,profit=0,profit_percent=0,updated_at=now()
     WHERE id=p.trader_id AND current_account_id=a.id;
 END IF;
 RETURN jsonb_build_object('state','completed','paid_cycles',used,'cycle_limit',purchase.payout_cycles);
END $fn$;
REVOKE ALL ON FUNCTION public.np_plan_paid_cycle_decision(uuid,boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.np_plan_paid_cycle_decision(uuid,boolean) FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION public.np_plan_paid_cycle_decision(uuid,boolean) TO service_role;
-- Every funded assignment path must respect a finite purchase, including
-- legacy resets and compatibility assignment handlers.
CREATE OR REPLACE FUNCTION public.np_guard_funded_cycle_assignment() RETURNS trigger
LANGUAGE plpgsql SET search_path=public,pg_temp AS $guard$
DECLARE cap integer; owner_id uuid; paid_count integer;
BEGIN
 IF NEW.stage<>'funded' OR NEW.purchase_id IS NULL OR NEW.package_contract_id IS NOT NULL THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' THEN
   IF NEW.purchase_id IS NOT DISTINCT FROM OLD.purchase_id AND NEW.stage IS NOT DISTINCT FROM OLD.stage THEN RETURN NEW; END IF;
 END IF;
 SELECT payout_cycles,trader_id INTO cap,owner_id FROM public.challenge_purchases WHERE id=NEW.purchase_id FOR UPDATE;
 IF cap IS NULL OR cap=0 THEN RETURN NEW; END IF;
 IF NEW.trader_id IS DISTINCT FROM owner_id THEN RAISE EXCEPTION 'Funded purchase identity mismatch'; END IF;
 SELECT count(DISTINCT p.trader_account_id) INTO paid_count FROM public.payouts p
 JOIN public.trader_accounts a ON a.id=p.trader_account_id
 WHERE a.purchase_id=NEW.purchase_id AND a.trader_id=owner_id AND a.stage='funded'
 AND p.trader_id=owner_id AND p.mt5_login::text=a.mt5_login::text AND p.status='paid' AND p.paid_at IS NOT NULL;
 IF paid_count>=cap THEN RAISE EXCEPTION 'Purchased funded cycles exhausted; no further funded assignment'; END IF;
 RETURN NEW;
END $guard$;
DROP TRIGGER IF EXISTS np_guard_funded_cycle_assignment ON public.trader_accounts;
CREATE TRIGGER np_guard_funded_cycle_assignment BEFORE INSERT OR UPDATE OF purchase_id,stage
ON public.trader_accounts FOR EACH ROW EXECUTE FUNCTION public.np_guard_funded_cycle_assignment();
NOTIFY pgrst, 'reload schema';
COMMIT;
