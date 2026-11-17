-- Owner-requested beneficial upgrade: NGN500k / NGN1m / NGN2m one-phase plans.
-- Unlimited uses payout_cycles=0. No fee, paid payout, trade, broker account,
-- breach latch, reset allowance or risk/schedule rule is changed.
-- Later admin plan edits affect future purchases; these granted terms remain 0.
BEGIN;
LOCK TABLE public.challenge_plans, public.challenge_purchases IN SHARE ROW EXCLUSIVE MODE;
DO $upgrade$
DECLARE
 audit_key constant text := 'open_one_phase_cycles_upgrade_20261117_v1';
 before_plans jsonb;
 before_purchases jsonb;
 plan_count integer;
 purchase_count integer;
BEGIN
 IF EXISTS (SELECT 1 FROM public.package_studio_private_settings WHERE key=audit_key) THEN
  RAISE NOTICE 'This upgrade is already recorded. No changes made.';
  RETURN;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='public.challenge_purchases'::regclass AND tgname='np_keep_purchase_cycles' AND tgenabled='O') THEN
  RAISE EXCEPTION 'Expected enabled purchase-cycle guard not found. No changes made.';
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',p.id,'name',p.name,'account_size',p.account_size,'payout_cycles',p.payout_cycles,'fee',p.fee)), '[]'::jsonb)
 INTO before_plans FROM public.challenge_plans p
 WHERE p.account_size IN (500000,1000000,2000000)
 AND p.challenge_journey::jsonb='["phase1","funded"]'::jsonb;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',p.id,'trader_id',p.trader_id,'account_size',p.account_size,'payout_cycles',p.payout_cycles,'status',p.status,'lifecycle_state',p.lifecycle_state,'account_state',p.account_state,'archived_at',p.archived_at,'archive_reason',p.archive_reason,'fee',p.fee)), '[]'::jsonb)
 INTO before_purchases FROM public.challenge_purchases p
 WHERE p.account_size IN (500000,1000000,2000000)
 AND p.challenge_journey::jsonb='["phase1","funded"]'::jsonb
 AND p.payment_status='approved' AND p.package_contract_id IS NULL
 AND coalesce(p.admin_note,'') NOT LIKE '%[NP_RESET_REQUEST:%'
 AND p.payout_cycles IS DISTINCT FROM 0;
 -- DDL and all updates are in one transaction. Other sessions cannot bypass
 -- the guard: the table lock is held until it is enabled again at COMMIT.
 ALTER TABLE public.challenge_purchases DISABLE TRIGGER np_keep_purchase_cycles;
 UPDATE public.challenge_purchases p SET
  payout_cycles=0,
  status=CASE WHEN p.lifecycle_state='funded_cycles_completed' THEN 'approved_active' ELSE p.status END,
  account_state=CASE WHEN p.lifecycle_state='funded_cycles_completed' THEN 'waiting_mt5' ELSE p.account_state END,
  archived_at=CASE WHEN p.lifecycle_state='funded_cycles_completed' THEN NULL ELSE p.archived_at END,
  archive_reason=concat_ws(' | ',nullif(trim(both ' |' from replace(coalesce(p.archive_reason,''),'[NP_PLAN_CYCLES_COMPLETED:'||p.id::text||']','')),''),'[NP_OPEN_PAYOUT_CYCLES_V1:20261117] Unlimited cycle benefit granted by owner; prior terms retained in upgrade audit.'),
  lifecycle_state=CASE WHEN p.lifecycle_state='funded_cycles_completed' THEN 'funded_waiting_mt5' ELSE p.lifecycle_state END,
  updated_at=now()
 WHERE p.account_size IN (500000,1000000,2000000)
 AND p.challenge_journey::jsonb='["phase1","funded"]'::jsonb
 AND p.payment_status='approved' AND p.package_contract_id IS NULL
 AND coalesce(p.admin_note,'') NOT LIKE '%[NP_RESET_REQUEST:%'
 AND p.payout_cycles IS DISTINCT FROM 0;
 GET DIAGNOSTICS purchase_count=ROW_COUNT;
 ALTER TABLE public.challenge_purchases ENABLE TRIGGER np_keep_purchase_cycles;
 UPDATE public.challenge_plans p SET payout_cycles=0,updated_at=now()
 WHERE p.account_size IN (500000,1000000,2000000)
 AND p.challenge_journey::jsonb='["phase1","funded"]'::jsonb;
 GET DIAGNOSTICS plan_count=ROW_COUNT;
 INSERT INTO public.package_studio_private_settings(key,value,updated_at,updated_by)
 VALUES(audit_key,jsonb_build_object('requested_by','owner','at',now(),'plans_before',before_plans,'purchases_before',before_purchases,'plans_changed',plan_count,'purchases_upgraded',purchase_count,'payout_cycles_after',0,'risk_and_payout_records_changed',false)::text,now(),'owner_authorized_open_cycles');
 RAISE NOTICE 'Unlimited cycles applied: % plans, % existing purchases. Snapshot guard restored.',plan_count,purchase_count;
END $upgrade$;
COMMIT;
SELECT value::jsonb->>'plans_changed' AS plans_updated,
 value::jsonb->>'purchases_upgraded' AS existing_purchases_upgraded,
 'Unlimited; future limits can be restored in Business settings' AS result
FROM public.package_studio_private_settings WHERE key='open_one_phase_cycles_upgrade_20261117_v1';
