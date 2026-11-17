-- Restore only purchases recorded in the mistaken historical upgrade audit.
-- Current plan settings and purchases made afterwards are untouched.
BEGIN;
LOCK TABLE public.challenge_purchases IN SHARE ROW EXCLUSIVE MODE;
DO $restore$
DECLARE
 original jsonb;
 restored integer;
 marker constant text := '[NP_OPEN_PAYOUT_CYCLES_V1:20261117] Unlimited cycle benefit granted by owner; prior terms retained in upgrade audit.';
BEGIN
 IF EXISTS(SELECT 1 FROM public.package_studio_private_settings WHERE key='open_one_phase_cycles_rollback_20261117_v1') THEN
  RAISE NOTICE 'Restoration already recorded; no changes made.'; RETURN;
 END IF;
 SELECT value::jsonb INTO original FROM public.package_studio_private_settings WHERE key='open_one_phase_cycles_upgrade_20261117_v1';
 IF original IS NULL OR jsonb_array_length(original->'purchases_before')=0 THEN
  RAISE EXCEPTION 'Original purchase audit missing; no changes made.';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.challenge_purchases'::regclass AND tgname='np_keep_purchase_cycles' AND tgenabled='O') THEN
  RAISE EXCEPTION 'Expected enabled immutable purchase guard missing.';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(original->'purchases_before') b LEFT JOIN public.challenge_purchases p ON p.id=(b->>'id')::uuid WHERE p.id IS NULL OR p.trader_id IS DISTINCT FROM (b->>'trader_id')::uuid) THEN
  RAISE EXCEPTION 'Audited purchase ownership changed or row missing; review required.';
 END IF;
 INSERT INTO public.package_studio_private_settings(key,value,updated_at,updated_by)
 SELECT 'open_one_phase_cycles_rollback_20261117_v1',jsonb_build_object('at',now(),'scope','Restore original purchases; new purchases only for future plan edits','purchases_before',jsonb_agg(to_jsonb(p)),'status','restoring')::text,now(),'owner_new_purchases_only'
 FROM public.challenge_purchases p JOIN jsonb_array_elements(original->'purchases_before') b ON p.id=(b->>'id')::uuid;
 ALTER TABLE public.challenge_purchases DISABLE TRIGGER np_keep_purchase_cycles;
 UPDATE public.challenge_purchases p SET
  payout_cycles=(b->>'payout_cycles')::integer,
  archive_reason=CASE
   WHEN p.archive_reason=marker OR p.archive_reason=concat_ws(' | ',nullif(trim(both ' |' from replace(coalesce(b->>'archive_reason',''),'[NP_PLAN_CYCLES_COMPLETED:'||p.id::text||']','')),''),marker)
    THEN b->>'archive_reason'
   ELSE CASE WHEN b->>'lifecycle_state'='funded_cycles_completed' AND position('[NP_PLAN_CYCLES_COMPLETED:'||p.id::text||']' in coalesce(p.archive_reason,''))=0
    THEN concat_ws(' | ',nullif(trim(both ' |' from replace(coalesce(p.archive_reason,''),marker,'')),''),b->>'archive_reason')
    ELSE nullif(trim(both ' |' from replace(coalesce(p.archive_reason,''),marker,'')),'') END
   END,
  updated_at=now()
 FROM jsonb_array_elements(original->'purchases_before') b WHERE p.id=(b->>'id')::uuid;
 GET DIAGNOSTICS restored=ROW_COUNT;
 ALTER TABLE public.challenge_purchases ENABLE TRIGGER np_keep_purchase_cycles;
 -- Restore the exact closed states only if still in the mistaken reopened state.
 -- Other purchases retain all subsequent legitimate lifecycle activity.
 UPDATE public.challenge_purchases p SET status=b->>'status',lifecycle_state=b->>'lifecycle_state',account_state=b->>'account_state',archived_at=(b->>'archived_at')::timestamptz,updated_at=now()
 FROM jsonb_array_elements(original->'purchases_before') b
 WHERE p.id=(b->>'id')::uuid AND b->>'lifecycle_state'='funded_cycles_completed'
 AND p.lifecycle_state='funded_waiting_mt5' AND p.status='approved_active';
 UPDATE public.package_studio_private_settings SET value=(value::jsonb||jsonb_build_object('status','restored','purchases_restored',restored))::text WHERE key='open_one_phase_cycles_rollback_20261117_v1';
END $restore$;
COMMIT;
SELECT value::jsonb->>'purchases_restored' AS historical_purchases_restored,
 'Original terms restored; unlimited applies to new purchases only' AS result
FROM public.package_studio_private_settings WHERE key='open_one_phase_cycles_rollback_20261117_v1';
