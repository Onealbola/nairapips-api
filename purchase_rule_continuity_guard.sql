-- One invariant for every writer, including legacy assignment/recovery paths.
-- No balances, statuses, breach records or original purchase terms are changed.
CREATE OR REPLACE FUNCTION public.np_guard_purchase_rule_continuity_v1()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path = '' AS $$
DECLARE
  p public.challenge_purchases%ROWTYPE;
  original_source text;
  withdrawal_marker text;
  risk_marker text;
  base_source text;
  contract_hash text;
  contract_document jsonb;
  stage_rule jsonb;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF OLD.purchase_id IS NOT NULL AND NEW.purchase_id IS DISTINCT FROM OLD.purchase_id THEN
      RAISE EXCEPTION 'An issued account cannot move to another purchase journey';
    END IF;
    IF NEW.purchase_id IS NOT DISTINCT FROM OLD.purchase_id
      AND NEW.trader_id IS NOT DISTINCT FROM OLD.trader_id
      AND NEW.dd_limit_percent IS NOT DISTINCT FROM OLD.dd_limit_percent
      AND NEW.challenge_journey IS NOT DISTINCT FROM OLD.challenge_journey
      AND NEW.journey_source IS NOT DISTINCT FROM OLD.journey_source
      AND NEW.target_percent IS NOT DISTINCT FROM OLD.target_percent
      AND NEW.package_contract_id IS NOT DISTINCT FROM OLD.package_contract_id
      AND NEW.package_rules_hash IS NOT DISTINCT FROM OLD.package_rules_hash
      AND NEW.start_balance IS NOT DISTINCT FROM OLD.start_balance
      AND NEW.account_size IS NOT DISTINCT FROM OLD.account_size
      AND NEW.stage IS NOT DISTINCT FROM OLD.stage THEN RETURN NEW; END IF;
  END IF;
  IF NEW.purchase_id IS NULL THEN
    IF NEW.package_contract_id IS NULL AND coalesce(NEW.journey_source,'') !~ '^(equity_trailing_v1:)?manual_plan_snapshot' THEN
      RAISE EXCEPTION 'Account creation or rule changes require an exact purchase or explicit manual plan snapshot';
    END IF;
    RETURN NEW;
  END IF;
  SELECT * INTO p FROM public.challenge_purchases WHERE id = NEW.purchase_id;
  IF NOT FOUND OR p.trader_id IS DISTINCT FROM NEW.trader_id THEN
    RAISE EXCEPTION 'Exact purchase ownership is required for account rules';
  END IF;
  -- Package stages use their existing immutable contract/entitlement validator.
  IF p.package_contract_id IS NOT NULL THEN
    SELECT rules_hash,document INTO contract_hash,contract_document FROM public.np_package_contracts
      WHERE id=p.package_contract_id AND purchase_id=p.id AND trader_id=p.trader_id;
    IF NOT FOUND OR NEW.package_contract_id IS DISTINCT FROM p.package_contract_id
        OR NEW.package_rules_hash IS DISTINCT FROM contract_hash THEN
      RAISE EXCEPTION 'Account must retain its exact immutable package contract';
    END IF;
    IF NEW.start_balance IS DISTINCT FROM (contract_document->>'capital')::numeric
        OR NEW.account_size IS DISTINCT FROM (contract_document->>'capital')::numeric
        OR NOT ((contract_document->'stages') ? lower(NEW.stage)) THEN
      RAISE EXCEPTION 'Account capital or stage differs from immutable package contract';
    END IF;
    stage_rule := contract_document->'stage_rules'->lower(NEW.stage);
    IF stage_rule IS NULL THEN RAISE EXCEPTION 'Immutable package stage rules unavailable'; END IF;
    NEW.dd_limit_percent := (stage_rule->>'drawdown_percent')::numeric;
    NEW.target_percent := coalesce((stage_rule->>'target_percent')::numeric,0);
    NEW.challenge_journey := contract_document->'stages';
    RETURN NEW;
  ELSIF NEW.package_contract_id IS NOT NULL THEN
    RAISE EXCEPTION 'A legacy purchase cannot substitute a package contract';
  END IF;
  IF p.max_drawdown IS NULL OR p.max_drawdown <= 0 OR p.max_drawdown >= 100
      OR p.challenge_journey IS NULL OR jsonb_typeof(p.challenge_journey) <> 'array'
      OR jsonb_array_length(p.challenge_journey) = 0 THEN
    RAISE EXCEPTION 'Purchased rules are incomplete; explicit snapshot recovery is required';
  END IF;
  IF NEW.start_balance IS DISTINCT FROM p.account_size OR NEW.account_size IS DISTINCT FROM p.account_size THEN
    RAISE EXCEPTION 'Account capital differs from the exact purchase';
  END IF;
  IF NEW.stage IS NULL OR NOT (p.challenge_journey ? lower(NEW.stage)) THEN
    RAISE EXCEPTION 'Account stage is not included in the purchased journey';
  END IF;
  original_source := coalesce(p.journey_source, '');
  withdrawal_marker := substring(original_source from '\|np_withdrawal_v1:[^|]+');
  risk_marker := substring(original_source from '\|np_risk_controls_v1:[^|]+');
  IF (withdrawal_marker IS NULL AND coalesce(NEW.journey_source,'') LIKE '%|np_withdrawal_v1:%')
     OR (risk_marker IS NULL AND coalesce(NEW.journey_source,'') LIKE '%|np_risk_controls_v1:%') THEN
    RAISE EXCEPTION 'Conflicting account controls require an explicit purchase snapshot review';
  END IF;
  base_source := regexp_replace(coalesce(NEW.journey_source,'purchase_rule_continuity_v1'),
      '\|np_(withdrawal|risk_controls)_v1:[^|]*', '', 'g');
  base_source := regexp_replace(base_source, '^equity_trailing_v1:', '');
  IF upper(coalesce(nullif(p.drawdown_type,''),'STATIC')) NOT IN ('STATIC','TRAILING') THEN
    RAISE EXCEPTION 'Invalid purchased drawdown type';
  END IF;
  IF upper(coalesce(p.drawdown_type,'STATIC')) = 'TRAILING' THEN
    base_source := 'equity_trailing_v1:' || base_source;
  END IF;
  NEW.journey_source := base_source || coalesce(withdrawal_marker,'') || coalesce(risk_marker,'');
  NEW.challenge_journey := p.challenge_journey;
  NEW.dd_limit_percent := p.max_drawdown;
  IF lower(NEW.stage) = 'phase1' THEN
    IF p.phase1_target IS NULL OR p.phase1_target <= 0 THEN RAISE EXCEPTION 'Purchased Phase 1 target is unavailable'; END IF;
    NEW.target_percent := p.phase1_target;
  ELSIF lower(NEW.stage) = 'phase2' THEN
    IF p.phase2_target IS NULL OR p.phase2_target <= 0 THEN RAISE EXCEPTION 'Purchased Phase 2 target is unavailable'; END IF;
    NEW.target_percent := p.phase2_target;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS np_purchase_rule_continuity_v1 ON public.trader_accounts;
CREATE TRIGGER np_purchase_rule_continuity_v1
BEFORE INSERT OR UPDATE OF purchase_id,trader_id,stage,dd_limit_percent,challenge_journey,journey_source,target_percent,package_contract_id,package_rules_hash,start_balance,account_size
ON public.trader_accounts FOR EACH ROW EXECUTE FUNCTION public.np_guard_purchase_rule_continuity_v1();
