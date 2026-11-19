-- Test the real trigger against temporary copies only. No public account writes.
DO $$
DECLARE a public.trader_accounts%ROWTYPE; p public.challenge_purchases%ROWTYPE;
        copied record; rejected boolean := false;
BEGIN
  SELECT a0.* INTO a FROM public.trader_accounts a0
  JOIN public.challenge_purchases p0 ON p0.id=a0.purchase_id AND p0.trader_id=a0.trader_id
  WHERE a0.account_status='assigned_active' AND a0.stage='funded'
    AND p0.max_drawdown>0 AND p0.package_contract_id IS NULL
    AND p0.challenge_journey ? 'phase2'
    AND p0.journey_source LIKE '%|np_risk_controls_v1:%'
  ORDER BY a0.created_at DESC LIMIT 1;
  IF NOT FOUND THEN RAISE EXCEPTION 'No complete purchase fixture for continuity verification'; END IF;
  SELECT * INTO p FROM public.challenge_purchases WHERE id=a.purchase_id;
  CREATE TEMP TABLE np_rule_continuity_check (LIKE public.trader_accounts INCLUDING DEFAULTS) ON COMMIT DROP;
  CREATE TRIGGER np_rule_check BEFORE INSERT OR UPDATE ON np_rule_continuity_check
    FOR EACH ROW EXECUTE FUNCTION public.np_guard_purchase_rule_continuity_v1();
  a.journey_source := 'purchase_rule_continuity_test';
  a.challenge_journey := '["funded"]'::jsonb;
  a.dd_limit_percent := 99;
  INSERT INTO np_rule_continuity_check SELECT (a).*;
  SELECT * INTO copied FROM np_rule_continuity_check WHERE id=a.id;
  IF copied.dd_limit_percent IS DISTINCT FROM p.max_drawdown
      OR copied.challenge_journey IS DISTINCT FROM p.challenge_journey
      OR position(substring(p.journey_source from '\|np_risk_controls_v1:[^|]+') in copied.journey_source)=0
      OR position(substring(p.journey_source from '\|np_withdrawal_v1:[^|]+') in copied.journey_source)=0 THEN
    RAISE EXCEPTION 'Insert did not preserve exact purchase controls';
  END IF;
  UPDATE np_rule_continuity_check SET stage='phase2',dd_limit_percent=99,target_percent=99,journey_source='dropped';
  SELECT * INTO copied FROM np_rule_continuity_check WHERE id=a.id;
  IF copied.target_percent IS DISTINCT FROM p.phase2_target OR copied.dd_limit_percent IS DISTINCT FROM p.max_drawdown THEN
    RAISE EXCEPTION 'Phase progression lost purchased rules';
  END IF;
  BEGIN
    UPDATE np_rule_continuity_check SET purchase_id=NULL;
  EXCEPTION WHEN raise_exception THEN rejected:=true; END;
  IF NOT rejected THEN RAISE EXCEPTION 'Purchase detachment was accepted'; END IF;
  rejected:=false;
  BEGIN
    UPDATE np_rule_continuity_check SET trader_id=gen_random_uuid();
  EXCEPTION WHEN raise_exception THEN rejected:=true; END;
  IF NOT rejected THEN RAISE EXCEPTION 'Cross-owner purchase was accepted'; END IF;
  rejected:=false;
  BEGIN
    UPDATE np_rule_continuity_check SET account_size=1;
  EXCEPTION WHEN raise_exception THEN rejected:=true; END;
  IF NOT rejected THEN RAISE EXCEPTION 'Different purchase capital was accepted'; END IF;
END $$;
