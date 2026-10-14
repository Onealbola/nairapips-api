"""Read-only forensic report. No engine, lifecycle, cache or notification writes."""
from datetime import datetime,timezone
from breach_history import BreachReader,BreachReadUnavailable
class BreachRecordError(ValueError):
    def __init__(self,message,status):
        super().__init__(message)
        self.status=status

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def num(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace("₦", "").replace(",", "").strip())
    except Exception:
        return default

def bool_true(value):
    return str(value).strip().lower() in {"true", "1", "yes", "on"}

def resolve_account_rules(account, stage, reader):
    """Resolve exact commercial rules for THIS trader_account.

    Authority order:
      1) frozen trader_accounts values,
      2) linked challenge purchase,
      3) linked challenge plan.

    NairaPips has multiple commercial rule sets (including 10% and 15%
    targets and plan-specific DD). Missing authority must never be converted
    into a guessed pass/breach rule.
    """
    account = account or {}
    stage = str(stage or account.get("stage") or "phase1").strip().lower()
    cache_key = "rules:" + str(account.get("id") or account.get("mt5_login") or "")
    purchase = {}
    plan = {}
    purchase_id = str(account.get("purchase_id") or account.get("challenge_purchase_id") or "").strip()
    if purchase_id:
        purchase = (reader("challenge_purchases", {"id":"eq."+purchase_id}, limit=1) or [{}])[0]

    plan_id = str(
        account.get("plan_id")
        or purchase.get("plan_id")
        or purchase.get("challenge_plan_id")
        or ""
    ).strip()
    if plan_id:
        plan = (reader("challenge_plans", {"id":"eq."+plan_id}, limit=1) or [{}])[0]

    def first_num(*values):
        for v in values:
            if v not in (None, ""):
                n = num(v, None)
                if n is not None and n > 0:
                    return float(n)
        return None

    dd_limit = first_num(
        account.get("dd_limit_percent"),
        account.get("max_drawdown"),
        account.get("max_drawdown_percent"),
        purchase.get("dd_limit_percent"),
        purchase.get("max_drawdown"),
        purchase.get("max_drawdown_percent"),
        plan.get("dd_limit_percent"),
        plan.get("max_drawdown"),
        plan.get("max_drawdown_percent"),
        plan.get("total_dd"),
    )
    dd_authority_present = dd_limit is not None

    if stage == "phase1":
        target = first_num(
            account.get("target_percent"),
            account.get("profit_target"),
            account.get("phase1_target"),
            purchase.get("target_percent"),
            purchase.get("phase1_target"),
            purchase.get("profit_target"),
            plan.get("target_percent"),
            plan.get("phase1_target"),
            plan.get("profit_target"),
        )
    elif stage == "phase2":
        target = first_num(
            account.get("target_percent"),
            account.get("profit_target"),
            account.get("phase2_target"),
            purchase.get("target_percent"),
            purchase.get("phase2_target"),
            purchase.get("profit_target"),
            plan.get("target_percent"),
            plan.get("phase2_target"),
            plan.get("profit_target"),
        )
    else:
        target = 0.0

    target_authority_present = (stage not in {"phase1", "phase2"}) or (target is not None)

    second_life_enabled = bool_true(
        purchase.get("second_life_enabled")
        if purchase.get("second_life_enabled") is not None
        else plan.get("second_life_enabled")
    )

    one_phase = second_life_enabled
    journey_text = " ".join(str(x or "") for x in (
        account.get("challenge_journey"),
        purchase.get("challenge_journey"),
        purchase.get("journey_stages"),
        purchase.get("route"),
        plan.get("challenge_journey"),
        plan.get("journey_stages"),
    )).lower()
    if "one_phase" in journey_text or "1-phase" in journey_text or "1 phase" in journey_text:
        one_phase = True

    rules = {
        "dd_limit_percent": float(dd_limit) if dd_limit is not None else 0.0,
        "dd_authority_present": bool(dd_authority_present),
        "target_percent": float(target) if target is not None else 0.0,
        "target_authority_present": bool(target_authority_present),
        "second_life_enabled": bool(second_life_enabled),
        "one_phase": bool(one_phase),
        "purchase_id": purchase_id or None,
        "plan_id": plan_id or None,
        "plan_name": plan.get("name") or plan.get("plan_name") or purchase.get("plan_name"),
    }
    return rules

def build_breach_report(account_id, trader_id, reader):
    if not account_id:
        raise BreachRecordError('Exact account ID is required',400)
    account=(reader('trader_accounts',{'id':'eq.'+account_id},limit=1) or [None])[0]
    if account is None:
        raise BreachRecordError('Exact trader account not found',404)
    if trader_id and str(account.get('trader_id') or '') != trader_id:
        raise BreachRecordError('Trader/account ownership mismatch',409)
    def _rows(table):
        return reader(table, {"trader_account_id":"eq."+account_id})

    snapshots = _rows("monitoring_snapshots")
    events = _rows("monitoring_events")

    rules = resolve_account_rules(account, account.get("stage"), reader=reader)
    dd_limit = num(rules.get("dd_limit_percent"), 0.0)
    start = num(
        account.get("start_balance")
        or account.get("account_size")
        or account.get("initial_balance")
        or account.get("starting_balance")
        or account.get("challenge_balance")
        or account.get("original_balance")
        or 0
    )
    breach_level = round(start * (1 - dd_limit / 100.0), 2) if start and dd_limit > 0 else num(account.get("breach_equity_level"), 0.0)

    def snap_time(row):
        return str(row.get("created_at") or row.get("timestamp") or row.get("last_sync_at") or "")

    def row_dd(row):
        return num(row.get("drawdown_percent"), 0.0)

    def row_equity(row):
        return num(row.get("current_equity") if row.get("current_equity") not in (None, "") else row.get("equity"), 0.0)

    def row_balance(row):
        return num(row.get("current_balance") if row.get("current_balance") not in (None, "") else row.get("balance"), 0.0)

    # FIRST CROSSING is immutable historical evidence: earliest stored live observation
    # at/beyond the exact DD rule. Later recovery cannot replace this row.
    first_crossing = None
    first_index = None
    for i, row in enumerate(snapshots):
        limit = num(row.get("dd_limit_percent"), dd_limit)
        level = num(row.get("breach_equity_level"), breach_level)
        eq = row_equity(row)
        bal = row_balance(row)
        dd = row_dd(row)
        crossed = bool(
            (limit > 0 and dd >= limit)
            or (level > 0 and eq > 0 and eq <= level)
            or (level > 0 and bal > 0 and bal <= level)
            or str(row.get("risk_zone") or row.get("zone") or "").lower() == "breached"
            or str(row.get("event_type") or "").lower() == "breached"
        )
        if crossed:
            first_crossing = row
            first_index = i
            break

    # If the historical snapshot table predates the breach capture, preserve that fact
    # instead of inventing an exact crossing that was never stored.
    evidence_quality = "EXACT_STORED_FIRST_CROSSING" if first_crossing else "LEGACY_TERMINAL_RECORD_ONLY"
    if not first_crossing:
        first_crossing = next((e for e in events if str(e.get("event_type") or "").lower() == "breached"), None)

    lowest_row = None
    if snapshots:
        positive = [r for r in snapshots if row_equity(r) > 0]
        if positive:
            lowest_row = min(positive, key=row_equity)

    latest = snapshots[-1] if snapshots else {}
    fc_eq = row_equity(first_crossing or {})
    fc_bal = row_balance(first_crossing or {})
    fc_dd = row_dd(first_crossing or {})
    fc_limit = num((first_crossing or {}).get("dd_limit_percent"), dd_limit)
    fc_level = num((first_crossing or {}).get("breach_equity_level"), breach_level)
    fc_float = num((first_crossing or {}).get("floating_profit"), (fc_eq - fc_bal) if fc_eq and fc_bal else 0.0)
    exceeded = max(0.0, fc_level - min(x for x in [fc_eq, fc_bal] if x > 0)) if fc_level and (fc_eq > 0 or fc_bal > 0) else 0.0

    post = snapshots[(first_index + 1):] if first_index is not None else []
    recovered_rows = [
        r for r in post
        if fc_level > 0 and row_equity(r) > fc_level
    ]
    recovered = bool(recovered_rows)
    recovery_row = recovered_rows[0] if recovered_rows else None

    # Compact immutable timeline around the breach plus all explicit breach/lock events.
    timeline = []
    if first_index is not None:
        lo = max(0, first_index - 5)
        hi = min(len(snapshots), first_index + 11)
        for r in snapshots[lo:hi]:
            timeline.append({
                "kind": "snapshot",
                "timestamp": snap_time(r),
                "balance": row_balance(r),
                "equity": row_equity(r),
                "floating_profit": num(r.get("floating_profit"), row_equity(r) - row_balance(r)),
                "drawdown_percent": row_dd(r),
                "dd_limit_percent": num(r.get("dd_limit_percent"), dd_limit),
                "risk_zone": r.get("risk_zone") or r.get("zone"),
                "event_type": r.get("event_type"),
                "message": r.get("message"),
            })
    for e in events:
        et = str(e.get("event_type") or "").lower()
        if et in {"breached", "breach", "disabled", "locked"} or "breach" in et:
            timeline.append({
                "kind": "event",
                "timestamp": snap_time(e),
                "balance": row_balance(e),
                "equity": row_equity(e),
                "drawdown_percent": row_dd(e),
                "dd_limit_percent": num(e.get("dd_limit_percent"), dd_limit),
                "risk_zone": e.get("risk_zone"),
                "event_type": e.get("event_type"),
                "message": e.get("message"),
            })
    timeline.sort(key=lambda x: str(x.get("timestamp") or ""))

    event_id = None
    if first_crossing:
        event_id = first_crossing.get("intelligence_event_id")
    if not event_id:
        event_id = f"NP-BREACH-{account_id}-{str(account.get('breached_at') or snap_time(first_crossing or {}) or 'legacy').replace(':','').replace('-','')}"

    payload = {
        "event_id": event_id,
        "evidence_quality": evidence_quality,
        "immutable": True,
        "account": {
            "id": account.get("id"),
            "trader_id": account.get("trader_id"),
            "mt5_login": account.get("mt5_login"),
            "stage": account.get("stage"),
            "account_status": account.get("account_status"),
            "account_size": num(account.get("account_size"), start),
            "breached_at": account.get("breached_at"),
            "breach_reason": account.get("breach_reason"),
            "current_balance": num(account.get("current_balance"), 0.0),
            "current_equity": num(account.get("current_equity"), 0.0),
        },
        "rule": {
            "start_balance": start,
            "dd_limit_percent": dd_limit,
            "breach_equity_level": breach_level,
            "authority_present": bool(rules.get("dd_authority_present")),
            "authority": "exact trader_account / linked purchase / linked plan",
        },
        "first_crossing": {
            "timestamp": snap_time(first_crossing or {}) or account.get("breached_at"),
            "balance": fc_bal,
            "equity": fc_eq,
            "floating_profit": fc_float,
            "drawdown_percent": fc_dd,
            "dd_limit_percent": fc_limit,
            "breach_equity_level": fc_level,
            "exceeded_amount": round(exceeded, 2),
            "risk_zone": (first_crossing or {}).get("risk_zone") or (first_crossing or {}).get("zone"),
            "breach_source": (first_crossing or {}).get("breach_source"),
            "message": (first_crossing or {}).get("message"),
        },
        "lowest_observed": {
            "timestamp": snap_time(lowest_row or {}),
            "equity": row_equity(lowest_row or {}),
            "balance": row_balance(lowest_row or {}),
            "drawdown_percent": row_dd(lowest_row or {}),
        },
        "recovery_after_breach": {
            "recovered": recovered,
            "first_recovery_timestamp": snap_time(recovery_row or {}),
            "recovery_equity": row_equity(recovery_row or {}),
            "latest_timestamp": snap_time(latest),
            "latest_balance": row_balance(latest),
            "latest_equity": row_equity(latest),
        },
        "timeline": timeline,
        "snapshot_count": len(snapshots),
        "event_count": len(events),
        "generated_at": now_iso(),
    }
    return payload
