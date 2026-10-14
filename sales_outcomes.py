"""Read-only purchase cohort analytics. No lifecycle changes or notifications."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from decimal import Decimal, InvalidOperation

TZ = ZoneInfo("Africa/Lagos")
OUTCOMES = {
    "awaiting_account": "Awaiting first account",
    "no_recorded_result": "Challenge: no pass or breach recorded",
    "passed_not_funded": "Passed a phase, not funded yet",
    "failed_before_funded": "Breached before reaching funded",
    "funded_no_payout": "Funded: no payout, no funded breach recorded",
    "funded_breached_no_payout": "Funded: breached without collecting payout",
    "paid_no_funded_breach": "Collected payout: no funded breach recorded",
    "paid_and_funded_breached": "Collected payout and had a funded breach",
    "needs_review": "Records need review",
}


def outcome_for(row):
    # Exclusive historical result, not a guess at whether trading is active.
    if row["collected_payout"] and not row["funded"]:
        return "needs_review"
    if row["funded"]:
        if row["collected_payout"]:
            return "paid_and_funded_breached" if row["funded_breached"] else "paid_no_funded_breach"
        return "funded_breached_no_payout" if row["funded_breached"] else "funded_no_payout"
    if row["breached"]:
        return "failed_before_funded"
    if row["passed"]:
        return "passed_not_funded"
    return "no_recorded_result" if row["assigned"] else "awaiting_account"


def sale_day(p):
    for key in ("created_at", "payment_submitted_at", "payment_date", "date", "paid_at", "payment_confirmed_at", "approved_at"):
        value = p.get(key)
        if value:
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=TZ)
                return parsed.astimezone(TZ).date().isoformat()
            except (ValueError, TypeError):
                continue
    return None

def _recorded_time(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=TZ)
    except (ValueError, TypeError):
        return None


def _paid_then_breached_closed(journey, paid_times):
    """Same account, recorded chronology, and explicit terminal state only."""
    for account in journey:
        stage = str(account.get("stage") or account.get("phase") or "").lower()
        state = " ".join(str(account.get(key) or "").lower() for key in ("status", "account_status", "lifecycle_state"))
        breached = _recorded_time(account.get("breached_at") or account.get("breach_at"))
        if "funded" not in stage or not breached:
            continue
        terminal = any(word in state for word in ("breached", "closed", "archived"))
        if not terminal:
            continue
        closed = _recorded_time(account.get("closed_at"))
        if closed and closed < breached:
            continue
        times = paid_times.get((str(account.get("id") or ""), str(account.get("trader_id") or "")), [])
        if any(when < breached for when in times):
            return True
    return False


def build_report(purchases, accounts, start, end, payouts=None):
    for value in (start, end):
        if value:
            datetime.strptime(value, "%Y-%m-%d")
    if start and end and start > end:
        raise ValueError("Start date must be on or before end date")
    linked = defaultdict(list)
    by_id = {str(a.get("id")): a for a in accounts if a.get("id")}
    children = defaultdict(list)
    for a in accounts:
        source = str(a.get("source_account_id") or a.get("reset_source_account_id") or a.get("replaces_trader_account_id") or "")
        if source:
            children[source].append(a)
        pid = str(a.get("purchase_id") or a.get("challenge_purchase_id") or "")
        if pid:
            linked[pid].append(a)
    paid_by_account = defaultdict(list)
    paid_times = defaultdict(list)
    paid_seen = set()
    unlinked_paid = 0
    for payout in payouts or []:
        payout_id = str(payout.get("id") or "")
        if not payout_id or payout_id in paid_seen:
            continue
        paid_seen.add(payout_id)
        if str(payout.get("status") or "").strip().lower() != "paid":
            continue
        account_id = str(payout.get("trader_account_id") or payout.get("account_id") or "")
        account = by_id.get(account_id)
        if not account or str(payout.get("trader_id") or "") != str(account.get("trader_id") or ""):
            unlinked_paid += 1
            continue
        try:
            amount = Decimal(str(payout.get("amount") or 0))
        except (InvalidOperation, ValueError):
            raise ValueError("Invalid paid payout amount")
        if not amount.is_finite() or amount < 0:
            raise ValueError("Invalid paid payout amount")
        paid_by_account[account_id].append((payout_id, amount))
        paid_when = _recorded_time(payout.get("paid_at"))
        if paid_when:
            paid_times[(account_id, str(payout.get("trader_id") or ""))].append(paid_when)
    rows, seen, undated = [], set(), 0
    for p in purchases:
        pid = str(p.get("id") or "")
        if not pid or pid in seen:
            continue
        seen.add(pid)
        status = str(p.get("status") or "").lower()
        payment = str(p.get("payment_status") or "").lower()
        if status in ("rejected", "cancelled", "canceled") or payment in ("rejected", "refunded"):
            continue
        if payment not in ("approved", "paid") and status not in ("approved", "approved_active", "assigned", "completed", "approved_waiting_mt5"):
            continue
        kind = str(p.get("purchase_type") or p.get("type") or "").lower()
        if "reset" in kind or "[NP_RESET_REQUEST" in str(p.get("admin_note") or ""):
            continue
        day = sale_day(p)
        if not day:
            undated += 1
            continue
        if (start and day < start) or (end and day > end):
            continue
        journey = list(linked.get(pid, []))
        mirror = by_id.get(str(p.get("trader_account_id") or ""))
        if mirror and mirror not in journey and str(mirror.get("trader_id") or "") == str(p.get("trader_id") or "") and not (mirror.get("purchase_id") and str(mirror["purchase_id"]) != pid):
            journey.append(mirror)
        # Exact source links only. Names, emails and broker logins never merge sales.
        ids = {str(a.get("id")) for a in journey}
        queue = list(ids)
        while queue:
            for a in children.get(queue.pop(), []):
                aid = str(a.get("id") or "")
                if not aid or aid in ids or str(a.get("trader_id") or "") != str(p.get("trader_id") or ""):
                    continue
                other = str(a.get("purchase_id") or "")
                if other and other != pid:
                    continue
                journey.append(a); ids.add(aid); queue.append(aid)
        breached = passed = funded = funded_breached = False
        for a in journey:
            stage = str(a.get("stage") or a.get("phase") or "").lower().replace("_", "").replace(" ", "")
            state = " ".join(str(a.get(k) or "").lower() for k in ("status", "account_status", "lifecycle_state"))
            account_breached = bool(a.get("breached_at") or a.get("breach_at") or "breach" in state)
            breached |= account_breached
            funded_breached |= account_breached and "funded" in stage
            issued = bool(a.get("mt5_login"))
            funded |= issued and "funded" in stage
            passed |= bool(a.get("passed_at") or "passed" in state or (issued and ("phase2" in stage or "funded" in stage)))
        paid_records = {paid_id: amount for aid in ids for paid_id, amount in paid_by_account.get(aid, [])}
        payout_amount = float(sum(paid_records.values(), Decimal("0")).quantize(Decimal("0.01")))
        rows.append({"purchase_id": pid, "sale_date": day, "plan": str(p.get("plan_name") or p.get("plan") or "Unspecified"), "account_size": p.get("account_size") or 0, "mt5_login": str(p.get("mt5_login") or (journey[0].get("mt5_login") if journey else "") or ""), "breached": breached, "passed": passed, "funded": funded, "funded_breached": funded_breached, "collected_payout": bool(paid_records), "paid_then_breached_closed": _paid_then_breached_closed(journey, paid_times) if paid_records else False, "paid_payout_count": len(paid_records), "payout_amount": payout_amount, "assigned": any(a.get("mt5_login") for a in journey)})
    for row in rows:
        row["outcome"] = outcome_for(row)
        row["outcome_label"] = OUTCOMES[row["outcome"]]
    def totals(group):
        sold = len(group)
        result = {"sold": sold}
        for key in ("breached", "passed", "funded", "funded_breached", "collected_payout", "paid_then_breached_closed", "assigned"):
            result[key] = sum(bool(r[key]) for r in group)
            result[key + "_rate"] = round(result[key] * 100 / sold, 1) if sold else 0
        result["paid_payout_count"] = sum(r["paid_payout_count"] for r in group)
        result["payout_amount"] = float(sum((Decimal(str(r["payout_amount"])) for r in group), Decimal("0")))
        result["awaiting_assignment"] = sold - result["assigned"]
        result["no_milestone"] = sum(not (r["breached"] or r["passed"] or r["funded"]) for r in group)
        result["outcomes"] = {key: sum(r["outcome"] == key for r in group) for key in OUTCOMES}
        result["not_funded_yet"] = sum(result["outcomes"][key] for key in ("awaiting_account", "no_recorded_result", "passed_not_funded"))
        result["failed_before_funded"] = result["outcomes"]["failed_before_funded"]
        result["needs_review"] = result["outcomes"]["needs_review"]
        result["reconciliation_total"] = sum(result["outcomes"].values())
        if result["reconciliation_total"] != sold:
            raise ValueError("Cohort outcomes do not reconcile")
        return result
    plans = defaultdict(list)
    for row in rows:
        plans[row["plan"]].append(row)
    return {"success": True, "from": start, "to": end, "timezone": "Africa/Lagos", "summary": totals(rows), "outcome_labels": OUTCOMES, "plans": [{"plan": name, **totals(group)} for name, group in sorted(plans.items())], "rows": sorted(rows, key=lambda r: (r["sale_date"], r["purchase_id"]), reverse=True), "undated_sales_excluded": undated, "unlinked_paid_records": unlinked_paid, "definition": "Approved original purchases by transaction date. Milestones cover their linked account history up to now; passed means at least one evaluation phase passed. Counts overlap: a funded trader can receive payouts and later breach. Collected payouts are records marked paid, linked by exact account and owner; pending/approved requests are excluded. Amounts cover payments up to now for the selected sales cohort, not payments made only within the sale dates. Resets are excluded as new sales."}
