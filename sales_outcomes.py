"""Read-only purchase cohort analytics. No lifecycle changes or notifications."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

TZ = ZoneInfo("Africa/Lagos")

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

def build_report(purchases, accounts, start, end):
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
        breached = passed = funded = False
        for a in journey:
            stage = str(a.get("stage") or a.get("phase") or "").lower().replace("_", "").replace(" ", "")
            state = " ".join(str(a.get(k) or "").lower() for k in ("status", "account_status", "lifecycle_state"))
            breached |= bool(a.get("breached_at") or a.get("breach_at") or "breach" in state)
            issued = bool(a.get("mt5_login"))
            funded |= issued and "funded" in stage
            passed |= bool(a.get("passed_at") or "passed" in state or (issued and ("phase2" in stage or "funded" in stage)))
        rows.append({"purchase_id": pid, "sale_date": day, "plan": str(p.get("plan_name") or p.get("plan") or "Unspecified"), "account_size": p.get("account_size") or 0, "mt5_login": str(p.get("mt5_login") or (journey[0].get("mt5_login") if journey else "") or ""), "breached": breached, "passed": passed, "funded": funded, "assigned": any(a.get("mt5_login") for a in journey)})
    def totals(group):
        sold = len(group)
        result = {"sold": sold}
        for key in ("breached", "passed", "funded", "assigned"):
            result[key] = sum(bool(r[key]) for r in group)
            result[key + "_rate"] = round(result[key] * 100 / sold, 1) if sold else 0
        result["awaiting_assignment"] = sold - result["assigned"]
        result["no_milestone"] = sum(not (r["breached"] or r["passed"] or r["funded"]) for r in group)
        return result
    plans = defaultdict(list)
    for row in rows:
        plans[row["plan"]].append(row)
    return {"success": True, "from": start, "to": end, "timezone": "Africa/Lagos", "summary": totals(rows), "plans": [{"plan": name, **totals(group)} for name, group in sorted(plans.items())], "rows": sorted(rows, key=lambda r: (r["sale_date"], r["purchase_id"]), reverse=True), "undated_sales_excluded": undated, "definition": "Approved original purchases by transaction date. Milestones cover their linked account history up to now; passed means at least one evaluation phase passed. Counts overlap: a funded trader can later breach. Resets are excluded as new sales."}
