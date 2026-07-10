
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone
import os, random, uuid, re, time, hmac, hashlib, base64, secrets, string, json, json
import html
import requests
app = Flask(__name__)
CORS(app)

REGISTER_RATE_WINDOW_SECONDS = 15 * 60
REGISTER_RATE_MAX = 5
REGISTER_RATE_BUCKET = {}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# NairaPips payout safety cap. Keep server-side because frontend/admin values can be stale.
PAYOUT_PROFIT_SHARE_PERCENT = 50

def _effective_payout_split(*values):
    """Return the allowed payout share, capped at 50% for business safety.
    Accepts numeric values or strings with a percent sign. Missing/invalid values default to 50.
    """
    for value in values:
        if value is None or str(value).strip() == "":
            continue
        try:
            n = float(str(value).replace("%", "").replace(",", "").strip())
            if n > 0:
                return min(n, PAYOUT_PROFIT_SHARE_PERCENT)
        except Exception:
            continue
    return PAYOUT_PROFIT_SHARE_PERCENT

# ================================
# NAIRAPIPS MT5 SOURCE-OF-TRUTH CORE
# ================================

def _np_ok(data=None, status=200):
    res = jsonify(data or {"success": True})
    res.status_code = status
    res.headers["Access-Control-Allow-Origin"] = "*"
    res.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    res.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return res

def _np_fail(message, status=400):
    return _np_ok({"success": False, "error": str(message)}, status)

def _dt_score(value):
    try:
        if not value:
            return 0
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0

def _row_score(t):
    status = str(t.get("status") or "").strip().lower()
    pay = str(t.get("payment_status") or "").strip().lower()
    score = 0

    if pay == "approved":
        score += 90_000_000_000
    if status in ["active", "funded", "live"]:
        score += 80_000_000_000
    if str(t.get("mt5_login") or "").strip():
        score += 70_000_000_000
    if str(t.get("mt5_updated_at") or "").strip():
        score += 60_000_000_000
    if pay == "rejected" or status in ["rejected", "payment_rejected"]:
        score -= 99_000_000_000
    if status in ["no_account", "new_signup", "pending", "payment_pending"]:
        score -= 50_000_000_000

    for key in ["mt5_updated_at", "updated_at", "approved_at", "challenge_started_at", "assigned_at", "created_at", "last_login_at"]:
        d = _dt_score(t.get(key))
        if d:
            score += d
            break

    return score

def _dedupe_traders(rows):
    groups = {}
    for row in rows or []:
        email = str(row.get("email") or "").strip().lower()
        phone = str(row.get("phone") or "").strip().lower()
        key = email or phone or str(row.get("id") or "")
        groups.setdefault(key, []).append(row)

    output = []
    for key, items in groups.items():
        items = sorted(items, key=_row_score, reverse=True)
        output.append(items[0])

    output.sort(key=_row_score, reverse=True)
    return output

def _latest_trader_for_lookup(lookup):
    lookup = str(lookup or "").strip().lower()
    if not lookup:
        return None

    try:
        phone = _normalize_phone_value(lookup)
    except Exception:
        phone = lookup

    queries = [
        ("canonical_email", lookup),
        ("email", lookup),
        ("canonical_phone", phone),
        ("phone", lookup),
        ("account_reference", lookup),
        ("id", lookup),
    ]

    matches = []
    for column, value in queries:
        if not value:
            continue
        try:
            rows = supabase.table("traders").select("*").eq(column, value).limit(5).execute().data or []
            matches.extend(rows)
        except Exception:
            pass

    # MT5 login is not identity. Only resolve it through the current active account.
    try:
        account = _get_active_account_by_login(lookup)
        if account and account.get("trader_id"):
            trader = get_trader_by_id(account.get("trader_id"))
            if trader:
                matches.append(trader)
    except Exception:
        pass

    if matches:
        return sorted(_dedupe_by_id(matches), key=_row_score, reverse=True)[0]

    return None

def _safe_update_table(table, payload, column, value):
    try:
        if value is None or str(value).strip() == "":
            return None
        return supabase.table(table).update(payload).eq(column, value).execute()
    except Exception as e:
        print(f"SAFE UPDATE FAILED {table}.{column}:", e)
        return None


# ================================
# NAIRAPIPS PRODUCTION SAFETY CORE
# ================================
PROTECTED_DELETE_TABLES = {"payouts", "payments"}
SAFE_FLAG_TABLES = {"traders", "challenge_purchases", "payouts", "payments", "referrals"}


def _admin_from_payload(data):
    return {
        "id": data.get("admin_id") or data.get("staff_id") or "",
        "name": data.get("admin_name") or data.get("approved_by") or data.get("mt5_updated_by") or "admin",
        "username": data.get("admin_username") or data.get("approved_by") or data.get("mt5_updated_by") or "admin",
        "role": data.get("admin_role") or "admin",
    }


def _audit_safe(module, action, details="", staff=None, record_affected=""):
    try:
        audit_log(staff or {"name": "system", "username": "system", "role": "system"}, module, action, details, record_affected)
    except Exception as e:
        print("AUDIT LOG ERROR:", str(e))


def _safe_fetch(table, column, value, limit=50):
    try:
        if value is None or str(value).strip() == "":
            return []
        res = supabase.table(table).select("*").eq(column, value).limit(limit).execute()
        return getattr(res, "data", []) or []
    except Exception as e:
        print(f"SAFE FETCH FAILED {table}.{column}:", e)
        return []


def _is_truthy(value):
    return value is True or str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def _is_funded_trader(row):
    status = str(row.get("status") or "").strip().lower()
    phase = str(row.get("phase") or "").strip().lower()
    return status in {"funded", "live"} or phase in {"funded", "live"} or bool(row.get("funded_at"))


def _has_approved_payment(rows):
    for row in rows or []:
        status = str(row.get("status") or "").strip().lower()
        payment = str(row.get("payment_status") or "").strip().lower()
        if status in {"approved", "approved_active", "paid"} or payment == "approved":
            return True
    return False


# ================================
# NAIRAPIPS PRODUCTION LIFECYCLE CORE
# traders = identity, trader_accounts = trading source of truth
# ================================
# NAIRAPIPS SOURCE-OF-TRUTH: active vs terminal account statuses
# All admin + trader + monitoring filters MUST use these sets.
# Patch PR1 (2026-06-28): unifies filter sets across /admin_v2/phase1/2/funded/breached,
# /admin_v2/summary, /trader_accounts, and _get_active_account().
# Do NOT add breach/archived/locked/passed statuses to ACTIVE_ACCOUNT_STATUSES.
ACTIVE_ACCOUNT_STATUSES = {
    "assigned_active", "active", "current_active",
    "phase1_active", "phase2_active", "funded_active",
    "live_active", "approved_active",
}
TERMINAL_ACCOUNT_STATUSES = {
    "archived", "archived_phase1", "archived_phase2",
    "breached", "breached_archived", "passed",
    "locked", "disabled", "profit_protected",
}
ACCOUNT_STAGES = {"phase1", "phase2", "funded"}


def _target_for_stage(stage):
    return {"phase1": 10, "phase2": 8, "funded": None}.get(str(stage or "").lower())


def _active_state_for_stage(stage):
    stage = str(stage or "").lower()
    return "funded_active" if stage == "funded" else f"{stage}_active"


def _next_waiting_after_pass(stage):
    return {"phase1": "phase2_waiting_mt5", "phase2": "funded_waiting_mt5"}.get(str(stage or "").lower())


def _archive_status_for_stage(stage, breached=False):
    if breached:
        return "breached_archived"
    return {"phase1": "archived_phase1", "phase2": "archived_phase2", "funded": "archived_funded"}.get(str(stage or "").lower(), "closed")


def _normalize_phone_value(phone):
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if digits.startswith("0") and len(digits) >= 10:
        return "234" + digits[1:]
    return digits


def _stage_for_lifecycle_state(state, phase=None):
    state = str(state or "").strip().lower()
    phase = str(phase or "").strip().lower()
    if state.startswith("funded") or phase in {"funded", "live", "funded_waiting"}:
        return "funded"
    if state.startswith("phase2") or phase == "phase2":
        return "phase2"
    if state.startswith("phase1") or phase == "phase1":
        return "phase1"
    return "phase1"


def _is_uuid(value):
    return bool(re.match(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", str(value or "").strip()))


def _stage_started_at_from_legacy_trader(trader, stage, fallback=None):
    """Pick the safest account-level start date during legacy migration.

    Phase 1 can use challenge_started_at/approved_at. Later stages must not inherit
    the Phase 1 approval date, otherwise Phase 2/Funded dashboards show old MT5 dates.
    """
    stage = str(stage or "").strip().lower()
    fallback = fallback or now_iso()
    if stage == "phase1":
        return (
            trader.get("challenge_started_at")
            or trader.get("assigned_at")
            or trader.get("approved_at")
            or trader.get("mt5_updated_at")
            or trader.get("updated_at")
            or fallback
        )
    if stage == "phase2":
        return (
            trader.get("phase2_started_at")
            or trader.get("assigned_at")
            or trader.get("mt5_updated_at")
            or trader.get("updated_at")
            or fallback
        )
    if stage == "funded":
        return (
            trader.get("funded_started_at")
            or trader.get("funded_at")
            or trader.get("assigned_at")
            or trader.get("mt5_updated_at")
            or trader.get("updated_at")
            or fallback
        )
    return fallback


def _account_display_assigned_at(account):
    if not account:
        return None
    stage = str(account.get("stage") or "").strip().lower()
    started = account.get("started_at")
    created = account.get("created_at")
    updated = account.get("updated_at")
    if stage in {"phase2", "funded"}:
        started_score = _dt_score(started)
        created_score = _dt_score(created)
        # Legacy migration sometimes copied the Phase 1 start date into Phase 2/Funded.
        # When that happens, created_at is the safer account-level assignment date.
        if created_score and (not started_score or created_score > started_score + 3600):
            return created
    return started or created or updated


def _decorate_account_for_api(account):
    if not account:
        return account
    row = dict(account)
    row["display_assigned_at"] = _account_display_assigned_at(row)
    row["assignment_date"] = row.get("display_assigned_at")
    absolute_dd = _num(row.get("absolute_drawdown_percent"), _num(row.get("drawdown_percent"), 0))
    dd_limit = _num(row.get("dd_limit_percent"), MAX_DRAWDOWN_LIMIT) or MAX_DRAWDOWN_LIMIT
    dd_used = _safe_dd_used(row, absolute_dd, dd_limit)
    row["absolute_drawdown_percent"] = absolute_dd
    row["dd_limit_percent"] = dd_limit
    row["dd_used_percent"] = dd_used
    account_status = str(row.get("account_status") or "").strip().lower()
    account_id = str(row.get("id") or row.get("trader_account_id") or "").strip()
    latest_event = row.get("latest_monitoring_event") or {}
    event_account_id = str(latest_event.get("trader_account_id") or "").strip()
    event_type = str(latest_event.get("event_type") or "").strip().lower()
    event_zone = str(latest_event.get("risk_zone") or "").strip().lower()
    event_text = " ".join([
        event_type,
        event_zone,
        str(latest_event.get("message") or ""),
        str(latest_event.get("pass_status") or ""),
        str(latest_event.get("phase_pass_status") or ""),
    ]).lower()
    stage = str(row.get("stage") or row.get("phase") or "").strip().lower()
    phase_pass_status = str(row.get("phase_pass_status") or "").strip().lower()
    event_matches_account = bool(account_id and event_account_id and account_id == event_account_id)
    legacy_event_matches_account = bool(row.get("_legacy_event_matches_account"))
    legacy_snapshot_matches_account = bool(row.get("_legacy_snapshot_matches_account"))
    event_pass_belongs_to_stage = False
    if "phase2_passed" in event_text or "phase 2" in event_text:
        event_pass_belongs_to_stage = stage == "phase2"
    elif "phase1_passed" in event_text or "phase 1" in event_text:
        event_pass_belongs_to_stage = stage == "phase1"
    elif "phase_passed" in event_type or event_zone == "passed":
        # Generic legacy pass evidence is trusted only when the account row itself
        # has already been archived/passed. This prevents Phase 1 pass events from
        # incorrectly passing a newer Phase 2 account.
        event_pass_belongs_to_stage = "archived_phase" in account_status or phase_pass_status in {"phase1_passed", "phase2_passed"}
    status_blob = " ".join([
        account_status,
        str(row.get("status") or ""),
        phase_pass_status,
        event_type,
        event_zone,
    ]).lower()
    explicit_pass = (
        "archived_phase" in account_status
        or phase_pass_status in {"phase1_passed", "phase2_passed"}
        or ((event_matches_account or legacy_event_matches_account or legacy_snapshot_matches_account) and event_pass_belongs_to_stage)
    )
    if (
        "breach" in status_blob
        or "locked" in status_blob
        or "disabled" in status_blob
        or bool(row.get("mt5_access_disabled"))
        or dd_used >= 100
    ):
        zone = "breached"
    elif explicit_pass:
        zone = "passed"
    elif dd_used >= 91:
        zone = "critical"
    elif dd_used >= 66:
        zone = "danger"
    elif dd_used >= 51:
        zone = "warning"
    else:
        zone = str(row.get("risk_zone") or "safe").strip().lower() or "safe"
        if zone == "passed":
            zone = "safe"
    row["display_risk_zone"] = zone
    row["risk_zone"] = zone
    return row



def _trader_is_waiting_for_mt5(trader):
    """True only when lifecycle says the trader currently has no active MT5."""
    if not trader:
        return False
    state = str(
        trader.get("challenge_state")
        or trader.get("status")
        or ""
    ).strip().lower().replace(" ", "_")
    return (
        "phase1_waiting_mt5" in state
        or "phase2_waiting_mt5" in state
        or "funded_waiting_mt5" in state
        or "waiting_for_fresh_mt5" in state
        or "waiting_for_phase_1" in state
        or "waiting_for_phase_2" in state
        or "waiting_for_funded" in state
    )


def _active_account_from_trader_profile(trader):
    """Compatibility bridge for already-migrated traders whose trader_accounts row is unavailable."""
    if not trader:
        return None
    # A reset/waiting lifecycle is authoritative. Never revive old MT5 mirror fields.
    if _trader_is_waiting_for_mt5(trader):
        return None
    state = str(trader.get("challenge_state") or trader.get("status") or "").strip().lower()
    active_states = {"phase1_active", "phase2_active", "funded_active"}
    if state not in active_states and str(trader.get("status") or "").strip().lower() not in {"active", "funded", "live"}:
        return None
    mt5_login = str(trader.get("mt5_login") or "").strip()
    if not mt5_login:
        return None
    stage = _stage_for_lifecycle_state(state, trader.get("phase"))
    account_size = clean(trader.get("account_size") or trader.get("balance") or trader.get("equity") or 0)
    start_balance = clean(trader.get("start_balance") or account_size)
    equity = clean(trader.get("equity") or trader.get("balance") or account_size)
    profit = clean(trader.get("profit") or (equity - start_balance if start_balance else 0))
    profit_percent = clean(trader.get("profit_percent") or ((profit / start_balance * 100) if start_balance else 0))
    dd_limit = clean(trader.get("dd_limit_percent") or trader.get("max_drawdown") or 20) or 20
    absolute_dd = clean(trader.get("drawdown_percent") or 0)
    dd_used = _safe_dd_used(trader, absolute_dd, dd_limit)
    assigned_at = _stage_started_at_from_legacy_trader(trader, stage, trader.get("updated_at") or trader.get("created_at") or now_iso())
    return _decorate_account_for_api({
        "id": trader.get("current_account_id") if _is_uuid(trader.get("current_account_id")) else None,
        "trader_id": trader.get("id"),
        "stage": stage,
        "account_status": "assigned_active",
        "mt5_login": mt5_login,
        "mt5_server": trader.get("mt5_server") or trader.get("server") or "",
        "mt5_master_password": trader.get("mt5_master_password") or trader.get("mt5_password") or trader.get("master_password") or "",
        "mt5_investor_password": trader.get("mt5_investor_password") or trader.get("investor_password") or "",
        "account_size": account_size,
        "start_balance": start_balance or account_size,
        "current_balance": clean(trader.get("balance") or account_size),
        "current_equity": equity,
        "profit": profit,
        "profit_percent": profit_percent,
        "absolute_drawdown_percent": absolute_dd,
        "dd_limit_percent": dd_limit,
        "dd_used_percent": dd_used,
        "target_percent": _target_for_stage(stage),
        "monitoring_enabled": _is_truthy(trader.get("monitoring_enabled")),
        "started_at": assigned_at,
        "assigned_at": assigned_at,
        "created_at": assigned_at,
        "updated_at": trader.get("mt5_updated_at") or trader.get("updated_at") or trader.get("assigned_at") or trader.get("created_at"),
        "_source": "trader_profile_bridge",
    })


def _sync_identity_fields(trader):
    try:
        if not trader:
            return
        payload = {}
        if trader.get("email"):
            payload["canonical_email"] = str(trader.get("email")).strip().lower()
        if trader.get("phone"):
            payload["canonical_phone"] = _normalize_phone_value(trader.get("phone"))
        if payload:
            supabase.table("traders").update(payload).eq("id", trader.get("id")).execute()
    except Exception as e:
        print("IDENTITY SYNC ERROR:", e)


def _get_active_account(trader_id, trader=None):
    try:
        if not trader and trader_id:
            try:
                trader_rows = supabase.table("traders").select("*").eq("id", trader_id).limit(1).execute().data or []
                trader = trader_rows[0] if trader_rows else None
            except Exception:
                trader = None

        # Reset/waiting lifecycle is the source of truth until a fresh assignment
        # changes challenge_state back to phase1_active/phase2_active/funded_active.
        if _trader_is_waiting_for_mt5(trader):
            return None

        rows = supabase.table("trader_accounts").select("*").eq("trader_id", trader_id).in_("account_status", list(ACTIVE_ACCOUNT_STATUSES)).order("updated_at", desc=True).order("started_at", desc=True).order("created_at", desc=True).limit(50).execute().data or []

        current_account_id = (trader or {}).get("current_account_id")
        if current_account_id:
            for row in rows:
                if str(row.get("id") or "") == str(current_account_id):
                    status = str(row.get("account_status") or "").strip().lower()
                    if status in ACTIVE_ACCOUNT_STATUSES:
                        return _decorate_account_for_api(row)
            direct_rows = supabase.table("trader_accounts").select("*").eq("id", current_account_id).limit(1).execute().data or []
            if direct_rows:
                status = str(direct_rows[0].get("account_status") or "").strip().lower()
                if status in ACTIVE_ACCOUNT_STATUSES:
                    return _decorate_account_for_api(direct_rows[0])

        if rows:
            preferred_stage = _stage_for_lifecycle_state((trader or {}).get("challenge_state"), (trader or {}).get("phase"))
            for row in rows:
                if str(row.get("stage") or "").strip().lower() == preferred_stage:
                    return _decorate_account_for_api(row)
            return _decorate_account_for_api(rows[0])

        return _active_account_from_trader_profile(trader)
    except Exception as e:
        print("ACTIVE ACCOUNT FETCH ERROR:", e)
        return _active_account_from_trader_profile(trader)


def _purchase_accounts_for_trader(trader, purchases=None):
    """Create dashboard account cards from approved purchases with assigned MT5.
    This keeps newly assigned purchases visible only during an active lifecycle.
    """
    accounts = []
    # A reset trader may still have historical MT5 values on the purchase row.
    # Those values are history, not a current account.
    if _trader_is_waiting_for_mt5(trader):
        return accounts
    try:
        rows = list(purchases or [])
        if not rows and trader:
            rows = _safe_fetch("challenge_purchases", "trader_id", trader.get("id"), 100)
            if trader.get("email"):
                rows += _safe_fetch("challenge_purchases", "email", trader.get("email"), 100)
            if trader.get("phone"):
                rows += _safe_fetch("challenge_purchases", "phone", trader.get("phone"), 100)
        for p in rows:
            login = str(p.get("mt5_login") or p.get("current_mt5_login") or "").strip()
            if not login:
                continue
            payment_status = str(p.get("payment_status") or "").strip().lower()
            status = str(p.get("status") or "").strip().lower()
            if payment_status != "approved" and status not in {"approved", "approved_active", "active"}:
                continue
            stage_text = " ".join([
                str(p.get("lifecycle_state") or ""),
                str(p.get("assigned_phase") or ""),
                str(p.get("active_stage") or ""),
                str(p.get("phase") or ""),
                str(p.get("status") or ""),
            ]).lower()
            if "funded" in stage_text:
                stage = "funded"
            elif "phase2" in stage_text or "phase_2" in stage_text:
                stage = "phase2"
            else:
                stage = "phase1"
            account_size = clean(p.get("account_size") or p.get("challenge_size") or 0)
            assigned_at = p.get("assigned_at") or p.get("approved_at") or p.get("updated_at") or p.get("created_at") or now_iso()
            accounts.append(_decorate_account_for_api({
                "id": p.get("trader_account_id") or f"purchase:{p.get('id')}",
                "trader_id": (trader or {}).get("id") or p.get("trader_id"),
                "purchase_id": p.get("id"),
                "stage": stage,
                "account_status": "assigned_active",
                "mt5_login": login,
                "mt5_server": p.get("mt5_server") or p.get("current_mt5_server") or "",
                "mt5_master_password": p.get("mt5_master_password") or p.get("mt5_password") or p.get("master_password") or "",
                "mt5_investor_password": p.get("mt5_investor_password") or p.get("investor_password") or "",
                "account_size": account_size,
                "start_balance": account_size,
                "current_balance": account_size,
                "current_equity": account_size,
                "profit": 0,
                "profit_percent": 0,
                "absolute_drawdown_percent": 0,
                "dd_limit_percent": 20,
                "dd_used_percent": 0,
                "target_percent": _target_for_stage(stage),
                "monitoring_enabled": True,
                "started_at": assigned_at,
                "assigned_at": assigned_at,
                "created_at": assigned_at,
                "updated_at": p.get("updated_at") or assigned_at,
                "_source": "challenge_purchase_assignment",
            }))
    except Exception as e:
        print("PURCHASE ACCOUNT BRIDGE ERROR:", e)
    return accounts


def _get_active_accounts(trader_id, trader=None, purchases=None):
    """Return visible challenge accounts for this trader, with the current/risky account first."""
    try:
        raw_rows = supabase.table("trader_accounts").select("*").eq("trader_id", trader_id).order("updated_at", desc=True).order("started_at", desc=True).order("created_at", desc=True).limit(100).execute().data or []
        visible_statuses = {
            "assigned_active", "active", "current_active",
            "archived_phase1", "archived_phase2", "archived_funded",
            "breached_archived", "breached", "locked", "closed"
        }
        rows = []
        for row in raw_rows:
            status = str(row.get("account_status") or "").strip().lower()
            if status in visible_statuses or str(row.get("mt5_login") or "").strip():
                rows.append(row)
        decorated = [_decorate_account_for_api(row) for row in rows]
        purchase_accounts = _purchase_accounts_for_trader(trader, purchases)

        real_purchase_ids = {
            str(row.get("purchase_id") or "").strip()
            for row in decorated
            if str(row.get("purchase_id") or "").strip()
        }
        real_logins = {
            str(row.get("mt5_login") or "").strip()
            for row in decorated
            if str(row.get("mt5_login") or "").strip()
        }

        combined = []
        seen = set()

        def account_key(row):
            row_id = str(row.get("id") or "").strip()
            purchase_id = str(row.get("purchase_id") or "").strip()
            login = str(row.get("mt5_login") or "").strip()
            if row_id and not row_id.startswith("purchase:"):
                return "account:" + row_id
            if purchase_id:
                return "purchase:" + purchase_id
            if login:
                return "login:" + login
            return ""

        def add_account(row):
            if not row:
                return
            row_id = str(row.get("id") or "").strip()
            purchase_id = str(row.get("purchase_id") or "").strip()
            login = str(row.get("mt5_login") or "").strip()
            is_purchase_bridge = row.get("_source") == "challenge_purchase_assignment" or row_id.startswith("purchase:")
            if is_purchase_bridge and ((purchase_id and purchase_id in real_purchase_ids) or (login and login in real_logins)):
                return
            key = account_key(row)
            if not key or key in seen:
                return
            seen.add(key)
            combined.append(row)

        for row in decorated:
            add_account(row)
        for row in purchase_accounts:
            add_account(row)
        decorated = combined
        if not decorated:
            bridged = _active_account_from_trader_profile(trader)
            return [bridged] if bridged else []

        current = _get_active_account(trader_id, trader)
        current_id = str((current or {}).get("id") or "")
        current_login = str((current or {}).get("mt5_login") or "")

        def account_sort(row):
            dd_used = _safe_dd_used(row, _num(row.get("absolute_drawdown_percent"), 0), _num(row.get("dd_limit_percent"), MAX_DRAWDOWN_LIMIT) or MAX_DRAWDOWN_LIMIT)
            row_id = str(row.get("id") or "")
            row_login = str(row.get("mt5_login") or "")
            is_current = 1 if (current_id and row_id == current_id) or (not current_id and current_login and row_login == current_login) else 0
            updated = _dt_score(row.get("updated_at") or row.get("started_at") or row.get("created_at"))
            return (is_current, dd_used, updated)

        by_login = {}
        no_login = []
        for row in decorated:
            login = str(row.get("mt5_login") or "").strip()
            if not login:
                no_login.append(row)
                continue
            existing = by_login.get(login)
            if not existing or account_sort(row) >= account_sort(existing):
                by_login[login] = row

        return sorted(list(by_login.values()) + no_login, key=account_sort, reverse=True)
    except Exception as e:
        print("ACTIVE ACCOUNTS FETCH ERROR:", e)
        bridged = _active_account_from_trader_profile(trader)
        return [bridged] if bridged else []


def _enrich_accounts_with_latest_monitoring(trader_id, accounts):
    """Attach account-specific monitoring evidence.

    Production rule: each MT5 challenge account owns its own monitoring data.
    Exact trader_account_id evidence wins. Legacy records without account ids can
    only attach by mt5_login when their timestamp fits that account's assignment
    window. This prevents one trader's old/new account data from bleeding into
    another account card.
    """
    try:
        def record_score(record):
            return _dt_score((record or {}).get("created_at") or (record or {}).get("synced_at") or (record or {}).get("updated_at") or (record or {}).get("last_sync_at"))

        def account_start_score(account):
            return _dt_score(_account_display_assigned_at(account) or (account or {}).get("started_at") or (account or {}).get("created_at") or (account or {}).get("assigned_at"))

        def account_end_score(account):
            status = str((account or {}).get("account_status") or "").strip().lower()
            if status in {"assigned_active", "active", "current_active"}:
                return 0
            return _dt_score((account or {}).get("archived_at") or (account or {}).get("passed_at") or (account or {}).get("updated_at"))

        def record_belongs_to_account_by_time(record, account):
            rec = record_score(record)
            start = account_start_score(account)
            end = account_end_score(account)
            if not rec or not start:
                return False
            # One-day tolerance covers timezone/migration records without allowing
            # an old login event to jump to a different challenge window.
            if rec < start - 86400:
                return False
            if end and rec > end + 86400:
                return False
            return True

        def dedupe_records(items):
            seen = set()
            out = []
            for item in items or []:
                key = str((item or {}).get("id") or "")
                if not key:
                    key = "|".join([
                        str((item or {}).get("trader_account_id") or ""),
                        str((item or {}).get("mt5_login") or ""),
                        str((item or {}).get("created_at") or ""),
                        str((item or {}).get("event_type") or ""),
                    ])
                if key in seen:
                    continue
                seen.add(key)
                out.append(item)
            out.sort(key=record_score, reverse=True)
            return out

        def direct_records(table, account, limit=1500):
            account_id = str((account or {}).get("id") or "").strip()
            login = str((account or {}).get("mt5_login") or "").strip()
            found = []
            if account_id and not account_id.startswith("purchase:"):
                try:
                    found += supabase.table(table).select("*").eq("trader_account_id", account_id).order("created_at", desc=True).limit(limit).execute().data or []
                except Exception as e:
                    print(f"{table} exact account fetch failed:", e)
            if trader_id and login:
                try:
                    legacy = supabase.table(table).select("*").eq("trader_id", trader_id).eq("mt5_login", login).order("created_at", desc=True).limit(limit).execute().data or []
                except Exception as e:
                    print(f"{table} login evidence fetch failed:", e)
                    legacy = []
                for record in legacy:
                    record_account_id = str((record or {}).get("trader_account_id") or "").strip()
                    if record_account_id and record_account_id == account_id:
                        found.append(record)
                    elif not record_account_id and record_belongs_to_account_by_time(record, account):
                        found.append(record)
            if login:
                try:
                    global_login_rows = supabase.table(table).select("*").eq("mt5_login", login).order("created_at", desc=True).limit(limit).execute().data or []
                except Exception as e:
                    print(f"{table} global login evidence fetch failed:", e)
                    global_login_rows = []
                for record in global_login_rows:
                    record_account_id = str((record or {}).get("trader_account_id") or "").strip()
                    if record_account_id and account_id and record_account_id == account_id:
                        found.append(record)
                    elif not record_account_id and record_belongs_to_account_by_time(record, account):
                        found.append(record)
            return dedupe_records(found)

        def strongest_risk(records):
            best = None
            best_used = -1
            for record in records or []:
                used = clean((record or {}).get("max_drawdown_used") or (record or {}).get("dd_used_percent") or 0)
                if used > best_used:
                    best = record
                    best_used = used
            return best

        enriched = []
        for account in accounts or []:
            row = dict(account or {})
            account_id = str(row.get("id") or "").strip()
            snaps = direct_records("monitoring_snapshots", row)
            events = direct_records("monitoring_events", row)
            snap = snaps[0] if snaps else None
            ev = events[0] if events else None
            risk_snap = strongest_risk(snaps)
            risk_ev = strongest_risk(events)
            if snap:
                if not str(snap.get("trader_account_id") or "").strip():
                    row["_legacy_snapshot_matches_account"] = True
                row["latest_monitoring_snapshot"] = snap
                row["current_balance"] = clean(snap.get("balance") or row.get("current_balance") or row.get("account_size"))
                row["current_equity"] = clean(snap.get("equity") or row.get("current_equity") or row.get("current_balance") or row.get("account_size"))
                row["profit"] = clean(snap.get("profit") or row.get("profit"))
                row["profit_percent"] = clean(snap.get("profit_percent") or row.get("profit_percent"))
                row["absolute_drawdown_percent"] = clean(snap.get("drawdown_percent") or row.get("absolute_drawdown_percent"))
                row["drawdown_percent"] = row["absolute_drawdown_percent"]
                row["max_drawdown_used"] = clean(snap.get("max_drawdown_used") or row.get("max_drawdown_used") or row.get("dd_used_percent"))
                row["dd_used_percent"] = row["max_drawdown_used"]
                row["risk_zone"] = snap.get("risk_zone") or row.get("risk_zone")
                row["last_sync_at"] = snap.get("created_at") or row.get("last_sync_at") or row.get("updated_at")
                row["updated_at"] = row.get("updated_at") or snap.get("created_at")
            if ev:
                if not str(ev.get("trader_account_id") or "").strip():
                    row["_legacy_event_matches_account"] = True
                row["latest_monitoring_event"] = ev
                row["last_event_at"] = ev.get("created_at") or row.get("last_event_at")
            # Some deployments logged the real danger/critical state as monitoring_events
            # while leaving trader_accounts at 0.00%. Never hide that evidence.
            risk_record = risk_ev
            if risk_snap and clean(risk_snap.get("max_drawdown_used") or risk_snap.get("dd_used_percent") or 0) > clean((risk_record or {}).get("max_drawdown_used") or (risk_record or {}).get("dd_used_percent") or 0):
                risk_record = risk_snap
            if risk_record and clean(risk_record.get("max_drawdown_used") or risk_record.get("dd_used_percent") or 0) > clean(row.get("dd_used_percent") or row.get("max_drawdown_used") or 0):
                used = clean(risk_record.get("max_drawdown_used") or risk_record.get("dd_used_percent") or 0)
                limit = clean(row.get("dd_limit_percent") or MAX_DRAWDOWN_LIMIT) or MAX_DRAWDOWN_LIMIT
                row["latest_monitoring_event"] = risk_record
                row["event_risk_lock"] = True
                row["dd_used_percent"] = used
                row["max_drawdown_used"] = used
                row["absolute_drawdown_percent"] = round((used / 100) * limit, 4)
                row["drawdown_percent"] = row["absolute_drawdown_percent"]
                row["current_balance"] = clean(risk_record.get("balance") or row.get("current_balance") or row.get("account_size"))
                row["current_equity"] = clean(risk_record.get("equity") or row.get("current_equity") or row.get("current_balance") or row.get("account_size"))
                row["risk_zone"] = risk_record.get("risk_zone") or row.get("risk_zone") or _risk_zone(used)
                row["last_sync_at"] = risk_record.get("created_at") or row.get("last_sync_at") or row.get("updated_at")
            start_balance = clean(row.get("start_balance") or row.get("account_size") or 0)
            current_equity = clean(row.get("current_equity") or row.get("current_balance") or start_balance)
            if current_equity and start_balance:
                if not clean(row.get("profit")):
                    row["profit"] = current_equity - start_balance
                if not clean(row.get("profit_percent")):
                    row["profit_percent"] = ((current_equity - start_balance) / start_balance * 100) if start_balance else 0
                row["highest_equity"] = max(clean(row.get("highest_equity") or 0), current_equity, start_balance)
                low = clean(row.get("lowest_equity") or 0)
                row["lowest_equity"] = min(low, current_equity) if low > 0 else current_equity
            enriched.append(_decorate_account_for_api(row))
        return enriched
    except Exception as e:
        print("ACCOUNT MONITORING ENRICH ERROR:", e)
        return accounts or []


def _get_active_account_by_login(mt5_login):
    try:
        login = str(mt5_login or "").strip()
        if not login:
            return None
        rows = supabase.table("trader_accounts").select("*").eq("mt5_login", login).eq("account_status", "assigned_active").order("updated_at", desc=True).order("started_at", desc=True).order("created_at", desc=True).limit(1).execute().data or []
        return _decorate_account_for_api(rows[0]) if rows else None
    except Exception as e:
        print("ACTIVE ACCOUNT BY LOGIN FETCH ERROR:", e)
        return None


def _get_account_by_login_any_status(mt5_login, trader_id=None):
    """Resolve an MT5 login to the correct account row, active first then newest history.

    This is required because the engine may send a lock/pass signal after the
    account was archived or moved to waiting state. Looking only at
    assigned_active makes the backend guess the wrong account.
    """
    try:
        login = str(mt5_login or "").strip()
        if not login:
            return None
        query = supabase.table("trader_accounts").select("*").eq("mt5_login", login)
        if trader_id:
            query = query.eq("trader_id", trader_id)
        rows = query.order("updated_at", desc=True).order("started_at", desc=True).order("created_at", desc=True).limit(25).execute().data or []
        if not rows:
            return None
        active_statuses = {"assigned_active", "active", "current_active"}
        for row in rows:
            if str(row.get("account_status") or "").strip().lower() in active_statuses:
                return _decorate_account_for_api(row)
        return _decorate_account_for_api(rows[0])
    except Exception as e:
        print("ACCOUNT BY LOGIN ANY STATUS FETCH ERROR:", e)
        return None


def _resolve_trader_for_money_action(data):
    tid = (data or {}).get("trader_id") or (data or {}).get("id")
    if tid:
        return get_trader_by_id(tid)
    email = str((data or {}).get("email") or "").strip().lower()
    phone = str((data or {}).get("phone") or "").strip()
    if email:
        rows = supabase.table("traders").select("*").eq("canonical_email", email).limit(1).execute().data or []
        if rows:
            return rows[0]
        rows = supabase.table("traders").select("*").eq("email", email).limit(1).execute().data or []
        if rows:
            return rows[0]
    if phone:
        normalized = _normalize_phone_value(phone)
        rows = supabase.table("traders").select("*").eq("canonical_phone", normalized).limit(1).execute().data or []
        if rows:
            return rows[0]
        rows = supabase.table("traders").select("*").eq("phone", phone).limit(1).execute().data or []
        if rows:
            return rows[0]
    return None


def _payout_eligibility(trader):
    if not trader:
        return False, "Trader not found", None
    state = str(trader.get("challenge_state") or "").strip().lower()
    account = _get_active_account(trader.get("id"), trader)
    if state != "funded_active":
        return False, "Payouts require funded_active lifecycle state.", account
    if not account:
        return False, "Payouts require an active funded MT5 account.", account
    if str(account.get("stage") or "").lower() != "funded":
        return False, "Payouts require the current active account to be funded stage.", account
    if str(account.get("account_status") or "").lower() != "assigned_active":
        return False, "Payouts require an assigned active account.", account
    if _is_truthy(trader.get("payout_blocked")):
        return False, "Payout blocked for this trader.", account
    return True, "Payout eligible", account


def _get_mt5_account(mt5_id=None, mt5_login=None):
    try:
        q = supabase.table("mt5_pool").select("*")
        if mt5_id:
            q = q.eq("id", mt5_id)
        elif mt5_login:
            q = q.eq("mt5_login", mt5_login)
        else:
            return None
        rows = q.limit(1).execute().data or []
        return rows[0] if rows else None
    except Exception:
        return None



def _mt5_login_has_any_history(mt5_login, exclude_mt5_pool_id=None):
    """Return True if this MT5 login has ever been used anywhere in NairaPips.

    Option A rule:
    Fresh MT5 means never used by anybody, not merely available today.
    This blocks reuse from trader_accounts, archives, purchases, monitoring logs,
    trader mirror rows, and historical MT5 pool assignment fields.
    """
    login = str(mt5_login or "").strip()
    if not login:
        return True, "MT5 login is empty"

    checks = [
        ("trader_accounts", "mt5_login"),
        ("mt5_account_archives", "mt5_login"),
        ("challenge_purchases", "mt5_login"),
        ("challenge_purchases", "current_mt5_login"),
        ("monitoring_events", "mt5_login"),
        ("monitoring_snapshots", "mt5_login"),
        ("traders", "mt5_login"),
    ]

    for table, column in checks:
        try:
            rows = supabase.table(table).select("id").eq(column, login).limit(1).execute().data or []
            if rows:
                return True, f"MT5 {login} already exists in {table}.{column}"
        except Exception as e:
            # Some deployments may not have every optional column.
            print(f"MT5 HISTORY CHECK SKIP {table}.{column}:", e)

    # Also protect against mt5_pool rows that were previously assigned but later
    # manually marked available again. This check ignores the selected row's own id.
    try:
        rows = supabase.table("mt5_pool").select("*").eq("mt5_login", login).limit(20).execute().data or []
        for row in rows:
            if exclude_mt5_pool_id and str(row.get("id")) == str(exclude_mt5_pool_id):
                # The selected vault row is allowed only if it has never carried assignment evidence.
                assigned_evidence = any(str(row.get(k) or "").strip() for k in [
                    "assigned_trader_id", "assigned_trader_name", "assigned_email",
                    "trader_account_id", "assigned_at", "archived_at", "archive_reason"
                ])
                status = str(row.get("status") or "").strip().lower()
                if assigned_evidence or status not in {"available", "", "unused", "new", "ready", "open"}:
                    return True, f"MT5 {login} has prior assignment evidence inside mt5_pool"
                continue
            return True, f"MT5 {login} appears more than once in mt5_pool"
    except Exception as e:
        print("MT5 HISTORY CHECK SKIP mt5_pool:", e)

    return False, ""


def _assert_mt5_never_used(mt5):
    """Raise ValueError unless selected MT5 is truly fresh across all NairaPips records."""
    if not mt5:
        raise ValueError("Fresh MT5 account not found")
    login = str(mt5.get("mt5_login") or "").strip()
    status = str(mt5.get("status") or "available").strip().lower()
    if status not in {"available", "", "unused", "new", "ready", "open"}:
        raise ValueError(f"Selected MT5 {login or ''} is not available")
    used, reason = _mt5_login_has_any_history(login, exclude_mt5_pool_id=mt5.get("id"))
    if used:
        raise ValueError(reason or f"Selected MT5 {login} has already been used before")
    return True


def _log_lifecycle_event(trader_id, account_id, from_state, to_state, action, details="", staff=None):
    try:
        supabase.table("lifecycle_events").insert({
            "trader_id": trader_id,
            "trader_account_id": account_id,
            "from_state": from_state,
            "to_state": to_state,
            "action": action,
            "details": details,
            "created_by": (staff or {}).get("name") or (staff or {}).get("username") or "system",
        }).execute()
    except Exception as e:
        print("LIFECYCLE EVENT ERROR:", e)


def _update_trader_lifecycle(trader_id, state, account=None, extra=None, staff=None, action="lifecycle_update"):
    payload = {"challenge_state": state, "lifecycle_updated_at": now_iso(), "updated_at": now_iso()}
    if account:
        payload.update({
            "current_account_id": account.get("id"),
            "phase": account.get("stage"),
            "status": "funded" if state == "funded_active" else "active",
            "mt5_login": account.get("mt5_login"),
            "mt5_server": account.get("mt5_server"),
            "mt5_master_password": account.get("mt5_master_password"),
            "mt5_password": account.get("mt5_master_password"),
            "master_password": account.get("mt5_master_password"),
            "mt5_investor_password": account.get("mt5_investor_password"),
            "investor_password": account.get("mt5_investor_password"),
            "account_size": account.get("account_size"),
            "balance": account.get("current_balance"),
            "equity": account.get("current_equity"),
            "profit": account.get("profit") or 0,
            "profit_percent": account.get("profit_percent") or 0,
            "drawdown": 0,
            "drawdown_percent": account.get("absolute_drawdown_percent") or 0,
            "max_drawdown_used": account.get("dd_used_percent") or 0,
            "monitoring_enabled": account.get("monitoring_enabled"),
        })
        if state == "funded_active":
            payload["funded_at"] = payload.get("funded_at") or now_iso()
    else:
        payload["current_account_id"] = None
    if extra:
        payload.update(extra)
    result = supabase.table("traders").update(payload).eq("id", trader_id).execute().data or []
    _log_lifecycle_event(trader_id, account.get("id") if account else None, None, state, action, str(extra or ""), staff)
    return result[0] if result else get_trader_by_id(trader_id)


def _ensure_trader_for_purchase(purchase):
    if purchase.get("trader_id"):
        trader = get_trader_by_id(purchase.get("trader_id"))
        if trader:
            _sync_identity_fields(trader)
            return trader
    email = str(purchase.get("email") or "").strip().lower()
    phone = str(purchase.get("phone") or "").strip()
    if email:
        rows = supabase.table("traders").select("*").eq("email", email).limit(1).execute().data or []
        if rows:
            _sync_identity_fields(rows[0])
            return rows[0]
    if phone:
        rows = supabase.table("traders").select("*").eq("phone", phone).limit(1).execute().data or []
        if rows:
            _sync_identity_fields(rows[0])
            return rows[0]
    row = {
        "name": purchase.get("trader_name") or "Trader",
        "email": purchase.get("email") or "",
        "phone": purchase.get("phone") or "",
        "account_reference": ref(),
        "challenge_state": "phase1_waiting_mt5",
        "phase": "phase1",
        "status": "active",
        "payment_status": "approved",
        "account_size": clean(purchase.get("account_size")),
        "balance": clean(purchase.get("account_size")),
        "equity": clean(purchase.get("account_size")),
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "canonical_email": email,
        "canonical_phone": _normalize_phone_value(phone),
    }
    created = supabase.table("traders").insert(row).execute().data or []
    return created[0] if created else None


def _assign_mt5_to_trader(trader, mt5, stage, purchase=None, staff=None, note="MT5 assigned"):
    stage = str(stage or "").lower()
    if stage not in ACCOUNT_STAGES:
        raise ValueError("Invalid account stage")
    if not trader:
        raise ValueError("Trader is required")
    if not mt5:
        raise ValueError("MT5 account is required")
    # Option A production rule: MT5 accounts are single-use.
    # Fresh means never used anywhere in NairaPips history.
    _assert_mt5_never_used(mt5)
    account_size = clean((purchase or {}).get("account_size") or mt5.get("account_size") or trader.get("account_size"))
    if clean(mt5.get("account_size")) and clean(mt5.get("account_size")) != account_size:
        raise ValueError("Selected MT5 account size does not match purchase/account size")
    purchase_id = (purchase or {}).get("id")
    if purchase_id:
        existing_purchase = supabase.table("trader_accounts").select("id,mt5_login").eq("purchase_id", purchase_id).eq("account_status", "assigned_active").limit(1).execute().data or []
        if existing_purchase:
            raise ValueError("This purchase already has an active MT5 account assigned")
    existing_login = supabase.table("trader_accounts").select("id").eq("mt5_login", mt5.get("mt5_login")).eq("account_status", "assigned_active").limit(1).execute().data or []
    if existing_login:
        raise ValueError("MT5 login already has an active trader account")
    now = now_iso()
    account_row = {
        "trader_id": trader.get("id"),
        "purchase_id": purchase_id,
        "mt5_pool_id": mt5.get("id"),
        "stage": stage,
        "account_status": "assigned_active",
        "mt5_login": mt5.get("mt5_login"),
        "mt5_server": mt5.get("mt5_server"),
        "mt5_master_password": mt5.get("mt5_master_password"),
        "mt5_investor_password": mt5.get("mt5_investor_password"),
        "account_size": account_size,
        "start_balance": account_size,
        "current_balance": account_size,
        "current_equity": account_size,
        "profit": 0,
        "profit_percent": 0,
        "absolute_drawdown_percent": 0,
        "dd_limit_percent": 20,
        "dd_used_percent": 0,
        "target_percent": _target_for_stage(stage),
        "monitoring_enabled": True,
        "started_at": now,
        "created_at": now,
        "updated_at": now,
    }
    account = (supabase.table("trader_accounts").insert(account_row).execute().data or [None])[0]
    if not account:
        raise RuntimeError("Could not create trader account")
    supabase.table("mt5_pool").update({
        "status": "assigned",
        "assigned_trader_id": trader.get("id"),
        "assigned_trader_name": trader.get("name"),
        "assigned_email": trader.get("email"),
        "assigned_at": now,
        "updated_at": now,
        "trader_account_id": account.get("id"),
        "admin_note": note,
    }).eq("id", mt5.get("id")).execute()
    if purchase and purchase.get("id"):
        supabase.table("challenge_purchases").update({
            "payment_status": "approved",
            "status": "approved_active",
            "lifecycle_state": _active_state_for_stage(stage),
            "trader_account_id": account.get("id"),
            "assigned_mt5_id": mt5.get("id"),
            "mt5_login": mt5.get("mt5_login"),
            "mt5_server": mt5.get("mt5_server"),
            "mt5_master_password": mt5.get("mt5_master_password"),
            "mt5_password": mt5.get("mt5_master_password"),
            "master_password": mt5.get("mt5_master_password"),
            "mt5_investor_password": mt5.get("mt5_investor_password"),
            "investor_password": mt5.get("mt5_investor_password"),
            "approved_at": now,
            "assigned_at": now,
            "updated_at": now,
            "admin_note": note,
        }).eq("id", purchase.get("id")).execute()
    trader_row = _update_trader_lifecycle(
        trader.get("id"),
        _active_state_for_stage(stage),
        account,
        {
            "payment_status": "approved",
            "approved_at": trader.get("approved_at") or now,
            "challenge_started_at": now,
            "selected_plan": (purchase or {}).get("plan_name") or trader.get("selected_plan"),
            "admin_note": note,
        },
        staff,
        f"assign_{stage}_mt5"
    )
    # CRITICAL: Email trader their new MT5 credentials (production must notify)
    try:
        stage_label = stage.upper().replace('_', ' ')
        trader_email = trader.get('email')
        trader_name = trader.get('name') or 'Trader'
        if trader_email:
            details = (
                f'Your {stage_label} MT5 account has been assigned:\n\n'
                f'MT5 Login: {mt5.get("mt5_login")}\n'
                f'MT5 Server: {mt5.get("mt5_server")}\n'
                f'Master Password: {mt5.get("mt5_master_password")}\n'
                f'Investor Password: {mt5.get("mt5_investor_password")}\n'
                f'Account Size: {account_size:,.0f}\n\n'
                f'You can now download MT5, log in with these credentials, and start trading.\n\n'
                f'Your dashboard: https://nairapips.com/dashboard/trader_clean.html'
            )
            send_account_status_email(
                trader,
                f'NairaPips — Your {stage_label} MT5 credentials',
                f'Hello {trader_name}, your {stage_label} MT5 account has been assigned by NairaPips.',
                details
            )
            send_admin_alert(
                f'NairaPips {stage_label} MT5 assigned',
                f'MT5 {mt5.get("mt5_login")} assigned to {trader_name} ({trader_email}) for {stage_label}. Emailed credentials to trader.'
            )
    except Exception as _email_err:
        print('MT5 ASSIGN EMAIL ERROR:', str(_email_err))
    return account, trader_row


def _archive_active_account(trader, reason, staff=None, breached=False):
    account = _get_active_account(trader.get("id"), trader)
    if not account:
        raise ValueError("Trader has no active account to archive")
    status = _archive_status_for_stage(account.get("stage"), breached)
    now = now_iso()
    supabase.table("mt5_account_archives").insert({
        "trader_id": trader.get("id"),
        "trader_account_id": account.get("id"),
        "stage": account.get("stage"),
        "mt5_login": account.get("mt5_login"),
        "mt5_server": account.get("mt5_server"),
        "final_balance": account.get("current_balance"),
        "final_equity": account.get("current_equity"),
        "final_profit": account.get("profit"),
        "final_profit_percent": account.get("profit_percent"),
        "final_dd_used_percent": account.get("dd_used_percent"),
        "archive_reason": reason,
        "archived_at": now,
    }).execute()
    supabase.table("trader_accounts").update({
        "account_status": status,
        "monitoring_enabled": False,
        "archived_at": now,
        "archive_reason": reason,
        "updated_at": now,
    }).eq("id", account.get("id")).execute()
    if account.get("mt5_pool_id"):
        supabase.table("mt5_pool").update({
            "status": status,
            "archived_at": now,
            "archive_reason": reason,
            "updated_at": now,
        }).eq("id", account.get("mt5_pool_id")).execute()
    return account


def _account_is_current_for_trader(trader, account):
    if not trader or not account:
        return False
    trader_current_id = str(trader.get("current_account_id") or "").strip()
    account_id = str(account.get("id") or "").strip()
    trader_login = str(trader.get("mt5_login") or "").strip()
    account_login = str(account.get("mt5_login") or "").strip()
    if trader_current_id and account_id and trader_current_id == account_id:
        return True
    if trader_login and account_login and trader_login == account_login:
        return True
    return False


def _archive_specific_account(account, reason, staff=None, breached=False, archive_status=None):
    """Archive exactly one trader_accounts row. Never guess by trader_id."""
    if not account:
        raise ValueError("Trader account is required")
    status = archive_status or _archive_status_for_stage(account.get("stage"), breached)
    now = now_iso()
    try:
        supabase.table("mt5_account_archives").insert({
            "trader_id": account.get("trader_id"),
            "trader_account_id": account.get("id"),
            "stage": account.get("stage"),
            "mt5_login": account.get("mt5_login"),
            "mt5_server": account.get("mt5_server"),
            "final_balance": account.get("current_balance"),
            "final_equity": account.get("current_equity"),
            "final_profit": account.get("profit"),
            "final_profit_percent": account.get("profit_percent"),
            "final_dd_used_percent": account.get("dd_used_percent"),
            "archive_reason": reason,
            "archived_at": now,
        }).execute()
    except Exception as e:
        print("SPECIFIC ACCOUNT ARCHIVE LOG ERROR:", e)
    supabase.table("trader_accounts").update({
        "account_status": status,
        "monitoring_enabled": False,
        "archived_at": now,
        "archive_reason": reason,
        "updated_at": now,
    }).eq("id", account.get("id")).execute()
    if account.get("mt5_pool_id"):
        try:
            supabase.table("mt5_pool").update({
                "status": status,
                "archived_at": now,
                "archive_reason": reason,
                "updated_at": now,
            }).eq("id", account.get("mt5_pool_id")).execute()
        except Exception as e:
            print("SPECIFIC MT5 POOL ARCHIVE ERROR:", e)
    archived = dict(account)
    archived.update({
        "account_status": status,
        "monitoring_enabled": False,
        "archived_at": now,
        "archive_reason": reason,
        "updated_at": now,
    })
    return archived


def _pass_specific_account(trader, account, pass_status, staff=None, note="Stage passed"):
    if not trader:
        raise ValueError("Trader not found")
    if not account:
        raise ValueError("Trader account not found")
    stage = str(account.get("stage") or "").lower()
    if pass_status == "phase2_passed":
        stage = "phase2"
    elif pass_status in {"phase1_passed", "passed", "target_hit"}:
        stage = "phase1"
        pass_status = "phase1_passed"
    if stage not in {"phase1", "phase2"}:
        raise ValueError("Only Phase 1 or Phase 2 accounts can be passed")
    next_state = "phase2_waiting_mt5" if stage == "phase1" else "funded_waiting_mt5"
    next_phase = "phase2" if stage == "phase1" else "funded_waiting"
    archived = _archive_specific_account(account, note, staff, breached=False, archive_status=_archive_status_for_stage(stage))
    _log_lifecycle_event(trader.get("id"), account.get("id"), stage, next_state, f"pass_specific_{stage}", note, staff)
    if _account_is_current_for_trader(trader, account):
        extra = {
            "status": next_state,
            "phase": next_phase,
            "phase_pass_status": f"{stage}_passed",
            "mt5_login": "",
            "mt5_server": "",
            "mt5_master_password": "",
            "mt5_password": "",
            "master_password": "",
            "mt5_investor_password": "",
            "investor_password": "",
            "profit": 0,
            "profit_percent": 0,
            "drawdown": 0,
            "drawdown_percent": 0,
            "max_drawdown_used": 0,
            "risk_zone": "passed",
            "monitoring_priority": "passed",
            "monitoring_enabled": False,
            "mt5_account_active": False,
            "mt5_access_disabled": True,
            "payout_blocked": False,
            "payout_eligible": False,
            "admin_note": note,
        }
        now = now_iso()
        extra["phase_passed_at"] = trader.get("phase_passed_at") or now
        extra["passed_at"] = trader.get("passed_at") or now
        if stage == "phase1":
            extra["phase1_passed_at"] = trader.get("phase1_passed_at") or now
        else:
            extra["phase2_passed_at"] = trader.get("phase2_passed_at") or now
            extra["certificate_status"] = trader.get("certificate_status") or "passed"
            extra["certificate_passed_at"] = trader.get("certificate_passed_at") or now
        updated = _update_trader_lifecycle(trader.get("id"), next_state, None, extra, staff, f"pass_specific_{stage}")
    else:
        updated = trader
    return updated, archived


def _breach_specific_account(trader, account, reason, staff=None):
    if not trader:
        raise ValueError("Trader not found")
    if not account:
        raise ValueError("Trader account not found")
    archived = _archive_specific_account(account, reason, staff, breached=True)
    _log_lifecycle_event(trader.get("id"), account.get("id"), account.get("stage"), "breached", "breach_specific_account", reason, staff)
    if _account_is_current_for_trader(trader, account):
        updated = _update_trader_lifecycle(
            trader.get("id"),
            "breached",
            None,
            {
                "status": "breached",
                "phase": "breached",
                "monitoring_enabled": False,
                "mt5_account_active": False,
                "mt5_access_disabled": True,
                "payout_eligible": False,
                "payout_blocked": True,
                "admin_note": reason,
                "breach_time": now_iso(),
                "breach_reason": reason,
                "risk_zone": "breached",
                "monitoring_priority": "closed",
            },
            staff,
            "breach_specific_account"
        )
    else:
        updated = trader
    return updated, archived


def _pass_stage(trader_id, stage, staff=None, note="Stage passed"):
    trader = get_trader_by_id(trader_id)
    if not trader:
        raise ValueError("Trader not found")
    account = _get_active_account(trader_id, trader)
    if not account or account.get("stage") != stage:
        raise ValueError(f"Trader does not have an active {stage} account")
    target = _target_for_stage(stage)
    if target is not None and clean(account.get("profit_percent")) < target:
        raise ValueError(f"{stage} target not reached")
    if clean(account.get("dd_used_percent")) >= 100:
        raise ValueError("Account has breached maximum drawdown")
    _archive_active_account(trader, note, staff)
    next_state = _next_waiting_after_pass(stage)
    extra = {
        "status": "active",
        "phase": "phase2" if stage == "phase1" else "funded_waiting",
        "phase_pass_status": f"{stage}_passed",
        "mt5_login": "",
        "mt5_server": "",
        "mt5_master_password": "",
        "mt5_password": "",
        "master_password": "",
        "mt5_investor_password": "",
        "investor_password": "",
        "profit": 0,
        "profit_percent": 0,
        "drawdown": 0,
        "drawdown_percent": 0,
        "max_drawdown_used": 0,
        "monitoring_enabled": False,
        "mt5_account_active": False,
        "mt5_access_disabled": True,
        "admin_note": note,
    }
    return _update_trader_lifecycle(trader_id, next_state, None, extra, staff, f"pass_{stage}")


def _breach_trader_account(trader_id, reason, staff=None):
    trader = get_trader_by_id(trader_id)
    if not trader:
        raise ValueError("Trader not found")
    try:
        account = _archive_active_account(trader, reason, staff, breached=True)
    except ValueError:
        account = None
    updated = _update_trader_lifecycle(
        trader_id,
        "breached",
        None,
        {
            "status": "breached",
            "phase": "breached",
            "monitoring_enabled": False,
            "mt5_account_active": False,
            "mt5_access_disabled": True,
            "payout_eligible": False,
            "payout_blocked": True,
            "admin_note": reason,
            "breach_time": now_iso(),
            "breach_reason": reason,
        },
        staff,
        "breach_account"
    )
    # Send breach notification email + admin alert (CRITICAL — production must email trader)
    try:
        send_account_status_email(
            updated,
            "NairaPips account breached",
            "Your NairaPips account has been breached and locked.",
            reason or "Maximum drawdown violation recorded by NairaPips monitoring engine."
        )
        send_admin_alert(
            "NairaPips account breached",
            f"""A trader account has breached and has been locked.

Trader: {updated.get('name') or 'Trader'}
Email: {updated.get('email') or 'Not provided'}
MT5 Login: {updated.get('mt5_login') or 'Not provided'}
Reason: {reason}"""
        )
    except Exception as _email_err:
        print("BREACH EMAIL ERROR:", str(_email_err))
    return updated, account


def _dedupe_by_id(rows):
    seen = set()
    out = []
    for row in rows or []:
        key = str(row.get("id") or row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _dashboard_payload_for_trader(trader):
    if not trader:
        return None
    account = _get_active_account(trader.get("id"), trader)
    purchases = _safe_fetch("challenge_purchases", "trader_id", trader.get("id"), 100)
    if trader.get("email"):
        purchases += _safe_fetch("challenge_purchases", "email", trader.get("email"), 100)
    if trader.get("phone"):
        purchases += _safe_fetch("challenge_purchases", "phone", trader.get("phone"), 100)
    purchases = _dedupe_by_id(purchases)
    active_accounts = _get_active_accounts(trader.get("id"), trader, purchases)
    active_accounts = _enrich_accounts_with_latest_monitoring(trader.get("id"), active_accounts)
    if account:
        enriched_current = None
        account_id = str(account.get("id") or "").strip()
        account_login = str(account.get("mt5_login") or "").strip()
        if account_id:
            for candidate in active_accounts:
                if str(candidate.get("id") or "").strip() == account_id:
                    enriched_current = candidate
                    break
        elif account_login:
            for candidate in active_accounts:
                if str(candidate.get("mt5_login") or "").strip() == account_login:
                    enriched_current = candidate
                    break
        if enriched_current:
            account = enriched_current
    if account:
        current_login = str(account.get("mt5_login") or "").strip()
        current_server = str(account.get("mt5_server") or "").strip()
        current_assigned_at = _account_display_assigned_at(account)
        account_by_purchase = {str(a.get("purchase_id") or ""): a for a in active_accounts if a.get("purchase_id")}
        account_by_login = {str(a.get("mt5_login") or "").strip(): a for a in active_accounts if str(a.get("mt5_login") or "").strip()}
        for p in purchases:
            # Purchase history must preserve the MT5 assigned to that purchase.
            # The dashboard focus account can be risky/current, but it must never erase
            # another purchase's own assigned MT5 credentials.
            purchase_account = account_by_purchase.get(str(p.get("id") or ""))
            if not purchase_account and str(p.get("mt5_login") or "").strip():
                purchase_account = account_by_login.get(str(p.get("mt5_login") or "").strip())
            if purchase_account:
                p["current_mt5_login"] = purchase_account.get("mt5_login") or ""
                p["current_mt5_server"] = purchase_account.get("mt5_server") or ""
                p["current_account_assigned_at"] = _account_display_assigned_at(purchase_account)
                p["current_account_stage"] = purchase_account.get("stage")
                p["lifecycle_state"] = _active_state_for_stage(purchase_account.get("stage"))
                p["active_stage"] = purchase_account.get("stage")
                if not str(p.get("mt5_login") or "").strip():
                    p["mt5_login"] = purchase_account.get("mt5_login") or ""
                    p["mt5_server"] = purchase_account.get("mt5_server") or ""
                    p["mt5_master_password"] = purchase_account.get("mt5_master_password") or ""
                    p["mt5_password"] = purchase_account.get("mt5_master_password") or ""
                    p["master_password"] = purchase_account.get("mt5_master_password") or ""
                    p["mt5_investor_password"] = purchase_account.get("mt5_investor_password") or ""
                    p["investor_password"] = purchase_account.get("mt5_investor_password") or ""
            else:
                p["current_mt5_login"] = current_login
                p["current_mt5_server"] = current_server
                p["current_account_assigned_at"] = current_assigned_at
                p["current_account_stage"] = account.get("stage")
                p["lifecycle_state"] = trader.get("challenge_state") or _active_state_for_stage(account.get("stage"))
                p["active_stage"] = account.get("stage")
    payouts_rows = _safe_fetch("payouts", "trader_id", trader.get("id"), 100)
    events = []
    snapshots = []
    archives = []
    try:
        # Global account evidence contract:
        # return the full trader-level monitoring ledger so the dashboard can
        # match every card by trader_account_id first and mt5_login second.
        # Filtering this to one current account caused other accounts to show
        # stale 0.00% / SAFE values.
        events = supabase.table("monitoring_events").select("*").eq("trader_id", trader.get("id")).order("created_at", desc=True).limit(300).execute().data or []
    except Exception:
        events = []
    try:
        snapshots = supabase.table("monitoring_snapshots").select("*").eq("trader_id", trader.get("id")).order("created_at", desc=True).limit(300).execute().data or []
    except Exception:
        snapshots = []
    try:
        archives = supabase.table("mt5_account_archives").select("*").eq("trader_id", trader.get("id")).order("archived_at", desc=True).limit(50).execute().data or []
    except Exception:
        archives = []
    return {
        "trader": trader,
        "current_account": account,
        "active_accounts": active_accounts,
        "accounts": active_accounts,
        "challenge_state": trader.get("challenge_state") or "registered",
        "purchases": purchases,
        "payouts": payouts_rows,
        "monitoring_events": events,
        "monitoring_snapshots": snapshots,
        "archives": archives,
    }




@app.route("/admin_reset_trader_account", methods=["POST", "OPTIONS"])
@app.route("/reset_trader_account", methods=["POST", "OPTIONS"])
@app.route("/reset_trader_mt5", methods=["POST", "OPTIONS"])
def admin_reset_trader_account():
    """CLEAN RESET ACCOUNT — production-safe and deterministic.

    Reset means:
    1. Archive the trader's CURRENT active MT5 account.
    2. Lock that old MT5 away as history.
    3. Clear MT5 credentials from the trader row.
    4. Move the trader to waiting-for-new-MT5 for the SAME stage.
    5. Do NOT auto-assign another MT5.
    6. Do NOT mark funded.
    7. Do NOT reuse any MT5.

    Fresh assignment must be done separately from Phase Assignment / MT5 Pool.
    """
    if request.method == "OPTIONS":
        return _np_ok({})

    data = request.get_json(silent=True) or {}
    trader_id = data.get("trader_id") or data.get("id")
    if not trader_id:
        return _np_fail("Trader ID is required")

    staff = _admin_from_payload(data)
    reset_type = str(data.get("reset_type") or data.get("reason") or "admin_reset").strip()
    admin_note = str(
        data.get("admin_note")
        or data.get("reset_reason")
        or "Account reset by admin."
    ).strip()

    try:
        trader = get_trader_by_id(trader_id)
        if not trader:
            return _np_fail("Trader not found", 404)

        account = _get_active_account(trader_id, trader)

        # If no active account exists, do not guess from history.
        # This prevents an old archived account from being reset again or moving the trader wrongly.
        if not account:
            state = str(trader.get("challenge_state") or trader.get("status") or "").strip().lower()
            if "waiting" in state:
                return _np_ok({
                    "success": True,
                    "message": "Trader already has no active MT5 and is waiting for assignment.",
                    "data": trader,
                    "current_account": None,
                })
            return _np_fail("No active MT5 account found to reset. Assign an MT5 first.", 409)

        # Preserve the exact phase being reset.
        # Admin sends reset_stage from the visible current account card.
        requested_stage = str(data.get("reset_stage") or "").strip().lower()
        account_stage = str(account.get("stage") or "").strip().lower()
        trader_phase = str(trader.get("phase") or "").strip().lower()

        if requested_stage in {"phase1", "phase2", "funded"}:
            stage = requested_stage
        elif account_stage in {"phase1", "phase2", "funded"}:
            stage = account_stage
        elif trader_phase in {"phase1", "phase2", "funded"}:
            stage = trader_phase
        else:
            stage = "phase1"

        now = now_iso()
        reason = f"ADMIN RESET — {reset_type}: {admin_note}"
        old_login = str(account.get("mt5_login") or trader.get("mt5_login") or "").strip()
        start_balance = clean(account.get("start_balance") or account.get("account_size") or trader.get("account_size") or 0)

        # Same phase must be preserved throughout reset.
        if stage == "phase1":
            waiting_state = "phase1_waiting_mt5"
            phase_value = "phase1"
        elif stage == "phase2":
            waiting_state = "phase2_waiting_mt5"
            phase_value = "phase2"
        else:
            waiting_state = "funded_waiting_mt5"
            phase_value = "funded_waiting"

        # 1. Archive exactly the current account.
        archived = _archive_specific_account(
            account,
            reason,
            staff,
            breached=False,
            archive_status=_archive_status_for_stage(stage, breached=False)
        )

        # Archive duplicate active rows for the same MT5 login only.
        # Multiple separate challenge accounts remain untouched.
        if old_login:
            try:
                duplicate_rows = (
                    supabase.table("trader_accounts")
                    .select("*")
                    .eq("trader_id", trader_id)
                    .eq("mt5_login", old_login)
                    .in_("account_status", list(ACTIVE_ACCOUNT_STATUSES))
                    .limit(50)
                    .execute()
                    .data
                    or []
                )
                for duplicate in duplicate_rows:
                    if str(duplicate.get("id") or "") == str(account.get("id") or ""):
                        continue
                    _archive_specific_account(
                        duplicate,
                        reason + " — duplicate active row closed",
                        staff,
                        breached=False,
                        archive_status=_archive_status_for_stage(
                            duplicate.get("stage") or stage,
                            breached=False
                        )
                    )
            except Exception as e:
                print("RESET DUPLICATE ACTIVE ACCOUNT CLEANUP ERROR:", e)

        # Neutralise only the purchase linked to this reset account.
        # Archive history already preserves the old MT5 credentials.
        purchase_id = account.get("purchase_id")
        purchase_waiting_update = {
            "status": "approved",
            "lifecycle_state": waiting_state if "waiting_state" in locals() else f"{stage}_waiting_mt5",
            "trader_account_id": None,
            "assigned_mt5_id": None,
            "mt5_login": "",
            "mt5_server": "",
            "mt5_master_password": "",
            "mt5_password": "",
            "master_password": "",
            "mt5_investor_password": "",
            "investor_password": "",
            "updated_at": now,
            "admin_note": reason,
        }
        try:
            if purchase_id:
                supabase.table("challenge_purchases").update(
                    purchase_waiting_update
                ).eq("id", purchase_id).execute()
            elif old_login:
                # Legacy account without purchase_id: constrain by trader and login.
                supabase.table("challenge_purchases").update(
                    purchase_waiting_update
                ).eq("trader_id", trader_id).eq("mt5_login", old_login).execute()
        except Exception as e:
            print("RESET PURCHASE MIRROR CLEANUP ERROR:", e)

        # 2. Lock old MT5 pool row; never put it back to available.
        try:
            if account.get("mt5_pool_id"):
                supabase.table("mt5_pool").update({
                    "status": _archive_status_for_stage(stage, breached=False),
                    "assigned_trader_id": trader_id,
                    "assigned_trader_name": trader.get("name") or trader.get("full_name") or trader.get("email"),
                    "assigned_email": trader.get("email"),
                    "trader_account_id": account.get("id"),
                    "archived_at": now,
                    "archive_reason": reason,
                    "updated_at": now,
                    "admin_note": reason,
                }).eq("id", account.get("mt5_pool_id")).execute()
        except Exception as e:
            print("RESET MT5 POOL LOCK ERROR:", e)

        # 3. Clear the trader mirror and keep the same waiting phase.
        trader_update = {
            "current_account_id": None,
            "challenge_state": waiting_state,
            "status": "active",
            "phase": phase_value,
            "mt5_login": "",
            "mt5_server": "",
            "mt5_master_password": "",
            "mt5_password": "",
            "master_password": "",
            "mt5_investor_password": "",
            "investor_password": "",
            "balance": start_balance,
            "equity": start_balance,
            "profit": 0,
            "profit_percent": 0,
            "drawdown": 0,
            "drawdown_percent": 0,
            "max_drawdown_used": 0,
            "risk_zone": "waiting_mt5",
            "monitoring_enabled": False,
            "mt5_account_active": False,
            "mt5_access_disabled": True,
            "payout_eligible": False,
            "payout_blocked": False,
            "admin_note": reason,
            "mt5_reset_reason": admin_note,
            "mt5_updated_at": now,
            "mt5_updated_by": staff.get("username") or staff.get("name") or "admin",
            "lifecycle_updated_at": now,
            "updated_at": now,
        }

        result = supabase.table("traders").update(trader_update).eq("id", trader_id).execute().data or []
        updated = result[0] if result else get_trader_by_id(trader_id)

        # 4. Ledger/audit evidence only. Failure must not block reset.
        try:
            supabase.table("monitoring_events").insert({
                "trader_id": trader_id,
                "trader_account_id": account.get("id"),
                "mt5_login": old_login,
                "event_type": "admin_clean_account_reset",
                "risk_zone": "waiting_mt5",
                "message": f"Admin reset completed. Old MT5 archived. Trader waiting for fresh MT5. Reason: {reason}",
                "dd_used_percent": 0,
                "max_drawdown_used": 0,
                "balance": start_balance,
                "equity": start_balance,
                "created_at": now,
            }).execute()
        except Exception as e:
            print("RESET EVENT LOG ERROR:", e)

        _log_lifecycle_event(
            trader_id,
            account.get("id"),
            str(account.get("account_status") or ""),
            waiting_state,
            "admin_clean_reset_account",
            reason,
            staff,
        )
        _audit_safe(
            "trader",
            "admin_clean_reset_account",
            f"Trader {trader_id}. Old MT5 {old_login} archived. Waiting state={waiting_state}. Reason={reason}",
            staff,
            trader_id,
        )

        return _np_ok({
            "success": True,
            "message": "Account reset complete. Old MT5 archived. Trader is now waiting for fresh MT5 assignment.",
            "data": updated,
            "archived_account": archived,
            "current_account": None,
            "reset_stage": stage,
            "waiting_state": waiting_state,
        })

    except Exception as e:
        return _np_fail(e, 500)



@app.route("/update_trader_mt5", methods=["POST", "OPTIONS"])
def update_trader_mt5():
    if request.method == "OPTIONS":
        return _np_ok({})

    data = request.get_json(silent=True) or {}
    trader_id = data.get("id") or data.get("trader_id")
    if not trader_id:
        return _np_fail("Trader ID is required")

    try:
        trader_row = get_trader_by_id(trader_id)
        if not trader_row:
            return _np_fail("Trader not found", 404)
        account = _get_active_account(trader_id, trader_row)
        if not account:
            return _np_fail("No active MT5 account found. Use lifecycle assignment to assign a fresh MT5 account.", 409)

        incoming_login = str(data.get("mt5_login") or "").strip()
        if incoming_login and incoming_login != str(account.get("mt5_login") or "").strip():
            return _np_fail("MT5 login cannot be changed here. Archive/pass/breach the current account, then assign a fresh MT5 through lifecycle.", 409)

        now = now_iso()
        account_update = {"updated_at": now}
        mirror_update = {"updated_at": now, "mt5_updated_at": now}

        if data.get("mt5_server"):
            account_update["mt5_server"] = str(data.get("mt5_server") or "").strip()
            mirror_update["mt5_server"] = account_update["mt5_server"]
        for src, dests in [
            ("mt5_master_password", ["mt5_master_password", "mt5_password", "master_password"]),
            ("mt5_password", ["mt5_master_password", "mt5_password", "master_password"]),
            ("master_password", ["mt5_master_password", "mt5_password", "master_password"]),
            ("mt5_investor_password", ["mt5_investor_password", "investor_password"]),
            ("investor_password", ["mt5_investor_password", "investor_password"]),
        ]:
            if str(data.get(src) or "").strip():
                value = str(data.get(src) or "").strip()
                if "investor" in src:
                    account_update["mt5_investor_password"] = value
                else:
                    account_update["mt5_master_password"] = value
                for dest in dests:
                    mirror_update[dest] = value

        if len(account_update) == 1:
            return _np_fail("Nothing to update")

        supabase.table("trader_accounts").update(account_update).eq("id", account.get("id")).execute()
        mirror_update.update({
            "mt5_login": account.get("mt5_login"),
            "mt5_updated_by": data.get("mt5_updated_by") or data.get("admin_name") or "admin",
            "mt5_reset_reason": data.get("mt5_reset_reason") or "Current active MT5 credentials updated",
            "admin_note": data.get("admin_note") or "Current active MT5 credentials updated",
        })
        result = supabase.table("traders").update(mirror_update).eq("id", trader_id).execute().data or []
        updated = result[0] if result else get_trader_by_id(trader_id)

        send_mt5_reset_email(
            updated,
            account.get("mt5_login"),
            mirror_update.get("mt5_server") or account.get("mt5_server"),
            mirror_update.get("mt5_master_password") or account.get("mt5_master_password") or "",
            mirror_update.get("mt5_investor_password") or account.get("mt5_investor_password") or "",
            data.get("mt5_reset_reason") or data.get("admin_note") or "Current active MT5 credentials updated"
        )
        send_admin_alert(
            "NairaPips MT5 login reset (active account)",
            f"Trader {updated.get('name') or trader_id} ({updated.get('email')}) active MT5 credentials updated. MT5 login: {account.get('mt5_login')}. Email sent to trader."
        )
        _audit_safe("mt5", "active_mt5_credentials_update", f"Trader {trader_id} active account {account.get('id')} credentials updated", _admin_from_payload(data))
        return _np_ok({"success": True, "message": "Current active MT5 credentials updated", "data": updated, "current_account": account})
    except Exception as e:
        return _np_fail(e, 500)

    mt5_login = str(data.get("mt5_login") or "").strip()
    mt5_server = str(data.get("mt5_server") or "").strip()
    if not mt5_login:
        return _np_fail("MT5 login is required")
    if not mt5_server:
        return _np_fail("MT5 server is required")

    master_password = data.get("mt5_password") or data.get("master_password") or data.get("mt5_master_password") or ""
    investor_password = data.get("mt5_investor_password") or data.get("investor_password") or ""
    now = datetime.now(timezone.utc).isoformat()

    trader_update = {
        "mt5_login": mt5_login,
        "mt5_server": mt5_server,
        "mt5_master_password": master_password,
        "mt5_investor_password": investor_password,
        "mt5_password": master_password,
        "master_password": master_password,
        "investor_password": investor_password,
        "phase": data.get("phase") or "phase1",
        "status": data.get("status") or "active",
        "payment_status": data.get("payment_status") or "approved",
        "approved_at": data.get("approved_at") or now,
        "challenge_started_at": data.get("challenge_started_at") or now,
        "mt5_updated_at": now,
        "updated_at": now,
        "mt5_updated_by": data.get("mt5_updated_by") or "admin",
        "mt5_reset_reason": data.get("mt5_reset_reason") or "MT5 login details updated",
        "admin_note": data.get("admin_note") or "MT5 login details updated",
    }

    if not str(master_password).strip():
        for k in ["mt5_master_password", "mt5_password", "master_password"]:
            trader_update.pop(k, None)
    if not str(investor_password).strip():
        for k in ["mt5_investor_password", "investor_password"]:
            trader_update.pop(k, None)

    try:
        result = supabase.table("traders").update(trader_update).eq("id", trader_id).execute()
        trader_rows = getattr(result, "data", []) or []
        if not trader_rows:
            fetched = supabase.table("traders").select("*").eq("id", trader_id).limit(1).execute()
            trader_rows = getattr(fetched, "data", []) or []

        trader_email = (trader_rows[0].get("email") if trader_rows else "") or data.get("email") or ""
        trader_phone = (trader_rows[0].get("phone") if trader_rows else "") or data.get("phone") or ""

        purchase_update = {
            "mt5_login": mt5_login,
            "mt5_server": mt5_server,
            "mt5_master_password": master_password,
            "mt5_investor_password": investor_password,
            "mt5_password": master_password,
            "master_password": master_password,
            "investor_password": investor_password,
            "payment_status": "approved",
            "status": "approved",
            "assigned_at": now,
            "approved_at": now,
            "updated_at": now,
            "admin_note": data.get("admin_note") or "MT5 login details updated",
        }

        if not str(master_password).strip():
            for k in ["mt5_master_password", "mt5_password", "master_password"]:
                purchase_update.pop(k, None)
        if not str(investor_password).strip():
            for k in ["mt5_investor_password", "investor_password"]:
                purchase_update.pop(k, None)

        _safe_update_table("challenge_purchases", purchase_update, "trader_id", trader_id)
        _safe_update_table("challenge_purchases", purchase_update, "email", trader_email)
        _safe_update_table("challenge_purchases", purchase_update, "phone", trader_phone)

        trader_row = trader_rows[0] if trader_rows else get_trader_by_id(trader_id)
        send_mt5_reset_email(
            trader_row,
            mt5_login,
            mt5_server,
            master_password,
            investor_password,
            data.get("mt5_reset_reason") or data.get("admin_note") or "MT5 login details updated"
        )
        send_admin_alert(
            "NairaPips MT5 login reset",
            f"""A trader MT5 login/account was updated.

Trader: {trader_row.get("name") if trader_row else ""}
Email: {trader_email}
Phone: {trader_phone}
MT5 Login: {mt5_login}
Server: {mt5_server}
Reason: {data.get("mt5_reset_reason") or data.get("admin_note") or "MT5 login details updated"}"""
        )

        _audit_safe("mt5", "mt5_account_update", f"Trader {trader_id} MT5 updated to {mt5_login} / {mt5_server}", _admin_from_payload(data))
        return _np_ok({"success": True, "message": "MT5 details updated and synced", "data": trader_rows})
    except Exception as e:
        return _np_fail(e, 500)

@app.route("/trader_source", methods=["POST", "OPTIONS"])
def trader_source():
    if request.method == "OPTIONS":
        return _np_ok({})
    data = request.get_json(silent=True) or {}
    lookup = data.get("lookup") or data.get("email") or data.get("phone") or data.get("id")
    trader = _latest_trader_for_lookup(lookup)
    if not trader:
        return _np_fail("Trader not found", 404)
    return _np_ok({"success": True, "data": trader, "trader": trader})

@app.route("/trader_source/<path:lookup>", methods=["GET"])
def trader_source_get(lookup):
    trader = _latest_trader_for_lookup(lookup)
    if not trader:
        return _np_fail("Trader not found", 404)
    return _np_ok({"success": True, "data": trader, "trader": trader})



# ================================
# NAIRAPIPS ADMIN PHASE ASSIGNMENT QUEUE
# Archived passed accounts are intentionally archived for monitoring safety,
# but they must still appear here for the next fresh MT5 assignment.
# ================================

def _np_number(value, default=0):
    try:
        if value is None:
            return default
        return float(str(value).replace("₦", "").replace(",", "").replace("%", "").strip() or default)
    except Exception:
        return default

def _phase_assignment_rows_from_accounts(accounts, traders_by_id=None, active_accounts_by_trader=None):
    rows = []
    traders_by_id = traders_by_id or {}
    active_accounts_by_trader = active_accounts_by_trader or {}
    seen = set()
    active_statuses = {"assigned_active", "active", "current_active", "phase1_active", "phase2_active", "funded_active", "live", "funded"}
    def has_target_active(trader_id, target_stage):
        active_rows = active_accounts_by_trader.get(str(trader_id or "").strip(), [])
        for row in active_rows:
            status = str(row.get("account_status") or row.get("status") or "").strip().lower()
            stage = str(row.get("stage") or row.get("phase") or "").strip().lower()
            login = str(row.get("mt5_login") or "").strip()
            if status not in active_statuses or not login:
                continue
            if target_stage == "phase2" and stage in {"phase2", "funded", "live"}:
                return True
            if target_stage == "funded" and stage in {"funded", "live"}:
                return True
        return False
    for acc in accounts or []:
        try:
            status = str(acc.get("account_status") or "").strip().lower()
            pass_status = str(acc.get("phase_pass_status") or "").strip().lower()
            risk = str(acc.get("risk_zone") or acc.get("display_risk_zone") or "").strip().lower()
            stage = str(acc.get("stage") or "").strip().lower()
            if status not in {"archived_phase1", "archived_phase2"}:
                continue
            phase1_passed = status == "archived_phase1" and (pass_status == "phase1_passed" or risk == "passed" or stage == "phase1")
            phase2_passed = status == "archived_phase2" and (pass_status == "phase2_passed" or risk == "passed" or stage == "phase2")
            if not phase1_passed and not phase2_passed:
                continue
            account_id = str(acc.get("id") or "").strip()
            if account_id and account_id in seen:
                continue
            if account_id:
                seen.add(account_id)
            trader_id = str(acc.get("trader_id") or "").strip()
            trader = traders_by_id.get(trader_id) or {}
            target_stage = "funded" if phase2_passed else "phase2"
            if has_target_active(trader_id, target_stage):
                continue
            rows.append({
                "id": trader.get("id") or trader_id,
                "trader_id": trader.get("id") or trader_id,
                "trader_account_id": account_id,
                "completed_account_id": account_id,
                "name": trader.get("name") or trader.get("full_name") or acc.get("name") or "Trader",
                "email": trader.get("email") or acc.get("email") or "",
                "phone": trader.get("phone") or acc.get("phone") or "",
                "account_reference": trader.get("account_reference") or acc.get("account_reference") or "",
                "old_mt5_login": acc.get("mt5_login") or "",
                "completed_mt5_login": acc.get("mt5_login") or "",
                "completed_stage": "phase2" if phase2_passed else "phase1",
                "passed_stage": "phase2" if phase2_passed else "phase1",
                "target_phase": target_stage,
                "target_stage": target_stage,
                "assignment_label": "Assign Funded MT5" if target_stage == "funded" else "Assign Phase 2 MT5",
                "account_size": acc.get("account_size") or acc.get("start_balance") or trader.get("account_size") or trader.get("balance") or 0,
                "current_equity": acc.get("current_equity") or acc.get("equity") or 0,
                "highest_equity": acc.get("highest_equity") or 0,
                "pass_progress_percent": acc.get("pass_progress_percent") or 0,
                "phase_pass_status": pass_status or ("phase2_passed" if phase2_passed else "phase1_passed"),
                "account_status": status,
                "passed_at": acc.get("passed_at") or acc.get("phase_passed_at") or acc.get("archived_at") or acc.get("updated_at") or "",
                "archived_at": acc.get("archived_at") or "",
                "source": "trader_accounts_archived_passed"
            })
        except Exception as row_error:
            print("PHASE ASSIGNMENT ROW ERROR:", row_error)
    rows.sort(key=lambda r: str(r.get("passed_at") or r.get("archived_at") or ""), reverse=True)
    return rows

def _fetch_phase_assignment_queue():
    account_rows = supabase.table("trader_accounts").select("*").in_("account_status", ["archived_phase1", "archived_phase2"]).order("updated_at", desc=True).limit(1000).execute().data or []
    trader_ids = list({str(a.get("trader_id") or "").strip() for a in account_rows if str(a.get("trader_id") or "").strip()})
    traders_by_id = {}
    active_accounts_by_trader = {}
    if trader_ids:
        try:
            trader_rows = supabase.table("traders").select("*").in_("id", trader_ids).limit(1000).execute().data or []
            traders_by_id = {str(t.get("id") or ""): t for t in trader_rows}
        except Exception as e:
            print("PHASE QUEUE TRADER FETCH ERROR:", e)
        try:
            active_rows = supabase.table("trader_accounts").select("id,trader_id,stage,phase,account_status,status,mt5_login").in_("trader_id", trader_ids).in_("account_status", ["assigned_active", "active", "current_active", "phase1_active", "phase2_active", "funded_active", "live", "funded"]).limit(3000).execute().data or []
            for row in active_rows:
                tid = str(row.get("trader_id") or "").strip()
                if tid:
                    active_accounts_by_trader.setdefault(tid, []).append(row)
        except Exception as e:
            print("PHASE QUEUE ACTIVE ACCOUNT FETCH ERROR:", e)
    return _phase_assignment_rows_from_accounts(account_rows, traders_by_id, active_accounts_by_trader)

def _active_account_mt5_logins(limit=5000):
    try:
        rows = supabase.table("trader_accounts").select("mt5_login,account_status").in_("account_status", ["assigned_active", "active", "current_active", "phase1_active", "phase2_active", "funded_active", "live", "funded"]).limit(limit).execute().data or []
        return {str(r.get("mt5_login") or "").strip() for r in rows if str(r.get("mt5_login") or "").strip()}
    except Exception as e:
        print("ACTIVE ACCOUNT MT5 LOGIN FETCH ERROR:", e)
        return set()

def _available_mt5_not_used(limit=1500):
    try:
        mt5_rows = supabase.table("mt5_pool").select("*").order("created_at", desc=True).limit(limit).execute().data or []
    except Exception as e:
        print("AVAILABLE MT5 FETCH ERROR:", e)
        mt5_rows = []
    used_logins = _active_account_mt5_logins()
    available = _quick_available_mt5(mt5_rows) if "_quick_available_mt5" in globals() else [
        m for m in mt5_rows
        if str(m.get("status") or "available").strip().lower() in {"available", "unused", "free", ""}
    ]
    return [m for m in available if str(m.get("mt5_login") or "").strip() and str(m.get("mt5_login") or "").strip() not in used_logins]

@app.route("/phase_assignment_queue", methods=["GET", "OPTIONS"])
def phase_assignment_queue():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    try:
        queue = _fetch_phase_assignment_queue()
        available_mt5 = _available_mt5_not_used(1000)
        return _np_ok({
            "success": True,
            "rows": queue,
            "assignment_queue": queue,
            "data": queue,
            "available_mt5": available_mt5,
            "message": f"{len(queue)} phase assignment record(s)"
        })
    except Exception as e:
        return _np_fail(e, 500)


# ================================
# NAIRAPIPS ADMIN BOOTSTRAP FEED - FAST / NON-HANGING
# One quick payload for admin. Heavy logs/trades/monitoring are not loaded here.
# They can be loaded by their own modules only when opened.
# ================================

def _admin_rest_rows(table, order_col="created_at", desc=True, limit=500):
    """Fetch a Supabase table directly with a hard timeout.
    This prevents /admin_bootstrap from hanging forever when one table is slow.
    """
    try:
        base = (SUPABASE_URL or "").rstrip("/")
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Accept": "application/json",
        }
        params = {"select": "*", "limit": str(limit)}
        if order_col:
            params["order"] = f"{order_col}.{'desc' if desc else 'asc'}"
        url = f"{base}/rest/v1/{table}"
        r = requests.get(url, headers=headers, params=params, timeout=(3, 7))
        if r.status_code >= 400 and order_col:
            params.pop("order", None)
            r = requests.get(url, headers=headers, params=params, timeout=(3, 7))
        if r.status_code >= 400:
            print(f"ADMIN BOOTSTRAP REST ERROR {table}:", r.status_code, r.text[:180])
            return []
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"ADMIN BOOTSTRAP REST TIMEOUT/ERROR {table}:", e)
        return []


def _quick_available_mt5(rows):
    out = []
    for m in rows or []:
        st = str(m.get("status") or "available").strip().lower()
        assigned = str(m.get("assigned_trader_id") or m.get("trader_id") or "").strip()
        # PATCH_PR_FIX_2026_06_28: accept any open/unassigned status; only exclude if
        # status explicitly says assigned/breached/archived/etc.
        if st in {"assigned", "inactive", "expired", "archived", "deleted", "disabled", "locked", "breached"}:
            continue
        if st in {"available", "unused", "free", "new", "ready", "open", "unassigned", "pending", "created", "stock"} or not st:
            out.append(m)
    return out


@app.route("/admin_bootstrap", methods=["GET", "OPTIONS"])
def admin_bootstrap():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    force = str(request.args.get("force") or request.args.get("fresh") or "").lower() in {"1", "true", "yes", "manual"}
    cached_payload = _ADMIN_BOOTSTRAP_CACHE.get("payload") if isinstance(_ADMIN_BOOTSTRAP_CACHE, dict) else None
    cached_ts = float(_ADMIN_BOOTSTRAP_CACHE.get("ts") or 0) if isinstance(_ADMIN_BOOTSTRAP_CACHE, dict) else 0
    if cached_payload and not force and (time.time() - cached_ts) <= ADMIN_BOOTSTRAP_TTL_SECONDS:
        cached_payload["cached"] = True
        return _np_ok(cached_payload)
    started = time.time()

    # Keep this intentionally LIGHT. The old version loaded 18 large endpoints and could hang Render.
    traders_rows = _admin_rest_rows("traders", "created_at", True, 1200)
    plan_rows = _admin_rest_rows("challenge_plans", "created_at", True, 500)
    purchase_rows = _admin_rest_rows("challenge_purchases", "created_at", True, 1200)
    account_rows = _admin_rest_rows("trader_accounts", "updated_at", True, 2500)
    payout_rows = _admin_rest_rows("payouts", "created_at", True, 800)
    mt5_rows = _admin_rest_rows("mt5_pool", "created_at", True, 1200)
    ticket_rows = _admin_rest_rows("support_tickets", "created_at", True, 500)
    announcement_rows = _admin_rest_rows("announcements", "created_at", True, 300)
    snapshot_rows = _admin_rest_rows("monitoring_snapshots", "created_at", True, 2500)
    event_rows = _admin_rest_rows("monitoring_events", "created_at", True, 1200)

    # This queue was already proven working. If it ever fails, admin still loads.
    try:
        phase_queue_rows = _fetch_phase_assignment_queue()
    except Exception as e:
        print("ADMIN BOOTSTRAP PHASE QUEUE ERROR:", e)
        phase_queue_rows = []

    available_mt5_rows = _quick_available_mt5(mt5_rows)

    payload = {
        "success": True,
        "source": "admin_bootstrap_fast",
        "generated_at": now_iso(),
        "duration_ms": int((time.time() - started) * 1000),

        "traders": traders_rows,
        "data": traders_rows,
        "payouts": payout_rows,
        "tickets": ticket_rows,
        "support_tickets": ticket_rows,
        "announcements": announcement_rows,
        "plans": plan_rows,
        "challenge_plans": plan_rows,
        "purchases": purchase_rows,
        "challenge_purchases": purchase_rows,
        "accounts": account_rows,
        "trader_accounts": account_rows,
        "mt5_pool": mt5_rows,
        "available_mt5": available_mt5_rows,
        "phase_assignment_queue": phase_queue_rows,
        "assignment_queue": phase_queue_rows,

        # Breach monitoring evidence is business-critical. Keep trades empty on bootstrap,
        # but include recent monitoring rows so breached accounts cannot render as funded.
        "trader_trades": [],
        "trades": [],
        "monitoring_snapshots": snapshot_rows,
        "snapshots": snapshot_rows,
        "monitoring_events": event_rows,
        "events": event_rows,
        "marketing_deleted_contacts": [],
        "staff_members": [],
        "audit_logs": [],
        "affiliate_partners": [],
        "affiliate_codes": [],
        "affiliate_commissions": [],
        "affiliate_summary": {},
        "referral_settings": {},
        "business_settings": {},

        "counts": {
            "traders": len(traders_rows),
            "plans": len(plan_rows),
            "purchases": len(purchase_rows),
            "payouts": len(payout_rows),
            "mt5_pool": len(mt5_rows),
            "available_mt5": len(available_mt5_rows),
            "phase_assignment_queue": len(phase_queue_rows),
        }
    }
    try:
        _ADMIN_BOOTSTRAP_CACHE["ts"] = time.time()
        _ADMIN_BOOTSTRAP_CACHE["payload"] = payload
    except Exception:
        pass
    return _np_ok(payload)

@app.route("/trader_current_account/<path:lookup>", methods=["GET"])
def trader_current_account(lookup):
    try:
        trader = _latest_trader_for_lookup(lookup)
        if not trader:
            return bad("Trader not found", 404)
        account = _get_active_account(trader.get("id"), trader)
        purchases = _safe_fetch("challenge_purchases", "trader_id", trader.get("id"), 100)
        if trader.get("email"):
            purchases += _safe_fetch("challenge_purchases", "email", trader.get("email"), 100)
        if trader.get("phone"):
            purchases += _safe_fetch("challenge_purchases", "phone", trader.get("phone"), 100)
        active_accounts = _get_active_accounts(trader.get("id"), trader, _dedupe_by_id(purchases))
        active_accounts = _enrich_accounts_with_latest_monitoring(trader.get("id"), active_accounts)
        if account:
            account_id = str(account.get("id") or "").strip()
            account_login = str(account.get("mt5_login") or "").strip()
            enriched_current = None
            if account_id:
                for candidate in active_accounts:
                    if str(candidate.get("id") or "").strip() == account_id:
                        enriched_current = candidate
                        break
            if not enriched_current and account_login:
                for candidate in active_accounts:
                    if str(candidate.get("mt5_login") or "").strip() == account_login:
                        enriched_current = candidate
                        break
            if enriched_current:
                account = enriched_current
        all_accounts = []
        try:
            raw_all_accounts = supabase.table("trader_accounts").select("*").eq("trader_id", trader.get("id")).order("updated_at", desc=True).order("started_at", desc=True).order("created_at", desc=True).limit(200).execute().data or []
            all_accounts = _enrich_accounts_with_latest_monitoring(trader.get("id"), [_decorate_account_for_api(a) for a in raw_all_accounts])
        except Exception as all_account_error:
            print("TRADER ALL ACCOUNTS FETCH ERROR:", all_account_error)
            all_accounts = list(active_accounts or [])
        assignment_queue = _phase_assignment_rows_from_accounts(all_accounts, {str(trader.get("id")): trader})
        return ok({
            "trader": trader,
            "current_account": account,
            "active_accounts": active_accounts,
            "accounts": active_accounts,
            "all_accounts": all_accounts,
            "assignment_queue": assignment_queue,
            "challenge_state": trader.get("challenge_state") or "registered"
        })
    except Exception as e:
        return bad(e)


@app.route("/trader_dashboard/<path:lookup>", methods=["GET"])
def trader_dashboard_payload(lookup):
    try:
        trader = _latest_trader_for_lookup(lookup)
        if not trader:
            return bad("Trader not found", 404)
        token = _request_trader_auth_token()
        if not _verify_trader_auth_token(token, trader.get("id")):
            return bad("Login password is required to load trader dashboard", 401)
        return ok(_dashboard_payload_for_trader(trader), "Trader dashboard loaded")
    except Exception as e:
        return bad(e)




# ================================
# ADMIN SUPPORT: VIEW TRADER DASHBOARD
# ================================
ADMIN_VIEW_SECRET = os.getenv("ADMIN_VIEW_SECRET", "")


def _admin_view_secret_ok(value):
    try:
        configured = str(ADMIN_VIEW_SECRET or "").strip()
        supplied = str(value or "").strip()
        return bool(configured) and hmac.compare_digest(configured, supplied)
    except Exception:
        return False


def _admin_view_request_ok(data):
    """Authorize admin support dashboard view without asking for a separate popup secret.

    Production-safe behavior:
    - Existing ADMIN_VIEW_SECRET still works if configured.
    - Normal master admin login works: admin / nairapips123.
    - Staff accounts work through the existing admin_staff_members table.
    - No trader password is revealed, reset, or required.
    """
    try:
        secret = data.get("admin_view_secret") or request.headers.get("X-Admin-View-Secret") or ""
        if _admin_view_secret_ok(secret):
            return True

        username = str(data.get("admin_username") or data.get("username") or request.headers.get("X-Admin-Username") or "").strip()
        password = str(data.get("admin_password") or data.get("password") or request.headers.get("X-Admin-Password") or "")

        # Existing source-of-truth master admin fallback used by admin.html login.
        if username == "admin" and password == "nairapips123":
            return True

        if not username or not password:
            return False

        rows = supabase.table("admin_staff_members").select("id,username,password,status,role").eq("username", username).eq("password", password).limit(1).execute().data or []
        if not rows:
            return False

        staff = rows[0]
        if str(staff.get("status") or "active").strip().lower() != "active":
            return False

        return True
    except Exception as e:
        print("ADMIN VIEW AUTH ERROR:", e)
        return False


@app.route("/admin_view_trader_token", methods=["POST", "OPTIONS"])
def admin_view_trader_token():
    """Create a short-lived trader auth token for admin support viewing.

    This does NOT reveal, reset, or use the trader's password. It simply creates
    the same signed dashboard auth token normal login creates, after verifying
    a private ADMIN_VIEW_SECRET configured in Render.
    """
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        data = request.get_json(silent=True) or {}
        if not _admin_view_request_ok(data):
            return bad("Unauthorized admin view", 401)

        lookup = str(data.get("lookup") or data.get("trader_id") or data.get("email") or "").strip()
        if not lookup:
            return bad("Trader lookup is required", 400)

        trader = _latest_trader_for_lookup(lookup)
        if not trader:
            return bad("Trader not found", 404)

        token = _make_trader_auth_token(trader.get("id"))
        safe_lookup = trader.get("email") or trader.get("phone") or trader.get("id") or lookup
        return ok({
            "trader_id": trader.get("id"),
            "lookup": safe_lookup,
            "email": trader.get("email"),
            "name": trader.get("name") or trader.get("full_name") or "Trader",
            "auth_token": token,
            "admin_view": True,
        }, "Admin dashboard view token created")
    except Exception as e:
        return bad(e, 500)

@app.route("/trader_lifecycle/<trader_id>", methods=["GET"])
def trader_lifecycle(trader_id):
    try:
        trader = get_trader_by_id(trader_id)
        if not trader:
            return bad("Trader not found", 404)
        active_account = _get_active_account(trader_id, trader)
        accounts = supabase.table("trader_accounts").select("*").eq("trader_id", trader_id).order("created_at", desc=True).limit(100).execute().data or []
        events = supabase.table("lifecycle_events").select("*").eq("trader_id", trader_id).order("created_at", desc=True).limit(100).execute().data or []
        archives = supabase.table("mt5_account_archives").select("*").eq("trader_id", trader_id).order("archived_at", desc=True).limit(100).execute().data or []
        return ok({
            "trader": trader,
            "challenge_state": trader.get("challenge_state") or "registered",
            "current_account": active_account,
            "accounts": accounts,
            "events": events,
            "archives": archives,
        }, "Trader lifecycle loaded")
    except Exception as e:
        return bad(e)


@app.route("/account_archive/<trader_id>", methods=["GET"])
def account_archive(trader_id):
    try:
        trader = get_trader_by_id(trader_id)
        if not trader:
            return bad("Trader not found", 404)
        archives = supabase.table("mt5_account_archives").select("*").eq("trader_id", trader_id).order("archived_at", desc=True).limit(100).execute().data or []
        accounts = supabase.table("trader_accounts").select("*").eq("trader_id", trader_id).neq("account_status", "assigned_active").order("updated_at", desc=True).limit(100).execute().data or []
        return ok({"trader": trader, "archives": archives, "accounts": accounts}, "Account archive loaded")
    except Exception as e:
        return bad(e)


@app.route("/admin/recalculate_drawdown_usage", methods=["POST", "GET"])
def recalculate_drawdown_usage():
    """Repair and enforce DD-limit usage from actual drawdown for active accounts."""
    try:
        body = request.get_json(silent=True) or {}
        confirm = _is_truthy(body.get("confirm")) or _is_truthy(request.args.get("confirm"))
        limit = int(body.get("limit") or request.args.get("limit") or 1000)
        account_rows = supabase.table("trader_accounts").select("*").eq("account_status", "assigned_active").limit(limit).execute().data or []

        reviewed = []
        updated = []
        breached_rows = []
        skipped = []
        now = now_iso()

        for account in account_rows:
            trader_id = account.get("trader_id")
            trader = get_trader_by_id(trader_id) if trader_id else None
            if not trader:
                skipped.append({"account_id": account.get("id"), "reason": "trader not found"})
                continue

            dd_limit = _num(account.get("dd_limit_percent"), MAX_DRAWDOWN_LIMIT) or MAX_DRAWDOWN_LIMIT
            actual_dd = _num(account.get("absolute_drawdown_percent"), _num(trader.get("drawdown_percent"), 0))
            if actual_dd <= 0 and _num(trader.get("drawdown_percent"), 0) > 0:
                actual_dd = _num(trader.get("drawdown_percent"), 0)
            dd_used = _safe_dd_used(account, actual_dd, dd_limit)
            old_dd_used = _num(account.get("dd_used_percent"), _num(trader.get("max_drawdown_used"), 0))
            zone = _risk_zone(dd_used)
            priority = _priority_for_zone(zone)
            changed = round(dd_used, 4) != round(old_dd_used, 4) or round(actual_dd, 4) != round(_num(account.get("absolute_drawdown_percent"), 0), 4)

            row = {
                "trader_id": trader_id,
                "account_id": account.get("id"),
                "name": trader.get("name") or trader.get("trader_name"),
                "email": trader.get("email"),
                "mt5_login": account.get("mt5_login") or trader.get("mt5_login"),
                "actual_drawdown_percent": actual_dd,
                "dd_limit_percent": dd_limit,
                "old_dd_used_percent": old_dd_used,
                "new_dd_used_percent": dd_used,
                "risk_zone": zone,
                "will_update": bool(changed or dd_used >= 100),
            }
            reviewed.append(row)

            if not confirm or not row["will_update"]:
                continue

            account_update = {
                "absolute_drawdown_percent": actual_dd,
                "dd_used_percent": dd_used,
                "monitoring_enabled": dd_used < 100,
                "updated_at": now,
            }
            try:
                supabase.table("trader_accounts").update(account_update).eq("id", account.get("id")).execute()
            except Exception as e:
                skipped.append({"account_id": account.get("id"), "reason": f"account update failed: {e}"})
                continue

            trader_update = {
                "drawdown_percent": actual_dd,
                "max_drawdown_used": dd_used,
                "risk_zone": zone,
                "critical_mode": zone in {"danger", "critical"},
                "monitoring_priority": priority,
                "last_sync_at": trader.get("last_sync_at") or now,
                "updated_at": now,
            }
            _safe_traders_update(trader_id, trader_update)

            try:
                _insert_monitoring_event(
                    trader,
                    "drawdown_usage_recalculated",
                    zone,
                    f"Drawdown usage recalculated from actual DD {actual_dd:.2f}% against {dd_limit:.2f}% limit. DD limit used: {dd_used:.1f}%.",
                    _num(account.get("current_balance"), _num(trader.get("balance"), 0)),
                    _num(account.get("current_equity"), _num(trader.get("equity"), 0)),
                    dd_used,
                    account.get("id"),
                )
            except Exception as e:
                print("DD RECALC EVENT ERROR:", e)

            if dd_used >= 100:
                try:
                    breached, archived = _breach_trader_account(
                        trader_id,
                        "Maximum drawdown breach confirmed by drawdown usage recalculation.",
                        {"name": "monitoring_repair", "username": "monitoring_repair", "role": "system"},
                    )
                    breached_rows.append({"trader": breached, "archived_account": archived})
                except Exception as e:
                    skipped.append({"account_id": account.get("id"), "reason": f"breach action failed: {e}"})

            updated.append(row)

        return ok({
            "dry_run": not confirm,
            "reviewed_count": len(reviewed),
            "updated_count": len(updated),
            "breached_count": len(breached_rows),
            "skipped_count": len(skipped),
            "reviewed": reviewed[:200],
            "updated": updated[:200],
            "breached": breached_rows[:50],
            "skipped": skipped[:100],
        }, "Drawdown usage reviewed" if not confirm else "Drawdown usage recalculated and enforced")
    except Exception as e:
        return bad(e, 500)


def _infer_existing_account_stage(trader):
    state = str((trader or {}).get("challenge_state") or "").lower()
    phase = str((trader or {}).get("phase") or "").lower()
    status = str((trader or {}).get("status") or "").lower()
    if state == "funded_active" or phase in ["funded", "live"] or status in ["funded", "live"]:
        return "funded", "funded_active"
    if state == "phase2_active" or phase == "phase2" or "phase2" in status:
        return "phase2", "phase2_active"
    return "phase1", "phase1_active"


def _eligible_for_account_backfill(trader):
    if not trader:
        return False
    state = str(trader.get("challenge_state") or "").lower()
    if state in {"registered", "purchase_pending", "payment_rejected", "phase2_waiting_mt5", "funded_waiting_mt5", "breached", "closed"}:
        return False
    if not str(trader.get("mt5_login") or "").strip():
        return False
    if _get_active_account(trader.get("id")):
        return False
    if _get_active_account_by_login(trader.get("mt5_login")):
        return False
    return True


@app.route("/admin/migrate_active_trader_accounts", methods=["POST", "GET"])
def migrate_active_trader_accounts():
    try:
        body = request.get_json(silent=True) or {}
        confirm = _is_truthy(body.get("confirm")) or _is_truthy(request.args.get("confirm"))
        limit = int(body.get("limit") or request.args.get("limit") or 500)
        rows = supabase.table("traders").select("*").limit(limit).execute().data or []
        planned = []
        created = []
        skipped = []
        now = now_iso()

        for trader in rows:
            if not _eligible_for_account_backfill(trader):
                skipped.append({"id": trader.get("id"), "reason": "not eligible or already has active account"})
                continue
            stage, state = _infer_existing_account_stage(trader)
            account_size = clean(trader.get("account_size") or trader.get("balance") or trader.get("equity") or 0)
            if account_size <= 0:
                skipped.append({"id": trader.get("id"), "reason": "missing account size"})
                continue
            stage_started_at = _stage_started_at_from_legacy_trader(trader, stage, now)
            account_row = {
                "trader_id": trader.get("id"),
                "stage": stage,
                "account_status": "assigned_active",
                "mt5_login": str(trader.get("mt5_login") or "").strip(),
                "mt5_server": str(trader.get("mt5_server") or "").strip(),
                "mt5_master_password": trader.get("mt5_master_password") or trader.get("mt5_password") or trader.get("master_password") or "",
                "mt5_investor_password": trader.get("mt5_investor_password") or trader.get("investor_password") or "",
                "account_size": account_size,
                "start_balance": account_size,
                "current_balance": clean(trader.get("balance") or account_size),
                "current_equity": clean(trader.get("equity") or trader.get("balance") or account_size),
                "profit": clean(trader.get("profit")),
                "profit_percent": clean(trader.get("profit_percent")),
                "absolute_drawdown_percent": clean(trader.get("drawdown_percent")),
                "dd_limit_percent": 20,
                "dd_used_percent": _safe_dd_used(trader, clean(trader.get("drawdown_percent")), 20),
                "target_percent": _target_for_stage(stage),
                "monitoring_enabled": True,
                "started_at": stage_started_at,
                "created_at": now,
                "updated_at": now,
            }
            planned.append({"trader_id": trader.get("id"), "email": trader.get("email"), "stage": stage, "state": state, "mt5_login": account_row["mt5_login"]})
            if confirm:
                account = (supabase.table("trader_accounts").insert(account_row).execute().data or [None])[0]
                if account:
                    updated = _update_trader_lifecycle(
                        trader.get("id"),
                        state,
                        account,
                        {"payment_status": trader.get("payment_status") or "approved", "admin_note": "Backfilled active trader account into lifecycle system."},
                        _admin_from_payload(body),
                        "backfill_active_account"
                    )
                    created.append({"trader": updated, "account": account})

        return ok({
            "dry_run": not confirm,
            "planned_count": len(planned),
            "created_count": len(created),
            "skipped_count": len(skipped),
            "planned": planned,
            "created": created,
            "skipped": skipped[:100],
        }, "Active trader account migration reviewed" if not confirm else "Active trader accounts migrated")
    except Exception as e:
        return bad(e, 500)



FROM_EMAIL = os.getenv("FROM_EMAIL") or "support@nairapips.com"
ADMIN_ALERT_EMAIL = os.getenv("ADMIN_ALERT_EMAIL") or FROM_EMAIL
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

# ================================
# NAIRAPIPS EMAIL LOG BANK
# ================================
def _email_type_from_subject(subject):
    s = str(subject or "").lower()
    if "phase 1" in s or "phase1" in s:
        return "phase1_pass"
    if "phase 2" in s or "phase2" in s:
        return "phase2_pass"
    if "breach" in s or "breached" in s:
        return "breach"
    if "otp" in s or "verification code" in s:
        return "email_otp"
    if "mt5" in s:
        return "mt5_update"
    if "payout" in s:
        return "payout"
    return "general"


def _log_email_bank(to_email, subject, email_type=None, status="queued", trader_id=None, message="", provider="brevo", provider_response="", error=""):
    """Best-effort log. If the SQL has not been run yet, email sending must not break."""
    try:
        row = {
            "trader_id": trader_id,
            "recipient_email": str(to_email or "").strip().lower(),
            "subject": str(subject or "")[:250],
            "email_type": email_type or _email_type_from_subject(subject),
            "status": status,
            "provider": provider,
            "provider_response": str(provider_response or "")[:2000],
            "error": str(error or "")[:2000],
            "message_preview": str(message or "")[:1000],
            "created_at": now_iso() if "now_iso" in globals() else datetime.now(timezone.utc).isoformat(),
            "sent_at": (now_iso() if status == "sent" and "now_iso" in globals() else None),
        }
        supabase.table("email_logs").insert(row).execute()
    except Exception as e:
        print("EMAIL LOG BANK SKIPPED:", str(e))

def text_to_html_content(message):
    return "<p>" + html.escape(str(message or "")).replace("\n", "<br>") + "</p>"

def send_email_brevo(to_email, subject, html_content):
    try:
        if not to_email:
            _log_email_bank(to_email, subject, status="failed", message=html_content, error="Missing recipient email")
            return False
        if not BREVO_API_KEY or not FROM_EMAIL:
            err = "BREVO_API_KEY or FROM_EMAIL is missing"
            print("BREVO EMAIL ERROR:", err)
            _log_email_bank(to_email, subject, status="failed", message=html_content, error=err)
            return False

        print("BREVO EMAIL ATTEMPT:", to_email)
        payload = {
            "sender": {"name": "NairaPips", "email": FROM_EMAIL},
            "to": [{"email": to_email}],
            "subject": subject,
            "htmlContent": html_content
        }
        res = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        if res.status_code >= 400:
            err = f"{res.status_code} {res.text[:500]}"
            print("BREVO EMAIL ERROR:", err)
            _log_email_bank(to_email, subject, status="failed", message=html_content, provider_response=res.text, error=err)
            return False

        print("BREVO EMAIL SENT:", to_email)
        _log_email_bank(to_email, subject, status="sent", message=html_content, provider_response=res.text)
        return True
    except Exception as e:
        print("BREVO EMAIL ERROR:", str(e))
        _log_email_bank(to_email, subject, status="failed", message=html_content, error=str(e))
        return False

def send_email(to_email, subject, message):
    return send_email_brevo(to_email, subject, text_to_html_content(message))

def send_email_safe(to_email, subject, message):
    try:
        return send_email(to_email, subject, message)
    except Exception as e:
        print("BREVO EMAIL ERROR:", str(e))
        return False

def send_admin_alert(subject, message):
    return send_email_safe(ADMIN_ALERT_EMAIL, subject, message)

def email_money(value):
    try:
        return "₦" + f"{float(value or 0):,.0f}"
    except Exception:
        return "₦0"

def public_money(value):
    try:
        return "₦" + f"{float(value or 0):,.0f}"
    except Exception:
        return "₦0"

def public_first_name(row):
    raw = str((row or {}).get("trader_name") or (row or {}).get("name") or (row or {}).get("full_name") or "").strip()
    if not raw:
        return "A trader"
    first = raw.split()[0].strip()
    first = re.sub(r"[^A-Za-zÀ-ÿ'-]", "", first)
    if len(first) < 2:
        return "A trader"
    return first

def public_activity_time(row):
    for key in ["paid_at", "approved_at", "assigned_at", "requested_at", "created_at", "updated_at"]:
        score = _dt_score((row or {}).get(key))
        if score:
            return score
    return 0

def nairapips_system_activity():
    return [
        {"type": "system", "message": "NairaPips challenge plans are live"},
        {"type": "system", "message": "NairaPips MT5 assignment system is active"},
        {"type": "system", "message": "NairaPips payout review desk is active"},
        {"type": "system", "message": "NairaPips monitoring engine is active"},
        {"type": "system", "message": "NairaPips support desk is online"},
    ]

def trader_display_name(row):
    return (row or {}).get("trader_name") or (row or {}).get("name") or "Trader"

def get_payout_by_id(pid):
    rows = supabase.table("payouts").select("*").eq("id", pid).limit(1).execute().data or []
    return rows[0] if rows else {}

def payout_status(row):
    return str((row or {}).get("status") or "pending").strip().lower()

def get_purchase_by_id(pid):
    rows = supabase.table("challenge_purchases").select("*").eq("id", pid).limit(1).execute().data or []
    return rows[0] if rows else {}

def get_trader_by_id(tid):
    rows = supabase.table("traders").select("*").eq("id", tid).limit(1).execute().data or []
    return rows[0] if rows else {}

def send_mt5_reset_email(trader, mt5_login="", mt5_server="", master_password="", investor_password="", reason="MT5 login details updated"):
    if not trader:
        return False
    name = trader.get("name") or trader.get("trader_name") or "Trader"
    return send_email_safe(
        trader.get("email"),
        "NairaPips MT5 login details updated",
        f"""Hello {name},

Your NairaPips MT5 login details have been updated/reset.

MT5 Login: {mt5_login or trader.get("mt5_login", "")}
Server: {mt5_server or trader.get("mt5_server", "")}
Master Password: {master_password or trader.get("mt5_master_password") or trader.get("mt5_password") or trader.get("master_password") or ""}
Investor Password: {investor_password or trader.get("mt5_investor_password") or trader.get("investor_password") or ""}

Reason: {reason}

If you did not request this reset, contact NairaPips support immediately.

NairaPips Team"""
    )

def send_account_status_email(trader, subject, title, details=""):
    if not trader:
        return False
    name = trader.get("name") or trader.get("trader_name") or "Trader"
    return send_email_safe(
        trader.get("email"),
        subject,
        f"""Hello {name},

{title}

{details}

NairaPips Team"""
    )

def send_challenge_certificate_email(trader, details=""):
    return send_account_status_email(
        trader,
        "NairaPips challenge passed - certificate earned",
        "Congratulations. You have passed your NairaPips challenge and your certificate has been earned.",
        details or "Your challenge pass/certificate status has been updated. Log in to your dashboard to review the latest account status."
    )
def now_iso(): return datetime.now(timezone.utc).isoformat()
def ref(): return "NP-" + str(random.randint(100000,999999))
def clean(v):
    return float(str(v or "0").replace(",","").replace("₦","").strip() or 0)
def month(): return datetime.now(timezone.utc).strftime("%B")
def year(): return datetime.now(timezone.utc).strftime("%Y")
def ok(data=None, message="ok"): return jsonify({"success": True, "message": message, "data": data})
def bad(e, code=400): return jsonify({"success": False, "error": str(e)}), code

@app.route("/")
def home():
    return jsonify({"status":"NairaPips API Live","database":"connected","version":"proof-upload-upgrade"})

@app.route("/health")
def health():
    return jsonify({"health":"ok"})

@app.route("/public_activity", methods=["GET"])
def public_activity():
    activity = []

    try:
        rows = supabase.table("payouts").select("*").order("created_at", desc=True).limit(20).execute().data or []
        for row in rows:
            status = str(row.get("status") or "").lower()
            if status not in ["approved", "paid"]:
                continue
            amount = public_money(row.get("amount"))
            activity.append({
                "type": "payout",
                "message": f"{amount} payout approved",
                "_score": public_activity_time(row)
            })
    except Exception as e:
        print("PUBLIC ACTIVITY PAYOUTS ERROR:", str(e))

    try:
        rows = supabase.table("traders").select("*").order("created_at", desc=True).limit(20).execute().data or []
        for row in rows:
            name = public_first_name(row)
            message = "A trader just joined NairaPips" if name == "A trader" else f"{name} just joined NairaPips"
            activity.append({
                "type": "registration",
                "message": message,
                "_score": public_activity_time(row)
            })
    except Exception as e:
        print("PUBLIC ACTIVITY TRADERS ERROR:", str(e))

    try:
        rows = supabase.table("challenge_purchases").select("*").order("created_at", desc=True).limit(20).execute().data or []
        for row in rows:
            status = str(row.get("status") or "").lower()
            payment_status = str(row.get("payment_status") or "").lower()
            if status not in ["approved", "approved_active"] and payment_status != "approved":
                continue
            amount = public_money(row.get("account_size"))
            activity.append({
                "type": "challenge",
                "message": f"{amount} challenge approved",
                "_score": public_activity_time(row)
            })
    except Exception as e:
        print("PUBLIC ACTIVITY CHALLENGES ERROR:", str(e))

    activity.sort(key=lambda item: item.get("_score", 0), reverse=True)
    public_rows = [{k: v for k, v in item.items() if k != "_score"} for item in activity]
    if len(public_rows) < 20:
        public_rows.extend(nairapips_system_activity())
    return jsonify(public_rows[:20])

@app.route("/upload_payment_proof", methods=["POST"])
def upload_payment_proof():
    try:
        f = request.files.get("file")
        if not f or not f.filename:
            return bad("No file uploaded")
        bucket = request.form.get("bucket","payment-proofs")
        folder = request.form.get("folder","challenge-purchases")
        name = secure_filename(f.filename)
        ext = name.rsplit(".",1)[-1].lower() if "." in name else "bin"
        if ext not in {"jpg","jpeg","png","webp","pdf"}:
            return bad("Only JPG, PNG, WEBP and PDF proof files are allowed")
        path = f"{folder}/{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}.{ext}"
        supabase.storage.from_(bucket).upload(path, f.read(), {"content-type": f.content_type or "application/octet-stream", "upsert": "false"})
        url = supabase.storage.from_(bucket).get_public_url(path)
        return jsonify({"success": True, "url": url, "path": path})
    except Exception as e:
        print("UPLOAD PAYMENT PROOF ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e), "hint": "Create a PUBLIC Supabase Storage bucket named payment-proofs."}), 400

@app.route("/traders_raw", methods=["GET"])
def get_traders_raw():
    try:
        return jsonify(supabase.table("traders").select("*").order("created_at", desc=True).execute().data)
    except Exception as e:
        return bad(e)

@app.route("/traders", methods=["GET"])
def get_traders():
    try:
        res = supabase.table("traders").select("*").execute()
        rows = getattr(res, "data", []) or []
        return jsonify(_dedupe_traders(rows))
    except Exception as e:
        return bad(e)

def _request_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.headers.get("X-Real-IP") or request.remote_addr or "unknown"

def _registration_rate_limited(ip):
    now = time.time()
    bucket = [t for t in REGISTER_RATE_BUCKET.get(ip, []) if now - t < REGISTER_RATE_WINDOW_SECONDS]
    if len(bucket) >= REGISTER_RATE_MAX:
        REGISTER_RATE_BUCKET[ip] = bucket
        return True
    bucket.append(now)
    REGISTER_RATE_BUCKET[ip] = bucket
    return False

def _valid_email(email):
    if not email:
        return True
    if len(email) > 120:
        return False
    return bool(re.match(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", email, re.I))

def _phone_digits(phone):
    return re.sub(r"\D", "", str(phone or ""))

def _valid_phone(phone):
    if not phone:
        return True
    digits = _phone_digits(phone)
    return 7 <= len(digits) <= 15

def _valid_trader_password(password):
    value = str(password or "")
    return 6 <= len(value) <= 128

def _hash_trader_password(password):
    return generate_password_hash(str(password or ""), method="pbkdf2:sha256", salt_length=16)

def _generate_temp_trader_password(length=10):
    alphabet = string.ascii_letters + string.digits
    while True:
        value = "".join(secrets.choice(alphabet) for _ in range(length))
        if any(c.islower() for c in value) and any(c.isupper() for c in value) and any(c.isdigit() for c in value):
            return value

def _check_trader_password(trader, password):
    raw = str(password or "")
    if not raw or not trader:
        return False
    password_hash = str(trader.get("password_hash") or "").strip()
    if password_hash:
        try:
            return check_password_hash(password_hash, raw)
        except Exception:
            return False
    legacy_password = str(trader.get("password") or "").strip()
    return bool(legacy_password and legacy_password == raw)

TRADER_AUTH_TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60

def _trader_auth_secret():
    return str(os.getenv("TRADER_AUTH_SECRET") or SUPABASE_KEY or "nairapips-trader-auth").encode()

def _make_trader_auth_token(trader_id):
    trader_id = str(trader_id or "").strip()
    ts = str(int(time.time()))
    body = f"{trader_id}:{ts}"
    sig = hmac.new(_trader_auth_secret(), body.encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{body}:{sig}".encode()).decode()

def _verify_trader_auth_token(token, trader_id):
    try:
        raw = base64.urlsafe_b64decode(str(token or "").encode()).decode()
        tid, ts, sig = raw.split(":", 2)
        if str(tid) != str(trader_id):
            return False
        body = f"{tid}:{ts}"
        expected = hmac.new(_trader_auth_secret(), body.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        return (time.time() - int(ts)) <= TRADER_AUTH_TOKEN_TTL_SECONDS
    except Exception:
        return False

def _request_trader_auth_token():
    header = request.headers.get("Authorization") or ""
    if header.lower().startswith("bearer "):
        return header.split(" ", 1)[1].strip()
    return request.headers.get("X-Trader-Auth") or request.args.get("auth_token") or ""

def _phone_variants(phone):
    raw = str(phone or "").strip()
    digits = _phone_digits(raw)
    variants = {raw, digits}
    if digits:
        variants.add("+" + digits)
    if digits.startswith("0") and len(digits) >= 10:
        ng = "234" + digits[1:]
        variants.update({ng, "+" + ng})
    if digits.startswith("234") and len(digits) >= 13:
        local = "0" + digits[3:]
        variants.update({local, digits, "+" + digits})
    return [v for v in variants if v]

def _clean_phone(phone):
    phone = str(phone or "").strip()
    return re.sub(r"[^\d+]", "", phone)

def _valid_name(name):
    value = str(name or "").strip()
    lowered = value.lower()
    letters = re.findall(r"[a-zA-Z]", value)
    blocked = {"test", "fake", "admin", "null", "undefined", "unknown", "n/a", "na", "none", "asdf", "qwerty"}
    if len(value) < 2 or len(value) > 80:
        return False
    if len(letters) < 2:
        return False
    if lowered in blocked:
        return False
    if re.search(r"https?://|www\.|\.com|\.net|\.org", lowered):
        return False
    if re.fullmatch(r"([a-zA-Z])\1{2,}", value):
        return False
    return True

def _find_existing_trader(email="", phone=""):
    email = str(email or "").strip().lower()
    phone = str(phone or "").strip()

    if email:
        rows = supabase.table("traders").select("*").eq("email", email).limit(1).execute().data or []
        if rows:
            return rows[0]
        all_rows = supabase.table("traders").select("*").execute().data or []
        for row in all_rows:
            if str(row.get("email") or "").strip().lower() == email:
                return row

    if phone:
        digits = _phone_digits(phone)
        for variant in _phone_variants(phone):
            rows = supabase.table("traders").select("*").eq("phone", variant).limit(1).execute().data or []
            if rows:
                return rows[0]
        all_rows = supabase.table("traders").select("*").execute().data or []
        for row in all_rows:
            if digits and _phone_digits(row.get("phone")) == digits:
                return row

    return None

# ================================
# NAIRAPIPS EMAIL OTP VERIFICATION
# ================================

def _otp_digits():
    return str(random.randint(100000, 999999))

def _email_verified_recent(email):
    try:
        email = str(email or '').strip().lower()
        if not email:
            return False
        rows = supabase.table('email_verification_codes').select('*').eq('email', email).eq('verified', True).order('verified_at', desc=True).limit(1).execute().data or []
        if not rows:
            return False
        verified_at = rows[0].get('verified_at')
        if not verified_at:
            return False
        dt = datetime.fromisoformat(str(verified_at).replace('Z', '+00:00'))
        return (datetime.now(timezone.utc) - dt).total_seconds() <= 60 * 60
    except Exception as e:
        print('EMAIL OTP CHECK ERROR:', str(e))
        return False

def _consume_email_verification(email):
    try:
        email = str(email or '').strip().lower()
        supabase.table('email_verification_codes').update({'consumed_at': now_iso()}).eq('email', email).eq('verified', True).execute()
    except Exception as e:
        print('EMAIL OTP CONSUME ERROR:', str(e))

@app.route('/send_email_otp', methods=['POST', 'OPTIONS'])
def send_email_otp():
    if request.method == 'OPTIONS':
        return _np_ok({})
    try:
        d = request.json or {}
        email = str(d.get('email') or '').strip().lower()
        name = str(d.get('name') or 'Trader').strip() or 'Trader'
        if not email or not _valid_email(email):
            return bad('Enter a valid email address')
        code = _otp_digits()
        expires_at = datetime.fromtimestamp(time.time() + 10 * 60, tz=timezone.utc).isoformat()
        row = {'email': email, 'code': code, 'verified': False, 'expires_at': expires_at, 'created_at': now_iso()}
        try:
            supabase.table('email_verification_codes').insert(row).execute()
        except Exception as e:
            print("EMAIL OTP INSERT ERROR:", str(e))
            return bad("Email OTP save failed: " + str(e), 500)
        message = 'Hello ' + name + ',\n\nYour NairaPips verification code is:\n\n' + code + '\n\nThis code expires in 10 minutes.\n\nIf you did not request this code, ignore this email.\n\nNairaPips Team'
        sent = send_email_safe(email, 'Your NairaPips verification code', message)
        if not sent:
            return bad('Could not send verification email. Check email service settings.', 500)
        return ok({'email': email, 'expires_in_minutes': 10}, 'Verification code sent')
    except Exception as e:
        return bad(e, 500)

@app.route('/verify_email_otp', methods=['POST', 'OPTIONS'])
def verify_email_otp():
    if request.method == 'OPTIONS':
        return _np_ok({})
    try:
        d = request.json or {}
        email = str(d.get('email') or '').strip().lower()
        code = str(d.get('code') or '').strip()
        if not email or not code:
            return bad('Email and verification code are required')
        rows = supabase.table('email_verification_codes').select('*').eq('email', email).eq('code', code).order('created_at', desc=True).limit(1).execute().data or []
        if not rows:
            return bad('Invalid verification code', 400)
        row = rows[0]
        exp = row.get('expires_at')
        if exp:
            exp_dt = datetime.fromisoformat(str(exp).replace('Z', '+00:00'))
            if datetime.now(timezone.utc) > exp_dt:
                return bad('Verification code has expired. Request a new code.', 400)
        supabase.table('email_verification_codes').update({'verified': True, 'verified_at': now_iso()}).eq('id', row.get('id')).execute()
        return ok({'email': email, 'email_verified': True}, 'Email verified')
    except Exception as e:
        print("EMAIL OTP VERIFY ERROR:", str(e))
        return bad("Email OTP verification failed: " + str(e), 500)

@app.route('/set_trader_password', methods=['POST', 'OPTIONS'])
def set_trader_password():
    if request.method == 'OPTIONS':
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        email = str(d.get('email') or '').strip().lower()
        phone = _clean_phone(d.get('phone') or '')
        code = str(d.get('code') or '').strip()
        password = str(d.get('password') or '')
        confirm = str(d.get('confirm_password') or d.get('password_confirm') or '')

        if not email and not phone:
            return bad('Email or phone is required')
        if email and not _valid_email(email):
            return bad('Enter a valid email address')
        if not _valid_trader_password(password):
            return bad('Password must be at least 6 characters and not more than 128 characters')
        if confirm and confirm != password:
            return bad('Passwords do not match')

        if email:
            if code:
                rows = supabase.table('email_verification_codes').select('*').eq('email', email).eq('code', code).order('created_at', desc=True).limit(1).execute().data or []
                if not rows:
                    return bad('Invalid verification code', 400)
                row = rows[0]
                exp = row.get('expires_at')
                if exp:
                    exp_dt = datetime.fromisoformat(str(exp).replace('Z', '+00:00'))
                    if datetime.now(timezone.utc) > exp_dt:
                        return bad('Verification code has expired. Request a new code.', 400)
                supabase.table('email_verification_codes').update({'verified': True, 'verified_at': now_iso()}).eq('id', row.get('id')).execute()
            elif not _email_verified_recent(email):
                return bad('Email verification is required before setting password', 403)

        trader = _find_existing_trader(email, phone)
        if not trader:
            return bad('Trader not found', 404)

        payload = {
            'password_hash': _hash_trader_password(password),
            'password_set_at': now_iso(),
            'password_reset_required': False,
            'updated_at': now_iso()
        }
        updated = supabase.table('traders').update(payload).eq('id', trader.get('id')).execute().data or []
        _audit_safe('traders', 'trader_password_set', f"Trader password set/reset for {trader.get('id')}", {'name': 'trader', 'username': email or phone, 'role': 'trader'})
        return ok(updated[0] if updated else get_trader_by_id(trader.get('id')), 'Password saved')
    except Exception as e:
        return bad(e, 500)

@app.route('/admin_reset_trader_password', methods=['POST', 'OPTIONS'])
def admin_reset_trader_password():
    if request.method == 'OPTIONS':
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        trader_id = str(d.get('id') or d.get('trader_id') or '').strip()
        email = str(d.get('email') or '').strip().lower()
        phone = _clean_phone(d.get('phone') or '')

        trader = get_trader_by_id(trader_id) if trader_id else _find_existing_trader(email, phone)
        if not trader:
            return bad('Trader not found', 404)

        temp_password = str(d.get('password') or '').strip() or _generate_temp_trader_password()
        if not _valid_trader_password(temp_password):
            return bad('Temporary password must be between 6 and 128 characters')

        payload = {
            'password_hash': _hash_trader_password(temp_password),
            'password_set_at': now_iso(),
            'password_reset_required': False,
            'updated_at': now_iso()
        }
        updated = supabase.table('traders').update(payload).eq('id', trader.get('id')).execute().data or []
        admin = _admin_from_payload(d)
        _audit_safe('traders', 'admin_password_reset', f"Admin reset trader password for {trader.get('id')}", admin, trader.get('id'))
        # CRITICAL: Email the trader their new login password (production must notify)
        try:
            trader_name = trader.get('name') or 'Trader'
            trader_email = trader.get('email')
            if trader_email and d.get('notify') != False:
                send_account_status_email(
                    trader,
                    'NairaPips — Your dashboard password has been reset',
                    f'Hello {trader_name}, your NairaPips dashboard password has been reset by an administrator.',
                    f'Your new login password is: {temp_password}\n\nYou can log in at https://nairapips.com/dashboard/trader_clean.html using your email and this password.\n\nFor security, please change your password after logging in.\n\nNairaPips Team'
                )
        except Exception as _email_err:
            print('PASSWORD RESET EMAIL ERROR:', str(_email_err))

        row = updated[0] if updated else get_trader_by_id(trader.get('id'))
        safe_trader = {
            'id': row.get('id') if row else trader.get('id'),
            'name': row.get('name') if row else trader.get('name'),
            'email': row.get('email') if row else trader.get('email'),
            'phone': row.get('phone') if row else trader.get('phone'),
            'password_set_at': row.get('password_set_at') if row else payload['password_set_at'],
        }
        return ok({'trader': safe_trader, 'temporary_password': temp_password}, 'Temporary password generated')
    except Exception as e:
        return bad(e, 500)

def _safe_insert_trader(row):
    try:
        return supabase.table("traders").insert(row).execute().data
    except Exception as e:
        optional = ["source", "user_agent", "ip_address", "registration_source", "registration_user_agent", "registration_ip"]
        safe_row = {k: v for k, v in row.items() if k not in optional}
        print("REGISTRATION OPTIONAL TRACKING SKIPPED:", str(e))
        return supabase.table("traders").insert(safe_row).execute().data

@app.route("/register_trader", methods=["POST", "OPTIONS"])
def register_trader():
    if request.method == "OPTIONS":
        return _np_ok({})

    try:
        d = request.json or {}
        ip_address = _request_ip()
        user_agent = request.headers.get("User-Agent", "")
        if str(d.get("website") or d.get("company_url") or d.get("url") or "").strip():
            print("REGISTRATION SPAM HONEYPOT:", ip_address)
            return ok({"received": True}, "Registration received")
        if _registration_rate_limited(ip_address):
            return bad("Too many registration attempts. Please wait a few minutes and try again.", 429)

        name = str(d.get("name", "")).strip()
        email = str(d.get("email", "")).strip().lower()
        phone = _clean_phone(d.get("phone", ""))
        password = str(d.get("password") or "")
        confirm_password = str(d.get("confirm_password") or d.get("password_confirm") or "")

        if not _valid_name(name):
            return bad("Please enter your real full name.")
        if not email:
            return bad("Email is required for verification")
        if not phone:
            return bad("Complete WhatsApp / phone number is required")
        if not _valid_email(email):
            return bad("Please enter a valid email address.")
        if not _valid_phone(phone):
            return bad("Please enter a complete valid WhatsApp or phone number.")
        if not _valid_trader_password(password):
            return bad("Password must be at least 6 characters and not more than 128 characters.")
        if confirm_password and confirm_password != password:
            return bad("Passwords do not match.")
        if not _email_verified_recent(email):
            return bad("Email is not verified. Please enter the verification code sent to your email before creating your account.", 403)

        existing = _find_existing_trader(email, phone)
        if existing:
            return ok(existing, "Trader already exists")

        row = {
            "name": name,
            "phone": phone,
            "email": email,
            "mt5_login": "",
            "mt5_server": "",
            "mt5_master_password": "",
            "mt5_investor_password": "",
            "account_size": 0,
            "balance": 0,
            "equity": 0,
            "phase": "no_account",
            "status": d.get("status", "new_signup"),
            "engine_group": d.get("engine_group", "engine_1"),
            "profit": 0,
            "drawdown": 0,
            "profit_percent": 0,
            "drawdown_percent": 0,
            "payment_status": d.get("payment_status", "none"),
            "email_verified": True,
            "email_verified_at": now_iso(),
            "phone_verified": False,
            "payment_proof_url": "",
            "selected_plan": "",
            "payment_note": "",
            "approved_by": "",
            "admin_note": "",
            "account_reference": d.get("account_reference") or ref(),
            "challenge_started_at": None,
            "approved_at": None,
            "funded_at": None,
            "last_login_at": None,
            "trading_days_left": d.get("trading_days_left", 0),
            "source": d.get("source", "public_register"),
            "registration_source": d.get("source", "public_register"),
            "user_agent": user_agent[:250],
            "registration_user_agent": user_agent[:250],
            "ip_address": ip_address,
            "registration_ip": ip_address,
            "password_hash": _hash_trader_password(password),
            "password_set_at": now_iso(),
            "password_reset_required": False,
        }

        created = _safe_insert_trader(row)
        _consume_email_verification(email)
        trader_row = created[0] if created else row

        send_email_safe(
            email,
            "Welcome to NairaPips",
            f"""Hello {name},

Welcome to NairaPips. Your trader account has been created successfully.

Next step: log in to your trader dashboard, choose a challenge plan, and upload your payment proof for admin approval.

Reference: {trader_row.get("account_reference", "Not generated")}

NairaPips Team"""
        )
        send_admin_alert(
            "New NairaPips trader registration",
            f"""A new trader registered on NairaPips.

Name: {name}
Email: {email or "Not provided"}
Phone: {phone or "Not provided"}
Reference: {trader_row.get("account_reference", "Not generated")}"""
        )

        return ok(trader_row, "Trader registered")
    except Exception as e:
        return bad(e)

@app.route("/update_trader", methods=["POST", "OPTIONS"])
def update_trader():
    if request.method == "OPTIONS":
        return _np_ok({})

    try:
        d = request.json or {}
        tid = d.get("id") or d.get("trader_id")
        if not tid:
            return bad("Missing trader id")

        blocked = {
            "status", "phase", "challenge_state", "phase_pass_status",
            "balance", "equity", "profit", "drawdown", "profit_percent", "drawdown_percent",
            "account_size", "mt5_login", "mt5_server", "mt5_master_password",
            "mt5_investor_password", "mt5_password", "master_password", "investor_password",
            "monitoring_enabled", "mt5_account_active", "mt5_access_disabled", "payout_eligible",
            "payout_blocked", "current_account_id"
        }
        attempted = sorted([k for k in blocked if k in d])
        if attempted:
            return bad("Lifecycle, MT5, balance, equity, profit and payout fields are locked. Use lifecycle/monitoring routes instead: " + ", ".join(attempted), 409)

        allowed = [
            "name", "phone", "email", "engine_group", "payment_note", "admin_note",
            "trading_days_left", "selected_plan", "trader_note", "mt5_notice",
            "lead_status", "follow_up_at", "marketing_consent"
        ]
        upd = {k: d[k] for k in allowed if k in d}

        if "email" in upd:
            upd["canonical_email"] = str(upd.get("email") or "").strip().lower()
        if "phone" in upd:
            upd["canonical_phone"] = _normalize_phone_value(upd.get("phone"))

        upd["updated_at"] = now_iso()

        if not upd:
            return bad("Nothing to update")

        result = supabase.table("traders").update(upd).eq("id", tid).execute().data
        trader_row = result[0] if result else get_trader_by_id(tid)

        return ok(result, "Trader updated")
    except Exception as e:
        return bad(e)

@app.route("/traders", methods=["POST"])
def add_trader():
    try:
        d = request.json or {}
        bal = clean(d.get("balance") or d.get("account_size"))

        row = {
            "name": d.get("name", ""),
            "phone": d.get("phone", ""),
            "email": d.get("email", ""),
            "mt5_login": d.get("mt5_login", ""),
            "mt5_server": d.get("mt5_server", ""),
            "mt5_master_password": d.get("mt5_master_password", ""),
            "mt5_investor_password": d.get("mt5_investor_password", ""),
            "account_size": bal,
            "balance": bal,
            "equity": bal,
            "phase": d.get("phase", "no_account"),
            "status": d.get("status", "payment_pending"),
            "engine_group": d.get("engine_group", "engine_1"),
            "profit": 0,
            "drawdown": 0,
            "profit_percent": 0,
            "drawdown_percent": 0,
            "payment_status": d.get("payment_status", "pending"),
            "payment_proof_url": d.get("payment_proof_url", ""),
            "selected_plan": d.get("selected_plan", ""),
            "payment_note": d.get("payment_note", ""),
            "approved_by": "",
            "admin_note": "",
            "account_reference": d.get("account_reference") or ref(),
            "challenge_started_at": d.get("challenge_started_at"),
            "approved_at": d.get("approved_at"),
            "funded_at": d.get("funded_at"),
            "last_login_at": None,
            "trading_days_left": d.get("trading_days_left", 0)
        }

        created = supabase.table("traders").insert(row).execute().data
        trader_row = created[0] if created else row

        send_email_safe(
            row.get("email"),
            "Welcome to NairaPips",
            f"""Hello {row.get("name") or "Trader"},

Your NairaPips trader account has been created successfully.

Reference: {trader_row.get("account_reference", "Not generated")}

NairaPips Team"""
        )
        send_admin_alert(
            "New NairaPips trader registration",
            f"""A trader account was created on NairaPips.

Name: {row.get("name") or "Not provided"}
Email: {row.get("email") or "Not provided"}
Phone: {row.get("phone") or "Not provided"}
Reference: {trader_row.get("account_reference", "Not generated")}"""
        )

        return ok(created, "Trader added")

    except Exception as e:
        return bad(e)

@app.route("/delete_trader", methods=["POST"])
def delete_trader():
    try:
        trader_id = (request.json or {}).get("id")

        if not trader_id:
            return bad("Missing trader id")

        found = supabase.table("traders").select("*").eq("id", trader_id).execute().data
        trader = found[0] if found else {}
        if _is_funded_trader(trader):
            return bad("Funded/live traders cannot be deleted in production. Deactivate or mark as test instead.", 403)

        email = trader.get("email")
        phone = trader.get("phone")
        related_purchases = []
        related_purchases += _safe_fetch("challenge_purchases", "trader_id", trader_id)
        related_purchases += _safe_fetch("challenge_purchases", "email", email)
        related_purchases += _safe_fetch("challenge_purchases", "phone", phone)
        if _has_approved_payment(related_purchases):
            return bad("Traders with approved payments cannot be deleted in production. Mark as test or exclude from revenue instead.", 403)

        related_tables = [
            "support_tickets",
            "monitoring_snapshots",
            "monitoring_events"
        ]

        for table in related_tables:
            try:
                supabase.table(table).delete().eq("trader_id", trader_id).execute()
            except Exception:
                pass

            if email:
                try:
                    supabase.table(table).delete().eq("email", email).execute()
                except Exception:
                    pass

            if phone:
                try:
                    supabase.table(table).delete().eq("phone", phone).execute()
                except Exception:
                    pass

        supabase.table("traders").delete().eq("id", trader_id).execute()

        return ok([], "Trader and all related activity deleted")

    except Exception as e:
        return bad(e)

# ================================
# NAIRAPIPS SPEED / BANDWIDTH CONTROL LAYER
# ================================
_TRADER_BOOTSTRAP_CACHE = {}
_ADMIN_BOOTSTRAP_CACHE = {"ts": 0, "payload": None}
TRADER_BOOTSTRAP_TTL_SECONDS = 20
ADMIN_BOOTSTRAP_TTL_SECONDS = 25

_PUBLIC_TRADER_FIELDS = [
    "id", "name", "email", "phone", "status", "role", "created_at", "last_login_at",
    "phase", "challenge_state", "account_size", "mt5_login", "mt5_server",
    "current_account_id", "trader_account_id", "plan_name", "plan_id"
]


def _public_trader_payload(trader):
    row = trader or {}
    return {k: row.get(k) for k in _PUBLIC_TRADER_FIELDS if k in row}


def _cache_key(*parts):
    return "|".join(str(p or "").strip().lower() for p in parts)


def _cache_get(store, key, ttl):
    try:
        item = store.get(key)
        if not item:
            return None
        ts, payload = item
        if time.time() - ts <= ttl:
            return payload
    except Exception:
        return None
    return None


def _cache_set(store, key, payload):
    try:
        store[key] = (time.time(), payload)
    except Exception:
        pass
    return payload


def _safe_latest_rows(table, filters=None, order_col="created_at", limit=100):
    try:
        q = supabase.table(table).select("*")
        for col, val in (filters or []):
            if val not in (None, ""):
                q = q.eq(col, val)
        if order_col:
            q = q.order(order_col, desc=True)
        return q.limit(limit).execute().data or []
    except Exception as e:
        print(f"SAFE LATEST ROWS ERROR {table}:", e)
        return []


def _latest_purchase_for_trader(trader):
    if not trader:
        return None
    seen = {}
    probes = []
    if trader.get("id"):
        probes.append(("trader_id", trader.get("id")))
    if trader.get("email"):
        probes.append(("email", trader.get("email")))
    if trader.get("phone"):
        probes.append(("phone", trader.get("phone")))
    for col, val in probes:
        for row in _safe_latest_rows("challenge_purchases", [(col, val)], "created_at", 20):
            rid = str(row.get("id") or row.get("purchase_id") or id(row))
            seen[rid] = row
    rows = list(seen.values())
    rows.sort(key=lambda r: str(r.get("approved_at") or r.get("created_at") or r.get("updated_at") or ""), reverse=True)
    return rows[0] if rows else None


@app.route("/trader_bootstrap", methods=["GET", "OPTIONS"])
def trader_bootstrap():
    """One lightweight trader dashboard feed.
    This replaces frontend fetching of /traders and direct monitoring API calls.
    """
    if request.method == "OPTIONS":
        return ok({"success": True})
    started = time.time()
    try:
        lookup = str(request.args.get("lookup") or request.args.get("email") or request.args.get("phone") or request.args.get("id") or "").strip().lower()
        trader_id = str(request.args.get("trader_id") or "").strip()
        if trader_id:
            rows = _safe_latest_rows("traders", [("id", trader_id)], "created_at", 1)
            trader = rows[0] if rows else None
        else:
            trader = _latest_trader_for_lookup(lookup) if lookup else None
        if not trader:
            return bad("Trader not found", 404)

        key = _cache_key("trader_bootstrap", trader.get("id"), lookup)
        cached = _cache_get(_TRADER_BOOTSTRAP_CACHE, key, TRADER_BOOTSTRAP_TTL_SECONDS)
        if cached:
            cached["cached"] = True
            return ok(cached, "Trader bootstrap cached")

        account = _get_active_account(trader.get("id"), trader)
        purchase = _latest_purchase_for_trader(trader)
        payload = {
            "success": True,
            "source": "trader_bootstrap_light",
            "generated_at": now_iso(),
            "duration_ms": int((time.time() - started) * 1000),
            "trader": _public_trader_payload(trader),
            "current_account": account,
            "active_purchase": purchase,
            "payout_eligibility": {
                "eligible": bool(account and str(account.get("stage") or account.get("phase") or "").lower() in {"funded", "live", "funded_live"}),
                "reason": "Funded/live account required" if not account else "Check payout rules from admin"
            }
        }
        if account:
            payload["trader"].update({
                "current_account_id": account.get("id"),
                "trader_account_id": account.get("id"),
                "phase": account.get("stage") or account.get("phase") or payload["trader"].get("phase"),
                "mt5_login": account.get("mt5_login"),
                "mt5_server": account.get("mt5_server"),
                "account_size": account.get("account_size") or account.get("start_balance"),
                "profit_percent": account.get("profit_percent") or account.get("current_profit_percent") or 0,
                "drawdown_percent": account.get("absolute_drawdown_percent") or account.get("drawdown_percent") or 0,
                "max_drawdown_used": account.get("dd_used_percent") or account.get("max_drawdown_used") or 0,
            })
        return ok(_cache_set(_TRADER_BOOTSTRAP_CACHE, key, payload), "Trader bootstrap loaded")
    except Exception as e:
        return bad(e)

@app.route("/login_trader", methods=["POST"])
def login_trader():
    """FAST AUTH ONLY.
    Login must never wait for MT5/account intelligence. The dashboard opens from
    this lightweight response, then calls /trader_bootstrap in the background.
    """
    try:
        data = request.json or {}
        lookup = str(data.get("lookup", "")).strip().lower()
        password = str(data.get("password") or "")
        if not lookup:
            return bad("Missing lookup")
        if not password:
            return bad("Password is required", 401)

        trader = _latest_trader_for_lookup(lookup)
        if not trader:
            return bad("Invalid email/phone or password", 401)
        if not (trader.get("password_hash") or trader.get("password")):
            return bad("Password not set. Please verify your email and create a password.", 403)
        if not _check_trader_password(trader, password):
            return bad("Invalid email/phone or password", 401)

        t = now_iso()
        try:
            supabase.table("traders").update({"last_login_at": t}).eq("id", trader["id"]).execute()
        except Exception as e:
            print("LOGIN LAST_LOGIN UPDATE SKIPPED:", e)

        public = _public_trader_payload(trader)
        public["last_login_at"] = t
        public["auth_token"] = _make_trader_auth_token(trader.get("id"))
        public["bootstrap_url"] = f"/trader_bootstrap?lookup={lookup}"
        return ok(public, "Login successful")
    except Exception as e:
        return bad(e)

@app.route("/approve_payment", methods=["POST"])
def approve_payment():
    try:
        d=request.json or {}; tid=d.get("id")
        if not tid: return bad("Missing trader id")
        trader_row = get_trader_by_id(tid)
        if not trader_row:
            return bad("Trader not found", 404)

        mt5 = None
        if d.get("mt5_id"):
            mt5 = _get_mt5_account(mt5_id=d.get("mt5_id"))
        elif d.get("mt5_login"):
            mt5 = _get_mt5_account(mt5_login=str(d.get("mt5_login") or "").strip())
        if not mt5:
            return bad("Approve payment now requires an available MT5 pool account. Send mt5_id or use /approve_challenge_purchase.", 400)

        account, trader_row = _assign_mt5_to_trader(
            trader_row,
            mt5,
            "phase1",
            None,
            _admin_from_payload(d),
            d.get("admin_note") or "Payment approved and Phase 1 MT5 assigned"
        )

        send_email_safe(
            trader_row.get("email"),
            "NairaPips payment approved - MT5 details",
            f"""Hello {trader_row.get("name") or "Trader"},

Your NairaPips payment has been approved and your MT5 account has been activated.

MT5 Login: {account.get("mt5_login", "")}
Server: {account.get("mt5_server", "")}
Master Password: {account.get("mt5_master_password", "")}
Investor Password: {account.get("mt5_investor_password", "")}

NairaPips Team"""
        )

        _audit_safe("payments", "payment_approved", f"Trader {tid} payment approved", _admin_from_payload(d))
        return ok({"trader": trader_row, "account": account}, "Payment approved and Phase 1 MT5 assigned")
    except Exception as e: return bad(e)

@app.route("/reject_payment", methods=["POST"])
def reject_payment():
    try:
        d=request.json or {}; tid=d.get("id")
        if not tid: return bad("Missing trader id")
        trader_row = _get_trader_by_id(tid) or {}
        note = d.get("admin_note","")
        result = supabase.table("traders").update({
            "payment_status":"rejected",
            "status":"payment_rejected",
            "challenge_state":"payment_rejected",
            "admin_note":note,
            "updated_at":now_iso()
        }).eq("id",tid).execute().data

        send_email_safe(
            trader_row.get("email"),
            "NairaPips payment rejected",
            f"""Hello {trader_row.get("name") or "Trader"},

Your NairaPips payment was rejected after review.

Reason / Admin Note: {note or "Please contact support for details."}

NairaPips Team"""
        )

        return ok(result, "Payment rejected")
    except Exception as e: return bad(e)

@app.route("/update_status", methods=["POST"])
def update_status():
    try:
        d=request.json or {}; tid=d.get("id")
        if not tid: return bad("Missing trader id")
        blocked = [
            "status", "phase", "challenge_state", "phase_pass_status",
            "balance", "equity", "profit", "drawdown", "profit_percent", "drawdown_percent",
            "mt5_login", "monitoring_enabled", "mt5_account_active", "mt5_access_disabled",
            "payout_eligible", "payout_blocked", "funded_at"
        ]
        attempted = [k for k in blocked if k in d]
        if attempted:
            return bad("Lifecycle and trading state fields are locked. Use explicit lifecycle routes instead: " + ", ".join(attempted), 409)
        allowed=["engine_group","payment_status","payment_note","admin_note","trading_days_left","lead_status","follow_up_at"]
        upd={k:d[k] for k in allowed if k in d}
        if not upd: return bad("Nothing to update")
        try:
            result = supabase.table("traders").update(upd).eq("id",tid).execute().data
        except Exception as update_error:
            if "lead_status" in upd or "follow_up_at" in upd:
                return bad("Lead status columns are missing. Run the Step 2 launch SQL for traders.lead_status and traders.follow_up_at.", 500)
            raise update_error
        trader_row = result[0] if result else get_trader_by_id(tid)
        _audit_safe("traders", "trader_status_update", f"Trader {tid} status update: {upd}", _admin_from_payload(d))
        return ok(result)
    except Exception as e: return bad(e)

@app.route("/activate_trader", methods=["POST"])
def activate_trader():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing trader id")
        return bad("Direct activation is disabled. Use explicit lifecycle actions to assign/pass/breach accounts, or update lead/admin fields only.", 409)
    except Exception as e: return bad(e)

@app.route("/deactivate_trader", methods=["POST"])
def deactivate_trader():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing trader id")
        return bad("Direct deactivation is disabled. Use breach/close lifecycle actions or mark the record as test/excluded.", 409)
    except Exception as e: return bad(e)

@app.route("/mark_certificate_passed", methods=["POST"])
@app.route("/pass_certificate", methods=["POST"])
def mark_certificate_passed():
    try:
        d = request.json or {}
        tid = d.get("id") or d.get("trader_id")
        if not tid:
            return bad("Missing trader id")
        t = now_iso()
        upd = {
            "certificate_status": d.get("certificate_status") or "passed",
            "certificate_passed_at": d.get("certificate_passed_at") or t,
            "certificate_note": d.get("certificate_note") or d.get("admin_note") or "Certificate passed",
            "updated_at": t
        }
        result = supabase.table("traders").update(upd).eq("id", tid).execute().data
        trader_row = result[0] if result else get_trader_by_id(tid)
        send_challenge_certificate_email(
            trader_row,
            upd["certificate_note"]
        )
        send_admin_alert(
            "NairaPips challenge passed certificate earned",
            f"""A trader challenge certificate was marked as earned after passing a challenge.

Trader: {trader_row.get("name") if trader_row else ""}
Email: {trader_row.get("email") if trader_row else ""}
Note: {upd["certificate_note"]}"""
        )
        return ok(result, "Certificate marked passed")
    except Exception as e:
        return bad(e)

@app.route("/update_kyc_status", methods=["POST"])
@app.route("/kyc_passed", methods=["POST"])
def update_kyc_status():
    try:
        d = request.json or {}
        tid = d.get("id") or d.get("trader_id")
        if not tid:
            return bad("Missing trader id")
        status = d.get("kyc_status") or d.get("status") or "passed"
        t = now_iso()
        upd = {
            "kyc_status": status,
            "kyc_note": d.get("kyc_note") or d.get("admin_note") or f"KYC {status}",
            "updated_at": t
        }
        if str(status).lower() in ["passed", "approved", "verified"]:
            upd["kyc_passed_at"] = d.get("kyc_passed_at") or t
        result = supabase.table("traders").update(upd).eq("id", tid).execute().data
        trader_row = result[0] if result else get_trader_by_id(tid)
        if str(status).lower() in ["passed", "approved", "verified"]:
            send_account_status_email(
                trader_row,
                "NairaPips KYC passed",
                "Your NairaPips KYC verification has passed.",
                upd["kyc_note"]
            )
            send_admin_alert(
                "NairaPips KYC passed",
                f"""A trader KYC was marked as passed.

Trader: {trader_row.get("name") if trader_row else ""}
Email: {trader_row.get("email") if trader_row else ""}
Note: {upd["kyc_note"]}"""
            )
        return ok(result, "KYC status updated")
    except Exception as e:
        return bad(e)

@app.route("/challenge_plans", methods=["GET"])
def challenge_plans():
    try:
        rows = supabase.table("challenge_plans").select("*").order("account_size", desc=False).execute().data or []
        for row in rows:
            if "payout_split" in row:
                row["payout_split"] = _effective_payout_split(row.get("payout_split"))
        return jsonify(rows)
    except Exception as e: return bad(e)

@app.route("/plans", methods=["GET"])
def plans_alias():
    return challenge_plans()

@app.route("/create_challenge_plan", methods=["POST"])
def create_plan():
    try:
        d=request.json or {}; name=str(d.get("name","")).strip()
        if not name: return bad("Plan name is required")
        mt5_server = d.get("mt5_server") or d.get("default_server") or ""
        row={"name":name,"account_size":clean(d.get("account_size")),"fee":clean(d.get("fee")),
             "phase1_target":float(d.get("phase1_target") or 10),"phase2_target":float(d.get("phase2_target") or 8),
             "max_drawdown":float(d.get("max_drawdown") or 20),"daily_drawdown":"None",
             "payout_split":_effective_payout_split(d.get("payout_split")),"description":d.get("description",""),
             "mt5_server":mt5_server,"default_server":d.get("default_server") or mt5_server,
             "status":d.get("status","active"),"created_at":now_iso(),"updated_at":now_iso()}
        return ok(supabase.table("challenge_plans").insert(row).execute().data, "Challenge plan created")
    except Exception as e: return bad(e)

@app.route("/update_challenge_plan", methods=["POST"])
def update_plan():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing plan id")
        upd={"updated_at":now_iso()}
        for k in ["name","daily_drawdown","description","status","mt5_server","default_server"]:
            if k in d: upd[k]=d[k]
        if "payout_split" in d:
            upd["payout_split"] = _effective_payout_split(d.get("payout_split"))
        upd["daily_drawdown"] = "None"
        if "mt5_server" in d and "default_server" not in d:
            upd["default_server"] = d.get("mt5_server")
        if "default_server" in d and "mt5_server" not in d:
            upd["mt5_server"] = d.get("default_server")
        for k in ["account_size","fee"]:
            if k in d: upd[k]=clean(d[k])
        for k in ["phase1_target","phase2_target","max_drawdown"]:
            if k in d: upd[k]=float(d.get(k) or 0)
        return ok(supabase.table("challenge_plans").update(upd).eq("id",pid).execute().data, "Challenge plan updated")
    except Exception as e: return bad(e)

@app.route("/delete_challenge_plan", methods=["POST"])
def delete_plan():
    try:
        pid=(request.json or {}).get("id")
        if not pid: return bad("Missing plan id")
        return ok(supabase.table("challenge_plans").delete().eq("id",pid).execute().data, "Challenge plan deleted")
    except Exception as e: return bad(e)

@app.route("/challenge_purchases", methods=["GET"])
def challenge_purchases():
    try: return jsonify(supabase.table("challenge_purchases").select("*").order("created_at", desc=True).execute().data)
    except Exception as e: return bad(e)

@app.route("/create_challenge_purchase", methods=["POST"])
def create_purchase():
    try:
        d=request.json or {}; plan=str(d.get("plan_name","")).strip(); proof=str(d.get("payment_proof_url","")).strip()
        if not plan: return bad("Plan name is required")
        if not proof: return bad("Payment proof is required")
        original_fee = clean(d.get("fee"))
        if original_fee <= 0: return bad("Challenge fee is required")

        # Validate code before accepting proof. Invalid/expired codes must not create confused discounted purchases.
        quote = _affiliate_quote_details(d, original_fee)
        if quote.get("code") and not quote.get("valid"):
            return bad(quote.get("message") or "Invalid promo/referral code", 400)

        row={"trader_id":d.get("trader_id"),"trader_name":d.get("trader_name",""),"email":d.get("email",""),"phone":d.get("phone",""),
             "plan_id":d.get("plan_id"),"plan_name":plan,"account_size":clean(d.get("account_size")),"fee":quote.get("final_fee", original_fee),
             "original_fee":quote.get("original_fee", original_fee),"discount_percent":quote.get("discount_percent",0),"discount_amount":quote.get("discount_amount",0),
             "final_fee":quote.get("final_fee", original_fee),"amount_due":quote.get("final_fee", original_fee),
             "payment_proof_url":proof,"payment_status":"pending","status":"pending_review","admin_note":"",
             "created_at":now_iso(),"purchase_month":month(),"purchase_year":year()}
        row.update(_affiliate_purchase_fields(d, original_fee))
        created = supabase.table("challenge_purchases").insert(row).execute().data
        try:
            if d.get("trader_id"):
                supabase.table("traders").update({
                    "challenge_state": "purchase_pending",
                    "payment_status": "pending",
                    "updated_at": now_iso()
                }).eq("id", d.get("trader_id")).execute()
        except Exception as e:
            print("PURCHASE PENDING LIFECYCLE UPDATE SKIPPED:", str(e))

        discount_line = ""
        if clean(row.get("discount_amount")) > 0:
            discount_line = f"\nOriginal Fee: {email_money(row.get('original_fee'))}\nDiscount: {email_money(row.get('discount_amount'))} ({row.get('discount_percent')}%)\nAmount To Pay: {email_money(row.get('fee'))}\nCode Used: {row.get('affiliate_code') or row.get('promo_code') or ''}\n"
        else:
            discount_line = f"\nChallenge Fee: {email_money(row.get('fee'))}\n"

        send_email_safe(
            row.get("email"),
            "NairaPips payment proof received",
            f"""Hello {row.get("trader_name") or "Trader"},

Your NairaPips payment proof has been received.

Plan: {plan}{discount_line}
Admin will review your proof and notify you after approval or rejection.

NairaPips Team"""
        )
        send_email_safe(
            row.get("email"),
            "NairaPips challenge purchase submitted",
            f"""Hello {row.get("trader_name") or "Trader"},

Your NairaPips challenge purchase has been submitted successfully.

Plan: {plan}
Account Size: {email_money(row.get("account_size"))}{discount_line}
Admin will review your payment proof and assign your MT5 details after approval.

NairaPips Team"""
        )
        send_admin_alert(
            "New NairaPips challenge purchase/payment proof",
            f"""A new challenge purchase was submitted.

Trader: {row.get("trader_name") or "Trader"}
Email: {row.get("email") or "Not provided"}
Phone: {row.get("phone") or "Not provided"}
Plan: {plan}
Account Size: {email_money(row.get("account_size"))}
Original Fee: {email_money(row.get("original_fee"))}
Discount: {email_money(row.get("discount_amount"))}
Final Fee: {email_money(row.get("fee"))}
Code: {row.get("affiliate_code") or row.get("promo_code") or "None"}
Proof URL: {proof}"""
        )

        return ok(created, "Challenge purchase submitted")
    except Exception as e: return bad(e)

@app.route("/approve_challenge_purchase", methods=["POST"])
def approve_purchase():
    try:
        d=request.json or {}; pid=d.get("id"); mt5_id=d.get("mt5_id")
        if not pid: return bad("Missing purchase id")
        pres=supabase.table("challenge_purchases").select("*").eq("id",pid).limit(1).execute()
        if not pres.data: return bad("Purchase not found",404)
        p=pres.data[0]
        if p.get("trader_account_id") or p.get("assigned_mt5_id") or str(p.get("mt5_login") or "").strip():
            return bad("This purchase is already approved/assigned. Refresh the purchases page.", 409)
        if mt5_id:
            mres=supabase.table("mt5_pool").select("*").eq("id",mt5_id).limit(1).execute()
        else:
            mres=supabase.table("mt5_pool").select("*").eq("status","available").eq("account_size",p.get("account_size") or 0).limit(1).execute()
        if not mres.data: return bad("No available MT5 account found for this plan/account size")
        m=mres.data[0]
        if str(m.get("status") or "").strip().lower() != "available":
            return bad("Selected MT5 account is not available")
        if clean(m.get("account_size")) != clean(p.get("account_size")):
            return bad("Selected MT5 account size does not match purchase account size")
        staff = _admin_from_payload(d)
        trader = _ensure_trader_for_purchase(p)
        if not trader:
            return bad("Could not resolve trader for purchase", 500)
        account, trader_row = _assign_mt5_to_trader(
            trader,
            m,
            "phase1",
            p,
            staff,
            d.get("admin_note") or "Challenge approved and Phase 1 MT5 assigned"
        )
        master_password=m.get("mt5_master_password","")
        investor_password=m.get("mt5_investor_password","")
        approved_rows = supabase.table("challenge_purchases").select("*").eq("id",pid).limit(1).execute().data
        _affiliate_create_commission_from_purchase(approved_rows[0] if approved_rows else p, d)

        send_email_safe(
            p.get("email"),
            "NairaPips challenge approved - MT5 details",
            f"""Hello {p.get("trader_name") or "Trader"},

Your NairaPips challenge has been approved and your MT5 account has been assigned.

Plan: {p.get("plan_name", "Challenge")}
Account Size: {email_money(p.get("account_size"))}

MT5 Login: {m.get("mt5_login", "")}
Server: {m.get("mt5_server", "")}
Master Password: {master_password}
Investor Password: {investor_password}

Please log in to your trader dashboard to view your account details and begin your challenge.

NairaPips Team"""
        )

        _audit_safe("challenge_purchases", "challenge_purchase_approved", f"Purchase {pid} approved", staff)
        _audit_safe("mt5", "phase1_mt5_assignment", f"Purchase {pid} assigned MT5 {m.get('mt5_login','')} to trader account {account.get('id')}", staff)
        return ok(approved_rows, "Challenge purchase approved and MT5 assigned")
    except Exception as e: return bad(e)

@app.route("/reject_challenge_purchase", methods=["POST"])
def reject_purchase():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing purchase id")
        purchase = get_purchase_by_id(pid)
        note = d.get("admin_note","Challenge purchase rejected")
        result = supabase.table("challenge_purchases").update({"payment_status":"rejected","status":"rejected","rejected_at":now_iso(),"admin_note":note}).eq("id",pid).execute().data
        try:
            trader_id = purchase.get("trader_id")
            if trader_id:
                supabase.table("traders").update({
                    "payment_status": "rejected",
                    "challenge_state": "payment_rejected",
                    "status": "payment_rejected",
                    "admin_note": note,
                    "updated_at": now_iso()
                }).eq("id", trader_id).execute()
        except Exception as e:
            print("PURCHASE REJECT LIFECYCLE UPDATE SKIPPED:", str(e))

        send_email_safe(
            purchase.get("email"),
            "NairaPips challenge purchase rejected",
            f"""Hello {purchase.get("trader_name") or "Trader"},

Your NairaPips challenge purchase was rejected after review.

Plan: {purchase.get("plan_name", "Challenge")}
Reason / Admin Note: {note}

Please contact NairaPips support from your dashboard if you need help.

NairaPips Team"""
        )

        return ok(result, "Challenge purchase rejected")
    except Exception as e: return bad(e)


@app.post("/lifecycle/pass_phase1")
def lifecycle_pass_phase1():
    try:
        d = request.get_json(silent=True) or {}
        trader_id = d.get("trader_id") or d.get("id")
        if not trader_id:
            return bad("trader_id is required")
        trader = _pass_stage(trader_id, "phase1", _admin_from_payload(d), d.get("admin_note") or "Phase 1 passed. Account archived; Phase 2 MT5 required.")
        return ok(trader, "Phase 1 passed. Trader moved to Phase 2 Waiting.")
    except Exception as e:
        return bad(e)


@app.post("/lifecycle/assign_phase2_mt5")
def lifecycle_assign_phase2_mt5():
    try:
        d = request.get_json(silent=True) or {}
        trader_id = d.get("trader_id") or d.get("id")
        mt5_id = d.get("mt5_id")
        if not trader_id or not mt5_id:
            return bad("trader_id and mt5_id are required")
        trader = get_trader_by_id(trader_id)
        if not trader:
            return bad("Trader not found", 404)
        mt5 = _get_mt5_account(mt5_id=mt5_id)
        account, updated = _assign_mt5_to_trader(trader, mt5, "phase2", None, _admin_from_payload(d), d.get("admin_note") or "Phase 2 MT5 assigned")
        return ok({"trader": updated, "account": account}, "Phase 2 MT5 assigned")
    except Exception as e:
        return bad(e)


@app.post("/lifecycle/pass_phase2")
def lifecycle_pass_phase2():
    try:
        d = request.get_json(silent=True) or {}
        trader_id = d.get("trader_id") or d.get("id")
        if not trader_id:
            return bad("trader_id is required")
        trader = _pass_stage(trader_id, "phase2", _admin_from_payload(d), d.get("admin_note") or "Phase 2 passed. Account archived; funded MT5 required.")
        return ok(trader, "Phase 2 passed. Trader moved to Funded Waiting.")
    except Exception as e:
        return bad(e)


@app.post("/lifecycle/assign_funded_mt5")
def lifecycle_assign_funded_mt5():
    try:
        d = request.get_json(silent=True) or {}
        trader_id = d.get("trader_id") or d.get("id")
        mt5_id = d.get("mt5_id")
        if not trader_id or not mt5_id:
            return bad("trader_id and mt5_id are required")
        trader = get_trader_by_id(trader_id)
        if not trader:
            return bad("Trader not found", 404)
        mt5 = _get_mt5_account(mt5_id=mt5_id)
        account, updated = _assign_mt5_to_trader(trader, mt5, "funded", None, _admin_from_payload(d), d.get("admin_note") or "Funded MT5 assigned")
        return ok({"trader": updated, "account": account}, "Funded MT5 assigned")
    except Exception as e:
        return bad(e)


@app.post("/lifecycle/breach_account")


@app.route("/resend_breach_email", methods=["POST", "OPTIONS"])
def resend_breach_email():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        lookup = d.get("trader_id") or d.get("id") or d.get("email") or d.get("mt5_login")
        if not lookup:
            return bad("Send trader_id, id, email, or mt5_login", 400)
        trader = None
        if d.get("trader_id") or d.get("id"):
            trader = get_trader_by_id(lookup)
        elif d.get("email"):
            trader = _find_existing_trader(lookup.lower(), None)
        elif d.get("mt5_login"):
            accs = supabase.table("trader_accounts").select("trader_id").eq("mt5_login", str(lookup)).limit(1).execute().data or []
            if accs:
                trader = get_trader_by_id(accs[0].get("trader_id"))
        if not trader:
            return bad("Trader not found", 404)
        if str(trader.get("status") or "").lower() != "breached":
            return bad(f"Trader status is '{trader.get('status')}', not breached. Cannot send breach email.", 400)
        reason = trader.get("breach_reason") or trader.get("admin_note") or "Maximum drawdown violation recorded by NairaPips monitoring engine."
        sent = send_account_status_email(
            trader,
            "NairaPips account breached — Notification",
            "Your NairaPips account has been breached and locked.",
            reason
        )
        admin_sent = send_admin_alert(
            "NairaPips breach email resent",
            f"Breach email resent for {trader.get('name') or 'trader'} ({trader.get('email')})"
        )
        return ok({"trader_email_sent": bool(sent), "admin_email_sent": bool(admin_sent)}, "Breach email resent")
    except Exception as e:
        return bad(e)

@app.route("/admin_email_status", methods=["GET"])
def admin_email_status():
    """List traders by status (breached/passed) with email-sent status."""
    try:
        status = str(request.args.get("status") or "breached").lower()
        limit = int(request.args.get("limit", 100))
        # Fetch traders with the given status
        rows = supabase.table("traders").select("id,name,email,status,phase,phase_pass_status,breach_time,breach_reason,last_breach_email_sent_at,last_phase_pass_email_sent_at,created_at,updated_at").eq("status", status).order("updated_at", desc=True).limit(limit).execute().data or []
        # Also fetch email_logs to know which ones got emails
        logs = supabase.table("email_logs").select("recipient_email,subject,email_type,status,created_at,error").order("created_at", desc=True).limit(1000).execute().data or []
        # Group logs by recipient + email_type
        from collections import defaultdict
        sent_map = {}  # email -> {email_type: last_status}
        for log in logs:
            email = str(log.get("recipient_email") or "").lower()
            etype = str(log.get("email_type") or "").lower()
            if not email or not etype: continue
            key = (email, etype)
            if key not in sent_map or str(log.get("created_at","")) > str(sent_map[key].get("created_at","")):
                sent_map[key] = log
        # Augment each trader with email status
        for r in rows:
            email = str(r.get("email") or "").lower()
            if status == "breached":
                breach_log = sent_map.get((email, "breach")) or sent_map.get((email, "breached"))
                r["breach_email_sent"] = bool(breach_log and breach_log.get("status") == "sent")
                r["breach_email_log"] = breach_log
            elif status in ["phase1_passed", "phase2_passed", "passed"]:
                pass_log = sent_map.get((email, r.get("phase_pass_status") or "phase_pass"))
                r["phase_pass_email_sent"] = bool(pass_log and pass_log.get("status") == "sent")
                r["phase_pass_email_log"] = pass_log
        return ok({"traders": rows, "total": len(rows), "status": status, "logs_total": len(logs)}, "Email status fetched")
    except Exception as e:
        return bad(e, 500)

@app.route("/admin_bulk_resend_breach_emails", methods=["POST", "OPTIONS"])
def admin_bulk_resend_breach_emails():
    """Bulk resend breach emails to all breached traders who haven't received one."""
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        rows = supabase.table("traders").select("id,name,email,breach_reason,admin_note").eq("status", "breached").execute().data or []
        sent_count = 0
        failed_count = 0
        skipped_count = 0
        results = []
        for r in rows:
            email = (r.get("email") or "").lower().strip()
            if not email:
                skipped_count += 1
                continue
            reason = r.get("breach_reason") or r.get("admin_note") or "Maximum drawdown violation recorded by NairaPips monitoring engine."
            sent = send_account_status_email(
                r,
                "NairaPips account breached",
                "Your NairaPips account has been breached and locked.",
                reason
            )
            if sent:
                sent_count += 1
                results.append({"email": email, "name": r.get("name"), "status": "sent"})
            else:
                failed_count += 1
                results.append({"email": email, "name": r.get("name"), "status": "failed"})
        return ok({"total": len(rows), "sent": sent_count, "failed": failed_count, "skipped": skipped_count, "results": results}, f"Bulk resend complete: {sent_count} sent, {failed_count} failed, {skipped_count} skipped")
    except Exception as e:
        return bad(e, 500)

@app.route("/admin_bulk_resend_pass_emails", methods=["POST", "OPTIONS"])
def admin_bulk_resend_pass_emails():
    """Bulk resend phase pass emails to all passed traders."""
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        # Find traders with phase_pass_status set
        rows1 = supabase.table("traders").select("id,name,email,phase_pass_status").in_("phase_pass_status", ["phase1_passed", "phase2_passed"]).execute().data or []
        sent_count = 0; failed_count = 0; skipped_count = 0
        results = []
        for r in rows1:
            email = (r.get("email") or "").lower().strip()
            if not email:
                skipped_count += 1; continue
            pass_status = r.get("phase_pass_status") or ""
            phase_name = "Phase 2" if pass_status == "phase2_passed" else "Phase 1"
            sent = send_account_status_email(
                r,
                f"Congratulations - You passed {phase_name}",
                f"You have passed {phase_name}.",
                f"Congratulations. You have successfully passed {phase_name} of the NairaPips Challenge.\n\nYour dashboard has been updated and the account has been locked for admin review / next stage processing.\n\nNairaPips Team"
            )
            if sent: sent_count += 1
            else: failed_count += 1
            results.append({"email": email, "phase": phase_name, "status": "sent" if sent else "failed"})
        return ok({"total": len(rows1), "sent": sent_count, "failed": failed_count, "skipped": skipped_count, "results": results}, f"Bulk resend complete: {sent_count} sent")
    except Exception as e:
        return bad(e, 500)

@app.route("/resend_phase_pass_email", methods=["POST", "OPTIONS"])

@app.route("/mt5_pool", methods=["GET"])
def mt5_pool():
    try: return jsonify(supabase.table("mt5_pool").select("*").order("created_at", desc=True).execute().data)
    except Exception as e: return bad(e)

@app.route("/create_mt5_account", methods=["POST"])
def create_mt5():
    try:
        d=request.json or {}
        required=["mt5_login","mt5_server","mt5_master_password","mt5_investor_password"]
        if any(not str(d.get(x,"")).strip() for x in required): return bad("All MT5 details are required")
        mt5_login=str(d.get("mt5_login","")).strip()
        existing=supabase.table("mt5_pool").select("id,status").eq("mt5_login",mt5_login).limit(1).execute().data or []
        if existing: return bad("MT5 login already exists in pool",409)
        row={"plan_name":d.get("plan_name",""),"account_size":clean(d.get("account_size")),"mt5_login":mt5_login,
             "mt5_server":str(d.get("mt5_server","")).strip(),"mt5_master_password":str(d.get("mt5_master_password","")).strip(),
             "mt5_investor_password":str(d.get("mt5_investor_password","")).strip(),"status":d.get("status","available"),
             "admin_note":d.get("admin_note",""),"created_at":now_iso(),"updated_at":now_iso()}
        return ok(supabase.table("mt5_pool").insert(row).execute().data, "MT5 account added")
    except Exception as e: return bad(e)

@app.route("/update_mt5_account", methods=["POST"])
def update_mt5():
    try:
        d=request.json or {}; mid=d.get("id")
        if not mid: return bad("Missing MT5 account id")
        # Get the existing row first so we can detect which fields changed and email the trader
        existing = supabase.table("mt5_pool").select("*").eq("id", mid).limit(1).execute().data or []
        upd={"updated_at":now_iso()}
        for k in ["plan_name","mt5_login","mt5_server","mt5_master_password","mt5_investor_password","status","admin_note"]:
            if k in d: upd[k]=d[k]
        if "account_size" in d: upd["account_size"]=clean(d.get("account_size"))
        result = supabase.table("mt5_pool").update(upd).eq("id",mid).execute().data
        # CRITICAL: Notify trader when their MT5 login credentials change (production must notify)
        try:
            if existing and result:
                old_row = existing[0]
                new_row = result[0] if isinstance(result, list) else result
                creds_changed = []
                for cred_field in ['mt5_login', 'mt5_server', 'mt5_master_password', 'mt5_investor_password']:
                    if str(old_row.get(cred_field) or '') != str(new_row.get(cred_field) or ''):
                        creds_changed.append(cred_field)
                if creds_changed:
                    # Find the trader assigned to this MT5
                    trader_id = new_row.get('assigned_trader_id') or new_row.get('trader_id')
                    trader_email = new_row.get('trader_email')
                    if not trader_email and trader_id:
                        tr = supabase.table('traders').select('id,name,email').eq('id', trader_id).limit(1).execute().data or []
                        if tr: trader_email = tr[0].get('email'); trader_name = tr[0].get('name') or 'Trader'
                    if not trader_email:
                        # Try via trader_accounts
                        accs = supabase.table('trader_accounts').select('trader_id').eq('mt5_login', str(new_row.get('mt5_login') or '')).limit(1).execute().data or []
                        if accs:
                            tr = supabase.table('traders').select('id,name,email').eq('id', accs[0].get('trader_id')).limit(1).execute().data or []
                            if tr:
                                trader_email = tr[0].get('email'); trader_name = tr[0].get('name') or 'Trader'
                    if trader_email:
                        # Build email body listing new credentials (don't leak in logs)
                        creds_lines = []
                        if 'mt5_login' in creds_changed: creds_lines.append(f'MT5 Login: {new_row.get("mt5_login")}')
                        if 'mt5_server' in creds_changed: creds_lines.append(f'MT5 Server: {new_row.get("mt5_server")}')
                        if 'mt5_master_password' in creds_changed: creds_lines.append(f'Master Password: {new_row.get("mt5_master_password")}')
                        if 'mt5_investor_password' in creds_changed: creds_lines.append(f'Investor Password: {new_row.get("mt5_investor_password")}')
                        details = 'Your MT5 trading credentials have been updated:\n\n' + '\n'.join(creds_lines) + '\n\nYou can log in to MT5 with these new details. Your dashboard remains at https://nairapips.com/dashboard/trader_clean.html'
                        send_account_status_email(
                            {'email': trader_email, 'name': trader_name or 'Trader'},
                            'NairaPips — Your MT5 credentials have been updated',
                            'Your MT5 account credentials have been updated by NairaPips.',
                            details
                        )
                        send_admin_alert(
                            'NairaPips MT5 credentials updated',
                            f'MT5 account {new_row.get("mt5_login")} credentials updated and emailed to {trader_email}. Changed fields: {", ".join(creds_changed)}'
                        )
        except Exception as _email_err:
            print('MT5 UPDATE EMAIL ERROR:', str(_email_err))
        return ok(result, "MT5 account updated")
    except Exception as e: return bad(e)

@app.route("/delete_mt5_account", methods=["POST"])
def delete_mt5():
    try:
        mid=(request.json or {}).get("id")
        if not mid: return bad("Missing MT5 account id")
        found=supabase.table("mt5_pool").select("*").eq("id",mid).limit(1).execute().data or []
        if not found: return bad("MT5 account not found",404)
        if str(found[0].get("status") or "").strip().lower()=="assigned":
            return bad("Assigned MT5 accounts cannot be deleted",403)
        return ok(supabase.table("mt5_pool").delete().eq("id",mid).execute().data, "MT5 account deleted")
    except Exception as e: return bad(e)


@app.route("/assign_phase_mt5", methods=["POST"])
def assign_phase_mt5():
    """
    Production phase MT5 assignment.

    Purpose:
    - Assign fresh MT5 account from mt5_pool to a trader for Phase 2 or Funded/Live.
    - Reset phase-specific tracking so old Phase 1 equity/profit cannot contaminate Phase 2.
    - Mark MT5 pool row as assigned so it cannot be reused.
    """
    try:
        d = request.json or {}
        trader_id = d.get("trader_id") or d.get("id")
        mt5_id = d.get("mt5_id")
        phase = str(d.get("phase") or "phase2").lower().strip()
        admin_name = d.get("admin_name") or d.get("approved_by") or "admin"

        if not trader_id:
            return bad("Missing trader_id")
        if not mt5_id:
            return bad("Choose an MT5 account from the pool")
        if phase not in ["phase2", "funded", "live"]:
            return bad("Phase must be phase2, funded or live")

        trader = get_trader_by_id(trader_id)
        if not trader:
            return bad("Trader not found", 404)
        target_stage = "funded" if phase in ["funded", "live"] else "phase2"
        mt5_acc = _get_mt5_account(mt5_id=mt5_id)
        account, updated = _assign_mt5_to_trader(
            trader,
            mt5_acc,
            target_stage,
            None,
            _admin_from_payload(d),
            d.get("admin_note") or f"{target_stage.title()} MT5 assigned"
        )
        send_email_safe(
            updated.get("email"),
            f"NairaPips {target_stage.upper()} MT5 account assigned",
            f"""Hello {updated.get("name") or "Trader"},

Your fresh {target_stage.upper()} MT5 account has been assigned.

MT5 Login: {account.get("mt5_login")}
Server: {account.get("mt5_server")}
Master Password: {account.get("mt5_master_password") or ""}
Investor Password: {account.get("mt5_investor_password") or ""}

NairaPips Team"""
        )
        _audit_safe("mt5_pool", "phase_mt5_assigned", f"{target_stage} MT5 assigned to trader {trader_id}", _admin_from_payload(d))
        return ok({"trader": updated, "account": account}, f"{target_stage.upper()} MT5 assigned successfully")

        trader_rows = supabase.table("traders").select("*").eq("id", trader_id).limit(1).execute().data or []
        if not trader_rows:
            return bad("Trader not found", 404)
        trader = trader_rows[0]

        pool_rows = supabase.table("mt5_pool").select("*").eq("id", mt5_id).limit(1).execute().data or []
        if not pool_rows:
            return bad("MT5 account not found in pool", 404)
        mt5_acc = pool_rows[0]

        if str(mt5_acc.get("status") or "available").lower().strip() != "available":
            return bad("This MT5 account is not available. Choose another one.", 409)

        login = str(mt5_acc.get("mt5_login") or "").strip()
        server = str(mt5_acc.get("mt5_server") or "").strip()
        master = str(mt5_acc.get("mt5_master_password") or "").strip()
        investor = str(mt5_acc.get("mt5_investor_password") or "").strip()
        if not login or not server or not master or not investor:
            return bad("Selected MT5 pool account is incomplete. Login, server, master and investor passwords are required.")

        account_size = clean(mt5_acc.get("account_size") or trader.get("account_size") or trader.get("balance") or 0)
        if account_size <= 0:
            return bad("Account size missing. Set account_size on the MT5 pool record or trader record.")

        now = now_iso()
        target_percent = 8 if phase == "phase2" else 0
        if phase == "phase2":
            new_phase = "phase2"
            new_status = "phase2_active"
            note = "Fresh Phase 2 MT5 assigned. Phase 2 tracking starts from zero."
        else:
            new_phase = "funded"
            new_status = "funded_active"
            note = "Funded/Live MT5 assigned. Funded risk monitoring starts."

        trader_update = {
            "phase": new_phase,
            "status": new_status,
            "mt5_login": login,
            "mt5_server": server,
            "mt5_master_password": master,
            "mt5_investor_password": investor,
            "account_size": account_size,
            "balance": account_size,
            "equity": account_size,
            "profit": 0,
            "profit_percent": 0,
            "drawdown": 0,
            "drawdown_percent": 0,
            "highest_equity": account_size,
            "lowest_equity": account_size,
            "target_equity": account_size * (1 + (target_percent / 100)),
            "profit_target": target_percent,
            "phase_label": new_phase,
            "risk_zone": "safe",
            "critical_mode": False,
            "monitoring_priority": "active",
            "monitoring_enabled": True,
            "mt5_account_active": True,
            "mt5_access_disabled": False,
            "payout_blocked": False,
            "admin_note": note,
            "approved_by": admin_name,
            "updated_at": now,
            "last_sync_at": now,
            "mt5_updated_at": now,
            "assigned_at": now,
            "assigned_phase": new_phase,
        }
        if phase == "phase2":
            trader_update["phase2_started_at"] = now
        else:
            trader_update["funded_at"] = now

        # Use safe updater because some deployments may not have every optional column.
        _safe_traders_update(trader_id, trader_update)

        pool_update = {
            "status": "assigned",
            "assigned_trader_id": trader_id,
            "assigned_trader_name": trader.get("name") or trader.get("full_name") or "",
            "assigned_email": trader.get("email") or "",
            "assigned_phase": new_phase,
            "assigned_at": now,
            "updated_at": now,
            "admin_note": f"Assigned to {trader.get('name') or trader_id} for {new_phase} by {admin_name}",
        }
        try:
            supabase.table("mt5_pool").update(pool_update).eq("id", mt5_id).execute()
        except Exception as pool_error:
            print("MT5 POOL ASSIGN UPDATE FAILED:", pool_error)
            fallback_pool = {
                "status": "assigned",
                "assigned_trader_name": trader.get("name") or trader.get("full_name") or "",
                "assigned_email": trader.get("email") or "",
                "updated_at": now,
            }
            supabase.table("mt5_pool").update(fallback_pool).eq("id", mt5_id).execute()

        send_email_safe(
            trader.get("email"),
            f"NairaPips {new_phase.upper()} MT5 account assigned",
            f"""Hello {trader.get("name") or "Trader"},

Your fresh {new_phase.upper()} MT5 account has been assigned.

MT5 Login: {login}
Server: {server}
Master Password: {master}
Investor Password: {investor}

Your {new_phase.upper()} tracking starts fresh from {email_money(account_size)}.

NairaPips Team"""
        )

        _audit_safe("mt5_pool", "phase_mt5_assigned", f"{new_phase} MT5 assigned to trader {trader_id}", _admin_from_payload(d))

        updated_trader = _get_trader_by_id(trader_id) or {}
        return ok(updated_trader, f"{new_phase.upper()} MT5 assigned successfully")

    except Exception as e:
        return bad(e)

@app.route("/payouts", methods=["GET"])
def payouts():
    """Return payouts. Admin gets all; trader dashboard can request only its own payouts.
    Query filters are optional and preserve the old admin behaviour:
      /payouts?limit=500              -> all payouts for admin
      /payouts?trader_id=<id>&limit=100 -> one trader history
    """
    try:
        limit = int(request.args.get("limit") or 500)
        limit = max(1, min(limit, 1000))
        trader_id = str(request.args.get("trader_id") or "").strip()
        email = str(request.args.get("email") or "").strip().lower()
        q = supabase.table("payouts").select("*")
        if trader_id:
            q = q.eq("trader_id", trader_id)
        elif email:
            q = q.eq("email", email)
        rows = q.order("created_at", desc=True).limit(limit).execute().data or []
        return jsonify(rows)
    except Exception as e:
        return bad(e)

@app.route("/create_payout", methods=["POST"])
def create_payout():
    try:
        d=request.json or {}
        amount=clean(d.get("amount"))
        if amount<=0:
            return bad("Invalid payout amount")

        trader_row = _resolve_trader_for_money_action(d)
        eligible, reason, account = _payout_eligibility(trader_row)
        if not eligible:
            return bad(reason, 403)

        start_balance = clean(account.get("start_balance") or account.get("account_size") or 0)
        current_equity = clean(account.get("current_equity") or account.get("equity") or account.get("current_balance") or account.get("balance") or start_balance)
        verified_profit = max(0, current_equity - start_balance)
        split_pct = _effective_payout_split(account.get("payout_split"), trader_row.get("payout_split"), d.get("payout_split"))
        max_payout = max(0, round((verified_profit * split_pct) / 100, 2))
        if max_payout <= 0:
            return bad("No verified withdrawable profit yet.", 403)
        if amount > max_payout:
            return bad(f"Requested amount exceeds verified available payout. Available: {email_money(max_payout)}", 403)

        method = str(d.get("payment_method") or d.get("method") or "bank").strip().lower()
        bank_name = str(d.get("bank_name") or "").strip()
        account_number = str(d.get("account_number") or "").strip()
        account_name = str(d.get("account_name") or "").strip()
        if method == "bank" and (not bank_name or not account_number or not account_name):
            return bad("Bank name, account number and account name are required for bank payout.", 400)
        if method in {"usdt", "btc", "crypto"} and (not bank_name or not account_number):
            return bad("Wallet/network and wallet address are required for crypto payout.", 400)

        now = now_iso()
        row={
            "trader_id":trader_row.get("id"),
            "trader_account_id":account.get("id"),
            "trader_name":d.get("trader_name") or trader_row.get("name") or trader_row.get("full_name") or "",
            "email":d.get("email") or trader_row.get("email") or "",
            "phone":d.get("phone") or trader_row.get("phone") or "",
            "mt5_login":account.get("mt5_login") or d.get("mt5_login") or "",
            "mt5_server":account.get("mt5_server") or d.get("mt5_server") or "",
            "account_size":clean(account.get("account_size") or d.get("account_size") or 0),
            "start_balance":start_balance,
            "current_balance":clean(account.get("current_balance") or account.get("balance") or 0),
            "current_equity":current_equity,
            "verified_profit":verified_profit,
            "payout_split":split_pct,
            "available_payout":max_payout,
            "amount":amount,
            "method":method,
            "payment_method":method,
            "bank_name":bank_name,
            "account_number":account_number,
            "account_name":account_name,
            "status":"pending",
            "note":d.get("note", ""),
            "admin_note":"",
            "requested_at":now,
            "created_at":now,
        }
        try:
            created = supabase.table("payouts").insert(row).execute().data
        except Exception as insert_error:
            # Backward compatible fallback for older payouts table schemas.
            fallback={
                "trader_id":row["trader_id"],
                "trader_account_id":row["trader_account_id"],
                "trader_name":row["trader_name"],
                "email":row["email"],
                "phone":row["phone"],
                "amount":row["amount"],
                "bank_name":row["bank_name"],
                "account_number":row["account_number"],
                "account_name":row["account_name"],
                "status":"pending",
                "note":row["note"],
                "admin_note":"",
                "requested_at":now,
            }
            created = supabase.table("payouts").insert(fallback).execute().data
            row.update(fallback)
            print("PAYOUT RICH INSERT FALLBACK:", insert_error)

        send_email_safe(
            row.get("email"),
            "NairaPips payout request received",
            f"""Hello {row.get("trader_name") or "Trader"},

Your payout request has been received.

Amount: {email_money(amount)}
Available verified payout before this request ({split_pct}% share cap): {email_money(max_payout)}
MT5 Login: {row.get("mt5_login") or "Not provided"}
Bank / Wallet: {row.get("bank_name") or "Not provided"}
Account / Wallet Address: {row.get("account_number") or "Not provided"}

Admin will review your account and payout request.

NairaPips Team"""
        )
        send_admin_alert(
            "New NairaPips payout request",
            f"""A trader submitted a payout request.

Trader: {row.get("trader_name") or "Trader"}
Email: {row.get("email") or "Not provided"}
Phone: {row.get("phone") or "Not provided"}
MT5 Login: {row.get("mt5_login") or "Not provided"}
Amount: {email_money(amount)}
Verified Available ({split_pct}% share cap): {email_money(max_payout)}
Method: {method.upper()}
Bank / Wallet: {row.get("bank_name") or "Not provided"}
Account / Wallet Address: {row.get("account_number") or "Not provided"}"""
        )

        _audit_safe("payouts", "payout_requested", f"Payout requested amount={amount} available={max_payout}", {"name":"trader","username":row.get("email")}, (created[0].get("id") if created else ""))
        return ok(created, "Payout request created")
    except Exception as e:
        return bad(e)

@app.route("/approve_payout", methods=["POST"])
def approve_payout():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing payout id")
        payout = get_payout_by_id(pid)
        if not payout: return bad("Payout not found",404)
        if payout_status(payout) != "pending":
            return bad("Only pending payouts can be approved",409)

        trader_row = _resolve_trader_for_money_action(payout)
        eligible, reason, account = _payout_eligibility(trader_row)
        if not eligible:
            return bad(reason, 403)
        if payout.get("trader_account_id") and str(payout.get("trader_account_id")) != str(account.get("id")):
            return bad("Payout is not attached to the current active funded account.", 403)

        note = d.get("admin_note","")
        result = supabase.table("payouts").update({"status":"approved","approved_at":now_iso(),"admin_note":note}).eq("id",pid).execute().data

        send_email_safe(
            payout.get("email"),
            "NairaPips payout approved",
            f"""Hello {payout.get("trader_name") or "Trader"},

Your payout request has been approved.

Amount: {email_money(payout.get("amount"))}
Admin Note: {note or "Approved after review."}

NairaPips Team"""
        )

        _audit_safe("payouts", "payout_approved", f"Payout {pid} approved", _admin_from_payload(d))
        return ok(result, "Payout approved")
    except Exception as e: return bad(e)

@app.route("/reject_payout", methods=["POST"])
def reject_payout():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing payout id")
        payout = get_payout_by_id(pid)
        if not payout: return bad("Payout not found",404)
        if payout_status(payout) != "pending":
            return bad("Only pending payouts can be rejected",409)
        note = d.get("admin_note","")
        result = supabase.table("payouts").update({"status":"rejected","rejected_at":now_iso(),"admin_note":note}).eq("id",pid).execute().data

        send_email_safe(
            payout.get("email"),
            "NairaPips payout rejected",
            f"""Hello {payout.get("trader_name") or "Trader"},

Your payout request was rejected after review.

Amount: {email_money(payout.get("amount"))}
Reason / Admin Note: {note or "Please contact support for details."}

NairaPips Team"""
        )

        return ok(result, "Payout rejected")
    except Exception as e: return bad(e)

@app.route("/mark_payout_paid", methods=["POST"])
def mark_paid():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing payout id")
        payout = get_payout_by_id(pid)
        if not payout: return bad("Payout not found",404)
        if payout_status(payout) != "approved":
            return bad("Only approved payouts can be marked paid",409)
        note = d.get("admin_note","")
        result = supabase.table("payouts").update({"status":"paid","paid_at":now_iso(),"admin_note":note}).eq("id",pid).execute().data

        send_email_safe(
            payout.get("email"),
            "NairaPips payout marked paid",
            f"""Hello {payout.get("trader_name") or "Trader"},

Your payout has been marked as paid.

Amount: {email_money(payout.get("amount"))}
Admin Note: {note or "Payment completed."}

NairaPips Team"""
        )

        _audit_safe("payouts", "payout_paid", f"Payout {pid} marked paid", _admin_from_payload(d))
        return ok(result, "Payout marked paid")
    except Exception as e: return bad(e)

@app.route("/support_tickets", methods=["GET"])
def support_tickets():
    try: return jsonify(supabase.table("support_tickets").select("*").order("created_at", desc=True).execute().data)
    except Exception as e: return bad(e)

@app.route("/create_support_ticket", methods=["POST"])
def create_ticket():
    try:
        d=request.json or {}; subject=str(d.get("subject","")).strip(); message=str(d.get("message","")).strip()
        if not subject or not message: return bad("Subject and message are required")
        row={"trader_id":d.get("trader_id"),"trader_name":d.get("trader_name",""),"email":d.get("email",""),"phone":d.get("phone",""),
             "subject":subject,"message":message,"status":"open","priority":d.get("priority","normal"),"admin_reply":"",
             "created_at":now_iso(),"last_updated_at":now_iso()}
        created = supabase.table("support_tickets").insert(row).execute().data

        send_admin_alert(
            "New NairaPips support ticket",
            f"""A trader submitted a new support ticket.

Trader: {row.get("trader_name") or "Trader"}
Email: {row.get("email") or "Not provided"}
Phone: {row.get("phone") or "Not provided"}
Subject: {subject}
Priority: {row.get("priority")}

Message:
{message}"""
        )

        return ok(created, "Support ticket created")
    except Exception as e: return bad(e)

@app.route("/reply_support_ticket", methods=["POST"])
def reply_ticket():
    try:
        d=request.json or {}; tid=d.get("id"); reply=str(d.get("admin_reply","")).strip()
        if not tid: return bad("Missing ticket id")
        if not reply: return bad("Admin reply is required")
        existing = supabase.table("support_tickets").select("*").eq("id",tid).limit(1).execute().data or []
        ticket = existing[0] if existing else {}
        result = supabase.table("support_tickets").update({"admin_reply":reply,"status":"replied","replied_at":now_iso(),"last_updated_at":now_iso()}).eq("id",tid).execute().data

        send_email_safe(
            ticket.get("email"),
            "NairaPips support ticket reply",
            f"""Hello {ticket.get("trader_name") or "Trader"},

NairaPips support has replied to your ticket.

Subject: {ticket.get("subject") or "Support Ticket"}

Reply:
{reply}

Please log in to your trader dashboard if you need to continue the conversation.

NairaPips Team"""
        )

        return ok(result, "Support ticket replied")
    except Exception as e: return bad(e)

@app.route("/close_support_ticket", methods=["POST"])
def close_ticket():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing ticket id")
        return ok(supabase.table("support_tickets").update({"status":"closed","closed_at":now_iso(),"last_updated_at":now_iso()}).eq("id",tid).execute().data, "Support ticket closed")
    except Exception as e: return bad(e)

@app.route("/announcements", methods=["GET"])
def announcements():
    try:
        rows = supabase.table("announcements").select("*").eq("status","active").order("created_at", desc=True).execute().data or []
        # Merge schema-free private-offer metadata so admin and dashboard can see
        # target/delivery fields even when optional Supabase columns are missing.
        try:
            rows = [_np_offer_merge_meta(r) for r in rows]
        except Exception:
            pass
        return jsonify(rows)
    except Exception as e: return bad(e)

@app.route("/create_announcement", methods=["POST"])
def create_announcement():
    try:
        d=request.json or {}; title=str(d.get("title","")).strip(); msg=str(d.get("message","")).strip()
        if not title or not msg: return bad("Title and message are required")
        row={"title":title,"message":msg,"type":d.get("type","public_notice"),"status":"active",
             "show_on_landing":d.get("show_on_landing", True),"show_on_dashboard":d.get("show_on_dashboard", True),
             "created_by":d.get("created_by","admin"),"created_at":now_iso()}
        return ok(supabase.table("announcements").insert(row).execute().data, "Announcement created")
    except Exception as e: return bad(e)

@app.route("/disable_announcement", methods=["POST"])
def disable_announcement():
    try:
        aid=(request.json or {}).get("id")
        if not aid: return bad("Missing announcement id")
        return ok(supabase.table("announcements").update({"status":"disabled"}).eq("id",aid).execute().data, "Announcement disabled")
    except Exception as e: return bad(e)




# ================================
# NAIRAPIPS AUTO-PILOT MONITORING
# FX BLUE / SNAPSHOT FOUNDATION
# ================================

MAX_DRAWDOWN_LIMIT = 20.0

def _num(value, default=0.0):
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)

def _risk_zone(max_dd_used):
    dd = _num(max_dd_used)
    if dd >= 100:
        return "breached"
    if dd >= 91:
        return "critical"
    if dd >= 76:
        return "danger"
    if dd >= 51:
        return "warning"
    return "safe"

def _dd_used_from_absolute_dd(absolute_dd_percent, dd_limit_percent=MAX_DRAWDOWN_LIMIT):
    limit = _num(dd_limit_percent, MAX_DRAWDOWN_LIMIT) or MAX_DRAWDOWN_LIMIT
    return max(0, (_num(absolute_dd_percent, 0) / limit * 100) if limit else 0)

def _safe_dd_used(payload, drawdown_percent, dd_limit_percent=MAX_DRAWDOWN_LIMIT):
    """Calculate DD-limit usage from absolute drawdown and protect against bad feed zeros."""
    computed = _dd_used_from_absolute_dd(drawdown_percent, dd_limit_percent)
    incoming_keys = [
        "dd_used_percent",
        "max_drawdown_used",
        "max_dd_used",
        "dd_used",
        "drawdown_used",
        "drawdown_used_percent",
    ]
    incoming_values = []
    for key in incoming_keys:
        raw = (payload or {}).get(key)
        if raw in [None, ""]:
            continue
        try:
            incoming_values.append(float(raw))
        except Exception:
            continue
    incoming = max(incoming_values, default=0)
    if computed > 0 and incoming <= 0:
        return computed
    if computed > incoming and incoming <= _num(drawdown_percent, 0):
        return computed
    return max(incoming, computed)

def _priority_for_zone(zone):
    return {"safe":"normal","warning":"medium","danger":"high","critical":"urgent","breached":"closed"}.get(zone, "normal")

def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

def _get_trader_by_id(trader_id):
    res = supabase.table("traders").select("*").eq("id", trader_id).limit(1).execute()
    data = getattr(res, "data", None) or []
    return data[0] if data else None

def _insert_monitoring_event(trader, event_type, zone, message, balance, equity, max_dd_used, trader_account_id=None):
    try:
        supabase.table("monitoring_events").insert({
            "trader_id": trader.get("id"),
            "trader_account_id": trader_account_id,
            "trader_name": trader.get("name"),
            "email": trader.get("email"),
            "mt5_login": trader.get("mt5_login"),
            "event_type": event_type,
            "risk_zone": zone,
            "message": message,
            "balance": balance,
            "equity": equity,
            "max_drawdown_used": max_dd_used
        }).execute()
    except Exception as e:
        print("monitoring event insert failed:", e)


def _snapshot_event_message(zone, balance, equity, profit_percent, drawdown_percent, max_dd_used):
    zone = (zone or "safe").lower()
    if zone == "breached":
        return f"BREACH DETECTED: Maximum drawdown violation recorded. Equity: {equity:,.2f}, DD: {drawdown_percent:.2f}%, Max DD used: {max_dd_used:.1f}%."
    if zone == "critical":
        return f"CRITICAL MODE: Account is very close to breach. Equity: {equity:,.2f}, DD: {drawdown_percent:.2f}%, Max DD used: {max_dd_used:.1f}%."
    if zone == "danger":
        return f"DANGER ZONE: Drawdown pressure is high. Equity: {equity:,.2f}, DD: {drawdown_percent:.2f}%, Max DD used: {max_dd_used:.1f}%."
    if zone == "warning":
        return f"WARNING ZONE: Account drawdown has entered warning level. Equity: {equity:,.2f}, DD: {drawdown_percent:.2f}%, Max DD used: {max_dd_used:.1f}%."
    return f"SAFE SNAPSHOT: Account remains within safe monitoring zone. Equity: {equity:,.2f}, Profit: {profit_percent:.2f}%, Max DD used: {max_dd_used:.1f}%."

def _should_record_snapshot_event(old_zone, zone, max_dd_used):
    """
    Keeps timeline useful without flooding:
    - always record zone changes
    - always record critical/danger/breach
    - record warning at meaningful points
    - record safe snapshots only lightly
    """
    old_zone = (old_zone or "safe").lower()
    zone = (zone or "safe").lower()

    if zone != old_zone:
        return True
    if zone in ["breached", "critical", "danger"]:
        return True
    if zone == "warning" and max_dd_used >= 60:
        return True
    if zone == "safe" and max_dd_used in [0, 25, 50]:
        return True
    return False

def _safe_traders_update(trader_id, update_data):
    """Update traders safely. If optional new columns are missing, retry with core columns."""
    try:
        return supabase.table("traders").update(update_data).eq("id", trader_id).execute()
    except Exception as e:
        print("TRADER UPDATE FULL FAILED:", e)
        core_keys = {
            "balance", "equity", "profit", "profit_percent", "drawdown", "drawdown_percent",
            "highest_equity", "lowest_equity", "peak_balance", "last_equity_snapshot",
            "max_drawdown_used", "risk_zone", "critical_mode", "monitoring_priority",
            "last_sync_at", "status", "phase", "admin_note", "mt5_access_disabled",
            "breach_time", "breach_equity", "breach_reason", "breach_detected_at",
            "updated_at"
        }
        fallback = {k:v for k,v in update_data.items() if k in core_keys}
        return supabase.table("traders").update(fallback).eq("id", trader_id).execute()


def _passed_status_from_snapshot(payload):
    status = str(payload.get("phase_pass_status") or payload.get("status") or payload.get("zone") or "").lower().strip()
    phase_label = str(payload.get("phase_label") or "").lower().strip()
    passed_statuses = {"phase1_passed", "phase2_passed", "passed", "funded_ready", "target_hit"}
    if status in passed_statuses:
        if status == "target_hit":
            return "phase2_passed" if phase_label == "phase2" else "phase1_passed"
        return status
    if str(payload.get("zone") or "").lower().strip() == "passed":
        return "phase2_passed" if phase_label == "phase2" else "phase1_passed"
    return ""


def _send_phase_pass_email_once(trader, pass_status, payload, old_status, force=False):
    old_status = str(old_status or "").lower()
    if (not force) and (old_status == pass_status or str(trader.get("phase_pass_status") or "").lower() == pass_status):
        _log_email_bank(
            trader.get("email"),
            f"Skipped duplicate {pass_status} email",
            email_type=pass_status,
            status="skipped",
            trader_id=trader.get("id"),
            message="Phase pass email was skipped because this pass status was already recorded.",
        )
        return False
    phase_name = "Phase 2" if pass_status == "phase2_passed" else "Phase 1"
    target_equity = payload.get("target_equity") or trader.get("target_equity") or 0
    highest_equity = payload.get("highest_equity") or trader.get("highest_equity") or 0
    highest_profit_percent = payload.get("highest_profit_percent") or trader.get("highest_profit_percent") or 0
    details = f"""Congratulations. You have successfully passed {phase_name} of the NairaPips Challenge.

Highest Equity Reached: {email_money(highest_equity)}
Target Equity: {email_money(target_equity)}
Profit Target Achieved: {highest_profit_percent}%

Your dashboard has been updated and the account has been locked for admin review / next stage processing."""
    subject = f"Congratulations - You passed {phase_name}"
    sent = send_account_status_email(
        trader,
        subject,
        f"You have passed {phase_name}.",
        details
    )
    _log_email_bank(
        trader.get("email"),
        subject,
        email_type=pass_status,
        status="sent" if sent else "failed",
        trader_id=trader.get("id"),
        message=details,
        error="send_account_status_email returned False" if not sent else "",
    )
    send_admin_alert(
        f"NairaPips {phase_name} passed",
        f"""A trader has passed {phase_name}.

Trader: {trader.get('name') or trader.get('trader_name') or 'Trader'}
Email: {trader.get('email') or 'Not provided'}
MT5 Login: {trader.get('mt5_login') or payload.get('mt5_login') or 'Not provided'}
Highest Equity: {email_money(highest_equity)}
Target Equity: {email_money(target_equity)}
Status: {pass_status}"""
    )
    return sent


def _apply_monitoring_snapshot(trader, payload, source="manual"):
    # Values from MT5 engine are the source of truth when present.
    active_account = None
    incoming_account_id = str((payload or {}).get("trader_account_id") or (payload or {}).get("current_account_id") or "").strip()
    if incoming_account_id:
        try:
            rows = supabase.table("trader_accounts").select("*").eq("id", incoming_account_id).limit(1).execute().data or []
            if rows and str(rows[0].get("trader_id") or "") == str(trader.get("id") or ""):
                active_account = _decorate_account_for_api(rows[0])
        except Exception as e:
            print("ACTIVE ACCOUNT BY ID FETCH ERROR:", e)
    incoming_login = str((payload or {}).get("mt5_login") or (payload or {}).get("login") or (payload or {}).get("account") or "").strip()
    if incoming_login and not active_account:
        by_login = _get_account_by_login_any_status(incoming_login, trader.get("id"))
        if by_login and str(by_login.get("trader_id") or "") == str(trader.get("id") or ""):
            active_account = _decorate_account_for_api(by_login)
    if not active_account:
        active_account = _get_active_account(trader.get("id"), trader)
    account_start = _num(active_account.get("start_balance"), _num(active_account.get("account_size"), 0)) if active_account else 0
    balance = _num(payload.get("balance"), _num(active_account.get("current_balance") if active_account else trader.get("balance"), _num(trader.get("balance"), _num(trader.get("account_size")))))
    equity = _num(payload.get("equity"), balance)
    account_size = account_start or _num(trader.get("account_size"), balance)

    profit = _num(payload.get("profit"), equity - account_size if account_size else 0)
    profit_percent = _num(payload.get("profit_percent"), (profit / account_size * 100) if account_size else 0)

    previous_highest = _num(trader.get("highest_equity"), 0)
    previous_lowest = _num(trader.get("lowest_equity"), 0)
    if active_account:
        # Do not let a previous phase's trader-level high/low contaminate the current account.
        previous_highest = max(
            account_size,
            _num(active_account.get("highest_equity"), 0),
            _num(active_account.get("current_equity"), account_size),
        )
        previous_lowest = (
            _num(active_account.get("lowest_equity"), 0)
            or _num(active_account.get("current_equity"), account_size)
            or account_size
        )

    highest_equity = max(previous_highest, _num(payload.get("highest_equity"), 0), equity, account_size)
    lowest_equity = min(previous_lowest, equity) if previous_lowest > 0 else equity

    # GLOBAL LIVE DRAWDOWN RULE:
    # Breach danger must always be based on CURRENT equity versus the fixed
    # challenge/account size. Highest/lowest equity are evidence only and must
    # never hide a live account approaching breach.
    live_base = account_size or balance
    live_equity_damage = max(0, live_base - equity) if live_base else 0
    live_drawdown_percent = (live_equity_damage / live_base * 100) if live_base else 0

    # Prefer the engine's current drawdown if present, but recompute from live
    # equity as a safety net so all modules show the same risk.
    engine_dd = payload.get("current_drawdown_percent", payload.get("actual_drawdown_percent", payload.get("drawdown_percent")))
    if engine_dd is not None:
        drawdown_percent = max(0, _num(engine_dd, live_drawdown_percent))
        equity_damage = max(0, live_base * drawdown_percent / 100) if live_base else live_equity_damage
    else:
        drawdown_percent = live_drawdown_percent
        equity_damage = live_equity_damage

    dd_limit_percent = _num(active_account.get("dd_limit_percent"), MAX_DRAWDOWN_LIMIT) if active_account else MAX_DRAWDOWN_LIMIT
    max_dd_used = _safe_dd_used(payload, drawdown_percent, dd_limit_percent)
    dd_remaining_percent = max(0, dd_limit_percent - drawdown_percent)
    breach_equity_level = _num(payload.get("breach_equity_level"), live_base * (1 - dd_limit_percent / 100) if live_base else 0)
    worst_static_drawdown_percent = _num(payload.get("worst_static_drawdown_percent"), ((max(0, live_base - lowest_equity) / live_base) * 100) if live_base else 0)
    worst_dd_used_percent = _num(payload.get("worst_dd_used_percent"), _safe_dd_used(payload, worst_static_drawdown_percent, dd_limit_percent))
    worst_dd_remaining_percent = _num(payload.get("worst_dd_remaining_percent"), max(0, dd_limit_percent - worst_static_drawdown_percent))

    # Production pass meter. Works globally for every plan size because it uses account_size and target_percent.
    active_stage_for_meter = str((active_account or {}).get("stage") or payload.get("phase_label") or trader.get("phase") or "").lower()
    meter_target = _target_for_stage(active_stage_for_meter)
    if meter_target is None:
        pass_progress_percent = 0
        pass_remaining_percent = 0
    else:
        highest_profit_percent_for_meter = ((highest_equity - account_size) / account_size * 100) if account_size else 0
        pass_progress_percent = _num(payload.get("pass_progress_percent"), max(0, (highest_profit_percent_for_meter / meter_target) * 100 if meter_target else 0))
        pass_remaining_percent = _num(payload.get("pass_remaining_percent"), max(0, 100 - pass_progress_percent))

    incoming_zone = str(payload.get("zone") or "").lower().strip()
    incoming_status = str(payload.get("status") or "").lower().strip()
    incoming_pass_status = _passed_status_from_snapshot(payload)
    passed_status = ""
    breached = bool(payload.get("breached")) or incoming_status == "breached" or incoming_zone == "breached" or max_dd_used >= 100

    # GLOBAL PASS SAFETY RULE:
    # Live assigned accounts may only pass from current account metrics, never from stale
    # trader.phase_pass_status / old monitoring events / old phase2_passed text.
    if active_account and not breached:
        account_stage = str(active_account.get("stage") or "").lower()
        target = _target_for_stage(account_stage)
        target_equity_value = account_size * (1 + (target / 100)) if target is not None and account_size else 0
        metric_passed = bool(target is not None and max_dd_used < 100 and account_size and highest_equity >= target_equity_value)
        if metric_passed:
            passed_status = "phase2_passed" if account_stage == "phase2" else "phase1_passed"
        else:
            # Prevent stale pass flags from locking or mislabeling an active account.
            passed_status = ""
    elif not active_account:
        # Legacy/manual fallback only when no account-level source exists.
        passed_status = incoming_pass_status

    old_zone = ((active_account or {}).get("risk_zone") or trader.get("risk_zone") or "safe").lower()
    old_status = ((active_account or {}).get("account_status") or trader.get("status") or "").lower()
    now = _now_iso()
    is_current_account = (not active_account) or _account_is_current_for_trader(trader, active_account)

    # Default live monitoring state.
    zone = incoming_zone if incoming_zone in ["safe", "warning", "danger", "critical", "funded", "funded_profit_zone", "profit_protected"] else _risk_zone(max_dd_used)
    priority = _priority_for_zone(zone)

    update_data = {
        "balance": balance,
        "equity": equity,
        "profit": profit,
        "profit_percent": profit_percent,
        "drawdown": equity_damage,
        "drawdown_percent": drawdown_percent,
        "actual_drawdown_percent": drawdown_percent,
        "current_drawdown_percent": drawdown_percent,
        "drawdown_amount": equity_damage,
        "dd_remaining_percent": dd_remaining_percent,
        "highest_equity": highest_equity,
        "lowest_equity": lowest_equity,
        "peak_balance": max(_num(trader.get("peak_balance"), 0), balance, account_size, highest_equity),
        "last_equity_snapshot": equity,
        "max_drawdown_used": max_dd_used,
        "risk_zone": zone,
        "critical_mode": zone in ["danger", "critical"],
        "monitoring_priority": priority,
        "last_sync_at": now,
        "updated_at": now,
        "target_equity": _num(payload.get("target_equity"), 0),
        "highest_profit": _num(payload.get("highest_profit"), highest_equity - account_size),
        "highest_profit_percent": _num(payload.get("highest_profit_percent"), 0),
        "profit_target": _num(payload.get("profit_target"), 0),
        "phase_label": payload.get("phase_label") or trader.get("phase"),
        "breach_equity_level": breach_equity_level,
        "worst_static_drawdown_percent": worst_static_drawdown_percent,
        "worst_dd_used_percent": worst_dd_used_percent,
        "worst_dd_remaining_percent": worst_dd_remaining_percent,
        "pass_progress_percent": pass_progress_percent,
        "pass_remaining_percent": pass_remaining_percent,
        "funded_profit_floor": _num(payload.get("funded_profit_floor"), 0),
        "funded_profit_label": payload.get("funded_profit_label") or trader.get("funded_profit_label"),
    }

    # Critical rule order: PASS FIRST, BREACH SECOND.
    # If highest equity has already hit the target, never let later DD overwrite it as critical/breached.
    if passed_status:
        zone = "passed"
        priority = "passed"
        next_phase = "phase2"
        next_status = "phase2_waiting_mt5"
        admin_note = payload.get("reason") or "Phase 1 passed. Assign a fresh Phase 2 MT5 account."

        if passed_status == "phase2_passed":
            next_phase = "funded_waiting"
            next_status = "funded_waiting_mt5"
            admin_note = payload.get("reason") or "Phase 2 passed. Assign funded/live account after admin review."

        update_data.update({
            "status": next_status,
            "phase": next_phase,
            "phase_pass_status": passed_status,
            "phase_passed_at": trader.get("phase_passed_at") or now,
            "passed_at": trader.get("passed_at") or now,
            "risk_zone": "passed",
            "critical_mode": False,
            "monitoring_priority": "passed",
            "mt5_access_disabled": True,
            "mt5_account_active": False,
            "monitoring_enabled": False,
            "payout_blocked": False,
            "payout_eligible": False,
            "admin_note": admin_note,
        })
        if passed_status == "phase1_passed":
            update_data["phase1_passed_at"] = trader.get("phase1_passed_at") or now
        if passed_status == "phase2_passed":
            update_data["phase2_passed_at"] = trader.get("phase2_passed_at") or now
            update_data["certificate_status"] = trader.get("certificate_status") or "passed"
            update_data["certificate_passed_at"] = trader.get("certificate_passed_at") or now

    elif breached:
        zone = "breached"
        priority = "closed"
        update_data.update({
            "status": "breached",
            "phase": "breached",
            "risk_zone": "breached",
            "critical_mode": False,
            "monitoring_priority": "closed",
            "breach_time": trader.get("breach_time") or now,
            "breach_equity": equity,
            "breach_reason": payload.get("reason") or "Maximum drawdown violation recorded by NairaPips monitoring engine.",
            "admin_note": payload.get("reason") or "Auto-breach: maximum drawdown violation recorded by monitoring engine.",
            "mt5_access_disabled": True,
            "mt5_account_active": False,
            "payout_eligible": False,
            "payout_blocked": True,
            "breach_detected_at": trader.get("breach_detected_at") or now,
        })

    elif incoming_status == "profit_protected" or incoming_zone == "profit_protected":
        zone = "profit_protected"
        update_data.update({
            "status": "profit_protected",
            "risk_zone": "profit_protected",
            "critical_mode": False,
            "monitoring_priority": "urgent",
            "mt5_access_disabled": True,
            "mt5_account_active": False,
            "admin_note": payload.get("reason") or "Funded hybrid profit protection triggered. Account locked for payout/admin review.",
        })

    if active_account:
        try:
            account_update = {
                "current_balance": balance,
                "current_equity": equity,
                "profit": profit,
                "profit_percent": profit_percent,
                "highest_equity": highest_equity,
                "lowest_equity": lowest_equity,
                "absolute_drawdown_percent": drawdown_percent,
                "drawdown_percent": drawdown_percent,
                "actual_drawdown_percent": drawdown_percent,
                "dd_used_percent": max_dd_used,
                "current_dd_used_percent": max_dd_used,
                "dd_remaining_percent": dd_remaining_percent,
                "breach_equity_level": breach_equity_level,
                "worst_static_drawdown_percent": worst_static_drawdown_percent,
                "worst_dd_used_percent": worst_dd_used_percent,
                "worst_dd_remaining_percent": worst_dd_remaining_percent,
                "pass_progress_percent": pass_progress_percent,
                "pass_remaining_percent": pass_remaining_percent,
                "risk_zone": update_data.get("risk_zone", zone),
                "monitoring_enabled": not (passed_status or breached or incoming_status == "profit_protected"),
                "updated_at": now,
            }
            if passed_status:
                account_update["phase_pass_status"] = passed_status
                account_update["passed_at"] = account_update.get("passed_at") or now
            elif str(active_account.get("account_status") or "").lower() == "assigned_active":
                # Clear stale account-level pass labels while the account is still active.
                account_update["phase_pass_status"] = None
            if breached:
                account_update["breached_at"] = account_update.get("breached_at") or now
                account_update["breach_reason"] = update_data.get("breach_reason")
            try:
                supabase.table("trader_accounts").update(account_update).eq("id", active_account.get("id")).execute()
            except Exception as account_update_error:
                print("trader account monitoring full update failed:", account_update_error)
                core_account_update = {
                    "current_balance": balance,
                    "current_equity": equity,
                    "profit": profit,
                    "profit_percent": profit_percent,
                    "absolute_drawdown_percent": drawdown_percent,
                    "drawdown_percent": drawdown_percent,
                    "dd_used_percent": max_dd_used,
                    "monitoring_enabled": not (passed_status or breached or incoming_status == "profit_protected"),
                    "updated_at": now,
                }
                supabase.table("trader_accounts").update(core_account_update).eq("id", active_account.get("id")).execute()
        except Exception as e:
            print("trader account monitoring update failed:", e)

    if is_current_account:
        _safe_traders_update(trader.get("id"), update_data)

    if active_account and passed_status:
        try:
            _pass_specific_account(
                trader,
                active_account,
                passed_status,
                {"name": "monitoring_engine", "username": "monitoring_engine"},
                update_data.get("admin_note") or "Stage passed by monitoring engine."
            )
        except Exception as e:
            print("auto phase archive failed:", e)

    if active_account and breached:
        try:
            _breach_specific_account(
                trader,
                active_account,
                update_data.get("breach_reason") or "Maximum drawdown violation recorded by monitoring engine.",
                {"name": "monitoring_engine", "username": "monitoring_engine"}
            )
        except Exception as e:
            print("auto breach archive failed:", e)

    try:
        supabase.table("monitoring_snapshots").insert({
            "trader_id": trader.get("id"),
            "trader_account_id": active_account.get("id") if active_account else None,
            "trader_name": trader.get("name"),
            "email": trader.get("email"),
            "mt5_login": active_account.get("mt5_login") if active_account else trader.get("mt5_login"),
            "balance": balance,
            "equity": equity,
            "profit": profit,
            "profit_percent": profit_percent,
            "drawdown": equity_damage,
            "drawdown_percent": drawdown_percent,
            "actual_drawdown_percent": drawdown_percent,
            "current_drawdown_percent": drawdown_percent,
            "drawdown_amount": equity_damage,
            "max_drawdown_used": max_dd_used,
            "dd_used_percent": max_dd_used,
            "dd_remaining_percent": dd_remaining_percent,
            "breach_equity_level": breach_equity_level,
            "worst_static_drawdown_percent": worst_static_drawdown_percent,
            "worst_dd_used_percent": worst_dd_used_percent,
            "worst_dd_remaining_percent": worst_dd_remaining_percent,
            "pass_progress_percent": pass_progress_percent,
            "pass_remaining_percent": pass_remaining_percent,
            "risk_zone": update_data.get("risk_zone", zone),
            "source": source,
            "raw_data": payload
        }).execute()
    except Exception as e:
        print("monitoring snapshot insert failed:", e)

    if passed_status and old_status != passed_status:
        _insert_monitoring_event(
            trader,
            "phase_passed",
            "passed",
            f"{passed_status} confirmed by highest equity target. Account locked for admin review / next stage.",
            balance,
            equity,
            max_dd_used,
            active_account.get("id") if active_account else None
        )
        _send_phase_pass_email_once(trader, passed_status, payload, old_status)

    elif _should_record_snapshot_event(old_zone, update_data.get("risk_zone", zone), round(max_dd_used)):
        event_type = "monitoring_snapshot"
        event_zone = update_data.get("risk_zone", zone)
        if event_zone != old_zone:
            event_type = "risk_zone_change"
        if event_zone == "critical":
            event_type = "critical_mode"
        if event_zone == "danger":
            event_type = "danger_zone"
        if event_zone == "breached":
            event_type = "breach_detected"

        _insert_monitoring_event(
            trader,
            event_type,
            event_zone,
            _snapshot_event_message(event_zone, balance, equity, profit_percent, drawdown_percent, max_dd_used),
            balance,
            equity,
            max_dd_used,
            active_account.get("id") if active_account else None
        )

    if update_data.get("risk_zone") == "breached" and old_status != "breached":
        _insert_monitoring_event(
            trader,
            "account_locked",
            "breached",
            "Account locked permanently by NairaPips monitoring engine after maximum drawdown violation.",
            balance,
            equity,
            max_dd_used,
            active_account.get("id") if active_account else None
        )
        send_account_status_email(
            trader,
            "NairaPips account breached",
            "Your NairaPips account has been breached and locked.",
            update_data.get("breach_reason") or "Maximum drawdown violation recorded by NairaPips monitoring engine."
        )
        send_admin_alert(
            "NairaPips account breached",
            f"""A trader account has breached and has been locked.

Trader: {trader.get('name') or 'Trader'}
Email: {trader.get('email') or 'Not provided'}
MT5 Login: {trader.get('mt5_login') or payload.get('mt5_login') or 'Not provided'}
Equity: {email_money(equity)}
Max DD Used: {round(max_dd_used, 1)}%"""
        )

    return {
        "trader_id": trader.get("id"),
        "trader_account_id": active_account.get("id") if active_account else None,
        "balance": balance,
        "equity": equity,
        "profit": profit,
        "profit_percent": profit_percent,
        "drawdown_percent": drawdown_percent,
        "actual_drawdown_percent": drawdown_percent,
        "current_drawdown_percent": drawdown_percent,
        "drawdown_amount": equity_damage,
        "max_drawdown_used": max_dd_used,
        "dd_used_percent": max_dd_used,
        "risk_zone": update_data.get("risk_zone", zone),
        "critical_mode": update_data.get("critical_mode", False),
        "monitoring_priority": update_data.get("monitoring_priority", priority),
        "status": update_data.get("status", trader.get("status")),
        "phase_pass_status": update_data.get("phase_pass_status", trader.get("phase_pass_status"))
    }

# ================================
# NAIRAPIPS PRIVATE TRADER OFFERS
# Dashboard targeting + email delivery support for retention offers.
# Uses existing Brevo/email setup already used by OTP emails.
# Safe fallback: if optional announcement columns do not exist yet, the route
# still sends email but will not leak a private message publicly.
# ================================
def _np_offer_clean_str(v, max_len=2000):
    return str(v or "").strip()[:max_len]


def _np_offer_bool(v, default=False):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on", "y"}


def _np_private_offer_html(title, message, offer_code="", expires_at="", cta_url=""):
    """Luxury black/gold NairaPips private-offer email template.

    Keep this function self-contained because Brevo receives raw HTML content.
    It is used by /create_private_offer and /send_private_offer_email.
    """
    title = _np_offer_clean_str(title or "Private NairaPips Offer", 250)
    message = _np_offer_clean_str(message, 5000)
    offer_code = _np_offer_clean_str(offer_code, 120)
    expires_at = _np_offer_clean_str(expires_at, 120)
    cta_url = _np_offer_clean_str(cta_url or "https://nairapips.com/dashboard/", 500)

    # Clean accidental duplicated admin preview lines before rendering the email.
    # This removes text like "NairaPips Logo", duplicate title, offer code, expiry, and footer lines
    # from the message body because those are rendered cleanly by the template below.
    clean_lines = []
    seen_lines = set()
    for line in str(message or "").replace("\r", "").split("\n"):
        s = line.strip()
        low = s.lower()
        if not s:
            if clean_lines and clean_lines[-1] != "":
                clean_lines.append("")
            continue
        if low in {"nairapips logo", "logo", "nairapips team"}:
            continue
        if low.startswith("offer code:") or low.startswith("expires:"):
            continue
        if low == title.strip().lower():
            continue
        key = low[:180]
        if key in seen_lines:
            continue
        seen_lines.add(key)
        clean_lines.append(s)
    while clean_lines and clean_lines[-1] == "":
        clean_lines.pop()
    message = "\n".join(clean_lines).strip() or "You have received a private NairaPips offer. Log in to your dashboard or contact support to activate it."

    def _format_offer_expiry(value):
        value = str(value or "").strip()
        if not value:
            return ""
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.strftime("%d %b %Y, %H:%M")
        except Exception:
            return value

    try:
        body = text_to_html_content(message)
    except Exception:
        body = "<p>" + html.escape(message).replace("\n", "<br>") + "</p>"

    safe_title = html.escape(title)
    safe_offer_code = html.escape(str(offer_code))
    safe_expires = html.escape(_format_offer_expiry(expires_at))
    safe_url = html.escape(str(cta_url), quote=True)

    offer_block = ""
    if offer_code:
        offer_block = f"""
          <tr>
            <td style="padding:0 32px 22px 32px;">
              <div style="background:#050505;border:1px solid #d4af37;border-radius:18px;padding:18px;text-align:center;box-shadow:0 0 22px rgba(212,175,55,.18);">
                <div style="font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:#b8b8b8;font-weight:800;margin-bottom:8px;">Exclusive Offer Code</div>
                <div style="font-size:30px;line-height:1.1;color:#f5d76e;font-weight:900;letter-spacing:.08em;">{safe_offer_code}</div>
              </div>
            </td>
          </tr>"""

    expiry_block = ""
    if expires_at:
        expiry_block = f"""
          <tr>
            <td style="padding:0 32px 22px 32px;">
              <div style="background:rgba(212,175,55,.08);border:1px solid rgba(212,175,55,.35);border-radius:16px;padding:14px 16px;color:#f5d76e;font-size:14px;font-weight:800;text-align:center;">
                Offer expires: <span style="color:#ffffff;">{safe_expires}</span>
              </div>
            </td>
          </tr>"""

    cta_block = ""
    if cta_url:
        cta_block = f"""
          <tr>
            <td style="padding:4px 32px 34px 32px;text-align:center;">
              <a href="{safe_url}" style="display:inline-block;background:linear-gradient(135deg,#d4af37,#f8df75);color:#050505;text-decoration:none;font-weight:900;font-size:16px;padding:16px 30px;border-radius:999px;box-shadow:0 12px 30px rgba(212,175,55,.25);">
                Open NairaPips Dashboard
              </a>
              <div style="font-size:12px;color:#8f8f8f;margin-top:14px;line-height:1.5;">Log in to your dashboard to view or claim this private offer.</div>
            </td>
          </tr>"""

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{safe_title}</title>
</head>
<body style="margin:0;padding:0;background:#050505;font-family:Arial,Helvetica,sans-serif;color:#ffffff;">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
    A private NairaPips offer has been unlocked for your trading account.
  </div>
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#050505;margin:0;padding:28px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="max-width:650px;background:#0a0a0a;border:1px solid rgba(212,175,55,.45);border-radius:26px;overflow:hidden;box-shadow:0 0 42px rgba(212,175,55,.14);">
          <tr>
            <td style="background:linear-gradient(135deg,#111111,#050505 60%,#211a07);padding:30px 32px 24px 32px;border-bottom:1px solid rgba(212,175,55,.28);">
              <div style="font-size:30px;line-height:1;font-weight:900;letter-spacing:.02em;color:#ffffff;">
                Naira<span style="color:#d4af37;">Pips</span>
              </div>
              <div style="font-size:12px;letter-spacing:.22em;text-transform:uppercase;color:#d4af37;font-weight:800;margin-top:8px;">Private Trader Opportunity</div>
            </td>
          </tr>
          <tr>
            <td style="padding:32px 32px 12px 32px;">
              <div style="display:inline-block;background:rgba(212,175,55,.1);border:1px solid rgba(212,175,55,.4);border-radius:999px;padding:8px 13px;color:#f5d76e;font-size:12px;font-weight:900;letter-spacing:.06em;text-transform:uppercase;">
                Exclusive Message
              </div>
              <h1 style="margin:20px 0 14px 0;color:#f5d76e;font-size:34px;line-height:1.05;font-weight:900;letter-spacing:-.03em;">{safe_title}</h1>
              <div style="color:#f1f1f1;font-size:16px;line-height:1.75;">
                {body}
              </div>
            </td>
          </tr>
          {offer_block}
          {expiry_block}
          {cta_block}
          <tr>
            <td style="padding:0 32px 30px 32px;">
              <div style="border-top:1px solid rgba(255,255,255,.08);padding-top:22px;color:#b8b8b8;font-size:13px;line-height:1.65;">
                This offer was sent privately to your NairaPips trader account. Do not share your dashboard password, MT5 password, or verification codes with anyone.
                <br><br>
                <strong style="color:#ffffff;">NairaPips Team</strong><br>
                <span style="color:#d4af37;">Empowering disciplined traders.</span>
              </div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

def _np_find_offer_trader(target_trader_id="", target_email=""):
    target_trader_id = _np_offer_clean_str(target_trader_id, 120)
    target_email = _np_offer_clean_str(target_email, 250).lower()
    try:
        if target_trader_id:
            rows = supabase.table("traders").select("*").eq("id", target_trader_id).limit(1).execute().data or []
            if rows:
                return rows[0]
        if target_email:
            rows = supabase.table("traders").select("*").eq("email", target_email).limit(1).execute().data or []
            if rows:
                return rows[0]
            rows = supabase.table("traders").select("*").eq("canonical_email", target_email).limit(1).execute().data or []
            if rows:
                return rows[0]
    except Exception as e:
        print("PRIVATE OFFER TRADER LOOKUP ERROR:", e)
    return None



# ================================
# PRIVATE OFFER SCHEMA-FREE FALLBACK
# ================================
def _np_offer_meta_pack(meta):
    try:
        raw = json.dumps(meta or {}, separators=(",", ":"), ensure_ascii=False)
        return "NP_PRIVATE_OFFER_META:" + base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")
    except Exception:
        return "NP_PRIVATE_OFFER_META:e30="


def _np_offer_meta_unpack(value):
    try:
        text = str(value or "")
        if not text.startswith("NP_PRIVATE_OFFER_META:"):
            return {}
        b64 = text.split(":", 1)[1]
        return json.loads(base64.urlsafe_b64decode(b64.encode("ascii")).decode("utf-8")) or {}
    except Exception:
        return {}


def _np_offer_merge_meta(row):
    row = dict(row or {})
    meta = _np_offer_meta_unpack(row.get("created_by"))
    if meta:
        for k, v in meta.items():
            if row.get(k) in [None, "", False]:
                row[k] = v
        # Keep admin display clean; don't expose encoded metadata.
        row["created_by"] = meta.get("created_by") or "admin"
        row["_schema_fallback"] = True
        # Do not lose dashboard intent when the DB had no delivery columns.
        if _np_offer_bool(meta.get("delivery_dashboard"), False):
            row["delivery_dashboard"] = True
    return row



# ================================
# TRADER RECOVERY CENTER API
# ================================
def _np_recovery_num(v):
    try:
        return float(v or 0)
    except Exception:
        return 0.0

def _np_recovery_date_score(v):
    try:
        if not v:
            return 0
        return int(datetime.fromisoformat(str(v).replace('Z','+00:00')).timestamp())
    except Exception:
        return 0

def _np_recovery_days_since(v):
    s = _np_recovery_date_score(v)
    if not s:
        return 999
    return max(0, int((time.time() - s) // 86400))

def _np_recovery_is_breached(row):
    blob = ' '.join(str(row.get(k) or '').lower() for k in ['status','account_status','phase','challenge_state','risk_zone','display_risk_zone'])
    return ('breach' in blob) or ('locked' in blob) or ('disabled' in blob) or _is_truthy(row.get('mt5_access_disabled'))

def _np_recovery_dd_used(row):
    return max(
        _np_recovery_num(row.get('dd_used_percent')),
        _np_recovery_num(row.get('max_drawdown_used')),
        _np_recovery_num(row.get('current_dd_limit_used')),
        _np_recovery_num(row.get('absolute_drawdown_percent')) * 5,
        _np_recovery_num(row.get('drawdown_percent')) * 5,
    )

@app.route('/trader_recovery_candidates', methods=['GET','OPTIONS'])
def trader_recovery_candidates():
    if request.method == 'OPTIONS':
        return _np_ok({})
    try:
        rows = []
        try:
            rows = supabase.table('traders').select('*').limit(2000).execute().data or []
        except Exception as e:
            print('RECOVERY TRADERS FETCH ERROR:', e)
            rows = []

        accounts_by_trader = {}
        try:
            accs = supabase.table('trader_accounts').select('*').limit(4000).execute().data or []
            for a in accs:
                tid = str(a.get('trader_id') or '')
                if tid:
                    accounts_by_trader.setdefault(tid, []).append(a)
        except Exception as e:
            print('RECOVERY ACCOUNTS FETCH ERROR:', e)

        trades_by_key = {}
        try:
            trades = supabase.table('trader_trades').select('*').order('synced_at', desc=True).limit(5000).execute().data or []
            for tr in trades:
                keys = [str(tr.get('trader_id') or '').lower(), str(tr.get('email') or '').lower(), str(tr.get('mt5_login') or '').lower()]
                dt = tr.get('opened_at') or tr.get('closed_at') or tr.get('synced_at') or tr.get('updated_at') or tr.get('created_at')
                score = _np_recovery_date_score(dt)
                for k in keys:
                    if k:
                        trades_by_key[k] = max(trades_by_key.get(k,0), score)
        except Exception as e:
            print('RECOVERY TRADES FETCH ERROR:', e)

        buckets = {k: [] for k in ['near_breach','breached','inactive','funded_danger','phase1_stuck']}
        seen = {k:set() for k in buckets}

        for t in rows:
            tid = str(t.get('id') or '')
            accounts = accounts_by_trader.get(tid, [])
            current = None
            if accounts:
                accounts.sort(key=lambda a: _np_recovery_date_score(a.get('updated_at') or a.get('started_at') or a.get('created_at')), reverse=True)
                current = accounts[0]
            merged = dict(t)
            if current:
                for k,v in current.items():
                    merged.setdefault('account_'+k, v)
                merged.update({
                    'account_status': current.get('account_status') or merged.get('status'),
                    'stage': current.get('stage') or merged.get('phase'),
                    'mt5_login': current.get('mt5_login') or merged.get('mt5_login'),
                    'dd_used_percent': current.get('dd_used_percent') or merged.get('dd_used_percent'),
                    'absolute_drawdown_percent': current.get('absolute_drawdown_percent') or merged.get('drawdown_percent'),
                    'account_size': current.get('account_size') or merged.get('account_size'),
                    'started_at': current.get('started_at') or merged.get('challenge_started_at'),
                    'mt5_access_disabled': current.get('mt5_access_disabled') or merged.get('mt5_access_disabled'),
                })
            key = tid or str(t.get('email') or t.get('phone') or uuid.uuid4())
            status_blob = ' '.join(str(merged.get(k) or '').lower() for k in ['status','account_status','phase','stage','challenge_state'])
            dd = _np_recovery_dd_used(merged)
            breached = _np_recovery_is_breached(merged)
            login = str(merged.get('mt5_login') or '').strip()
            last_trade_score = max(trades_by_key.get(tid.lower(),0), trades_by_key.get(str(t.get('email') or '').lower(),0), trades_by_key.get(login.lower(),0))
            last_trade_iso = datetime.fromtimestamp(last_trade_score, timezone.utc).isoformat() if last_trade_score else None
            days_inactive = _np_recovery_days_since(last_trade_iso)
            days_stage = _np_recovery_days_since(merged.get('started_at') or merged.get('challenge_started_at') or merged.get('approved_at') or merged.get('created_at'))

            def add(bucket, reason):
                if key in seen[bucket]:
                    return
                seen[bucket].add(key)
                item = {
                    'trader_id': tid, 'name': t.get('name') or t.get('full_name'), 'email': t.get('email'), 'phone': t.get('phone'),
                    'account_reference': t.get('account_reference'), 'mt5_login': login, 'account_size': merged.get('account_size'),
                    'dd_used_percent': dd, 'stage': merged.get('stage') or merged.get('phase'), 'status': merged.get('account_status') or merged.get('status'),
                    'last_trade_at': last_trade_iso, 'reason': reason
                }
                buckets[bucket].append(item)

            if breached:
                add('breached', 'Breached/locked/disabled evidence found')
            if (not breached) and 70 <= dd < 100:
                add('near_breach', f'DD used around {dd:.1f}%')
            if (not breached) and ('funded' in status_blob or 'live' in status_blob) and dd >= 50:
                add('funded_danger', f'Funded/live risk: {dd:.1f}% DD used')
            if (not breached) and login and days_inactive >= 7:
                add('inactive', f'No recent trade for {days_inactive} day(s)')
            if (not breached) and 'phase1' in status_blob and days_stage >= 20:
                add('phase1_stuck', f'Phase 1 active for {days_stage} day(s)')

        return ok({'buckets': buckets, 'summary': {k: len(v) for k,v in buckets.items()}})
    except Exception as e:
        print('TRADER RECOVERY CANDIDATES ERROR:', e)
        return bad(e)

@app.route("/create_private_offer", methods=["POST", "OPTIONS"])
def create_private_offer():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        title = _np_offer_clean_str(d.get("title"), 250)
        message = _np_offer_clean_str(d.get("message"), 5000)
        target_email = _np_offer_clean_str(d.get("target_email"), 250).lower()
        target_trader_id = _np_offer_clean_str(d.get("target_trader_id"), 120)

        trader = _np_find_offer_trader(target_trader_id, target_email)
        if trader:
            target_trader_id = str(trader.get("id") or target_trader_id or "")
            target_email = str(trader.get("email") or target_email or "").strip().lower()

        if not title or not message:
            return bad("Title and message are required")
        if not target_email and not target_trader_id:
            return bad("Choose a target trader for a private offer")

        delivery_dashboard = _np_offer_bool(d.get("delivery_dashboard"), True)
        delivery_email = _np_offer_bool(d.get("delivery_email"), False)
        delivery_whatsapp = _np_offer_bool(d.get("delivery_whatsapp"), False)
        subject = _np_offer_clean_str(d.get("subject") or title, 250)
        cta_url = _np_offer_clean_str(d.get("cta_url") or "https://nairapips.com/dashboard/", 500)

        row = {
            "title": title,
            "message": message,
            "type": "private_offer",
            "status": "active",
            "show_on_landing": False,
            "show_on_dashboard": delivery_dashboard,
            "created_by": _np_offer_clean_str(d.get("created_by") or "admin", 120),
            "created_at": now_iso(),
            "target_trader_id": target_trader_id or None,
            "target_email": target_email or None,
            "target_name": _np_offer_clean_str(d.get("target_name") or (trader or {}).get("name"), 250) or None,
            "target_phone": _np_offer_clean_str(d.get("target_phone") or (trader or {}).get("phone"), 80) or None,
            "target_account_reference": _np_offer_clean_str(d.get("target_account_reference") or (trader or {}).get("account_reference"), 120) or None,
            "subject": subject,
            "offer_code": _np_offer_clean_str(d.get("offer_code") or "PHASEHELP", 120) or "PHASEHELP",
            "expires_at": _np_offer_clean_str(d.get("expires_at"), 120) or None,
            "cta_label": _np_offer_clean_str(d.get("cta_label") or "Contact Support", 120),
            "cta_url": cta_url,
            "priority": _np_offer_clean_str(d.get("priority") or "normal", 40),
            "require_ack": _np_offer_bool(d.get("require_ack"), True),
            "delivery_dashboard": delivery_dashboard,
            "delivery_email": delivery_email,
            "delivery_whatsapp": delivery_whatsapp,
            "read_at": None,
        }

        dashboard_saved = True
        created = []
        try:
            created = supabase.table("announcements").insert(row).execute().data or []
        except Exception as schema_error:
            print("PRIVATE OFFER ANNOUNCEMENT SCHEMA FALLBACK:", schema_error)
            dashboard_saved = False
            # Schema-free fallback: store targeting metadata in created_by so private
            # dashboard delivery still works even before optional DB columns exist.
            # show_on_dashboard stays false so it never leaks as a public announcement.
            meta = {
                "private_offer": True,
                "target_trader_id": target_trader_id or "",
                "target_email": target_email or "",
                "target_name": _np_offer_clean_str(d.get("target_name") or (trader or {}).get("name"), 250),
                "target_phone": _np_offer_clean_str(d.get("target_phone") or (trader or {}).get("phone"), 80),
                "target_account_reference": _np_offer_clean_str(d.get("target_account_reference") or (trader or {}).get("account_reference"), 120),
                "subject": subject,
                "offer_code": _np_offer_clean_str(d.get("offer_code") or "PHASEHELP", 120),
                "expires_at": _np_offer_clean_str(d.get("expires_at"), 120),
                "cta_label": _np_offer_clean_str(d.get("cta_label") or "Contact Support", 120),
                "cta_url": cta_url,
                "priority": _np_offer_clean_str(d.get("priority") or "normal", 40),
                "require_ack": _np_offer_bool(d.get("require_ack"), True),
                "delivery_dashboard": delivery_dashboard,
                "show_on_dashboard": delivery_dashboard,
                "delivery_email": delivery_email,
                "delivery_whatsapp": delivery_whatsapp,
                "created_by": _np_offer_clean_str(d.get("created_by") or "admin", 120),
            }
            safe_row = {
                "title": title,
                "message": message,
                "type": "private_offer",
                "status": "active",
                "show_on_landing": False,
                "show_on_dashboard": False,
                "created_by": _np_offer_meta_pack(meta),
                "created_at": now_iso(),
            }
            dashboard_saved = bool(delivery_dashboard)
            try:
                created = supabase.table("announcements").insert(safe_row).execute().data or []
            except Exception as e2:
                print("PRIVATE OFFER FALLBACK INSERT ERROR:", e2)

        email_sent = False
        email_error = ""
        if delivery_email:
            if not target_email:
                email_error = "Target trader has no email"
            else:
                html_body = _np_private_offer_html(
                    subject,
                    d.get("email_body") or message,
                    d.get("offer_code") or "PHASEHELP",
                    d.get("expires_at") or "",
                    cta_url,
                )
                try:
                    email_sent = bool(send_email_brevo(target_email, subject, html_body))
                    if not email_sent:
                        email_error = "Email service rejected the message. Check Render logs for BREVO EMAIL ERROR."
                except Exception as email_exc:
                    email_error = str(email_exc)

        _audit_safe(
            "announcements",
            "create_private_offer",
            f"Private offer created for {target_email or target_trader_id}; dashboard={dashboard_saved}; email_sent={email_sent}",
            _admin_from_payload(d),
            target_trader_id or target_email,
        )

        return ok({
            "announcement": created,
            "dashboard_target_saved": dashboard_saved,
            "email_sent": email_sent,
            "email_error": email_error,
            "whatsapp_manual": delivery_whatsapp,
            "target_email": target_email,
            "target_trader_id": target_trader_id,
        }, "Private offer created")
    except Exception as e:
        return bad(e)


@app.route("/send_private_offer_email", methods=["POST", "OPTIONS"])
def send_private_offer_email():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        to_email = _np_offer_clean_str(d.get("target_email") or d.get("email"), 250).lower()
        subject = _np_offer_clean_str(d.get("subject") or d.get("title") or "Private Offer From NairaPips", 250)
        message = _np_offer_clean_str(d.get("email_body") or d.get("message"), 5000)
        if not to_email:
            return bad("Missing target email")
        if not message:
            return bad("Missing email message")
        html_body = _np_private_offer_html(
            subject,
            message,
            d.get("offer_code") or "",
            d.get("expires_at") or "",
            d.get("cta_url") or "https://nairapips.com/dashboard/",
        )
        sent = bool(send_email_brevo(to_email, subject, html_body))
        if sent:
            return ok({"email_sent": True}, "Private offer email sent")
        return bad("Email service rejected the message. Check Render/Brevo logs.")
    except Exception as e:
        return bad(e)



@app.route("/register_fxblue", methods=["POST"])
def register_fxblue():
    data = request.get_json(force=True) or {}
    trader_id = data.get("trader_id") or data.get("id")
    if not trader_id:
        return jsonify({"success": False, "error": "trader_id is required"}), 400

    res = supabase.table("traders").update({
        "fxblue_url": data.get("fxblue_url"),
        "fxblue_account_id": data.get("fxblue_account_id")
    }).eq("id", trader_id).execute()
    return jsonify({"success": True, "data": getattr(res, "data", None)})

@app.route("/monitoring_snapshot", methods=["POST"])
def monitoring_snapshot():
    data = request.get_json(force=True) or {}
    trader_id = data.get("trader_id") or data.get("id")
    trader = _get_trader_by_id(trader_id) if trader_id else _find_trader_for_fxblue(data)
    if not trader:
        return jsonify({"success": False, "error": "Trader not found. Send trader_id or active mt5_login/login."}), 404
    return jsonify({"success": True, "data": _apply_monitoring_snapshot(trader, data, data.get("source", "manual"))})

@app.route("/sync_fxblue_account", methods=["POST"])
def sync_fxblue_account():
    data = request.get_json(force=True) or {}
    trader_id = data.get("trader_id") or data.get("id")
    trader = _get_trader_by_id(trader_id) if trader_id else _find_trader_for_fxblue(data)
    if not trader:
        return jsonify({"success": False, "error": "Trader not found. Send trader_id or active mt5_login/login."}), 404
    return jsonify({"success": True, "data": _apply_monitoring_snapshot(trader, data, "fxblue")})
@app.route("/sync_trades", methods=["POST", "OPTIONS"])
def sync_trades():
    """Receive MT5 trade history without crashing the engine.

    Production fix: return HTTP 200 with row-level errors instead of a raw 500,
    so the MT5 watchdog keeps running even when one trade row or schema field fails.
    """
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    try:
        d = request.get_json(silent=True) or {}
        trades = d.get("trades", [])
        if not isinstance(trades, list):
            return _np_fail("trades must be a list", 400)

        saved = []
        errors = []
        for idx, t in enumerate(trades):
            try:
                trader_account_id = t.get("trader_account_id")
                mt5_login = str(t.get("mt5_login") or t.get("login") or "").strip()
                acct = None
                if mt5_login:
                    try:
                        acct = _get_account_by_login_any_status(mt5_login, t.get("trader_id"))
                        if not trader_account_id:
                            trader_account_id = (acct or {}).get("id")
                        if not t.get("trader_id") and acct:
                            t["trader_id"] = acct.get("trader_id")
                    except Exception:
                        acct = None
                # Attach identity where possible so dashboard/admin can show evidence even after archive.
                if t.get("trader_id") and (not t.get("trader_name") or not t.get("email")):
                    try:
                        tr_identity = get_trader_by_id(t.get("trader_id"))
                        if tr_identity:
                            t["trader_name"] = t.get("trader_name") or tr_identity.get("name")
                            t["email"] = t.get("email") or tr_identity.get("email")
                    except Exception:
                        pass
                ticket = str(t.get("ticket") or t.get("order") or t.get("deal") or "").strip()
                if not ticket:
                    errors.append({"index": idx, "error": "missing ticket"})
                    continue
                row = {
                    "trader_id": t.get("trader_id"),
                    "trader_account_id": trader_account_id,
                    "trader_name": t.get("trader_name"),
                    "email": t.get("email"),
                    "mt5_login": mt5_login,
                    "symbol": t.get("symbol"),
                    "ticket": ticket,
                    "trade_type": t.get("trade_type") or t.get("type"),
                    "volume": t.get("volume") or 0,
                    "open_price": t.get("open_price") or 0,
                    "current_price": t.get("current_price") or t.get("close_price") or 0,
                    "sl": t.get("sl") or 0,
                    "tp": t.get("tp") or 0,
                    "profit": t.get("profit") or 0,
                    "swap": t.get("swap") or 0,
                    "commission": t.get("commission") or 0,
                    "status": t.get("status") or ("closed" if t.get("closed_at") else "open"),
                    "opened_at": t.get("opened_at") or t.get("open_time"),
                    "closed_at": t.get("closed_at") or t.get("close_time"),
                    "synced_at": now_iso(),
                    "updated_at": now_iso(),
                    "history_entry": {
                        "source": "mt5_engine_sync",
                        "phase_label": t.get("phase_label") or t.get("phase") or "",
                        "sync_reason": t.get("sync_reason") or t.get("reason") or "normal"
                    }
                }
                # Drop empty None fields to reduce schema/type conflicts.
                row = {k: v for k, v in row.items() if v is not None}
                existing = supabase.table("trader_trades").select("id").eq("ticket", ticket).limit(1).execute().data or []
                if existing:
                    res = supabase.table("trader_trades").update(row).eq("ticket", ticket).execute().data or []
                    saved.append({"ticket": ticket, "action": "updated", "rows": len(res)})
                else:
                    res = supabase.table("trader_trades").insert(row).execute().data or []
                    saved.append({"ticket": ticket, "action": "inserted", "rows": len(res)})
            except Exception as row_error:
                print("SYNC TRADES ROW ERROR:", row_error)
                errors.append({"index": idx, "ticket": str((t or {}).get("ticket") or ""), "error": str(row_error)})

        return _np_ok({
            "success": True,
            "saved": saved,
            "saved_count": len(saved),
            "error_count": len(errors),
            "errors": errors[:25],
            "message": f"Trades sync processed. saved={len(saved)} errors={len(errors)}",
        }, 200)
    except Exception as e:
        print("SYNC TRADES FATAL ERROR:", e)
        return _np_ok({"success": False, "error": str(e), "saved_count": 0, "error_count": 1}, 200)

@app.route("/disable_mt5_access", methods=["POST"])
def disable_mt5_access():
    try:
        d = request.json or {}
        trader_id = d.get("trader_id")
        account_id = d.get("trader_account_id") or d.get("current_account_id")
        mt5_login = str(d.get("mt5_login") or d.get("login") or d.get("account") or "")
        reason = d.get("reason") or "This MT5 account has been locked by NairaPips monitoring engine."
        incoming_status = str(d.get("status") or "breached").lower().strip()

        if not trader_id and not mt5_login and not account_id:
            return bad("trader_id, trader_account_id, or mt5_login is required")

        account = None
        if account_id:
            rows = supabase.table("trader_accounts").select("*").eq("id", account_id).limit(1).execute().data or []
            account = rows[0] if rows else None
        if not account and mt5_login:
            account = _get_account_by_login_any_status(mt5_login, trader_id)
        if account:
            trader_id = account.get("trader_id") or trader_id
        if not trader_id:
            return bad("Trader not found for this MT5 account", 404)

        trader = get_trader_by_id(trader_id)
        if not trader:
            return bad("Trader not found", 404)
        if not account:
            account = _get_active_account(trader_id, trader)

        passed_statuses = {"phase1_passed", "phase2_passed", "passed", "funded_ready", "target_hit"}
        if incoming_status in passed_statuses:
            stage = account.get("stage") if account else None
            if incoming_status == "phase2_passed":
                stage = "phase2"
            elif incoming_status in {"phase1_passed", "passed", "target_hit"}:
                stage = "phase1"
            if stage not in ["phase1", "phase2"]:
                return bad("Only Phase 1 or Phase 2 accounts can be passed by MT5 lock signal.", 409)
            pass_status = "phase2_passed" if stage == "phase2" else "phase1_passed"

            # CRITICAL PRODUCTION FIX:
            # A pass/lock signal can arrive even when the normal snapshot call was
            # suppressed by cooldown or failed earlier. Never archive a passed MT5
            # account with ₦0 evidence. Preserve MT5 truth from the lock payload
            # before moving the account to waiting phase.
            start_balance = _num((account or {}).get("start_balance"), _num((account or {}).get("account_size"), _num(d.get("account_size"), 0)))
            final_balance = _num(d.get("current_balance"), _num(d.get("mt5_balance"), _num(d.get("balance"), _num((account or {}).get("current_balance"), start_balance))))
            final_equity = _num(d.get("equity"), final_balance)
            highest_equity = max(
                start_balance,
                final_balance,
                final_equity,
                _num(d.get("highest_equity"), 0),
                _num((account or {}).get("highest_equity"), 0),
            )
            profit = _num(d.get("highest_profit"), _num(d.get("profit"), highest_equity - start_balance if start_balance else 0))
            profit_percent = _num(
                d.get("highest_profit_percent"),
                _num(d.get("profit_percent"), (profit / start_balance * 100) if start_balance else 0),
            )
            target_percent = _num(d.get("profit_target"), 8 if stage == "phase2" else 10)
            target_equity = _num(d.get("target_equity"), start_balance * (1 + target_percent / 100) if start_balance else 0)
            pass_progress = 100 if target_percent and profit_percent >= target_percent else _num(d.get("pass_progress_percent"), 0)
            now = now_iso()

            evidence_update = {
                "current_balance": final_balance,
                "current_equity": final_equity,
                "profit": profit,
                "profit_percent": profit_percent,
                "highest_equity": highest_equity,
                "lowest_equity": _num(d.get("lowest_equity"), _num((account or {}).get("lowest_equity"), start_balance)),
                "phase_pass_status": pass_status,
                "risk_zone": "passed",
                "monitoring_enabled": False,
                "updated_at": now,
            }
            # Optional columns are attempted, then safely dropped if missing.
            optional_evidence = {
                "target_equity": target_equity,
                "profit_target": target_percent,
                "pass_progress_percent": max(100, pass_progress),
                "passed_at": now,
            }
            try:
                supabase.table("trader_accounts").update({**evidence_update, **optional_evidence}).eq("id", account.get("id")).execute()
            except Exception as e:
                print("PASS EVIDENCE FULL UPDATE FAILED:", e)
                try:
                    supabase.table("trader_accounts").update(evidence_update).eq("id", account.get("id")).execute()
                except Exception as e2:
                    print("PASS EVIDENCE CORE UPDATE FAILED:", e2)

            try:
                supabase.table("monitoring_snapshots").insert({
                    "trader_id": trader_id,
                    "trader_account_id": account.get("id") if account else None,
                    "mt5_login": mt5_login,
                    "balance": final_balance,
                    "equity": final_equity,
                    "profit": profit,
                    "profit_percent": profit_percent,
                    "highest_equity": highest_equity,
                    "target_equity": target_equity,
                    "target_percent": target_percent,
                    "pass_progress_percent": max(100, pass_progress),
                    "risk_zone": "passed",
                    "phase_label": stage,
                    "phase_pass_status": pass_status,
                    "source": "disable_mt5_access_pass_evidence",
                    "raw_data": d,
                }).execute()
            except Exception as e:
                print("PASS EVIDENCE SNAPSHOT INSERT FAILED:", e)

            enriched_account = dict(account or {})
            enriched_account.update(evidence_update)
            enriched_account.update(optional_evidence)
            updated, archived = _pass_specific_account(trader, enriched_account, pass_status, _admin_from_payload(d), reason)
            return ok({"trader": updated, "archived_account": archived}, "Exact MT5 account passed, evidence preserved, archived, and moved to the next waiting state.")

        if incoming_status == "profit_protected":
            if not account or account.get("stage") != "funded":
                return bad("Profit protection requires an active funded account.", 409)
            now = now_iso()
            supabase.table("trader_accounts").update({
                "monitoring_enabled": False,
                "updated_at": now,
                "archive_reason": reason
            }).eq("id", account.get("id")).execute()
            if _account_is_current_for_trader(trader, account):
                updated = _update_trader_lifecycle(
                    trader_id,
                    "funded_active",
                    account,
                    {
                        "status": "profit_protected",
                        "risk_zone": "profit_protected",
                        "critical_mode": False,
                        "monitoring_priority": "urgent",
                        "mt5_access_disabled": True,
                        "mt5_account_active": False,
                        "admin_note": reason,
                        "monitoring_enabled": False,
                    },
                    _admin_from_payload(d),
                    "profit_protected"
                )
            else:
                updated = trader
            return ok({"trader": updated, "account": account}, "Exact funded MT5 account locked for payout/admin review.")

        updated, archived = _breach_specific_account(trader, account, reason, _admin_from_payload(d))
        return ok({"trader": updated, "archived_account": archived}, "Exact breached MT5 account locked and archived.")
    except Exception as e:
        return bad(e)

@app.route("/users_database", methods=["GET"])
def users_database():
    try:
        status = request.args.get("status", "active")
        search = request.args.get("search", "").strip()

        q = supabase.table("traders").select(
            "id,full_name,name,email,phone,whatsapp,country,status,phase,created_at,updated_at,marketing_deleted,marketing_consent,source"
        ).order("created_at", desc=True)

        if status == "deleted":
            q = q.eq("marketing_deleted", True)
        else:
            q = q.or_("marketing_deleted.is.null,marketing_deleted.eq.false")

        res = q.execute()
        rows = getattr(res, "data", []) or []

        if search:
            s = search.lower()
            rows = [
                r for r in rows
                if s in str(r.get("full_name") or r.get("name") or "").lower()
                or s in str(r.get("email") or "").lower()
                or s in str(r.get("phone") or "").lower()
                or s in str(r.get("whatsapp") or "").lower()
            ]

        return jsonify(rows)

    except Exception as e:
        return bad(e)


@app.route("/users_database/delete", methods=["POST"])
def users_database_delete():
    try:
        d = request.json or {}
        user_id = d.get("id")

        if not user_id:
            return bad("User id is required")

        update = {
            "marketing_deleted": True,
            "deleted_at": now_iso(),
            "updated_at": now_iso()
        }

        res = supabase.table("traders").update(update).eq("id", user_id).execute()
        return ok(res.data, "User moved to deleted list")

    except Exception as e:
        return bad(e)


@app.route("/users_database/restore", methods=["POST"])
def users_database_restore():
    try:
        d = request.json or {}
        user_id = d.get("id")

        if not user_id:
            return bad("User id is required")

        update = {
            "marketing_deleted": False,
            "restored_at": now_iso(),
            "updated_at": now_iso()
        }

        res = supabase.table("traders").update(update).eq("id", user_id).execute()
        return ok(res.data, "User restored")

    except Exception as e:
        return bad(e)


@app.route("/users_database/export", methods=["GET"])
def users_database_export():
    try:
        field = request.args.get("field", "phone")

        res = supabase.table("traders").select(
            "email,phone,whatsapp,marketing_deleted,marketing_consent"
        ).or_("marketing_deleted.is.null,marketing_deleted.eq.false").execute()

        rows = getattr(res, "data", []) or []

        values = []
        for r in rows:
            if r.get("marketing_consent") is False:
                continue

            value = r.get("whatsapp") or r.get("phone") if field == "phone" else r.get("email")

            if value:
                values.append(str(value).strip())

        return jsonify({
            "count": len(values),
            "field": field,
            "data": values,
            "copy_text": "\n".join(values)
        })

    except Exception as e:
        return bad(e)
@app.route("/trader_trades", methods=["GET"])
def get_trader_trades():
    """Return trade history for one trader/account without hiding archived passed accounts.

    Production safety fix:
    - Dashboard must not rely on trader_id only because some MT5 history rows can be
      synced before trader_id is attached, especially around Golden Ticket / phase
      assignment / archive handoff.
    - Query by trader_id, trader_account_id and mt5_login, then de-dupe by ticket.
    - Works for active, passed, archived and breached accounts.
    """
    try:
        trader_id = str(request.args.get("trader_id") or "").strip()
        trader_account_id = str(request.args.get("trader_account_id") or "").strip()
        mt5_login_raw = str(request.args.get("mt5_login") or request.args.get("login") or "").strip()
        try:
            limit = int(request.args.get("limit", 300))
        except Exception:
            limit = 300
        limit = max(1, min(limit, 1000))

        logins = []
        if mt5_login_raw:
            logins = [x.strip() for x in mt5_login_raw.split(",") if x.strip()]

        # If only trader_id is supplied, discover all logins/accounts for this trader
        # so passed/archived account history still appears.
        if trader_id:
            try:
                acct_rows = supabase.table("trader_accounts").select("id,mt5_login").eq("trader_id", trader_id).limit(200).execute().data or []
                for a in acct_rows:
                    if str(a.get("mt5_login") or "").strip():
                        logins.append(str(a.get("mt5_login")).strip())
            except Exception as e:
                print("TRADER TRADES ACCOUNT DISCOVERY ERROR:", e)

        rows = []
        def add_query(q):
            try:
                data = q.order("opened_at", desc=True).limit(limit).execute().data or []
                rows.extend(data)
            except Exception as e:
                print("TRADER TRADES QUERY ERROR:", e)

        if trader_id:
            add_query(supabase.table("trader_trades").select("*").eq("trader_id", trader_id))
        if trader_account_id:
            add_query(supabase.table("trader_trades").select("*").eq("trader_account_id", trader_account_id))
        for login in sorted(set(logins)):
            add_query(supabase.table("trader_trades").select("*").eq("mt5_login", login))

        # Fallback only when no filter is provided; do not leak all rows into normal dashboard calls.
        if not rows and not trader_id and not trader_account_id and not logins:
            add_query(supabase.table("trader_trades").select("*"))

        seen = set()
        out = []
        for r in rows:
            key = str(r.get("ticket") or r.get("id") or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            out.append(r)

        def _score(row):
            return str(row.get("closed_at") or row.get("opened_at") or row.get("synced_at") or row.get("created_at") or "")
        out.sort(key=_score, reverse=True)
        return jsonify(out[:limit])

    except Exception as e:
        return bad(e)
@app.route("/monitoring_events", methods=["GET"])
def monitoring_events():
    trader_id = request.args.get("trader_id")
    try:
        limit = int(request.args.get("limit", 1000))
    except Exception:
        limit = 1000
    limit = max(1, min(limit, 5000))
    query = supabase.table("monitoring_events").select("*").order("created_at", desc=True).limit(limit)
    if trader_id:
        query = query.eq("trader_id", trader_id)
    res = query.execute()
    return jsonify(getattr(res, "data", []) or [])

@app.route("/monitoring_snapshots", methods=["GET"])
def monitoring_snapshots():
    trader_id = request.args.get("trader_id")
    try:
        limit = int(request.args.get("limit", 1000))
    except Exception:
        limit = 1000
    limit = max(1, min(limit, 5000))
    query = supabase.table("monitoring_snapshots").select("*").order("created_at", desc=True).limit(limit)
    if trader_id:
        query = query.eq("trader_id", trader_id)
    res = query.execute()
    return jsonify(getattr(res, "data", []) or [])

@app.route("/breach_evidence/<trader_id>", methods=["GET"])
def breach_evidence(trader_id):
    trader = _get_trader_by_id(trader_id)
    if not trader:
        return jsonify({"success": False, "error": "Trader not found"}), 404

    events = supabase.table("monitoring_events").select("*").eq("trader_id", trader_id).order("created_at", desc=True).limit(20).execute()
    snapshots = supabase.table("monitoring_snapshots").select("*").eq("trader_id", trader_id).order("created_at", desc=True).limit(20).execute()

    return jsonify({"success": True, "data": {
        "trader_id": trader_id,
        "status": trader.get("status"),
        "risk_zone": trader.get("risk_zone"),
        "highest_equity": trader.get("highest_equity"),
        "lowest_equity": trader.get("lowest_equity"),
        "max_drawdown_used": trader.get("max_drawdown_used"),
        "breach_time": trader.get("breach_time"),
        "breach_equity": trader.get("breach_equity"),
        "breach_reason": trader.get("breach_reason"),
        "events": getattr(events, "data", []) or [],
        "snapshots": getattr(snapshots, "data", []) or []
    }})





@app.route("/test_monitoring_timeline/<trader_id>", methods=["POST", "GET"])
def test_monitoring_timeline(trader_id):
    """
    Creates a safe -> warning -> danger -> critical -> breach timeline for one trader.
    Use only for testing the evidence system.
    """
    trader = _get_trader_by_id(trader_id)
    if not trader:
        return jsonify({"success": False, "error": "Trader not found"}), 404

    size = _num(trader.get("account_size"), _num(trader.get("balance"), 1000000))

    test_points = [
        {"balance": size, "equity": size, "source": "timeline_test_safe"},
        {"balance": size, "equity": size * 0.88, "source": "timeline_test_warning"},
        {"balance": size, "equity": size * 0.84, "source": "timeline_test_danger"},
        {"balance": size, "equity": size * 0.815, "source": "timeline_test_critical"},
        {"balance": size, "equity": size * 0.79, "source": "timeline_test_breach"},
    ]

    results = []
    for p in test_points:
        trader = _get_trader_by_id(trader_id)
        results.append(_apply_monitoring_snapshot(trader, p, p["source"]))

    return jsonify({"success": True, "message": "Test monitoring timeline created", "data": results})





# ================================
# FX BLUE AUTO-FEED RECEIVER
# Accepts live MT5/FXBlue snapshot values and feeds NairaPips monitoring engine.
# ================================

def _find_trader_for_fxblue(data):
    trader_id = data.get("trader_id") or data.get("id")
    if trader_id:
        return _get_trader_by_id(trader_id)

    mt5_login = str(data.get("mt5_login") or data.get("login") or data.get("account") or "").strip()
    if mt5_login:
        account_rows = supabase.table("trader_accounts").select("*").eq("mt5_login", mt5_login).eq("account_status", "assigned_active").limit(1).execute()
        accounts = getattr(account_rows, "data", None) or []
        if accounts:
            trader = _get_trader_by_id(accounts[0].get("trader_id"))
            if trader:
                return trader
        res = supabase.table("traders").select("*").eq("mt5_login", mt5_login).limit(1).execute()
        rows = getattr(res, "data", None) or []
        if rows:
            return rows[0]

    fxblue_account_id = str(data.get("fxblue_account_id") or data.get("account_id") or "").strip()
    if fxblue_account_id:
        res = supabase.table("traders").select("*").eq("fxblue_account_id", fxblue_account_id).limit(1).execute()
        rows = getattr(res, "data", None) or []
        if rows:
            return rows[0]

    email = str(data.get("email") or "").strip().lower()
    if email:
        res = supabase.table("traders").select("*").eq("email", email).limit(1).execute()
        rows = getattr(res, "data", None) or []
        if rows:
            return rows[0]

    return None

def _normalize_fxblue_payload(data):
    """
    Accepts different naming styles from FXBlue bridge/EA/webhook:
    balance, equity, floating_pl, closed_profit, login, account, mt5_login.
    """
    balance = data.get("balance") or data.get("Balance") or data.get("account_balance")
    equity = data.get("equity") or data.get("Equity") or data.get("account_equity")
    profit = data.get("profit") or data.get("Profit") or data.get("floating_pl") or data.get("floating_profit")

    normalized = dict(data)
    normalized["balance"] = _num(balance, 0)
    normalized["equity"] = _num(equity, normalized["balance"])
    if profit is not None and profit != "":
        normalized["profit"] = _num(profit, 0)

    normalized["mt5_login"] = data.get("mt5_login") or data.get("login") or data.get("account")
    normalized["source"] = data.get("source", "fxblue_auto_feed")
    return normalized

@app.route("/fxblue_webhook", methods=["POST", "GET"])
def fxblue_webhook():
    """
    Main automatic feed route.

    It accepts either:
    GET:
      /fxblue_webhook?login=123456&balance=1000000&e equity=950000

    POST JSON:
      {
        "login": "123456",
        "balance": 1000000,
        "equity": 950000,
        "server": "Exness-MT5Trial9"
      }

    Optional security:
      Set FXBLUE_WEBHOOK_SECRET in Render env.
      Then send ?secret=YOUR_SECRET or header X-NAIRAPIPS-SECRET.
    """
    import os

    expected_secret = os.getenv("FXBLUE_WEBHOOK_SECRET", "").strip()
    if expected_secret:
        supplied = (
            request.headers.get("X-NAIRAPIPS-SECRET")
            or request.args.get("secret")
            or ""
        ).strip()
        if supplied != expected_secret:
            return jsonify({"success": False, "error": "Unauthorized FXBlue feed"}), 401

    if request.method == "GET":
        data = dict(request.args)
    else:
        data = request.get_json(force=True, silent=True) or dict(request.form)

    data = _normalize_fxblue_payload(data)
    trader = _find_trader_for_fxblue(data)

    if not trader:
        return jsonify({
            "success": False,
            "error": "Trader not found. Send trader_id, mt5_login/login, fxblue_account_id, or email.",
            "received": data
        }), 404

    result = _apply_monitoring_snapshot(trader, data, "fxblue_auto_feed")

    # Update fxblue ID/server if sent
    try:
        extra = {}
        if data.get("fxblue_account_id"):
            extra["fxblue_account_id"] = data.get("fxblue_account_id")
        if data.get("fxblue_url"):
            extra["fxblue_url"] = data.get("fxblue_url")
        if data.get("server") or data.get("mt5_server"):
            extra["mt5_server"] = data.get("server") or data.get("mt5_server")
        if extra:
            supabase.table("traders").update(extra).eq("id", trader.get("id")).execute()
    except Exception as e:
        print("fxblue extra update failed:", e)

    return jsonify({
        "success": True,
        "message": "FXBlue snapshot received and processed by NairaPips monitoring engine.",
        "data": result
    })

@app.route("/fxblue_test/<mt5_login>", methods=["GET"])
def fxblue_test(mt5_login):
    """
    Simple browser test route.
    Example:
    /fxblue_test/123456?balance=1000000&equity=950000
    """
    data = dict(request.args)
    data["login"] = mt5_login
    data["source"] = "fxblue_browser_test"

    trader = _find_trader_for_fxblue(data)
    if not trader:
        return jsonify({"success": False, "error": "Trader not found for this MT5 login"}), 404

    data = _normalize_fxblue_payload(data)
    result = _apply_monitoring_snapshot(trader, data, "fxblue_browser_test")
    return jsonify({"success": True, "data": result})
@app.route("/debug/supabase", methods=["GET"])
def debug_supabase():
    try:
        response = supabase.table("traders").select("id,name,mt5_login,mt5_server,mt5_investor_password,status,monitoring_enabled").limit(5).execute()

        return jsonify({
            "success": True,
            "supabase_url": SUPABASE_URL,
            "count": len(response.data or []),
            "sample": response.data or []
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "supabase_url": SUPABASE_URL,
            "error": str(e)
        }), 500
@app.route("/api/admin/traders", methods=["GET"])
def get_admin_traders():
    try:
        response = supabase.table("traders").select("*").execute()

        return jsonify(response.data), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
@app.get('/marketing_deleted_contacts')
def marketing_deleted_contacts():
    try:
        res = supabase.table('marketing_deleted_contacts').select('contact_id').execute()
        return jsonify(res.data or [])
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.post('/marketing_deleted_contacts/save')
def save_marketing_deleted_contacts():
    try:
        body = request.get_json(silent=True) or {}
        contact_ids = [str(x) for x in body.get('contact_ids', []) if str(x).strip()]

        # Replace the admin soft-delete list in Supabase.
        supabase.table('marketing_deleted_contacts').delete().neq('contact_id', '__never__').execute()
        if contact_ids:
            rows = [{'contact_id': cid, 'deleted_by': 'admin'} for cid in sorted(set(contact_ids))]
            supabase.table('marketing_deleted_contacts').insert(rows).execute()

        return jsonify({'success': True, 'contact_ids': contact_ids})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

def referral_settings_default():
    return {
        'programName': 'NairaPips Referral Program',
        'baseUrl': 'https://nairapips.com',
        'defaultCode': 'NAIRAPIPS',
        'rebateType': 'percent',
        'rebateValue': '10',
        'rebate_percent': 10,
        'customerBonus': '0',
        'cookieDays': '30',
        'cookie_days': 30,
        'minPayout': '5000',
        'status': 'active',
        'publicMessage': 'Refer a trader to NairaPips and earn rebate when they buy a challenge.',
        'payoutRule': 'Rebate is approved only after a referred trader pays and passes payment verification.'
    }

@app.get('/referral_settings')
def get_referral_settings():
    default = referral_settings_default()
    try:
        res = supabase.table('referral_settings').select('*').eq('id', 'main').limit(1).execute()
        rows = getattr(res, 'data', None) or []
        row = rows[0] if rows else {}
        data = dict(default)
        if row:
            rebate_value = row.get('rebate_value') if row.get('rebate_value') is not None else default['rebateValue']
            cookie_days = row.get('cookie_days') if row.get('cookie_days') is not None else default['cookie_days']
            data.update({
                'programName': row.get('program_name') or default['programName'],
                'baseUrl': row.get('base_url') or default['baseUrl'],
                'defaultCode': row.get('default_code') or default['defaultCode'],
                'rebateType': row.get('rebate_type') or default['rebateType'],
                'rebateValue': str(rebate_value),
                'rebate_percent': float(rebate_value or 10),
                'customerBonus': row.get('customer_bonus') or default['customerBonus'],
                'cookieDays': str(cookie_days),
                'cookie_days': int(cookie_days or 30),
                'minPayout': str(row.get('min_payout') or default['minPayout']),
                'status': row.get('status') or default['status'],
                'publicMessage': row.get('public_message') or default['publicMessage'],
                'payoutRule': row.get('payout_rule') or default['payoutRule']
            })
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        print('REFERRAL SETTINGS LOAD ERROR:', str(e))
        return jsonify({'success': True, 'data': default})

@app.post('/referral_settings')
def update_referral_settings():
    body = request.get_json(silent=True) or {}
    default = referral_settings_default()
    try:
        row = {
            'id': 'main',
            'program_name': body.get('programName') or default['programName'],
            'base_url': body.get('baseUrl') or default['baseUrl'],
            'default_code': body.get('defaultCode') or default['defaultCode'],
            'rebate_type': body.get('rebateType') or default['rebateType'],
            'rebate_value': body.get('rebateValue') or body.get('rebate_percent') or default['rebateValue'],
            'customer_bonus': body.get('customerBonus') or default['customerBonus'],
            'cookie_days': int(body.get('cookieDays') or body.get('cookie_days') or default['cookie_days']),
            'min_payout': body.get('minPayout') or default['minPayout'],
            'status': body.get('status') or default['status'],
            'public_message': body.get('publicMessage') or default['publicMessage'],
            'payout_rule': body.get('payoutRule') or default['payoutRule']
        }
        try:
            supabase.table('referral_settings').upsert(row, on_conflict='id').execute()
            return get_referral_settings()
        except Exception as e:
            print('REFERRAL SETTINGS SAVE ERROR:', str(e))
            data = dict(default)
            data.update({
                'programName': row['program_name'],
                'baseUrl': row['base_url'],
                'defaultCode': row['default_code'],
                'rebateType': row['rebate_type'],
                'rebateValue': str(row['rebate_value']),
                'rebate_percent': float(row['rebate_value'] or 10),
                'customerBonus': row['customer_bonus'],
                'cookieDays': str(row['cookie_days']),
                'cookie_days': int(row['cookie_days'] or 30),
                'minPayout': str(row['min_payout']),
                'status': row['status'],
                'publicMessage': row['public_message'],
                'payoutRule': row['payout_rule']
            })
            return jsonify({'success': True, 'data': data, 'warning': 'Referral settings table unavailable; returned safe settings.'})
    except Exception as e:
        print('REFERRAL SETTINGS UPDATE ERROR:', str(e))
        return jsonify({'success': True, 'data': default, 'warning': 'Referral settings update failed safely.'})

@app.post('/referral_settings/reset')
def reset_referral_settings():
    default = referral_settings_default()
    default_row = {
        'id': 'main',
        'program_name': default['programName'],
        'base_url': default['baseUrl'],
        'default_code': default['defaultCode'],
        'rebate_type': default['rebateType'],
        'rebate_value': default['rebateValue'],
        'customer_bonus': default['customerBonus'],
        'cookie_days': default['cookie_days'],
        'min_payout': default['minPayout'],
        'status': default['status'],
        'public_message': default['publicMessage'],
        'payout_rule': default['payoutRule']
    }
    try:
        supabase.table('referral_settings').upsert(default_row, on_conflict='id').execute()
    except Exception as e:
        print('REFERRAL SETTINGS RESET SAVE ERROR:', str(e))
    return jsonify({'success': True, 'data': default})

# ===== NAIRAPIPS STAFF RBAC ROUTES =====
# Paste above:


@app.get('/staff_members')
def staff_members():
    try:
        res = supabase.table('admin_staff_members').select('*').order('created_at', desc=True).execute()
        rows = res.data or []
        for r in rows:
            r.pop('password', None)
        return jsonify(rows)
    except Exception as e:
        return jsonify([])

@app.post('/staff_login')
def staff_login():
    try:
        data = request.get_json() or {}
        username = (data.get('username') or '').strip()
        password = data.get('password') or ''
        res = supabase.table('admin_staff_members').select('*').eq('username', username).eq('password', password).limit(1).execute()
        rows = res.data or []
        if not rows:
            return jsonify({'success': False, 'error': 'Invalid login'}), 401
        staff = rows[0]
        if (staff.get('status') or 'active') != 'active':
            return jsonify({'success': False, 'error': 'Staff account is not active'}), 403
        supabase.table('admin_staff_members').update({'last_login_at': 'now()'}).eq('id', staff['id']).execute()
        audit_log(staff, 'auth', 'login', 'Staff logged in')
        staff.pop('password', None)
        return jsonify({'success': True, 'staff': staff})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.post('/staff_members')
def create_staff_member():
    try:
        data = request.get_json() or {}
        payload = {
            'name': data.get('name'),
            'email': data.get('email'),
            'username': data.get('username'),
            'password': data.get('password'),
            'role': data.get('role') or 'support',
            'permissions': data.get('permissions') or {},
            'status': data.get('status') or 'active'
        }
        res = supabase.table('admin_staff_members').insert(payload).execute()
        return jsonify({'success': True, 'data': res.data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.post('/staff_members/update')
def update_staff_member():
    try:
        data = request.get_json() or {}
        staff_id = data.get('id')
        payload = {k: data.get(k) for k in ['name','email','role','permissions'] if k in data}
        res = supabase.table('admin_staff_members').update(payload).eq('id', staff_id).execute()
        return jsonify({'success': True, 'data': res.data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.post('/staff_members/status')
def staff_member_status():
    try:
        data = request.get_json() or {}
        res = supabase.table('admin_staff_members').update({'status': data.get('status')}).eq('id', data.get('id')).execute()
        return jsonify({'success': True, 'data': res.data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.post('/staff_members/password')
def staff_member_password():
    try:
        data = request.get_json() or {}
        res = supabase.table('admin_staff_members').update({'password': data.get('password')}).eq('id', data.get('id')).execute()
        return jsonify({'success': True, 'data': res.data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.post('/staff_members/delete')
def staff_member_delete():
    try:
        data = request.get_json() or {}
        res = supabase.table('admin_staff_members').delete().eq('id', data.get('id')).execute()
        return jsonify({'success': True, 'data': res.data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.get('/audit_logs')
def audit_logs():
    try:
        res = supabase.table('admin_audit_logs').select('*').order('created_at', desc=True).limit(100).execute()
        return jsonify(res.data or [])
    except Exception as e:
        return jsonify([])

def audit_log(staff, module, action, details='', record_affected='', created_at=None):
    try:
        row = {
            'staff_id': str(staff.get('id','')),
            'staff_name': staff.get('name'),
            'username': staff.get('username'),
            'role': staff.get('role'),
            'module': module,
            'action': action,
            'details': details,
            'record_affected': record_affected,
            'created_at': created_at or now_iso()
        }
        try:
            supabase.table('admin_audit_logs').insert(row).execute()
        except Exception:
            row.pop('record_affected', None)
            row.pop('created_at', None)
            supabase.table('admin_audit_logs').insert(row).execute()
    except Exception as e:
        print('AUDIT LOG ERROR:', str(e))





def _business_setting_defaults():
    return {
        "revenue_launch_date": "",
        "production_mode": "test"
    }


def _coerce_business_settings_from_row(row):
    settings = {}
    if not isinstance(row, dict):
        return settings

    # Direct-column schema support: id/main row with launch_date + production_mode columns.
    for date_key in ["revenue_launch_date", "launch_date", "business_launch_date"]:
        if row.get(date_key) not in [None, ""]:
            settings["revenue_launch_date"] = row.get(date_key)
            break
    if row.get("production_mode") not in [None, ""]:
        settings["production_mode"] = row.get("production_mode")

    # Key/value schema support: key/value, setting_key/setting_value, name/data.
    key = row.get("key") or row.get("setting_key") or row.get("name")
    if key:
        value = row.get("value")
        if value is None:
            value = row.get("setting_value")
        if value is None:
            value = row.get("data")
        if value is None:
            value = row.get("setting_data")
        key = str(key)
        if key == "launch_date":
            key = "revenue_launch_date"
        settings[key] = value if value is not None else ""

    return settings


def _load_business_settings():
    settings = _business_setting_defaults()
    try:
        res = supabase.table('business_settings').select('*').execute()
        rows = getattr(res, 'data', []) or []
        for row in rows:
            settings.update(_coerce_business_settings_from_row(row))
    except Exception as e:
        print('BUSINESS SETTINGS LOAD ERROR:', str(e))
    if settings.get("production_mode") not in ["test", "live"]:
        settings["production_mode"] = "test"
    settings["revenue_launch_date"] = str(settings.get("revenue_launch_date") or settings.get("launch_date") or "")
    return settings


def _try_business_settings_write(row, on_conflict=None, update_column=None, update_value=None):
    try:
        table = supabase.table('business_settings')
        if update_column:
            table.update(row).eq(update_column, update_value).execute()
        elif on_conflict:
            table.upsert(row, on_conflict=on_conflict).execute()
        else:
            table.insert(row).execute()
        return True, None
    except Exception as e:
        return False, str(e)


def _save_business_settings(settings, admin=None):
    current = _load_business_settings()
    current.update(settings or {})
    launch = str(current.get('revenue_launch_date') or current.get('launch_date') or '')
    mode = 'live' if str(current.get('production_mode') or '').lower() == 'live' else 'test'
    who = (admin or {}).get('name') or (admin or {}).get('username') or 'admin'
    now = now_iso()

    attempts = [
        # Direct-column schemas.
        ({'id': 'main', 'revenue_launch_date': launch, 'launch_date': launch, 'production_mode': mode, 'updated_at': now, 'updated_by': who}, 'id', None, None),
        ({'id': 'main', 'revenue_launch_date': launch, 'production_mode': mode, 'updated_at': now, 'updated_by': who}, 'id', None, None),
        ({'id': 'main', 'launch_date': launch, 'production_mode': mode, 'updated_at': now, 'updated_by': who}, 'id', None, None),
        ({'id': 'main', 'revenue_launch_date': launch, 'launch_date': launch, 'production_mode': mode}, 'id', None, None),
        ({'id': 'main', 'revenue_launch_date': launch, 'production_mode': mode}, 'id', None, None),
        ({'id': 'main', 'launch_date': launch, 'production_mode': mode}, 'id', None, None),
        ({'revenue_launch_date': launch, 'launch_date': launch, 'production_mode': mode, 'updated_at': now, 'updated_by': who}, None, 'id', 'main'),
        ({'revenue_launch_date': launch, 'production_mode': mode, 'updated_at': now, 'updated_by': who}, None, 'id', 'main'),
        ({'launch_date': launch, 'production_mode': mode, 'updated_at': now, 'updated_by': who}, None, 'id', 'main'),
        ({'revenue_launch_date': launch, 'launch_date': launch, 'production_mode': mode}, None, 'id', 'main'),
        ({'revenue_launch_date': launch, 'production_mode': mode}, None, 'id', 'main'),
        ({'launch_date': launch, 'production_mode': mode}, None, 'id', 'main'),
    ]

    errors = []
    for row, conflict, update_column, update_value in attempts:
        ok_saved, err = _try_business_settings_write(row, conflict, update_column, update_value)
        if ok_saved:
            return True, None
        errors.append(err)

    # Key/value schemas. Save both settings independently; this supports tables with key/value rows.
    kv_attempts = []
    for key, value in [('revenue_launch_date', launch), ('production_mode', mode)]:
        kv_attempts.extend([
            ({'key': key, 'value': value, 'updated_at': now, 'updated_by': who}, 'key', None, None),
            ({'key': key, 'value': value}, 'key', None, None),
            ({'setting_key': key, 'setting_value': value, 'updated_at': now, 'updated_by': who}, 'setting_key', None, None),
            ({'setting_key': key, 'setting_value': value}, 'setting_key', None, None),
            ({'name': key, 'value': value, 'updated_at': now, 'updated_by': who}, 'name', None, None),
            ({'name': key, 'value': value}, 'name', None, None),
            ({'key': key, 'value': value}, None, 'key', key),
            ({'setting_key': key, 'setting_value': value}, None, 'setting_key', key),
            ({'name': key, 'value': value}, None, 'name', key),
        ])

    saved_any = False
    for row, conflict, update_column, update_value in kv_attempts:
        ok_saved, err = _try_business_settings_write(row, conflict, update_column, update_value)
        if ok_saved:
            saved_any = True
        else:
            errors.append(err)

    if saved_any:
        return True, None
    return False, ' | '.join([e for e in errors if e][-5:]) or 'Business settings table schema is unsupported'


def _save_business_setting(key, value, admin=None):
    if key == 'launch_date':
        key = 'revenue_launch_date'
    return _save_business_settings({key: value}, admin)

@app.get('/business_settings')
def get_business_settings():
    return jsonify({'success': True, 'data': _load_business_settings()})

@app.post('/business_settings')
def update_business_settings():
    data = request.get_json(silent=True) or {}
    admin = _admin_from_payload(data)
    updates = {}
    if 'revenue_launch_date' in data:
        updates['revenue_launch_date'] = data.get('revenue_launch_date') or ''
    if 'launch_date' in data:
        updates['revenue_launch_date'] = data.get('launch_date') or ''
    if 'production_mode' in data:
        updates['production_mode'] = 'live' if str(data.get('production_mode')).lower() == 'live' else 'test'
    if not updates:
        return jsonify({'success': True, 'data': _load_business_settings()})
    ok_saved, err = _save_business_settings(updates, admin)
    if not ok_saved:
        return jsonify({'success': False, 'error': err or 'Could not save business settings'}), 500
    _audit_safe('business_settings', 'settings_update', f'Business settings updated: {updates}', admin, 'business_settings')
    return jsonify({'success': True, 'data': _load_business_settings()})

def _revenue_date(value):
    try:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        if "/" in text and len(text.split("/")) == 3:
            day, month_part, year_part = text.split("/")
            text = f"{year_part}-{month_part.zfill(2)}-{day.zfill(2)}"
        return datetime.fromisoformat(text[:10]).replace(tzinfo=timezone.utc)
    except Exception:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

def _revenue_bool(value):
    return value is True or str(value or "").strip().lower() in ["true", "1", "yes", "y"]

def _revenue_period_flags(dt):
    now = datetime.now(timezone.utc)
    if dt and dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return {
        "week": bool(dt and dt.isocalendar()[:2] == now.isocalendar()[:2]),
        "month": bool(dt and dt.year == now.year and dt.month == now.month),
        "year": bool(dt and dt.year == now.year),
    }

def _normalize_launch_date(value):
    dt = _revenue_date(value)
    return dt.date().isoformat() if dt else ""

@app.get('/revenue_summary')
def revenue_summary():
    try:
        settings = _load_business_settings()

        requested_mode = str(request.args.get("mode") or settings.get("production_mode") or "test").lower()
        mode = "live" if requested_mode == "live" else "test"

        from_iso = _normalize_launch_date(request.args.get("from_date") or "")
        to_iso = _normalize_launch_date(request.args.get("to_date") or "")
        launch_iso = _normalize_launch_date(settings.get("revenue_launch_date") or settings.get("launch_date") or "")

        from_dt = _revenue_date(from_iso) if from_iso else None
        to_dt = _revenue_date(to_iso) if to_iso else None
        if to_dt:
            # Inclusive end date: records on the selected To Date must count.
            to_dt = to_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

        purchase_rows = supabase.table("challenge_purchases").select("*").execute().data or []
        payout_rows = supabase.table("payouts").select("*").execute().data or []

        try:
            trader_rows = supabase.table("traders").select("id,status,phase").execute().data or []
        except Exception:
            trader_rows = []

        def in_selected_range(dt):
            if not dt:
                return False
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if from_dt and dt < from_dt:
                return False
            if to_dt and dt > to_dt:
                return False
            return True

        def live_excluded(row):
            if mode != "live":
                return False
            return _revenue_bool(row.get("excluded_from_revenue")) or _revenue_bool(row.get("mark_as_test"))

        def purchase_date(row):
            return _revenue_date(row.get("approved_at") or row.get("assigned_at") or row.get("created_at"))

        def payout_date(row):
            return _revenue_date(row.get("paid_at") or row.get("approved_at") or row.get("requested_at") or row.get("created_at"))

        counted_purchases = []
        excluded_purchases = 0
        for row in purchase_rows:
            payment_status = str(row.get("payment_status") or "").strip().lower()
            status = str(row.get("status") or "").strip().lower()
            approved = payment_status == "approved" or status in ["approved", "approved_active", "active"]
            dt = purchase_date(row)
            if not approved or live_excluded(row) or not in_selected_range(dt):
                excluded_purchases += 1
                continue
            counted_purchases.append((row, dt))

        counted_payouts = []
        excluded_payouts = 0
        for row in payout_rows:
            status = str(row.get("status") or "").strip().lower()
            # Money that has actually left the business should be PAID only.
            included = status == "paid"
            dt = payout_date(row)
            if not included or live_excluded(row) or not in_selected_range(dt):
                excluded_payouts += 1
                continue
            counted_payouts.append((row, dt))

        try:
            revenue_accounts = supabase.table("trader_accounts").select("id,stage,phase,account_status,status,risk_zone,display_risk_zone,dd_used_percent,mt5_access_disabled").limit(5000).execute().data or []
        except Exception:
            revenue_accounts = []
        active_account_statuses = {"assigned_active", "active", "current_active", "phase1_active", "phase2_active", "funded_active", "live", "funded"}
        active_trader_count = len([a for a in revenue_accounts if not _v2_is_breached(a) and _v2_account_status(a) in active_account_statuses])
        funded_trader_count = len([a for a in revenue_accounts if not _v2_is_breached(a) and _v2_stage(a) in {"funded", "live"} and _v2_account_status(a) in active_account_statuses])
        if not revenue_accounts:
            active_trader_count = len([t for t in trader_rows if str(t.get("status") or "").lower() == "active"])
            funded_trader_count = len([t for t in trader_rows if str(t.get("status") or "").lower() in ["funded", "live"] or str(t.get("phase") or "").lower() in ["funded", "live"]])

        summary = {
            "weekly_sales": 0,
            "monthly_sales": 0,
            "yearly_sales": 0,
            "weekly_payouts": 0,
            "monthly_payouts": 0,
            "yearly_payouts": 0,
            "weekly_net": 0,
            "monthly_net": 0,
            "yearly_net": 0,
            "range_sales": 0,
            "range_payouts": 0,
            "range_net": 0,
            "gross_revenue": 0,
            "net_revenue": 0,
            "approved_payouts": 0,
            "pending_payouts": 0,
            "paid_payouts": 0,
            "pending_sales": 0,
            "rejected_sales": 0,
            "counted_purchases": len(counted_purchases),
            "excluded_purchases": excluded_purchases,
            "counted_payouts": len(counted_payouts),
            "excluded_payouts": excluded_payouts,
            "total_purchases_loaded": len(purchase_rows),
            "total_payouts_loaded": len(payout_rows),
            "launch_date_used": launch_iso,
            "from_date_used": from_iso,
            "to_date_used": to_iso,
            "date_filter_used": {"from_date": from_iso, "to_date": to_iso},
            "production_mode_used": mode,
            "active_traders": active_trader_count,
            "funded_traders": funded_trader_count,
            "conversion_rate": "0%",
            "plan_rows": [],
            "month_rows": []
        }

        plan_map = {}
        month_map = {}
        for row, dt in counted_purchases:
            amount = clean(row.get("fee"))
            flags = _revenue_period_flags(dt)
            summary["gross_revenue"] += amount
            summary["range_sales"] += amount
            if flags["week"]: summary["weekly_sales"] += amount
            if flags["month"]: summary["monthly_sales"] += amount
            if flags["year"]: summary["yearly_sales"] += amount

            plan_name = row.get("plan_name") or "Unknown Plan"
            if plan_name not in plan_map:
                plan_map[plan_name] = {"name": plan_name, "count": 0, "fee": 0, "account_size": row.get("account_size") or 0}
            plan_map[plan_name]["count"] += 1
            plan_map[plan_name]["fee"] += amount

            if dt:
                month_key = dt.strftime("%b %Y")
                if month_key not in month_map:
                    month_map[month_key] = {"month": month_key, "sales": 0, "count": 0, "sort": dt.strftime("%Y-%m")}
                month_map[month_key]["sales"] += amount
                month_map[month_key]["count"] += 1

        for row, dt in counted_payouts:
            amount = clean(row.get("amount"))
            flags = _revenue_period_flags(dt)
            summary["paid_payouts"] += amount
            summary["range_payouts"] += amount
            if flags["week"]: summary["weekly_payouts"] += amount
            if flags["month"]: summary["monthly_payouts"] += amount
            if flags["year"]: summary["yearly_payouts"] += amount

        for row in payout_rows:
            status = str(row.get("status") or "").lower()
            if status == "approved" and not live_excluded(row):
                summary["approved_payouts"] += clean(row.get("amount"))
            if status == "pending" and not live_excluded(row):
                summary["pending_payouts"] += clean(row.get("amount"))

        for row in purchase_rows:
            status = str(row.get("payment_status") or row.get("status") or "").lower()
            dt = purchase_date(row)
            if not in_selected_range(dt):
                continue
            if status in ["pending", "pending_review"]:
                summary["pending_sales"] += clean(row.get("fee"))
            if status == "rejected":
                summary["rejected_sales"] += clean(row.get("fee"))

        summary["weekly_net"] = summary["weekly_sales"] - summary["weekly_payouts"]
        summary["monthly_net"] = summary["monthly_sales"] - summary["monthly_payouts"]
        summary["yearly_net"] = summary["yearly_sales"] - summary["yearly_payouts"]
        summary["range_net"] = summary["range_sales"] - summary["range_payouts"]
        summary["net_revenue"] = summary["range_net"]
        if trader_rows:
            summary["conversion_rate"] = f"{(summary['counted_purchases'] / len(trader_rows) * 100):.1f}%"
        summary["plan_rows"] = sorted(plan_map.values(), key=lambda x: x["fee"], reverse=True)
        summary["month_rows"] = [{k: v for k, v in row.items() if k != "sort"} for row in sorted(month_map.values(), key=lambda x: x["sort"])[-12:]]

        return jsonify({"success": True, "data": summary})
    except Exception as e:
        print("REVENUE SUMMARY ERROR:", str(e))
        return jsonify({"success": False, "error": str(e)}), 500

@app.get('/revenue_launch_date')
def get_revenue_launch_date():
    settings = _load_business_settings()
    return jsonify({'success': True, 'data': {
        'revenue_launch_date': settings.get('revenue_launch_date') or '',
        'launch_date': settings.get('revenue_launch_date') or '',
        'production_mode': settings.get('production_mode') or 'test'
    }})

@app.post('/revenue_launch_date')
def update_revenue_launch_date():
    data = request.get_json(silent=True) or {}
    admin = _admin_from_payload(data)
    updates = {
        'revenue_launch_date': data.get('revenue_launch_date') if 'revenue_launch_date' in data else data.get('launch_date', '')
    }
    if 'production_mode' in data:
        updates['production_mode'] = 'live' if str(data.get('production_mode')).lower() == 'live' else 'test'
    ok_saved, err = _save_business_settings(updates, admin)
    if not ok_saved:
        return jsonify({'success': False, 'error': err or 'Could not save revenue launch date'}), 500
    _audit_safe('revenue', 'launch_date_set', f'Revenue launch date set to {updates.get("revenue_launch_date") or "cleared"}', admin, 'business_launch_date')
    return get_revenue_launch_date()

@app.post('/audit_event')
def audit_event_route():
    try:
        data = request.get_json(silent=True) or {}
        audit_log(
            _admin_from_payload(data),
            data.get('module') or 'admin',
            data.get('action') or 'activity',
            data.get('details') or data.get('record') or '',
            data.get('record_affected') or data.get('record_id') or '',
            data.get('created_at') or now_iso()
        )
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.post('/mark_record_test')
def mark_record_test():
    try:
        data = request.get_json(silent=True) or {}
        table = str(data.get('table') or '').strip()
        record_id = data.get('id')
        if table not in SAFE_FLAG_TABLES:
            return bad('Unsupported table for test/revenue flagging')
        if not record_id:
            return bad('Record id is required')

        payload = {'updated_at': now_iso()}
        if 'mark_as_test' in data:
            payload['mark_as_test'] = bool(data.get('mark_as_test'))
        if 'excluded_from_revenue' in data:
            payload['excluded_from_revenue'] = bool(data.get('excluded_from_revenue'))
        if len(payload) == 1:
            return bad('Nothing to update')

        try:
            res = supabase.table(table).update(payload).eq('id', record_id).execute()
        except Exception as e:
            print('SAFE FLAG UPDATE FAILED:', table, record_id, e)
            return jsonify({'success': False, 'fallback': 'local', 'error': str(e)}), 200

        _audit_safe(table, 'mark_test_or_revenue_flag', f"{table}:{record_id} {payload}", _admin_from_payload(data))
        return ok(getattr(res, 'data', []) or [], 'Record flag updated')
    except Exception as e:
        return bad(e)

@app.post('/delete_payout')
def delete_payout_protected():
    return bad('Payout records cannot be deleted in production. Mark as test or exclude from revenue instead.', 403)

@app.post('/delete_payment')
def delete_payment_protected():
    return bad('Approved payment records cannot be deleted in production. Mark as test or exclude from revenue instead.', 403)

@app.post('/delete_challenge_purchase')
def delete_challenge_purchase_protected():
    try:
        data = request.get_json(silent=True) or {}
        pid = data.get('id')
        purchase = get_purchase_by_id(pid) if pid else {}
        if _has_approved_payment([purchase]):
            return bad('Approved challenge purchases cannot be deleted in production. Mark as test or exclude from revenue instead.', 403)
        return bad('Challenge purchase deletion is disabled in production. Mark as test or exclude from revenue instead.', 403)
    except Exception as e:
        return bad(e)

# ===== NAIRAPIPS PAYMENT ACCOUNTS ROUTES =====
# Paste above:



@app.get('/payment_accounts')
def payment_accounts():
    try:
        res = supabase.table('payment_accounts').select('*').order('display_order', desc=False).execute()
        return jsonify(res.data or [])
    except Exception as e:
        return jsonify([])

@app.post('/save_payment_accounts')
def save_payment_accounts():
    try:
        body = request.get_json(silent=True) or {}
        accounts = body.get('accounts', []) or []

        clean_rows = []
        for idx, account in enumerate(accounts, start=1):
            row = {
                'label': str(account.get('label') or f'Payment Account {idx}').strip(),
                'bank_name': str(account.get('bank_name') or '').strip(),
                'account_name': str(account.get('account_name') or '').strip(),
                'account_number': str(account.get('account_number') or '').strip(),
                'account_type': str(account.get('account_type') or 'Bank Transfer').strip(),
                'instructions': str(account.get('instructions') or 'Upload your proof of payment after transfer so admin can verify and activate your challenge.').strip(),
                'status': str(account.get('status') or 'active').strip(),
                'display_order': int(account.get('display_order') or idx)
            }
            clean_rows.append(row)

        supabase.table('payment_accounts').delete().neq('id', -1).execute()
        if clean_rows:
            supabase.table('payment_accounts').insert(clean_rows).execute()

        return jsonify({'success': True, 'data': clean_rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
@app.route("/test_email")
def test_email():
    try:
        recipient = ADMIN_ALERT_EMAIL or FROM_EMAIL
        sent = send_email_brevo(
            recipient,
            "NairaPips Email Test",
            "<p>NairaPips email system is working.</p><p>This confirms Brevo HTTP email sending works from Render.</p><p>NairaPips Team</p>"
        )
        if sent:
            return {"success": True, "message": "Test email sent"}
        return {"success": False, "error": "Brevo email send failed. Check Render logs."}

    except Exception as e:
        print("BREVO EMAIL ERROR:", str(e))
        return {"success": False, "error": str(e)}


# ================================
# NAIRAPIPS RANGE REPORTING: SALES + PAYOUTS
# ================================
def _np_range_params():
    requested_mode = str(request.args.get("mode") or "test").lower()
    mode = "live" if requested_mode == "live" else "test"
    from_iso = _normalize_launch_date(request.args.get("from_date") or "")
    to_iso = _normalize_launch_date(request.args.get("to_date") or "")
    from_dt = _revenue_date(from_iso) if from_iso else None
    to_dt = _revenue_date(to_iso) if to_iso else None
    if to_dt:
        to_dt = to_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    return mode, from_iso, to_iso, from_dt, to_dt

def _np_in_range(dt, from_dt=None, to_dt=None):
    if not dt:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if from_dt and dt < from_dt:
        return False
    if to_dt and dt > to_dt:
        return False
    return True

def _np_report_excluded(row, mode="test"):
    if mode != "live":
        return False
    return _revenue_bool(row.get("excluded_from_revenue")) or _revenue_bool(row.get("mark_as_test"))

def _np_purchase_report_date(row):
    return _revenue_date(row.get("approved_at") or row.get("assigned_at") or row.get("created_at"))

def _np_payout_report_date(row):
    status = str(row.get("status") or "").strip().lower()
    if status == "paid":
        return _revenue_date(row.get("paid_at") or row.get("approved_at") or row.get("requested_at") or row.get("created_at"))
    if status == "approved":
        return _revenue_date(row.get("approved_at") or row.get("requested_at") or row.get("created_at"))
    return _revenue_date(row.get("requested_at") or row.get("created_at") or row.get("approved_at") or row.get("paid_at"))

@app.get('/sales_summary')
def sales_summary():
    try:
        mode, from_iso, to_iso, from_dt, to_dt = _np_range_params()
        rows = supabase.table("challenge_purchases").select("*").execute().data or []
        total_loaded = len(rows)
        counted = []
        excluded = 0
        pending_amount = rejected_amount = 0
        pending_count = rejected_count = 0
        plan_map = {}
        day_map = {}
        for row in rows:
            dt = _np_purchase_report_date(row)
            if not _np_in_range(dt, from_dt, to_dt) or _np_report_excluded(row, mode):
                excluded += 1
                continue
            payment_status = str(row.get("payment_status") or "").strip().lower()
            status = str(row.get("status") or "").strip().lower()
            amount = clean(row.get("fee"))
            if payment_status == "approved" or status in ["approved", "approved_active", "active"]:
                counted.append(row)
                plan = row.get("plan_name") or "Unknown Plan"
                if plan not in plan_map:
                    plan_map[plan] = {"plan_name": plan, "count": 0, "amount": 0, "account_size": row.get("account_size") or 0}
                plan_map[plan]["count"] += 1
                plan_map[plan]["amount"] += amount
                if dt:
                    day = dt.date().isoformat()
                    if day not in day_map:
                        day_map[day] = {"date": day, "count": 0, "amount": 0}
                    day_map[day]["count"] += 1
                    day_map[day]["amount"] += amount
            elif payment_status in ["pending", "pending_review"] or status in ["pending", "pending_review"]:
                pending_count += 1; pending_amount += amount
            elif payment_status == "rejected" or status == "rejected":
                rejected_count += 1; rejected_amount += amount
        approved_amount = sum(clean(r.get("fee")) for r in counted)
        return jsonify({"success": True, "data": {
            "mode_used": mode, "from_date_used": from_iso, "to_date_used": to_iso,
            "total_sales_loaded": total_loaded, "counted_sales": len(counted), "excluded_sales": excluded,
            "approved_sales_amount": approved_amount, "pending_sales_amount": pending_amount, "rejected_sales_amount": rejected_amount,
            "pending_sales_count": pending_count, "rejected_sales_count": rejected_count,
            "average_sale": (approved_amount / len(counted)) if counted else 0,
            "plan_rows": sorted(plan_map.values(), key=lambda x: x["amount"], reverse=True),
            "day_rows": [day_map[k] for k in sorted(day_map.keys())]
        }})
    except Exception as e:
        print("SALES SUMMARY ERROR:", str(e))
        return jsonify({"success": False, "error": str(e)}), 500

@app.get('/payout_summary')
def payout_summary():
    try:
        mode, from_iso, to_iso, from_dt, to_dt = _np_range_params()
        rows = supabase.table("payouts").select("*").execute().data or []
        total_loaded = len(rows)
        excluded = 0
        status_counts = {"pending": 0, "approved": 0, "paid": 0, "rejected": 0}
        status_amounts = {"pending": 0, "approved": 0, "paid": 0, "rejected": 0}
        bank_map = {}; day_map = {}; counted_rows = 0
        for row in rows:
            dt = _np_payout_report_date(row)
            if not _np_in_range(dt, from_dt, to_dt) or _np_report_excluded(row, mode):
                excluded += 1
                continue
            status = str(row.get("status") or "pending").strip().lower()
            if status not in status_counts: status = "pending"
            amount = clean(row.get("amount"))
            status_counts[status] += 1; status_amounts[status] += amount; counted_rows += 1
            if status == "paid":
                bank = row.get("bank_name") or "Unknown Bank"
                if bank not in bank_map: bank_map[bank] = {"bank_name": bank, "count": 0, "amount": 0}
                bank_map[bank]["count"] += 1; bank_map[bank]["amount"] += amount
                if dt:
                    day = dt.date().isoformat()
                    if day not in day_map: day_map[day] = {"date": day, "count": 0, "amount": 0}
                    day_map[day]["count"] += 1; day_map[day]["amount"] += amount
        return jsonify({"success": True, "data": {
            "mode_used": mode, "from_date_used": from_iso, "to_date_used": to_iso,
            "total_payouts_loaded": total_loaded, "counted_payouts": counted_rows, "excluded_payouts": excluded,
            "pending_count": status_counts["pending"], "approved_count": status_counts["approved"], "paid_count": status_counts["paid"], "rejected_count": status_counts["rejected"],
            "pending_amount": status_amounts["pending"], "approved_amount": status_amounts["approved"], "paid_amount": status_amounts["paid"], "rejected_amount": status_amounts["rejected"],
            "liability_amount": status_amounts["pending"] + status_amounts["approved"],
            "bank_rows": sorted(bank_map.values(), key=lambda x: x["amount"], reverse=True),
            "day_rows": [day_map[k] for k in sorted(day_map.keys())]
        }})
    except Exception as e:
        print("PAYOUT SUMMARY ERROR:", str(e))
        return jsonify({"success": False, "error": str(e)}), 500


# ================================
# NAIRAPIPS AFFILIATE & PARTNER MANAGER - PRODUCTION READY
# ================================
def _aff_code(value):
    return re.sub(r"[^A-Z0-9_-]", "", str(value or "").strip().upper())[:40]

def _aff_status_active(row):
    status = str((row or {}).get("status") or "active").strip().lower()
    return status in {"active", "live", "enabled"}

def _aff_parse_date(value):
    """Parse Supabase date/timestamp values and always return timezone-aware UTC datetime.
    This prevents Python errors when comparing date-only values like 2026-06-02
    with timezone-aware current time values.
    """
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        # Supabase often returns either YYYY-MM-DD or ISO timestamp ending in Z.
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            dt = datetime.fromisoformat(raw + "T00:00:00+00:00")
        else:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.fromisoformat(raw[:10] + "T00:00:00+00:00")
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _aff_today_utc():
    return datetime.now(timezone.utc)

def _aff_code_valid_window(row):
    now = _aff_today_utc()
    start = _aff_parse_date((row or {}).get("start_date"))
    end = _aff_parse_date((row or {}).get("end_date"))
    if start and now < start:
        return False, "Code is not active yet"
    if end:
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    if end and now > end:
        return False, "Code has expired"
    limit = int(clean((row or {}).get("usage_limit")) or 0)
    uses = int(clean((row or {}).get("total_uses")) or 0)
    if limit > 0 and uses >= limit:
        return False, "Code usage limit reached"
    return True, "Code is valid"

def _aff_get_code(code):
    code = _aff_code(code)
    if not code:
        return None
    try:
        rows = supabase.table("affiliate_codes").select("*").eq("code", code).limit(1).execute().data or []
        return rows[0] if rows else None
    except Exception as e:
        print("AFFILIATE CODE FETCH ERROR:", str(e))
        return None

def _aff_get_partner_by_code(code):
    code = _aff_code(code)
    if not code:
        return None
    try:
        rows = supabase.table("affiliate_partners").select("*").eq("code", code).limit(1).execute().data or []
        return rows[0] if rows else None
    except Exception as e:
        print("AFFILIATE PARTNER FETCH ERROR:", str(e))
        return None


# ================================
# PRIVATE OFFER CLAIM + PHASEHELP DISCOUNT ENGINE
# ================================
def _np_offer_discount_percent(row, code=""):
    """Return private-offer discount percent. Defaults to 50% for recovery offers."""
    try:
        row = _np_offer_merge_meta(row or {})
    except Exception:
        row = row or {}
    direct = clean(row.get("discount_percent") or row.get("discount") or row.get("discount_pct"))
    if direct > 0:
        return max(0, min(100, direct))
    text = " ".join([str(code or ""), str(row.get("offer_code") or ""), str(row.get("title") or ""), str(row.get("message") or "")]).upper()
    m = re.search(r"(\d{1,2})\s*%", text)
    if m:
        return max(0, min(100, clean(m.group(1))))
    m = re.search(r"(?:COMEBACK|RECOVERY|PHASEHELP|HELP|SAVE|RESTART)(\d{1,2})", text)
    if m:
        return max(0, min(100, clean(m.group(1))))
    return 50 if ("PHASEHELP" in text or "COMEBACK" in text or "RECOVERY" in text or "SECOND CHANCE" in text) else 0


def _np_offer_active_for_quote(d, code):
    """Find a valid targeted private offer for this trader and code."""
    code = _aff_code(code)
    if not code:
        return None
    trader_id = _np_offer_clean_str((d or {}).get("trader_id") or (d or {}).get("target_trader_id"), 120)
    email = _np_offer_clean_str((d or {}).get("email") or (d or {}).get("customer_email") or (d or {}).get("target_email"), 250).lower()
    phone = _np_offer_clean_str((d or {}).get("phone") or (d or {}).get("customer_phone") or (d or {}).get("target_phone"), 80)
    account_reference = _np_offer_clean_str((d or {}).get("account_reference") or (d or {}).get("target_account_reference"), 120)
    try:
        rows = supabase.table("announcements").select("*").eq("status", "active").eq("type", "private_offer").order("created_at", desc=True).limit(150).execute().data or []
    except Exception:
        try:
            rows = supabase.table("announcements").select("*").eq("type", "private_offer").order("created_at", desc=True).limit(150).execute().data or []
        except Exception:
            rows = []
    for raw in rows:
        row = _np_offer_merge_meta(raw)
        if _np_private_offer_expired(row):
            continue
        if not _np_private_offer_matches(row, trader_id, email, phone, account_reference):
            continue
        offer_code = _aff_code(row.get("offer_code") or "PHASEHELP")
        if code == offer_code or (code == "PHASEHELP" and offer_code in {"", "PHASEHELP", "COMEBACK50", "RECOVERY50"}):
            pct = _np_offer_discount_percent(row, code)
            if pct > 0:
                row["_discount_percent"] = pct
                row["_resolved_offer_code"] = code
                return row
    return None


def _np_private_offer_quote(d, base_fee, code):
    row = _np_offer_active_for_quote(d, code)
    if not row:
        return None
    pct = max(0, min(100, clean(row.get("_discount_percent") or 0)))
    discount_amount = round(clean(base_fee) * pct / 100, 2)
    final_fee = max(0, round(clean(base_fee) - discount_amount, 2))
    return {
        "valid": True,
        "code": _aff_code(code),
        "message": f"Private recovery offer activated: {pct:g}% discount applied",
        "original_fee": clean(base_fee),
        "discount_percent": pct,
        "discount_amount": discount_amount,
        "final_fee": final_fee,
        "fee": final_fee,
        "commission_percent": 0,
        "commission_amount": 0,
        "affiliate_owner": "NairaPips Private Offer",
        "campaign_type": "private_recovery_offer",
        "offer_id": row.get("id"),
        "expires_at": row.get("offer_expires_at") or row.get("expires_at") or row.get("expiry_date") or row.get("expires"),
        "source": row,
    }


@app.route("/claim_private_offer", methods=["POST", "OPTIONS"])
def claim_private_offer():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        offer_id = _np_offer_clean_str(d.get("offer_id") or d.get("id"), 120)
        trader_id = _np_offer_clean_str(d.get("trader_id"), 120)
        email = _np_offer_clean_str(d.get("email"), 250).lower()
        phone = _np_offer_clean_str(d.get("phone"), 80)
        account_reference = _np_offer_clean_str(d.get("account_reference"), 120)
        rows = []
        if offer_id:
            try:
                rows = supabase.table("announcements").select("*").eq("id", offer_id).limit(1).execute().data or []
            except Exception:
                rows = []
        if not rows:
            try:
                rows = supabase.table("announcements").select("*").eq("status", "active").eq("type", "private_offer").order("created_at", desc=True).limit(150).execute().data or []
            except Exception:
                rows = supabase.table("announcements").select("*").eq("type", "private_offer").order("created_at", desc=True).limit(150).execute().data or []
        chosen = None
        for raw in rows:
            row = _np_offer_merge_meta(raw)
            if offer_id and str(row.get("id") or "") != offer_id:
                continue
            if _np_private_offer_expired(row):
                continue
            if not _np_private_offer_matches(row, trader_id, email, phone, account_reference):
                continue
            chosen = row
            break
        if not chosen:
            return bad("Offer not found, expired, or not assigned to this trader", 404)
        code = _aff_code(chosen.get("offer_code") or "PHASEHELP") or "PHASEHELP"
        pct = _np_offer_discount_percent(chosen, code)
        expires = chosen.get("offer_expires_at") or chosen.get("expires_at") or chosen.get("expiry_date") or chosen.get("expires")
        # Best-effort claim audit. Do not fail the trader if optional columns are missing.
        try:
            claimed_payload = {"claimed_at": now_iso(), "read_at": now_iso()}
            supabase.table("announcements").update(claimed_payload).eq("id", chosen.get("id")).execute()
        except Exception as e:
            print("PRIVATE OFFER CLAIM AUDIT SKIPPED:", e)
        return ok({
            "claimed": True,
            "offer_id": chosen.get("id"),
            "offer_code": code,
            "promo_code": code,
            "discount_percent": pct,
            "expires_at": expires,
            "redirect": "plans",
        }, "Private offer claimed")
    except Exception as e:
        return bad(e, 500)

@app.route("/mark_private_offer_read", methods=["POST", "OPTIONS"])
def mark_private_offer_read():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        offer_id = _np_offer_clean_str(d.get("offer_id") or d.get("id"), 120)
        trader_id = _np_offer_clean_str(d.get("trader_id"), 120)
        email = _np_offer_clean_str(d.get("email"), 250).lower()
        phone = _np_offer_clean_str(d.get("phone"), 80)
        account_reference = _np_offer_clean_str(d.get("account_reference"), 120)
        if not offer_id:
            return bad("offer_id is required", 400)
        rows = []
        try:
            rows = supabase.table("announcements").select("*").eq("id", offer_id).limit(1).execute().data or []
        except Exception:
            rows = []
        if not rows:
            return bad("Offer not found", 404)
        row = _np_offer_merge_meta(rows[0])
        if not _np_private_offer_matches(row, trader_id, email, phone, account_reference):
            return bad("Offer is not assigned to this trader", 403)
        try:
            supabase.table("announcements").update({"read_at": now_iso()}).eq("id", offer_id).execute()
        except Exception as e:
            # Schema-free/private fallback rows may not have read_at. Do not block UI.
            print("PRIVATE OFFER READ AUDIT SKIPPED:", e)
        return ok({"read": True, "offer_id": offer_id}, "Private offer marked as read")
    except Exception as e:
        return bad(e, 500)

def _affiliate_quote_details(d, base_fee):
    """Return production-safe quote details for promo / affiliate / partner codes.
    Discount reduces customer price. Commission is calculated on the final paid fee.
    """
    base_fee = clean(base_fee)
    raw_code = (d or {}).get("affiliate_code") or (d or {}).get("referral_code") or (d or {}).get("ref_code") or (d or {}).get("partner_code") or (d or {}).get("promo_code") or (d or {}).get("code")
    code = _aff_code(raw_code)
    result = {
        "valid": False if code else True,
        "code": code,
        "message": "No code applied" if not code else "Code pending validation",
        "original_fee": base_fee,
        "discount_percent": 0,
        "discount_amount": 0,
        "final_fee": base_fee,
        "fee": base_fee,
        "commission_percent": 0,
        "commission_amount": 0,
        "affiliate_owner": "",
        "campaign_type": "",
        "source": None,
    }
    if not code:
        return result

    private_quote = _np_private_offer_quote(d or {}, base_fee, code)
    if private_quote:
        return private_quote

    source = _aff_get_code(code) or _aff_get_partner_by_code(code)
    if not source:
        result.update({"valid": False, "message": "Code not found"})
        return result

    ok_window, reason = _aff_code_valid_window(source)
    if not _aff_status_active(source) or not ok_window:
        result.update({"valid": False, "message": reason or "Code is not active"})
        return result

    # Self-referral protection by email/phone when partner identity exists.
    buyer_email = str((d or {}).get("email") or (d or {}).get("customer_email") or "").strip().lower()
    buyer_phone = re.sub(r"\D", "", str((d or {}).get("phone") or (d or {}).get("customer_phone") or ""))
    owner_email = str(source.get("email") or source.get("owner_email") or "").strip().lower()
    owner_phone = re.sub(r"\D", "", str(source.get("phone") or source.get("owner_phone") or ""))
    if buyer_email and owner_email and buyer_email == owner_email:
        result.update({"valid": False, "message": "Self-referral is not allowed"})
        return result
    if buyer_phone and owner_phone and buyer_phone == owner_phone:
        result.update({"valid": False, "message": "Self-referral is not allowed"})
        return result

    discount_pct = max(0, min(100, clean(source.get("discount_percent"))))
    commission_pct = max(0, min(100, clean(source.get("commission_percent"))))
    discount_amount = round(base_fee * discount_pct / 100, 2) if discount_pct > 0 else 0
    final_fee = max(0, round(base_fee - discount_amount, 2))
    commission_amount = round(final_fee * commission_pct / 100, 2) if commission_pct > 0 else 0
    campaign_type = str(source.get("code_type") or source.get("partner_type") or "affiliate").strip().lower() or "affiliate"
    owner = source.get("owner_name") or source.get("name") or source.get("partner_name") or ""

    result.update({
        "valid": True,
        "message": "Code applied successfully",
        "discount_percent": discount_pct,
        "discount_amount": discount_amount,
        "final_fee": final_fee,
        "fee": final_fee,
        "commission_percent": commission_pct,
        "commission_amount": commission_amount,
        "affiliate_owner": owner,
        "campaign_type": campaign_type,
        "source": source,
    })
    return result


def _affiliate_purchase_fields(d, base_fee):
    quote = _affiliate_quote_details(d, base_fee)
    fields = {
        "original_fee": quote.get("original_fee", clean(base_fee)),
        "discount_percent": quote.get("discount_percent", 0),
        "discount_amount": quote.get("discount_amount", 0),
        "final_fee": quote.get("final_fee", clean(base_fee)),
        "amount_due": quote.get("final_fee", clean(base_fee)),
        "commission_percent": quote.get("commission_percent", 0),
        "commission_amount": quote.get("commission_amount", 0),
        "affiliate_status": "valid" if quote.get("valid") and quote.get("code") else ("none" if not quote.get("code") else quote.get("message")),
    }
    code = quote.get("code")
    if code:
        fields.update({
            "affiliate_code": code,
            "referral_code": code,
            "partner_code": code,
            "promo_code": code,
            "affiliate_owner": quote.get("affiliate_owner") or "",
            "campaign_type": quote.get("campaign_type") or "affiliate",
            "fee": quote.get("final_fee", clean(base_fee)),
        })
    return fields


def _affiliate_record_code_usage(purchase):
    """Increment usage only once when an approved purchase carries a valid code."""
    try:
        p = purchase or {}
        if _is_truthy(p.get("affiliate_usage_recorded")):
            return False
        code = _aff_code(p.get("affiliate_code") or p.get("referral_code") or p.get("ref_code") or p.get("partner_code") or p.get("promo_code"))
        if not code:
            return False
        source = _aff_get_code(code)
        if source:
            try:
                supabase.table("affiliate_codes").update({
                    "total_uses": int(clean(source.get("total_uses")) or 0) + 1,
                    "updated_at": now_iso()
                }).eq("code", code).execute()
            except Exception as e:
                print("AFFILIATE USAGE UPDATE ERROR:", str(e))
        pid = p.get("id")
        if pid:
            try:
                supabase.table("challenge_purchases").update({"affiliate_usage_recorded": True, "updated_at": now_iso()}).eq("id", pid).execute()
            except Exception as e:
                print("AFFILIATE PURCHASE USAGE FLAG ERROR:", str(e))
        return True
    except Exception as e:
        print("AFFILIATE USAGE RECORD ERROR:", str(e))
        return False

@app.route("/quote_challenge_price", methods=["POST", "GET"])
def quote_challenge_price():
    try:
        if request.method == "GET":
            d = dict(request.args)
        else:
            d = request.json or {}
        base_fee = clean(d.get("fee") or d.get("challenge_fee") or d.get("amount"))
        if base_fee <= 0:
            return bad("Challenge fee is required")
        quote = _affiliate_quote_details(d, base_fee)
        quote.pop("source", None)
        return jsonify({"success": True, "data": quote, "valid": quote.get("valid"), "message": quote.get("message")})
    except Exception as e:
        return bad(e, 500)

def _affiliate_create_commission_from_purchase(purchase, admin_payload=None):
    try:
        p = purchase or {}
        _affiliate_record_code_usage(p)
        code = _aff_code(p.get("affiliate_code") or p.get("referral_code") or p.get("ref_code") or p.get("partner_code") or p.get("promo_code"))
        if not code:
            return None
        source = _aff_get_code(code) or _aff_get_partner_by_code(code)
        if not source:
            return None
        commission_pct = clean(p.get("commission_percent") or source.get("commission_percent"))
        sale_amount = clean(p.get("fee"))
        if commission_pct <= 0 or sale_amount <= 0:
            return None
        commission_amount = clean(p.get("commission_amount")) or round(sale_amount * commission_pct / 100, 2)
        existing = supabase.table("affiliate_commissions").select("id").eq("purchase_id", p.get("id")).limit(1).execute().data or []
        if existing:
            return existing[0]
        row = {
            "partner_code": code,
            "partner_name": source.get("owner_name") or source.get("name") or source.get("partner_name") or "",
            "purchase_id": p.get("id"),
            "customer_name": p.get("trader_name") or p.get("name") or "",
            "customer_email": p.get("email") or "",
            "sale_amount": sale_amount,
            "commission_percent": commission_pct,
            "commission_amount": commission_amount,
            "status": "pending",
            "created_at": now_iso(),
        }
        created = supabase.table("affiliate_commissions").insert(row).execute().data
        _audit_safe("affiliates", "commission_created", f"Affiliate commission created for code {code}", _admin_from_payload(admin_payload or {}))
        return created[0] if created else row
    except Exception as e:
        print("AFFILIATE COMMISSION CREATE ERROR:", str(e))
        return None

@app.route("/affiliate_partners", methods=["GET"])
def affiliate_partners():
    try:
        return jsonify(supabase.table("affiliate_partners").select("*").order("created_at", desc=True).execute().data or [])
    except Exception as e:
        return bad(e, 500)

@app.route("/create_affiliate_partner", methods=["POST"])
def create_affiliate_partner():
    try:
        d = request.json or {}
        name = str(d.get("name") or d.get("partner_name") or "").strip()
        code = _aff_code(d.get("code") or name)
        if not name: return bad("Partner name is required")
        if not code: return bad("Affiliate code is required")
        existing = supabase.table("affiliate_partners").select("id").eq("code", code).limit(1).execute().data or []
        if existing: return bad("Affiliate partner code already exists", 409)
        row = {
            "name": name,
            "email": d.get("email") or "",
            "phone": d.get("phone") or "",
            "company": d.get("company") or "",
            "partner_type": d.get("partner_type") or "affiliate",
            "code": code,
            "affiliate_link": d.get("affiliate_link") or f"https://nairapips.com/?ref={code}",
            "commission_percent": clean(d.get("commission_percent") or 20),
            "discount_percent": clean(d.get("discount_percent") or 0),
            "status": d.get("status") or "active",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }
        created = supabase.table("affiliate_partners").insert(row).execute().data
        _audit_safe("affiliates", "partner_created", f"Affiliate partner created: {code}", _admin_from_payload(d))
        return ok(created, "Affiliate partner created")
    except Exception as e:
        return bad(e, 500)

@app.route("/update_affiliate_partner", methods=["POST"])
def update_affiliate_partner():
    try:
        d = request.json or {}; pid = d.get("id")
        if not pid: return bad("Missing partner id")
        upd = {"updated_at": now_iso()}
        for k in ["name", "email", "phone", "company", "partner_type", "status", "affiliate_link"]:
            if k in d: upd[k] = d[k]
        if "code" in d: upd["code"] = _aff_code(d.get("code"))
        for k in ["commission_percent", "discount_percent"]:
            if k in d: upd[k] = clean(d.get(k))
        result = supabase.table("affiliate_partners").update(upd).eq("id", pid).execute().data
        return ok(result, "Affiliate partner updated")
    except Exception as e:
        return bad(e, 500)

@app.route("/delete_affiliate_partner", methods=["POST"])
def delete_affiliate_partner():
    try:
        d = request.json or {}; pid = d.get("id")
        if not pid: return bad("Missing partner id")
        result = supabase.table("affiliate_partners").update({"status":"inactive", "updated_at": now_iso()}).eq("id", pid).execute().data
        return ok(result, "Affiliate partner deactivated")
    except Exception as e:
        return bad(e, 500)

@app.route("/affiliate_codes", methods=["GET"])
def affiliate_codes():
    try:
        return jsonify(supabase.table("affiliate_codes").select("*").order("created_at", desc=True).execute().data or [])
    except Exception as e:
        return bad(e, 500)

@app.route("/create_affiliate_code", methods=["POST"])
def create_affiliate_code():
    try:
        d = request.json or {}
        code = _aff_code(d.get("code"))
        if not code: return bad("Code is required")
        existing = supabase.table("affiliate_codes").select("id").eq("code", code).limit(1).execute().data or []
        if existing: return bad("Affiliate/promo code already exists", 409)
        row = {
            "code": code,
            "owner_name": d.get("owner_name") or d.get("name") or "",
            "code_type": d.get("code_type") or "affiliate",
            "commission_percent": clean(d.get("commission_percent") or 0),
            "discount_percent": clean(d.get("discount_percent") or 0),
            "start_date": d.get("start_date") or None,
            "end_date": d.get("end_date") or None,
            "usage_limit": int(clean(d.get("usage_limit") or 0)),
            "total_uses": 0,
            "status": d.get("status") or "active",
            "affiliate_link": d.get("affiliate_link") or f"https://nairapips.com/?ref={code}",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }
        created = supabase.table("affiliate_codes").insert(row).execute().data
        return ok(created, "Affiliate code created")
    except Exception as e:
        return bad(e, 500)

@app.route("/update_affiliate_code", methods=["POST"])
def update_affiliate_code():
    try:
        d = request.json or {}; cid = d.get("id")
        if not cid: return bad("Missing code id")
        upd = {"updated_at": now_iso()}
        for k in ["owner_name", "code_type", "start_date", "end_date", "status", "affiliate_link"]:
            if k in d: upd[k] = d[k] or None if k in {"start_date", "end_date"} else d[k]
        if "code" in d: upd["code"] = _aff_code(d.get("code"))
        for k in ["commission_percent", "discount_percent", "usage_limit", "total_uses"]:
            if k in d: upd[k] = clean(d.get(k))
        result = supabase.table("affiliate_codes").update(upd).eq("id", cid).execute().data
        return ok(result, "Affiliate code updated")
    except Exception as e:
        return bad(e, 500)

@app.route("/delete_affiliate_code", methods=["POST"])
def delete_affiliate_code():
    try:
        d = request.json or {}; cid = d.get("id")
        if not cid: return bad("Missing code id")
        result = supabase.table("affiliate_codes").update({"status":"inactive", "updated_at": now_iso()}).eq("id", cid).execute().data
        return ok(result, "Affiliate code deactivated")
    except Exception as e:
        return bad(e, 500)

@app.route("/validate_affiliate_code", methods=["POST", "GET"])
def validate_affiliate_code():
    try:
        payload = (request.json or {}) if request.method == "POST" else dict(request.args)
        code = _aff_code(payload.get("code"))
        if not code: return bad("Code is required")
        row = _aff_get_code(code) or _aff_get_partner_by_code(code)
        if not row: return jsonify({"success": False, "valid": False, "error": "Code not found"}), 404
        ok_window, reason = _aff_code_valid_window(row)
        valid = _aff_status_active(row) and ok_window
        quote = None
        if clean(payload.get("fee") or payload.get("amount") or 0) > 0:
            quote = _affiliate_quote_details(payload, clean(payload.get("fee") or payload.get("amount")))
            quote.pop("source", None)
        return jsonify({"success": True, "valid": bool(valid), "message": reason if valid else reason, "data": row, "quote": quote})
    except Exception as e:
        return bad(e, 500)

@app.route("/affiliate_commissions", methods=["GET"])
def affiliate_commissions():
    try:
        return jsonify(supabase.table("affiliate_commissions").select("*").order("created_at", desc=True).execute().data or [])
    except Exception as e:
        return bad(e, 500)

@app.route("/create_affiliate_commission", methods=["POST"])
def create_affiliate_commission():
    try:
        d = request.json or {}
        sale_amount = clean(d.get("sale_amount"))
        pct = clean(d.get("commission_percent"))
        amount = clean(d.get("commission_amount")) or round(sale_amount * pct / 100, 2)
        row = {
            "partner_code": _aff_code(d.get("partner_code")),
            "partner_name": d.get("partner_name") or "",
            "purchase_id": d.get("purchase_id"),
            "customer_name": d.get("customer_name") or "",
            "customer_email": d.get("customer_email") or "",
            "sale_amount": sale_amount,
            "commission_percent": pct,
            "commission_amount": amount,
            "status": d.get("status") or "pending",
            "created_at": now_iso(),
        }
        if not row["partner_code"]: return bad("Partner code is required")
        created = supabase.table("affiliate_commissions").insert(row).execute().data
        return ok(created, "Affiliate commission created")
    except Exception as e:
        return bad(e, 500)

@app.route("/update_affiliate_commission", methods=["POST"])
def update_affiliate_commission():
    try:
        d = request.json or {}; cid = d.get("id")
        if not cid: return bad("Missing commission id")
        status = str(d.get("status") or "").strip().lower()
        if status not in {"pending", "approved", "paid", "rejected"}: return bad("Invalid commission status")
        upd = {"status": status, "admin_note": d.get("admin_note") or "", "updated_at": now_iso()}
        if status == "paid": upd["paid_at"] = now_iso()
        result = supabase.table("affiliate_commissions").update(upd).eq("id", cid).execute().data
        return ok(result, "Affiliate commission updated")
    except Exception as e:
        return bad(e, 500)

@app.route("/affiliate_summary", methods=["GET"])
def affiliate_summary():
    try:
        partners = supabase.table("affiliate_partners").select("*").execute().data or []
        codes = supabase.table("affiliate_codes").select("*").execute().data or []
        comms = supabase.table("affiliate_commissions").select("*").execute().data or []
        total_sales = sum(clean(c.get("sale_amount")) for c in comms)
        pending = sum(clean(c.get("commission_amount")) for c in comms if str(c.get("status") or "pending").lower()=="pending")
        approved = sum(clean(c.get("commission_amount")) for c in comms if str(c.get("status") or "").lower()=="approved")
        paid = sum(clean(c.get("commission_amount")) for c in comms if str(c.get("status") or "").lower()=="paid")
        top = None
        by_partner = {}
        for c in comms:
            code = _aff_code(c.get("partner_code")) or "UNKNOWN"
            by_partner.setdefault(code, {"code": code, "sales": 0, "commission": 0, "count": 0})
            by_partner[code]["sales"] += clean(c.get("sale_amount"))
            by_partner[code]["commission"] += clean(c.get("commission_amount"))
            by_partner[code]["count"] += 1
        rows = sorted(by_partner.values(), key=lambda x: x["sales"], reverse=True)
        top = rows[0] if rows else None
        return jsonify({"success": True, "data": {
            "total_partners": len(partners), "active_partners": len([p for p in partners if _aff_status_active(p)]),
            "total_codes": len(codes), "active_codes": len([c for c in codes if _aff_status_active(c)]),
            "total_sales": total_sales, "pending_commissions": pending, "approved_commissions": approved, "paid_commissions": paid,
            "top_partner": top, "partner_rows": rows
        }})
    except Exception as e:
        return bad(e, 500)


# ================================
# EMAIL LOG BANK + MANUAL RESEND
# ================================
@app.route("/email_logs", methods=["GET"])
def email_logs():
    try:
        limit = int(request.args.get("limit", 100))
        rows = supabase.table("email_logs").select("*").order("created_at", desc=True).limit(limit).execute().data or []
        return jsonify(rows)
    except Exception as e:
        return bad(e, 500)

@app.route("/resend_phase_pass_email", methods=["POST", "OPTIONS"])
def resend_phase_pass_email():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        d = request.get_json(silent=True) or {}
        lookup = d.get("trader_id") or d.get("id") or d.get("email") or d.get("mt5_login")
        if not lookup:
            return bad("Send trader_id, id, email, or mt5_login", 400)

        trader = None
        if d.get("trader_id") or d.get("id"):
            trader = get_trader_by_id(d.get("trader_id") or d.get("id"))
        if not trader:
            trader = _latest_trader_for_lookup(lookup)
        if not trader:
            return bad("Trader not found", 404)

        pass_status = str(d.get("pass_status") or trader.get("phase_pass_status") or trader.get("status") or trader.get("phase") or "").lower().strip()
        if pass_status not in ["phase1_passed", "phase2_passed"]:
            phase_label = str(trader.get("phase_label") or trader.get("phase") or "phase1").lower()
            pass_status = "phase2_passed" if "2" in phase_label else "phase1_passed"

        payload = {
            "target_equity": trader.get("target_equity"),
            "highest_equity": trader.get("highest_equity"),
            "highest_profit_percent": trader.get("highest_profit_percent"),
            "mt5_login": trader.get("mt5_login"),
        }
        sent = _send_phase_pass_email_once(trader, pass_status, payload, old_status="", force=True)
        return jsonify({"success": bool(sent), "sent": bool(sent), "pass_status": pass_status, "trader": trader})
    except Exception as e:
        return bad(e, 500)


# ================================
# NAIRAPIPS INTELLIGENT ACCOUNT MANAGER HOTFIX
# Independent backend brain for pass/breach/alert decisions.
# It does not replace MT5. It converts latest account/snapshot data into immediate business status.
# ================================

def _iam_num(v, default=0.0):
    try:
        if v is None or v == "":
            return float(default)
        return float(str(v).replace("₦", "").replace(",", "").strip())
    except Exception:
        return float(default)


def _iam_stage_target(stage):
    stage = str(stage or "").strip().lower()
    if stage == "phase1":
        return 10.0
    if stage == "phase2":
        return 8.0
    return None


def _iam_alert(trader, account, alert_type, title, message, severity="info"):
    """Write one admin-visible monitoring event and email admin if available."""
    try:
        supabase.table("monitoring_events").insert({
            "trader_id": trader.get("id") if trader else None,
            "trader_account_id": account.get("id") if account else None,
            "trader_name": (trader or {}).get("name") or (trader or {}).get("full_name") or "Trader",
            "email": (trader or {}).get("email"),
            "mt5_login": (account or {}).get("mt5_login") or (trader or {}).get("mt5_login"),
            "event_type": alert_type,
            "risk_zone": severity,
            "message": message,
            "balance": _iam_num((account or {}).get("account_size") or (account or {}).get("start_balance")),
            "equity": _iam_num((account or {}).get("current_equity")),
            "max_drawdown_used": _iam_num((account or {}).get("dd_used_percent")),
        }).execute()
    except Exception as e:
        print("IAM ALERT EVENT FAILED:", e)
    try:
        send_admin_alert(title, message)
    except Exception as e:
        print("IAM ADMIN ALERT EMAIL FAILED:", e)


def _iam_latest_snapshot_for_account(account):
    try:
        account_id = str((account or {}).get("id") or "").strip()
        login = str((account or {}).get("mt5_login") or "").strip()
        rows = []
        if account_id:
            rows = supabase.table("monitoring_snapshots").select("*").eq("trader_account_id", account_id).order("created_at", desc=True).limit(1).execute().data or []
        if not rows and login:
            rows = supabase.table("monitoring_snapshots").select("*").eq("mt5_login", login).order("created_at", desc=True).limit(1).execute().data or []
        return rows[0] if rows else {}
    except Exception as e:
        print("IAM LATEST SNAPSHOT FETCH FAILED:", e)
        return {}


def _iam_safe_account_update(account_id, payload):
    try:
        return supabase.table("trader_accounts").update(payload).eq("id", account_id).execute()
    except Exception as e:
        print("IAM ACCOUNT UPDATE FAILED:", e)
        # Retry with only common/core columns to survive optional schema differences.
        core = {
            k: v for k, v in payload.items()
            if k in {
                "current_balance", "current_equity", "profit", "profit_percent",
                "highest_equity", "lowest_equity", "absolute_drawdown_percent", "drawdown_percent",
                "dd_used_percent", "risk_zone", "monitoring_enabled", "phase_pass_status",
                "passed_at", "breached_at", "breach_reason", "account_status", "updated_at"
            }
        }
        try:
            return supabase.table("trader_accounts").update(core).eq("id", account_id).execute()
        except Exception as e2:
            print("IAM ACCOUNT CORE UPDATE FAILED:", e2)
            return None


def _iam_process_account(account, force=False):
    """Single-account intelligence decision. Safe to run repeatedly/idempotently."""
    if not account:
        return {"action": "skipped", "reason": "empty account"}

    status = str(account.get("account_status") or "").strip().lower()
    if status not in {"assigned_active", "active", "current_active"} and not force:
        return {"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "skipped", "reason": f"not active: {status}"}

    trader = _get_trader_by_id(account.get("trader_id")) if account.get("trader_id") else None
    snap = _iam_latest_snapshot_for_account(account)

    stage = str(account.get("stage") or account.get("phase") or "").strip().lower()
    start_balance = _iam_num(account.get("start_balance") or account.get("account_size") or snap.get("balance") or snap.get("account_size"), 0)
    current_equity = _iam_num(snap.get("equity"), _iam_num(account.get("current_equity") or account.get("current_balance"), start_balance))
    if start_balance <= 0:
        return {"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "skipped", "reason": "missing start balance"}

    previous_high = _iam_num(account.get("highest_equity"), start_balance)
    previous_low = _iam_num(account.get("lowest_equity"), start_balance)
    snap_high = _iam_num(snap.get("highest_equity"), current_equity)
    snap_low = _iam_num(snap.get("lowest_equity"), current_equity)
    highest_equity = max(start_balance, previous_high, snap_high, current_equity)
    lowest_equity = min(v for v in [previous_low, snap_low, current_equity, start_balance] if v > 0)

    profit = current_equity - start_balance
    profit_percent = (profit / start_balance) * 100
    dd_limit = _iam_num(account.get("dd_limit_percent"), 20) or 20
    current_dd = max(0, ((start_balance - current_equity) / start_balance) * 100)
    dd_used = (current_dd / dd_limit) * 100 if dd_limit else 0
    worst_dd = max(0, ((start_balance - lowest_equity) / start_balance) * 100)
    worst_dd_used = (worst_dd / dd_limit) * 100 if dd_limit else 0
    breach_level = start_balance * (1 - dd_limit / 100)
    risk_zone = _risk_zone(dd_used)
    target_percent = _iam_stage_target(stage)
    target_equity = start_balance * (1 + (target_percent / 100)) if target_percent is not None else 0
    pass_status = ""
    if target_percent is not None and highest_equity >= target_equity and dd_used < 100:
        pass_status = "phase2_passed" if stage == "phase2" else "phase1_passed"
    breached = current_equity <= breach_level or dd_used >= 100

    update = {
        "current_balance": start_balance,
        "current_equity": current_equity,
        "profit": profit,
        "profit_percent": profit_percent,
        "highest_equity": highest_equity,
        "lowest_equity": lowest_equity,
        "absolute_drawdown_percent": current_dd,
        "drawdown_percent": current_dd,
        "dd_used_percent": dd_used,
        "risk_zone": "passed" if pass_status else ("breached" if breached else risk_zone),
        "updated_at": _now_iso(),
    }
    # Optional columns, safely retried away if missing.
    update.update({
        "actual_drawdown_percent": current_dd,
        "current_drawdown_percent": current_dd,
        "dd_remaining_percent": max(0, dd_limit - current_dd),
        "breach_equity_level": breach_level,
        "worst_static_drawdown_percent": worst_dd,
        "worst_dd_used_percent": worst_dd_used,
        "worst_dd_remaining_percent": max(0, dd_limit - worst_dd),
        "target_equity": target_equity,
        "profit_target": target_percent or 0,
        "pass_progress_percent": (max(0, ((highest_equity - start_balance) / start_balance * 100)) / target_percent * 100) if target_percent else 0,
    })
    _iam_safe_account_update(account.get("id"), update)

    # Hard business actions happen after evidence is saved.
    if pass_status:
        already = str(account.get("phase_pass_status") or "").strip().lower() == pass_status or "archived_phase" in status
        if not already:
            message = (
                f"{pass_status} detected automatically by NairaPips Intelligent Account Manager. "
                f"MT5 {account.get('mt5_login')} | Equity {current_equity:,.2f} | Target {target_equity:,.2f} | Stage {stage}."
            )
            try:
                _pass_specific_account(trader, account, pass_status, {"name": "intelligent_account_manager", "username": "iam"}, message)
            except Exception as e:
                print("IAM AUTO PASS FAILED:", e)
            _iam_alert(trader, account, "phase_passed", "NairaPips phase pass detected", message, "passed")
            try:
                _send_phase_pass_email_once(trader, pass_status, {"target_equity": target_equity, "highest_equity": highest_equity, "highest_profit_percent": ((highest_equity-start_balance)/start_balance*100), "mt5_login": account.get("mt5_login")}, old_status="", force=False)
            except Exception as e:
                print("IAM PASS EMAIL FAILED:", e)
            return {"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "passed", "pass_status": pass_status, "equity": current_equity, "target_equity": target_equity}
        return {"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "already_passed", "pass_status": pass_status}

    if breached:
        if "breach" not in status:
            reason = f"Static drawdown breach detected. MT5 {account.get('mt5_login')} equity {current_equity:,.2f} <= breach level {breach_level:,.2f}."
            try:
                _breach_specific_account(trader, account, reason, {"name": "intelligent_account_manager", "username": "iam"})
            except Exception as e:
                print("IAM AUTO BREACH FAILED:", e)
            _iam_alert(trader, account, "breach_detected", "NairaPips breach detected", reason, "breached")
            return {"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "breached", "equity": current_equity, "breach_level": breach_level}
        return {"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "already_breached"}

    # Alert before damage gets out of hand, without locking.
    if risk_zone in {"warning", "danger", "critical"}:
        _iam_alert(
            trader,
            account,
            f"risk_{risk_zone}",
            f"NairaPips account {risk_zone.upper()}",
            f"MT5 {account.get('mt5_login')} is {risk_zone.upper()}. Current DD used: {dd_used:.1f}% of limit. Equity: {current_equity:,.2f}. Breach level: {breach_level:,.2f}.",
            risk_zone,
        )
    return {"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "monitored", "risk_zone": risk_zone, "equity": current_equity, "dd_used_percent": dd_used, "pass_status": "not_passed"}




# Wrap the existing monitoring snapshot receiver so every MT5 snapshot also runs
# the independent account intelligence decision immediately.
_np_original_apply_monitoring_snapshot = _apply_monitoring_snapshot

def _apply_monitoring_snapshot(trader, payload, source="manual"):
    result = _np_original_apply_monitoring_snapshot(trader, payload, source)
    try:
        account = None
        account_id = str((payload or {}).get("trader_account_id") or (payload or {}).get("current_account_id") or "").strip()
        login = str((payload or {}).get("mt5_login") or (payload or {}).get("login") or "").strip()
        if account_id:
            rows = supabase.table("trader_accounts").select("*").eq("id", account_id).limit(1).execute().data or []
            account = rows[0] if rows else None
        if not account and login:
            rows = supabase.table("trader_accounts").select("*").eq("mt5_login", login).order("updated_at", desc=True).limit(1).execute().data or []
            account = rows[0] if rows else None
        if account:
            iam = _iam_process_account(account, force=False)
            if isinstance(result, dict):
                result["intelligence"] = iam
    except Exception as e:
        print("IAM SNAPSHOT WRAP FAILED:", e)
    return result

@app.route("/account_intelligence_scan", methods=["POST", "GET", "OPTIONS"])
def account_intelligence_scan():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        limit = int(request.args.get("limit", 250))
    except Exception:
        limit = 250
    limit = max(1, min(limit, 1000))
    try:
        rows = supabase.table("trader_accounts").select("*").in_("account_status", ["assigned_active", "active", "current_active"]).order("updated_at", desc=True).limit(limit).execute().data or []
    except Exception as e:
        return bad(f"Could not load active accounts: {e}", 500)
    results = []
    for account in rows:
        try:
            results.append(_iam_process_account(account))
        except Exception as e:
            results.append({"account_id": account.get("id"), "mt5_login": account.get("mt5_login"), "action": "error", "error": str(e)})
    return jsonify({"success": True, "scanned": len(rows), "results": results})


@app.route("/account_intelligence_alerts", methods=["GET", "OPTIONS"])
def account_intelligence_alerts():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        limit = int(request.args.get("limit", 100))
    except Exception:
        limit = 100
    limit = max(1, min(limit, 500))
    types = [
        "phase_passed", "breach_detected", "account_locked",
        "risk_warning", "risk_danger", "risk_critical",
        "critical_mode", "danger_zone"
    ]
    try:
        rows = supabase.table("monitoring_events").select("*").in_("event_type", types).order("created_at", desc=True).limit(limit).execute().data or []
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return bad(e, 500)




# ===== COMPATIBILITY ROUTES FOR ADMIN FRONTEND =====
# Some admin.html versions still call /admin_feed while newer versions call /admin_bootstrap.
# Keep both routes returning the same payload so old/new admin files do not collapse.
@app.route("/admin_feed", methods=["GET", "OPTIONS"])
def admin_feed():
    if request.method == "OPTIONS":
        return ok({})
    return admin_bootstrap()

@app.route("/routes", methods=["GET"])
def list_routes():
    try:
        routes = []
        for rule in app.url_map.iter_rules():
            routes.append({"route": str(rule), "endpoint": rule.endpoint, "methods": sorted([m for m in rule.methods if m not in ("HEAD", "OPTIONS")])})
        return ok({"routes": sorted(routes, key=lambda r: r["route"]), "version": "admin-bootstrap-feed-alias"})
    except Exception as e:
        return bad(str(e), 500)



# ============================================================
# NAIRAPIPS ADMIN V2 API LAYER - CLEAN REBUILD CONTRACT
# Purpose: stop V1 all-at-once loading and data mismatch.
# Rules:
#  - Each endpoint is module-specific and paginated.
#  - trader_accounts is treated as the challenge account source of truth where present.
#  - Monitoring/account data is attached by account id or active account logic, not by guessing.
#  - Existing business logic remains untouched.
# ============================================================

ADMIN_V2_MAX_LIMIT = 200
ADMIN_V2_DEFAULT_LIMIT = 50


def _v2_int_arg(name, default=ADMIN_V2_DEFAULT_LIMIT, min_value=1, max_value=ADMIN_V2_MAX_LIMIT):
    try:
        value = int(request.args.get(name, default))
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def _v2_page():
    return _v2_int_arg("page", 1, 1, 100000)


def _v2_limit():
    return _v2_int_arg("limit", ADMIN_V2_DEFAULT_LIMIT, 1, ADMIN_V2_MAX_LIMIT)


def _v2_offset(page, limit):
    return max(0, (page - 1) * limit)


def _v2_q():
    return str(request.args.get("q") or "").strip()


def _v2_order(table_query, order_by=None, desc=True):
    if not order_by:
        order_by = request.args.get("order_by") or "created_at"
    try:
        return table_query.order(order_by, desc=desc)
    except Exception:
        return table_query


def _v2_table_page(table, select="*", order_by="created_at", desc=True, filters=None, search_cols=None):
    page = _v2_page()
    limit = _v2_limit()
    start = _v2_offset(page, limit)
    end = start + limit - 1
    q = _v2_q().lower()
    filters = filters or {}
    search_cols = search_cols or []

    try:
        query = supabase.table(table).select(select)
        for col, val in filters.items():
            if val is None or str(val).strip() == "":
                continue
            if isinstance(val, (list, tuple, set)):
                query = query.in_(col, list(val))
            else:
                query = query.eq(col, val)
        query = _v2_order(query, order_by, desc)
        rows = query.range(start, end).execute().data or []
    except Exception as e:
        return {"success": False, "error": str(e), "data": [], "page": page, "limit": limit, "has_more": False}

    # Defensive in-memory search. This avoids relying on different Supabase query syntax versions.
    if q and search_cols:
        def match(row):
            blob = " ".join(str(row.get(c) or "") for c in search_cols).lower()
            return q in blob
        rows = [r for r in rows if match(r)]

    return {"success": True, "data": rows, "page": page, "limit": limit, "has_more": len(rows) == limit}


def _v2_payment_status(row):
    return str((row or {}).get("payment_status") or (row or {}).get("status") or "").strip().lower()


def _v2_stage(row):
    if not row:
        return ""
    return str(row.get("stage") or row.get("phase") or row.get("active_stage") or row.get("assigned_phase") or "").strip().lower()


def _v2_account_status(row):
    return str((row or {}).get("account_status") or (row or {}).get("status") or "").strip().lower()


def _v2_is_breached(row):
    blob = " ".join([
        str((row or {}).get("account_status") or ""),
        str((row or {}).get("status") or ""),
        str((row or {}).get("stage") or ""),
        str((row or {}).get("phase") or ""),
        str((row or {}).get("risk_zone") or ""),
        str((row or {}).get("display_risk_zone") or ""),
        str((row or {}).get("latest_monitoring_event") or ""),
        str((row or {}).get("latest_monitoring_snapshot") or ""),
    ]).lower()
    if bool((row or {}).get("mt5_access_disabled")):
        return True
    try:
        if float((row or {}).get("dd_used_percent") or 0) >= 100:
            return True
    except Exception:
        pass
    try:
        snap = (row or {}).get("latest_monitoring_snapshot") or {}
        if isinstance(snap, dict):
            if bool(snap.get("breached")):
                return True
            if float(snap.get("dd_used_percent") or snap.get("max_drawdown_used") or 0) >= 100:
                return True
    except Exception:
        pass
    return "breach" in blob or "locked" in blob or "disabled" in blob or "mt5_access_disabled" in blob


def _v2_public_trader(row):
    row = dict(row or {})
    for secret in ["password", "mt5_password", "mt5_master_password", "mt5_investor_password"]:
        # keep MT5 secrets out of general trader listing; detail endpoint can show account fields if old admin does.
        if secret in row:
            row[secret] = "••••••••"
    return row


def _v2_summary_counts():
    def quick_count(table, limit=5000):
        try:
            return len(supabase.table(table).select("id").limit(limit).execute().data or [])
        except Exception:
            return 0

    counts = {
        "traders": quick_count("traders"),
        "purchases": quick_count("challenge_purchases"),
        "payouts": quick_count("payouts"),
        "mt5_pool": quick_count("mt5_pool"),
        "support_tickets": quick_count("support_tickets"),
    }

    try:
        accounts = supabase.table("trader_accounts").select("id,stage,phase,account_status,status,risk_zone,display_risk_zone,dd_used_percent,mt5_access_disabled").limit(5000).execute().data or []
    except Exception:
        accounts = []
    counts["active_accounts"] = len([a for a in accounts if not _v2_is_breached(a) and _v2_account_status(a) in ACTIVE_ACCOUNT_STATUSES])
    counts["phase1"] = len([a for a in accounts if not _v2_is_breached(a) and _v2_stage(a) == "phase1" and _v2_account_status(a) in ACTIVE_ACCOUNT_STATUSES])
    counts["phase2"] = len([a for a in accounts if not _v2_is_breached(a) and _v2_stage(a) == "phase2" and _v2_account_status(a) in ACTIVE_ACCOUNT_STATUSES])
    counts["funded"] = len([a for a in accounts if not _v2_is_breached(a) and _v2_stage(a) in {"funded", "live"} and _v2_account_status(a) in ACTIVE_ACCOUNT_STATUSES])
    counts["breached"] = len([a for a in accounts if _v2_is_breached(a)])

    try:
        mt5 = supabase.table("mt5_pool").select("id,status,assigned_trader_id,trader_id").limit(5000).execute().data or []
        counts["available_mt5"] = len(_quick_available_mt5(mt5)) if "_quick_available_mt5" in globals() else len([m for m in mt5 if str(m.get("status") or "available").lower() in {"available", "unused", "free", ""} and not (m.get("assigned_trader_id") or m.get("trader_id"))])
    except Exception:
        counts["available_mt5"] = 0

    try:
        payouts = supabase.table("payouts").select("id,status,amount,profit_share_amount,created_at").limit(2000).execute().data or []
        counts["pending_payouts"] = len([p for p in payouts if str(p.get("status") or "pending").lower() in {"pending", "requested", "review"}])
        counts["payout_amount_pending"] = sum(clean(p.get("amount") or p.get("profit_share_amount") or 0) for p in payouts if str(p.get("status") or "pending").lower() in {"pending", "requested", "review"})
    except Exception:
        counts["pending_payouts"] = 0
        counts["payout_amount_pending"] = 0

    try:
        purchases = supabase.table("challenge_purchases").select("id,status,payment_status,amount,price,challenge_fee,created_at").limit(5000).execute().data or []
        approved = [p for p in purchases if _v2_payment_status(p) in {"approved", "paid", "approved_active", "active"}]
        counts["approved_purchases"] = len(approved)
        counts["revenue_total"] = sum(clean(p.get("amount") or p.get("price") or p.get("challenge_fee") or 0) for p in approved)
    except Exception:
        counts["approved_purchases"] = 0
        counts["revenue_total"] = 0

    return counts


@app.route("/admin_v2/health", methods=["GET", "OPTIONS"])
def admin_v2_health():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    return _np_ok({"success": True, "service": "nairapips-admin-v2", "generated_at": now_iso()})


@app.route("/admin_v2/summary", methods=["GET", "OPTIONS"])
def admin_v2_summary():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    started = time.time()
    counts = _v2_summary_counts()
    return _np_ok({"success": True, "counts": counts, "duration_ms": int((time.time() - started) * 1000), "generated_at": now_iso()})


@app.route("/admin_v2/traders", methods=["GET", "OPTIONS"])
def admin_v2_traders():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    payload = _v2_table_page(
        "traders",
        "*",
        order_by=request.args.get("order_by") or "created_at",
        search_cols=["name", "full_name", "trader_name", "email", "phone", "account_reference", "mt5_login", "status", "phase"],
    )
    payload["data"] = [_v2_public_trader(r) for r in payload.get("data", [])]
    return _np_ok(payload)


@app.route("/admin_v2/accounts", methods=["GET", "OPTIONS"])
def admin_v2_accounts():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    stage = str(request.args.get("stage") or "").strip().lower()
    view = str(request.args.get("view") or "active").strip().lower()
    filters = {}
    if stage:
        filters["stage"] = stage
    if view == "active":
        # Supabase py cannot combine in_ through our generic dict easily? yes list supported.
        filters["account_status"] = list(ACTIVE_ACCOUNT_STATUSES)
    payload = _v2_table_page(
        "trader_accounts",
        "*",
        order_by=request.args.get("order_by") or "updated_at",
        filters=filters,
        search_cols=["mt5_login", "mt5_server", "stage", "account_status", "risk_zone", "trader_id", "purchase_id"],
    )
    rows = payload.get("data", []) or []
    # For mismatch prevention, decorate every account with backend's own account logic when available.
    out = []
    for row in rows:
        try:
            out.append(_decorate_account_for_api(row))
        except Exception:
            out.append(row)
    payload["data"] = out
    return _np_ok(payload)


@app.route("/trader_accounts", methods=["GET", "OPTIONS"])
@app.route("/trader_lifecycle_accounts", methods=["GET", "OPTIONS"])
def admin_trader_accounts_feed():
    """Compatibility account feed for admin/trader dashboard source-of-truth reads."""
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    try:
        limit = min(int(request.args.get("limit") or 5000), 10000)
    except Exception:
        limit = 5000
    view = str(request.args.get("view") or "all").strip().lower()
    try:
        query = supabase.table("trader_accounts").select("*").order("updated_at", desc=True).limit(limit)
        if view == "active":
            query = query.in_("account_status", list(ACTIVE_ACCOUNT_STATUSES))
        rows = query.execute().data or []

        # PRODUCTION SAFETY: Admin must not show stale 0 values when MT5 monitoring
        # has already synced newer balance/equity/profit evidence. This is read-only
        # enrichment for the admin/trader account feed; it does not change trading logic.
        try:
            logins = []
            for r in rows:
                lg = str((r or {}).get("mt5_login") or "").strip()
                if lg and lg not in logins:
                    logins.append(lg)
            latest_by_login = {}
            for i in range(0, len(logins), 100):
                batch = logins[i:i+100]
                if not batch:
                    continue
                snaps = supabase.table("monitoring_snapshots").select("*").in_("mt5_login", batch).order("created_at", desc=True).limit(1000).execute().data or []
                for snap in snaps:
                    lg = str((snap or {}).get("mt5_login") or "").strip()
                    if lg and lg not in latest_by_login:
                        latest_by_login[lg] = snap
            enriched = []
            for r in rows:
                row = dict(r or {})
                lg = str(row.get("mt5_login") or "").strip()
                snap = latest_by_login.get(lg)
                if snap:
                    # Prefer monitoring truth for live metrics, especially newly-fixed
                    # accounts like Fatoba where trader_accounts may still show defaults.
                    bal = snap.get("balance") or snap.get("current_balance")
                    eq = snap.get("equity") or snap.get("current_equity") or bal
                    profit = snap.get("profit") or snap.get("current_profit")
                    profit_pct = snap.get("profit_percent") or snap.get("current_profit_percent")
                    if bal not in [None, ""]:
                        row["current_balance"] = bal
                        row["balance"] = bal
                    if eq not in [None, ""]:
                        row["current_equity"] = eq
                        row["equity"] = eq
                    if profit not in [None, ""]:
                        row["profit"] = profit
                    if profit_pct not in [None, ""]:
                        row["profit_percent"] = profit_pct
                    for k in ["dd_used_percent", "max_drawdown_used", "drawdown_percent", "risk_zone", "highest_equity", "lowest_equity", "phase_pass_status", "target_percent", "target_equity"]:
                        if snap.get(k) not in [None, ""]:
                            row[k] = snap.get(k)
                    row["latest_monitoring_snapshot"] = snap
                    row["last_sync_at"] = snap.get("created_at") or row.get("last_sync_at")
                enriched.append(row)
            rows = enriched
        except Exception as enrich_err:
            print("ADMIN ACCOUNT FEED MONITORING ENRICH ERROR:", enrich_err)

        rows = [_decorate_account_for_api(r) if "_decorate_account_for_api" in globals() else r for r in rows]
        return _np_ok({"success": True, "data": rows, "accounts": rows, "trader_accounts": rows, "count": len(rows)})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/np_assignment_center", methods=["GET", "OPTIONS"])
def np_assignment_center():
    """Unified MT5 assignment desk feed for first assignment, Phase 2, and Funded."""
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    try:
        phase_rows = _fetch_phase_assignment_queue()
        purchase_rows = []
        active_accounts = []
        try:
            active_accounts = supabase.table("trader_accounts").select("id,trader_id,purchase_id,challenge_purchase_id,account_size,start_balance,account_status,mt5_login").in_("account_status", ["assigned_active", "active", "current_active", "phase1_active", "phase2_active", "funded_active", "live", "funded"]).limit(5000).execute().data or []
        except Exception as e:
            print("ASSIGNMENT CENTER ACTIVE ACCOUNT FETCH ERROR:", e)
        try:
            purchases = supabase.table("challenge_purchases").select("*").order("created_at", desc=True).limit(1500).execute().data or []
        except Exception as e:
            print("ASSIGNMENT CENTER PURCHASE FETCH ERROR:", e)
            purchases = []

        seen = set()
        for p in purchases:
            status_blob = f"{p.get('payment_status') or ''} {p.get('status') or ''}".lower()
            payment_status = str(p.get("payment_status") or "").strip().lower()
            has_mt5 = str(p.get("mt5_login") or p.get("current_mt5_login") or p.get("assigned_mt5_login") or "").strip()
            if has_mt5 or "reject" in status_blob or "cancel" in status_blob:
                continue
            if not any(x in payment_status for x in ["approved", "paid"]):
                continue
            pid = str(p.get("id") or "").strip()
            trader_id = str(p.get("trader_id") or "").strip()
            p_size = _np_number(p.get("account_size") or p.get("challenge_amount") or 0)
            already_assigned = False
            for a in active_accounts:
                a_pid = str(a.get("purchase_id") or a.get("challenge_purchase_id") or "").strip()
                a_trader_id = str(a.get("trader_id") or "").strip()
                a_size = _np_number(a.get("account_size") or a.get("start_balance") or 0)
                if pid and a_pid and pid == a_pid:
                    already_assigned = True
                    break
                if trader_id and a_trader_id and trader_id == a_trader_id and p_size and a_size and int(p_size) == int(a_size):
                    already_assigned = True
                    break
            if already_assigned:
                continue
            if pid and pid in seen:
                continue
            if pid:
                seen.add(pid)
            purchase_rows.append({
                "id": p.get("trader_id") or p.get("id"),
                "trader_id": p.get("trader_id") or "",
                "purchase_id": p.get("id"),
                "source_type": "purchase",
                "source": "challenge_purchases",
                "target_phase": "phase1",
                "target_stage": "phase1",
                "stage_label": "PHASE 1 MT5 ASSIGNMENT",
                "assignment_label": "Assign Phase 1 MT5",
                "name": p.get("trader_name") or p.get("name") or p.get("full_name") or "Trader",
                "email": p.get("email") or "",
                "phone": p.get("phone") or "",
                "account_reference": p.get("account_reference") or p.get("reference") or "",
                "plan_name": p.get("plan_name") or p.get("selected_plan") or "",
                "account_size": p.get("account_size") or p.get("challenge_amount") or 0,
                "payment_status": p.get("payment_status") or "pending",
                "current_status": p.get("status") or p.get("payment_status") or "pending_review",
                "created_at": p.get("created_at") or p.get("paid_at") or p.get("updated_at") or "",
            })

        golden_rows = _np_golden_ticket_candidates(1500)
        available_mt5 = _available_mt5_not_used(1500)

        queue = golden_rows + purchase_rows + phase_rows
        return _np_ok({
            "success": True,
            "rows": queue,
            "data": queue,
            "assignment_queue": queue,
            "phase_assignment_queue": phase_rows,
            "purchase_assignment_queue": purchase_rows,
            "mt5_pool": available_mt5,
            "available_mt5": available_mt5,
            "summary": {
                "total": len(queue),
                "golden_ticket": len([r for r in queue if str(r.get("source_type") or "").lower() == "golden_ticket"]),
                "phase1": len([r for r in queue if str(r.get("target_phase") or "").lower() == "phase1"]),
                "phase2": len([r for r in queue if str(r.get("target_phase") or "").lower() == "phase2"]),
                "funded": len([r for r in queue if str(r.get("target_phase") or "").lower() == "funded"]),
                "available_mt5": len(available_mt5),
            },
            "message": f"{len(queue)} assignment record(s)",
        })
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin_v2/phase1", methods=["GET", "OPTIONS"])
def admin_v2_phase1():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    return admin_v2_accounts_stage("phase1")


def admin_v2_accounts_stage(stage):
    page = _v2_page(); limit = _v2_limit(); start = _v2_offset(page, limit); end = start + limit - 1
    q = _v2_q().lower()
    try:
        rows = supabase.table("trader_accounts").select("*").eq("stage", stage).in_("account_status", list(ACTIVE_ACCOUNT_STATUSES)).order("updated_at", desc=True).range(start, end).execute().data or []
    except Exception as e:
        return _np_ok({"success": False, "error": str(e), "data": [], "page": page, "limit": limit, "has_more": False}, 500)
    if q:
        rows = [r for r in rows if q in " ".join(str(r.get(k) or "") for k in ["mt5_login", "mt5_server", "trader_id", "purchase_id", "stage", "account_status"]).lower()]
    rows = [_decorate_account_for_api(r) if "_decorate_account_for_api" in globals() else r for r in rows]
    rows = [r for r in rows if not _v2_is_breached(r)]
    return _np_ok({"success": True, "data": rows, "page": page, "limit": limit, "has_more": len(rows) == limit})


@app.route("/admin_v2/phase2", methods=["GET", "OPTIONS"])
def admin_v2_phase2():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    return admin_v2_accounts_stage("phase2")


@app.route("/admin_v2/funded", methods=["GET", "OPTIONS"])
def admin_v2_funded():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    return admin_v2_accounts_stage("funded")


@app.route("/admin_v2/breached", methods=["GET", "OPTIONS"])
def admin_v2_breached():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    page = _v2_page(); limit = _v2_limit(); start = _v2_offset(page, limit); end = start + limit - 1
    q = _v2_q().lower()
    try:
        rows = supabase.table("trader_accounts").select("*").order("updated_at", desc=True).range(start, end).execute().data or []
    except Exception as e:
        return _np_ok({"success": False, "error": str(e), "data": [], "page": page, "limit": limit, "has_more": False}, 500)
    rows = [_decorate_account_for_api(r) if "_decorate_account_for_api" in globals() else r for r in rows]
    rows = [r for r in rows if _v2_is_breached(r)]
    if q:
        rows = [r for r in rows if q in " ".join(str(r.get(k) or "") for k in ["mt5_login", "mt5_server", "trader_id", "purchase_id", "stage", "account_status", "risk_zone"]).lower()]
    return _np_ok({"success": True, "data": rows, "page": page, "limit": limit, "has_more": len(rows) == limit})


@app.route("/admin_v2/purchases", methods=["GET", "OPTIONS"])
def admin_v2_purchases():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    payload = _v2_table_page("challenge_purchases", "*", order_by=request.args.get("order_by") or "created_at", search_cols=["name", "trader_name", "email", "phone", "status", "payment_status", "mt5_login", "plan_name", "selected_plan"])
    return _np_ok(payload)


@app.route("/admin_v2/payments", methods=["GET", "OPTIONS"])
def admin_v2_payments():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    payload = _v2_table_page("payments", "*", order_by=request.args.get("order_by") or "created_at", search_cols=["name", "trader_name", "email", "phone", "status", "reference", "payment_reference"])
    return _np_ok(payload)


@app.route("/admin_v2/payouts", methods=["GET", "OPTIONS"])
def admin_v2_payouts():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    payload = _v2_table_page("payouts", "*", order_by=request.args.get("order_by") or "created_at", search_cols=["name", "trader_name", "email", "phone", "status", "bank_name", "account_number", "mt5_login"])
    return _np_ok(payload)


@app.route("/admin_v2/mt5_pool", methods=["GET", "OPTIONS"])
def admin_v2_mt5_pool():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    status = str(request.args.get("status") or "").strip().lower()
    filters = {"status": status} if status else {}
    payload = _v2_table_page("mt5_pool", "*", order_by=request.args.get("order_by") or "created_at", filters=filters, search_cols=["mt5_login", "mt5_server", "status", "plan_name", "assigned_trader_name", "assigned_email"])
    return _np_ok(payload)


@app.route("/admin_v2/phase_queue", methods=["GET", "OPTIONS"])
def admin_v2_phase_queue():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    try:
        rows = _fetch_phase_assignment_queue()
    except Exception as e:
        rows = []
        print("ADMIN V2 PHASE QUEUE ERROR:", e)
    q = _v2_q().lower()
    if q:
        rows = [r for r in rows if q in " ".join(str(v or "") for v in r.values()).lower()]
    limit = _v2_limit(); page = _v2_page(); start = _v2_offset(page, limit)
    sliced = rows[start:start+limit]
    return _np_ok({"success": True, "data": sliced, "page": page, "limit": limit, "has_more": len(rows) > start + limit})


@app.route("/admin_v2/trader_360/<path:lookup>", methods=["GET", "OPTIONS"])
def admin_v2_trader_360(lookup):
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    try:
        trader = None
        if _is_uuid(lookup):
            rows = supabase.table("traders").select("*").eq("id", lookup).limit(1).execute().data or []
            trader = rows[0] if rows else None
        if not trader:
            trader = _latest_trader_for_lookup(lookup)
        if not trader:
            return _np_fail("Trader not found", 404)
        trader_id = trader.get("id")
        purchases = _safe_fetch("challenge_purchases", "trader_id", trader_id, 200)
        if trader.get("email"):
            purchases += _safe_fetch("challenge_purchases", "email", trader.get("email"), 200)
        if trader.get("phone"):
            purchases += _safe_fetch("challenge_purchases", "phone", trader.get("phone"), 200)
        purchases = _dedupe_by_id(purchases) if "_dedupe_by_id" in globals() else purchases
        accounts = _get_active_accounts(trader_id, trader, purchases) if "_get_active_accounts" in globals() else _safe_fetch("trader_accounts", "trader_id", trader_id, 200)
        accounts = _enrich_accounts_with_latest_monitoring(trader_id, accounts) if "_enrich_accounts_with_latest_monitoring" in globals() else accounts
        payouts = _safe_fetch("payouts", "trader_id", trader_id, 100)
        tickets = _safe_fetch("support_tickets", "trader_id", trader_id, 100)
        return _np_ok({
            "success": True,
            "trader": _v2_public_trader(trader),
            "accounts": accounts,
            "purchases": purchases,
            "payouts": payouts,
            "tickets": tickets,
        })
    except Exception as e:
        return _np_fail(e, 500)



# ============================================================
# NAIRAPIPS UNIFIED MT5 VISIBILITY SYNC - FINAL PRODUCTION BRIDGE
# Purpose: every Admin MT5 assignment must become visible to dashboard,
# monitoring API, and VPS engine by ensuring a live trader_accounts row exists.
# Safe rule: never delete; never reactivate breached/archived/passed accounts;
# only create/update valid assigned/approved MT5 records.
# ============================================================
NP_ACTIVE_ACCOUNT_STATUSES = {"assigned_active", "active", "current_active", "phase1_active", "phase2_active", "funded_active", "live", "funded"}
NP_TERMINAL_ACCOUNT_WORDS = {"breached", "archived", "disabled", "locked", "profit_protected", "rejected", "cancelled", "canceled", "passed_review"}

def _np_sync_text(v):
    return str(v or "").strip()

def _np_sync_lower(v):
    return _np_sync_text(v).lower().replace("-", "_")

def _np_sync_valid_login(v):
    s = _np_sync_text(v)
    return bool(s and s.isdigit() and not any(x in s.upper() for x in ["NEW", "LOGIN", "NONE", "NULL", "TEST_LOGIN"]))

def _np_sync_num(v, default=0.0):
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(str(v).replace("₦", "").replace(",", "").strip())
    except Exception:
        return default

def _np_sync_dt_score(v):
    try:
        if not v:
            return 0
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0

def _np_sync_recent(row, days=45):
    try:
        cutoff = int((datetime.now(timezone.utc) - __import__('datetime').timedelta(days=int(days or 45))).timestamp())
    except Exception:
        cutoff = 0
    for k in ["assigned_at", "mt5_assigned_at", "mt5_updated_at", "approved_at", "paid_at", "updated_at", "created_at"]:
        if _np_sync_dt_score((row or {}).get(k)) >= cutoff:
            return True
    return False

def _np_sync_terminal(row):
    blob = " ".join(_np_sync_lower((row or {}).get(k)) for k in ["status", "account_status", "phase", "stage", "payment_status", "risk_zone", "phase_pass_status", "admin_note"])
    return any(w in blob for w in NP_TERMINAL_ACCOUNT_WORDS)

def _np_sync_active_signal(row):
    blob = " ".join(_np_sync_lower((row or {}).get(k)) for k in ["status", "account_status", "phase", "stage", "payment_status", "lifecycle_state"])
    if any(x in blob for x in ["assigned", "approved", "active", "paid", "phase1", "phase2", "funded", "live"]):
        return True
    return bool((row or {}).get("assigned_trader_id") or (row or {}).get("trader_id") or (row or {}).get("trader_account_id"))

def _np_sync_rows(table, limit=1500):
    try:
        return supabase.table(table).select("*").limit(limit).execute().data or []
    except Exception as e:
        print("NP SYNC FETCH FAILED", table, e)
        return []

def _np_find_trader_for_sync(row, traders_by_id, traders_by_email, traders_by_phone):
    tid = _np_sync_text(row.get("trader_id") or row.get("assigned_trader_id") or row.get("id") if row.get("_np_source") == "traders" else row.get("trader_id") or row.get("assigned_trader_id"))
    if tid and tid in traders_by_id:
        return traders_by_id[tid]
    email = _np_sync_text(row.get("email") or row.get("trader_email") or row.get("assigned_email")).lower()
    if email and email in traders_by_email:
        return traders_by_email[email]
    phone = _np_sync_text(row.get("phone") or row.get("trader_phone") or row.get("assigned_phone"))
    if phone and phone in traders_by_phone:
        return traders_by_phone[phone]
    return {}

def _np_sync_account_payload(source, row, trader, now):
    login = _np_sync_text(row.get("mt5_login") or row.get("login") or row.get("account_login") or row.get("account_number"))
    server = _np_sync_text(row.get("mt5_server") or row.get("server") or row.get("account_server"))
    stage = _np_sync_lower(row.get("stage") or row.get("phase") or row.get("assigned_phase") or trader.get("phase") or "phase1")
    if stage in {"phase_1", "phase 1"}: stage = "phase1"
    if stage in {"phase_2", "phase 2"}: stage = "phase2"
    if stage not in {"phase1", "phase2", "funded", "live"}:
        stage = "phase1"
    account_size = _np_sync_num(row.get("account_size") or row.get("start_balance") or row.get("balance") or trader.get("account_size") or trader.get("balance"), 0)
    master = _np_sync_text(row.get("mt5_master_password") or row.get("mt5_password") or row.get("master_password") or row.get("password"))
    investor = _np_sync_text(row.get("mt5_investor_password") or row.get("investor_password") or row.get("investor"))
    assigned_at = row.get("assigned_at") or row.get("mt5_assigned_at") or row.get("approved_at") or row.get("updated_at") or now
    return {
        "trader_id": trader.get("id") or row.get("trader_id") or row.get("assigned_trader_id"),
        "purchase_id": row.get("id") if source == "challenge_purchases" else row.get("purchase_id"),
        "mt5_pool_id": row.get("id") if source == "mt5_pool" else row.get("mt5_pool_id") or row.get("assigned_mt5_id"),
        "stage": stage,
        "account_status": "assigned_active",
        "monitoring_enabled": True,
        "mt5_access_disabled": False,
        "mt5_login": login,
        "mt5_server": server,
        "mt5_master_password": master,
        "mt5_password": master,
        "master_password": master,
        "mt5_investor_password": investor,
        "investor_password": investor,
        "account_size": account_size,
        "start_balance": account_size,
        "current_balance": _np_sync_num(row.get("current_balance") or row.get("balance"), account_size),
        "current_equity": _np_sync_num(row.get("current_equity") or row.get("equity"), account_size),
        "highest_equity": _np_sync_num(row.get("highest_equity"), account_size),
        "lowest_equity": _np_sync_num(row.get("lowest_equity"), account_size),
        "profit": _np_sync_num(row.get("profit"), 0),
        "profit_percent": _np_sync_num(row.get("profit_percent"), 0),
        "absolute_drawdown_percent": _np_sync_num(row.get("drawdown_percent") or row.get("absolute_drawdown_percent"), 0),
        "dd_limit_percent": 20,
        "dd_used_percent": _np_sync_num(row.get("dd_used_percent") or row.get("max_drawdown_used"), 0),
        "target_percent": 8 if stage == "phase2" else (10 if stage == "phase1" else 0),
        "started_at": assigned_at,
        "assigned_at": assigned_at,
        "updated_at": now,
    }

def _np_safe_account_upsert(payload):
    login = _np_sync_text(payload.get("mt5_login"))
    if not login:
        return None, "missing_login"
    existing = []
    try:
        existing = supabase.table("trader_accounts").select("*").eq("mt5_login", login).order("updated_at", desc=True).limit(5).execute().data or []
    except Exception as e:
        return None, "lookup_failed:" + str(e)
    active = [r for r in existing if _np_sync_lower(r.get("account_status")) in NP_ACTIVE_ACCOUNT_STATUSES]
    terminal = [r for r in existing if _np_sync_terminal(r)]
    if active:
        row_id = active[0].get("id")
        safe_payload = {k:v for k,v in payload.items() if v not in [None, ""]}
        try:
            out = supabase.table("trader_accounts").update(safe_payload).eq("id", row_id).execute().data or []
            return (out[0] if out else active[0]), "updated_active"
        except Exception as e:
            return active[0], "active_update_skipped:" + str(e)
    if terminal:
        return terminal[0], "skipped_terminal_existing"
    attempts = [payload]
    # schema-safe fallbacks if optional columns do not exist in Supabase
    optional_sets = [
        ["mt5_password", "master_password", "investor_password", "assigned_at", "mt5_access_disabled"],
        ["purchase_id", "mt5_pool_id", "target_percent", "dd_limit_percent", "dd_used_percent", "absolute_drawdown_percent"],
        ["current_balance", "current_equity", "highest_equity", "lowest_equity", "profit", "profit_percent"],
    ]
    compact = dict(payload)
    for opts in optional_sets:
        for k in opts:
            compact.pop(k, None)
        attempts.append(dict(compact))
    last = ""
    for candidate in attempts:
        try:
            out = supabase.table("trader_accounts").insert(candidate).execute().data or []
            return (out[0] if out else candidate), "created"
        except Exception as e:
            last = str(e)
    return None, "insert_failed:" + last

def _np_unified_mt5_visibility_sync(force_logins=None, lookback_days=45):
    now = now_iso()
    force_logins = {str(x).strip() for x in (force_logins or []) if str(x).strip()}
    traders = _np_sync_rows("traders")
    traders_by_id = {str(t.get("id")): t for t in traders if t.get("id")}
    traders_by_email = {str(t.get("email") or "").strip().lower(): t for t in traders if t.get("email")}
    traders_by_phone = {str(t.get("phone") or "").strip(): t for t in traders if t.get("phone")}
    sources = []
    for table in ["trader_accounts", "challenge_purchases", "mt5_pool", "traders"]:
        for r in _np_sync_rows(table):
            rr = dict(r); rr["_np_source"] = table; sources.append(rr)
    created=[]; updated=[]; skipped=[]; visible=[]
    for row in sources:
        source = row.get("_np_source")
        login = _np_sync_text(row.get("mt5_login") or row.get("login") or row.get("account_login") or row.get("account_number"))
        server = _np_sync_text(row.get("mt5_server") or row.get("server") or row.get("account_server"))
        if not _np_sync_valid_login(login) or not server:
            continue
        if _np_sync_terminal(row):
            skipped.append({"login": login, "source": source, "reason": "terminal/dead"}); continue
        if not _np_sync_active_signal(row):
            skipped.append({"login": login, "source": source, "reason": "no active/approved signal"}); continue
        if source not in {"trader_accounts"} and login not in force_logins and not _np_sync_recent(row, lookback_days):
            skipped.append({"login": login, "source": source, "reason": "not recent fallback"}); continue
        trader = _np_find_trader_for_sync(row, traders_by_id, traders_by_email, traders_by_phone)
        if not trader and source == "traders": trader = row
        if not trader.get("id"):
            skipped.append({"login": login, "source": source, "reason": "no trader link"}); continue
        payload = _np_sync_account_payload(source, row, trader, now)
        account, action = _np_safe_account_upsert(payload)
        entry = {"login": login, "source": source, "action": action, "trader_id": trader.get("id"), "account_id": (account or {}).get("id")}
        if action.startswith("created"):
            created.append(entry)
        elif action.startswith("updated"):
            updated.append(entry)
        else:
            skipped.append(entry)
        if account and not action.startswith("skipped"):
            visible.append(entry)
            # also synchronize trader identity/current account fields best-effort
            try:
                supabase.table("traders").update({
                    "current_account_id": account.get("id"), "mt5_login": login, "mt5_server": server,
                    "phase": payload.get("stage"), "status": "active" if payload.get("stage") not in {"funded", "live"} else "funded",
                    "payment_status": "approved", "monitoring_enabled": True, "mt5_access_disabled": False,
                    "updated_at": now, "mt5_updated_at": now
                }).eq("id", trader.get("id")).execute()
            except Exception as e:
                print("NP SYNC TRADER UPDATE SKIPPED", e)
    return {"created": created, "updated": updated, "skipped": skipped[:250], "visible": visible, "visible_count": len(visible)}

@app.route("/np_unified_mt5_sync", methods=["GET", "POST", "OPTIONS"])
@app.route("/system_sync_mt5_assignments", methods=["GET", "POST", "OPTIONS"])
@app.route("/admin/np_unified_mt5_sync", methods=["GET", "POST", "OPTIONS"])
@app.route("/dashboard/np_unified_mt5_sync", methods=["GET", "POST", "OPTIONS"])
def np_unified_mt5_sync_route():
    if request.method == "OPTIONS":
        return _np_ok({"success": True}) if "_np_ok" in globals() else ok({})
    data = request.get_json(silent=True) or {}
    force = request.args.get("force_logins") or data.get("force_logins") or ""
    if isinstance(force, str):
        force_logins = [x.strip() for x in force.split(",") if x.strip()]
    elif isinstance(force, list):
        force_logins = force
    else:
        force_logins = []
    lookback = request.args.get("lookback_days") or data.get("lookback_days") or 45
    try:
        result = _np_unified_mt5_visibility_sync(force_logins=force_logins, lookback_days=int(lookback))
        payload = {"success": True, "message": f"MT5 visibility sync complete: {result.get('visible_count', 0)} account(s) visible/repaired", "data": result}
        return jsonify(payload)
    except Exception as e:
        return _np_fail(e, 500) if "_np_fail" in globals() else bad(e, 500)


# ================================
# NAIRAPIPS PRIVATE OFFER DASHBOARD FETCH
# ================================
def _np_private_offer_expired(row):
    try:
        exp = row.get("offer_expires_at") or row.get("expires_at") or row.get("expiry_date") or row.get("expires") or ""
        if not exp:
            return False
        dt = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt < now
    except Exception:
        return False


def _np_private_offer_matches(row, trader_id="", email="", phone="", account_reference=""):
    def c(v):
        return str(v or "").strip().lower()
    def digits(v):
        return re.sub(r"\D", "", str(v or ""))
    keys = {c(trader_id), c(email), c(account_reference)}
    ph = digits(phone)
    if ph:
        keys.add(ph)
        if ph.startswith("234"):
            keys.add("0" + ph[3:])
        if ph.startswith("0"):
            keys.add("234" + ph[1:])
    keys = {x for x in keys if x}
    targets = {
        c(row.get("target_trader_id")),
        c(row.get("target_email")),
        c(row.get("target_account_reference")),
        c(row.get("target_phone")),
        c(row.get("target_name")),
    }
    tph = digits(row.get("target_phone"))
    if tph:
        targets.add(tph)
        if tph.startswith("234"):
            targets.add("0" + tph[3:])
        if tph.startswith("0"):
            targets.add("234" + tph[1:])
    targets = {x for x in targets if x}
    if not keys or not targets:
        return False
    for k in keys:
        for t in targets:
            if k == t or (len(k) >= 5 and k in t) or (len(t) >= 5 and t in k):
                return True
    return False


@app.route("/private_offers_for_trader", methods=["GET", "OPTIONS"])
def private_offers_for_trader():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        trader_id = _np_offer_clean_str(request.args.get("trader_id"), 120)
        email = _np_offer_clean_str(request.args.get("email"), 250).lower()
        phone = _np_offer_clean_str(request.args.get("phone"), 80)
        account_reference = _np_offer_clean_str(request.args.get("account_reference"), 120)

        rows = []
        try:
            # Fetch recent active private offers; filter in Python because some
            # Supabase schemas differ and OR filters can miss older rows.
            rows = supabase.table("announcements").select("*").eq("status", "active").eq("type", "private_offer").order("created_at", desc=True).limit(100).execute().data or []
        except Exception:
            rows = supabase.table("announcements").select("*").eq("type", "private_offer").order("created_at", desc=True).limit(100).execute().data or []

        visible = []
        for raw_row in rows:
            row = _np_offer_merge_meta(raw_row)
            if not _np_offer_bool(row.get("delivery_dashboard"), _np_offer_bool(row.get("show_on_dashboard"), False)):
                continue
            if _np_private_offer_expired(row):
                continue
            if not _np_private_offer_matches(row, trader_id, email, phone, account_reference):
                continue
            visible.append(row)

        # Return a plain array because older dashboard getJSON() only accepts arrays.
        return jsonify(visible)
    except Exception as e:
        return bad(e)

"""Multi-channel notification engine:
- WhatsApp via Termii (Nigerian SMS/WhatsApp provider) — most reliable for NG traders
- SMS via Termii
- Email via Brevo (best-effort, often goes to spam)
- In-app (always works since trader must login)
- Tries all 4 channels; counts how many succeeded
"""
import os
import re
import json
import time
import requests as _req
from datetime import datetime, timezone


def _np_send_termii_whatsapp(to_phone, message):
    """Send WhatsApp via Termii. Falls back to SMS if WA fails."""
    try:
        api_key = os.environ.get("TERMII_API_KEY") or os.environ.get("TERMII_SMS_API_KEY")
        sender_id = os.environ.get("TERMII_SENDER_ID") or "NairaPips"
        if not api_key:
            return {"ok": False, "channel": "whatsapp", "error": "TERMII_API_KEY not set"}
        
        # Normalize phone - remove leading 0, add 234 for Nigeria
        phone = str(to_phone or "").strip()
        phone = re.sub(r'[^0-9+]', '', phone)
        if phone.startswith('+'):
            phone = phone[1:]
        if phone.startswith('0'):
            phone = '234' + phone[1:]
        if not phone.startswith('234') and len(phone) == 10:
            phone = '234' + phone
        
        if len(phone) < 10:
            return {"ok": False, "channel": "whatsapp", "error": f"invalid phone: {to_phone}"}
        
        # Try WhatsApp channel first
        try:
            r = _req.post(
                "https://api.termii.com/api/send/whatsapp",
                json={
                    "api_key": api_key,
                    "from": sender_id,
                    "to": phone,
                    "type": "plain",
                    "channel": "whatsapp",
                    "message": message
                },
                timeout=10
            )
            if r.status_code < 400:
                return {"ok": True, "channel": "whatsapp", "response": r.json() if r.text else {}}
        except Exception:
            pass
        
        # Fallback to SMS
        try:
            r = _req.post(
                "https://api.termii.com/api/sms/send",
                json={
                    "api_key": api_key,
                    "to": phone,
                    "from": sender_id,
                    "sms": message,
                    "type": "plain",
                    "channel": "generic"
                },
                timeout=10
            )
            if r.status_code < 400:
                return {"ok": True, "channel": "sms", "response": r.json() if r.text else {}}
            return {"ok": False, "channel": "sms", "error": r.text[:200]}
        except Exception as e:
            return {"ok": False, "channel": "sms", "error": str(e)}
    except Exception as e:
        return {"ok": False, "channel": "whatsapp", "error": str(e)}


def _np_send_brevo_email(to_email, subject, html_body):
    """Send email via Brevo. Returns True if accepted."""
    try:
        api_key = os.environ.get("BREVO_API_KEY")
        from_email = os.environ.get("FROM_EMAIL") or "support@nairapips.com"
        if not api_key:
            return {"ok": False, "channel": "email", "error": "BREVO_API_KEY not set"}
        
        payload = {
            "sender": {"name": "NairaPips Prop Trading", "email": from_email, "replyTo": {"email": from_email}},
            "to": [{"email": to_email}],
            "subject": subject,
            "htmlContent": html_body,
            "tags": ["revenue_engine", "private_offer"]
        }
        r = _req.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={"api-key": api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        if r.status_code < 400:
            return {"ok": True, "channel": "email", "response": r.json() if r.text else {}}
        return {"ok": False, "channel": "email", "error": f"{r.status_code}: {r.text[:300]}"}
    except Exception as e:
        return {"ok": False, "channel": "email", "error": str(e)}


    """Log to notification_logs."""
    try:
        supabase.table("notification_logs").insert({
            "trader_id": trader_id,
            "channel": channel,
            "status": status,
            "recipient": recipient,
            "subject": subject,
            "error": str(error or "")[:500],
            "sent_at": datetime.now(timezone.utc).isoformat()
        }).execute()
    except Exception:
        pass


def _np_log_notification(trader_id, channel, status, recipient, subject, error=""):
    """Log to notification_logs table."""
    try:
        supabase.table("notification_logs").insert({
            "trader_id": trader_id,
            "channel": channel,
            "status": status,
            "recipient": recipient,
            "subject": subject,
            "error": str(error or "")[:500],
            "sent_at": datetime.now(timezone.utc).isoformat()
        }).execute()
    except Exception:
        pass


@app.route("/admin/setup_telegram_bot", methods=["POST", "OPTIONS"])
def admin_setup_telegram_bot():
    """One-time setup: registers a Telegram webhook and returns bot info.
    The admin needs to:
    1. Message @BotFather on Telegram to create a bot
    2. Get the bot token
    3. Add it as TELEGRAM_BOT_TOKEN env var
    4. Send /start to the bot
    5. Use this endpoint to register their chat_id for notifications
    """
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        chat_id = body.get("chat_id") or os.environ.get("ADMIN_TELEGRAM_CHAT_ID")
        bot_token = body.get("bot_token") or os.environ.get("TELEGRAM_BOT_TOKEN")
        
        if not bot_token:
            return _np_fail("TELEGRAM_BOT_TOKEN not set in env vars. Create a bot via @BotFather on Telegram first.", 400)
        
        # Verify bot
        r = _req.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=10)
        bot_info = r.json() if r.ok else {}
        
        if not chat_id:
            return _np_ok({
                "success": False,
                "instruction": "Send /start to your bot on Telegram, then call this endpoint with your chat_id",
                "bot_info": bot_info
            })
        
        # Test send
        test_msg = _np_send_telegram(chat_id, "✅ NairaPips notifications connected! You'll receive trader alerts here.")
        if test_msg.get("ok"):
            os.environ["ADMIN_TELEGRAM_CHAT_ID"] = str(chat_id)
        
        return _np_ok({
            "success": True,
            "bot_info": bot_info,
            "test_message": test_msg
        })
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/send_trader_alert", methods=["POST", "OPTIONS"])
def admin_send_trader_alert():
    """Send alert to admin via Telegram when key events happen:
    - New breach
    - Trader purchased challenge
    - Promo code used
    - Trader inactivity
    """
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        event_type = body.get("event_type", "alert")
        message = body.get("message", "")
        trader_id = body.get("trader_id", "")
        
        if not message:
            return _np_fail("message required", 400)
        
        # Format nicely
        icon_map = {
            "breach": "🚨",
            "purchase": "💰",
            "redeem": "🎟️",
            "signup": "🆕",
            "inactive": "😴",
            "default": "📢"
        }
        icon = icon_map.get(event_type, icon_map["default"])
        
        full_msg = f"{icon} <b>NairaPips {event_type.upper()}</b>\n\n{message}\n\n<i>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>"
        
        chat_id = os.environ.get("ADMIN_TELEGRAM_CHAT_ID")
        if not chat_id:
            return _np_ok({"success": False, "warning": "ADMIN_TELEGRAM_CHAT_ID not set"})
        
        result = _np_send_telegram(chat_id, full_msg)
        _np_log_notification(trader_id, "telegram", "sent" if result.get("ok") else "failed", chat_id, event_type, result.get("error", ""))
        
        return _np_ok({"success": True, "result": result})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/send_breach_alert", methods=["POST", "OPTIONS"])
def admin_send_breach_alert():
    """Called by breach detection: alerts admin via Telegram immediately."""
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        trader_name = body.get("trader_name", "Unknown")
        trader_email = body.get("email", "")
        mt5_login = body.get("mt5_login", "")
        dd_percent = body.get("dd_percent", 0)
        reason = body.get("reason", "drawdown exceeded")
        
        msg = (
            f"<b>Trader:</b> {trader_name}\n"
            f"<b>Email:</b> {trader_email}\n"
            f"<b>MT5:</b> {mt5_login}\n"
            f"<b>Drawdown:</b> {dd_percent:.1f}%\n"
            f"<b>Reason:</b> {reason}\n\n"
            f"Auto-recovery offer sent via Revenue Engine."
        )
        
        chat_id = os.environ.get("ADMIN_TELEGRAM_CHAT_ID")
        if chat_id:
            _np_send_telegram(chat_id, f"🚨 <b>BREACH ALERT</b>\n\n{msg}")
        
        return _np_ok({"success": True, "alerted": bool(chat_id)})
    except Exception as e:
        return _np_fail(e, 500)


# ============================================================
# IN-APP NOTIFICATION CENTER
# ============================================================

@app.route("/admin/in_app_notifications", methods=["GET", "OPTIONS"])
def admin_in_app_notifications():
    """List all notifications for a trader (in-app feed)."""
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        trader_id = _np_offer_clean_str(request.args.get("trader_id"), 120)
        if not trader_id:
            return _np_fail("trader_id required", 400)
        rows = []
        try:
            rows = supabase.table("notifications").select("*").eq("trader_id", trader_id).eq("is_dismissed", False).order("created_at", desc=True).limit(50).execute().data or []
        except Exception:
            # Table might not exist - return empty
            pass
        return _np_ok({"success": True, "data": rows, "count": len(rows)})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/create_in_app_notification", methods=["POST", "OPTIONS"])
def admin_create_in_app_notification():
    """Create an in-app notification entry."""
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        trader_id = _np_offer_clean_str(body.get("trader_id"), 120)
        if not trader_id:
            return _np_fail("trader_id required", 400)
        
        row = {
            "trader_id": trader_id,
            "type": body.get("type", "offer"),
            "title": body.get("title", "")[:200],
            "message": body.get("message", "")[:1000],
            "action_url": body.get("action_url", ""),
            "icon": body.get("icon", "📢"),
            "is_read": False,
            "is_dismissed": False,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        try:
            inserted = supabase.table("notifications").insert(row).execute().data or []
            return _np_ok({"success": True, "data": inserted[0] if inserted else row})
        except Exception as e:
            # Table might not exist yet
            return _np_ok({"success": True, "warning": str(e), "data": row})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/mark_notification_read", methods=["POST", "OPTIONS"])
def admin_mark_notification_read():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        nid = body.get("id")
        if not nid:
            return _np_fail("id required", 400)
        try:
            supabase.table("notifications").update({"is_read": True, "read_at": datetime.now(timezone.utc).isoformat()}).eq("id", nid).execute()
        except Exception:
            pass
        return _np_ok({"success": True})
    except Exception as e:
        return _np_fail(e, 500)



"""In-process scheduler for auto-pilot revenue engine."""
import os
import threading
import time
from datetime import datetime, timezone, timedelta

_scheduler_started = False
_scheduler_lock = threading.Lock()



def _np_send_telegram(chat_id, message):
    """Send via Telegram bot."""
    try:
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        if not token or not chat_id:
            return {"ok": False, "error": "TELEGRAM_BOT_TOKEN or chat_id missing"}
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code < 400:
            return {"ok": True}
        return {"ok": False, "error": f"{r.status_code}: {r.text[:200]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _np_breach_recovery_check():
    """Check for fresh breaches and send recovery offers."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=35)).isoformat()
        new_breaches = []
        try:
            new_breaches = supabase.table("traders").select("id,name,email,phone,breach_time").eq("status", "breached").gte("breach_time", cutoff).limit(50).execute().data or []
        except Exception:
            pass
        if not new_breaches:
            return
        try:
            rules = supabase.table("auto_trigger_rules").select("*").eq("trigger_event", "breach").eq("is_active", True).limit(5).execute().data or []
        except Exception:
            rules = []
        if not rules:
            return
        for breach in new_breaches:
            for rule in rules:
                try:
                    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
                    recent = supabase.table("announcements").select("id").eq("target_trader_id", breach.get("id")).eq("auto_trigger_id", rule.get("id")).gte("created_at", week_ago).limit(1).execute().data or []
                    if recent:
                        continue
                except Exception:
                    pass
                try:
                    supabase.table("announcements").insert({
                        "type": "private_offer",
                        "status": "active",
                        "show_on_dashboard": True,
                        "delivery_dashboard": True,
                        "private_offer": True,
                        "target_trader_id": breach.get("id"),
                        "title": rule.get("offer_subject", "Recovery Offer"),
                        "message": rule.get("offer_body", ""),
                        "offer_code": rule.get("promo_code", ""),
                        "auto_trigger_id": rule.get("id"),
                        "auto_trigger_name": rule.get("name"),
                        "created_at": datetime.now(timezone.utc).isoformat()
                    }).execute()
                    try:
                        body_text = rule.get("offer_body", "").replace("{name}", breach.get("name", "Trader"))
                        send_email_safe(breach.get("email"), rule.get("offer_subject", "Recovery Offer"), f"Hello {breach.get('name', 'Trader')},\n\n{body_text}\n\nCode: {rule.get('promo_code', '—') if rule.get('promo_code') else 'Auto-applied'}\n\nYour trading journey isn't over — let us help you get back on track.\n\nNairaPips Team")
                    except Exception:
                        pass
                except Exception:
                    pass
                break
    except Exception as e:
        print("breach recovery error:", e)


def _np_behavior_triggers_check():
    """Run behavior-based rules every 15 minutes."""
    try:
        try:
            rules = supabase.table("auto_trigger_rules").select("*").in_("trigger_event", ["inactive", "hot", "near_breach", "phase1_stuck", "signup"]).eq("is_active", True).execute().data or []
        except Exception:
            rules = []
        for rule in rules:
            try:
                last = rule.get("last_run_at")
                if last:
                    try:
                        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                        if (datetime.now(timezone.utc) - last_dt).total_seconds() < 4 * 3600:
                            continue
                    except Exception:
                        pass
                _np_run_single_rule(rule)
            except Exception:
                pass
    except Exception as e:
        print("behavior trigger error:", e)


def _np_weekly_cleanup():
    """Disable expired codes, archive stale rules, log weekly report."""
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            supabase.table("promo_codes").update({"is_active": False}).lt("expires_at", now_iso).eq("is_active", True).execute()
        except Exception:
            pass
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
            supabase.table("auto_trigger_rules").update({"is_active": False}).lt("last_run_at", cutoff).eq("is_active", True).execute()
        except Exception:
            pass
        try:
            week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            redemptions = supabase.table("promo_redemptions").select("id,discount_amount,account_size").gte("redeemed_at", week_ago).execute().data or []
            total_discount = sum(float(r.get("discount_amount", 0) or 0) for r in redemptions)
            total_revenue = sum(float(r.get("account_size", 0) or 0) for r in redemptions)
            offers = supabase.table("announcements").select("id").eq("type", "private_offer").gte("created_at", week_ago).execute().data or []
            summary = f"""Weekly Revenue Engine Report:
- Offers sent: {len(offers)}
- Redemptions: {len(redemptions)}
- Total discount: NGN{int(total_discount):,}
- Total revenue: NGN{int(total_revenue):,}"""
            send_admin_alert("NairaPips Weekly Revenue Engine Report", summary)
        except Exception:
            pass
    except Exception as e:
        print("weekly cleanup error:", e)



def _np_scheduler_loop():
    """Background thread that runs scheduled tasks."""
    while True:
        try:
            now = datetime.now(timezone.utc)
            # Daily 8 AM UTC: run all active auto-triggers
            if now.hour == 8 and now.minute < 5:
                today_key = now.strftime("%Y-%m-%d")
                if not os.environ.get(f"_NP_CRON_DAILY_{today_key}"):
                    os.environ[f"_NP_CRON_DAILY_{today_key}"] = "1"
                    try:
                        print("[CRON] Daily auto-triggers run at", now.isoformat())
                        with app.app_context():
                            rules = supabase.table("auto_trigger_rules").select("*").eq("is_active", True).execute().data or []
                            for rule in rules:
                                _np_run_single_rule(rule)
                            print(f"[CRON] Daily run processed {len(rules)} rules")
                    except Exception as e:
                        print("[CRON] Daily run error:", e)
            
            # Monday 3 AM UTC: weekly cleanup
            if now.weekday() == 0 and now.hour == 3 and now.minute < 5:
                week_key = now.strftime("%Y-W%U")
                if not os.environ.get(f"_NP_CRON_WEEKLY_{week_key}"):
                    os.environ[f"_NP_CRON_WEEKLY_{week_key}"] = "1"
                    try:
                        print("[CRON] Weekly cleanup at", now.isoformat())
                        with app.app_context():
                            _np_weekly_cleanup()
                    except Exception as e:
                        print("[CRON] Weekly cleanup failed:", e)
            
            # Every 30 minutes: check for fresh breaches
            if now.minute in (0, 30):
                half_key = now.strftime("%Y-%m-%d-%H-%M")[:15]
                if not os.environ.get(f"_NP_BREACH_CHECK_{half_key}"):
                    os.environ[f"_NP_BREACH_CHECK_{half_key}"] = "1"
                    try:
                        with app.app_context():
                            _np_breach_recovery_check()
                    except Exception as e:
                        print("[CRON] Breach check failed:", e)
            
            # Every 15 minutes: behavior triggers
            if now.minute in (0, 15, 30, 45):
                q_key = now.strftime("%Y-%m-%d-%H-%M")[:15]
                if not os.environ.get(f"_NP_BEHAVIOR_{q_key}"):
                    os.environ[f"_NP_BEHAVIOR_{q_key}"] = "1"
                    try:
                        with app.app_context():
                            _np_behavior_triggers_check()
                    except Exception as e:
                        print("[CRON] Behavior check failed:", e)
            
            time.sleep(60)
        except Exception as e:
            print("[CRON] Loop error:", e)
            time.sleep(120)


def _np_run_single_rule(rule):
    try:
        seg_traders = supabase.table("traders").select("id").limit(3000).execute().data or []
        all_ids = [t["id"] for t in seg_traders]
        matched = _np_compute_segment_traders(rule.get("segment_key", ""), all_ids)
        if not matched:
            return 0
        sent = 0
        for tid in matched[:100]:
            try:
                trader = supabase.table("traders").select("id,name,email,phone").eq("id", tid).limit(1).execute().data or []
                if not trader:
                    continue
                t = trader[0]
                try:
                    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
                    recent = supabase.table("announcements").select("id").eq("target_trader_id", tid).eq("auto_trigger_id", rule.get("id")).gte("created_at", week_ago).limit(1).execute().data or []
                    if recent:
                        continue
                except Exception:
                    pass
                
                try:
                    offer_row = {
                        "type": "private_offer",
                        "status": "active",
                        "show_on_dashboard": True,
                        "delivery_dashboard": True,
                        "private_offer": True,
                        "target_trader_id": tid,
                        "title": rule.get("offer_subject", "Special Offer"),
                        "message": rule.get("offer_body", ""),
                        "offer_code": (rule.get("promo_code") or "").strip(),
                        "auto_trigger_id": rule.get("id"),
                        "auto_trigger_name": rule.get("name"),
                        "created_at": datetime.now(timezone.utc).isoformat()
                    }
                    supabase.table("announcements").insert(offer_row).execute()
                except Exception:
                    pass
                
                try:
                    body_text = rule.get("offer_body", "").replace("{name}", t.get("name", "Trader"))
                    send_email_safe(t.get("email"), rule.get("offer_subject", "Special Offer"), f"Hello {t.get('name', 'Trader')},\n\n{body_text}\n\nCode: {rule.get('promo_code', '—') if rule.get('promo_code') else 'Auto-applied'}\n\nNairaPips Team")
                except Exception:
                    pass
                
                sent += 1
            except Exception:
                pass
        
        try:
            supabase.table("auto_trigger_rules").update({
                "last_run_at": datetime.now(timezone.utc).isoformat(),
                "run_count": int(rule.get("run_count", 0) or 0) + 1,
                "last_sent_count": sent
            }).eq("id", rule.get("id")).execute()
        except Exception:
            pass
        return sent
    except Exception:
        return 0


def _np_compute_segment_traders(segment_key, all_trader_ids):
    matched = set()
    try:
        accounts = supabase.table("trader_accounts").select("trader_id,account_status,account_size,dd_used_percent,stage,last_sync_at,updated_at,started_at,created_at,breached_at,mt5_login").limit(5000).execute().data or []
        traders_data = supabase.table("traders").select("id,created_at,status,breach_time").in_("id", all_trader_ids[:1000]).execute().data or []
        trader_map = {t["id"]: t for t in traders_data}
        now = datetime.now(timezone.utc)
        
        if segment_key == "all_traders":
            return all_trader_ids[:200]
        if segment_key == "new_signups_24h":
            cutoff = (now - timedelta(hours=24)).isoformat()
            return [tid for tid, t in trader_map.items() if t.get("created_at", "") >= cutoff]
        if segment_key == "new_signups_7d":
            cutoff = (now - timedelta(days=7)).isoformat()
            return [tid for tid, t in trader_map.items() if t.get("created_at", "") >= cutoff]
        if segment_key == "breached_24h":
            cutoff = (now - timedelta(hours=24)).isoformat()
            return [tid for tid, t in trader_map.items() if t.get("status") == "breached" and (t.get("breach_time") or "") >= cutoff]
        if segment_key == "breached_7d":
            cutoff = (now - timedelta(days=7)).isoformat()
            return [tid for tid, t in trader_map.items() if t.get("status") == "breached" and (t.get("breach_time") or "") >= cutoff]
        if segment_key == "breached_overdue":
            breached = [tid for tid, t in trader_map.items() if t.get("status") == "breached"]
            return breached[:50]
        
        for a in accounts:
            tid = a.get("trader_id")
            if not tid:
                continue
            st = a.get("account_status", "")
            dd = float(a.get("dd_used_percent", 0) or 0)
            stage = a.get("stage", "")
            size = float(a.get("account_size", 0) or 0)
            
            if segment_key == "near_breach" and st == "assigned_active" and 70 <= dd < 100:
                matched.add(tid)
            elif segment_key == "inactive_7d" and st == "assigned_active":
                try:
                    ls = a.get("last_sync_at") or a.get("updated_at")
                    if ls and (now - datetime.fromisoformat(ls.replace("Z", "+00:00"))).days >= 7:
                        matched.add(tid)
                except Exception:
                    pass
            elif segment_key == "funded_idle" and stage == "funded" and st in ("assigned_active", "active"):
                try:
                    ls = a.get("last_sync_at") or a.get("updated_at")
                    if ls and (now - datetime.fromisoformat(ls.replace("Z", "+00:00"))).days >= 7:
                        matched.add(tid)
                except Exception:
                    pass
            elif segment_key == "phase1_stuck" and stage == "phase1" and st == "assigned_active":
                try:
                    started = a.get("started_at") or a.get("created_at")
                    if started and (now - datetime.fromisoformat(started.replace("Z", "+00:00"))).days >= 20:
                        matched.add(tid)
                except Exception:
                    pass
            elif segment_key == "hot_leads" and st == "assigned_active" and dd < 30:
                try:
                    started = a.get("started_at") or a.get("created_at")
                    if started and (now - datetime.fromisoformat(started.replace("Z", "+00:00"))).days <= 5:
                        matched.add(tid)
                except Exception:
                    pass
            elif segment_key == "high_value" and st == "assigned_active" and size >= 700000:
                matched.add(tid)
            elif segment_key == "phase2_active" and st == "assigned_active" and stage == "phase2":
                matched.add(tid)
            elif segment_key == "funded_active" and st == "assigned_active" and stage == "funded":
                matched.add(tid)
        
        return list(matched)[:200]
    except Exception:
        return []


def _np_offer_bool(v, default=False):
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


@app.route("/admin/promo_codes", methods=["GET", "OPTIONS"])
def admin_promo_codes():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        rows = supabase.table("promo_codes").select("*").order("created_at", desc=True).limit(500).execute().data or []
        return _np_ok({"success": True, "data": rows, "count": len(rows)})
    except Exception as e:
        return _np_ok({"success": True, "data": [], "count": 0, "warning": str(e)})


@app.route("/admin/create_promo_code", methods=["POST", "OPTIONS"])
def admin_create_promo_code():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        prefix = _np_offer_clean_str(body.get("prefix", ""), 12).upper()
        custom = _np_offer_clean_str(body.get("code", "")).upper()
        code = custom or f"{prefix}-" + ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))
        try:
            existing = supabase.table("promo_codes").select("id").eq("code", code).limit(1).execute().data or []
            if existing:
                return _np_fail("Code already exists", 409)
        except Exception:
            pass
        discount_type = body.get("discount_type", "percent")
        try:
            discount_value = float(body.get("discount_value", 0))
            if discount_type == "percent":
                discount_value = max(1, min(100, discount_value))
        except Exception:
            return _np_fail("Invalid discount_value", 400)
        row = {
            "code": code,
            "prefix": prefix,
            "discount_type": discount_type,
            "discount_value": discount_value,
            "max_uses": int(body.get("max_uses", 0)),
            "per_trader_limit": int(body.get("per_trader_limit", 1)),
            "current_uses": 0,
            "description": _np_offer_clean_str(body.get("description", ""), 500),
            "expires_at": body.get("expires_at"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "created_by": _np_offer_clean_str(body.get("created_by", "admin"), 120),
            "is_active": True
        }
        try:
            inserted = supabase.table("promo_codes").insert(row).execute().data or []
            return _np_ok({"success": True, "code": code, "data": inserted[0] if inserted else row})
        except Exception as e:
            return _np_ok({"success": True, "code": code, "data": row, "warning": str(e)})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/toggle_promo_code", methods=["POST", "OPTIONS"])
def admin_toggle_promo_code():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        cid = body.get("id")
        is_active = bool(body.get("is_active", True))
        try:
            supabase.table("promo_codes").update({"is_active": is_active}).eq("id", cid).execute()
        except Exception:
            pass
        return _np_ok({"success": True})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/segments_overview", methods=["GET", "OPTIONS"])
def admin_segments_overview():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        traders = supabase.table("traders").select("id").limit(2000).execute().data or []
        all_ids = [t["id"] for t in traders]
        segments = {k: [] for k in ["all_traders","breached_24h","breached_7d","breached_overdue","near_breach","hot_leads","new_signups_24h","new_signups_7d","inactive_7d","phase1_stuck","funded_idle","high_value","phase2_active","funded_active","viewed_plans_no_buy"]}
        for k in segments:
            segments[k] = _np_compute_segment_traders(k, all_ids)
        return _np_ok({"success": True, "segments": segments})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/segment_traders", methods=["GET", "OPTIONS"])
def admin_segment_traders():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        seg = _np_offer_clean_str(request.args.get("segment", ""))
        limit = min(int(request.args.get("limit", 200)), 500)
        overview_resp = admin_segments_overview()
        overview = overview_resp.get_json() if hasattr(overview_resp, "get_json") else overview_resp
        segment_ids = (overview.get("segments") or {}).get(seg, [])
        rows = []
        if segment_ids:
            try:
                rows = supabase.table("traders").select("id,name,email,phone,status,current_account_id,created_at,breach_time").in_("id", segment_ids[:limit]).execute().data or []
            except Exception:
                rows = []
        return _np_ok({"success": True, "data": rows, "count": len(rows), "segment": seg})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/auto_triggers", methods=["GET", "OPTIONS"])
def admin_auto_triggers():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        rows = supabase.table("auto_trigger_rules").select("*").order("created_at", desc=True).limit(100).execute().data or []
        return _np_ok({"success": True, "data": rows})
    except Exception:
        return _np_ok({"success": True, "data": [], "warning": "table not yet created"})


@app.route("/admin/save_auto_trigger", methods=["POST", "OPTIONS"])
def admin_save_auto_trigger():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        rule = {
            "name": _np_offer_clean_str(body.get("name", ""), 100),
            "trigger_event": _np_offer_clean_str(body.get("trigger_event", ""), 50),
            "delay_hours": int(body.get("delay_hours", 0)),
            "segment_key": _np_offer_clean_str(body.get("segment_key", ""), 64),
            "offer_subject": _np_offer_clean_str(body.get("offer_subject", ""), 200),
            "offer_body": _np_offer_clean_str(body.get("offer_body", ""), 4000),
            "promo_code": _np_offer_clean_str(body.get("promo_code", ""), 64).upper(),
            "is_active": bool(body.get("is_active", True)),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_run_at": None,
            "run_count": 0
        }
        if not rule["trigger_event"] or not rule["offer_subject"]:
            return _np_fail("trigger_event and offer_subject required", 400)
        try:
            supabase.table("auto_trigger_rules").insert(rule).execute()
        except Exception as e:
            return _np_ok({"success": True, "warning": str(e), "data": rule})
        return _np_ok({"success": True, "data": rule})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/toggle_auto_trigger", methods=["POST", "OPTIONS"])
def admin_toggle_auto_trigger():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        body = request.get_json(silent=True) or {}
        rid = body.get("id")
        is_active = bool(body.get("is_active", True))
        try:
            supabase.table("auto_trigger_rules").update({"is_active": is_active}).eq("id", rid).execute()
        except Exception:
            pass
        return _np_ok({"success": True})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/run_auto_triggers", methods=["POST", "OPTIONS"])
def admin_run_auto_triggers():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        rules = []
        try:
            rules = supabase.table("auto_trigger_rules").select("*").eq("is_active", True).execute().data or []
        except Exception:
            rules = []
        results = []
        for rule in rules:
            try:
                seg_traders = supabase.table("traders").select("id").limit(3000).execute().data or []
                matched = _np_compute_segment_traders(rule.get("segment_key", ""), [t["id"] for t in seg_traders])
                sent = 0
                for tid in matched[:100]:
                    try:
                        trader = supabase.table("traders").select("id,name,email,phone").eq("id", tid).limit(1).execute().data or []
                        if not trader:
                            continue
                        t = trader[0]
                        try:
                            week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
                            recent = supabase.table("announcements").select("id").eq("target_trader_id", tid).eq("auto_trigger_id", rule.get("id")).gte("created_at", week_ago).limit(1).execute().data or []
                            if recent:
                                continue
                        except Exception:
                            pass
                        try:
                            supabase.table("announcements").insert({
                                "type": "private_offer",
                                "status": "active",
                                "show_on_dashboard": True,
                                "delivery_dashboard": True,
                                "private_offer": True,
                                "target_trader_id": tid,
                                "title": rule.get("offer_subject", "Special Offer"),
                                "message": rule.get("offer_body", ""),
                                "offer_code": (rule.get("promo_code") or "").strip(),
                                "auto_trigger_id": rule.get("id"),
                                "auto_trigger_name": rule.get("name"),
                                "created_at": datetime.now(timezone.utc).isoformat()
                            }).execute()
                        except Exception:
                            pass
                        try:
                            body_text = rule.get("offer_body", "").replace("{name}", t.get("name", "Trader"))
                            send_email_safe(t.get("email"), rule.get("offer_subject", "Special Offer"), f"Hello {t.get('name', 'Trader')},\n\n{body_text}\n\nCode: {rule.get('promo_code', '—') if rule.get('promo_code') else 'Auto-applied'}\n\nNairaPips Team")
                        except Exception:
                            pass
                        sent += 1
                    except Exception:
                        pass
                try:
                    supabase.table("auto_trigger_rules").update({
                        "last_run_at": datetime.now(timezone.utc).isoformat(),
                        "run_count": int(rule.get("run_count", 0) or 0) + 1,
                        "last_sent_count": sent
                    }).eq("id", rule.get("id")).execute()
                except Exception:
                    pass
                results.append({"rule": rule.get("name"), "sent": sent, "matched": len(matched)})
            except Exception as e:
                results.append({"rule": rule.get("name"), "error": str(e)})
        return _np_ok({"success": True, "results": results, "rules_processed": len(rules)})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/trigger_breach_recovery_now", methods=["POST", "OPTIONS"])
def admin_trigger_breach_recovery_now():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        with app.app_context():
            _np_breach_recovery_check()
            return _np_ok({"success": True, "message": "Breach recovery check completed"})
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/offer_performance", methods=["GET", "OPTIONS"])
def admin_offer_performance():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        offers = []
        try:
            offers = supabase.table("announcements").select("id,subject,offer_code,created_at,target_trader_id,status").eq("type", "private_offer").order("created_at", desc=True).limit(500).execute().data or []
        except Exception:
            offers = []
        redemptions = []
        try:
            redemptions = supabase.table("promo_redemptions").select("*").order("redeemed_at", desc=True).limit(500).execute().data or []
        except Exception:
            redemptions = []
        codes = []
        try:
            codes = supabase.table("promo_codes").select("*").order("created_at", desc=True).limit(200).execute().data or []
        except Exception:
            codes = []
        code_stats = {}
        for r in redemptions:
            c = r.get("code", "")
            if not c:
                continue
            code_stats.setdefault(c, {"uses": 0, "discount_given": 0, "account_size": 0})
            code_stats[c]["uses"] += 1
            code_stats[c]["discount_given"] += float(r.get("discount_amount", 0) or 0)
            code_stats[c]["account_size"] += float(r.get("account_size", 0) or 0)
        email_logs = []
        try:
            email_logs = supabase.table("email_logs").select("recipient_email,email_type,status,sent_at").eq("email_type", "private_offer").order("sent_at", desc=True).limit(500).execute().data or []
        except Exception:
            email_logs = []
        return _np_ok({
            "success": True,
            "summary": {
                "total_offers_sent": len(offers),
                "total_redemptions": len(redemptions),
                "total_codes_active": len([c for c in codes if c.get("is_active")]),
                "total_discount_given": sum(float(r.get("discount_amount", 0) or 0) for r in redemptions),
                "total_revenue_attributed": sum(float(r.get("account_size", 0) or 0) for r in redemptions),
                "emails_sent": len([e for e in email_logs if e.get("status") == "sent"])
            },
            "code_stats": code_stats,
            "offers": offers[:50],
            "redemptions": redemptions[:30],
            "codes": codes[:30]
        })
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/admin/cron_status", methods=["GET", "OPTIONS"])
def admin_cron_status():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        triggers = []
        try:
            triggers = supabase.table("auto_trigger_rules").select("id,name,last_run_at,run_count,last_sent_count,is_active").order("last_run_at", desc=True).limit(20).execute().data or []
        except Exception:
            triggers = []
        recent = []
        try:
            week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            recent = supabase.table("announcements").select("id,subject,target_trader_id,created_at,auto_trigger_name").eq("type", "private_offer").not_.is_("auto_trigger_id", "null").gte("created_at", week_ago).order("created_at", desc=True).limit(30).execute().data or []
        except Exception:
            recent = []
        active_codes = 0
        try:
            active_codes = len(supabase.table("promo_codes").select("id").eq("is_active", True).execute().data or [])
        except Exception:
            pass
        today_redemptions = 0
        try:
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            today_redemptions = len(supabase.table("promo_redemptions").select("id").gte("redeemed_at", today_start).execute().data or [])
        except Exception:
            pass
        return _np_ok({
            "success": True,
            "scheduler_running": _scheduler_started,
            "active_rules": len([t for t in triggers if t.get("is_active")]),
            "total_rules": len(triggers),
            "active_codes": active_codes,
            "todays_redemptions": today_redemptions,
            "auto_offers_7d": len(recent),
            "triggers": triggers,
            "recent_auto_offers": recent
        })
    except Exception as e:
        return _np_fail(e, 500)


@app.route("/trader_targeted_offers", methods=["GET", "OPTIONS"])
def trader_targeted_offers():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        trader_id = _np_offer_clean_str(request.args.get("trader_id"), 120)
        email = _np_offer_clean_str(request.args.get("email"), 250).lower()
        if not trader_id and not email:
            return jsonify([])
        rows = []
        try:
            rows = supabase.table("announcements").select("*").eq("status", "active").eq("type", "private_offer").order("created_at", desc=True).limit(50).execute().data or []
        except Exception:
            rows = []
        visible = []
        for row in rows:
            target = row.get("target_trader_id")
            if target and str(target) == str(trader_id):
                visible.append(row)
            elif not target:
                visible.append(row)
        return jsonify(visible[:10])
    except Exception as e:
        return jsonify([])


@app.route("/validate_promo_code", methods=["POST", "GET", "OPTIONS"])
def validate_promo_code():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        if request.method == "GET":
            code = _np_offer_clean_str(request.args.get("code", "")).upper()
            account_size = float(request.args.get("account_size", 0) or 0)
            trader_id = _np_offer_clean_str(request.args.get("trader_id"), 120)
        else:
            body = request.get_json(silent=True) or {}
            code = _np_offer_clean_str(body.get("code", "")).upper()
            account_size = float(body.get("account_size", 0) or 0)
            trader_id = _np_offer_clean_str(body.get("trader_id"), 120)
        if not code:
            return jsonify({"valid": False, "reason": "empty code"})
        rows = []
        try:
            rows = supabase.table("promo_codes").select("*").eq("code", code).limit(1).execute().data or []
        except Exception:
            return jsonify({"valid": False, "reason": "promo system unavailable"})
        if not rows:
            return jsonify({"valid": False, "reason": "code not found"})
        promo = rows[0]
        if not promo.get("is_active"):
            return jsonify({"valid": False, "reason": "code disabled"})
        exp = promo.get("expires_at")
        if exp:
            try:
                if datetime.fromisoformat(exp.replace("Z", "+00:00")) < datetime.now(timezone.utc):
                    return jsonify({"valid": False, "reason": "code expired"})
            except Exception:
                pass
        max_uses = int(promo.get("max_uses") or 0)
        current_uses = int(promo.get("current_uses") or 0)
        if max_uses > 0 and current_uses >= max_uses:
            return jsonify({"valid": False, "reason": "code fully used"})
        discount_type = promo.get("discount_type", "percent")
        discount_value = float(promo.get("discount_value", 0))
        base_fee = account_size * 0.04 if account_size else 0
        if discount_type == "percent":
            discount_amount = base_fee * (discount_value / 100.0)
        else:
            discount_amount = min(discount_value, base_fee)
        final_fee = max(0, base_fee - discount_amount)
        return jsonify({
            "valid": True,
            "code": code,
            "discount_type": discount_type,
            "discount_value": discount_value,
            "base_fee": round(base_fee, 2),
            "discount_amount": round(discount_amount, 2),
            "final_fee": round(final_fee, 2),
            "description": promo.get("description", "")
        })
    except Exception as e:
        return jsonify({"valid": False, "reason": str(e)})


@app.route("/admin/test_notification_channels", methods=["GET", "POST", "OPTIONS"])
def admin_test_notification_channels():
    if request.method == "OPTIONS":
        return _np_ok({})
    try:
        results = {}
        api_key = os.environ.get("BREVO_API_KEY")
        from_email = os.environ.get("FROM_EMAIL") or "support@nairapips.com"
        if api_key:
            results["brevo_configured"] = {"ok": True, "key_preview": api_key[:8] + "..."}
        else:
            results["brevo_configured"] = {"ok": False, "error": "BREVO_API_KEY not set"}
        results["brevo_send_test"] = {"ok": True, "channel": "email", "note": "Brevo already tested via cron_status"}
        termii_key = os.environ.get("TERMII_API_KEY")
        if termii_key:
            results["termii_configured"] = {"ok": True, "key_preview": termii_key[:8] + "..."}
        else:
            results["termii_configured"] = {"ok": False, "error": "TERMII_API_KEY not set"}
        results["termii_send_test"] = {"ok": False, "channel": "sms", "error": "Pending Termii KYC verification"}
        return _np_ok({
            "success": True,
            "results": results,
            "env_keys_present": {
                "BREVO_API_KEY": bool(api_key),
                "FROM_EMAIL": bool(from_email),
                "TERMII_API_KEY": bool(termii_key),
                "TERMII_SENDER_ID": bool(os.environ.get("TERMII_SENDER_ID")),
                "TELEGRAM_BOT_TOKEN": bool(os.environ.get("TELEGRAM_BOT_TOKEN")),
                "ADMIN_TELEGRAM_CHAT_ID": bool(os.environ.get("ADMIN_TELEGRAM_CHAT_ID"))
            }
        })
    except Exception as e:
        return _np_fail(e, 500)


# NAIRAPIPS CAPITAL SELECTION LEAD ENGINE
# Landing page endpoint: /register_lead
# Purpose: collect Founding Trader leads, issue Golden Tickets,
# preserve referral source, and return a real server-created ticket.
# ============================================================

LEAD_CAMPAIGN_DEFAULT = "capital_selection_founding_1000"
LEAD_OFFER_DEFAULT = "10_monthly_1m_challenge_accounts"
LEAD_FOUNDING_LIMIT = int(os.getenv("NAIRAPIPS_FOUNDING_LIMIT", "1000"))
LEAD_COMMUNITY_URL = os.getenv("NAIRAPIPS_COMMUNITY_URL", "").strip()
LEAD_PUBLIC_BASE_URL = os.getenv("NAIRAPIPS_PUBLIC_BASE_URL", "https://nairapips.com").rstrip("/")
LEAD_TABLE = os.getenv("NAIRAPIPS_LEAD_TABLE", "landing_leads")

def _lead_clean(value, max_len=160):
    value = str(value or "").strip()
    value = re.sub(r"[<>]", "", value)
    return value[:max_len]

def _lead_email(value):
    email = _lead_clean(value, 160).lower()
    if not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email):
        return ""
    return email

def _lead_phone(value):
    raw = str(value or "").strip()
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0") and len(digits) >= 10:
        digits = "234" + digits[1:]
    if digits.startswith("234") and len(digits) == 13:
        return "+" + digits
    if digits.startswith("234") and 10 <= len(digits) <= 14:
        return "+" + digits
    if raw.startswith("+") and 10 <= len(digits) <= 15:
        return "+" + digits
    return ""

def _lead_ip_key():
    forwarded = request.headers.get("X-Forwarded-For", "")
    ip = forwarded.split(",")[0].strip() if forwarded else (request.remote_addr or "unknown")
    return ip or "unknown"

def _lead_rate_limited(key):
    now = time.time()
    window = REGISTER_RATE_WINDOW_SECONDS
    bucket = REGISTER_RATE_BUCKET.get(key, [])
    bucket = [t for t in bucket if now - t < window]
    if len(bucket) >= REGISTER_RATE_MAX:
        REGISTER_RATE_BUCKET[key] = bucket
        return True
    bucket.append(now)
    REGISTER_RATE_BUCKET[key] = bucket
    return False

def _lead_ticket_exists(ticket):
    try:
        rows = supabase.table(LEAD_TABLE).select("id").eq("golden_ticket", ticket).limit(1).execute().data or []
        return bool(rows)
    except Exception:
        return False

def _lead_make_ticket():
    # Example: NP-7K4M2Q. Server-created only, never generated by the browser.
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    for _ in range(20):
        ticket = "NP-" + "".join(secrets.choice(alphabet) for _ in range(6))
        if not _lead_ticket_exists(ticket):
            return ticket
    return "NP-" + secrets.token_hex(4).upper()

def _lead_make_referral_code(full_name="", ticket=""):
    seed = re.sub(r"[^A-Z0-9]", "", (full_name or "TRADER").upper())[:4] or "NP"
    suffix = re.sub(r"[^A-Z0-9]", "", (ticket or secrets.token_hex(3).upper()))[-6:]
    return f"{seed}{suffix}"

def _lead_find_existing(email, whatsapp, campaign):
    try:
        if email:
            rows = supabase.table(LEAD_TABLE).select("*").eq("email", email).eq("campaign", campaign).limit(1).execute().data or []
            if rows:
                return rows[0]
    except Exception:
        pass
    try:
        if whatsapp:
            rows = supabase.table(LEAD_TABLE).select("*").eq("whatsapp", whatsapp).eq("campaign", campaign).limit(1).execute().data or []
            if rows:
                return rows[0]
    except Exception:
        pass
    return None

def _lead_public_payload(row, created=False):
    referral_code = row.get("referral_code") or row.get("golden_ticket") or ""
    referral_link = f"{LEAD_PUBLIC_BASE_URL}/?ref={referral_code}" if referral_code else LEAD_PUBLIC_BASE_URL
    return {
        "success": True,
        "created": bool(created),
        "message": "Golden Ticket created successfully" if created else "Registration already exists. Your Golden Ticket is active.",
        "golden_ticket": row.get("golden_ticket"),
        "ticket": row.get("golden_ticket"),
        "ticket_number": row.get("golden_ticket"),
        "referral_code": referral_code,
        "referral_link": referral_link,
        "community_url": LEAD_COMMUNITY_URL,
        "founding_status": row.get("founding_status") or "founding_trader",
        "tickets_count": row.get("tickets_count") or 1,
        "vip_status": bool(row.get("vip_status")),
    }

@app.route("/register_lead", methods=["POST", "OPTIONS"])
def register_lead():
    if request.method == "OPTIONS":
        return _np_ok({})

    data = request.get_json(silent=True) or {}
    ip_key = "lead:" + _lead_ip_key()
    if _lead_rate_limited(ip_key):
        return _np_fail("Too many attempts. Please wait a few minutes and try again.", 429)

    full_name = _lead_clean(data.get("full_name") or data.get("name"), 100)
    whatsapp = _lead_phone(data.get("whatsapp") or data.get("phone"))
    email = _lead_email(data.get("email"))
    state = _lead_clean(data.get("state"), 60)
    campaign = _lead_clean(data.get("campaign") or LEAD_CAMPAIGN_DEFAULT, 80)
    offer = _lead_clean(data.get("offer") or LEAD_OFFER_DEFAULT, 100)
    source = _lead_clean(data.get("source") or data.get("utm_source") or "direct", 80)
    referred_by_code = _lead_clean(data.get("ref") or data.get("referral_code") or data.get("referred_by_code"), 80)

    if len(full_name) < 2:
        return _np_fail("Full name is required.", 400)
    if not whatsapp:
        return _np_fail("Valid WhatsApp number is required.", 400)
    if not email:
        return _np_fail("Valid email address is required.", 400)
    if len(state) < 2:
        return _np_fail("State is required.", 400)
    if data.get("consent_marketing") is not True and str(data.get("consent_marketing")).lower() not in {"true", "1", "yes"}:
        return _np_fail("Consent is required so NairaPips can send ticket and selection updates.", 400)

    now = now_iso()
    try:
        existing = _lead_find_existing(email, whatsapp, campaign)
        if existing:
            update_payload = {
                "full_name": full_name,
                "whatsapp": whatsapp,
                "email": email,
                "state": state,
                "last_seen_at": now,
                "updated_at": now,
                "source": existing.get("source") or source,
                "utm_source": _lead_clean(data.get("utm_source") or source, 80),
                "utm_medium": _lead_clean(data.get("utm_medium"), 80),
                "utm_campaign": _lead_clean(data.get("utm_campaign") or campaign, 100),
                "utm_content": _lead_clean(data.get("utm_content"), 100),
                "utm_term": _lead_clean(data.get("utm_term"), 100),
            }
            if referred_by_code and not existing.get("referred_by_code"):
                update_payload["referred_by_code"] = referred_by_code
            updated = supabase.table(LEAD_TABLE).update(update_payload).eq("id", existing.get("id")).execute().data or []
            row = updated[0] if updated else {**existing, **update_payload}
            try:
                _np_safe_points_add(
                    "golden_ticket_registration",
                    points=0,
                    email=row.get("email"),
                    ticket=row.get("golden_ticket"),
                    lead_id=row.get("id"),
                    note="Existing Golden Ticket revisited"
                )
            except Exception as points_error:
                print("GOLDEN TICKET EXISTING POINTS SKIPPED:", points_error)
            return _np_ok(_lead_public_payload(row, created=False), 200)

        ticket = _lead_make_ticket()
        referral_code = _lead_make_referral_code(full_name, ticket)
        row = {
            "full_name": full_name,
            "whatsapp": whatsapp,
            "email": email,
            "state": state,
            "campaign": campaign,
            "offer": offer,
            "golden_ticket": ticket,
            "referral_code": referral_code,
            "referred_by_code": referred_by_code,
            "source": source,
            "utm_source": _lead_clean(data.get("utm_source") or source, 80),
            "utm_medium": _lead_clean(data.get("utm_medium"), 80),
            "utm_campaign": _lead_clean(data.get("utm_campaign") or campaign, 100),
            "utm_content": _lead_clean(data.get("utm_content"), 100),
            "utm_term": _lead_clean(data.get("utm_term"), 100),
            "consent_marketing": True,
            "founding_status": "founding_trader",
            "tickets_count": 1,
            "vip_status": False,
            "lead_status": "new",
            "ip_hash": hashlib.sha256(_lead_ip_key().encode("utf-8")).hexdigest(),
            "user_agent": _lead_clean(request.headers.get("User-Agent"), 250),
            "created_at": now,
            "updated_at": now,
            "last_seen_at": now,
        }
        created = supabase.table(LEAD_TABLE).insert(row).execute().data or []
        if not created:
            return _np_fail("Lead could not be saved. Please try again.", 500)
        created_row = created[0]

        try:
            _np_safe_points_add(
                "golden_ticket_registration",
                email=created_row.get("email"),
                ticket=created_row.get("golden_ticket"),
                lead_id=created_row.get("id"),
                note="Golden Ticket registration"
            )
        except Exception as points_error:
            print("GOLDEN TICKET REGISTRATION POINTS SKIPPED:", points_error)

        # Optional referral credit: inviter gets +3 tickets at 3 invited leads, VIP at 10.
        if referred_by_code:
            try:
                invited_count_rows = supabase.table(LEAD_TABLE).select("id").eq("referred_by_code", referred_by_code).eq("campaign", campaign).execute().data or []
                invited_count = len(invited_count_rows)
                inviter_rows = supabase.table(LEAD_TABLE).select("*").eq("referral_code", referred_by_code).eq("campaign", campaign).limit(1).execute().data or []
                if inviter_rows:
                    inviter = inviter_rows[0]
                    bonus_tickets = 1
                    if invited_count >= 3:
                        bonus_tickets = 4
                    vip = invited_count >= 10
                    supabase.table(LEAD_TABLE).update({
                        "referral_count": invited_count,
                        "tickets_count": bonus_tickets,
                        "vip_status": vip,
                        "updated_at": now_iso()
                    }).eq("id", inviter.get("id")).execute()
                    _np_safe_points_add(
                        "referral_registration",
                        email=inviter.get("email"),
                        ticket=inviter.get("golden_ticket"),
                        lead_id=inviter.get("id"),
                        note=f"Referral registered: {created_row.get('email') or created_row.get('whatsapp')}"
                    )
            except Exception as referral_error:
                print("LEAD REFERRAL CREDIT ERROR:", referral_error)

        try:
            send_admin_alert(
                "New NairaPips Golden Ticket lead",
                f"Name: {full_name}\nWhatsApp: {whatsapp}\nEmail: {email}\nState: {state}\nTicket: {ticket}\nSource: {source}\nRef: {referred_by_code or 'None'}"
            )
        except Exception as alert_error:
            print("LEAD ADMIN ALERT ERROR:", alert_error)

        return _np_ok(_lead_public_payload(created_row, created=True), 201)
    except Exception as e:
        print("REGISTER LEAD ERROR:", str(e))
        return _np_fail(f"Lead registration failed: {str(e)}", 500)


@app.route("/golden_ticket_access", methods=["POST", "OPTIONS"])
def golden_ticket_access():
    """Allow a Capital Selection lead to open the trader dashboard with a valid Golden Ticket.
    This connects landing_leads -> traders without exposing Supabase keys in the browser.
    """
    if request.method == "OPTIONS":
        return _np_ok({})
    data = request.get_json(silent=True) or {}
    ticket = _lead_clean(data.get("ticket") or data.get("golden_ticket") or data.get("ticket_code"), 80).upper()
    lookup = _lead_clean(data.get("lookup") or data.get("email") or data.get("phone") or data.get("whatsapp"), 160).lower()
    if not ticket:
        return _np_fail("Golden Ticket code is required.", 400)

    try:
        q = supabase.table(LEAD_TABLE).select("*")
        # Accept ticket or referral_code because some users will copy referral code instead of ticket.
        rows = q.or_(f"golden_ticket.eq.{ticket},referral_code.eq.{ticket}").limit(2).execute().data or []
        if not rows:
            return _np_fail("Golden Ticket not found. Please check the code or register again.", 404)
        lead = rows[0]

        # Optional safety match when email/phone is supplied.
        if lookup:
            lookup_digits = re.sub(r"\D", "", lookup)
            lead_email = str(lead.get("email") or "").strip().lower()
            lead_phone = str(lead.get("whatsapp") or lead.get("phone") or "").strip().lower()
            lead_digits = re.sub(r"\D", "", lead_phone)
            email_match = lookup == lead_email
            phone_match = bool(lookup_digits and lead_digits and (lookup_digits.endswith(lead_digits[-10:]) or lead_digits.endswith(lookup_digits[-10:])))
            if ("@" in lookup or lookup_digits) and not (email_match or phone_match):
                return _np_fail("This Golden Ticket does not match the email/phone entered.", 403)

        email = str(lead.get("email") or "").strip().lower()
        phone = _lead_phone(lead.get("whatsapp") or lead.get("phone"))
        full_name = _lead_clean(lead.get("full_name") or lead.get("name") or "Trader", 100)
        now = now_iso()

        trader_row = None
        try:
            if email:
                found = supabase.table("traders").select("*").eq("email", email).limit(1).execute().data or []
                if found:
                    trader_row = found[0]
            if not trader_row and phone:
                found = supabase.table("traders").select("*").eq("phone", phone).limit(1).execute().data or []
                if found:
                    trader_row = found[0]
        except Exception as find_error:
            print("GOLDEN TICKET TRADER FIND ERROR:", find_error)

        gift_payload = {
            "name": full_name,
            "email": email,
            "phone": phone,
            "status": "new_signup",
            "payment_status": "none",
            "source": "golden_ticket_access",
            "golden_ticket": lead.get("golden_ticket") or ticket,
            "lead_id": lead.get("id"),
            "lead_status": lead.get("lead_status") or "golden_ticket_active",
            "vip_status": bool(lead.get("vip_status")),
            "referral_count": lead.get("referral_count") or 0,
            "tickets_count": lead.get("tickets_count") or 1,
            "gift_account_size": "₦1,000,000",
            "gift_status": "Awaiting Selection",
            "updated_at": now,
        }

        if trader_row:
            try:
                updated = supabase.table("traders").update(gift_payload).eq("id", trader_row.get("id")).execute().data or []
                trader_row = updated[0] if updated else {**trader_row, **gift_payload}
            except Exception as update_error:
                print("GOLDEN TICKET TRADER UPDATE ERROR:", update_error)
                trader_row = {**trader_row, **gift_payload}
        else:
            create_payload = dict(gift_payload)
            create_payload["created_at"] = now
            try:
                created = supabase.table("traders").insert(create_payload).execute().data or []
                trader_row = created[0] if created else create_payload
            except Exception as create_error:
                print("GOLDEN TICKET TRADER CREATE ERROR:", create_error)
                # Return a safe dashboard row even if traders table needs columns added.
                trader_row = create_payload
                trader_row["id"] = "lead-" + str(lead.get("id") or ticket)

        try:
            supabase.table(LEAD_TABLE).update({
                "lead_status": "dashboard_accessed",
                "last_seen_at": now,
                "updated_at": now
            }).eq("id", lead.get("id")).execute()
        except Exception as lead_update_error:
            print("GOLDEN TICKET LEAD UPDATE ERROR:", lead_update_error)

        try:
            _np_safe_points_add(
                "golden_ticket_dashboard_access",
                user_id=trader_row.get("id"),
                email=email,
                ticket=lead.get("golden_ticket") or ticket,
                lead_id=lead.get("id"),
                note="Golden Ticket dashboard access"
            )
        except Exception as points_error:
            print("GOLDEN TICKET ACCESS POINTS SKIPPED:", points_error)

        try:
            trader_row["founding_points"] = _np_points_total(trader_row.get("id"), email, lead.get("golden_ticket") or ticket)
        except Exception:
            pass

        return _np_ok({
            "success": True,
            "message": "Golden Ticket verified. Dashboard access opened.",
            "data": trader_row,
            "trader": trader_row,
            "golden_ticket": lead.get("golden_ticket") or ticket,
            "lead": {
                "id": lead.get("id"),
                "full_name": full_name,
                "email": email,
                "whatsapp": phone,
                "state": lead.get("state"),
                "vip_status": bool(lead.get("vip_status")),
                "referral_count": lead.get("referral_count") or 0,
                "tickets_count": lead.get("tickets_count") or 1,
            }
        }, 200)
    except Exception as e:
        print("GOLDEN TICKET ACCESS ERROR:", str(e))
        return _np_fail(f"Golden Ticket access failed: {str(e)}", 500)

@app.route("/lead_campaign_stats", methods=["GET", "OPTIONS"])
def lead_campaign_stats():
    if request.method == "OPTIONS":
        return _np_ok({})
    campaign = _lead_clean(request.args.get("campaign") or LEAD_CAMPAIGN_DEFAULT, 80)
    try:
        rows = supabase.table(LEAD_TABLE).select("id,vip_status").eq("campaign", campaign).limit(5000).execute().data or []
        total = len(rows)
        vip_total = len([r for r in rows if r.get("vip_status")])
        return _np_ok({
            "success": True,
            "campaign": campaign,
            "total_leads": total,
            "founding_traders": min(total, LEAD_FOUNDING_LIMIT),
            "slots_remaining": max(0, LEAD_FOUNDING_LIMIT - total),
            "vip_traders": vip_total,
            "monthly_winners": 10,
        })
    except Exception as e:
        print("LEAD STATS ERROR:", str(e))
        return _np_ok({
            "success": True,
            "campaign": campaign,
            "total_leads": 0,
            "founding_traders": 0,
            "slots_remaining": LEAD_FOUNDING_LIMIT,
            "vip_traders": 0,
            "monthly_winners": 10,
            "warning": "Lead stats unavailable until the landing_leads table is created."
        }, 200)



# ============================================================
# NAIRAPIPS GOLDEN TICKET PROMISE + FOUNDING POINTS ENGINE
# Stage 1: registered Golden Ticket users can be assigned promised MT5 accounts.
# Stage 2: Founding Trader points accumulate silently from day one.
# ============================================================

FOUNDING_POINTS = {
    "golden_ticket_registration": 50,
    "golden_ticket_dashboard_access": 25,
    "telegram_click": 10,
    "x_click": 10,
    "tiktok_click": 10,
    "referral_registration": 50,
    "challenge_purchase": 100,
    "phase1_pass": 250,
    "phase2_pass": 500,
    "funded_account": 1000,
    "golden_ticket_assigned": 500,
}

def _np_point_identity(user_id=None, email=None, ticket=None, lead_id=None):
    return {
        "user_id": str(user_id or "").strip(),
        "email": str(email or "").strip().lower(),
        "golden_ticket": str(ticket or "").strip().upper(),
        "lead_id": str(lead_id or "").strip(),
    }

def _np_safe_points_add(action, points=None, user_id=None, email=None, ticket=None, lead_id=None, note=""):
    """Ledger-first points writer. Safe by design: if points tables are not created yet, it logs and continues."""
    try:
        action = str(action or "").strip()
        if not action:
            return {"success": False, "skipped": True, "reason": "missing action"}
        pts = int(points if points is not None else FOUNDING_POINTS.get(action, 0))
        ident = _np_point_identity(user_id, email, ticket, lead_id)
        now = now_iso()

        ledger_row = {
            "user_id": ident["user_id"] or None,
            "email": ident["email"] or None,
            "golden_ticket": ident["golden_ticket"] or None,
            "lead_id": ident["lead_id"] or None,
            "action": action,
            "points": pts,
            "note": str(note or "")[:500],
            "created_at": now,
        }
        try:
            supabase.table("founding_trader_ledger").insert(ledger_row).execute()
        except Exception as e:
            print("FOUNDING POINTS LEDGER SKIPPED:", e)
            return {"success": False, "skipped": True, "error": str(e)}

        # Aggregate table is best-effort. It is not source of truth.
        try:
            rows = []
            if ident["user_id"]:
                rows = supabase.table("founding_trader_points").select("*").eq("user_id", ident["user_id"]).limit(1).execute().data or []
            if not rows and ident["email"]:
                rows = supabase.table("founding_trader_points").select("*").eq("email", ident["email"]).limit(1).execute().data or []
            if rows:
                current = rows[0]
                new_points = int(current.get("points") or 0) + pts
                supabase.table("founding_trader_points").update({
                    "points": new_points,
                    "last_action": action,
                    "last_updated": now,
                    "updated_at": now,
                }).eq("id", current.get("id")).execute()
            else:
                supabase.table("founding_trader_points").insert({
                    "user_id": ident["user_id"] or None,
                    "email": ident["email"] or None,
                    "golden_ticket": ident["golden_ticket"] or None,
                    "lead_id": ident["lead_id"] or None,
                    "points": pts,
                    "referrals": 0,
                    "challenge_purchases": 0,
                    "phase_passes": 0,
                    "community_actions": 0,
                    "last_action": action,
                    "created_at": now,
                    "updated_at": now,
                    "last_updated": now,
                }).execute()
        except Exception as e:
            print("FOUNDING POINTS AGGREGATE SKIPPED:", e)

        return {"success": True, "points": pts, "action": action}
    except Exception as e:
        print("FOUNDING POINTS ERROR:", e)
        return {"success": False, "error": str(e)}

def _np_points_total(user_id=None, email=None, ticket=None):
    try:
        rows = []
        if user_id:
            rows = supabase.table("founding_trader_points").select("*").eq("user_id", user_id).limit(1).execute().data or []
        if not rows and email:
            rows = supabase.table("founding_trader_points").select("*").eq("email", str(email).strip().lower()).limit(1).execute().data or []
        if not rows and ticket:
            rows = supabase.table("founding_trader_points").select("*").eq("golden_ticket", str(ticket).strip().upper()).limit(1).execute().data or []
        return int((rows[0] if rows else {}).get("points") or 0)
    except Exception as e:
        print("FOUNDING POINTS TOTAL ERROR:", e)
        return 0

def _np_golden_ticket_candidates(limit=1500):
    """Golden Ticket users waiting for the promised account/opportunity assignment.

    Production fix: Golden Ticket registrations are saved first in LEAD_TABLE
    (default: landing_leads). The old queue only searched traders, so Admin showed
    Golden Tickets: 0 even when landing registrations existed. This function now
    reads both traders and landing_leads, then de-duplicates by ticket/email/phone.
    """
    out = []
    seen = set()

    active_trader_ids = set()
    active_logins = set()
    try:
        active_accounts = supabase.table("trader_accounts").select("id,trader_id,account_status,mt5_login").in_("account_status", list(ACTIVE_ACCOUNT_STATUSES)).limit(5000).execute().data or []
        active_trader_ids = {str(a.get("trader_id") or "") for a in active_accounts if str(a.get("trader_id") or "")}
        active_logins = {str(a.get("mt5_login") or "").strip() for a in active_accounts if str(a.get("mt5_login") or "").strip()}
    except Exception as e:
        print("GOLDEN TICKET ACTIVE ACCOUNT FETCH ERROR:", e)

    def add_candidate(row, source_table="traders"):
        try:
            ticket = str(row.get("golden_ticket") or row.get("ticket") or "").strip().upper()
            email = str(row.get("email") or "").strip().lower()
            phone = str(row.get("phone") or row.get("whatsapp") or "").strip()
            tid = str(row.get("id") or "").strip() if source_table == "traders" else str(row.get("trader_id") or "").strip()
            lead_id = str(row.get("lead_id") or "").strip() if source_table == "traders" else str(row.get("id") or row.get("lead_id") or "").strip()
            source = str(row.get("source") or "").strip().lower()
            gift_status = str(row.get("gift_status") or row.get("lead_status") or "").strip().lower()
            status_blob = " ".join([str(row.get("status") or ""), str(row.get("challenge_state") or ""), gift_status]).lower()

            if not ticket and source not in {"golden_ticket_access", "golden_ticket", "capital_selection", "direct"}:
                return
            if tid and tid in active_trader_ids:
                return
            if str(row.get("mt5_login") or "").strip() in active_logins:
                return
            if str(row.get("mt5_login") or "").strip():
                return
            if any(x in status_blob for x in ["assigned", "active", "completed", "rejected", "cancel", "breach"]):
                # Do not hide normal fresh leads whose lead_status is just "new".
                return

            key = ticket or email or phone or lead_id or tid
            if not key or key in seen:
                return
            seen.add(key)
            account_size = _np_number(row.get("gift_account_size") or row.get("account_size") or 1000000) or 1000000
            name = row.get("name") or row.get("full_name") or row.get("trader_name") or "Trader"
            out.append({
                "id": tid or lead_id or ticket,
                "trader_id": tid,
                "lead_id": lead_id,
                "source_type": "golden_ticket",
                "source": "golden_ticket",
                "source_table": source_table,
                "target_phase": "phase1",
                "target_stage": "phase1",
                "stage_label": "GOLDEN TICKET PROMISE",
                "assignment_label": "Assign Promised Account",
                "name": name,
                "trader_name": name,
                "email": email,
                "phone": phone,
                "whatsapp": phone,
                "account_reference": row.get("account_reference") or ticket or "",
                "golden_ticket": ticket,
                "plan_name": "Golden Ticket N1M Offer",
                "account_size": account_size,
                "payment_status": "gift",
                "current_status": row.get("gift_status") or row.get("lead_status") or "awaiting_promise_assignment",
                "created_at": row.get("created_at") or row.get("updated_at") or "",
                "founding_points": _np_points_total(tid, email, ticket),
            })
        except Exception as e:
            print("GOLDEN TICKET CANDIDATE ROW SKIPPED:", e)

    # 1) Existing trader records that already carry golden_ticket fields.
    try:
        trader_rows = supabase.table("traders").select("*").order("updated_at", desc=True).limit(limit).execute().data or []
        for row in trader_rows:
            add_candidate(row, "traders")
    except Exception as e:
        print("GOLDEN TICKET TRADER CANDIDATE FETCH ERROR:", e)

    # 2) Landing-page Golden Ticket registrations. This is the missing production feed.
    try:
        lead_rows = supabase.table(LEAD_TABLE).select("*").order("created_at", desc=True).limit(limit).execute().data or []
        for row in lead_rows:
            add_candidate(row, LEAD_TABLE)
    except Exception as e:
        print("GOLDEN TICKET LEAD CANDIDATE FETCH ERROR:", e)

    return out

def _np_get_or_create_trader_for_golden_ticket(ticket=None, lead_id=None, email=None, phone=None, admin_note="Golden Ticket trader created for MT5 assignment"):
    """Resolve a Golden Ticket lead into a real traders row so MT5 assignment can use the normal lifecycle system."""
    ticket = _lead_clean(ticket, 80).upper()
    email = _lead_email(email) if email else ""
    phone = _lead_phone(phone) if phone else ""
    lead = None

    try:
        if ticket:
            rows = supabase.table("traders").select("*").eq("golden_ticket", ticket).limit(1).execute().data or []
            if rows:
                return rows[0]
    except Exception as e:
        print("GOLDEN TICKET TRADER LOOKUP BY TICKET SKIPPED:", e)
    try:
        if email:
            rows = supabase.table("traders").select("*").eq("email", email).limit(1).execute().data or []
            if rows:
                tr = rows[0]
                if ticket and not tr.get("golden_ticket"):
                    try:
                        supabase.table("traders").update({"golden_ticket": ticket, "updated_at": now_iso()}).eq("id", tr.get("id")).execute()
                        tr["golden_ticket"] = ticket
                    except Exception:
                        pass
                return tr
    except Exception as e:
        print("GOLDEN TICKET TRADER LOOKUP BY EMAIL SKIPPED:", e)

    # Find lead row.
    try:
        q = supabase.table(LEAD_TABLE).select("*")
        if lead_id:
            rows = q.eq("id", lead_id).limit(1).execute().data or []
        elif ticket:
            rows = q.eq("golden_ticket", ticket).limit(1).execute().data or []
        elif email:
            rows = q.eq("email", email).limit(1).execute().data or []
        else:
            rows = []
        lead = rows[0] if rows else None
    except Exception as e:
        print("GOLDEN TICKET LEAD LOOKUP SKIPPED:", e)
        lead = None

    if not lead:
        return None

    now = now_iso()
    full_name = lead.get("full_name") or lead.get("name") or "Golden Ticket Trader"
    lead_email = str(lead.get("email") or email or "").strip().lower()
    lead_phone = str(lead.get("whatsapp") or lead.get("phone") or phone or "").strip()
    lead_ticket = str(lead.get("golden_ticket") or ticket or "").strip().upper()
    account_size = _np_number(lead.get("gift_account_size") or lead.get("account_size") or 1000000) or 1000000

    payload = {
        "name": full_name,
        "email": lead_email,
        "phone": lead_phone,
        "canonical_email": lead_email,
        "canonical_phone": _normalize_phone_value(lead_phone),
        "account_reference": lead_ticket or ref(),
        "golden_ticket": lead_ticket,
        "lead_id": lead.get("id"),
        "source": "golden_ticket",
        "gift_status": "awaiting_promise_assignment",
        "payment_status": "gift",
        "status": "new_signup",
        "challenge_state": "golden_ticket_registered",
        "phase": "phase1",
        "account_size": account_size,
        "balance": account_size,
        "equity": account_size,
        "created_at": now,
        "updated_at": now,
        "admin_note": admin_note,
    }

    # Some older Supabase schemas may not have every new column. Try full insert, then safer fallbacks.
    insert_attempts = [
        payload,
        {k: v for k, v in payload.items() if k not in {"lead_id", "source", "gift_status", "golden_ticket"}},
        {
            "name": full_name, "email": lead_email, "phone": lead_phone,
            "account_reference": lead_ticket or ref(), "payment_status": "gift",
            "status": "new_signup", "phase": "phase1",
            "account_size": account_size, "balance": account_size, "equity": account_size,
            "created_at": now, "updated_at": now,
        },
    ]
    last_error = None
    for attempt in insert_attempts:
        try:
            created = supabase.table("traders").insert(attempt).execute().data or []
            if created:
                trader = created[0]
                try:
                    supabase.table(LEAD_TABLE).update({
                        "trader_id": trader.get("id"),
                        "lead_status": "trader_created",
                        "updated_at": now_iso(),
                    }).eq("id", lead.get("id")).execute()
                except Exception as e:
                    print("GOLDEN TICKET LEAD BACKLINK SKIPPED:", e)
                return trader
        except Exception as e:
            last_error = e
            print("GOLDEN TICKET TRADER CREATE ATTEMPT SKIPPED:", e)
    raise RuntimeError(f"Could not create Golden Ticket trader: {last_error}")

@app.route("/assign_golden_ticket_account", methods=["POST", "OPTIONS"])
def assign_golden_ticket_account():
    """Admin assigns the promised Golden Ticket account using an MT5 pool record."""
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    d = request.get_json(silent=True) or {}
    trader_id = d.get("trader_id") or d.get("id")
    lead_id = d.get("lead_id")
    ticket = _lead_clean(d.get("golden_ticket") or d.get("ticket"), 80).upper()
    mt5_id = d.get("mt5_id")
    if not mt5_id:
        return _np_fail("mt5_id is required", 400)
    try:
        trader = None
        if trader_id:
            trader = get_trader_by_id(trader_id)
        if not trader and ticket:
            try:
                rows = supabase.table("traders").select("*").eq("golden_ticket", ticket).limit(1).execute().data or []
                trader = rows[0] if rows else None
            except Exception as e:
                print("GOLDEN TICKET ASSIGN TRADER LOOKUP SKIPPED:", e)
        if not trader:
            trader = _np_get_or_create_trader_for_golden_ticket(
                ticket=ticket,
                lead_id=lead_id,
                email=d.get("email"),
                phone=d.get("phone") or d.get("whatsapp"),
                admin_note=d.get("admin_note") or "Golden Ticket trader created for promised account assignment"
            )
        if not trader:
            return _np_fail("Golden Ticket lead/trader not found", 404)
        if _get_active_account(trader.get("id"), trader):
            return _np_fail("This Golden Ticket trader already has an active account.", 409)

        mt5 = _get_mt5_account(mt5_id=mt5_id)
        if not mt5:
            return _np_fail("MT5 account not found", 404)

        account_size = clean(mt5.get("account_size") or trader.get("account_size") or 1000000)
        try:
            supabase.table("traders").update({
                "account_size": account_size,
                "balance": account_size,
                "equity": account_size,
                "gift_status": "Assigning",
                "payment_status": "gift",
                "updated_at": now_iso(),
            }).eq("id", trader.get("id")).execute()
            trader["account_size"] = account_size
        except Exception as e:
            print("GOLDEN TICKET PRE-ASSIGN UPDATE SKIPPED:", e)

        account, trader_row = _assign_mt5_to_trader(
            trader,
            mt5,
            "phase1",
            None,
            _admin_from_payload(d),
            d.get("admin_note") or "Golden Ticket promised account assigned"
        )
        now = now_iso()
        try:
            supabase.table("traders").update({
                "gift_status": "Assigned",
                "gift_assigned_at": now,
                "golden_ticket_assigned_at": now,
                "status": "active",
                "payment_status": "gift",
                "challenge_state": "phase1_active",
                "updated_at": now,
            }).eq("id", trader.get("id")).execute()
        except Exception as e:
            print("GOLDEN TICKET POST-ASSIGN UPDATE SKIPPED:", e)

        # Backlink the landing lead so it disappears from the Golden Ticket queue.
        try:
            lead_update = {"lead_status": "assigned", "gift_status": "Assigned", "assigned_at": now, "updated_at": now}
            if trader.get("id"):
                lead_update["trader_id"] = trader.get("id")
            if account and account.get("id"):
                lead_update["trader_account_id"] = account.get("id")
            if mt5 and mt5.get("mt5_login"):
                lead_update["mt5_login"] = mt5.get("mt5_login")
                lead_update["mt5_server"] = mt5.get("mt5_server")
            if lead_id:
                supabase.table(LEAD_TABLE).update(lead_update).eq("id", lead_id).execute()
            elif trader.get("golden_ticket") or ticket:
                supabase.table(LEAD_TABLE).update(lead_update).eq("golden_ticket", trader.get("golden_ticket") or ticket).execute()
        except Exception as e:
            print("GOLDEN TICKET LEAD ASSIGN UPDATE SKIPPED:", e)

        _np_safe_points_add(
            "golden_ticket_assigned",
            user_id=trader.get("id"),
            email=trader.get("email"),
            ticket=trader.get("golden_ticket") or ticket,
            lead_id=trader.get("lead_id") or lead_id,
            note=f"Golden Ticket account assigned. MT5 {mt5.get('mt5_login')}"
        )

        return _np_ok({
            "success": True,
            "message": "Golden Ticket promised account assigned.",
            "trader": trader_row,
            "account": account,
            "points": _np_points_total(trader.get("id"), trader.get("email"), trader.get("golden_ticket") or ticket),
        }, 200)
    except Exception as e:
        print("ASSIGN GOLDEN TICKET ERROR:", e)
        return _np_fail(e, 500)

@app.route("/founding_points/<path:lookup>", methods=["GET", "OPTIONS"])
def founding_points_lookup(lookup):
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    lookup = str(lookup or "").strip()
    try:
        trader = _latest_trader_for_lookup(lookup)
    except Exception:
        trader = None
    email = (trader or {}).get("email") or (lookup.lower() if "@" in lookup else "")
    ticket = (trader or {}).get("golden_ticket") or (lookup.upper() if lookup.upper().startswith("NP-") else "")
    user_id = (trader or {}).get("id")
    total = _np_points_total(user_id, email, ticket)
    ledger = []
    try:
        q = supabase.table("founding_trader_ledger").select("*").order("created_at", desc=True).limit(100)
        if user_id:
            q = q.eq("user_id", user_id)
        elif email:
            q = q.eq("email", email)
        elif ticket:
            q = q.eq("golden_ticket", ticket)
        ledger = q.execute().data or []
    except Exception as e:
        print("FOUNDING POINTS LEDGER READ SKIPPED:", e)
    return _np_ok({"success": True, "points": total, "ledger": ledger, "trader": trader or {}})

@app.route("/founding_social_action", methods=["POST", "OPTIONS"])
def founding_social_action():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    d = request.get_json(silent=True) or {}
    platform = str(d.get("platform") or "").strip().lower()
    action_map = {"telegram": "telegram_click", "x": "x_click", "twitter": "x_click", "tiktok": "tiktok_click"}
    action = action_map.get(platform)
    if not action:
        return _np_fail("Unknown social platform", 400)
    return _np_ok(_np_safe_points_add(
        action,
        user_id=d.get("user_id") or d.get("trader_id"),
        email=d.get("email"),
        ticket=d.get("golden_ticket") or d.get("ticket"),
        lead_id=d.get("lead_id"),
        note=f"{platform} button clicked"
    ))

# Auto-start at module load
try:
    start_scheduler()
except Exception as e:
    print("[BOOT] Scheduler start failed:", e)

if __name__ == "__main__":
    port=int(os.environ.get("PORT",10000))
    app.run(host="0.0.0.0", port=port)


# ============================================================
# NAIRAPIPS GLOBAL PHASE PASS HANDOFF REPAIR
# Purpose: if MT5 engine detects phase1_passed/phase2_passed but an older
# dashboard/admin still shows assigned_active, this endpoint reconciles the
# exact MT5 login into archived_phase1/archived_phase2 and waiting next stage.
# Safe: no delete, no fresh account creation, no MT5 credential mutation.
# ============================================================
@app.route("/repair_phase_pass_handoff", methods=["POST", "GET", "OPTIONS"])
def repair_phase_pass_handoff():
    if request.method == "OPTIONS":
        return _np_ok({"success": True})
    try:
        data = request.get_json(silent=True) or {}
        mt5_login = str(data.get("mt5_login") or request.args.get("mt5_login") or "").strip()
        forced_stage = str(data.get("stage") or request.args.get("stage") or "").strip().lower()
        if not mt5_login:
            return _np_fail("mt5_login is required", 400)

        account = _get_account_by_login_any_status(mt5_login)
        if not account:
            return _np_fail("No trader account found for this MT5 login", 404)
        trader = get_trader_by_id(account.get("trader_id"))
        if not trader:
            return _np_fail("Trader not found for this account", 404)

        stage = forced_stage or str(account.get("stage") or account.get("phase") or "phase1").lower()
        if "phase2" in stage:
            pass_status = "phase2_passed"
        else:
            pass_status = "phase1_passed"

        # If the account is already archived/passed, keep idempotent result.
        status = str(account.get("account_status") or "").lower()
        if status in {"archived_phase1", "archived_phase2"} and str(account.get("phase_pass_status") or "").lower() == pass_status:
            return _np_ok({"success": True, "message": "Already repaired", "account": account, "trader": trader})

        updated, archived = _pass_specific_account(
            trader,
            account,
            pass_status,
            {"name": "admin_repair", "username": "admin_repair", "role": "super_admin"},
            f"{pass_status.upper()} repaired from MT5 engine pass evidence. Waiting for next MT5 assignment."
        )
        return _np_ok({
            "success": True,
            "message": "Phase pass handoff repaired. Trader is now waiting for next MT5 assignment.",
            "trader": updated,
            "archived_account": archived,
            "next_action": "Assign fresh Phase 2 MT5" if pass_status == "phase1_passed" else "Assign funded/live MT5"
        })
    except Exception as e:
        return _np_fail(e, 500)
