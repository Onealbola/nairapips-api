
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone
import os, random, uuid, re, time, hmac, hashlib, base64, secrets, string
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
    if "breach" in status_blob or "locked" in status_blob or dd_used >= 100:
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


def _active_account_from_trader_profile(trader):
    """Compatibility bridge for already-migrated traders whose trader_accounts row is unavailable."""
    if not trader:
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

        rows = supabase.table("trader_accounts").select("*").eq("trader_id", trader_id).eq("account_status", "assigned_active").order("updated_at", desc=True).order("started_at", desc=True).order("created_at", desc=True).limit(50).execute().data or []

        current_account_id = (trader or {}).get("current_account_id")
        if current_account_id:
            for row in rows:
                if str(row.get("id") or "") == str(current_account_id):
                    status = str(row.get("account_status") or "").strip().lower()
                    if status in {"assigned_active", "active", "current_active"}:
                        return _decorate_account_for_api(row)
            direct_rows = supabase.table("trader_accounts").select("*").eq("id", current_account_id).limit(1).execute().data or []
            if direct_rows:
                status = str(direct_rows[0].get("account_status") or "").strip().lower()
                if status in {"assigned_active", "active", "current_active"}:
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
    This keeps newly assigned purchases visible even if their trader_accounts row is
    missing, archived incorrectly, or not marked assigned_active yet.
    """
    accounts = []
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
    if str(mt5.get("status") or "available").lower() != "available":
        raise ValueError("Selected MT5 account is not available")
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

@app.route("/update_trader_mt5", methods=["POST", "OPTIONS"])
@app.route("/reset_trader_mt5", methods=["POST", "OPTIONS"])
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
        return ok({"trader": trader, "current_account": account, "active_accounts": active_accounts, "accounts": active_accounts, "challenge_state": trader.get("challenge_state") or "registered"})
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
            "trading_days_left": d.get("trading_days_left", 30),
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
            "trading_days_left": d.get("trading_days_left", 30)
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
@app.route("/login_trader", methods=["POST"])
def login_trader():
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
        supabase.table("traders").update({"last_login_at": t}).eq("id", trader["id"]).execute()
        trader["last_login_at"] = t
        trader["auth_token"] = _make_trader_auth_token(trader.get("id"))
        account = _get_active_account(trader.get("id"), trader)
        if account:
            trader["current_account"] = account
            trader["current_account_id"] = account.get("id")
            trader["challenge_state"] = trader.get("challenge_state") or _active_state_for_stage(account.get("stage"))
            trader["phase"] = account.get("stage") or trader.get("phase")
            trader["mt5_login"] = account.get("mt5_login") or trader.get("mt5_login")
            trader["mt5_server"] = account.get("mt5_server") or trader.get("mt5_server")
            trader["profit"] = account.get("profit") or 0
            trader["profit_percent"] = account.get("profit_percent") or 0
            trader["drawdown_percent"] = account.get("absolute_drawdown_percent") or 0
            trader["max_drawdown_used"] = account.get("dd_used_percent") or 0
        return ok(trader, "Login successful")
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
    try: return jsonify(supabase.table("challenge_plans").select("*").order("account_size", desc=False).execute().data)
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
             "max_drawdown":float(d.get("max_drawdown") or 20),"daily_drawdown":d.get("daily_drawdown","None"),
             "payout_split":d.get("payout_split","80%"),"description":d.get("description",""),
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
        for k in ["name","daily_drawdown","payout_split","description","status","mt5_server","default_server"]:
            if k in d: upd[k]=d[k]
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
def lifecycle_breach_account():
    try:
        d = request.get_json(silent=True) or {}
        trader_id = d.get("trader_id") or d.get("id")
        if not trader_id:
            return bad("trader_id is required")
        reason = d.get("reason") or d.get("admin_note") or "Maximum drawdown or rule breach."
        trader, account = _breach_trader_account(trader_id, reason, _admin_from_payload(d))
        return ok({"trader": trader, "archived_account": account}, "Trader breached and active account archived")
    except Exception as e:
        return bad(e)

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
        upd={"updated_at":now_iso()}
        for k in ["plan_name","mt5_login","mt5_server","mt5_master_password","mt5_investor_password","status","admin_note"]:
            if k in d: upd[k]=d[k]
        if "account_size" in d: upd["account_size"]=clean(d.get("account_size"))
        return ok(supabase.table("mt5_pool").update(upd).eq("id",mid).execute().data, "MT5 account updated")
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
    try: return jsonify(supabase.table("payouts").select("*").order("created_at", desc=True).execute().data)
    except Exception as e: return bad(e)

@app.route("/create_payout", methods=["POST"])
def create_payout():
    try:
        d=request.json or {}; amount=clean(d.get("amount"))
        if amount<=0: return bad("Invalid payout amount")

        trader_row = _resolve_trader_for_money_action(d)
        eligible, reason, account = _payout_eligibility(trader_row)
        if not eligible:
            return bad(reason, 403)

        row={"trader_id":trader_row.get("id"),"trader_account_id":account.get("id"),"trader_name":d.get("trader_name") or trader_row.get("name") or "","email":d.get("email") or trader_row.get("email") or "","phone":d.get("phone") or trader_row.get("phone") or "",
             "amount":amount,"bank_name":d.get("bank_name",""),"account_number":d.get("account_number",""),"account_name":d.get("account_name",""),
             "status":"pending","note":d.get("note",""),"admin_note":"","requested_at":now_iso()}
        created = supabase.table("payouts").insert(row).execute().data

        send_email_safe(
            row.get("email"),
            "NairaPips payout request received",
            f"""Hello {row.get("trader_name") or "Trader"},

Your payout request has been received.

Amount: {email_money(amount)}
Bank: {row.get("bank_name") or "Not provided"}
Account Number: {row.get("account_number") or "Not provided"}

Admin will review your account and payout request.

NairaPips Team"""
        )
        send_admin_alert(
            "New NairaPips payout request",
            f"""A trader submitted a payout request.

Trader: {row.get("trader_name") or "Trader"}
Email: {row.get("email") or "Not provided"}
Phone: {row.get("phone") or "Not provided"}
Amount: {email_money(amount)}
Bank: {row.get("bank_name") or "Not provided"}
Account Number: {row.get("account_number") or "Not provided"}"""
        )

        return ok(created, "Payout request created")
    except Exception as e: return bad(e)

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
    try: return jsonify(supabase.table("announcements").select("*").eq("status","active").order("created_at", desc=True).execute().data)
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

    # Prefer MT5 engine drawdown values. Fallback keeps old FXBlue/manual behaviour alive.
    engine_dd = payload.get("drawdown_percent")
    if engine_dd is not None:
        drawdown_percent = _num(engine_dd, 0)
        equity_damage = max(0, (account_size or balance) * drawdown_percent / 100)
    else:
        peak_base = max(highest_equity, account_size)
        equity_damage = max(0, peak_base - lowest_equity)
        drawdown_percent = (equity_damage / peak_base * 100) if peak_base else 0

    dd_limit_percent = _num(active_account.get("dd_limit_percent"), MAX_DRAWDOWN_LIMIT) if active_account else MAX_DRAWDOWN_LIMIT
    max_dd_used = _safe_dd_used(payload, drawdown_percent, dd_limit_percent)

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
        "breach_equity_level": _num(payload.get("breach_equity_level"), 0),
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
                "dd_used_percent": max_dd_used,
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
            "max_drawdown_used": max_dd_used,
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
        "max_drawdown_used": max_dd_used,
        "risk_zone": update_data.get("risk_zone", zone),
        "critical_mode": update_data.get("critical_mode", False),
        "monitoring_priority": update_data.get("monitoring_priority", priority),
        "status": update_data.get("status", trader.get("status")),
        "phase_pass_status": update_data.get("phase_pass_status", trader.get("phase_pass_status"))
    }

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
@app.route("/sync_trades", methods=["POST"])
def sync_trades():
    try:
        d = request.json or {}
        trades = d.get("trades", [])

        if not isinstance(trades, list):
            return bad("trades must be a list")

        saved = []

        for t in trades:
            trader_account_id = t.get("trader_account_id")
            if not trader_account_id and t.get("mt5_login"):
                try:
                    acct_rows = supabase.table("trader_accounts").select("id").eq("mt5_login", str(t.get("mt5_login") or "")).eq("account_status", "assigned_active").limit(1).execute().data or []
                    trader_account_id = acct_rows[0].get("id") if acct_rows else None
                except Exception:
                    trader_account_id = None
            row = {
                "trader_id": t.get("trader_id"),
                "trader_account_id": trader_account_id,
                "trader_name": t.get("trader_name"),
                "email": t.get("email"),
                "mt5_login": str(t.get("mt5_login") or ""),
                "symbol": t.get("symbol"),
                "ticket": str(t.get("ticket") or ""),
                "trade_type": t.get("trade_type"),
                "volume": t.get("volume") or 0,
                "open_price": t.get("open_price") or 0,
                "current_price": t.get("current_price") or 0,
                "sl": t.get("sl") or 0,
                "tp": t.get("tp") or 0,
                "profit": t.get("profit") or 0,
                "swap": t.get("swap") or 0,
                "commission": t.get("commission") or 0,
                "status": t.get("status") or "open",
                "opened_at": t.get("opened_at"),
                "closed_at": t.get("closed_at"),
                "synced_at": now_iso()
            }

            existing = supabase.table("trader_trades").select("id").eq("ticket", row["ticket"]).limit(1).execute().data

            if existing:
                saved.append(
                    supabase.table("trader_trades").update(row).eq("ticket", row["ticket"]).execute().data
                )
            else:
                saved.append(
                    supabase.table("trader_trades").insert(row).execute().data
                )

        return ok(saved, "Trades synced")

    except Exception as e:
        return bad(e)

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
            updated, archived = _pass_specific_account(trader, account, pass_status, _admin_from_payload(d), reason)
            return ok({"trader": updated, "archived_account": archived}, "Exact MT5 account passed, archived, and moved to the next waiting state when it is the current account.")

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
    try:
        q = supabase.table("trader_trades") \
            .select("*") \
            .order("synced_at", desc=True) \
            .execute()

        return jsonify(getattr(q, "data", []) or [])

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
# Paste above: if __name__ == "__main__":

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
            "active_traders": len([t for t in trader_rows if str(t.get("status") or "").lower() == "active"]),
            "funded_traders": len([t for t in trader_rows if str(t.get("status") or "").lower() in ["funded", "live"] or str(t.get("phase") or "").lower() in ["funded", "live"]]),
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
# Paste above: if __name__ == "__main__":

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


if __name__ == "__main__":
    port=int(os.environ.get("PORT",10000))
    app.run(host="0.0.0.0", port=port)
