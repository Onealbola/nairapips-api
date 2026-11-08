# NP_WITHDRAWAL_RULES_V1_20261108 — configurable cap; original DD ownership unchanged
import time
NAIRAPIPS_MT5_ENGINE_RELEASE = "NON_DD_OPERATIONS_DIRECT_TELEMETRY_V3_2026_10_04"
import requests
import MetaTrader5 as mt5
import os
from datetime import datetime, timezone, timedelta

# Dedicated lightweight Monitoring API.
# The MT5 engine stays on the Windows VPS; this API only receives/returns monitoring data.
API_BASE_URL = os.getenv("NAIRAPIPS_MONITORING_API_BASE_URL", "https://nairapips-monitoring-api.onrender.com").rstrip("/")
MAIN_API_BASE_URL = os.getenv("NAIRAPIPS_MAIN_API_BASE_URL", "https://nairapips-api.onrender.com").rstrip("/")
# Recent fallback window: newly assigned MT5 accounts may appear in purchases/MT5 pool
# before trader_accounts is created. Keep the window short to avoid stressing MT5.
RECENT_ASSIGNMENT_DAYS = int(os.getenv("NAIRAPIPS_RECENT_ASSIGNMENT_DAYS", "7"))
# Render/Supabase discovery can occasionally exceed 12 seconds under load.
# Use a bounded retry window so transient latency cannot remove every trader from monitoring.
DISCOVERY_TIMEOUT_SECONDS = int(os.getenv("NAIRAPIPS_DISCOVERY_TIMEOUT_SECONDS", "35"))
DISCOVERY_RETRIES = max(1, int(os.getenv("NAIRAPIPS_DISCOVERY_RETRIES", "2")))
FORCE_MT5_LOGINS = {x.strip() for x in os.getenv("NAIRAPIPS_FORCE_MT5_LOGINS", "").split(",") if x.strip()}

SNAPSHOT_ENDPOINT = f"{API_BASE_URL}/monitoring_snapshot"
TRADERS_ENDPOINTS = [
    f"{API_BASE_URL}/monitorable_accounts",
]

UNIFIED_SYSTEM_SYNC_ENDPOINTS = [
    f"{MAIN_API_BASE_URL}/np_unified_mt5_sync",
]

# Safe direct discovery endpoints. The first endpoint remains the source of truth.
# The rest are fallback-only and must pass recent-assignment checks before scanning.
DIRECT_DISCOVERY_ENDPOINTS = [
    (f"{API_BASE_URL}/monitorable_accounts", "monitoring_api"),
]
ENGINE_BACKUP_DISCOVERY_URL = os.getenv("NAIRAPIPS_ENGINE_BACKUP_DISCOVERY_URL", "").strip()
if ENGINE_BACKUP_DISCOVERY_URL:
    DIRECT_DISCOVERY_ENDPOINTS.append((ENGINE_BACKUP_DISCOVERY_URL, "engine_backup"))

# The monitoring API returns one live account per row, so dashboard expansion is not needed.
TRADER_DASHBOARD_ENDPOINT = f"{API_BASE_URL}/trader_current_account"
SYNC_TRADES_ENDPOINT = f"{API_BASE_URL}/sync_trades"
DISABLE_MT5_ENDPOINT = f"{API_BASE_URL}/disable_mt5_access"

ENGINE_MT5_POOL_URL = os.getenv("NAIRAPIPS_ENGINE_MT5_POOL_URL", "").strip()
MT5_POOL_ENDPOINTS = [ENGINE_MT5_POOL_URL] if ENGINE_MT5_POOL_URL else []


MT5_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"

ENGINE_ACTIVE_ACCOUNT_STATUSES = {
    "assigned_active", "active", "current_active", "phase1_active",
    "phase2_active", "funded_active", "live_active", "live", "funded",
    "approved_active",
}
ENGINE_TERMINAL_WORDS = ("archived", "breached", "closed", "locked", "disabled", "passed", "reset")


def env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


# Live-account freshness gate. A successful mt5.login() proves credentials/account identity,
# but not that the local terminal has already refreshed broker-side balance/equity.
# Settle + cross-check the account before ANY pass/breach/snapshot decision.
MT5_FRESHNESS_POLL_ATTEMPTS = max(4, int(os.getenv("NAIRAPIPS_MT5_FRESHNESS_POLL_ATTEMPTS", "8")))
MT5_FRESHNESS_POLL_SECONDS = max(0.25, float(os.getenv("NAIRAPIPS_MT5_FRESHNESS_POLL_SECONDS", "0.50")))
MT5_FRESHNESS_HARD_RECONNECT = env_bool("NAIRAPIPS_MT5_FRESHNESS_HARD_RECONNECT", True)


# Production safety default for NairaPips prop-firm protection:
# Monitor with investor password. If a REAL pass/breach/funded cap is confirmed,
# switch to master password and close trades automatically by default.
# Failed login/server/password must NEVER create false breach.
MT5_ALLOW_TRADE_CLOSE = env_bool("NAIRAPIPS_MT5_ALLOW_TRADE_CLOSE", True)
MT5_CLOSE_ON_BREACH = env_bool("NAIRAPIPS_MT5_CLOSE_ON_BREACH", True)
MT5_CLOSE_ON_PASS = env_bool("NAIRAPIPS_MT5_CLOSE_ON_PASS", True)
MT5_CLOSE_ON_PROFIT_PROTECTION = env_bool("NAIRAPIPS_MT5_CLOSE_ON_PROFIT_PROTECTION", True)
MT5_WATCHDOG_CLOSE = env_bool("NAIRAPIPS_MT5_WATCHDOG_CLOSE", True)

# ARCHITECTURE: the dedicated Breach Guard is the ONLY DD/breach authority.
# This engine keeps targets, passes, payout protection, lifecycle/history and other operations.
DEDICATED_DD_GUARD_OWNS_BREACH = True

# NairaPips prohibited-trading behaviour guard.
# MetaTrader 5 cannot identify the name of software installed on a trader device.
# This guard therefore acts only on HIGH-CONFIDENCE trading behaviour evidence.
RULE_VIOLATION_GUARD_ENABLED = env_bool("NAIRAPIPS_RULE_VIOLATION_GUARD_ENABLED", True)
# SAFETY: detect/report suspicious behaviour, but never auto-close unless explicitly enabled.
RULE_VIOLATION_ENFORCEMENT_ENABLED = env_bool("NAIRAPIPS_RULE_VIOLATION_ENFORCEMENT_ENABLED", False)
MT5_CLOSE_ON_RULE_VIOLATION = env_bool("NAIRAPIPS_MT5_CLOSE_ON_RULE_VIOLATION", False)
RULE_GUARD_LOOKBACK_MINUTES = int(os.getenv("NAIRAPIPS_RULE_GUARD_LOOKBACK_MINUTES", "15"))
RULE_GUARD_MIN_SCORE = int(os.getenv("NAIRAPIPS_RULE_GUARD_MIN_SCORE", "100"))
RULE_GUARD_MIN_SIGNALS = int(os.getenv("NAIRAPIPS_RULE_GUARD_MIN_SIGNALS", "2"))
RULE_GUARD_COOLDOWN_SECONDS = int(os.getenv("NAIRAPIPS_RULE_GUARD_COOLDOWN_SECONDS", "900"))

# Conservative defaults: one fast trade or one large position will never trigger closure.
RULE_BURST_COUNT = int(os.getenv("NAIRAPIPS_RULE_BURST_COUNT", "10"))
RULE_BURST_SECONDS = int(os.getenv("NAIRAPIPS_RULE_BURST_SECONDS", "30"))
RULE_EXTREME_BURST_COUNT = int(os.getenv("NAIRAPIPS_RULE_EXTREME_BURST_COUNT", "15"))
RULE_EXTREME_BURST_SECONDS = int(os.getenv("NAIRAPIPS_RULE_EXTREME_BURST_SECONDS", "20"))
RULE_RAPID_SCALP_COUNT = int(os.getenv("NAIRAPIPS_RULE_RAPID_SCALP_COUNT", "8"))
RULE_RAPID_SCALP_MAX_SECONDS = int(os.getenv("NAIRAPIPS_RULE_RAPID_SCALP_MAX_SECONDS", "10"))
RULE_MARTINGALE_STEPS = int(os.getenv("NAIRAPIPS_RULE_MARTINGALE_STEPS", "4"))
RULE_MARTINGALE_MULTIPLIER = float(os.getenv("NAIRAPIPS_RULE_MARTINGALE_MULTIPLIER", "1.8"))
RULE_GRID_POSITION_COUNT = int(os.getenv("NAIRAPIPS_RULE_GRID_POSITION_COUNT", "6"))

LAST_RULE_GUARD_ACTION = {}

# NAIRAPIPS FUNDED PROFIT CYCLE PROTECTION
# A funded/live account completes one profit cycle at 50% gross profit.
# The engine closes open trades and locks the SAME MT5 account pending payout.
# After payout, Admin manually returns that same Exness MT5 account to its starting capital.
# No new MT5 login/account is created by this engine.
FUNDED_PROFIT_HARD_CAP_PERCENT = 15.0  # BUSINESS AUTHORITY: absolute funded gross-profit ceiling
PHASE_DAILY_PROFIT_LIMIT_PERCENT = 5.0  # Phase/Challenge: maximum net daily growth from original account size
FUNDED_TRADER_SHARE_PERCENT = float(os.getenv("NAIRAPIPS_FUNDED_TRADER_SHARE_PERCENT", "60"))
FUNDED_NAIRAPIPS_SHARE_PERCENT = float(os.getenv("NAIRAPIPS_FUNDED_NAIRAPIPS_SHARE_PERCENT", "40"))
MT5_CLOSE_ON_FUNDED_HARD_CAP = env_bool("NAIRAPIPS_MT5_CLOSE_ON_FUNDED_HARD_CAP", True)

# Persistent payout-liability shield.
# Cooldowns may suppress repeated emails/API writes, but never broker-side closure.
PAYOUT_LOCK_WATCHDOG_ENABLED = env_bool("NAIRAPIPS_PAYOUT_LOCK_WATCHDOG_ENABLED", True)
PAYOUT_LOCK_CLOSE_PASSES = max(1, int(os.getenv("NAIRAPIPS_PAYOUT_LOCK_CLOSE_PASSES", "3")))
PAYOUT_LOCK_CLOSE_RETRY_SECONDS = max(0.2, float(os.getenv("NAIRAPIPS_PAYOUT_LOCK_CLOSE_RETRY_SECONDS", "1.0")))
PERSISTENT_LOCKED_LOGINS = set()
# HARD PAYOUT WATCHDOG: keep exact credentials for accounts that become hard-locked
# during this engine process. This lets the engine revisit them between normal
# portfolio scans even after the backend removes them from monitorable discovery.
LOCKED_ACCOUNT_CACHE = {}
WATCHDOG_NORMAL_ACCOUNTS_BETWEEN_SWEEPS = max(1, int(os.getenv("NAIRAPIPS_WATCHDOG_NORMAL_ACCOUNTS_BETWEEN_SWEEPS", "3")))
WATCHDOG_SWEEP_SLEEP_SECONDS = max(0.2, float(os.getenv("NAIRAPIPS_WATCHDOG_SWEEP_SLEEP_SECONDS", "0.5")))

# Strict automated-order guard.
# MT5 cannot reveal software installed on a device. It can only inspect order
# fingerprints already received by the broker: magic numbers and comments.
AUTOMATED_ORDER_GUARD_ENABLED = env_bool("NAIRAPIPS_AUTOMATED_ORDER_GUARD_ENABLED", True)
# SAFETY: automated-order fingerprints are evidence only by default.
AUTOMATED_ORDER_ENFORCEMENT_ENABLED = env_bool("NAIRAPIPS_AUTOMATED_ORDER_ENFORCEMENT_ENABLED", False)
AUTOMATED_ORDER_LOCK_ACCOUNT = env_bool("NAIRAPIPS_AUTOMATED_ORDER_LOCK_ACCOUNT", False)
AUTOMATED_ORDER_COMMENT_KEYWORDS = tuple(
    x.strip().lower()
    for x in os.getenv(
        "NAIRAPIPS_AUTOMATED_ORDER_COMMENT_KEYWORDS",
        "expert advisor,expert,ea,robot,bot,copy trade,copier,signal copier,autotrade,algo"
    ).split(",")
    if x.strip()
)

# Trade history is critical for disputes. The engine syncs open trades plus closed history.
HISTORY_LOOKBACK_DAYS = 180
# Production API protection: do not resend full 180-day history every 2 seconds.
# Open trades are synced every scan; closed history is synced on first scan and then periodically.
HISTORY_SYNC_INTERVAL_SECONDS = int(os.getenv("NAIRAPIPS_HISTORY_SYNC_INTERVAL_SECONDS", "600"))
MAX_HISTORY_TRADES_PER_SYNC = int(os.getenv("NAIRAPIPS_MAX_HISTORY_TRADES_PER_SYNC", "250"))
LAST_HISTORY_SYNC_BY_LOGIN = {}

# Official NairaPips static DD limit. Applies to EVERY account size dynamically.
# Example: 500k -> breach 400k, 1m -> breach 800k, 2m -> breach 1.6m.
MAX_DRAWDOWN_LIMIT_PERCENT = 20.0

# When a breach/pass/funded-cap is confirmed, rotate the trader's MT5 master password so
# they cannot place new trades after the engine closes existing positions.
MT5_ALLOW_PASSWORD_ROTATION = os.getenv("NAIRAPIPS_ALLOW_PASSWORD_ROTATION", "1") == "1"
# Force rotation for already-locked accounts on next run (one-shot).
# Use this to retroactively lock brokers for traders who breached BEFORE rotation was deployed.
FORCE_ROTATE_LOCKED_LOGINS = {x.strip() for x in os.getenv("NAIRAPIPS_FORCE_ROTATE_LOCKED", "").split(",") if x.strip()}
PASSWORD_ROTATION_DONE = set()  # in-memory dedupe per process
PASSWORD_ROTATION_COOLDOWN_SECONDS = int(os.getenv("NAIRAPIPS_PASSWORD_ROTATION_COOLDOWN_SECONDS", "86400"))  # 24h
LAST_PASSWORD_ROTATION = {}  # (login, status) -> timestamp

# FUNDED HYBRID PROFIT PROTECTION
FUNDED_PROFIT_ZONE_PERCENT = 10.0
FUNDED_PROFIT_PROTECT_LEVEL_1 = 25.0
FUNDED_PROFIT_PROTECT_LEVEL_2 = 50.0
FUNDED_PROTECT_RATIO_1 = 0.30
FUNDED_PROTECT_RATIO_2 = 0.50

SCAN_SECONDS = {
    "safe": 20,
    "warning": 10,
    "danger": 5,
    "critical": 3,
    "breached": 2,
    "target_hit": 2,
    "profit_protected": 2,
    "locked": 2,
    "rule_violation_review": 2,
    "phase_daily_profit_locked": 2,
    "funded": 20,
    "funded_profit_zone": 10,
    "offline": 20,
    "retry_pending": 2,
    "inactive": 20,
}

# ========================= NAIRAPIPS PROP-FIRM PRODUCTION GUARDS =========================
# These guards protect NairaPips from payout disasters caused by engine/API/MT5 glitches.
# They do NOT weaken real breach/pass detection. They only stop false states caused by
# failed logins, wrong credentials, stale rows, duplicate lock calls, or zero-balance terminal errors.
MAX_ACCOUNTS_PER_CYCLE = int(os.getenv("NAIRAPIPS_MAX_ACCOUNTS_PER_CYCLE", "80"))
FAILED_LOGIN_QUARANTINE_SECONDS = int(os.getenv("NAIRAPIPS_FAILED_LOGIN_QUARANTINE_SECONDS", "300"))
MAX_FAILED_LOGIN_BEFORE_QUARANTINE = int(os.getenv("NAIRAPIPS_MAX_FAILED_LOGIN_BEFORE_QUARANTINE", "2"))
LOCK_ACTION_COOLDOWN_SECONDS = int(os.getenv("NAIRAPIPS_LOCK_ACTION_COOLDOWN_SECONDS", "900"))
EVENT_SNAPSHOT_COOLDOWN_SECONDS = int(os.getenv("NAIRAPIPS_EVENT_SNAPSHOT_COOLDOWN_SECONDS", "120"))
NORMAL_SNAPSHOT_MIN_SECONDS = int(os.getenv("NAIRAPIPS_NORMAL_SNAPSHOT_MIN_SECONDS", "20"))

FAILED_LOGIN_STATE = {}       # login -> {count, until, reason}
LAST_LOCK_ACTION = {}         # (login, status) -> timestamp
LAST_SNAPSHOT_SENT = {}       # (login, status/zone) -> timestamp
DISCOVERY_FAILURE_STATE = {}
# FAIR-SCAN 2026-09-02: prevents low/stale-risk accounts from being permanently
# excluded when monitorable accounts exceed MAX_ACCOUNTS_PER_CYCLE.
FAIR_SCAN_CURSOR = 0

# LIVE BOUNDARY PRIORITY GUARD 2026-09-20
# Accounts nearest ANY financial boundary jump to the front of the next scan.
# This state is local/in-memory only; it never changes lifecycle or business rules.
LIVE_BOUNDARY_PRIORITY = {}  # login -> {score, label, updated_at}
HOT_RESCAN_MAX = max(1, int(os.getenv("NAIRAPIPS_HOT_RESCAN_MAX", "8")))
HOT_CRITICAL_SLEEP_SECONDS = max(1, int(os.getenv("NAIRAPIPS_HOT_CRITICAL_SLEEP_SECONDS", "1")))

DISCOVERY_WARNING_BASE_SECONDS = int(os.getenv("NAIRAPIPS_DISCOVERY_WARNING_BASE_SECONDS", "300"))
DISCOVERY_WARNING_MAX_SECONDS = int(os.getenv("NAIRAPIPS_DISCOVERY_WARNING_MAX_SECONDS", "3600"))



def now_iso():
    return datetime.now(timezone.utc).isoformat()


def log(message, level="INFO"):
    print(f"{datetime.utcnow()} | {level} | {message}", flush=True)


def _now_ts():
    return time.time()


def login_quarantined(login):
    state = FAILED_LOGIN_STATE.get(str(login)) or {}
    until = float(state.get("until") or 0)
    if until > _now_ts():
        remaining = int(until - _now_ts())
        log(f"Login {login} temporarily quarantined for {remaining}s after repeated credential/server failures. SKIPPED - no breach/snapshot", "WARNING")
        return True
    return False


def register_login_failure(login, reason):
    key = str(login)
    state = FAILED_LOGIN_STATE.setdefault(key, {"count": 0, "until": 0, "reason": ""})
    state["count"] = int(state.get("count") or 0) + 1
    state["reason"] = str(reason or "login failure")[:180]
    if state["count"] >= MAX_FAILED_LOGIN_BEFORE_QUARANTINE:
        state["until"] = _now_ts() + FAILED_LOGIN_QUARANTINE_SECONDS
        log(f"Login {login} quarantined for {FAILED_LOGIN_QUARANTINE_SECONDS}s. Reason={state['reason']}. No breach/snapshot will be sent while credentials are bad.", "WARNING")


def register_login_success(login):
    FAILED_LOGIN_STATE.pop(str(login), None)


def should_send_snapshot(login, status, zone):
    key = (str(login), str(status or zone or "active"))
    now = _now_ts()
    is_event = str(status or "").lower() in {"breached", "phase1_passed", "phase2_passed", "target_hit", "profit_protected", "funded_profit_cap_reached", "rule_violation_review"} or str(zone or "").lower() in {"breached", "passed", "profit_protected"}
    cooldown = EVENT_SNAPSHOT_COOLDOWN_SECONDS if is_event else NORMAL_SNAPSHOT_MIN_SECONDS
    last = float(LAST_SNAPSHOT_SENT.get(key) or 0)
    if now - last < cooldown:
        return False
    LAST_SNAPSHOT_SENT[key] = now
    return True


def should_send_lock_action(login, status):
    key = (str(login), str(status or "lock"))
    now = _now_ts()
    last = float(LAST_LOCK_ACTION.get(key) or 0)
    if now - last < LOCK_ACTION_COOLDOWN_SECONDS:
        log(f"Lock action suppressed for {login}/{status}: cooldown active to avoid duplicate emails/API writes.", "WARNING")
        return False
    LAST_LOCK_ACTION[key] = now
    return True


def safe_disable_mt5_access(trader, login, reason, status, evidence=None):
    if should_send_lock_action(login, status):
        return disable_mt5_access(trader, login, reason, status=status, evidence=evidence)
    return False


def api_get(url, timeout=10):
    return requests.get(url, timeout=timeout)


def discovery_get(url):
    """Bounded retry for authoritative account discovery only."""
    last_error = None
    for attempt in range(DISCOVERY_RETRIES):
        try:
            response = requests.get(url, timeout=DISCOVERY_TIMEOUT_SECONDS)
            if response.status_code == 200:
                return response
            # Retry transient upstream/rate-limit/server conditions only.
            if response.status_code in (408, 425, 429, 500, 502, 503, 504):
                last_error = RuntimeError(f"HTTP {response.status_code}")
                if attempt + 1 < DISCOVERY_RETRIES:
                    log(
                        f"Discovery transient HTTP {response.status_code}; retry "
                        f"{attempt + 2}/{DISCOVERY_RETRIES} in 2s",
                        "WARNING",
                    )
                    time.sleep(2)
                    continue
            return response
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt + 1 < DISCOVERY_RETRIES:
                log(
                    f"Discovery network delay; retry {attempt + 2}/{DISCOVERY_RETRIES} in 2s: "
                    f"{str(e)[:160]}",
                    "WARNING",
                )
                time.sleep(2)
                continue
            raise
    if last_error:
        raise last_error
    raise RuntimeError("Discovery request failed without response")


def api_post(url, payload, timeout=15):
    # One retry protects temporary Render/Supabase delays without duplicating heavy work.
    last_error = None
    for attempt in range(2):
        try:
            return requests.post(url, json=payload, timeout=timeout)
        except requests.exceptions.ReadTimeout as e:
            last_error = e
            log(f"API timeout attempt {attempt + 1}/2: {url}", "WARNING")
            time.sleep(2)
        except Exception as e:
            last_error = e
            break
    raise last_error


def initialize_mt5():
    """
    Start and authenticate the dedicated Operations/Target MT5 terminal.

    V2 bootstrap deliberately DOES NOT call get_traders() here.
    get_traders() performs unified-sync and account enrichment work, which can
    block on backend HTTP before MT5 has even started. Startup must be independent.

    Bootstrap path:
      monitoring API /monitorable_accounts -> exact row credentials -> optional
      pool credentials -> mt5.initialize(path, login, password, server) ->
      verify exact account_info().login.
    """
    try:
        mt5.shutdown()
        time.sleep(1)
    except Exception:
        pass

    bootstrap_rows = []

    # Direct authoritative feed only. No unified sync, no dashboard enrichment.
    try:
        response = api_get(TRADERS_ENDPOINTS[0], timeout=DISCOVERY_TIMEOUT_SECONDS)
        if response.status_code == 200:
            bootstrap_rows = unpack_rows(response.json())
            log(f"OPERATIONS BOOTSTRAP FEED rows={len(bootstrap_rows)}")
        else:
            log(
                f"OPERATIONS BOOTSTRAP FEED HTTP {response.status_code}",
                "WARNING",
            )
    except Exception as exc:
        log(f"OPERATIONS BOOTSTRAP FEED FAILED: {exc}", "WARNING")

    attempts = 0

    for trader in bootstrap_rows:
        if not isinstance(trader, dict):
            continue

        login_text = normalise_login(
            trader.get("mt5_login")
            or trader.get("login")
            or trader.get("account_login")
        )
        server = str(trader.get("mt5_server") or trader.get("server") or "").strip()

        if not login_text.isdigit() or not server:
            continue

        candidates = []

        # Prefer credentials already carried by the authoritative monitoring row.
        for source, value in [
            ("row-investor", trader.get("mt5_investor_password")),
            ("row-investor-alt", trader.get("investor_password")),
            ("row-master", trader.get("mt5_master_password")),
            ("row-master-alt", trader.get("master_password")),
            ("row-password", trader.get("mt5_password")),
        ]:
            value = str(value or "").strip()
            if value and value not in [pwd for _, pwd in candidates]:
                candidates.append((source, value))

        # Pool fallback only when row credentials are absent/insufficient.
        pool_acc = {}
        try:
            pool_acc = find_pool_account_by_login(login_text) or {}
        except Exception:
            pool_acc = {}

        for source, value in [
            ("pool-investor", pool_acc.get("mt5_investor_password")),
            ("pool-investor-alt", pool_acc.get("investor_password")),
            ("pool-master", pool_acc.get("mt5_master_password")),
            ("pool-master-alt", pool_acc.get("master_password")),
            ("pool-password", pool_acc.get("mt5_password") or pool_acc.get("password")),
        ]:
            value = str(value or "").strip()
            if value and value not in [pwd for _, pwd in candidates]:
                candidates.append((source, value))

        if not candidates:
            continue

        for source, password in candidates:
            attempts += 1

            try:
                mt5.shutdown()
            except Exception:
                pass
            time.sleep(0.35)

            ok = mt5.initialize(
                path=MT5_PATH,
                login=int(login_text),
                password=password,
                server=server,
                timeout=60000,
            )

            if not ok:
                log(
                    f"OPERATIONS BOOTSTRAP REJECTED MT5={login_text} "
                    f"source={source} error={mt5.last_error()}",
                    "WARNING",
                )
                continue

            time.sleep(0.75)
            info = mt5.account_info()
            current = normalise_login(getattr(info, "login", "")) if info else ""

            if current != login_text:
                log(
                    f"OPERATIONS BOOTSTRAP IDENTITY MISMATCH "
                    f"target={login_text} terminal={current or 'NONE'}",
                    "WARNING",
                )
                try:
                    mt5.shutdown()
                except Exception:
                    pass
                continue

            log(
                f"OPERATIONS TERMINAL BOOTSTRAPPED | MT5={login_text} "
                f"source={source} server={server} attempts={attempts}",
                "WARNING",
            )
            log(f"MT5 initialized: {mt5.version()} | terminal={MT5_PATH}")
            return True

    try:
        mt5.shutdown()
    except Exception:
        pass

    log(
        f"OPERATIONS MT5 STARTUP BLOCKED: direct authoritative bootstrap failed. "
        f"terminal={MT5_PATH} attempts={attempts}",
        "CRITICAL",
    )
    return False


def shutdown_mt5():
    mt5.shutdown()


def to_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", "").replace("₦", "").strip())
    except Exception:
        return default


def normalise_login(value):
    return str(value or "").strip()


def unpack_rows(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ["data", "sample", "rows", "accounts", "mt5_pool", "result"]:
            if isinstance(data.get(key), list):
                return data.get(key)
    return []


def dashboard_pack_for_trader(trader):
    """Load active MT5 account rows for one trader.
    The public trader row only has one mt5_login, but production can have
    multiple active challenge accounts. This endpoint carries account-level data.
    """
    lookup = (
        trader.get("email")
        or trader.get("id")
        or trader.get("mt5_login")
        or trader.get("phone")
        or ""
    )
    lookup = str(lookup).strip()
    if not lookup:
        return {}
    try:
        response = api_get(f"{TRADER_DASHBOARD_ENDPOINT}/{lookup}", timeout=10)
        if response.status_code != 200:
            log(f"Active account fetch failed for {lookup}: HTTP {response.status_code} | {response.text[:180]}", "WARNING")
            return {}
        data = response.json()
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            return data.get("data") or {}
        return data if isinstance(data, dict) else {}
    except Exception as e:
        log(f"Dashboard account fetch skipped for {lookup}: {e}", "WARNING")
        return {}


def expand_trader_active_accounts(trader):
    """Return one monitor row per active MT5 account.

    Production source-of-truth rule:
    - trader_accounts controls live MT5 monitoring.
    - the legacy traders row is identity/profile only and may still contain old
      breached/locked/pass flags from a previous phase.
    - never let stale traders.status / mt5_access_disabled push a fresh active
      account into WATCHDOG mode.
    """
    # HARD V3 RULE: canonical/direct telemetry rows are already account-level.
    if str(trader.get("_source_of_truth") or "").strip().lower() == "monitoring_api" or str(trader.get("_discovery_source") or "").strip().lower() in {"monitoring_api","local_roster_snapshot"}:
        return [trader]

    # Rows from the dedicated monitoring API are already one account per row.
    # Do not call the old heavy /trader_current_account endpoint again.
    source = str(trader.get("_source_of_truth") or "").strip().lower()
    discovery_source = str(trader.get("_discovery_source") or "").strip().lower()
    if (
        source in {"monitoring_api", "monitoring_api_fast_discovery"}
        or discovery_source in {"monitoring_api", "monitoring_sync", "monitoring_force_sync", "main_monitorable"}
    ):
        return [trader]

    pack = dashboard_pack_for_trader(trader)
    accounts = []
    if isinstance(pack, dict):
        accounts = pack.get("active_accounts") or pack.get("accounts") or []
    if not isinstance(accounts, list):
        accounts = []

    expanded = []
    seen_logins = set()
    active_statuses = {"assigned_active", "active", "current_active"}
    for account in accounts:
        if not isinstance(account, dict):
            continue
        login = normalise_login(account.get("mt5_login"))
        server = str(account.get("mt5_server") or "").strip()
        account_status = str(account.get("account_status") or "assigned_active").lower().strip()
        if account_status and account_status not in active_statuses:
            continue
        if not _valid_mt5_login(login) or not server or login in seen_logins:
            continue
        seen_logins.add(login)
        row = dict(trader)
        row.update({
            "mt5_login": login,
            "mt5_server": server,
            "mt5_master_password": account.get("mt5_master_password") or account.get("mt5_password") or account.get("master_password") or trader.get("mt5_master_password") or trader.get("mt5_password") or trader.get("master_password") or "",
            "mt5_password": account.get("mt5_password") or account.get("mt5_master_password") or account.get("master_password") or trader.get("mt5_password") or trader.get("mt5_master_password") or trader.get("master_password") or "",
            "master_password": account.get("master_password") or account.get("mt5_master_password") or account.get("mt5_password") or trader.get("master_password") or trader.get("mt5_master_password") or trader.get("mt5_password") or "",
            "mt5_investor_password": account.get("mt5_investor_password") or account.get("investor_password") or trader.get("mt5_investor_password") or trader.get("investor_password") or "",
            "investor_password": account.get("investor_password") or account.get("mt5_investor_password") or trader.get("investor_password") or trader.get("mt5_investor_password") or "",
            "account_size": account.get("account_size") or account.get("start_balance") or trader.get("account_size"),
            "balance": account.get("start_balance") or account.get("account_size") or trader.get("balance"),
            "equity": account.get("current_equity") or account.get("current_balance") or trader.get("equity"),
            "phase": account.get("stage") or trader.get("phase"),
            "status": "active",
            "account_status": account_status or "assigned_active",
            "payment_status": "approved",
            "monitoring_enabled": True,
            "mt5_access_disabled": False,
            "trader_account_id": account.get("id"),
            "current_account_id": account.get("id"),
            "absolute_drawdown_percent": account.get("absolute_drawdown_percent") or account.get("drawdown_percent") or 0,
            "max_drawdown_used": account.get("dd_used_percent") or account.get("max_drawdown_used") or 0,
            "highest_equity": account.get("highest_equity") or account.get("current_equity") or account.get("start_balance") or account.get("account_size"),
            "lowest_equity": account.get("lowest_equity") or account.get("current_equity") or account.get("start_balance") or account.get("account_size"),
            "_source_of_truth": "trader_accounts",
        })
        expanded.append(row)

    if expanded:
        return expanded

    # Safety: if the account endpoint responds but returns no active account, do
    # not monitor stale MT5 details from the identity row. This prevents old
    # breached/locked flags on traders from creating false WATCHDOG mode.
    if isinstance(pack, dict) and pack.get("source_of_truth") == "trader_accounts":
        log(f"No assigned_active trader_accounts row for {trader.get('email') or trader.get('id')}; stale trader row skipped.", "WARNING")
        return []

    # Backward fallback only for genuinely active legacy rows, never old locked rows.
    if is_locked_or_breached(trader):
        log(f"Legacy locked/breached trader row skipped without active trader_accounts source: {trader.get('email') or trader.get('id')}", "WARNING")
        return []
    return [trader]


def phase_text(trader):
    return str(trader.get("phase") or trader.get("status") or "").lower().replace("-", "_").strip()


def is_funded_or_live(trader):
    phase = phase_text(trader)
    status = str(trader.get("status") or "").lower().strip()
    return phase in ["funded", "live", "funded_live"] or status in ["funded", "live"]


def funded_cycle_release_authorized(trader):
    """Exact standalone funded-cycle release signal from trader_accounts.

    The Admin completion endpoint deliberately returns the SAME funded account to
    assigned_active and monitoring_enabled. This exact combination is allowed to
    clear only the old funded-cap/payout watchdog; it does not release breached,
    archived, disabled, reset, or rule-violation accounts.
    """
    account_status = str(trader.get("account_status") or "").lower().strip()
    stage = str(trader.get("stage") or trader.get("phase") or "").lower().strip()
    monitoring_enabled = trader.get("monitoring_enabled")
    source_ok = trader.get("_source_of_truth") == "trader_accounts"
    forbidden = {"breached", "archived", "disabled", "reset", "rule_violation_review", "profit_protected"}
    status_text = " ".join(str(trader.get(k) or "").lower() for k in ("account_status", "status", "risk_zone", "zone"))
    terminal_breach_recorded = any(bool(trader.get(k)) for k in ("breached_at", "breach_detected_at", "breach_time"))
    return (
        source_ok
        and account_status in {"assigned_active", "active", "current_active", "funded_active"}
        and stage in {"funded", "live", "funded_live"}
        and monitoring_enabled is not False
        and not terminal_breach_recorded
        and not any(word in status_text for word in forbidden)
    )


def is_locked_or_breached(trader):
    """True for every state that must prohibit new trading."""
    account_status = str(trader.get("account_status") or "").lower().strip()
    status = str(trader.get("status") or "").lower().strip()
    phase = str(trader.get("phase") or "").lower().strip()
    zone = str(trader.get("risk_zone") or trader.get("zone") or "").lower().strip()

    lock_words = {
        "breached", "locked", "disabled", "profit_protected",
        "funded_profit_cap_reached", "funded_profit_cap",
        "payout_required", "payout_pending", "approved_payout_pending",
        "payment_processing", "rule_violation_review", "rule_violation",
    }
    terminal_breach_recorded = any(bool(trader.get(k)) for k in ("breached_at", "breach_detected_at", "breach_time"))
    explicit_lock = (
        terminal_breach_recorded
        or bool(trader.get("mt5_access_disabled"))
        or bool(trader.get("trading_must_remain_locked_until_payout"))
        or bool(trader.get("payout_required"))
        or status in lock_words
        or phase in lock_words
        or zone in {"breached", "profit_protected", "funded_profit_cap"}
        or any(word in account_status for word in (
            "breached", "locked", "disabled", "profit_protected",
            "funded_profit_cap", "payout_required", "rule_violation"
        ))
    )
    if explicit_lock:
        return True

    if trader.get("_source_of_truth") == "trader_accounts" and account_status in {
        "assigned_active", "active", "current_active", "funded_active"
    }:
        return False
    return False


def is_breached_close_state(trader):
    """True for all states requiring persistent zero-exposure enforcement."""
    status = str(trader.get("status") or "").lower()
    phase = str(trader.get("phase") or "").lower()
    zone = str(trader.get("risk_zone") or trader.get("zone") or "").lower()
    account_status = str(trader.get("account_status") or "").lower()
    # Persistent broker-side closing is reserved for financial/lifecycle hard locks.
    # Rule-review flags are evidence/review states and must not repeatedly close trades.
    hard_close_words = {
        "breached", "locked", "profit_protected",
        "funded_profit_cap_reached", "funded_profit_cap",
        "payout_required", "payout_pending", "approved_payout_pending",
        "payment_processing",
    }
    terminal_breach_recorded = any(bool(trader.get(k)) for k in ("breached_at", "breach_detected_at", "breach_time"))
    hard_state = (
        terminal_breach_recorded
        or bool(trader.get("trading_must_remain_locked_until_payout"))
        or bool(trader.get("payout_required"))
        or status in hard_close_words
        or phase in hard_close_words
        or zone in {"breached", "profit_protected", "funded_profit_cap"}
        or any(word in account_status for word in (
            "breached", "locked", "profit_protected",
            "funded_profit_cap", "payout_required"
        ))
    )
    # A stale generic mt5_access_disabled flag alone is NOT enough to close positions.
    # It only reinforces a proven hard financial/lifecycle lock.
    return bool(hard_state)


def close_allowed(reason_type):
    if not MT5_ALLOW_TRADE_CLOSE:
        return False
    if reason_type == "breach":
        return MT5_CLOSE_ON_BREACH
    if reason_type == "pass":
        return MT5_CLOSE_ON_PASS
    if reason_type == "profit_protection":
        return MT5_CLOSE_ON_PROFIT_PROTECTION
    if reason_type == "funded_cap":
        return MT5_CLOSE_ON_FUNDED_HARD_CAP
    if reason_type == "watchdog":
        return MT5_WATCHDOG_CLOSE
    if reason_type == "rule_violation":
        return MT5_CLOSE_ON_RULE_VIOLATION
    return False


def _valid_mt5_login(value):
    value = normalise_login(value)
    if not value:
        return False
    bad_words = ["NEW", "LOGIN", "HERE", "NULL", "NONE", "TEST_LOGIN"]
    upper = value.upper()
    if any(word in upper for word in bad_words):
        return False
    return value.isdigit()


def _trader_is_monitorable(t):
    """Accept only exact canonical active-account rows from the monitoring API.

    Trader-level payment/status mirrors must never make a row monitorable. Terminal
    accounts are intentionally rejected; trade closure must occur before lifecycle
    archival, not by resurrecting archived/breached/passed rows into discovery.
    """
    login = normalise_login(t.get("mt5_login"))
    server = str(t.get("mt5_server") or "").strip()
    account_id = str(t.get("trader_account_id") or t.get("current_account_id") or "").strip()
    account_status = str(t.get("account_status") or "").lower().strip()
    blob = " ".join(str(t.get(k) or "").lower() for k in [
        "account_status", "status", "phase", "stage", "risk_zone", "zone"
    ])
    if not account_id or not _valid_mt5_login(login) or not server:
        return False
    if account_status not in ENGINE_ACTIVE_ACCOUNT_STATUSES:
        return False
    if any(word in blob for word in ENGINE_TERMINAL_WORDS):
        return False
    if t.get("mt5_access_disabled") is True or str(t.get("mt5_access_disabled") or "").lower() in ["true", "1", "yes"]:
        return False
    if t.get("monitoring_enabled") is False or str(t.get("monitoring_enabled") or "").lower() in ["false", "0", "no", "off"]:
        return False
    return True


def _parse_any_datetime(value):
    if not value:
        return None
    try:
        raw = str(value).strip()
        if not raw:
            return None
        raw = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _row_recent_assignment(row):
    """Only allow fallback discovery for accounts assigned/updated recently.
    This prevents the engine from dragging old dead MT5 accounts back into the scan.
    """
    login = normalise_login((row or {}).get("mt5_login") or (row or {}).get("login") or (row or {}).get("account_login"))
    if login in FORCE_MT5_LOGINS:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_ASSIGNMENT_DAYS)
    for key in [
        "assigned_at", "mt5_assigned_at", "mt5_updated_at", "assignment_date",
        "approved_at", "paid_at", "updated_at", "created_at"
    ]:
        dt = _parse_any_datetime((row or {}).get(key))
        if dt and dt >= cutoff:
            return True
    return False


def _row_blob(row):
    return " ".join(str((row or {}).get(k) or "") for k in [
        "status", "account_status", "phase", "stage", "payment_status",
        "phase_pass_status", "risk_zone", "zone", "admin_note"
    ]).lower().replace("-", "_")


def _row_terminal_or_dead(row):
    blob = _row_blob(row)
    return any(x in blob for x in [
        "breached", "archived", "disabled", "locked", "profit_protected",
        "rejected", "failed", "cancelled", "canceled", "passed", "passed_review", "reset", "closed"
    ])


def _row_has_approved_assignment_signal(row):
    blob = _row_blob(row)
    if any(x in blob for x in ["approved", "assigned", "active", "phase1", "phase2", "funded", "live", "paid"]):
        return True
    for k in ["assigned_trader_id", "trader_id", "assigned_email", "assigned_to", "purchase_id"]:
        if (row or {}).get(k):
            return True
    return False


def _first_value(row, keys, default=""):
    for k in keys:
        v = (row or {}).get(k)
        if v is not None and str(v).strip() != "":
            return v
    return default


def _safe_direct_row(row, source):
    """Convert a recent assignment from any business source into one monitor row.
    It must be valid + recent + not terminal. This is the safe anti-stress fallback.
    """
    if not isinstance(row, dict):
        return None
    login = normalise_login(_first_value(row, ["mt5_login", "login", "account_login", "account_number", "number"]))
    server = str(_first_value(row, ["mt5_server", "server", "account_server"], "")).strip()
    account_id = str(_first_value(row, ["trader_account_id", "current_account_id", "account_id"], "")).strip()
    account_status = str(_first_value(row, ["account_status"], "")).lower().strip()
    if not account_id or account_status not in ENGINE_ACTIVE_ACCOUNT_STATUSES:
        return None
    if not _valid_mt5_login(login) or not server:
        return None
    if _row_terminal_or_dead(row):
        return None
    if not _row_has_approved_assignment_signal(row):
        return None
    # Only fallback-discover recent rows. Proper trader_accounts rows are handled normally.
    if source not in {"monitoring_api", "main_monitorable"} and not _row_recent_assignment(row):
        return None

    master = _first_value(row, ["mt5_master_password", "master_password", "mt5_password", "password"], "")
    investor = _first_value(row, ["mt5_investor_password", "investor_password", "investor"], "")
    account_size = _first_value(row, ["account_size", "start_balance", "balance", "challenge_balance", "initial_balance"], 0)
    stage = str(_first_value(row, ["stage", "phase", "assigned_phase"], "phase1") or "phase1").lower().replace(" ", "")
    if stage in ["phase_1", "phase-1"]:
        stage = "phase1"
    if stage in ["phase_2", "phase-2"]:
        stage = "phase2"

    return {
        "id": _first_value(row, ["trader_id", "assigned_trader_id", "id"], ""),
        "trader_id": _first_value(row, ["trader_id", "assigned_trader_id", "id"], ""),
        "trader_account_id": _first_value(row, ["trader_account_id", "current_account_id", "account_id"], ""),
        "current_account_id": _first_value(row, ["current_account_id", "trader_account_id", "account_id"], ""),
        "name": _first_value(row, ["name", "trader_name", "full_name", "assigned_trader_name"], "Trader"),
        "full_name": _first_value(row, ["full_name", "name", "trader_name", "assigned_trader_name"], "Trader"),
        "email": _first_value(row, ["email", "trader_email", "assigned_email"], ""),
        "phone": _first_value(row, ["phone", "trader_phone", "assigned_phone"], ""),
        "phase": stage or "phase1",
        "stage": stage or "phase1",
        "status": "active",
        "account_status": "assigned_active",
        "payment_status": "approved",
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
        "balance": account_size,
        "equity": _first_value(row, ["equity", "current_equity", "current_balance", "balance", "account_size"], account_size),
        "highest_equity": _first_value(row, ["highest_equity", "equity", "current_equity", "account_size"], account_size),
        "lowest_equity": _first_value(row, ["lowest_equity", "equity", "current_equity", "account_size"], account_size),
        "profit_percent": _first_value(row, ["profit_percent", "highest_profit_percent", "current_profit_percent"], 0),
        "drawdown_percent": _first_value(row, ["drawdown_percent", "absolute_drawdown_percent", "current_drawdown_percent"], 0),
        "dd_used_percent": _first_value(row, ["dd_used_percent", "max_drawdown_used", "max_dd_used"], 0),
        "risk_zone": _first_value(row, ["risk_zone", "zone"], "safe"),
        "_source_of_truth": "recent_assignment_fallback" if source not in {"monitoring_api", "main_monitorable"} else "monitoring_api",
        "_discovery_source": source,
    }



def run_unified_assignment_sync():
    """Ask backend to repair/create trader_accounts before every scan.
    This is light enough for production and prevents newly assigned MT5 accounts
    from being invisible to the VPS engine.
    """
    for url in UNIFIED_SYSTEM_SYNC_ENDPOINTS:
        try:
            response = api_post(url, {"source": "mt5_engine", "force_logins": sorted(FORCE_MT5_LOGINS)}, timeout=45)
            if response.status_code in [200, 201]:
                try:
                    data = response.json()
                    msg = data.get('message') if isinstance(data, dict) else ''
                except Exception:
                    msg = response.text[:120]
                log(f"Unified MT5 controlled sync OK: {url} | {msg}")
                return True
            if response.status_code == 409:
                # Backend conflict/guard is not a reason to stop monitoring existing
                # authoritative trader_accounts. Discovery continues normally.
                try:
                    detail = response.json()
                    detail = detail.get("message") or detail.get("error") or str(detail)
                except Exception:
                    detail = response.text[:180]
                log(f"Unified MT5 sync guarded: {url} HTTP 409 | {detail}", "WARNING")
                continue
            log(f"Unified MT5 sync skipped: {url} HTTP {response.status_code}", "WARNING")
        except Exception as e:
            log(f"Unified MT5 sync error: {url} | {e}", "WARNING")
    return False


def _discovery_log_failure(source, error_text):
    key = str(source or "unknown")
    now = _now_ts()
    state = DISCOVERY_FAILURE_STATE.setdefault(key, {"count": 0, "next_log_at": 0})
    state["count"] = int(state.get("count") or 0) + 1
    delay = min(
        DISCOVERY_WARNING_MAX_SECONDS,
        DISCOVERY_WARNING_BASE_SECONDS * (2 ** max(state["count"] - 1, 0)),
    )
    if now >= float(state.get("next_log_at") or 0):
        log(
            f"Discovery source {key} unavailable: {str(error_text)[:220]}. "
            f"Retrying silently for up to {int(delay)}s while monitoring continues.",
            "WARNING",
        )
        state["next_log_at"] = now + delay

def _discovery_register_success(source):
    DISCOVERY_FAILURE_STATE.pop(str(source or "unknown"), None)


def get_traders():
    """
    V3 DIRECT TELEMETRY DISCOVERY

    The Operations/Target Engine must not call /trader_current_account for each
    account. /monitorable_accounts is already account-level and authoritative.

    Primary:
        Monitoring API /monitorable_accounts

    Fallback:
        local roster-watcher snapshot on the VPS

    This path intentionally skips /np_unified_mt5_sync and all dashboard
    enrichment during normal telemetry scans. Those services may be slow or
    return HTTP 500, but they must never block live MT5 telemetry to Admin.
    """
    monitorable = []
    seen = set()
    rows = []
    source = "monitoring_api"

    # 1) Direct authoritative Monitoring API.
    try:
        response = discovery_get(TRADERS_ENDPOINTS[0])
        if response.status_code == 200:
            rows = unpack_rows(response.json())
            _discovery_register_success(source)
            log(f"DIRECT TELEMETRY DISCOVERY source=monitoring_api rows={len(rows)}")
        else:
            _discovery_log_failure(source, f"HTTP {response.status_code}")
    except Exception as exc:
        _discovery_log_failure(source, str(exc))

    # 2) Local watcher snapshot fallback. This keeps telemetry alive during
    # Render latency/outage and uses the same roster already feeding the DD fleet.
    if not rows:
        snapshot_path = r"C:\NairaPips_DD_Fleet\state\monitorable_accounts_snapshot.json"
        try:
            if os.path.exists(snapshot_path):
                import json
                with open(snapshot_path, "r", encoding="utf-8") as fh:
                    payload = json.load(fh)
                rows = unpack_rows(payload)
                source = "local_roster_snapshot"
                log(f"DIRECT TELEMETRY DISCOVERY source=local_roster_snapshot rows={len(rows)}", "WARNING")
        except Exception as exc:
            log(f"Local roster telemetry fallback failed: {exc}", "WARNING")

    for raw in rows:
        try:
            if not isinstance(raw, dict):
                continue

            row = dict(raw)
            row["_source_of_truth"] = "monitoring_api"
            row["_discovery_source"] = source

            if not _trader_is_monitorable(row):
                continue

            login = normalise_login(row.get("mt5_login"))
            account_id = str(
                row.get("trader_account_id")
                or row.get("current_account_id")
                or row.get("id")
                or ""
            ).strip()

            if not login:
                continue

            key = ("account", account_id) if account_id else ("login", login)
            if key in seen:
                continue
            seen.add(key)
            monitorable.append(row)

        except Exception as row_error:
            bad_login = normalise_login(raw.get("mt5_login")) if isinstance(raw, dict) else ""
            log(
                f"DIRECT TELEMETRY ROW ISOLATED mt5={bad_login or 'unknown'} "
                f"error={str(row_error)[:180]}",
                "ERROR",
            )

    monitorable = sorted(monitorable, key=risk_priority_score, reverse=True)

    log(
        f"DIRECT TELEMETRY READY | monitorable={len(monitorable)} "
        f"| source={source} | dashboard_enrichment=OFF | unified_sync_blocking=OFF"
    )

    if not monitorable:
        log(
            "NO DIRECT TELEMETRY ACCOUNTS AVAILABLE: retrying without fabricating MT5 data.",
            "ERROR",
        )
    return monitorable


def get_mt5_pool_accounts():
    if not MT5_POOL_ENDPOINTS:
        return []
    for endpoint in MT5_POOL_ENDPOINTS:
        try:
            response = api_get(endpoint, timeout=20)
            if response.status_code != 200:
                continue
            rows = unpack_rows(response.json())
            if rows:
                return rows
        except Exception:
            continue
    return []


def find_pool_account_by_login(login):
    login = normalise_login(login)
    for acc in get_mt5_pool_accounts():
        acc_login = (
            acc.get("mt5_login")
            or acc.get("login")
            or acc.get("account_login")
            or acc.get("account_number")
            or acc.get("number")
        )
        if normalise_login(acc_login) == login:
            return acc
    return {}


def get_original_account_size(trader, mt5_balance):
    """
    Fixed challenge capital source of truth.

    Do NOT use max(account_size, balance). MT5 balance can grow after profitable
    trades and wrongly increase target equity. Always prefer the fixed plan size.
    """
    for key in ["account_size", "initial_balance", "starting_balance", "challenge_balance", "original_balance"]:
        value = to_float(trader.get(key), 0)
        if value > 0:
            return value

    stored_balance = to_float(trader.get("balance"), 0)
    if stored_balance > 0:
        return stored_balance

    return float(mt5_balance or 0)


def calculate_drawdown(reference_balance, equity):
    if reference_balance <= 0:
        return 0.0
    dd = ((reference_balance - equity) / reference_balance) * 100
    return round(max(dd, 0.0), 2)


def calculate_static_balance_dd(reference_balance, mt5_balance):
    """NairaPips static DD rule: breach if MT5 BALANCE or EQUITY touches 20% max DD.
    Balance is closed-realized P/L proof. Equity is floating risk proof.
    The lower of balance/equity is the breach evidence for dispute protection.
    """
    if reference_balance <= 0:
        return 0.0
    dd = ((reference_balance - float(mt5_balance or 0)) / reference_balance) * 100
    return round(max(dd, 0.0), 2)


def _first_positive_number(row, keys, default=0.0):
    row = row or {}
    for key in keys:
        try:
            value = row.get(key)
            if value is None or str(value).strip() == "":
                continue
            number = float(value)
            if number > 0:
                return number
        except Exception:
            continue
    return float(default or 0.0)


def get_account_dd_rule(trader):
    """Return (authority_present, dd_limit, source) for THIS exact account.

    Production safety: never invent 20% when the purchased account rule is missing.
    A missing DD authority disables automatic breach for that scan and raises a loud
    log so the record can be repaired without risking a false close.
    """
    trader = trader or {}
    if trader.get("dd_authority_present") is False:
        return False, 0.0, "feed.authority_missing"
    for key in (
        "dd_limit_percent", "max_drawdown", "max_drawdown_percent",
        "total_dd", "static_dd_limit_percent"
    ):
        try:
            raw = trader.get(key)
            if raw is None or str(raw).strip() == "":
                continue
            val = float(raw)
            if val > 0:
                return True, val, str(trader.get("dd_authority_source") or key)
        except Exception:
            continue
    return False, 0.0, "authority_missing"


def get_account_dd_limit(trader):
    """Compatibility helper for display/priority only; never use to authorize breach."""
    ok, value, _source = get_account_dd_rule(trader)
    return value if ok else 0.0


def calculate_dd_limit_used(drawdown_percent, dd_limit_percent=None):
    """Percentage of THIS account's allowed static DD already consumed."""
    limit = float(dd_limit_percent or MAX_DRAWDOWN_LIMIT_PERCENT or 20.0)
    if limit <= 0:
        return 0.0
    used = (float(drawdown_percent or 0) / limit) * 100
    return round(max(used, 0.0), 2)


def determine_zone(drawdown, funded=False, profit_percent=0, dd_limit_percent=None):
    """Risk bands scale to the exact account DD limit.

    20% plan => warning 5, danger 10, critical 15, breached 20.
    10% plan => warning 2.5, danger 5, critical 7.5, breached 10.
    """
    limit = float(dd_limit_percent or MAX_DRAWDOWN_LIMIT_PERCENT or 20.0)
    if drawdown >= limit:
        return "breached"
    if drawdown >= limit * 0.75:
        return "critical"
    if drawdown >= limit * 0.50:
        return "danger"
    if drawdown >= limit * 0.25:
        return "warning"
    if funded and profit_percent >= FUNDED_PROFIT_ZONE_PERCENT:
        return "funded_profit_zone"
    return "funded" if funded else "safe"


def send_snapshot(payload):
    try:
        response = api_post(SNAPSHOT_ENDPOINT, payload, timeout=45)
        if response.status_code not in [200, 201]:
            # Snapshot/backend paperwork is deliberately non-blocking. A broker-side
            # breach close has already happened before this path. Keep the HTTP code
            # visible without dumping a full HTML 500 page into the production console.
            body = (response.text or "").replace("\n", " ")[:240]
            log(f"Snapshot API failed HTTP {response.status_code}: {body}", "ERROR")
    except Exception as e:
        log(f"Snapshot send failed: {e}", "ERROR")


def disable_mt5_access(trader, login, reason, status="breached", evidence=None):
    account_id = trader.get("trader_account_id") or trader.get("current_account_id")
    payload = {
        "trader_id": trader.get("id"),
        "trader_account_id": account_id,
        "current_account_id": account_id,
        "mt5_login": str(login),
        "reason": reason,
        "status": status,
    }
    # CRITICAL: carry final MT5 evidence with pass/breach lock action.
    # This protects production when the normal snapshot API is suppressed by
    # cooldown or temporarily fails. Backend will save these numbers before archiving.
    if isinstance(evidence, dict):
        for k in [
            "balance", "equity", "profit", "profit_percent", "current_profit",
            "current_profit_percent", "highest_equity", "lowest_equity",
            "highest_profit", "highest_profit_percent", "target_equity",
            "profit_target", "target_percent", "pass_progress_percent",
            "mt5_balance", "current_balance", "start_balance", "account_size",
            # BREACH PERSISTENCE: the backend validates DD against the final live
            # balance/equity. Carry the complete DD evidence with the lock request
            # so it never compares fresh MT5 values with an older dashboard DD.
            "drawdown", "drawdown_percent", "actual_drawdown_percent",
            "current_drawdown_percent", "balance_drawdown_percent",
            "equity_drawdown_percent", "dd_used_percent", "max_drawdown_used",
            "max_dd_used", "current_dd_used_percent", "drawdown_amount",
            "dd_remaining_percent", "worst_static_drawdown_percent",
            "worst_dd_used_percent", "worst_dd_remaining_percent",
            "static_dd_limit_percent", "dd_limit_percent", "breach_evidence_value",
            "breached_by_balance", "breached_by_equity", "breach_equity_level",
            "breach_source", "breached",
            "phase_label", "phase_pass_status", "zone", "timestamp",
            "rule_violation_detected", "rule_violation_score",
            "rule_violation_signals", "rule_violation_evidence",
            "rule_violation_policy", "rule_violation_detected_at"
        ]:
            if k in evidence:
                payload[k] = evidence.get(k)
    try:
        response = api_post(DISABLE_MT5_ENDPOINT, payload, timeout=45)
        if response.status_code in [200, 201]:
            log(f"NairaPips account locked for {login}. Reason={reason}", "WARNING")
            return True
        log(f"Disable MT5 access failed for {login}: {response.text}", "ERROR")
        return False
    except Exception as e:
        log(f"Disable MT5 access error for {login}: {e}", "ERROR")
        return False


def _deal_time_iso(value):
    try:
        return datetime.fromtimestamp(int(value or 0), tz=timezone.utc).isoformat()
    except Exception:
        return now_iso()


def _deal_type_text(deal_type):
    if deal_type == getattr(mt5, "DEAL_TYPE_BUY", 0):
        return "BUY"
    if deal_type == getattr(mt5, "DEAL_TYPE_SELL", 1):
        return "SELL"
    return "TRADE"


def _is_trade_deal(deal):
    # Ignore deposits, balance adjustments, credits and non-symbol records.
    if not getattr(deal, "symbol", ""):
        return False
    return getattr(deal, "type", None) in [getattr(mt5, "DEAL_TYPE_BUY", 0), getattr(mt5, "DEAL_TYPE_SELL", 1)]


def _entry_name(entry):
    if entry == getattr(mt5, "DEAL_ENTRY_IN", 0):
        return "in"
    if entry == getattr(mt5, "DEAL_ENTRY_OUT", 1):
        return "out"
    if entry == getattr(mt5, "DEAL_ENTRY_INOUT", 2):
        return "inout"
    if entry == getattr(mt5, "DEAL_ENTRY_OUT_BY", 3):
        return "out_by"
    return str(entry)


def build_closed_history_trades(trader, login):
    """
    Build closed trade records from MT5 history deals.
    This is required for dispute settlement: traders/admin must see what happened
    even after a position is closed.
    """
    date_to = datetime.now(timezone.utc)
    date_from = date_to - timedelta(days=HISTORY_LOOKBACK_DAYS)

    deals = mt5.history_deals_get(date_from, date_to)
    if deals is None:
        log(f"History sync failed for {login}: {mt5.last_error()}", "ERROR")
        return []

    grouped = {}
    for deal in deals:
        if not _is_trade_deal(deal):
            continue
        position_id = str(getattr(deal, "position_id", "") or getattr(deal, "order", "") or getattr(deal, "ticket", ""))
        if not position_id or position_id == "0":
            position_id = str(getattr(deal, "ticket", ""))
        grouped.setdefault(position_id, []).append(deal)

    closed_trades = []
    for position_id, items in grouped.items():
        items = sorted(items, key=lambda d: getattr(d, "time", 0))
        open_deals = [d for d in items if getattr(d, "entry", None) in [getattr(mt5, "DEAL_ENTRY_IN", 0), getattr(mt5, "DEAL_ENTRY_INOUT", 2)]]
        close_deals = [d for d in items if getattr(d, "entry", None) in [getattr(mt5, "DEAL_ENTRY_OUT", 1), getattr(mt5, "DEAL_ENTRY_INOUT", 2), getattr(mt5, "DEAL_ENTRY_OUT_BY", 3)]]

        # Only closed records belong in history. Open positions are handled from positions_get().
        if not close_deals:
            continue

        first = open_deals[0] if open_deals else items[0]
        last = close_deals[-1]
        trade_type = _deal_type_text(getattr(first, "type", None))
        volume = sum(float(getattr(d, "volume", 0) or 0) for d in close_deals) or float(getattr(last, "volume", 0) or 0)
        profit = sum(float(getattr(d, "profit", 0) or 0) for d in items)
        swap = sum(float(getattr(d, "swap", 0) or 0) for d in items)
        commission = sum(float(getattr(d, "commission", 0) or 0) for d in items)

        closed_trades.append({
            "trader_id": trader.get("id"),
            "trader_account_id": trader.get("trader_account_id") or trader.get("current_account_id"),
            "trader_name": trader.get("full_name") or trader.get("name") or "Trader",
            "email": trader.get("email"),
            "mt5_login": str(login),
            "symbol": getattr(first, "symbol", ""),
            # Use a stable history ticket so closed records do not overwrite open rows.
            "ticket": f"H-{position_id}",
            "trade_type": trade_type,
            "volume": float(volume),
            "open_price": float(getattr(first, "price", 0) or 0),
            "current_price": float(getattr(last, "price", 0) or 0),
            "sl": 0,
            "tp": 0,
            "profit": float(profit),
            "swap": float(swap),
            "commission": float(commission),
            "status": "closed",
            "opened_at": _deal_time_iso(getattr(first, "time", 0)),
            "closed_at": _deal_time_iso(getattr(last, "time", 0)),
            "history_entry": _entry_name(getattr(last, "entry", "")),
        })

    return closed_trades


def sync_open_trades(trader, login):
    positions = mt5.positions_get()
    if positions is None:
        log(f"Open trade sync failed for {login}: {mt5.last_error()}", "ERROR")
        positions = []

    trades = []
    for pos in positions:
        trade_type = "BUY" if pos.type == mt5.POSITION_TYPE_BUY else "SELL"
        tick = mt5.symbol_info_tick(pos.symbol)
        current_price = 0
        if tick:
            current_price = tick.bid if trade_type == "BUY" else tick.ask

        try:
            opened_at = datetime.fromtimestamp(pos.time, tz=timezone.utc).isoformat()
        except Exception:
            opened_at = now_iso()

        trades.append({
            "trader_id": trader.get("id"),
            "trader_account_id": trader.get("trader_account_id") or trader.get("current_account_id"),
            "trader_name": trader.get("full_name") or trader.get("name") or "Trader",
            "email": trader.get("email"),
            "mt5_login": str(login),
            "symbol": pos.symbol,
            "ticket": str(pos.ticket),
            "trade_type": trade_type,
            "volume": float(pos.volume),
            "open_price": float(pos.price_open),
            "current_price": float(current_price),
            "sl": float(pos.sl or 0),
            "tp": float(pos.tp or 0),
            "profit": float(pos.profit),
            "swap": float(pos.swap),
            "commission": 0,
            "status": "open",
            "opened_at": opened_at,
            "closed_at": None,
        })

    now_ts = time.time()
    last_history = LAST_HISTORY_SYNC_BY_LOGIN.get(str(login), 0)
    should_sync_history = (now_ts - last_history) >= HISTORY_SYNC_INTERVAL_SECONDS
    history_trades = []
    if should_sync_history:
        history_trades = build_closed_history_trades(trader, login)
        if len(history_trades) > MAX_HISTORY_TRADES_PER_SYNC:
            history_trades = sorted(history_trades, key=lambda x: str(x.get("closed_at") or x.get("opened_at") or ""), reverse=True)[:MAX_HISTORY_TRADES_PER_SYNC]
        LAST_HISTORY_SYNC_BY_LOGIN[str(login)] = now_ts

    all_trades = trades + history_trades
    if not all_trades:
        return

    try:
        response = api_post(SYNC_TRADES_ENDPOINT, {"trades": all_trades}, timeout=60)
        if response.status_code not in [200, 201]:
            log(f"Trade sync API failed for {login}: {response.text[:500]}", "ERROR")
        else:
            log(f"Synced {len(trades)} open + {len(history_trades)} history trade(s) for {login}")
    except Exception as e:
        log(f"Trade sync error for {login}: {e}", "ERROR")

def rotate_master_password(login, server, master_password, reason, new_master=None, new_investor=None):
    """Compatibility no-op. NairaPips Exness setup does not support broker-side password rotation.

    The production lock is the persistent zero-exposure watchdog: once a hard financial
    lock is active, every newly opened position/pending order is closed/cancelled again.
    """
    log(
        f"BROKER PASSWORD ROTATION NOT USED for {login}. "
        f"Persistent zero-exposure watchdog is the trading lock. Reason={reason}",
        "INFO",
    )
    return False


ADMIN_ALERT_EMAIL_DEFAULT = os.getenv("ADMIN_ALERT_EMAIL", "support@nairapips.com")


def close_all_open_positions(login):
    if not MT5_ALLOW_TRADE_CLOSE:
        log(f"TRADE CLOSE DISABLED: close request suppressed for {login}", "WARNING")
        return False

    account_info = mt5.account_info()
    if account_info is None:
        log(f"ABORT CLOSE: MT5 account_info unavailable for target {login}: {mt5.last_error()}", "ERROR")
        return False

    if str(getattr(account_info, "login", "")) != str(login):
        log(f"ABORT CLOSE: terminal login {getattr(account_info, 'login', None)} does not match target {login}", "ERROR")
        return False

    positions = mt5.positions_get()
    if positions is None:
        log(f"No positions returned for {login}: {mt5.last_error()}", "ERROR")
        return False

    if len(positions) == 0:
        log(f"No open trades to close for {login}")
        return True

    log(f"WATCHDOG CLOSE: Closing {len(positions)} open trade(s) for {login}", "WARNING")
    all_closed = True

    for pos in positions:
        symbol = pos.symbol
        ticket = pos.ticket
        volume = pos.volume

        if not mt5.symbol_select(symbol, True):
            log(f"Symbol select failed: {symbol} | ticket={ticket}", "ERROR")
            all_closed = False
            continue

        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            log(f"No tick for {symbol} | ticket={ticket}", "ERROR")
            all_closed = False
            continue

        if pos.type == mt5.POSITION_TYPE_BUY:
            order_type = mt5.ORDER_TYPE_SELL
            price = tick.bid
        else:
            order_type = mt5.ORDER_TYPE_BUY
            price = tick.ask

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": volume,
            "type": order_type,
            "position": ticket,
            "price": price,
            "deviation": 500,
            "magic": 909090,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)
        if result is None:
            log(f"Close failed ticket={ticket}: {mt5.last_error()}", "ERROR")
            all_closed = False
            continue

        if result.retcode == mt5.TRADE_RETCODE_DONE:
            log(f"Closed ticket={ticket} | {symbol} | volume={volume}", "WARNING")
        else:
            log(f"Close failed ticket={ticket}: retcode={result.retcode} | comment={result.comment}", "ERROR")
            all_closed = False

    return all_closed


def cancel_all_pending_orders(login):
    """Remove pending orders while an account is locked."""
    account_info = mt5.account_info()
    if account_info is None or str(getattr(account_info, "login", "")) != str(login):
        log(f"ABORT ORDER CANCEL: terminal login does not match target {login}", "ERROR")
        return False

    orders = mt5.orders_get()
    if orders is None:
        log(f"Pending-order query failed for {login}: {mt5.last_error()}", "ERROR")
        return False
    if not orders:
        return True

    all_removed = True
    log(f"WATCHDOG CANCEL: Removing {len(orders)} pending order(s) for {login}", "WARNING")
    for order in orders:
        result = mt5.order_send({
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": int(order.ticket),
            "magic": 909090,
            "comment": "NairaPips payout lock",
        })
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            log(
                f"Pending order cancel failed ticket={getattr(order, 'ticket', None)} "
                f"retcode={getattr(result, 'retcode', None)} comment={getattr(result, 'comment', '')}",
                "ERROR",
            )
            all_removed = False
        else:
            log(f"Cancelled pending order ticket={order.ticket}", "WARNING")
    return all_removed


def persistent_master_lockdown(login, server, master_password, reason):
    """Repeatedly close positions, cancel orders, and verify zero exposure.

    This function intentionally has no email/API cooldown.
    """
    if not PAYOUT_LOCK_WATCHDOG_ENABLED:
        log(f"PERSISTENT PAYOUT WATCHDOG DISABLED for {login}", "ERROR")
        return False
    if not master_password:
        log(f"PAYOUT WATCHDOG cannot protect {login}: master password missing", "ERROR")
        return False

    # CREDENTIAL-RESILIENCE: the account row can temporarily contain an older
    # master password after an MT5 credential update.  For a locked/breached
    # account, try the exact same login/server with the MT5 Pool master as a
    # second credential.  Never change login or infer a breach from auth failure.
    master_candidates = []
    for candidate in [master_password]:
        if candidate and str(candidate) not in [str(x) for x in master_candidates]:
            master_candidates.append(candidate)
    pool_acc = find_pool_account_by_login(str(login))
    pool_master = (
        pool_acc.get("mt5_master_password")
        or pool_acc.get("master_password")
        or pool_acc.get("password")
        or pool_acc.get("mt5_password")
        or ""
    )
    if pool_master and str(pool_master) not in [str(x) for x in master_candidates]:
        master_candidates.append(pool_master)

    for attempt in range(1, PAYOUT_LOCK_CLOSE_PASSES + 1):
        login_ok = False
        last_auth_error = None
        for idx, candidate in enumerate(master_candidates, start=1):
            if mt5.login(login=login, password=candidate, server=server):
                master_password = candidate
                login_ok = True
                if idx > 1:
                    log(f"PAYOUT WATCHDOG master auth recovery SUCCESS for {login}", "WARNING")
                break
            last_auth_error = mt5.last_error()
        if not login_ok:
            log(f"PAYOUT WATCHDOG master login failed for {login} after {len(master_candidates)} credential candidate(s): {last_auth_error}", "ERROR")
            return False

        close_ok = close_all_open_positions(login)
        orders_ok = cancel_all_pending_orders(login)
        remaining_positions = mt5.positions_get()
        remaining_orders = mt5.orders_get()
        position_count = len(remaining_positions or [])
        order_count = len(remaining_orders or [])

        if position_count == 0 and order_count == 0:
            log(
                f"PAYOUT WATCHDOG VERIFIED ZERO EXPOSURE for {login}. "
                f"Reason={reason}; pass={attempt}",
                "WARNING",
            )
            return bool(close_ok and orders_ok)

        log(
            f"PAYOUT WATCHDOG RETRY {attempt}/{PAYOUT_LOCK_CLOSE_PASSES} for {login}: "
            f"positions={position_count}, pending_orders={order_count}",
            "ERROR",
        )
        time.sleep(PAYOUT_LOCK_CLOSE_RETRY_SECONDS)

    log(
        f"CRITICAL PAYOUT LIABILITY: unable to reach zero exposure for {login} "
        f"after {PAYOUT_LOCK_CLOSE_PASSES} passes. Manual broker action required.",
        "ERROR",
    )
    return False


def automated_order_evidence():
    """Return strong order fingerprints of EA/bot/copy-trader activity."""
    evidence = []
    for pos in (mt5.positions_get() or []):
        magic = int(getattr(pos, "magic", 0) or 0)
        comment = str(getattr(pos, "comment", "") or "").strip()
        keyword = next((k for k in AUTOMATED_ORDER_COMMENT_KEYWORDS if k in comment.lower()), "")
        if magic != 0 or keyword:
            evidence.append({
                "kind": "position",
                "ticket": int(getattr(pos, "ticket", 0) or 0),
                "symbol": str(getattr(pos, "symbol", "") or ""),
                "magic": magic,
                "comment": comment[:180],
                "matched_keyword": keyword,
            })
    for order in (mt5.orders_get() or []):
        magic = int(getattr(order, "magic", 0) or 0)
        comment = str(getattr(order, "comment", "") or "").strip()
        keyword = next((k for k in AUTOMATED_ORDER_COMMENT_KEYWORDS if k in comment.lower()), "")
        if magic != 0 or keyword:
            evidence.append({
                "kind": "pending_order",
                "ticket": int(getattr(order, "ticket", 0) or 0),
                "symbol": str(getattr(order, "symbol", "") or ""),
                "magic": magic,
                "comment": comment[:180],
                "matched_keyword": keyword,
            })
    return evidence


def _rule_guard_ts(value):
    try:
        return int(value or 0)
    except Exception:
        return 0


def _rule_guard_entry_deals(deals):
    entry_in = {
        getattr(mt5, "DEAL_ENTRY_IN", 0),
        getattr(mt5, "DEAL_ENTRY_INOUT", 2),
    }
    buy_sell = {
        getattr(mt5, "DEAL_TYPE_BUY", 0),
        getattr(mt5, "DEAL_TYPE_SELL", 1),
    }
    return [
        d for d in (deals or [])
        if getattr(d, "type", None) in buy_sell
        and getattr(d, "entry", None) in entry_in
        and getattr(d, "symbol", "")
    ]


def _rule_guard_max_events_in_window(times, window_seconds):
    times = sorted(t for t in times if t > 0)
    best = 0
    left = 0
    for right, current in enumerate(times):
        while left <= right and current - times[left] > window_seconds:
            left += 1
        best = max(best, right - left + 1)
    return best


def _rule_guard_closed_durations(deals):
    grouped = {}
    for d in deals or []:
        if not _is_trade_deal(d):
            continue
        pid = str(
            getattr(d, "position_id", "")
            or getattr(d, "order", "")
            or getattr(d, "ticket", "")
        )
        grouped.setdefault(pid, []).append(d)

    durations = []
    for items in grouped.values():
        items = sorted(items, key=lambda x: _rule_guard_ts(getattr(x, "time", 0)))
        entries = [
            x for x in items
            if getattr(x, "entry", None) in {
                getattr(mt5, "DEAL_ENTRY_IN", 0),
                getattr(mt5, "DEAL_ENTRY_INOUT", 2),
            }
        ]
        exits = [
            x for x in items
            if getattr(x, "entry", None) in {
                getattr(mt5, "DEAL_ENTRY_OUT", 1),
                getattr(mt5, "DEAL_ENTRY_INOUT", 2),
                getattr(mt5, "DEAL_ENTRY_OUT_BY", 3),
            }
        ]
        if entries and exits:
            opened = _rule_guard_ts(getattr(entries[0], "time", 0))
            closed = _rule_guard_ts(getattr(exits[-1], "time", 0))
            if opened and closed >= opened:
                durations.append(closed - opened)
    return durations


def _rule_guard_martingale_sequences(entries):
    grouped = {}
    for d in entries:
        key = (
            str(getattr(d, "symbol", "")),
            int(getattr(d, "type", -1)),
        )
        grouped.setdefault(key, []).append(d)

    evidence = []
    for (symbol, direction), items in grouped.items():
        items = sorted(items, key=lambda x: _rule_guard_ts(getattr(x, "time", 0)))
        streak = []
        for d in items:
            volume = float(getattr(d, "volume", 0) or 0)
            if volume <= 0:
                streak = []
                continue
            if not streak:
                streak = [d]
                continue
            previous_volume = float(getattr(streak[-1], "volume", 0) or 0)
            if previous_volume > 0 and volume >= previous_volume * RULE_MARTINGALE_MULTIPLIER:
                streak.append(d)
            else:
                streak = [d]

            if len(streak) >= RULE_MARTINGALE_STEPS:
                evidence.append({
                    "symbol": symbol,
                    "direction": "BUY" if direction == getattr(mt5, "DEAL_TYPE_BUY", 0) else "SELL",
                    "steps": len(streak),
                    "volumes": [float(getattr(x, "volume", 0) or 0) for x in streak[-RULE_MARTINGALE_STEPS:]],
                    "seconds": (
                        _rule_guard_ts(getattr(streak[-1], "time", 0))
                        - _rule_guard_ts(getattr(streak[-RULE_MARTINGALE_STEPS], "time", 0))
                    ),
                })
                break
    return evidence


def _rule_guard_grid_evidence(positions):
    grouped = {}
    for p in positions or []:
        key = (
            str(getattr(p, "symbol", "")),
            int(getattr(p, "type", -1)),
        )
        grouped.setdefault(key, []).append(p)

    evidence = []
    for (symbol, direction), items in grouped.items():
        if len(items) < RULE_GRID_POSITION_COUNT:
            continue
        prices = sorted(float(getattr(p, "price_open", 0) or 0) for p in items)
        prices = [p for p in prices if p > 0]
        if len(prices) < RULE_GRID_POSITION_COUNT:
            continue
        gaps = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        positive = [g for g in gaps if g > 0]
        if len(positive) < RULE_GRID_POSITION_COUNT - 2:
            continue
        mean_gap = sum(positive) / len(positive)
        variance = sum((g - mean_gap) ** 2 for g in positive) / len(positive)
        cv = (variance ** 0.5 / mean_gap) if mean_gap > 0 else 999
        # Regular spacing plus many same-direction positions is strong grid evidence.
        if cv <= 0.35:
            evidence.append({
                "symbol": symbol,
                "direction": "BUY" if direction == getattr(mt5, "POSITION_TYPE_BUY", 0) else "SELL",
                "positions": len(items),
                "spacing_cv": round(cv, 4),
                "volumes": [float(getattr(p, "volume", 0) or 0) for p in items[:12]],
            })
    return evidence


def detect_prohibited_trading_pattern(login):
    """Return high-confidence behavioural evidence.

    This does not claim to identify software by name. It identifies execution
    patterns covered by NairaPips rules: HFT/burst execution, rapid scalping,
    martingale escalation and regular grid structures.

    Auto-enforcement requires multiple independent signals, except an extreme
    machine-like burst which is sufficient by itself.
    """
    if not RULE_VIOLATION_GUARD_ENABLED:
        return {"detected": False, "score": 0, "signals": [], "evidence": {}}

    now = datetime.now(timezone.utc)
    date_from = now - timedelta(minutes=max(RULE_GUARD_LOOKBACK_MINUTES, 1))

    deals = mt5.history_deals_get(date_from, now)
    if deals is None:
        log(f"Rule guard skipped for {login}: history unavailable {mt5.last_error()}", "WARNING")
        return {"detected": False, "score": 0, "signals": [], "evidence": {"history_error": str(mt5.last_error())}}

    entries = _rule_guard_entry_deals(deals)
    positions = mt5.positions_get() or []

    score = 0
    signals = []
    evidence = {
        "lookback_minutes": RULE_GUARD_LOOKBACK_MINUTES,
        "entry_count": len(entries),
        "open_position_count": len(positions),
    }

    entry_times = [_rule_guard_ts(getattr(d, "time", 0)) for d in entries]
    burst_count = _rule_guard_max_events_in_window(entry_times, RULE_BURST_SECONDS)
    extreme_burst_count = _rule_guard_max_events_in_window(entry_times, RULE_EXTREME_BURST_SECONDS)
    evidence["max_entries_in_burst_window"] = burst_count
    evidence["max_entries_in_extreme_window"] = extreme_burst_count

    extreme_burst = extreme_burst_count >= RULE_EXTREME_BURST_COUNT
    if extreme_burst:
        score += 110
        signals.append("extreme_machine_like_execution_burst")
    elif burst_count >= RULE_BURST_COUNT:
        score += 65
        signals.append("high_frequency_execution_burst")

    durations = _rule_guard_closed_durations(deals)
    rapid_count = sum(1 for seconds in durations if seconds <= RULE_RAPID_SCALP_MAX_SECONDS)
    evidence["rapid_closed_trade_count"] = rapid_count
    evidence["rapid_trade_max_seconds"] = RULE_RAPID_SCALP_MAX_SECONDS
    if rapid_count >= RULE_RAPID_SCALP_COUNT:
        score += 55
        signals.append("repeated_ultra_short_trade_duration")

    martingale = _rule_guard_martingale_sequences(entries)
    evidence["martingale_sequences"] = martingale[:5]
    if martingale:
        score += 60
        signals.append("martingale_lot_escalation")

    grid = _rule_guard_grid_evidence(positions)
    evidence["grid_structures"] = grid[:5]
    if grid:
        score += 60
        signals.append("regular_grid_position_structure")

    detected = (
        extreme_burst
        or (
            score >= RULE_GUARD_MIN_SCORE
            and len(set(signals)) >= RULE_GUARD_MIN_SIGNALS
        )
    )

    return {
        "detected": bool(detected),
        "score": int(score),
        "signals": sorted(set(signals)),
        "evidence": evidence,
        "policy": (
            f"auto-close requires extreme burst OR score>={RULE_GUARD_MIN_SCORE} "
            f"with >={RULE_GUARD_MIN_SIGNALS} independent signals"
        ),
    }


def should_enforce_rule_violation(login):
    now = _now_ts()
    last = float(LAST_RULE_GUARD_ACTION.get(str(login)) or 0)
    if now - last < RULE_GUARD_COOLDOWN_SECONDS:
        return False
    LAST_RULE_GUARD_ACTION[str(login)] = now
    return True



def get_phase_target(trader):
    """Exact-account target authority.

    Monitoring feed already resolves account -> purchase -> plan. The engine must
    execute that rule, never invent a blanket Phase 1 target.
    """
    phase = phase_text(trader)
    if is_funded_or_live(trader):
        return False, 0.0, "funded"

    explicit_target = _first_positive_number(
        trader,
        [
            "target_percent",
            "profit_target",
            "phase_target_percent",
            "phase1_target" if phase not in ["phase2", "phase_2", "phase 2", "two", "2"] else "phase2_target",
        ],
        0.0,
    )

    if phase in ["phase2", "phase_2", "phase 2", "two", "2"]:
        if explicit_target > 0:
            return True, explicit_target, "phase2"
        log(f"TARGET AUTHORITY MISSING for MT5 {trader.get('mt5_login')}: Phase 2 exact target absent. PASS AUTO-CLOSE DISABLED; DD monitoring continues.", "WARNING")
        return False, 0.0, "phase2"

    if explicit_target > 0:
        return True, explicit_target, "phase1"

    # CRITICAL SAFETY: never guess a blanket 10% target. Missing target means no automatic pass/close.
    log(f"TARGET AUTHORITY MISSING for MT5 {trader.get('mt5_login')}: Phase 1 exact target absent. PASS AUTO-CLOSE DISABLED; DD monitoring continues.", "WARNING")
    return False, 0.0, "phase1"


def has_phase1_passed(trader):
    return bool(trader.get("phase1_passed_at")) or str(trader.get("phase_pass_status") or "").lower().strip() == "phase1_passed"


def has_phase2_passed(trader):
    return bool(trader.get("phase2_passed_at")) or str(trader.get("phase_pass_status") or "").lower().strip() == "phase2_passed"


def phase_aware_existing_highest(trader, current_equity, reference_balance, phase_label):
    """
    Account-level highest equity is the official pass evidence.

    Critical fix: Phase 2 must pass if the ACTIVE Phase 2 account ever touches
    the 8% target, even if equity later drops. The previous guard discarded
    account-level highest_equity when Phase 1 had passed, causing NairaPips to
    miss real Phase 2 passes.

    We only ignore stale legacy trader-row highs when there is no active
    trader_account_id / source_of_truth account.
    """
    existing = to_float(trader.get("highest_equity") or trader.get("peak_equity") or 0, 0)

    has_account_source = bool(trader.get("trader_account_id") or trader.get("current_account_id") or trader.get("_source_of_truth") == "trader_accounts")

    if phase_label == "phase2" and has_phase1_passed(trader) and not has_phase2_passed(trader) and not has_account_source:
        # Legacy-only safety: do not use old Phase 1 trader-level high for Phase 2.
        return max(float(current_equity or 0), float(reference_balance or 0))

    return max(existing, float(current_equity or 0), float(reference_balance or 0))


def stored_highest_equity(trader, current_equity, reference_balance):
    """
    Highest equity is the pass source of truth.
    It protects traders who already touched the profit target before equity later dropped.
    """
    existing = to_float(trader.get("highest_equity") or trader.get("peak_equity") or 0, 0)
    return max(existing, float(current_equity or 0), float(reference_balance or 0))


def phase_pass_status_for_label(phase_label):
    if phase_label == "phase2":
        return "phase2_passed"
    if phase_label == "phase1":
        return "phase1_passed"
    return "target_hit"

def phase_daily_net_profit_wat():
    """Return (net_daily_profit, realized_today, floating_now, day_label) for Nigeria/WAT.

    Business rule: Phase/Challenge daily growth is measured from original account size.
    Net daily profit = today's MT5 trading P/L (profit + swap + commission) + current floating P/L.
    Earlier losses on the same WAT trading day therefore reduce the amount counted toward the 5% limit.
    Deposits/balance adjustments are excluded because only BUY/SELL trade deals are counted.
    """
    wat = timezone(timedelta(hours=1))
    now_wat = datetime.now(wat)
    start_wat = now_wat.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_wat.astimezone(timezone.utc)
    end_utc = now_wat.astimezone(timezone.utc)

    deals = mt5.history_deals_get(start_utc, end_utc)
    if deals is None:
        raise RuntimeError(f"daily history unavailable: {mt5.last_error()}")

    realized = 0.0
    for deal in deals:
        if not _is_trade_deal(deal):
            continue
        realized += float(getattr(deal, "profit", 0) or 0)
        realized += float(getattr(deal, "swap", 0) or 0)
        realized += float(getattr(deal, "commission", 0) or 0)

    positions = mt5.positions_get()
    if positions is None:
        raise RuntimeError(f"open positions unavailable: {mt5.last_error()}")
    floating = 0.0
    for pos in positions:
        floating += float(getattr(pos, "profit", 0) or 0)
        floating += float(getattr(pos, "swap", 0) or 0)

    return round(realized + floating, 2), round(realized, 2), round(floating, 2), start_wat.date().isoformat()


def funded_profit_protection(reference_balance, equity, profit, profit_percent):
    if reference_balance <= 0:
        return False, 0.0, "no_reference", 0.0
    if profit <= 0:
        return False, 0.0, "no_profit", 0.0

    if profit_percent >= FUNDED_PROFIT_PROTECT_LEVEL_2:
        ratio = FUNDED_PROTECT_RATIO_2
        floor = reference_balance + (profit * ratio)
        return equity <= floor, floor, "funded_profit_protect_50", ratio

    if profit_percent >= FUNDED_PROFIT_PROTECT_LEVEL_1:
        ratio = FUNDED_PROTECT_RATIO_1
        floor = reference_balance + (profit * ratio)
        return equity <= floor, floor, "funded_profit_protect_30", ratio

    if profit_percent >= FUNDED_PROFIT_ZONE_PERCENT:
        return False, reference_balance, "funded_profit_zone", 0.0

    return False, reference_balance, "normal", 0.0




def funded_hard_cap_hit(reference_balance, current_profit_percent, highest_profit_percent, phase_label, cap_percent=15):
    """Hard funded profit cap.
    If a funded/live account reaches the configured cap, NairaPips must lock it for review.
    Use both current and highest profit so a quick spike to cap cannot be missed.
    """
    if reference_balance <= 0 or cap_percent is None:
        return False
    if str(phase_label or "").lower() != "funded":
        return False
    return max(float(current_profit_percent or 0), float(highest_profit_percent or 0)) >= float(cap_percent)


def close_reason_enabled(reason_type):
    return close_allowed(reason_type)

def get_investor_password(trader):
    login = normalise_login(trader.get("mt5_login"))
    password = (
        trader.get("mt5_investor_password")
        or trader.get("investor_password")
        or trader.get("investor")
        or ""
    )
    if password:
        return password

    pool_acc = find_pool_account_by_login(login)
    password = (
        pool_acc.get("mt5_investor_password")
        or pool_acc.get("investor_password")
        or pool_acc.get("investor")
        or ""
    )
    if password:
        log(f"Investor password recovered from MT5 Pool for {login}")
        return password
    return ""


def get_master_password(trader):
    login = normalise_login(trader.get("mt5_login"))
    password = (
        trader.get("mt5_master_password")
        or trader.get("master_password")
        or trader.get("mt5_password")
        or ""
    )
    if password:
        return password

    pool_acc = find_pool_account_by_login(login)
    password = (
        pool_acc.get("mt5_master_password")
        or pool_acc.get("master_password")
        or pool_acc.get("password")
        or pool_acc.get("mt5_password")
        or ""
    )
    if password:
        log(f"Master password recovered from MT5 Pool for {login}")
        return password
    return ""


def master_login_and_close(login, server, master_password, reason):
    if not MT5_ALLOW_TRADE_CLOSE:
        log(f"TRADE CLOSE DISABLED: would close {login}. Reason={reason}", "WARNING")
        return False

    if not master_password:
        log(f"No master password found for {login}. Cannot close trades.", "ERROR")
        return False

    master_login = mt5.login(login=login, password=master_password, server=server)
    if not master_login:
        log(f"Master login failed for {login}: {mt5.last_error()}", "ERROR")
        return False

    log(f"Master login successful for {login}. Reason={reason}", "WARNING")
    return persistent_master_lockdown(login, server, master_password, reason)


def _freshness_trade_evidence(trader):
    """Return (trade_deal_count, net_trade_pnl) for the current logged-in MT5 account.

    Only BUY/SELL deal types are counted so deposits/credits cannot masquerade as trading P/L.
    This is a freshness cross-check only; it never decides pass/breach by itself.
    """
    now = datetime.now(timezone.utc)
    started_raw = trader.get("started_at") or trader.get("created_at") or ""
    start = now - timedelta(days=30)
    try:
        if started_raw:
            text = str(started_raw).replace("Z", "+00:00")
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            start = max(parsed.astimezone(timezone.utc), now - timedelta(days=90))
    except Exception:
        pass

    try:
        deals = mt5.history_deals_get(start, now)
    except Exception:
        deals = None
    if deals is None:
        return 0, 0.0

    buy_type = getattr(mt5, "DEAL_TYPE_BUY", 0)
    sell_type = getattr(mt5, "DEAL_TYPE_SELL", 1)
    trade_types = {buy_type, sell_type}
    count = 0
    net = 0.0
    for d in deals:
        try:
            if getattr(d, "type", None) not in trade_types:
                continue
            count += 1
            net += float(getattr(d, "profit", 0.0) or 0.0)
            net += float(getattr(d, "commission", 0.0) or 0.0)
            net += float(getattr(d, "swap", 0.0) or 0.0)
            net += float(getattr(d, "fee", 0.0) or 0.0)
        except Exception:
            continue
    return count, round(net, 2)


def _reinitialize_and_login(login, server, password):
    """Hard-refresh the terminal session for one exact login without sending any order."""
    try:
        mt5.shutdown()
    except Exception:
        pass
    time.sleep(1.0)
    ok = mt5.initialize(path=MT5_PATH) if MT5_PATH.strip() else mt5.initialize()
    if not ok:
        log(f"FRESHNESS HARD RECONNECT initialize failed for {login}: {mt5.last_error()}", "ERROR")
        return False
    time.sleep(0.75)
    if not mt5.login(login=login, password=password, server=server):
        log(f"FRESHNESS HARD RECONNECT login failed for {login}: {mt5.last_error()}", "ERROR")
        return False
    return True


def _verified_live_account_info(login, server, monitor_password, trader):
    """Return fresh account_info or None.

    Safety contract:
    - exact terminal login must match;
    - give broker state time to settle after each account switch;
    - if MT5 reports untouched starting capital while its own trade history proves non-zero
      realised P/L, force one terminal reconnect and re-read;
    - if the contradiction survives reconnect, DO NOT publish a snapshot or make pass/breach
      decisions. A blind spot is safer than a false real-money action.
    """
    expected = to_float(trader.get("start_balance") or trader.get("account_size") or 0, 0.0)

    def sample(label):
        last = None
        for attempt in range(1, MT5_FRESHNESS_POLL_ATTEMPTS + 1):
            # These read-only calls nudge the terminal to finish account synchronization.
            try:
                mt5.positions_get()
            except Exception:
                pass
            try:
                mt5.orders_get()
            except Exception:
                pass
            info = mt5.account_info()
            if info is None:
                log(f"FRESHNESS {label} {attempt}/{MT5_FRESHNESS_POLL_ATTEMPTS}: account_info unavailable for {login}: {mt5.last_error()}", "WARNING")
                time.sleep(MT5_FRESHNESS_POLL_SECONDS)
                continue
            terminal_login = str(getattr(info, "login", "") or "").strip()
            if terminal_login != str(login):
                # Shared-terminal account switches are asynchronous. Never consume the
                # previous trader's metrics, but keep polling this exact target instead
                # of abandoning it after the first stale account_info() read.
                log(f"FRESHNESS {label} {attempt}/{MT5_FRESHNESS_POLL_ATTEMPTS}: terminal switching target={login} terminal={terminal_login}; waiting for exact login", "WARNING")
                time.sleep(MT5_FRESHNESS_POLL_SECONDS)
                continue
            last = info
            if attempt < MT5_FRESHNESS_POLL_ATTEMPTS:
                time.sleep(MT5_FRESHNESS_POLL_SECONDS)
        return last

    info = sample("SETTLE")
    if info is None and MT5_FRESHNESS_HARD_RECONNECT and monitor_password:
        # Exact-login recovery: reconnect only this requested account and re-verify.
        # This is read-only authentication; it never sends an order.
        log(f"ACCOUNT SWITCH RECOVERY {login}: exact login did not settle; reconnecting and retrying", "WARNING")
        if _reinitialize_and_login(login, server, monitor_password):
            info = sample("SWITCH-RETRY")
    if info is None:
        log(f"ACCOUNT SWITCH RETRY PENDING {login}: no foreign MT5 data published; account will remain in production retry rotation", "ERROR")
        return None

    balance = float(getattr(info, "balance", 0.0) or 0.0)
    equity = float(getattr(info, "equity", 0.0) or 0.0)
    deal_count, trade_pnl = _freshness_trade_evidence(trader)
    unchanged_start = bool(expected > 0 and abs(balance - expected) < 0.01 and abs(equity - expected) < 0.01)
    contradiction = bool(unchanged_start and deal_count > 0 and abs(trade_pnl) >= 1.0)

    if not contradiction:
        log(f"LIVE MT5 VERIFIED {login}: balance={balance:.2f} equity={equity:.2f} trade_deals={deal_count} trade_net={trade_pnl:.2f}")
        return info

    log(
        f"STALE MT5 SUSPECTED {login}: terminal reports untouched start balance/equity={expected:.2f} "
        f"but MT5 history has {deal_count} BUY/SELL deals with net trading P/L {trade_pnl:.2f}. "
        "Refusing this account state until broker data is refreshed.",
        "CRITICAL",
    )

    if MT5_FRESHNESS_HARD_RECONNECT and monitor_password:
        if _reinitialize_and_login(login, server, monitor_password):
            info = sample("RECONNECT")
            if info is not None:
                balance = float(getattr(info, "balance", 0.0) or 0.0)
                equity = float(getattr(info, "equity", 0.0) or 0.0)
                deal_count, trade_pnl = _freshness_trade_evidence(trader)
                unchanged_start = bool(expected > 0 and abs(balance - expected) < 0.01 and abs(equity - expected) < 0.01)
                contradiction = bool(unchanged_start and deal_count > 0 and abs(trade_pnl) >= 1.0)
                if not contradiction:
                    log(f"LIVE MT5 VERIFIED AFTER RECONNECT {login}: balance={balance:.2f} equity={equity:.2f} trade_deals={deal_count} trade_net={trade_pnl:.2f}", "WARNING")
                    return info

    log(
        f"FRESHNESS GATE BLOCKED {login}: live balance/equity could not be proven fresh. "
        "NO snapshot, pass, breach, or broker-close decision will be made for this scan.",
        "CRITICAL",
    )
    return None


def process_account(trader):
    # Immutable per-account cap. Unmarked legacy accounts retain 15%.
    from withdrawal_controls import cap_percent
    funded_cap = cap_percent(trader)
    funded_cap = float(funded_cap) if funded_cap is not None else None
    if not trader.get("mt5_login"):
        log("Skipping trader with empty MT5 login", "ERROR")
        return "inactive"

    raw_login = normalise_login(trader.get("mt5_login"))
    if not _valid_mt5_login(raw_login):
        log(f"Skipping invalid MT5 login value: {raw_login}", "ERROR")
        return "inactive"

    login = int(raw_login)
    server = trader.get("mt5_server")
    trader_id = trader.get("id")
    trader_name = trader.get("full_name") or trader.get("name") or "Trader"

    investor_password = get_investor_password(trader)
    master_password = get_master_password(trader)

    # Keep the latest exact account credentials/metadata in memory. If this account
    # becomes payout/breach locked later in this scan, the dedicated watchdog can
    # revisit it immediately without waiting for backend discovery.
    LOCKED_ACCOUNT_CACHE[str(login)] = dict(trader)

    if not server:
        log(f"No MT5 server found for {login}", "ERROR")
        register_login_failure(login, "missing MT5 server")
        return "offline"

    if login_quarantined(login):
        return "offline"

    cycle_release_authorized = funded_cycle_release_authorized(trader)
    funded_cycle_just_released = False
    locked = is_locked_or_breached(trader)

    if str(login) in PERSISTENT_LOCKED_LOGINS:
        if cycle_release_authorized:
            PERSISTENT_LOCKED_LOGINS.discard(str(login))
            LOCKED_ACCOUNT_CACHE.pop(str(login), None)
            funded_cycle_just_released = True
            locked = False
            log(f"FUNDED CYCLE WATCHDOG RELEASED for {login}: exact assigned_active funded account confirmed.", "WARNING")
        elif not is_locked_or_breached(trader) and not bool(trader.get("payout_required")):
            PERSISTENT_LOCKED_LOGINS.discard(str(login))
            LOCKED_ACCOUNT_CACHE.pop(str(login), None)
            log(f"PAYOUT WATCHDOG RELEASED for {login}: clean active cycle confirmed.", "WARNING")
        else:
            log(f"PERSISTENT PAYOUT WATCHDOG for {login}", "WARNING")
            persistent_master_lockdown(login, server, master_password, "PERSISTENT_PAYOUT_LOCK")
            return "locked"

    # The exact standalone funded-cycle action overrides stale old-cycle payout
    # flags that may still exist in a monitoring snapshot. It cannot override
    # breach/archive/rule-review states because the helper rejects them.
    if cycle_release_authorized:
        locked = False

    if locked:
        log(f"WATCHDOG MODE for locked/breached account {login} | {trader_name}", "WARNING")
        # FORCE ROTATION: if this login is in the force list AND we haven't rotated yet,
        # bypass the locked-skip and rotate the password anyway. One-shot per process.
        if str(login) in FORCE_ROTATE_LOCKED_LOGINS and str(login) not in PASSWORD_ROTATION_DONE:
            log(f"FORCE ROTATION: rotating password for already-locked {login}", "WARNING")
            rotated = rotate_master_password(login, server, master_password, "FORCE_ROTATE_LOCKED")
            if rotated:
                PASSWORD_ROTATION_DONE.add(str(login))
                log(f"FORCE ROTATION COMPLETE for {login}", "WARNING")
        # DD ownership split: a backend-recorded DD breach belongs exclusively to
        # the dedicated Breach Guard.  Do not re-close it from this operations engine.
        dd_terminal = (
            any(bool(trader.get(k)) for k in ("breached_at", "breach_detected_at", "breach_time"))
            or "breach" in str(trader.get("account_status") or "").lower()
            or str(trader.get("risk_zone") or trader.get("zone") or "").lower() == "breached"
            or str(trader.get("status") or "").lower() == "breached"
        )
        if DEDICATED_DD_GUARD_OWNS_BREACH and dd_terminal:
            log(f"DD-GUARD OWNERSHIP: {login} is already breach-terminal; old engine will not close or persist DD.", "WARNING")
        elif is_breached_close_state(trader) and close_allowed("watchdog"):
            PERSISTENT_LOCKED_LOGINS.add(str(login))
            persistent_master_lockdown(login, server, master_password, "LOCKED_ACCOUNT_WATCHDOG")
        else:
            log(f"WATCHDOG SAFE MODE: no trade close for {login}. Monitoring/account lock state only.", "WARNING")
        return "locked"

    if not investor_password:
        log(f"No investor password found for {login}", "ERROR")
        register_login_failure(login, "missing investor password")
        return "offline"

    log(f"Investor monitoring login {login} | {trader_name}")
    monitor_password = investor_password
    authorized = mt5.login(login=login, password=investor_password, server=server)

    if not authorized:
        # AUTH-RECOVERY: trader_accounts can temporarily carry an older investor
        # password while the MT5 Pool already has the current credential. Retry the
        # exact same login/server with a distinct Pool investor password before
        # quarantining. This NEVER fabricates balance/equity and can never breach
        # an account on failed authorization.
        first_error = mt5.last_error()
        pool_acc = find_pool_account_by_login(str(login))
        pool_investor = (
            pool_acc.get("mt5_investor_password")
            or pool_acc.get("investor_password")
            or pool_acc.get("investor")
            or ""
        )
        if pool_investor and str(pool_investor) != str(investor_password):
            log(f"Investor auth recovery retry from MT5 Pool for {login}", "WARNING")
            authorized = mt5.login(login=login, password=pool_investor, server=server)
            if authorized:
                investor_password = pool_investor
                monitor_password = pool_investor
                log(f"Investor auth recovery SUCCESS for {login}", "WARNING")

        if not authorized:
            # LAST SAFE AUTH FALLBACK: a master password has read access too.
            # This is monitoring-only: no order is sent here. It prevents a stale
            # investor password from creating a blind spot while preserving the
            # rule that authentication failure can NEVER create a breach.
            master_candidates = []
            for candidate in [master_password]:
                if candidate and str(candidate) not in [str(x) for x in master_candidates]:
                    master_candidates.append(candidate)
            pool_master = (
                pool_acc.get("mt5_master_password")
                or pool_acc.get("master_password")
                or pool_acc.get("password")
                or pool_acc.get("mt5_password")
                or ""
            )
            if pool_master and str(pool_master) not in [str(x) for x in master_candidates]:
                master_candidates.append(pool_master)
            for candidate in master_candidates:
                log(f"Investor auth unavailable; safe master-monitor fallback for {login}", "WARNING")
                if mt5.login(login=login, password=candidate, server=server):
                    authorized = True
                    master_password = candidate
                    monitor_password = candidate
                    log(f"MASTER-MONITOR AUTH RECOVERY SUCCESS for {login}", "WARNING")
                    break

        if not authorized:
            # CRITICAL SAFETY: failed MT5 authorization must NEVER be treated as a breach.
            # Do not send a zero-equity snapshot; just skip until credentials are corrected.
            final_error = mt5.last_error()
            log(
                f"Investor/master monitoring login failed for {login} on server {server}: first={first_error}, final={final_error} "
                "| SKIPPED - no breach/snapshot",
                "ERROR",
            )
            register_login_failure(login, f"authorization failed on {server}: {final_error}")
            return "offline"

    info = _verified_live_account_info(login, server, monitor_password, trader)
    if info is None:
        # Freshness/account-switch delay is NOT a bad-password event. Do not put a
        # valid production account into the 300s authorization quarantine. It stays
        # eligible for the next cycle and its last verified dashboard snapshot remains.
        log(f"PRODUCTION RETRY QUEUED {login}: freshness not yet verified; retry next cycle", "WARNING")
        return "retry_pending"

    register_login_success(login)

    mt5_balance = float(info.balance)
    equity = float(info.equity)

    expected_size = get_original_account_size(trader, mt5_balance)
    dd_authority_present, dd_limit_percent, dd_authority_source = get_account_dd_rule(trader)

    if expected_size > 0 and mt5_balance <= 0 and equity <= 0:
        # 2026-09-15 ZERO-BALANCE BREACH EVIDENCE FIX
        #
        # A genuine failed/disabled MT5 session can return 0/0, so zero values alone must
        # NEVER create a breach.  However, when THIS exact verified MT5 login also has
        # BUY/SELL history whose realised net loss already exceeds the exact account DD
        # limit, the breach is independently proven by trade history.  In that case,
        # refusing the snapshot leaves the trader dashboard and Journey Cockpit stale.
        #
        # Safety requirements before 0/0 may continue into normal breach processing:
        #   1) exact live MT5 login has already been verified above;
        #   2) exact DD authority exists for this account;
        #   3) at least one BUY/SELL deal exists for this account window;
        #   4) realised trading loss is already >= the exact DD limit.
        deal_count_zero, trade_net_zero = _freshness_trade_evidence(trader)
        realised_loss_zero = max(0.0, -float(trade_net_zero or 0.0))
        realised_loss_pct_zero = (
            (realised_loss_zero / float(expected_size)) * 100.0
            if expected_size > 0 else 0.0
        )
        zero_breach_proven = bool(
            dd_authority_present
            and float(dd_limit_percent or 0.0) > 0
            and int(deal_count_zero or 0) > 0
            and realised_loss_pct_zero >= float(dd_limit_percent)
        )

        if zero_breach_proven:
            log(
                f"ZERO BALANCE/EQUITY TERMINAL BREACH PROVEN for {login}: "
                f"deals={deal_count_zero} trade_net={trade_net_zero:.2f} "
                f"realised_loss={realised_loss_pct_zero:.2f}% "
                f">= exact DD limit {dd_limit_percent:.2f}% ({dd_authority_source}). "
                "Continuing through normal breach/snapshot pipeline.",
                "CRITICAL",
            )
            # Do NOT mark this as a login failure.  The exact login was verified and
            # trade history independently proves a terminal DD breach.
        else:
            # Preserve the original false-breach protection when 0/0 is not independently
            # proven by exact-account trading history.
            log(
                f"Zero MT5 balance/equity for {login} while expected size={expected_size}. "
                f"Trade proof insufficient: deals={deal_count_zero}, trade_net={trade_net_zero:.2f}, "
                f"loss={realised_loss_pct_zero:.2f}%, dd_authority={dd_authority_present}, "
                f"dd_limit={dd_limit_percent}. SKIPPED - check MT5 credentials/server; no breach/snapshot",
                "ERROR",
            )
            register_login_failure(login, "zero balance/equity without independently proven DD breach")
            return "offline"

    reference_balance = expected_size
    if not dd_authority_present:
        log(
            f"DD AUTHORITY MISSING for MT5 {login}: AUTO-BREACH DISABLED for this scan. "
            "Target/pass monitoring may continue only if its own authority is present.",
            "ERROR",
        )
    breach_equity_level = (reference_balance * (1 - dd_limit_percent / 100)) if dd_authority_present else 0.0

    # Current floating profit is used for live funded protection.
    current_profit = equity - reference_balance
    current_profit_percent = round((current_profit / reference_balance) * 100, 2) if reference_balance > 0 else 0

    # NairaPips official DD rule is STATIC 20% from starting account size.
    # Breach must be detected from the WORST of MT5 balance or equity, not equity alone.
    # This prevents a trader from breaching by closed loss, then recovering floating equity and escaping breach.
    balance_drawdown = calculate_static_balance_dd(reference_balance, mt5_balance)
    equity_drawdown = calculate_drawdown(reference_balance, equity)
    breach_evidence_value = min(float(mt5_balance or 0), float(equity or 0))
    drawdown = max(balance_drawdown, equity_drawdown)
    dd_limit_used = calculate_dd_limit_used(drawdown, dd_limit_percent) if dd_authority_present else 0.0

    target_enabled, target_percent, phase_label = get_phase_target(trader)
    funded = phase_label == "funded"
    current_pass_progress = ((current_profit_percent / target_percent) * 100.0) if (target_enabled and target_percent > 0 and not funded) else 0.0

    # Funded accounts are prioritised as they approach the 15% payout-cycle ceiling.
    if funded:
        usage, boundary = update_live_boundary_priority(
            login, dd_used=dd_limit_used, pass_progress=0.0, funded_profit_pct=current_profit_percent, cap_percent=funded_cap
        )
        if usage >= 90:
            log(f"BOUNDARY PRIORITY CRITICAL {login}: {boundary} at {usage:.2f}% of limit. Highest scan priority.", "WARNING")

    # ==================== PHASE DAILY PROFIT CAP DISABLED 2026-09-30 ====================
    # Business instruction: Phase/Challenge accounts no longer have the former 5%
    # daily profit cap. Do not close or daily-lock Phase exposure based on daily profit.
    # All other target/pass, Funded 15% hard cap, payout protection and lifecycle logic
    # below remains unchanged.

    # Highest equity is the official NairaPips pass/progress evidence.
    highest_equity = phase_aware_existing_highest(trader, equity, reference_balance, phase_label)
    if funded_cycle_just_released:
        # Old-cycle peak must not instantly retrigger the 30% cap after payout.
        highest_equity = round(max(reference_balance, equity), 2)
        log(f"FUNDED CYCLE BASELINE RESET for {login}: start={reference_balance}, equity={equity}.", "WARNING")
    # Production DD evidence: keep the lowest balance/equity for the CURRENT active account.
    # Static DD proof must survive recovery, so lowest_equity stores the lowest risk value observed.
    existing_lowest_equity = to_float(trader.get("lowest_equity") or 0, 0)
    lowest_candidates = [float(equity or 0), float(mt5_balance or 0), float(reference_balance or 0)]
    if existing_lowest_equity > 0:
        lowest_candidates.append(existing_lowest_equity)
    lowest_equity = round(min(v for v in lowest_candidates if v > 0), 2) if any(v > 0 for v in lowest_candidates) else 0.0

    # NairaPips STATIC DD evidence:
    # current_static_dd_percent protects the account right now.
    # worst_static_dd_percent shows whether the account has ever gone below starting balance
    # while monitoring was active. This is not trailing DD; it is still measured from the
    # original account size.
    worst_static_drawdown_percent = calculate_drawdown(reference_balance, lowest_equity)
    worst_dd_limit_used_percent = calculate_dd_limit_used(worst_static_drawdown_percent, dd_limit_percent)
    current_dd_remaining_percent = round(max(dd_limit_percent - drawdown, 0), 2)
    worst_dd_remaining_percent = round(max(dd_limit_percent - worst_static_drawdown_percent, 0), 2)

    highest_profit = highest_equity - reference_balance
    highest_profit_percent = round((highest_profit / reference_balance) * 100, 2) if reference_balance > 0 else 0
    target_equity = reference_balance * (1 + (target_percent / 100)) if target_enabled and reference_balance > 0 else 0

    # NairaPips production pass meter: progress is percentage-of-target, based on
    # the active account's highest equity for the current stage. This is display
    # evidence only; pass still requires target_hit below.
    pass_progress_percent = 0.0
    pass_remaining_percent = 0.0
    if target_enabled and target_percent and reference_balance > 0:
        pass_progress_percent = round(max(0.0, (highest_profit_percent / target_percent) * 100), 2)
        pass_remaining_percent = round(max(0.0, 100.0 - pass_progress_percent), 2)

    # Store/report progress from highest equity so Phase 1/2 cannot be lost after equity drops.
    profit = highest_profit
    profit_percent = highest_profit_percent

    # Critical rule order:
    # 1. If the phase target has ever been touched by highest equity, the trader PASSED.
    # 2. If target was not touched and current equity breaches max DD, the trader BREACHED.
    # This keeps breach protection intact while preventing a valid pass from being shown as critical/breached.
    target_hit = target_enabled and reference_balance > 0 and highest_equity >= target_equity

    # GLOBAL PASS SAFETY RULE:
    # Phase 2 pass is account-level highest-equity evidence for the ACTIVE Phase 2 account.
    # If the account ever touched 8%, it must be detected and locked for admin review,
    # even if equity later pulls back before the dashboard refreshes.
    if phase_label == "phase2":
        phase2_high_profit_percent = round(((highest_equity - reference_balance) / reference_balance) * 100, 2) if reference_balance > 0 else 0
        target_hit = highest_equity >= target_equity and phase2_high_profit_percent >= target_percent

    # Static DD breach: once either MT5 balance OR equity touches breach level, account is breached.
    breached_by_balance = dd_authority_present and reference_balance > 0 and mt5_balance <= breach_equity_level
    breached_by_equity = dd_authority_present and reference_balance > 0 and equity <= breach_equity_level
    breached = breached_by_balance or breached_by_equity

    # DD AUTHORITY HAND-OFF.  Never close, archive, rotate or send a DD breach from
    # this engine.  If this slower operations scan happens to observe a crossed DD
    # boundary, stop processing this account immediately so it cannot be passed or
    # otherwise lifecycle-mutated while the dedicated Breach Guard owns the event.
    if DEDICATED_DD_GUARD_OWNS_BREACH and breached:
        breach_source_now = "balance" if breached_by_balance else "equity"
        log(
            f"DD-GUARD DEFER {login}: {breach_source_now} crossed {dd_limit_percent:g}% DD "
            f"(balance={mt5_balance}, equity={equity}, breach_level={breach_equity_level}). "
            "No DD close/backend write from old engine; dedicated Breach Guard owns this event.",
            "WARNING",
        )
        return "dd_guard_deferred"

    # ==================== TARGET-FIRST BUSINESS PROTECTION ====================
    # Once exact target authority exists and the account has touched the target,
    # close broker exposure before slow snapshot/API/email work. This prevents a
    # passed challenge from continuing to trade while paperwork is being written.
    pass_closed_early = False
    if target_hit and not breached:
        pass_reason = (
            f"TARGET_FIRST_PASS_{target_percent:g}_PERCENT: highest equity {highest_equity} "
            f"touched target equity {target_equity} from reference {reference_balance}."
        )
        log(
            f"TARGET-FIRST INTERRUPT for {login}: confirmed exact target hit. "
            "Closing broker exposure BEFORE paperwork.",
            "WARNING",
        )
        PERSISTENT_LOCKED_LOGINS.add(str(login))
        if close_allowed("target"):
            pass_closed_early = master_login_and_close(login, server, master_password, pass_reason)
            if pass_closed_early:
                log(f"TARGET-FIRST ZERO-EXPOSURE PROTECTION COMPLETE for {login}.", "WARNING")
            else:
                log(
                    f"CRITICAL TARGET-FIRST CLOSE FAILED for {login}. "
                    "Paperwork will continue and the normal close stage will retry.",
                    "ERROR",
                )
        else:
            log(f"CRITICAL: target auto-close is disabled for {login}.", "ERROR")

    # ==================== BREACH-FIRST BUSINESS PROTECTION ====================
    # A confirmed NairaPips static-DD breach is an emergency broker action.
    # NOTHING slow is allowed to run before exposure is killed:
    # no trade/history sync, rule-history scan, snapshot, email, or backend paperwork.
    #
    # We already have verified live MT5 account_info() for the exact target login,
    # so this is not a guessed/stale breach. Close positions + pending orders now,
    # verify zero exposure, then continue below to preserve evidence and lifecycle records.
    breach_closed_early = False
    if breached:
        breach_source_now = "balance" if breached_by_balance else "equity"
        emergency_reason = (
            f"BREACH_FIRST_STATIC_DD_{dd_limit_percent:g}_PERCENT: {breach_source_now} crossed "
            f"breach level {breach_equity_level}. MT5 balance={mt5_balance}, equity={equity}, "
            f"reference={reference_balance}."
        )
        log(
            f"BREACH-FIRST INTERRUPT for {login}: confirmed static DD breach. "
            "Closing broker exposure BEFORE paperwork.",
            "ERROR",
        )
        PERSISTENT_LOCKED_LOGINS.add(str(login))
        if close_allowed("breach"):
            breach_closed_early = master_login_and_close(
                login, server, master_password, emergency_reason
            )
            if breach_closed_early:
                log(
                    f"BREACH-FIRST ZERO-EXPOSURE PROTECTION COMPLETE for {login}. "
                    "Proceeding to evidence/snapshot/backend lock.",
                    "WARNING",
                )
            else:
                log(
                    f"CRITICAL BREACH-FIRST CLOSE FAILED for {login}. "
                    "Paperwork will continue and the normal close stage will retry. "
                    "Manual broker action may be required immediately.",
                    "ERROR",
                )
        else:
            log(
                f"CRITICAL: breach auto-close is disabled for {login}. "
                "Backend paperwork will continue, but broker exposure remains at risk.",
                "ERROR",
            )

    # Automated-order enforcement remains active, but a confirmed DD breach always
    # gets broker-side closure priority before this secondary rule analysis.
    if (not breached) and AUTOMATED_ORDER_GUARD_ENABLED:
        automation_evidence = automated_order_evidence()
        if automation_evidence:
            reason = "AUTOMATED_ORDER_FINGERPRINT_DETECTED"
            if not AUTOMATED_ORDER_ENFORCEMENT_ENABLED:
                log(f"AUTOMATED ORDER EVIDENCE ONLY for {login}: {automation_evidence}. NO CLOSE / NO LOCK.", "WARNING")
            else:
                log(f"AUTOMATED ORDER GUARD TRIGGERED for {login}: {automation_evidence}", "WARNING")
                PERSISTENT_LOCKED_LOGINS.add(str(login))
                persistent_master_lockdown(login, server, master_password, reason)
            evidence_payload = {
                "rule_violation_detected": True,
                "rule_violation_policy": (
                    "Automated order fingerprint detected from a non-zero MT5 magic "
                    "number or explicit EA/bot/copy-trader order comment."
                ),
                "rule_violation_evidence": {"automated_orders": automation_evidence},
                "payout_blocked": True,
                "trading_must_remain_locked_until_review": True,
                "timestamp": now_iso(),
            }
            if AUTOMATED_ORDER_ENFORCEMENT_ENABLED:
                safe_disable_mt5_access(
                    trader, login, reason, "rule_violation_review", evidence_payload
                )
                if AUTOMATED_ORDER_LOCK_ACCOUNT:
                    rotate_master_password(login, server, master_password, "rule_violation_review")
                return "rule_violation_review"
            # Evidence-only mode continues normal DD/target monitoring without altering trading.

    funded_lock, funded_floor, funded_label, funded_ratio = funded_profit_protection(
        reference_balance,
        equity,
        current_profit,
        current_profit_percent
    ) if funded else (False, 0.0, "not_funded", 0.0)

    zone = determine_zone(drawdown, funded=funded, profit_percent=current_profit_percent, dd_limit_percent=dd_limit_percent)
    status = "active"
    reason = ""
    pending_lock_status = ""
    pending_lock_reason = ""
    pending_close_type = ""
    pending_close_reason = ""

    sync_open_trades(trader, login)

    rule_guard = detect_prohibited_trading_pattern(login)
    rule_violation_hit = (RULE_VIOLATION_ENFORCEMENT_ENABLED and bool(rule_guard.get("detected")) and should_enforce_rule_violation(login))

    if breached:
        zone = "breached"
        status = "breached"
        breach_source = "balance" if breached_by_balance else "equity"
        reason = (
            f"ACCOUNT BREACHED: static {dd_limit_percent:g}% DD exceeded by {breach_source}. "
            f"MT5 balance={mt5_balance}, equity={equity}, breach level={breach_equity_level}. "
            f"Reference={reference_balance}, balance DD={balance_drawdown}%, equity DD={equity_drawdown}%, max DD used={dd_limit_used}%."
        )
        log(reason, "WARNING")
        pending_close_type = "breach"
        pending_close_reason = f"ACCOUNT_LEVEL_MAX_DRAWDOWN_{dd_limit_percent:g}_PERCENT"
        pending_lock_status = "breached"
        pending_lock_reason = reason

    elif rule_violation_hit:
        zone = "rule_violation_review"
        status = "rule_violation_review"
        signals_text = ", ".join(rule_guard.get("signals") or [])
        reason = (
            "NAIRAPIPS TRADING RULE VIOLATION REVIEW: high-confidence prohibited "
            f"trading behaviour detected. Score={rule_guard.get('score')}. "
            f"Signals={signals_text}. Account locked, open positions closed and "
            "full trade evidence preserved for Admin review."
        )
        log(reason, "WARNING")
        pending_close_type = "rule_violation"
        pending_close_reason = "HIGH_CONFIDENCE_PROHIBITED_TRADING_PATTERN"
        pending_lock_status = "rule_violation_review"
        pending_lock_reason = reason

    elif target_hit:
        zone = "passed"
        status = phase_pass_status_for_label(phase_label)
        reason = (
            f"{phase_label.upper()} PASSED: highest equity {highest_equity} reached target equity {target_equity}. "
            f"Highest profit={highest_profit_percent}% target={target_percent}%. Account locked for admin review / next phase."
        )
        log(reason, "WARNING")
        # Snapshot first, then close/lock. This preserves evidence before action.
        pending_close_type = "pass"
        pending_close_reason = f"{phase_label.upper()}_PROFIT_TARGET_PASSED"
        pending_lock_status = status
        pending_lock_reason = reason

    elif funded_hard_cap_hit(reference_balance, current_profit_percent, highest_profit_percent, phase_label, funded_cap):
        zone = "funded_profit_cap"
        # Keep this established backend-compatible status. It now means:
        # funded cycle completed, payout required, same account waits for manual return to starting capital.
        status = "funded_profit_cap_reached"
        cycle_profit_amount = round(reference_balance * (funded_cap / 100.0), 2)
        trader_share_amount = round(cycle_profit_amount * (FUNDED_TRADER_SHARE_PERCENT / 100.0), 2)
        nairapips_share_amount = round(cycle_profit_amount * (FUNDED_NAIRAPIPS_SHARE_PERCENT / 100.0), 2)
        reason = (
            f"FUNDED PROFIT CYCLE COMPLETED: profit reached/exceeded {funded_cap}%. "
            f"Current profit={current_profit_percent}%, highest profit={highest_profit_percent}%. "
            f"Payout is required before trading can resume. Trader share={FUNDED_TRADER_SHARE_PERCENT}% "
            f"({trader_share_amount}); NairaPips share={FUNDED_NAIRAPIPS_SHARE_PERCENT}% "
            f"({nairapips_share_amount}). The SAME MT5 account must be manually returned to "
            f"starting capital {reference_balance} after payout. No new account must be created."
        )
        log(reason, "WARNING")
        pending_close_type = "funded_cap"
        pending_close_reason = "FUNDED_CYCLE_CONFIGURED_CAP_PAYOUT_REQUIRED"
        pending_lock_status = "funded_profit_cap_reached"
        PERSISTENT_LOCKED_LOGINS.add(str(login))
        pending_lock_reason = reason

    elif funded_lock:
        zone = "profit_protected"
        status = "profit_protected"
        reason = (
            f"FUNDED PROFIT PROTECTION HIT: equity {equity} <= protected floor {funded_floor}. "
            f"Profit={profit_percent}%, protection={round(funded_ratio * 100)}% of profit."
        )
        log(reason, "WARNING")
        pending_close_type = "profit_protection"
        pending_close_reason = "FUNDED_HYBRID_PROFIT_PROTECTION"
        pending_lock_status = "profit_protected"
        pending_lock_reason = reason

    payload = {
        "trader_id": trader_id,
        "trader_account_id": trader.get("trader_account_id") or trader.get("current_account_id"),
        "mt5_login": login,
        "balance": reference_balance,
        "equity": equity,
        "profit": profit,
        "profit_percent": profit_percent,
        "current_profit": current_profit,
        "current_profit_percent": current_profit_percent,
        "profit_target": target_percent,
        "target_enabled": target_enabled,
        "phase_label": phase_label,
        "highest_equity": highest_equity,
        "lowest_equity": lowest_equity,
        "highest_profit": highest_profit,
        "highest_profit_percent": highest_profit_percent,
        "target_equity": target_equity,
        "pass_progress_percent": pass_progress_percent,
        "pass_remaining_percent": pass_remaining_percent,
        "static_dd_limit_percent": dd_limit_percent,
        "dd_limit_percent": dd_limit_percent,
        # Do not forward stale pass flags when the current scan has not actually hit target.
        "phase_pass_status": status if target_hit else "",
        # LIVE BREACH METER SOURCE OF TRUTH:
        # drawdown/dd_used are calculated from the WORST of MT5 balance or equity vs fixed account size.
        # highest/lowest equity are evidence only; they must not hide current danger.
        "drawdown": drawdown,
        "drawdown_percent": drawdown,
        "actual_drawdown_percent": drawdown,
        "current_drawdown_percent": drawdown,
        "balance_drawdown_percent": balance_drawdown,
        "equity_drawdown_percent": equity_drawdown,
        "breach_evidence_value": breach_evidence_value,
        "breached_by_balance": breached_by_balance,
        "breached_by_equity": breached_by_equity,
        "dd_used_percent": dd_limit_used,
        "max_drawdown_used": dd_limit_used,
        "max_dd_used": dd_limit_used,
        "current_dd_used_percent": dd_limit_used,
        "drawdown_amount": round(max(reference_balance - equity, 0), 2),
        "dd_remaining_percent": current_dd_remaining_percent,
        "worst_static_drawdown_percent": worst_static_drawdown_percent,
        "worst_dd_used_percent": worst_dd_limit_used_percent,
        "worst_dd_remaining_percent": worst_dd_remaining_percent,
        "recorded_lowest_equity": lowest_equity,
        "breach_equity_level": breach_equity_level,
        "funded_profit_hard_cap_percent": float(funded_cap) if funded_cap is not None else None,
        "funded_profit_hard_cap_hit": status == "funded_profit_cap_reached",
        "funded_cycle_complete": status == "funded_profit_cap_reached",
        "payout_required": status == "funded_profit_cap_reached",
        "trading_must_remain_locked_until_payout": status == "funded_profit_cap_reached",
        "reuse_same_mt5_account": status == "funded_profit_cap_reached",
        "create_new_mt5_account": False,
        "return_to_starting_capital_after_payout": status == "funded_profit_cap_reached",
        "funded_cycle_starting_capital": reference_balance,
        "funded_cycle_profit_amount": round(reference_balance * (funded_cap / 100.0), 2) if status == "funded_profit_cap_reached" else 0,
        "funded_trader_share_percent": FUNDED_TRADER_SHARE_PERCENT,
        "funded_nairapips_share_percent": FUNDED_NAIRAPIPS_SHARE_PERCENT,
        "funded_trader_share_amount": round(reference_balance * (funded_cap / 100.0) * (FUNDED_TRADER_SHARE_PERCENT / 100.0), 2) if status == "funded_profit_cap_reached" else 0,
        "funded_nairapips_share_amount": round(reference_balance * (funded_cap / 100.0) * (FUNDED_NAIRAPIPS_SHARE_PERCENT / 100.0), 2) if status == "funded_profit_cap_reached" else 0,
        "funded_profit_floor": funded_floor,
        "funded_profit_label": funded_label,
        "funded_profit_protection_ratio": funded_ratio,
        "zone": zone,
        "breached": breached,
        "status": status,
        "priority_label": priority_label_from_metrics(dd_limit_used, pass_progress_percent, breached=breached, passed=target_hit),
        "monitoring_priority": "closed" if breached or status in {"funded_profit_cap_reached", "rule_violation_review"} else ("passed" if target_hit else ("urgent" if dd_limit_used >= 75 or pass_progress_percent >= 80 else "active")),
        "breach_source": ("balance" if breached_by_balance else ("equity" if breached_by_equity else "")),
        "breach_evidence_value": breach_evidence_value,
        "mt5_balance": mt5_balance,
        "current_balance": mt5_balance,
        "start_balance": reference_balance,
        "account_size": reference_balance,
        "reason": reason if (
            breached or target_hit or funded_lock
            or status in {"funded_profit_cap_reached", "rule_violation_review"}
        ) else "",
        "rule_violation_detected": status == "rule_violation_review",
        "rule_violation_score": int(rule_guard.get("score") or 0),
        "rule_violation_signals": rule_guard.get("signals") or [],
        "rule_violation_evidence": rule_guard.get("evidence") or {},
        "rule_violation_policy": rule_guard.get("policy") or "",
        "rule_violation_detected_at": now_iso() if status == "rule_violation_review" else "",
        "timestamp": now_iso(),
    }

    if should_send_snapshot(login, status, zone):
        send_snapshot(payload)
    else:
        log(f"Snapshot suppressed for {login}: cooldown active for status={status} zone={zone}")

    # Non-breach actions may reach this section after snapshot/evidence. REAL breach closure is already handled earlier by the BREACH-FIRST interrupt before paperwork.
    if pending_close_type and pending_close_reason:
        if pending_close_type == "breach" and breach_closed_early:
            log(
                f"BREACH close stage already completed before paperwork for {login}; "
                "no duplicate broker close required.",
                "WARNING",
            )
        elif target_hit and pass_closed_early:
            log(
                f"TARGET close stage already completed before paperwork for {login}; "
                "no duplicate broker close required.",
                "WARNING",
            )
        elif close_allowed(pending_close_type):
            closed_ok = master_login_and_close(login, server, master_password, pending_close_reason)
            if not closed_ok:
                log(f"TRADE CLOSE ATTEMPT FAILED for {login}. Backend lock will still continue. Check master password/server immediately.", "ERROR")
        else:
            log(f"AUTO CLOSE DISABLED for {login}/{pending_close_type}. Backend lock will continue.", "WARNING")

    if pending_lock_status and pending_lock_reason:
        safe_disable_mt5_access(trader, login, pending_lock_reason, pending_lock_status, payload)
        # PRODUCTION: Rotate the trader's master password so they cannot place new trades
        # after the engine closes existing positions. This is the broker-side stop.
        if pending_lock_status in {"breached", "phase1_passed", "phase2_passed", "funded_profit_cap_reached", "profit_protected", "target_hit", "rule_violation_review"}:
            rotate_master_password(login, server, master_password, pending_lock_status)

    target_source = str(trader.get("target_authority_source") or ("exact_account" if target_enabled else "funded"))
    dd_source = str(dd_authority_source or "authority_missing")
    target_log = (
        f"Target={target_percent}%({target_source})" if target_enabled
        else "Target=OFF_FUNDED" if funded
        else "Target=AUTHORITY_MISSING"
    )
    dd_log = f"DDLimit={dd_limit_percent}%({dd_source})" if dd_authority_present else "DDLimit=AUTHORITY_MISSING"
    funded_log = f"FundedFloor={funded_floor} ({funded_label})" if funded else "FundedFloor=N/A"

    log(
        f"Synced {login} | Phase={phase_label} | Reference={reference_balance} | "
        f"BreachLevel={breach_equity_level} | MT5Balance={mt5_balance} | Equity={equity} | CurrentProfit={current_profit_percent}% | StoredProgress={profit_percent}% | "
        f"PhaseHighEquity={highest_equity} | PhaseHighProfit={highest_profit_percent}% | "
        f"{target_log} | {dd_log} | {funded_log} | Zone={zone} | CurrentStaticDD={drawdown}% | CurrentDDLimitUsed={dd_limit_used}% | "
        f"WorstStaticDD={worst_static_drawdown_percent}% | WorstDDLimitUsed={worst_dd_limit_used_percent}% | "
        f"DDRemaining={current_dd_remaining_percent}% | BreachLevel={breach_equity_level}"
    )

    return zone



def update_live_boundary_priority(login, dd_used=0.0, phase_daily_pct=None, pass_progress=0.0, funded_profit_pct=None, cap_percent=15):
    """Remember the closest live boundary so that account is scanned first next pass."""
    candidates = []
    dd = max(0.0, float(dd_used or 0.0))
    candidates.append((dd, "DD"))  # already expressed as % of allowed DD consumed
    # phase_daily_pct intentionally ignored: Phase daily-profit cap was retired 2026-09-30.
    if pass_progress:
        candidates.append((max(0.0, float(pass_progress)), "PHASE_TARGET"))
    if funded_profit_pct is not None and cap_percent is not None:
        candidates.append((max(0.0, float(funded_profit_pct)) / float(cap_percent) * 100.0, "FUNDED_CAP"))
    usage, label = max(candidates, key=lambda x: x[0]) if candidates else (0.0, "NORMAL")
    # Large base makes live evidence outrank stale backend evidence.
    score = 2_000_000.0 + min(usage, 200.0) * 10_000.0
    LIVE_BOUNDARY_PRIORITY[str(login)] = {"score": score, "label": label, "usage": round(usage, 2), "updated_at": _now_ts()}
    return usage, label


def _num_field(row, keys, default=0.0):
    for k in keys:
        try:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return to_float(v, default)
        except Exception:
            pass
    return default


def risk_priority_score(row):
    """Sort accounts so urgent risk accounts are scanned first.

    This is based on the LAST stored monitoring evidence before the new scan:
    - breached/locked first
    - accounts near 20% static DD next
    - accounts close to phase target next
    - normal safe accounts last
    It works across all account sizes because it uses percentages, not naira amounts.
    """
    status_blob = " ".join([
        str(row.get("status") or ""),
        str(row.get("phase") or ""),
        str(row.get("risk_zone") or row.get("zone") or ""),
        str(row.get("account_status") or ""),
        str(row.get("phase_pass_status") or ""),
    ]).lower()

    live = LIVE_BOUNDARY_PRIORITY.get(str(row.get("mt5_login") or "")) or {}
    # Live boundary evidence from the previous successful scan has priority over stale API rows.
    # Expire after 10 minutes so an old observation cannot dominate forever.
    if live and (_now_ts() - float(live.get("updated_at") or 0) <= 600):
        return float(live.get("score") or 0)

    if "rule_violation" in status_blob:
        return 1_100_000
    if "breach" in status_blob or "locked" in status_blob:
        return 1_000_000
    if "passed" in status_blob or "target_hit" in status_blob:
        return 900_000

    dd_used = _num_field(row, [
        "dd_used_percent", "max_drawdown_used", "max_dd_used",
        "worst_dd_used_percent", "current_dd_used_percent"
    ], 0)
    drawdown = _num_field(row, [
        "drawdown_percent", "actual_drawdown_percent", "current_drawdown_percent",
        "worst_static_drawdown_percent", "absolute_drawdown_percent"
    ], 0)
    if not dd_used and drawdown:
        dd_used = calculate_dd_limit_used(drawdown, get_account_dd_limit(row))

    profit_percent = _num_field(row, ["highest_profit_percent", "profit_percent", "current_profit_percent"], 0)
    target_enabled, target, _priority_phase = get_phase_target(row)
    if not target_enabled:
        target = 0.0
    pass_progress = (profit_percent / target * 100.0) if target else 0.0

    # DD risk is more dangerous than pass progress.
    return (dd_used * 1000.0) + (pass_progress * 10.0)


def priority_label_from_metrics(dd_used, pass_progress, breached=False, passed=False):
    if breached:
        return "BREACH_NOW"
    if passed:
        return "PASS_NOW"
    if dd_used >= 90:
        return "BREACH_CRITICAL"
    if dd_used >= 75:
        return "BREACH_DANGER"
    if pass_progress >= 95:
        return "PASS_CRITICAL"
    if pass_progress >= 80:
        return "PASS_NEAR"
    if dd_used >= 50:
        return "RISK_WARNING"
    return "NORMAL"


def service_hard_locked_watchdogs():
    """Absolute-priority zero-exposure sweep for every in-memory hard lock.

    This is intentionally independent of the normal fair/priority discovery queue.
    It never releases a lock by itself; release remains controlled by process_account
    after the backend presents the exact authorized active-cycle state.
    """
    if not PERSISTENT_LOCKED_LOGINS:
        return 0
    protected = 0
    for login_text in list(PERSISTENT_LOCKED_LOGINS):
        trader = LOCKED_ACCOUNT_CACHE.get(str(login_text)) or {}
        server = trader.get("mt5_server")
        master = get_master_password(trader) if trader else ""
        if not server or not master:
            log(
                f"HARD WATCHDOG cannot service {login_text}: cached server/master credential unavailable. "
                "Lock remains active; waiting for a fresh authoritative row.",
                "ERROR",
            )
            continue
        log(f"HARD PAYOUT WATCHDOG SWEEP {login_text}: enforcing zero exposure", "WARNING")
        ok = persistent_master_lockdown(int(login_text), server, master, "HARD_PAYOUT_LOCK_SWEEP")
        if ok:
            protected += 1
        time.sleep(WATCHDOG_SWEEP_SLEEP_SECONDS)
    return protected

def main():
    log(f"Starting NairaPips MT5 PROP-FIRM PRODUCTION PROTECTION ENGINE... API={API_BASE_URL}")
    log("DD AUTHORITY = EXTERNAL DEDICATED BREACH GUARD | old engine DD close/persist disabled", "WARNING")
    log("PRODUCTION RETRY = ON | exact-login settle + reconnect | freshness failures never quarantined", "WARNING")
    log(r"OPERATIONS TERMINAL = C:\Program Files\MetaTrader 5\terminal64.exe | isolated from Breach Guard terminal", "WARNING")

    if not initialize_mt5():
        return

    try:
        while True:
            traders = get_traders()
            # Production priority queue: keep the highest-risk accounts in every cycle,
            # but rotate the remaining slots so NO active account can starve forever.
            # The previous hard slice (traders[:MAX_ACCOUNTS_PER_CYCLE]) could permanently
            # exclude newer/low-stale-risk accounts once the portfolio grew past the cap.
            traders = sorted(traders, key=risk_priority_score, reverse=True)
            discovered_count = len(traders)
            if MAX_ACCOUNTS_PER_CYCLE > 0 and discovered_count > MAX_ACCOUNTS_PER_CYCLE:
                global FAIR_SCAN_CURSOR
                cap = max(1, int(MAX_ACCOUNTS_PER_CYCLE))
                # Reserve 25% (minimum 10, maximum cap) for the highest current risk/pass rows.
                priority_slots = min(cap, max(10, cap // 4))
                priority_rows = traders[:priority_slots]
                rotating_pool = traders[priority_slots:]
                rotating_slots = max(0, cap - len(priority_rows))

                rotating_rows = []
                if rotating_pool and rotating_slots:
                    start = FAIR_SCAN_CURSOR % len(rotating_pool)
                    ordered_pool = rotating_pool[start:] + rotating_pool[:start]
                    rotating_rows = ordered_pool[:rotating_slots]
                    FAIR_SCAN_CURSOR = (start + rotating_slots) % len(rotating_pool)

                # De-duplicate by exact account id/login while preserving priority order.
                selected = []
                seen = set()
                for row in priority_rows + rotating_rows:
                    key = str(row.get("trader_account_id") or row.get("id") or row.get("mt5_login") or "")
                    if key and key in seen:
                        continue
                    if key:
                        seen.add(key)
                    selected.append(row)
                traders = selected
                log(
                    f"Fair account cycle: {discovered_count} found, scanning {len(traders)} this cycle "
                    f"({len(priority_rows)} priority + {len(rotating_rows)} rotating). "
                    f"No-starvation cursor={FAIR_SCAN_CURSOR}.",
                    "WARNING",
                )
            log(f"Loaded {len(traders)} of {discovered_count} monitorable/locked traders. Priority + fair rotation active.")

            if not traders:
                log("No monitorable traders found. Waiting 20 seconds.", "WARNING")
                time.sleep(20)
                continue

            fastest_scan = 20

            # HARD LOCKS ALWAYS COME FIRST. They are serviced outside the normal
            # 79-account rotation so a payout-locked trader cannot wait a full cycle.
            if PERSISTENT_LOCKED_LOGINS:
                service_hard_locked_watchdogs()
                fastest_scan = min(fastest_scan, 1)

            # Re-sort with fresh in-memory boundary evidence before processing this cycle.
            traders = sorted(traders, key=risk_priority_score, reverse=True)
            for account_index, trader in enumerate(traders, start=1):
                zone = process_account(trader)
                interval = SCAN_SECONDS.get(zone, 20)
                if interval < fastest_scan:
                    fastest_scan = interval

                # Do not let a long portfolio rotation delay payout/breach locks.
                # Re-sweep hard locks after a small number of ordinary accounts.
                if PERSISTENT_LOCKED_LOGINS and account_index % WATCHDOG_NORMAL_ACCOUNTS_BETWEEN_SWEEPS == 0:
                    service_hard_locked_watchdogs()
                    fastest_scan = min(fastest_scan, 1)

            # Immediate hot-account pass: do not make accounts already near DD, Phase 5%,
            # Phase target, or Funded 15% wait behind the whole normal portfolio again.
            hot = []
            now_ts = _now_ts()
            for trader in traders:
                login = str(trader.get("mt5_login") or "")
                state = LIVE_BOUNDARY_PRIORITY.get(login) or {}
                if state and now_ts - float(state.get("updated_at") or 0) <= 600 and float(state.get("usage") or 0) >= 80.0:
                    hot.append(trader)
            hot = sorted(hot, key=risk_priority_score, reverse=True)[:HOT_RESCAN_MAX]
            if hot:
                labels = [f"{t.get('mt5_login')}:{(LIVE_BOUNDARY_PRIORITY.get(str(t.get('mt5_login') or '')) or {}).get('label')}" for t in hot]
                log(f"HOT BOUNDARY RESCAN: {len(hot)} account(s) -> {', '.join(labels)}", "WARNING")
                for trader in hot:
                    zone = process_account(trader)
                    interval = SCAN_SECONDS.get(zone, 20)
                    if interval < fastest_scan:
                        fastest_scan = interval
                if any(float((LIVE_BOUNDARY_PRIORITY.get(str(t.get('mt5_login') or '')) or {}).get('usage') or 0) >= 90.0 for t in hot):
                    fastest_scan = min(fastest_scan, HOT_CRITICAL_SLEEP_SECONDS)

            if PERSISTENT_LOCKED_LOGINS:
                service_hard_locked_watchdogs()
                fastest_scan = min(fastest_scan, 1)

            log(
                f"Next scan in {fastest_scan} seconds. Live boundary priority active. "
                f"Hard payout watchdogs={len(PERSISTENT_LOCKED_LOGINS)}."
            )
            time.sleep(fastest_scan)

    except KeyboardInterrupt:
        log("Monitor stopped by user.")
    finally:
        shutdown_mt5()


if __name__ == "__main__":
    main()
