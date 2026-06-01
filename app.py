
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client
from werkzeug.utils import secure_filename
from datetime import datetime, timezone
import os, random, uuid, re, time
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

    res = supabase.table("traders").select("*").execute()
    rows = getattr(res, "data", []) or []

    matches = []
    for t in rows:
        keys = [
            str(t.get("email") or "").strip().lower(),
            str(t.get("phone") or "").strip().lower(),
            str(t.get("mt5_login") or "").strip().lower(),
            str(t.get("account_reference") or "").strip().lower(),
            str(t.get("id") or "").strip().lower(),
        ]
        if lookup in keys:
            matches.append(t)

    if not matches:
        return None

    return sorted(matches, key=_row_score, reverse=True)[0]

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

@app.route("/update_trader_mt5", methods=["POST", "OPTIONS"])
@app.route("/reset_trader_mt5", methods=["POST", "OPTIONS"])
def update_trader_mt5():
    if request.method == "OPTIONS":
        return _np_ok({})

    data = request.get_json(silent=True) or {}
    trader_id = data.get("id") or data.get("trader_id")
    if not trader_id:
        return _np_fail("Trader ID is required")

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



FROM_EMAIL = os.getenv("FROM_EMAIL") or "support@nairapips.com"
ADMIN_ALERT_EMAIL = os.getenv("ADMIN_ALERT_EMAIL") or FROM_EMAIL
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

def text_to_html_content(message):
    return "<p>" + html.escape(str(message or "")).replace("\n", "<br>") + "</p>"

def send_email_brevo(to_email, subject, html_content):
    try:
        if not to_email:
            return False
        if not BREVO_API_KEY or not FROM_EMAIL:
            print("BREVO EMAIL ERROR:", "BREVO_API_KEY or FROM_EMAIL is missing")
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
            print("BREVO EMAIL ERROR:", res.status_code, res.text[:500])
            return False

        print("BREVO EMAIL SENT:", to_email)
        return True
    except Exception as e:
        print("BREVO EMAIL ERROR:", str(e))
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

        if not _valid_name(name):
            return bad("Please enter your real full name.")
        if not email and not phone:
            return bad("Email or phone is required")
        if not _valid_email(email):
            return bad("Please enter a valid email address.")
        if not _valid_phone(phone):
            return bad("Please enter a valid WhatsApp or phone number.")

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
        }

        created = _safe_insert_trader(row)
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

        allowed = [
            "name", "phone", "email", "status", "phase", "balance", "equity",
            "profit", "drawdown", "profit_percent", "drawdown_percent",
            "engine_group", "payment_status", "payment_note", "admin_note",
            "trading_days_left", "selected_plan", "account_size",
            "mt5_login", "mt5_server", "mt5_master_password",
            "mt5_investor_password", "mt5_password", "master_password",
            "investor_password", "mt5_updated_by", "mt5_reset_reason",
            "trader_note", "mt5_notice"
        ]
        upd = {k: d[k] for k in allowed if k in d}

        for money_key in ["account_size", "balance", "equity"]:
            if money_key in upd:
                upd[money_key] = clean(upd[money_key])

        if any(k in upd for k in ["mt5_login", "mt5_server", "mt5_password", "master_password", "mt5_master_password"]):
            upd["mt5_updated_at"] = now_iso()

        upd["updated_at"] = now_iso()

        if not upd:
            return bad("Nothing to update")

        result = supabase.table("traders").update(upd).eq("id", tid).execute().data
        trader_row = result[0] if result else get_trader_by_id(tid)

        if any(k in upd for k in ["mt5_login", "mt5_server", "mt5_password", "master_password", "mt5_master_password"]):
            send_mt5_reset_email(
                trader_row,
                upd.get("mt5_login", ""),
                upd.get("mt5_server", ""),
                upd.get("mt5_master_password") or upd.get("mt5_password") or upd.get("master_password") or "",
                upd.get("mt5_investor_password") or upd.get("investor_password") or "",
                upd.get("mt5_reset_reason") or upd.get("admin_note") or "MT5 login details updated"
            )

        if str(upd.get("status", "")).lower() in ["reset", "account_reset"] or str(upd.get("phase", "")).lower() in ["reset", "account_reset"]:
            send_account_status_email(
                trader_row,
                "NairaPips account reset completed",
                "Your NairaPips trading account has been reset.",
                upd.get("admin_note") or "You can log in to your trader dashboard to review the updated account status."
            )

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
        lookup = str((request.json or {}).get("lookup", "")).strip().lower()
        if not lookup:
            return bad("Missing lookup")
        trader = _latest_trader_for_lookup(lookup)
        if not trader:
            return bad("Trader not found", 404)
        t = now_iso()
        supabase.table("traders").update({"last_login_at": t}).eq("id", trader["id"]).execute()
        trader["last_login_at"] = t
        return ok(trader, "Login successful")
    except Exception as e:
        return bad(e)

@app.route("/approve_payment", methods=["POST"])
def approve_payment():
    try:
        d=request.json or {}; tid=d.get("id")
        if not tid: return bad("Missing trader id")
        required=["mt5_login","mt5_server","mt5_master_password","mt5_investor_password"]
        if any(not str(d.get(x,"")).strip() for x in required): return bad("All MT5 credentials are required")
        upd={k:str(d.get(k,"")).strip() for k in required}
        upd.update({"payment_status":"approved","status":"active","phase":d.get("phase","phase1"),"approved_at":now_iso(),
                    "challenge_started_at":now_iso(),"approved_by":d.get("approved_by","admin"),"admin_note":d.get("admin_note","")})
        if d.get("balance") or d.get("account_size"):
            bal=clean(d.get("balance") or d.get("account_size")); upd.update({"account_size":bal,"balance":bal,"equity":bal})
        result = supabase.table("traders").update(upd).eq("id",tid).execute().data
        trader_row = result[0] if result else _get_trader_by_id(tid) or {}

        send_email_safe(
            trader_row.get("email"),
            "NairaPips payment approved - MT5 details",
            f"""Hello {trader_row.get("name") or "Trader"},

Your NairaPips payment has been approved and your MT5 account has been activated.

MT5 Login: {upd.get("mt5_login", "")}
Server: {upd.get("mt5_server", "")}
Master Password: {upd.get("mt5_master_password", "")}
Investor Password: {upd.get("mt5_investor_password", "")}

NairaPips Team"""
        )

        _audit_safe("payments", "payment_approved", f"Trader {tid} payment approved", _admin_from_payload(d))
        return ok(result, "Payment approved")
    except Exception as e: return bad(e)

@app.route("/reject_payment", methods=["POST"])
def reject_payment():
    try:
        d=request.json or {}; tid=d.get("id")
        if not tid: return bad("Missing trader id")
        trader_row = _get_trader_by_id(tid) or {}
        note = d.get("admin_note","")
        result = supabase.table("traders").update({"payment_status":"rejected","status":"payment_rejected","admin_note":note}).eq("id",tid).execute().data

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
        allowed=["status","phase","balance","equity","profit","drawdown","profit_percent","drawdown_percent","engine_group","payment_status","payment_note","admin_note","trading_days_left","lead_status","follow_up_at"]
        upd={k:d[k] for k in allowed if k in d}
        if d.get("phase")=="funded" or d.get("status")=="funded": upd["funded_at"]=now_iso()
        if not upd: return bad("Nothing to update")
        try:
            result = supabase.table("traders").update(upd).eq("id",tid).execute().data
        except Exception as update_error:
            if "lead_status" in upd or "follow_up_at" in upd:
                return bad("Lead status columns are missing. Run the Step 2 launch SQL for traders.lead_status and traders.follow_up_at.", 500)
            raise update_error
        trader_row = result[0] if result else get_trader_by_id(tid)
        status = str(upd.get("status") or "").lower()
        phase = str(upd.get("phase") or "").lower()
        if status in ["reset", "account_reset"] or phase in ["reset", "account_reset"]:
            send_account_status_email(
                trader_row,
                "NairaPips account reset completed",
                "Your NairaPips trading account has been reset.",
                upd.get("admin_note") or "You can log in to your trader dashboard to review the updated account status."
            )
        if status in ["funded", "passed", "phase2_passed"] or phase in ["funded", "passed", "phase2_passed"]:
            send_challenge_certificate_email(
                trader_row,
                upd.get("admin_note") or f"Current status: {upd.get('status', trader_row.get('status', 'updated'))}. Current phase: {upd.get('phase', trader_row.get('phase', 'updated'))}."
            )
            send_admin_alert(
                "NairaPips challenge passed certificate earned",
                f"""A trader challenge status was marked as passed/funded.

Trader: {trader_row.get("name") if trader_row else ""}
Email: {trader_row.get("email") if trader_row else ""}
Status: {upd.get("status", trader_row.get("status", ""))}
Phase: {upd.get("phase", trader_row.get("phase", ""))}
Note: {upd.get("admin_note") or "Challenge pass/certificate status updated."}"""
            )
        _audit_safe("traders", "trader_status_update", f"Trader {tid} status update: {upd}", _admin_from_payload(d))
        return ok(result)
    except Exception as e: return bad(e)

@app.route("/activate_trader", methods=["POST"])
def activate_trader():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing trader id")
        result = supabase.table("traders").update({"status":"active"}).eq("id",tid).execute().data
        _audit_safe("traders", "trader_activated", f"Trader {tid} activated", _admin_from_payload(request.json or {}))
        return ok(result)
    except Exception as e: return bad(e)

@app.route("/deactivate_trader", methods=["POST"])
def deactivate_trader():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing trader id")
        return ok(supabase.table("traders").update({"status":"inactive"}).eq("id",tid).execute().data)
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
        ref_code = str(d.get("referral_code") or d.get("ref_code") or d.get("affiliate_code") or "").strip().lower()
        own_ref_codes = [str(d.get(k) or "").strip().lower() for k in ["own_referral_code", "my_referral_code", "personal_referral_code"]]
        if ref_code and ref_code in [x for x in own_ref_codes if x]:
            return bad("Self-referral is not allowed.", 403)
        row={"trader_id":d.get("trader_id"),"trader_name":d.get("trader_name",""),"email":d.get("email",""),"phone":d.get("phone",""),
             "plan_id":d.get("plan_id"),"plan_name":plan,"account_size":clean(d.get("account_size")),"fee":clean(d.get("fee")),
             "payment_proof_url":proof,"payment_status":"pending","status":"pending_review","admin_note":"",
             "created_at":now_iso(),"purchase_month":month(),"purchase_year":year()}
        created = supabase.table("challenge_purchases").insert(row).execute().data

        send_email_safe(
            row.get("email"),
            "NairaPips payment proof received",
            f"""Hello {row.get("trader_name") or "Trader"},

Your NairaPips payment proof has been received.

Plan: {plan}
Challenge Fee: {email_money(row.get("fee"))}

Admin will review your proof and notify you after approval or rejection.

NairaPips Team"""
        )
        send_email_safe(
            row.get("email"),
            "NairaPips challenge purchase submitted",
            f"""Hello {row.get("trader_name") or "Trader"},

Your NairaPips challenge purchase has been submitted successfully.

Plan: {plan}
Account Size: {email_money(row.get("account_size"))}
Challenge Fee: {email_money(row.get("fee"))}

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
Fee: {email_money(row.get("fee"))}
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
        t=now_iso()
        master_password=m.get("mt5_master_password","")
        investor_password=m.get("mt5_investor_password","")
        supabase.table("challenge_purchases").update({"payment_status":"approved","status":"approved_active","assigned_mt5_id":m.get("id"),
            "mt5_login":m.get("mt5_login",""),"mt5_server":m.get("mt5_server",""),
            "mt5_master_password":master_password,"mt5_password":master_password,"master_password":master_password,
            "mt5_investor_password":investor_password,"investor_password":investor_password,
            "approved_at":t,"assigned_at":t,"updated_at":t,
            "admin_note":d.get("admin_note","Challenge approved and MT5 assigned")}).eq("id",pid).execute()
        supabase.table("mt5_pool").update({"status":"assigned","assigned_trader_id":p.get("trader_id"),"assigned_trader_name":p.get("trader_name",""),
            "assigned_email":p.get("email",""),"assigned_at":t,"updated_at":t,"admin_note":"Assigned through challenge purchase approval"}).eq("id",m.get("id")).execute()
        lookup=supabase.table("traders").select("*").or_(f"email.eq.{p.get('email','')},phone.eq.{p.get('phone','')}").limit(1).execute()
        td={"name":p.get("trader_name",""),"phone":p.get("phone",""),"email":p.get("email",""),
            "mt5_login":m.get("mt5_login",""),"mt5_server":m.get("mt5_server",""),"mt5_master_password":master_password,
            "mt5_password":master_password,"master_password":master_password,
            "mt5_investor_password":investor_password,"investor_password":investor_password,
            "mt5_updated_at":t,"updated_at":t,"account_size":p.get("account_size") or 0,
            "balance":p.get("account_size") or 0,"equity":p.get("account_size") or 0,"phase":"phase1","status":"active",
            "payment_status":"approved","payment_proof_url":p.get("payment_proof_url",""),"selected_plan":p.get("plan_name",""),
            "approved_at":t,"challenge_started_at":t,"approved_by":d.get("approved_by","admin"),"admin_note":d.get("admin_note",""),"trading_days_left":30}
        if lookup.data:
            supabase.table("traders").update(td).eq("id",lookup.data[0]["id"]).execute()
        else:
            td.update({"account_reference":ref(),"profit":0,"drawdown":0,"profit_percent":0,"drawdown_percent":0})
            supabase.table("traders").insert(td).execute()
        approved_rows = supabase.table("challenge_purchases").select("*").eq("id",pid).limit(1).execute().data

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

        _audit_safe("challenge_purchases", "challenge_purchase_approved", f"Purchase {pid} approved", _admin_from_payload(d))
        _audit_safe("mt5", "mt5_account_assignment", f"Purchase {pid} assigned MT5 {m.get('mt5_login','')}", _admin_from_payload(d))
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

@app.route("/payouts", methods=["GET"])
def payouts():
    try: return jsonify(supabase.table("payouts").select("*").order("created_at", desc=True).execute().data)
    except Exception as e: return bad(e)

@app.route("/create_payout", methods=["POST"])
def create_payout():
    try:
        d=request.json or {}; amount=clean(d.get("amount"))
        if amount<=0: return bad("Invalid payout amount")
        row={"trader_id":d.get("trader_id"),"trader_name":d.get("trader_name",""),"email":d.get("email",""),"phone":d.get("phone",""),
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

def _priority_for_zone(zone):
    return {"safe":"normal","warning":"medium","danger":"high","critical":"urgent","breached":"closed"}.get(zone, "normal")

def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

def _get_trader_by_id(trader_id):
    res = supabase.table("traders").select("*").eq("id", trader_id).limit(1).execute()
    data = getattr(res, "data", None) or []
    return data[0] if data else None

def _insert_monitoring_event(trader, event_type, zone, message, balance, equity, max_dd_used):
    try:
        supabase.table("monitoring_events").insert({
            "trader_id": trader.get("id"),
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

def _apply_monitoring_snapshot(trader, payload, source="manual"):
    balance = _num(payload.get("balance"), _num(trader.get("balance"), _num(trader.get("account_size"))))
    equity = _num(payload.get("equity"), balance)
    account_size = _num(trader.get("account_size"), balance)

    profit = equity - account_size if account_size else _num(payload.get("profit"), 0)
    profit_percent = (profit / account_size * 100) if account_size else 0

    previous_highest = _num(trader.get("highest_equity"), 0)
    previous_lowest = _num(trader.get("lowest_equity"), 0)

    highest_equity = max(previous_highest, equity, account_size)
    lowest_equity = min(previous_lowest, equity) if previous_lowest > 0 else equity

    peak_base = max(highest_equity, account_size)
    equity_damage = max(0, peak_base - lowest_equity)
    drawdown_percent = (equity_damage / peak_base * 100) if peak_base else 0
    max_dd_used = (drawdown_percent / MAX_DRAWDOWN_LIMIT * 100) if MAX_DRAWDOWN_LIMIT else 0

    zone = _risk_zone(max_dd_used)
    priority = _priority_for_zone(zone)
    old_zone = (trader.get("risk_zone") or "safe").lower()
    old_status = (trader.get("status") or "").lower()

    update_data = {
        "balance": balance,
        "equity": equity,
        "profit": profit,
        "profit_percent": profit_percent,
        "drawdown": equity_damage,
        "drawdown_percent": drawdown_percent,
        "highest_equity": highest_equity,
        "lowest_equity": lowest_equity,
        "peak_balance": max(_num(trader.get("peak_balance"), 0), balance, account_size),
        "last_equity_snapshot": equity,
        "max_drawdown_used": max_dd_used,
        "risk_zone": zone,
        "critical_mode": zone in ["danger", "critical"],
        "monitoring_priority": priority,
        "last_sync_at": _now_iso()
    }

    if zone == "breached":
        update_data.update({
            "status": "breached",
            "breach_time": trader.get("breach_time") or _now_iso(),
            "breach_equity": equity,
            "breach_reason": "Maximum drawdown violation recorded by NairaPips monitoring engine.",
            "admin_note": "Auto-breach: maximum drawdown violation recorded by monitoring engine.",
            "mt5_access_disabled": True if zone == "breached" else False,
"breach_detected_at": datetime.now(timezone.utc).isoformat() if zone == "breached" else trader.get("breach_detected_at"),
        })

    supabase.table("traders").update(update_data).eq("id", trader.get("id")).execute()

    try:
        supabase.table("monitoring_snapshots").insert({
            "trader_id": trader.get("id"),
            "trader_name": trader.get("name"),
            "email": trader.get("email"),
            "mt5_login": trader.get("mt5_login"),
            "balance": balance,
            "equity": equity,
            "profit": profit,
            "profit_percent": profit_percent,
            "drawdown": equity_damage,
            "drawdown_percent": drawdown_percent,
            "max_drawdown_used": max_dd_used,
            "risk_zone": zone,
            "source": source,
            "raw_data": payload
        }).execute()
    except Exception as e:
        print("monitoring snapshot insert failed:", e)

    # Auto Timeline + Evidence Population
    # Every important snapshot now becomes readable evidence for admin/trader dashboards.
    if _should_record_snapshot_event(old_zone, zone, round(max_dd_used)):
        event_type = "monitoring_snapshot"
        if zone != old_zone:
            event_type = "risk_zone_change"
        if zone == "critical":
            event_type = "critical_mode"
        if zone == "danger":
            event_type = "danger_zone"
        if zone == "breached":
            event_type = "breach_detected"

        _insert_monitoring_event(
            trader,
            event_type,
            zone,
            _snapshot_event_message(zone, balance, equity, profit_percent, drawdown_percent, max_dd_used),
            balance,
            equity,
            max_dd_used
        )

    if zone == "breached" and old_status != "breached":
        _insert_monitoring_event(
            trader,
            "account_locked",
            zone,
            "Account locked permanently by NairaPips monitoring engine after maximum drawdown violation.",
            balance,
            equity,
            max_dd_used
        )

    return {
        "trader_id": trader.get("id"),
        "balance": balance,
        "equity": equity,
        "profit": profit,
        "profit_percent": profit_percent,
        "drawdown_percent": drawdown_percent,
        "max_drawdown_used": max_dd_used,
        "risk_zone": zone,
        "critical_mode": zone in ["danger", "critical"],
        "monitoring_priority": priority,
        "status": update_data.get("status", trader.get("status"))
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
    if not trader_id:
        return jsonify({"success": False, "error": "trader_id is required"}), 400
    trader = _get_trader_by_id(trader_id)
    if not trader:
        return jsonify({"success": False, "error": "Trader not found"}), 404
    return jsonify({"success": True, "data": _apply_monitoring_snapshot(trader, data, data.get("source", "manual"))})

@app.route("/sync_fxblue_account", methods=["POST"])
def sync_fxblue_account():
    data = request.get_json(force=True) or {}
    trader_id = data.get("trader_id") or data.get("id")
    if not trader_id:
        return jsonify({"success": False, "error": "trader_id is required"}), 400
    trader = _get_trader_by_id(trader_id)
    if not trader:
        return jsonify({"success": False, "error": "Trader not found"}), 404
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
            row = {
                "trader_id": t.get("trader_id"),
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
        mt5_login = str(d.get("mt5_login") or "")
        reason = d.get("reason") or "This MT5 account breached NairaPips rules and is no longer payout eligible."

        if not trader_id and not mt5_login:
            return bad("trader_id or mt5_login is required")

        update = {
            "status": "breached",
            "phase": "breached",
            "monitoring_enabled": False,
            "mt5_account_active": False,
            "payout_eligible": False,
            "admin_note": reason,
            "updated_at": now_iso()
        }

        query = supabase.table("traders").update(update)

        if trader_id:
            res = query.eq("id", trader_id).execute()
        else:
            res = query.eq("mt5_login", mt5_login).execute()

        return ok(res.data, "Breached MT5 account locked. Trader profile remains active.")

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
    query = supabase.table("monitoring_events").select("*").order("created_at", desc=True).limit(100)
    if trader_id:
        query = query.eq("trader_id", trader_id)
    res = query.execute()
    return jsonify(getattr(res, "data", []) or [])

@app.route("/monitoring_snapshots", methods=["GET"])
def monitoring_snapshots():
    trader_id = request.args.get("trader_id")
    query = supabase.table("monitoring_snapshots").select("*").order("created_at", desc=True).limit(100)
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
        mode = "live" if str(settings.get("production_mode") or "").lower() == "live" else "test"
        launch_iso = _normalize_launch_date(settings.get("revenue_launch_date") or settings.get("launch_date") or "")
        launch_dt = _revenue_date(launch_iso)

        purchase_rows = supabase.table("challenge_purchases").select("*").execute().data or []
        payout_rows = supabase.table("payouts").select("*").execute().data or []

        try:
            trader_rows = supabase.table("traders").select("id,status,phase").execute().data or []
        except Exception:
            trader_rows = []

        def after_launch(dt):
            if not launch_dt:
                return True
            return bool(dt and dt >= launch_dt)

        def live_excluded(row):
            if mode != "live":
                return False
            return _revenue_bool(row.get("excluded_from_revenue")) or _revenue_bool(row.get("mark_as_test"))

        counted_purchases = []
        excluded_purchases = 0
        for row in purchase_rows:
            status = str(row.get("payment_status") or row.get("status") or "").strip().lower()
            approved = status in ["approved", "approved_active", "active"]
            dt = _revenue_date(row.get("approved_at") or row.get("created_at"))
            if not approved or live_excluded(row) or not after_launch(dt):
                excluded_purchases += 1
                continue
            counted_purchases.append((row, dt))

        counted_payouts = []
        excluded_payouts = 0
        for row in payout_rows:
            status = str(row.get("status") or "").strip().lower()
            included = status in ["paid", "approved"]
            dt = _revenue_date(row.get("paid_at") or row.get("approved_at") or row.get("requested_at") or row.get("created_at"))
            if not included or live_excluded(row) or not after_launch(dt):
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
            status = str(row.get("status") or "").lower()
            if status == "paid":
                summary["paid_payouts"] += amount
            if status == "approved":
                summary["approved_payouts"] += amount
            if flags["week"]: summary["weekly_payouts"] += amount
            if flags["month"]: summary["monthly_payouts"] += amount
            if flags["year"]: summary["yearly_payouts"] += amount

        for row in purchase_rows:
            status = str(row.get("payment_status") or row.get("status") or "").lower()
            if status in ["pending", "pending_review"]:
                summary["pending_sales"] += clean(row.get("fee"))
            if status == "rejected":
                summary["rejected_sales"] += clean(row.get("fee"))

        for row in payout_rows:
            if str(row.get("status") or "").lower() == "pending":
                summary["pending_payouts"] += clean(row.get("amount"))

        summary["weekly_net"] = summary["weekly_sales"] - summary["weekly_payouts"]
        summary["monthly_net"] = summary["monthly_sales"] - summary["monthly_payouts"]
        summary["yearly_net"] = summary["yearly_sales"] - summary["yearly_payouts"]
        summary["net_revenue"] = summary["gross_revenue"] - summary["paid_payouts"] - summary["approved_payouts"]
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
if __name__ == "__main__":
    port=int(os.environ.get("PORT",10000))
    app.run(host="0.0.0.0", port=port)
