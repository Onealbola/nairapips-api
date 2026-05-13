
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client
from werkzeug.utils import secure_filename
from datetime import datetime, timezone
import os, random, uuid

app = Flask(__name__)
CORS(app)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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

@app.route("/traders", methods=["GET"])
def get_traders():
    try: return jsonify(supabase.table("traders").select("*").order("created_at", desc=True).execute().data)
    except Exception as e: return bad(e)

@app.route("/traders", methods=["POST"])
def add_trader():
    try:
        d=request.json or {}; bal=clean(d.get("balance") or d.get("account_size"))
        row={
            "name":d.get("name",""),"phone":d.get("phone",""),"email":d.get("email",""),
            "mt5_login":d.get("mt5_login",""),"mt5_server":d.get("mt5_server",""),
            "mt5_master_password":d.get("mt5_master_password",""),"mt5_investor_password":d.get("mt5_investor_password",""),
            "account_size":bal,"balance":bal,"equity":bal,"phase":d.get("phase","no_account"),"status":d.get("status","payment_pending"),
            "engine_group":d.get("engine_group","engine_1"),"profit":0,"drawdown":0,"profit_percent":0,"drawdown_percent":0,
            "payment_status":d.get("payment_status","pending"),"payment_proof_url":d.get("payment_proof_url",""),
            "selected_plan":d.get("selected_plan",""),"payment_note":d.get("payment_note",""),"approved_by":"","admin_note":"",
            "account_reference":d.get("account_reference") or ref(),"challenge_started_at":d.get("challenge_started_at"),
            "approved_at":d.get("approved_at"),"funded_at":d.get("funded_at"),"last_login_at":None,"trading_days_left":d.get("trading_days_left",30)
        }
        return ok(supabase.table("traders").insert(row).execute().data, "Trader added")
    except Exception as e: return bad(e)

@app.route("/login_trader", methods=["POST"])
def login_trader():
    try:
        lookup=str((request.json or {}).get("lookup","")).strip().lower()
        if not lookup: return bad("Missing lookup")
        res=supabase.table("traders").select("*").or_(f"email.eq.{lookup},phone.eq.{lookup}").limit(1).execute()
        if not res.data: return bad("Trader not found",404)
        trader=res.data[0]; t=now_iso()
        supabase.table("traders").update({"last_login_at":t}).eq("id",trader["id"]).execute()
        trader["last_login_at"]=t
        return ok(trader, "Login successful")
    except Exception as e: return bad(e)

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
        return ok(supabase.table("traders").update(upd).eq("id",tid).execute().data, "Payment approved")
    except Exception as e: return bad(e)

@app.route("/reject_payment", methods=["POST"])
def reject_payment():
    try:
        d=request.json or {}; tid=d.get("id")
        if not tid: return bad("Missing trader id")
        return ok(supabase.table("traders").update({"payment_status":"rejected","status":"payment_rejected","admin_note":d.get("admin_note","")}).eq("id",tid).execute().data, "Payment rejected")
    except Exception as e: return bad(e)

@app.route("/update_status", methods=["POST"])
def update_status():
    try:
        d=request.json or {}; tid=d.get("id")
        if not tid: return bad("Missing trader id")
        allowed=["status","phase","balance","equity","profit","drawdown","profit_percent","drawdown_percent","engine_group","payment_status","payment_note","admin_note","trading_days_left"]
        upd={k:d[k] for k in allowed if k in d}
        if d.get("phase")=="funded" or d.get("status")=="funded": upd["funded_at"]=now_iso()
        if not upd: return bad("Nothing to update")
        return ok(supabase.table("traders").update(upd).eq("id",tid).execute().data)
    except Exception as e: return bad(e)

@app.route("/activate_trader", methods=["POST"])
def activate_trader():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing trader id")
        return ok(supabase.table("traders").update({"status":"active"}).eq("id",tid).execute().data)
    except Exception as e: return bad(e)

@app.route("/deactivate_trader", methods=["POST"])
def deactivate_trader():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing trader id")
        return ok(supabase.table("traders").update({"status":"inactive"}).eq("id",tid).execute().data)
    except Exception as e: return bad(e)

@app.route("/delete_trader", methods=["POST"])
def delete_trader():
    try:
        tid=(request.json or {}).get("id")
        if not tid: return bad("Missing trader id")
        return ok(supabase.table("traders").delete().eq("id",tid).execute().data)
    except Exception as e: return bad(e)

@app.route("/challenge_plans", methods=["GET"])
def challenge_plans():
    try: return jsonify(supabase.table("challenge_plans").select("*").order("account_size", desc=False).execute().data)
    except Exception as e: return bad(e)

@app.route("/create_challenge_plan", methods=["POST"])
def create_plan():
    try:
        d=request.json or {}; name=str(d.get("name","")).strip()
        if not name: return bad("Plan name is required")
        row={"name":name,"account_size":clean(d.get("account_size")),"fee":clean(d.get("fee")),
             "phase1_target":float(d.get("phase1_target") or 10),"phase2_target":float(d.get("phase2_target") or 8),
             "max_drawdown":float(d.get("max_drawdown") or 20),"daily_drawdown":d.get("daily_drawdown","None"),
             "payout_split":d.get("payout_split","80%"),"description":d.get("description",""),"status":"active","created_at":now_iso(),"updated_at":now_iso()}
        return ok(supabase.table("challenge_plans").insert(row).execute().data, "Challenge plan created")
    except Exception as e: return bad(e)

@app.route("/update_challenge_plan", methods=["POST"])
def update_plan():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing plan id")
        upd={"updated_at":now_iso()}
        for k in ["name","daily_drawdown","payout_split","description","status"]:
            if k in d: upd[k]=d[k]
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
        row={"trader_id":d.get("trader_id"),"trader_name":d.get("trader_name",""),"email":d.get("email",""),"phone":d.get("phone",""),
             "plan_id":d.get("plan_id"),"plan_name":plan,"account_size":clean(d.get("account_size")),"fee":clean(d.get("fee")),
             "payment_proof_url":proof,"payment_status":"pending","status":"pending_review","admin_note":"",
             "created_at":now_iso(),"purchase_month":month(),"purchase_year":year()}
        return ok(supabase.table("challenge_purchases").insert(row).execute().data, "Challenge purchase submitted")
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
        m=mres.data[0]; t=now_iso()
        supabase.table("challenge_purchases").update({"payment_status":"approved","status":"approved_active","assigned_mt5_id":m.get("id"),
            "mt5_login":m.get("mt5_login",""),"mt5_server":m.get("mt5_server",""),"approved_at":t,"assigned_at":t,
            "admin_note":d.get("admin_note","Challenge approved and MT5 assigned")}).eq("id",pid).execute()
        supabase.table("mt5_pool").update({"status":"assigned","assigned_trader_id":p.get("trader_id"),"assigned_trader_name":p.get("trader_name",""),
            "assigned_email":p.get("email",""),"assigned_at":t,"updated_at":t,"admin_note":"Assigned through challenge purchase approval"}).eq("id",m.get("id")).execute()
        lookup=supabase.table("traders").select("*").or_(f"email.eq.{p.get('email','')},phone.eq.{p.get('phone','')}").limit(1).execute()
        td={"name":p.get("trader_name",""),"phone":p.get("phone",""),"email":p.get("email",""),
            "mt5_login":m.get("mt5_login",""),"mt5_server":m.get("mt5_server",""),"mt5_master_password":m.get("mt5_master_password",""),
            "mt5_investor_password":m.get("mt5_investor_password",""),"account_size":p.get("account_size") or 0,
            "balance":p.get("account_size") or 0,"equity":p.get("account_size") or 0,"phase":"phase1","status":"active",
            "payment_status":"approved","payment_proof_url":p.get("payment_proof_url",""),"selected_plan":p.get("plan_name",""),
            "approved_at":t,"challenge_started_at":t,"approved_by":d.get("approved_by","admin"),"admin_note":d.get("admin_note",""),"trading_days_left":30}
        if lookup.data:
            supabase.table("traders").update(td).eq("id",lookup.data[0]["id"]).execute()
        else:
            td.update({"account_reference":ref(),"profit":0,"drawdown":0,"profit_percent":0,"drawdown_percent":0})
            supabase.table("traders").insert(td).execute()
        return ok(supabase.table("challenge_purchases").select("*").eq("id",pid).limit(1).execute().data, "Challenge purchase approved and MT5 assigned")
    except Exception as e: return bad(e)

@app.route("/reject_challenge_purchase", methods=["POST"])
def reject_purchase():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing purchase id")
        return ok(supabase.table("challenge_purchases").update({"payment_status":"rejected","status":"rejected","rejected_at":now_iso(),"admin_note":d.get("admin_note","Challenge purchase rejected")}).eq("id",pid).execute().data, "Challenge purchase rejected")
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
        row={"plan_name":d.get("plan_name",""),"account_size":clean(d.get("account_size")),"mt5_login":str(d.get("mt5_login","")).strip(),
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
        return ok(supabase.table("payouts").insert(row).execute().data, "Payout request created")
    except Exception as e: return bad(e)

@app.route("/approve_payout", methods=["POST"])
def approve_payout():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing payout id")
        return ok(supabase.table("payouts").update({"status":"approved","approved_at":now_iso(),"admin_note":d.get("admin_note","")}).eq("id",pid).execute().data, "Payout approved")
    except Exception as e: return bad(e)

@app.route("/reject_payout", methods=["POST"])
def reject_payout():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing payout id")
        return ok(supabase.table("payouts").update({"status":"rejected","rejected_at":now_iso(),"admin_note":d.get("admin_note","")}).eq("id",pid).execute().data, "Payout rejected")
    except Exception as e: return bad(e)

@app.route("/mark_payout_paid", methods=["POST"])
def mark_paid():
    try:
        d=request.json or {}; pid=d.get("id")
        if not pid: return bad("Missing payout id")
        return ok(supabase.table("payouts").update({"status":"paid","paid_at":now_iso(),"admin_note":d.get("admin_note","")}).eq("id",pid).execute().data, "Payout marked paid")
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
        return ok(supabase.table("support_tickets").insert(row).execute().data, "Support ticket created")
    except Exception as e: return bad(e)

@app.route("/reply_support_ticket", methods=["POST"])
def reply_ticket():
    try:
        d=request.json or {}; tid=d.get("id"); reply=str(d.get("admin_reply","")).strip()
        if not tid: return bad("Missing ticket id")
        if not reply: return bad("Admin reply is required")
        return ok(supabase.table("support_tickets").update({"admin_reply":reply,"status":"replied","replied_at":now_iso(),"last_updated_at":now_iso()}).eq("id",tid).execute().data, "Support ticket replied")
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
    lowest_equity = equity if previous_lowest <= 0 else min(previous_lowest, equity)

    peak_base = max(highest_equity, account_size, balance)
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
            "admin_note": "Auto-breach: maximum drawdown violation recorded by monitoring engine."
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
@app.route("/api/admin/traders", methods=["GET"])
def get_all_traders():

    try:
        response = (
            supabase.table("traders")
            .select("*")
            .execute()
        )

        traders = response.data or []

        monitorable = []

        for trader in traders:

            mt5_login = trader.get("mt5_login")

            server = trader.get("mt5_server") or trader.get("server")

            investor_password = trader.get("mt5_investor_password") or trader.get("investor_password")

            if mt5_login and server and investor_password:

                trader["mt5_server"] = server

                trader["mt5_investor_password"] = investor_password

                trader["monitoring_enabled"] = True

                monitorable.append(trader)

        return jsonify(monitorable), 200

    except Exception as e:

        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

if __name__ == "__main__":
    port=int(os.environ.get("PORT",10000))
    app.run(host="0.0.0.0", port=port)
