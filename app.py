from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client
import os
from datetime import datetime, timezone
import random

app = Flask(__name__)
CORS(app)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# =========================
# HELPERS
# =========================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def generate_reference():
    return "NP-" + str(random.randint(100000, 999999))


def clean_amount(value):
    raw = str(value or "0")
    return float(raw.replace(",", "").replace("₦", "").strip() or 0)


def month_text():
    return datetime.now(timezone.utc).strftime("%B")


def year_text():
    return datetime.now(timezone.utc).strftime("%Y")


# =========================
# BASIC ROUTES
# =========================

@app.route("/")
def home():
    return jsonify({
        "status": "NairaPips API Live",
        "database": "connected",
        "version": "plans-delete-mt5-vault-upgrade"
    })


@app.route("/health")
def health():
    return jsonify({"health": "ok"})


# =========================
# TRADERS
# =========================

@app.route("/traders", methods=["GET"])
def get_traders():
    try:
        res = supabase.table("traders").select("*").order("created_at", desc=True).execute()
        return jsonify(res.data)
    except Exception as e:
        print("GET TRADERS ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/traders", methods=["POST"])
def add_trader():
    try:
        data = request.json or {}
        balance = clean_amount(data.get("balance") or data.get("account_size"))

        trader = {
            "name": data.get("name", ""),
            "phone": data.get("phone", ""),
            "email": data.get("email", ""),

            "mt5_login": data.get("mt5_login", ""),
            "mt5_server": data.get("mt5_server", ""),
            "mt5_master_password": data.get("mt5_master_password", ""),
            "mt5_investor_password": data.get("mt5_investor_password", ""),

            "account_size": balance,
            "balance": balance,
            "equity": balance,

            "phase": data.get("phase", "no_account"),
            "status": data.get("status", "payment_pending"),
            "engine_group": data.get("engine_group", "engine_1"),

            "profit": 0,
            "drawdown": 0,
            "profit_percent": 0,
            "drawdown_percent": 0,

            "payment_status": data.get("payment_status", "pending"),
            "payment_proof_url": data.get("payment_proof_url", ""),
            "selected_plan": data.get("selected_plan", ""),
            "payment_note": data.get("payment_note", ""),

            "approved_by": "",
            "admin_note": "",

            "account_reference": data.get("account_reference") or generate_reference(),
            "challenge_started_at": data.get("challenge_started_at"),
            "approved_at": data.get("approved_at"),
            "funded_at": data.get("funded_at"),
            "last_login_at": None,
            "trading_days_left": data.get("trading_days_left", 30)
        }

        res = supabase.table("traders").insert(trader).execute()
        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("ADD TRADER ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/login_trader", methods=["POST"])
def login_trader():
    try:
        data = request.json or {}
        lookup = str(data.get("lookup", "")).strip().lower()

        if not lookup:
            return jsonify({"success": False, "error": "Missing lookup"}), 400

        res = (
            supabase
            .table("traders")
            .select("*")
            .or_(f"email.eq.{lookup},phone.eq.{lookup}")
            .limit(1)
            .execute()
        )

        if not res.data:
            return jsonify({"success": False, "error": "Trader not found"}), 404

        trader = res.data[0]
        login_time = now_iso()

        supabase.table("traders").update({
            "last_login_at": login_time
        }).eq("id", trader["id"]).execute()

        trader["last_login_at"] = login_time

        return jsonify({"success": True, "data": trader})

    except Exception as e:
        print("LOGIN ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# PAYMENT APPROVAL
# =========================

@app.route("/approve_payment", methods=["POST"])
def approve_payment():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        mt5_login = str(data.get("mt5_login", "")).strip()
        mt5_server = str(data.get("mt5_server", "")).strip()
        mt5_master_password = str(data.get("mt5_master_password", "")).strip()
        mt5_investor_password = str(data.get("mt5_investor_password", "")).strip()

        if not mt5_login or not mt5_server or not mt5_master_password or not mt5_investor_password:
            return jsonify({"success": False, "error": "All MT5 credentials are required"}), 400

        update_data = {
            "payment_status": "approved",
            "status": "active",
            "phase": data.get("phase", "phase1"),

            "mt5_login": mt5_login,
            "mt5_server": mt5_server,
            "mt5_master_password": mt5_master_password,
            "mt5_investor_password": mt5_investor_password,

            "approved_at": now_iso(),
            "challenge_started_at": now_iso(),
            "approved_by": data.get("approved_by", "admin"),
            "admin_note": data.get("admin_note", "")
        }

        if data.get("balance") or data.get("account_size"):
            balance = clean_amount(data.get("balance") or data.get("account_size"))
            update_data["account_size"] = balance
            update_data["balance"] = balance
            update_data["equity"] = balance

        res = supabase.table("traders").update(update_data).eq("id", trader_id).execute()

        return jsonify({
            "success": True,
            "message": "Payment approved and trader activated",
            "data": res.data
        })

    except Exception as e:
        print("APPROVE PAYMENT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/reject_payment", methods=["POST"])
def reject_payment():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        res = supabase.table("traders").update({
            "payment_status": "rejected",
            "status": "payment_rejected",
            "admin_note": data.get("admin_note", "")
        }).eq("id", trader_id).execute()

        return jsonify({"success": True, "message": "Payment rejected", "data": res.data})

    except Exception as e:
        print("REJECT PAYMENT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/update_status", methods=["POST"])
def update_status():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        update_data = {}
        allowed_fields = [
            "status", "phase", "balance", "equity", "profit", "drawdown",
            "profit_percent", "drawdown_percent", "engine_group",
            "payment_status", "payment_note", "admin_note", "trading_days_left"
        ]

        for field in allowed_fields:
            if field in data:
                update_data[field] = data[field]

        if data.get("phase") == "funded" or data.get("status") == "funded":
            update_data["funded_at"] = now_iso()

        if not update_data:
            return jsonify({"success": False, "error": "Nothing to update"}), 400

        res = supabase.table("traders").update(update_data).eq("id", trader_id).execute()
        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("UPDATE STATUS ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/activate_trader", methods=["POST"])
def activate_trader():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        res = supabase.table("traders").update({"status": "active"}).eq("id", trader_id).execute()
        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("ACTIVATE ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/deactivate_trader", methods=["POST"])
def deactivate_trader():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        res = supabase.table("traders").update({"status": "inactive"}).eq("id", trader_id).execute()
        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("DEACTIVATE ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/delete_trader", methods=["POST"])
def delete_trader():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        res = supabase.table("traders").delete().eq("id", trader_id).execute()
        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("DELETE ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# CHALLENGE PLANS
# =========================

@app.route("/challenge_plans", methods=["GET"])
def get_challenge_plans():
    try:
        res = (
            supabase
            .table("challenge_plans")
            .select("*")
            .order("account_size", desc=False)
            .execute()
        )
        return jsonify(res.data)

    except Exception as e:
        print("GET PLANS ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/create_challenge_plan", methods=["POST"])
def create_challenge_plan():
    try:
        data = request.json or {}

        name = str(data.get("name", "")).strip()

        if not name:
            return jsonify({"success": False, "error": "Plan name is required"}), 400

        plan = {
            "name": name,
            "account_size": clean_amount(data.get("account_size")),
            "fee": clean_amount(data.get("fee")),
            "phase1_target": float(data.get("phase1_target") or 10),
            "phase2_target": float(data.get("phase2_target") or 8),
            "max_drawdown": float(data.get("max_drawdown") or 20),
            "daily_drawdown": data.get("daily_drawdown", "None"),
            "payout_split": data.get("payout_split", "80%"),
            "description": data.get("description", ""),
            "status": "active",
            "created_at": now_iso(),
            "updated_at": now_iso()
        }

        res = supabase.table("challenge_plans").insert(plan).execute()
        return jsonify({"success": True, "message": "Challenge plan created", "data": res.data})

    except Exception as e:
        print("CREATE PLAN ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/update_challenge_plan", methods=["POST"])
def update_challenge_plan():
    try:
        data = request.json or {}
        plan_id = data.get("id")

        if not plan_id:
            return jsonify({"success": False, "error": "Missing plan id"}), 400

        update_data = {"updated_at": now_iso()}

        text_fields = [
            "name",
            "daily_drawdown",
            "payout_split",
            "description",
            "status"
        ]

        money_fields = [
            "account_size",
            "fee"
        ]

        number_fields = [
            "phase1_target",
            "phase2_target",
            "max_drawdown"
        ]

        for field in text_fields:
            if field in data:
                update_data[field] = data[field]

        for field in money_fields:
            if field in data:
                update_data[field] = clean_amount(data.get(field))

        for field in number_fields:
            if field in data:
                update_data[field] = float(data.get(field) or 0)

        res = supabase.table("challenge_plans").update(update_data).eq("id", plan_id).execute()

        return jsonify({
            "success": True,
            "message": "Challenge plan updated",
            "data": res.data
        })

    except Exception as e:
        print("UPDATE PLAN ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/delete_challenge_plan", methods=["POST"])
def delete_challenge_plan():
    try:
        data = request.json or {}
        plan_id = data.get("id")

        if not plan_id:
            return jsonify({"success": False, "error": "Missing plan id"}), 400

        res = supabase.table("challenge_plans").delete().eq("id", plan_id).execute()

        return jsonify({
            "success": True,
            "message": "Challenge plan deleted",
            "data": res.data
        })

    except Exception as e:
        print("DELETE PLAN ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# CHALLENGE PURCHASES
# =========================

@app.route("/challenge_purchases", methods=["GET"])
def get_challenge_purchases():
    try:
        res = (
            supabase
            .table("challenge_purchases")
            .select("*")
            .order("created_at", desc=True)
            .execute()
        )
        return jsonify(res.data)

    except Exception as e:
        print("GET PURCHASES ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/create_challenge_purchase", methods=["POST"])
def create_challenge_purchase():
    try:
        data = request.json or {}

        plan_name = str(data.get("plan_name", "")).strip()
        payment_proof_url = str(data.get("payment_proof_url", "")).strip()

        if not plan_name:
            return jsonify({"success": False, "error": "Plan name is required"}), 400

        if not payment_proof_url:
            return jsonify({"success": False, "error": "Payment proof is required"}), 400

        purchase = {
            "trader_id": data.get("trader_id"),
            "trader_name": data.get("trader_name", ""),
            "email": data.get("email", ""),
            "phone": data.get("phone", ""),

            "plan_id": data.get("plan_id"),
            "plan_name": plan_name,
            "account_size": clean_amount(data.get("account_size")),
            "fee": clean_amount(data.get("fee")),

            "payment_proof_url": payment_proof_url,

            "payment_status": "pending",
            "status": "pending_review",

            "admin_note": "",

            "created_at": now_iso(),
            "purchase_month": month_text(),
            "purchase_year": year_text()
        }

        res = supabase.table("challenge_purchases").insert(purchase).execute()

        return jsonify({
            "success": True,
            "message": "Challenge purchase submitted",
            "data": res.data
        })

    except Exception as e:
        print("CREATE PURCHASE ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/approve_challenge_purchase", methods=["POST"])
def approve_challenge_purchase():
    try:
        data = request.json or {}
        purchase_id = data.get("id")
        mt5_id = data.get("mt5_id")

        if not purchase_id:
            return jsonify({"success": False, "error": "Missing purchase id"}), 400

        purchase_res = (
            supabase
            .table("challenge_purchases")
            .select("*")
            .eq("id", purchase_id)
            .limit(1)
            .execute()
        )

        if not purchase_res.data:
            return jsonify({"success": False, "error": "Purchase not found"}), 404

        purchase = purchase_res.data[0]
        mt5_account = None

        if mt5_id:
            mt5_res = (
                supabase
                .table("mt5_pool")
                .select("*")
                .eq("id", mt5_id)
                .limit(1)
                .execute()
            )
            if mt5_res.data:
                mt5_account = mt5_res.data[0]
        else:
            mt5_res = (
                supabase
                .table("mt5_pool")
                .select("*")
                .eq("status", "available")
                .eq("account_size", purchase.get("account_size") or 0)
                .limit(1)
                .execute()
            )
            if mt5_res.data:
                mt5_account = mt5_res.data[0]

        if not mt5_account:
            return jsonify({
                "success": False,
                "error": "No available MT5 account found for this plan/account size"
            }), 400

        approved_time = now_iso()

        purchase_update = {
            "payment_status": "approved",
            "status": "approved_active",
            "assigned_mt5_id": mt5_account.get("id"),
            "mt5_login": mt5_account.get("mt5_login", ""),
            "mt5_server": mt5_account.get("mt5_server", ""),
            "approved_at": approved_time,
            "assigned_at": approved_time,
            "admin_note": data.get("admin_note", "Challenge approved and MT5 assigned")
        }

        supabase.table("challenge_purchases").update(purchase_update).eq("id", purchase_id).execute()

        supabase.table("mt5_pool").update({
            "status": "assigned",
            "assigned_trader_id": purchase.get("trader_id"),
            "assigned_trader_name": purchase.get("trader_name", ""),
            "assigned_email": purchase.get("email", ""),
            "assigned_at": approved_time,
            "updated_at": approved_time,
            "admin_note": "Assigned through challenge purchase approval"
        }).eq("id", mt5_account.get("id")).execute()

        trader_lookup = (
            supabase
            .table("traders")
            .select("*")
            .or_(f"email.eq.{purchase.get('email','')},phone.eq.{purchase.get('phone','')}")
            .limit(1)
            .execute()
        )

        trader_data = {
            "name": purchase.get("trader_name", ""),
            "phone": purchase.get("phone", ""),
            "email": purchase.get("email", ""),

            "mt5_login": mt5_account.get("mt5_login", ""),
            "mt5_server": mt5_account.get("mt5_server", ""),
            "mt5_master_password": mt5_account.get("mt5_master_password", ""),
            "mt5_investor_password": mt5_account.get("mt5_investor_password", ""),

            "account_size": purchase.get("account_size") or 0,
            "balance": purchase.get("account_size") or 0,
            "equity": purchase.get("account_size") or 0,

            "phase": "phase1",
            "status": "active",
            "payment_status": "approved",
            "payment_proof_url": purchase.get("payment_proof_url", ""),
            "selected_plan": purchase.get("plan_name", ""),

            "approved_at": approved_time,
            "challenge_started_at": approved_time,
            "approved_by": data.get("approved_by", "admin"),
            "admin_note": data.get("admin_note", ""),
            "trading_days_left": 30
        }

        if trader_lookup.data:
            trader_id = trader_lookup.data[0]["id"]
            supabase.table("traders").update(trader_data).eq("id", trader_id).execute()
        else:
            trader_data["account_reference"] = generate_reference()
            trader_data["profit"] = 0
            trader_data["drawdown"] = 0
            trader_data["profit_percent"] = 0
            trader_data["drawdown_percent"] = 0
            supabase.table("traders").insert(trader_data).execute()

        final_res = supabase.table("challenge_purchases").select("*").eq("id", purchase_id).limit(1).execute()

        return jsonify({
            "success": True,
            "message": "Challenge purchase approved and MT5 assigned",
            "data": final_res.data
        })

    except Exception as e:
        print("APPROVE PURCHASE ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/reject_challenge_purchase", methods=["POST"])
def reject_challenge_purchase():
    try:
        data = request.json or {}
        purchase_id = data.get("id")

        if not purchase_id:
            return jsonify({"success": False, "error": "Missing purchase id"}), 400

        res = supabase.table("challenge_purchases").update({
            "payment_status": "rejected",
            "status": "rejected",
            "rejected_at": now_iso(),
            "admin_note": data.get("admin_note", "Challenge purchase rejected")
        }).eq("id", purchase_id).execute()

        return jsonify({
            "success": True,
            "message": "Challenge purchase rejected",
            "data": res.data
        })

    except Exception as e:
        print("REJECT PURCHASE ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# MT5 POOL / VAULT
# =========================

@app.route("/mt5_pool", methods=["GET"])
def get_mt5_pool():
    try:
        res = supabase.table("mt5_pool").select("*").order("created_at", desc=True).execute()
        return jsonify(res.data)

    except Exception as e:
        print("GET MT5 POOL ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/create_mt5_account", methods=["POST"])
def create_mt5_account():
    try:
        data = request.json or {}

        mt5_login = str(data.get("mt5_login", "")).strip()
        mt5_server = str(data.get("mt5_server", "")).strip()
        mt5_master_password = str(data.get("mt5_master_password", "")).strip()
        mt5_investor_password = str(data.get("mt5_investor_password", "")).strip()

        if not mt5_login or not mt5_server or not mt5_master_password or not mt5_investor_password:
            return jsonify({"success": False, "error": "All MT5 details are required"}), 400

        account = {
            "plan_name": data.get("plan_name", ""),
            "account_size": clean_amount(data.get("account_size")),
            "mt5_login": mt5_login,
            "mt5_server": mt5_server,
            "mt5_master_password": mt5_master_password,
            "mt5_investor_password": mt5_investor_password,
            "status": data.get("status", "available"),
            "admin_note": data.get("admin_note", ""),
            "created_at": now_iso(),
            "updated_at": now_iso()
        }

        res = supabase.table("mt5_pool").insert(account).execute()

        return jsonify({
            "success": True,
            "message": "MT5 account added to vault",
            "data": res.data
        })

    except Exception as e:
        print("CREATE MT5 ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/update_mt5_account", methods=["POST"])
def update_mt5_account():
    try:
        data = request.json or {}
        mt5_id = data.get("id")

        if not mt5_id:
            return jsonify({"success": False, "error": "Missing MT5 account id"}), 400

        update_data = {"updated_at": now_iso()}

        fields = [
            "plan_name",
            "mt5_login",
            "mt5_server",
            "mt5_master_password",
            "mt5_investor_password",
            "status",
            "admin_note"
        ]

        for field in fields:
            if field in data:
                update_data[field] = data[field]

        if "account_size" in data:
            update_data["account_size"] = clean_amount(data.get("account_size"))

        res = supabase.table("mt5_pool").update(update_data).eq("id", mt5_id).execute()

        return jsonify({
            "success": True,
            "message": "MT5 account updated",
            "data": res.data
        })

    except Exception as e:
        print("UPDATE MT5 ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/delete_mt5_account", methods=["POST"])
def delete_mt5_account():
    try:
        data = request.json or {}
        mt5_id = data.get("id")

        if not mt5_id:
            return jsonify({"success": False, "error": "Missing MT5 account id"}), 400

        res = supabase.table("mt5_pool").delete().eq("id", mt5_id).execute()

        return jsonify({
            "success": True,
            "message": "MT5 account deleted",
            "data": res.data
        })

    except Exception as e:
        print("DELETE MT5 ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# PAYOUTS
# =========================

@app.route("/payouts", methods=["GET"])
def get_payouts():
    try:
        res = supabase.table("payouts").select("*").order("created_at", desc=True).execute()
        return jsonify(res.data)
    except Exception as e:
        print("GET PAYOUTS ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/create_payout", methods=["POST"])
def create_payout():
    try:
        data = request.json or {}
        amount = clean_amount(data.get("amount"))

        if amount <= 0:
            return jsonify({"success": False, "error": "Invalid payout amount"}), 400

        payout = {
            "trader_id": data.get("trader_id"),
            "trader_name": data.get("trader_name", ""),
            "email": data.get("email", ""),
            "phone": data.get("phone", ""),
            "amount": amount,
            "bank_name": data.get("bank_name", ""),
            "account_number": data.get("account_number", ""),
            "account_name": data.get("account_name", ""),
            "status": "pending",
            "note": data.get("note", ""),
            "admin_note": "",
            "requested_at": now_iso()
        }

        res = supabase.table("payouts").insert(payout).execute()

        return jsonify({
            "success": True,
            "message": "Payout request created",
            "data": res.data
        })

    except Exception as e:
        print("CREATE PAYOUT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/approve_payout", methods=["POST"])
def approve_payout():
    try:
        data = request.json or {}
        payout_id = data.get("id")

        if not payout_id:
            return jsonify({"success": False, "error": "Missing payout id"}), 400

        res = supabase.table("payouts").update({
            "status": "approved",
            "approved_at": now_iso(),
            "admin_note": data.get("admin_note", "")
        }).eq("id", payout_id).execute()

        return jsonify({"success": True, "message": "Payout approved", "data": res.data})

    except Exception as e:
        print("APPROVE PAYOUT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/reject_payout", methods=["POST"])
def reject_payout():
    try:
        data = request.json or {}
        payout_id = data.get("id")

        if not payout_id:
            return jsonify({"success": False, "error": "Missing payout id"}), 400

        res = supabase.table("payouts").update({
            "status": "rejected",
            "rejected_at": now_iso(),
            "admin_note": data.get("admin_note", "")
        }).eq("id", payout_id).execute()

        return jsonify({"success": True, "message": "Payout rejected", "data": res.data})

    except Exception as e:
        print("REJECT PAYOUT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/mark_payout_paid", methods=["POST"])
def mark_payout_paid():
    try:
        data = request.json or {}
        payout_id = data.get("id")

        if not payout_id:
            return jsonify({"success": False, "error": "Missing payout id"}), 400

        res = supabase.table("payouts").update({
            "status": "paid",
            "paid_at": now_iso(),
            "admin_note": data.get("admin_note", "")
        }).eq("id", payout_id).execute()

        return jsonify({"success": True, "message": "Payout marked as paid", "data": res.data})

    except Exception as e:
        print("MARK PAYOUT PAID ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# SUPPORT TICKETS
# =========================

@app.route("/support_tickets", methods=["GET"])
def get_support_tickets():
    try:
        res = supabase.table("support_tickets").select("*").order("created_at", desc=True).execute()
        return jsonify(res.data)
    except Exception as e:
        print("GET SUPPORT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/create_support_ticket", methods=["POST"])
def create_support_ticket():
    try:
        data = request.json or {}

        subject = str(data.get("subject", "")).strip()
        message = str(data.get("message", "")).strip()

        if not subject or not message:
            return jsonify({"success": False, "error": "Subject and message are required"}), 400

        ticket = {
            "trader_id": data.get("trader_id"),
            "trader_name": data.get("trader_name", ""),
            "email": data.get("email", ""),
            "phone": data.get("phone", ""),
            "subject": subject,
            "message": message,
            "status": "open",
            "priority": data.get("priority", "normal"),
            "admin_reply": "",
            "created_at": now_iso(),
            "last_updated_at": now_iso()
        }

        res = supabase.table("support_tickets").insert(ticket).execute()

        return jsonify({
            "success": True,
            "message": "Support ticket created",
            "data": res.data
        })

    except Exception as e:
        print("CREATE SUPPORT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/reply_support_ticket", methods=["POST"])
def reply_support_ticket():
    try:
        data = request.json or {}
        ticket_id = data.get("id")

        if not ticket_id:
            return jsonify({"success": False, "error": "Missing ticket id"}), 400

        admin_reply = str(data.get("admin_reply", "")).strip()

        if not admin_reply:
            return jsonify({"success": False, "error": "Admin reply is required"}), 400

        res = supabase.table("support_tickets").update({
            "admin_reply": admin_reply,
            "status": "replied",
            "replied_at": now_iso(),
            "last_updated_at": now_iso()
        }).eq("id", ticket_id).execute()

        return jsonify({
            "success": True,
            "message": "Support ticket replied",
            "data": res.data
        })

    except Exception as e:
        print("REPLY SUPPORT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/close_support_ticket", methods=["POST"])
def close_support_ticket():
    try:
        data = request.json or {}
        ticket_id = data.get("id")

        if not ticket_id:
            return jsonify({"success": False, "error": "Missing ticket id"}), 400

        res = supabase.table("support_tickets").update({
            "status": "closed",
            "closed_at": now_iso(),
            "last_updated_at": now_iso()
        }).eq("id", ticket_id).execute()

        return jsonify({
            "success": True,
            "message": "Support ticket closed",
            "data": res.data
        })

    except Exception as e:
        print("CLOSE SUPPORT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# ANNOUNCEMENTS
# =========================

@app.route("/announcements", methods=["GET"])
def get_announcements():
    try:
        res = (
            supabase
            .table("announcements")
            .select("*")
            .eq("status", "active")
            .order("created_at", desc=True)
            .execute()
        )

        return jsonify(res.data)

    except Exception as e:
        print("GET ANNOUNCEMENTS ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/create_announcement", methods=["POST"])
def create_announcement():
    try:
        data = request.json or {}

        title = str(data.get("title", "")).strip()
        message = str(data.get("message", "")).strip()

        if not title or not message:
            return jsonify({"success": False, "error": "Title and message are required"}), 400

        announcement = {
            "title": title,
            "message": message,
            "type": data.get("type", "public_notice"),
            "status": "active",
            "show_on_landing": data.get("show_on_landing", True),
            "show_on_dashboard": data.get("show_on_dashboard", True),
            "created_by": data.get("created_by", "admin"),
            "created_at": now_iso()
        }

        res = supabase.table("announcements").insert(announcement).execute()

        return jsonify({
            "success": True,
            "message": "Announcement created",
            "data": res.data
        })

    except Exception as e:
        print("CREATE ANNOUNCEMENT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/disable_announcement", methods=["POST"])
def disable_announcement():
    try:
        data = request.json or {}
        announcement_id = data.get("id")

        if not announcement_id:
            return jsonify({"success": False, "error": "Missing announcement id"}), 400

        res = supabase.table("announcements").update({
            "status": "disabled"
        }).eq("id", announcement_id).execute()

        return jsonify({
            "success": True,
            "message": "Announcement disabled",
            "data": res.data
        })

    except Exception as e:
        print("DISABLE ANNOUNCEMENT ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


# =========================
# START SERVER
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
