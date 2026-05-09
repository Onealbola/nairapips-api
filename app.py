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


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def generate_reference():
    return "NP-" + str(random.randint(100000, 999999))


@app.route("/")
def home():
    return jsonify({
        "status": "NairaPips API Live",
        "database": "connected"
    })


@app.route("/health")
def health():
    return jsonify({"health": "ok"})


@app.route("/traders", methods=["GET"])
def get_traders():
    res = supabase.table("traders").select("*").order("created_at", desc=True).execute()
    return jsonify(res.data)


@app.route("/traders", methods=["POST"])
def add_trader():
    try:
        data = request.json or {}

        balance_raw = str(data.get("balance") or data.get("account_size") or "0")
        balance = float(balance_raw.replace(",", "").replace("₦", "").strip() or 0)

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
            return jsonify({
                "success": False,
                "error": "All MT5 credentials are required"
            }), 400

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
            balance_raw = str(data.get("balance") or data.get("account_size"))
            balance = float(balance_raw.replace(",", "").replace("₦", "").strip() or 0)
            update_data["account_size"] = balance
            update_data["balance"] = balance
            update_data["equity"] = balance

        res = supabase.table("traders").update(update_data).eq("id", trader_id).execute()

        return jsonify({"success": True, "data": res.data})

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

        return jsonify({"success": True, "data": res.data})

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
            "status",
            "phase",
            "balance",
            "equity",
            "profit",
            "drawdown",
            "profit_percent",
            "drawdown_percent",
            "engine_group",
            "payment_status",
            "payment_note",
            "admin_note",
            "trading_days_left"
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
        print("UPDATE ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/activate_trader", methods=["POST"])
def activate_trader():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        res = supabase.table("traders").update({
            "status": "active"
        }).eq("id", trader_id).execute()

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

        res = supabase.table("traders").update({
            "status": "inactive"
        }).eq("id", trader_id).execute()

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
# PAYOUT BACKEND
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

        amount_raw = str(data.get("amount", "0"))
        amount = float(amount_raw.replace(",", "").replace("₦", "").strip() or 0)

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

        return jsonify({"success": True, "data": res.data})

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

        return jsonify({"success": True, "data": res.data})

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

        return jsonify({"success": True, "data": res.data})

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

        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("MARK PAYOUT PAID ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
