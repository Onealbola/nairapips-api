from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client
import os

app = Flask(__name__)
CORS(app)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route("/")
def home():
    return jsonify({"status": "NairaPips API Live", "database": "connected"})

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
        balance = float(balance_raw.replace(",", "").replace("₦", "").strip())

        trader = {
            "name": data.get("name", ""),
            "phone": data.get("phone", ""),
            "email": data.get("email", ""),
            "mt5_login": str(data.get("mt5_login", "")),
            "mt5_server": data.get("mt5_server", ""),
            "mt5_master_password": data.get("mt5_master_password", ""),
            "mt5_investor_password": data.get("mt5_investor_password", ""),
            "account_size": balance,
            "balance": balance,
            "equity": balance,
            "phase": data.get("phase", "phase1"),
            "status": data.get("status", "active"),
            "engine_group": data.get("engine_group", "engine_1"),
            "profit": 0,
            "drawdown": 0,
            "profit_percent": 0,
            "drawdown_percent": 0
        }

        res = supabase.table("traders").insert(trader).execute()
        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("ADD TRADER ERROR:", repr(e))
        return jsonify({"success": False, "error": str(e)}), 400

@app.route("/update_status", methods=["POST"])
def update_status():
    try:
        data = request.json or {}
        trader_id = data.get("id")

        update_data = {}
        for field in [
            "status", "phase", "balance", "equity",
            "profit", "drawdown", "profit_percent",
            "drawdown_percent", "engine_group"
        ]:
            if field in data:
                update_data[field] = data[field]

        if not trader_id:
            return jsonify({"success": False, "error": "Missing trader id"}), 400

        if not update_data:
            return jsonify({"success": False, "error": "Nothing to update"}), 400

        res = supabase.table("traders").update(update_data).eq("id", trader_id).execute()
        return jsonify({"success": True, "data": res.data})

    except Exception as e:
        print("UPDATE ERROR:", repr(e))
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

@app.route("/activate_trader", methods=["POST"])
def activate_trader():
    try:
        trader_id = (request.json or {}).get("id")
        res = supabase.table("traders").update({"status": "active"}).eq("id", trader_id).execute()
        return jsonify({"success": True, "data": res.data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@app.route("/deactivate_trader", methods=["POST"])
def deactivate_trader():
    try:
        trader_id = (request.json or {}).get("id")
        res = supabase.table("traders").update({"status": "inactive"}).eq("id", trader_id).execute()
        return jsonify({"success": True, "data": res.data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
