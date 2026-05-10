# =========================
# SUPPORT TICKET BACKEND
# =========================

@app.route("/support_tickets", methods=["GET"])
def get_support_tickets():
    try:
        res = (
            supabase
            .table("support_tickets")
            .select("*")
            .order("created_at", desc=True)
            .execute()
        )

        return jsonify(res.data)

    except Exception as e:
        print("GET SUPPORT ERROR:", repr(e))
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400


@app.route("/create_support_ticket", methods=["POST"])
def create_support_ticket():
    try:
        data = request.json or {}

        subject = str(data.get("subject", "")).strip()
        message = str(data.get("message", "")).strip()

        if not subject or not message:
            return jsonify({
                "success": False,
                "error": "Subject and message are required"
            }), 400

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
            "message": "Support ticket created successfully",
            "data": res.data
        })

    except Exception as e:
        print("CREATE SUPPORT ERROR:", repr(e))
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400


@app.route("/reply_support_ticket", methods=["POST"])
def reply_support_ticket():
    try:
        data = request.json or {}
        ticket_id = data.get("id")

        if not ticket_id:
            return jsonify({
                "success": False,
                "error": "Missing ticket id"
            }), 400

        admin_reply = str(data.get("admin_reply", "")).strip()

        if not admin_reply:
            return jsonify({
                "success": False,
                "error": "Admin reply is required"
            }), 400

        update_data = {
            "admin_reply": admin_reply,
            "status": "replied",
            "replied_at": now_iso(),
            "last_updated_at": now_iso()
        }

        res = (
            supabase
            .table("support_tickets")
            .update(update_data)
            .eq("id", ticket_id)
            .execute()
        )

        return jsonify({
            "success": True,
            "message": "Support ticket replied successfully",
            "data": res.data
        })

    except Exception as e:
        print("REPLY SUPPORT ERROR:", repr(e))
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400


@app.route("/close_support_ticket", methods=["POST"])
def close_support_ticket():
    try:
        data = request.json or {}
        ticket_id = data.get("id")

        if not ticket_id:
            return jsonify({
                "success": False,
                "error": "Missing ticket id"
            }), 400

        update_data = {
            "status": "closed",
            "closed_at": now_iso(),
            "last_updated_at": now_iso()
        }

        res = (
            supabase
            .table("support_tickets")
            .update(update_data)
            .eq("id", ticket_id)
            .execute()
        )

        return jsonify({
            "success": True,
            "message": "Support ticket closed successfully",
            "data": res.data
        })

    except Exception as e:
        print("CLOSE SUPPORT ERROR:", repr(e))
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
