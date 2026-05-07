from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client
import os

app = Flask(**name**)
CORS(app)

SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_KEY"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route("/")
def home():
return jsonify({
"status": "NairaPips API Running"
})

@app.route("/traders", methods=["GET"])
def get_traders():

```
response = supabase.table("traders").select("*").execute()

return jsonify(response.data)
```

@app.route("/traders", methods=["POST"])
def add_trader():

```
data = request.json

response = supabase.table("traders").insert(data).execute()

return jsonify({
    "success": True,
    "data": response.data
})
```

@app.route("/delete_trader", methods=["POST"])
def delete_trader():

```
data = request.json

trader_id = data.get("id")

supabase.table("traders").delete().eq("id", trader_id).execute()

return jsonify({
    "success": True
})
```

@app.route("/activate_trader", methods=["POST"])
def activate_trader():

```
data = request.json

trader_id = data.get("id")

supabase.table("traders").update({
    "status": "active"
}).eq("id", trader_id).execute()

return jsonify({
    "success": True
})
```

@app.route("/deactivate_trader", methods=["POST"])
def deactivate_trader():

```
data = request.json

trader_id = data.get("id")

supabase.table("traders").update({
    "status": "inactive"
}).eq("id", trader_id).execute()

return jsonify({
    "success": True
})
```

if **name** == "**main**":
app.run(host="0.0.0.0", port=5000)
