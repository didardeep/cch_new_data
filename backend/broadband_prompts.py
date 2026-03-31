"""
broadband_prompts.py
====================
AI prompt logic and Flask routes for Broadband / Internet Services.
Imported and registered by app.py -- do not run directly.
"""

from datetime import datetime, timezone
from flask import jsonify, Response, send_from_directory
from flask_jwt_extended import jwt_required, get_jwt_identity
from models import BillingAccount

# Injected from app.py
_client = None
_deployment = None
_db = None
_User = None

BROADBAND_SECTOR_KEY = "2"


def init(client, deployment_name, db, User):
    """Called once from app.py after Flask app and DB are set up."""
    global _client, _deployment, _db, _User
    _client = client
    _deployment = deployment_name
    _db = db
    _User = User


def is_broadband_sector(sector_key) -> bool:
    return str(sector_key) == BROADBAND_SECTOR_KEY


# --- Broadband system prompt -------------------------------------------------

def build_broadband_prompt(subprocess_name, language, attempt,
                            billing_context=None, connection_context=None,
                            query_block="", context_block="", prev_block=""):
    """Builds a context-aware broadband prompt that uses pre-run diagnostic results."""

    diag_block = ""
    if billing_context or connection_context:
        diag_block = (
            "\n\nAUTOMATED CHECKS ALREADY COMPLETED — the system has already run a billing check and "
            "connection check on the customer's account before this conversation. "
            "Do NOT ask the customer to check their bill, verify their plan, check account status, "
            "test their connection, or confirm if their service is active — all of this is already done.\n\n"
            "Diagnostic results:\n"
        )
        if billing_context:
            diag_block += f"  Billing: {billing_context}\n"
        if connection_context:
            diag_block += f"  Connection: {connection_context}\n"
        diag_block += (
            "\nHow to use these results:\n"
            "- 'Bill paid: No' or outstanding_amount > 0 → the unpaid bill is the likely cause; "
            "tell the customer to clear their dues and explain service will restore shortly after payment.\n"
            "- 'FUP hit: Yes' → Fair Usage Policy limit reached; speed is throttled by the provider; "
            "advise them to upgrade their plan or wait for the next billing cycle reset — "
            "no router fix will help here.\n"
            "- 'Account: Inactive' → account is suspended; advise contacting billing to reactivate — "
            "skip all router troubleshooting.\n"
            "- Bill paid, account active, FUP not hit → billing is clear; focus ONLY on "
            "router/Wi-Fi troubleshooting steps the customer can do at home right now.\n"
        )

    return (
        f"You are a broadband and Wi-Fi support specialist. "
        f"The customer is in 'Broadband / Internet Services' under '{subprocess_name}'. "
        f"This is solution attempt #{attempt}.\n\n"
        "Respond with ONE concise solution. Provide 3-5 short, numbered steps.\n\n"
        "Rules:\n"
        "- Stay within broadband/Wi-Fi/router context only\n"
        "- Assume the customer is non-technical — every step must be beginner-friendly, no jargon\n"
        "- Do NOT tell the customer to check their bill, verify their account, check if their plan is "
        "active, or test their connection speed — these have already been handled\n"
        "- Do NOT repeat any step that appears in previous solutions\n"
        "- Do NOT include URLs or hyperlinks\n"
        "- Avoid scheduling technician visits unless it is the only remaining option\n"
        + diag_block
        + query_block
        + context_block
        + prev_block
        + f"\nRespond entirely in {language}."
    )


# ─── Flask routes ────────────────────────────────────────────────────────────

def register_routes(app):
    """Register all /api/broadband/* routes on the Flask app."""

    # 25 MB of pseudo-random bytes — large enough for accurate measurements,
    # random-ish so gzip compression cannot shrink it (which would skew results).
    import os as _os
    _speedtest_payload = _os.urandom(25 * 1024 * 1024)

    @app.route("/api/broadband/billing-check", methods=["GET"])
    @jwt_required()
    def broadband_billing_check():
        """Returns billing and plan status for the logged-in customer."""
        user_id = int(get_jwt_identity())
        user = _db.session.get(_User, user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404

        acct = BillingAccount.query.filter_by(user_id=user.id).first()

        if acct:
            billing_data = acct.to_dict()
        else:
            # Fallback mock data (replace with real billing integration)
            billing_data = {
                "account_active": True,
                "plan_name": "100 Mbps Fiber",
                "plan_speed_mbps": 100,
                "bill_paid": True,
                "outstanding_amount": 0,
                "fup_hit": False,
                "fup_speed_mbps": None,
                "plan_expiry": "2026-06-15",
                "data_used_gb": 210,
                "data_limit_gb": 500,
                "customer_email": user.email,
                "customer_name": user.name,
            }

        return jsonify(billing_data), 200

    @app.route("/api/broadband/connection-check", methods=["GET"])
    @jwt_required()
    def broadband_connection_check():
        """Returns line quality and connection status for the logged-in customer."""
        user_id = int(get_jwt_identity())
        user = _db.session.get(_User, user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404

        # Mock data (replace with real NOC/OSS integration)
        connection_data = {
            "area_outage": False,
            "outage_message": None,
            "line_quality": "good",
            "router_status": "online",
            "last_sync": datetime.now(timezone.utc).isoformat(),
            "line_errors": 0,
            "sync_speed_mbps": 97,
        }
        return jsonify(connection_data), 200

    @app.route("/api/speedtest-widget", methods=["GET"])
    def speedtest_widget():
        """Serve the self-hosted speed test HTML widget (loaded inside an iframe)."""
        import os
        static_dir = os.path.join(os.path.dirname(__file__), "static", "speedtest")
        return send_from_directory(static_dir, "index.html")

    @app.route("/api/broadband/speedtest-file", methods=["GET"])
    @jwt_required()
    def broadband_speedtest_file():
        """
        Serves a 25 MB random payload for browser-side download speed measurement.
        - Random bytes prevent gzip/brotli compression from shrinking the payload (which would give falsely high speeds).
        - Content-Length is set so the browser can track progress accurately.
        - CORS headers expose Content-Length to JS fetch.
        """
        resp = Response(_speedtest_payload, mimetype="application/octet-stream")
        resp.headers["Cache-Control"]             = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"]                    = "no-cache"
        resp.headers["Content-Encoding"]          = "identity"   # disable any server-side compression
        resp.headers["Content-Length"]            = str(len(_speedtest_payload))
        resp.headers["Content-Disposition"]       = 'attachment; filename="speedtest.bin"'
        resp.headers["Access-Control-Allow-Origin"]   = "*"
        resp.headers["Access-Control-Expose-Headers"] = "Content-Length"
        return resp

    @app.route("/api/broadband/ping", methods=["GET"])
    @jwt_required()
    def broadband_ping():
        """Lightweight latency check endpoint."""
        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return jsonify({"ok": True, "timestamp": ts_ms}), 200

    @app.route("/api/broadband/speedtest-upload", methods=["POST"])
    @jwt_required()
    def broadband_speedtest_upload():
        """
        Accepts binary upload for upload speed measurement.
        Reads and discards the body — just measures bytes received.
        """
        from flask import request as _req
        body = _req.get_data(cache=False)
        return jsonify({"ok": True, "received_bytes": len(body)}), 200

    @app.route("/api/broadband/classify-connection-issue", methods=["POST"])
    @jwt_required()
    def broadband_classify_connection_issue():
        """Broadband-only classifier: detects if the user's query is about a connection/speed problem."""
        from flask import request as _req
        import json as _json
        text = (_req.json or {}).get("text", "")
        if not text:
            return jsonify({"mentions_connection_issue": False}), 200
        try:
            response = _client.chat.completions.create(
                model=_deployment,
                messages=[
                    {"role": "system", "content": (
                        "You are classifying a broadband customer's support query.\n\n"
                        "Determine: does the message semantically describe a broadband or internet "
                        "connectivity problem? This includes: slow speed, no internet, buffering, "
                        "connection dropping, high ping, latency, Wi-Fi not working, speed lower than "
                        "expected, internet cutting out, or any complaint about internet/broadband performance.\n\n"
                        'Respond with ONLY valid JSON: {"mentions_connection_issue": true/false}'
                    )},
                    {"role": "user", "content": text},
                ],
                temperature=0,
                max_tokens=20,
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()
            return jsonify(_json.loads(raw)), 200
        except Exception:
            return jsonify({"mentions_connection_issue": False}), 200
