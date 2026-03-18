"""
broadband_prompts.py
====================
AI prompt logic and Flask routes for Broadband / Internet Services.
Imported and registered by app.py -- do not run directly.
"""

from datetime import datetime, timezone
from flask import jsonify, Response
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
    """Builds a concise, generic broadband prompt."""

    diag_block = ""
    if billing_context or connection_context:
        diag_block = "\n\nAUTOMATED DIAGNOSTICS -- reference these values directly in your steps:\n"
        if billing_context:
            diag_block += f"Billing check: {billing_context}\n"
        if connection_context:
            diag_block += f"Connection check: {connection_context}\n"
        diag_block += "Use the diagnostic data explicitly when it helps the customer.\n"

    return (
        f"You are a broadband and Wi-Fi support specialist. The customer is in 'Broadband / Internet Services' under '{subprocess_name}'. "
        f"This is solution attempt #{attempt}.\n\n"
        "Respond with ONE concise solution tailored to the customer's query. Provide 3-5 short, numbered steps the customer can do right now at home with no special tools.\n"
        "Rules: stay within broadband/wi-fi/router context; keep every step beginner-friendly; avoid deep router admin changes unless essential (and then give the exact menu path); avoid mobile network steps; avoid telling them to contact support or schedule a technician unless absolutely necessary; keep wording clear and brief.\n"
        "Use any diagnostic data (plan speed, measured speed, latency, line status) to make the advice specific.\n"
        + diag_block
        + query_block
        + context_block
        + prev_block
        + "\nDo NOT include any URLs or hyperlinks.\n"
        f"Respond entirely in {language}."
    )


# ─── Flask routes ────────────────────────────────────────────────────────────

def register_routes(app):
    """Register all /api/broadband/* routes on the Flask app."""

    # Pre-generate a 2MB payload for the speed test endpoint to avoid repeated allocations.
    speedtest_bytes = b"0" * (2 * 1024 * 1024)

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

    @app.route("/api/broadband/speedtest-file", methods=["GET"])
    @jwt_required()
    def broadband_speedtest_file():
        """Returns a 2MB dummy payload to measure download speed from the browser."""
        resp = Response(speedtest_bytes, mimetype="application/octet-stream")
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        resp.headers["Content-Length"] = str(len(speedtest_bytes))
        resp.headers["Content-Disposition"] = 'attachment; filename="speedtest.bin"'
        return resp

    @app.route("/api/broadband/ping", methods=["GET"])
    @jwt_required()
    def broadband_ping():
        """Lightweight latency check endpoint."""
        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return jsonify({"ok": True, "timestamp": ts_ms}), 200
