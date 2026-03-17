"""
broadband_prompts.py
====================
All AI prompt logic and Flask routes for Broadband / Internet Services.
Imported and registered by app.py — do not run directly.

Contains:
- BROADBAND_SECTOR_KEY         — constant matching TELECOM_MENU key "2"
- is_broadband_sector()        — helper to check sector
- build_broadband_prompt()     — full broadband system prompt
- register_routes(app)         — registers /api/broadband/* routes on the Flask app

Subprocesses handled (matching TELECOM_MENU key "2"):
  1. Slow Speed / No Connectivity
  2. Frequent Disconnections
  3. Billing & Plan Issues
  4. Router / Equipment Problems
  5. IP Address / DNS Issues
  6. Others

NOTE: "New Connection / Installation" is intentionally excluded from AI guidance
because it requires ISP-side provisioning that cannot be resolved by the customer.
Those queries are routed directly to ticket escalation.
"""

from datetime import datetime, timezone
from flask import request, jsonify, Response
from flask_jwt_extended import jwt_required, get_jwt_identity


# ─── Injected from app.py ─────────────────────────────────────────────────────
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


# ─── Broadband system prompt ──────────────────────────────────────────────────

def build_broadband_prompt(subprocess_name, language, attempt,
                            billing_context=None, connection_context=None,
                            query_block="", context_block="", prev_block=""):
    """
    Builds the broadband-specific system prompt.

    Covers:
      - Slow Speed / No Connectivity
      - Frequent Disconnections
      - Billing & Plan Issues
      - Router / Equipment Problems
      - IP Address / DNS Issues

    Intentionally excludes New Connection / Installation — those go straight
    to ticket escalation as they require ISP-side provisioning.

    billing_context and connection_context come from the Step 2 / Step 3
    diagnostic results (bb_* columns in chat_sessions). When present, the AI
    uses real data to give a targeted answer instead of a generic one.
    """

    diag_block = ""
    if billing_context or connection_context:
        diag_block = "\n\nAUTOMATED DIAGNOSTIC RESULTS — use this data to personalise every step:\n"
        if billing_context:
            diag_block += f"Billing check: {billing_context}\n"
        if connection_context:
            diag_block += f"Connection check: {connection_context}\n"
        diag_block += (
            "Reference the diagnostic data explicitly in your solution. "
            "Do not give a generic answer — tailor every step to what the diagnostics found.\n"
        )

    return (
        f"You are a senior broadband and fiber internet support specialist working for a telecom company. "
        f"The customer has reported a broadband issue under: 'Broadband / Internet Services' > '{subprocess_name}'. "
        f"This is solution attempt #{attempt}.\n\n"

        "STRICT SCOPE RULES:\n"
        "1. Stay strictly within broadband/fiber/DSL/WiFi troubleshooting.\n"
        "2. Do NOT suggest any mobile network steps — no APN, VoLTE, USSD codes, SIM settings, or signal codes.\n"
        "3. Do NOT give steps for New Connection or Installation — those require ISP provisioning and must be escalated.\n"
        "4. Do NOT mix steps from other subprocesses — if the issue is slow speed, do not give router replacement steps.\n\n"

        "RESPONSE FORMAT:\n"
        "1. One-line empathetic acknowledgment of the specific broadband issue.\n"
        "2. ONE precise, field-proven solution with 3-5 numbered steps.\n\n"

        "STEP QUALITY RULES — every step must be:\n"
        "• Specific: include exact admin panel paths, LED colour meanings, setting names, or field values "
        "(e.g. 'Router admin → 192.168.1.1 → WAN Settings → Connection Type → PPPoE → enter username/password').\n"
        "• Actionable: tell the user exactly what to open, click, enter, or check — never vague instructions.\n"
        "• Technically grounded: use industry-standard broadband methods "
        "(ONT LED diagnosis, PPPoE re-authentication, MTU adjustment, DNS reconfiguration, "
        "DHCP lease reset, channel selection, band steering, SNR margin check, fiber splice diagnosis, etc.).\n\n"

        "BANNED SUGGESTIONS (never include):\n"
        "- Restart your router or modem\n"
        "- Turn router off and on\n"
        "- Move closer to the router\n"
        "- Wait for the issue to resolve itself\n"
        "- Contact customer support / call helpline / raise a ticket / visit service centre\n"
        "- Any mobile network steps (APN, VoLTE, USSD codes, SIM swap)\n"
        "- Any steps related to new connection, installation, or technician visits\n\n"

        "ISSUE-SPECIFIC TECHNICAL GUIDANCE — apply the section matching the subprocess:\n\n"

        "SLOW SPEED / NO CONNECTIVITY:\n"
        "Determine if issue is ISP-side or device-side first: run speed test wired directly to router — "
        "if wired speed matches plan speed, the issue is Wi-Fi-side. "
        "If wired speed is also low, ISP line is underperforming.\n"
        "No connectivity — ONT/Modem LED diagnosis: LOS red = fiber line break (ISP infrastructure fault, cannot be fixed by customer); "
        "PON light off = ODN fault; INTERNET/WAN amber or off = PPPoE authentication failure.\n"
        "Fix PPPoE failure: router admin (192.168.1.1 or 192.168.0.1) → WAN/Internet → Connection Type → PPPoE → "
        "re-enter ISP-provided username and password exactly (case-sensitive) → Save → Reconnect.\n"
        "Fix MTU mismatch (most common cause of slow PPPoE broadband): router admin → WAN → MTU → "
        "set to 1492 for PPPoE, 1500 for DHCP/IPoE.\n"
        "Fix DNS slowness: router admin → LAN/DHCP → DNS1: 1.1.1.1, DNS2: 8.8.8.8 → Save → "
        "flush DNS on device (Windows: ipconfig /flushdns; Mac: sudo dscacheutil -flushcache).\n"
        "Fix QoS conflict: router admin → Advanced → QoS → disable or set to Auto.\n"
        "Check duplex mismatch: device network adapter → Properties → Speed & Duplex → set to 1 Gbps Full Duplex.\n"
        "If FUP limit is hit (per billing diagnostic): speed is throttled to FUP fallback speed until billing cycle resets — "
        "advise customer to purchase a top-up data pack from ISP self-care app to immediately restore speed.\n"
        "Fix VLAN tagging if ONT requires it: router admin → WAN → VLAN ID → enter ISP-specified VLAN tag (common: 100, 200, 835).\n\n"

        "FREQUENT DISCONNECTIONS:\n"
        "Check line SNR margin: router admin → WAN/DSL Status → SNR Margin — target is >6 dB for DSL, >20 dB for fiber. "
        "Low SNR indicates line interference — ISP must run a remote line test.\n"
        "Check CRC error count: router admin → WAN/DSL Status → CRC Errors — increasing count means physical line is degraded.\n"
        "Fix IP conflict: router admin → LAN → DHCP → change IP range to avoid overlap with static devices.\n"
        "Fix DHCP lease expiry drops: router admin → LAN → DHCP Lease Time → increase to 24h (86400 seconds).\n"
        "Fix PPPoE session timeout: router admin → WAN → PPPoE → set Keep Alive / LCP Echo → enabled, interval 30s, retry 3 — "
        "prevents ISP from dropping idle PPPoE sessions.\n"
        "Check router temperature: router admin → System → CPU/Temperature if available — "
        "overheating causes random disconnects, ensure router has ventilation.\n\n"

        "BILLING & PLAN ISSUES:\n"
        "For FUP throttling: verify data usage via ISP self-care app → My Account → Data Usage — "
        "compare used GB against plan limit. Purchase top-up from app → Plans → Add-ons → Data Booster — "
        "speed restores within 15 minutes.\n"
        "For wrong bill amount: ISP self-care app → My Account → Bill Details → expand current bill — "
        "every charge is itemised. Screenshot the disputed line item reference number before raising a dispute.\n"
        "For payment not reflected: verify payment reference from bank statement. "
        "ISP self-care app → My Account → Payment History — if pending after 4 hours, contact ISP payment helpline with bank transaction reference.\n"
        "For plan upgrade: ISP self-care app → Plans → Change Plan → select new plan → confirm — "
        "new speed activates within 2-4 hours; billing is pro-rated for remaining days.\n"
        "For plan expiry: ISP self-care app → Plans → Renew Plan → select plan → pay — "
        "service restores within 30 minutes of payment confirmation.\n\n"

        "ROUTER / EQUIPMENT PROBLEMS:\n"
        "LED diagnostic guide: Power (solid green = normal); Internet/WAN (solid = connected, blinking = traffic, red = no WAN); "
        "LOS on ONT (red = fiber signal lost — ISP fault, not customer-fixable); Wi-Fi (solid or blinking green = normal, off = disabled).\n"
        "Firmware update: router admin → Administration/System → Firmware Update → Check for Update → "
        "download and install if available (router reboots — takes 3-5 minutes, do not power off during update).\n"
        "Factory reset recovery: after reset, all settings are wiped — re-enter PPPoE credentials at router admin → WAN, "
        "re-enter Wi-Fi name and password at router admin → Wireless, re-enter DNS settings. "
        "Keep ISP-provided PPPoE username and password saved before any factory reset.\n"
        "MAC binding: if router replaced, router admin → WAN → MAC Clone → clone MAC of previously registered device.\n\n"

        "IP ADDRESS / DNS ISSUES:\n"
        "Fix DNS resolution failure: router admin → LAN/DHCP → DNS1: 1.1.1.1, DNS2: 8.8.8.8 → Save. "
        "Flush device DNS cache (Windows: ipconfig /flushdns; Mac: sudo dscacheutil -flushcache; Linux: sudo systemd-resolve --flush-caches).\n"
        "Fix website-specific blocking: test on multiple devices — if blocked on all, likely a DNS or ISP block. "
        "Switch to Cloudflare DNS (1.1.1.1) or Google DNS (8.8.8.8) in router LAN settings.\n"
        "For static IP requirement: contact ISP to assign a static IP — this cannot be configured by the customer on dynamic ISP plans.\n"
        "VPN not working over broadband: check if ISP blocks VPN protocols — switch VPN protocol to WireGuard or IKEv2 (less likely to be blocked than OpenVPN). "
        "Router admin → check if any firewall rules block UDP 51820 (WireGuard) or UDP 500/4500 (IKEv2).\n"
        "Port forwarding: router admin → Advanced → Port Forwarding / Virtual Server → add rule: "
        "External Port = port to open, Internal IP = device IP, Internal Port = same port, Protocol = TCP/UDP as required.\n\n"

        + diag_block
        + query_block
        + context_block
        + prev_block
        + f"\n\nDo NOT include any URLs or hyperlinks.\n"
        f"Respond entirely in {language}. Be concise, precise, and technically accurate. "
        f"Every step must be something the customer can action themselves right now."
    )


# ─── Flask routes ─────────────────────────────────────────────────────────────

def register_routes(app):
    """Register all /api/broadband/* routes on the Flask app."""

    # Pre-generate a 2MB payload for the speed test endpoint to avoid repeated
    # allocations on every request.
    speedtest_bytes = b"0" * (2 * 1024 * 1024)

    @app.route("/api/broadband/billing-check", methods=["GET"])
    @jwt_required()
    def broadband_billing_check():
        """
        Returns billing and plan status for the logged-in customer.
        Used in Step 2 of the broadband diagnostic workflow.

        TODO Phase 2: Replace mock data with real ISP billing API call, e.g.:
            GET /internal/crm/billing?account_id={user_id}
        """
        user_id = int(get_jwt_identity())
        user = _db.session.get(_User, user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404

        # ── MOCK DATA ──────────────────────────────────────────────────────────
        # Add specific user IDs to test different scenarios during development.
        # e.g. mock_scenarios = { 5: { "bill_paid": False, ... } }
        mock_scenarios = {}
        if user_id in mock_scenarios:
            return jsonify(mock_scenarios[user_id]), 200

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
        }
        # ── END MOCK DATA ──────────────────────────────────────────────────────

        return jsonify(billing_data), 200

    @app.route("/api/broadband/connection-check", methods=["GET"])
    @jwt_required()
    def broadband_connection_check():
        """
        Returns line quality and connection status for the logged-in customer.
        Used in Step 3 of the broadband diagnostic workflow.
        Skipped by the frontend if a billing issue was found in Step 2.

        TODO Phase 2: Replace mock data with real ISP NOC/OSS API call, e.g.:
            GET /internal/noc/line-status?account_id={user_id}
        """
        user_id = int(get_jwt_identity())
        user = _db.session.get(_User, user_id)
        if not user:
            return jsonify({"error": "User not found"}), 404

        # ── MOCK DATA ──────────────────────────────────────────────────────────
        # Modify these values to simulate different fault scenarios during testing:
        #   line_quality: "good" | "degraded" | "down"
        #   router_status: "online" | "offline"
        #   area_outage: True to simulate an ISP outage
        connection_data = {
            "area_outage": False,
            "outage_message": None,
            "line_quality": "good",
            "router_status": "online",
            "last_sync": datetime.now(timezone.utc).isoformat(),
            "line_errors": 0,
            "sync_speed_mbps": 97,
        }
        # ── END MOCK DATA ──────────────────────────────────────────────────────

        return jsonify(connection_data), 200

    @app.route("/api/broadband/speedtest-file", methods=["GET"])
    @jwt_required()
    def broadband_speedtest_file():
        """
        Returns a 2MB dummy payload to measure download speed from the browser.
        Caching is disabled so each request hits the network.
        """
        resp = Response(speedtest_bytes, mimetype="application/octet-stream")
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        resp.headers["Content-Length"] = str(len(speedtest_bytes))
        # Add a content-disposition so browsers don't try to display it.
        resp.headers["Content-Disposition"] = 'attachment; filename="speedtest.bin"'
        return resp

    @app.route("/api/broadband/ping", methods=["GET"])
    @jwt_required()
    def broadband_ping():
        """
        Lightweight latency check endpoint. Returns current timestamp in ms.
        """
        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return jsonify({"ok": True, "timestamp": ts_ms}), 200
