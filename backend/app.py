"""
Telecom Customer Complaint Handling System - Backend
=====================================================
Full backend with auth, chat, tickets, and the original AI chatbot integrated.
"""

import os
import re
import json
import time
import math
import random
import string
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager, create_access_token, jwt_required, get_jwt_identity
)
from flask_mail import Mail, Message
from flask_socketio import SocketIO, join_room, emit
from openai import AzureOpenAI
from dotenv import load_dotenv
from types import SimpleNamespace
import urllib.request
import urllib.parse
import urllib.error
from flask_jwt_extended import decode_token

from sqlalchemy import case as sql_case
from sqlalchemy.orm import joinedload
from models import db, bcrypt, User, ChatSession, ChatMessage, Ticket, Feedback, SystemSetting, SlaAlert, TelecomSite, KpiData
# Add this import after other imports
from whatsapp_integration import send_whatsapp_message, format_chat_summary_for_whatsapp, format_ticket_alert_for_whatsapp
load_dotenv()

# ─── App Setup ────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", os.urandom(24).hex())
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/telecom_complaints"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["JWT_SECRET_KEY"] = os.environ.get("JWT_SECRET", "super-secret-jwt-key-change-in-prod")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=24)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max for image uploads

# Flask-Mail Configuration
app.config["MAIL_SERVER"] = os.environ.get("MAIL_SERVER", "smtp.gmail.com")
app.config["MAIL_PORT"] = int(os.environ.get("MAIL_PORT", 587))
app.config["MAIL_USE_TLS"] = os.environ.get("MAIL_USE_TLS", "True").lower() in ("true", "1", "yes")
app.config["MAIL_USERNAME"] = os.environ.get("MAIL_USERNAME")
app.config["MAIL_PASSWORD"] = os.environ.get("MAIL_PASSWORD")
app.config["MAIL_DEFAULT_SENDER"] = os.environ.get("MAIL_DEFAULT_SENDER", os.environ.get("MAIL_USERNAME"))

db.init_app(app)
bcrypt.init_app(app)
jwt = JWTManager(app)
mail = Mail(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")
CORS(
    app,
    resources={r"/api/*": {"origins": ["http://localhost:3000"]}},
    supports_credentials=True,
)


# ─── Azure OpenAI Configuration ──────────────────────────────────────────────
client = AzureOpenAI(
    api_key=os.environ.get("AZURE_OPENAI_API_KEY"),
    api_version=os.environ.get("AZURE_OPENAI_API_VERSION"),
    azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
)
DEPLOYMENT_NAME = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")



# ─── Nearest-Tower Lookup (DB-backed from admin uploads) ────────────────────────────



_SITE_DATA = []


def _load_site_data():
    """Legacy Excel loader (disabled). Site data is now loaded via admin upload into DB."""
    return


def _haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


NETWORK_PROBLEM_KPI_KEYWORDS = {
    "internet_signal": [
        "internet", "speed", "bandwidth", "data", "throughput", "latency", "packet", "prb", "resource block",
        "sinr", "rsrp", "rsrq", "cqi", "ul", "uplink", "dl", "downlink", "lte", "nr",
        "4g", "5g", "availability", "session", "traffic", "interference", "coverage"
    ],
    "call_failure": [
        "call setup", "cssr", "asr", "blocked", "failure", "accessibility", "sdcch",
        "tch", "moc", "mtc", "paging", "attach", "volte", "voice", "srvcc"
    ],
    "call_drop": [
        "drop", "cdr", "dcr", "tch drop", "rlf", "radio link failure", "handover",
        "ho ", "ho_", "speech", "retainability", "voice", "call"
    ],
}


def _normalize_problem_text(ticket: Ticket) -> str:
    return " ".join([
        (ticket.category or "").strip().lower(),
        (ticket.subcategory or "").strip().lower(),
        (ticket.description or "").strip().lower(),
    ])


def _detect_network_problem_type(ticket: Ticket) -> str:
    text = _normalize_problem_text(ticket)
    if any(x in text for x in ["call drop", "calls drop", "dropped call", "call disconnect", "drop rate"]):
        return "call_drop"
    if any(x in text for x in ["call failure", "calls fail", "unable to make call", "call not connecting", "call setup"]):
        return "call_failure"
    if "call / sms failures" in text:
        return "call_failure"
    return "internet_signal"


def _problem_type_label(problem_type: str) -> str:
    labels = {
        "internet_signal": "Internet Speed",
        "call_failure": "Call Failure",
        "call_drop": "Call Drop",
    }
    return labels.get(problem_type, "Internet Speed")


def _filter_kpi_names_for_problem(kpi_names, problem_type: str):
    keys = NETWORK_PROBLEM_KPI_KEYWORDS.get(problem_type, NETWORK_PROBLEM_KPI_KEYWORDS["internet_signal"])
    selected = [
        name for name in sorted(kpi_names)
        if any(k in (name or "").lower() for k in keys)
    ]
    if not selected:
        selected = sorted(kpi_names)[:8]
    return selected


def _period_key_for_row(row: KpiData, period: str) -> str:
    if period == "month":
        return row.date.strftime("%Y-%m")
    if period == "week":
        iso = row.date.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if period == "hour":
        return f"{row.date.strftime('%Y-%m-%d')} {row.hour:02d}:00"
    return row.date.strftime("%Y-%m-%d")


def _build_period_stats(rows, period: str):
    agg = {}
    for r in rows:
        if r.value is None:
            continue
        key = _period_key_for_row(r, period)
        agg.setdefault(key, []).append(float(r.value))
    if not agg:
        return "no data"
    ordered = sorted(agg.items(), key=lambda kv: kv[0])
    latest_key, latest_vals = ordered[-1]
    flat_vals = [v for _, vals in ordered for v in vals]
    avg_all = round(sum(flat_vals) / len(flat_vals), 4)
    return f"{latest_key}: avg={round(sum(latest_vals)/len(latest_vals), 4)}, overall_avg={avg_all}, min={round(min(flat_vals), 4)}, max={round(max(flat_vals), 4)}"


def _build_kpi_summary_text(rows, selected_kpis, data_level_label: str) -> str:
    from collections import defaultdict
    grouped = defaultdict(list)
    for r in rows:
        if r.kpi_name in selected_kpis and r.value is not None:
            grouped[r.kpi_name].append(r)
    if not grouped:
        return f"No {data_level_label} KPI data available."

    parts = []
    for kpi_name in sorted(grouped.keys()):
        kpi_rows = grouped[kpi_name]
        monthly = _build_period_stats(kpi_rows, "month")
        weekly = _build_period_stats(kpi_rows, "week")
        daily = _build_period_stats(kpi_rows, "day")
        hourly = _build_period_stats(kpi_rows, "hour")
        parts.append(
            f"- {kpi_name}\n"
            f"  Monthly: {monthly}\n"
            f"  Weekly: {weekly}\n"
            f"  Daily: {daily}\n"
            f"  Hourly: {hourly}"
        )
    return "\n".join(parts)


# ─── UPDATED: RCA text formatting helpers ────────────────────────────────────

def _normalize_ai_lines(text: str):
    """
    Split raw AI text into clean, complete lines.
    - Strips leading bullet / number markers only
    - Removes stray 'Crux:' prefix labels (all variants)
    - Deduplicates while preserving order
    - Does NOT strip **bold** from mid-line content (titles like **Title**: body are kept)
    - Does NOT truncate content — full sentences are preserved
    """
    lines = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        # Remove leading list markers: "1.", "2)", "-", "*", "•"
        line = re.sub(r"^\s*(?:[-*•]|\d+[.)]\s*)\s*", "", line).strip()
        # Remove "**Crux:**", "Crux:", "**Crux**:", etc. from the front only
        line = re.sub(r"^\*{0,2}[Cc]rux\*{0,2}\s*:?\s*", "", line).strip()
        # Only strip ** if they wrap the ENTIRE line AND there's no colon separator
        # i.e. "**Some Title**" alone → "Some Title"
        # but "**Title**: body text" is kept as-is (bold title with body)
        if re.match(r"^\*\*[^*]+\*\*$", line):
            line = re.sub(r"^\*\*(.*?)\*\*$", r"\1", line).strip()
        if not line:
            continue
        lines.append(line)

    # Deduplicate while preserving order
    unique, seen = [], set()
    for line in lines:
        key = line.lower()
        if key not in seen:
            seen.add(key)
            unique.append(line)

    # Drop the last line if it looks truncated (no closing punctuation).
    # This happens when the AI response is cut by max_tokens mid-sentence.
    if unique and not re.search(r'[.!?)>]$', unique[-1].rstrip()):
        unique = unique[:-1]

    return unique


def _force_numbered_points(raw_text: str, min_points: int, max_points: int, prefix: str = "", fallback_points=None):
    """
    Parse AI output into clean numbered points.
    - Full sentences preserved — no character truncation
    - No 'Crux:' prefix injected (prefix param kept for API compat but defaults to "")
    - Bold markdown (**Title**: body) preserved as-is for frontend rendering
    - Picks most technically relevant lines first
    """
    lines = _normalize_ai_lines(raw_text)

    # Score lines: prefer those with technical/diagnostic keywords and longer content
    ranked = sorted(
        lines,
        key=lambda l: (
            1 if re.search(r"\b(root cause|cause|kpi|trend|alarm|site|cell|impact|action|recommend)\b", l, re.IGNORECASE) else 0,
            len(l),
        ),
        reverse=True,
    )
    picked = []
    for line in ranked:
        if len(line) < 12:
            continue
        picked.append(line)  # No truncation — keep full sentence
        if len(picked) >= max_points:
            break

    for line in (fallback_points or []):
        if len(picked) >= min_points:
            break
        if line and line not in picked:
            picked.append(line)

    picked = picked[:max_points]
    if not picked:
        picked = ["Analysis could not be generated from available data."]

    # prefix param ignored (was used for "**Crux:** " — removed)
    return "\n".join(f"{idx + 1}. {line}" for idx, line in enumerate(picked))


def _strip_markdown_for_pdf(text: str) -> str:
    """Remove **bold** and *italic* markdown markers for plain-text PDF output."""
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"\*(.*?)\*", r"\1", text)
    return text


def _format_points_for_pdf(raw_text: str) -> str:
    """
    Convert numbered markdown points to clean plain-text for PDF sections.
    Input:  "1. **Site Offline**: The site is off air due to power failure.\n2. ..."
    Output: "1. Site Offline: The site is off air due to power failure.\n\n2. ..."
    """
    lines = _normalize_ai_lines(raw_text)
    plain_lines = []
    for i, line in enumerate(lines[:5], 1):
        plain = _strip_markdown_for_pdf(line)
        plain_lines.append(f"{i}. {plain}")
    return "\n\n".join(plain_lines)

# ─────────────────────────────────────────────────────────────────────────────


# ─── Socket.IO Helpers ───────────────────────────────────────────────────────

def _emit_session_message(msg: ChatMessage):
    try:
        socketio.emit("new_message", msg.to_dict(), room=f"session_{msg.session_id}")
    except Exception:
        pass


def _emit_session_update(session: ChatSession):
    try:
        socketio.emit("session_updated", {"session_id": session.id, "status": session.status}, room=f"session_{session.id}")
    except Exception:
        pass


@socketio.on("join_session")
def on_join_session(data):
    data = data or {}
    token = data.get("token")
    session_id = data.get("session_id")
    if not token or not session_id:
        emit("error", {"error": "token and session_id required"})
        return
    try:
        decoded = decode_token(token)
        user_id = int(decoded.get("sub"))
    except Exception:
        emit("error", {"error": "invalid token"})
        return
    session = ChatSession.query.get(session_id)
    if not session:
        emit("error", {"error": "session not found"})
        return
    user = User.query.get(user_id)
    if not user:
        emit("error", {"error": "user not found"})
        return
    if user.role != "human_agent" and session.user_id != user_id:
        emit("error", {"error": "unauthorized"})
        return
    join_room(f"session_{session_id}")
    emit("joined", {"session_id": session_id})


def find_nearest_sites(lat, lon, n=3):
    """Return the n nearest telecom sites to the given coordinates from DB uploads."""
    if lat is None or lon is None:
        return []

    sites = TelecomSite.query.all()
    if not sites:
        return []

    scored = []
    for site in sites:
        dist = _haversine(lat, lon, site.latitude, site.longitude)
        scored.append((dist, site))

    scored.sort(key=lambda x: x[0])
    results = []
    for dist, site in scored[:n]:
        status = (site.site_status or "on_air").upper()
        results.append({
            "site_id": site.site_id,
            "zone": site.zone or "",
            "latitude": site.latitude,
            "longitude": site.longitude,
            "status": status,
            "alarm": site.alarms or "None",
            "solution": site.solution or "No action required",
            "distance_km": round(dist, 2),
        })
    return results


# Excel loading disabled; site data comes from admin upload into DB.

# ═══════════════════════════════════════════════════════════════════════════════
#  CHATBOT CODE 
# ═══════════════════════════════════════════════════════════════════════════════

TELECOM_MENU = {
    "1": {
        "name": "Mobile Services (Prepaid / Postpaid)",
        "icon": "",
        "description": "Covers all issues related to mobile phone services including voice calls, SMS, mobile data, SIM cards, prepaid recharges, postpaid billing, roaming, number portability, and mobile network coverage.",
        "subprocesses": {
            "1": {"name": "Billing & Payment Issues", "semantic_scope": "Unexpected charges, wrong bill amount, double billing, payment failed but money deducted, recharge not credited, balance deducted without usage, auto-renewal charged, EMI issues on phone, refund not received for telecom services, incorrect tax on bill, bill dispute"},
            "2": {"name": "Network / Signal Problems", "semantic_scope": "No signal, weak signal, call drops, poor network coverage, network congestion, unable to make/receive calls, tower issue, dead zone, indoor coverage problem, 4G/5G not available, network outage in area"},
            "3": {"name": "SIM Card & Activation", "semantic_scope": "New SIM not activated, SIM blocked, SIM damaged, SIM swap, eSIM activation, lost SIM replacement, SIM not detected, PUK locked, KYC verification pending, Aadhaar linking with SIM, SIM upgrade to 4G/5G"},
            "4": {"name": "Data Plan & Recharge Issues", "semantic_scope": "Data not working after recharge, wrong plan activated, data exhausted too quickly, unable to recharge, recharge failed but amount debited, validity not extended, data speed throttled, unlimited plan not giving unlimited data, add-on pack issues, coupon/promo code not working"},
            "5": {"name": "International Roaming", "semantic_scope": "Roaming not working abroad, high roaming charges, incoming calls charged during roaming, data roaming activation, roaming pack not applied, unable to call from foreign country, roaming bill shock, ISD/STD calling issues"},
            "6": {"name": "Mobile Number Portability (MNP)", "semantic_scope": "Want to switch operator, MNP request rejected, porting delay, UPC code not received, number lost during porting, services disrupted after porting, porting to another network, port-out issues"},
            "7": {"name": "Call / SMS Failures", "semantic_scope": "Unable to make calls, calls not connecting, one-way audio, SMS not being delivered, SMS not received, OTP not coming, call going to voicemail, DND (Do Not Disturb) issues, spam calls, call forwarding not working, conference call issues"},
            "8": {"name": "Others", "semantic_scope": ""},
        },
    },
    "2": {
        "name": "Broadband / Internet Services",
        "icon": "",
        "description": "Covers all issues related to wired/wireless broadband, fiber internet, DSL connections, WiFi, and home/office internet services.",
        "subprocesses": {
            "1": {"name": "Slow Speed / No Connectivity", "semantic_scope": "Internet too slow, speed not matching plan, buffering while streaming, downloads very slow, no internet connection, WiFi connected but no internet, speed drops at night, latency/ping too high, speed test showing low results, bandwidth issue"},
            "2": {"name": "Frequent Disconnections", "semantic_scope": "Internet keeps disconnecting, connection drops every few minutes, unstable connection, intermittent connectivity, WiFi drops frequently, connection resets, have to restart router repeatedly, disconnects during video calls"},
            "3": {"name": "Billing & Plan Issues", "semantic_scope": "Wrong broadband bill, overcharged, plan upgrade/downgrade issues, FUP limit reached, auto-debit failed, payment not reflected, want to change plan, hidden charges, installation charges disputed, security deposit refund"},
            "4": {"name": "New Connection / Installation", "semantic_scope": "New broadband connection request, installation delayed, technician not showing up, fiber cable not laid, connection pending, availability check, shift connection to new address, relocation of broadband"},
            "5": {"name": "Router / Equipment Problems", "semantic_scope": "Router not working, WiFi router faulty, modem blinking red, ONT device issue, router overheating, need router replacement, firmware update problem, WiFi range too short, LAN port not working, equipment return"},
            "6": {"name": "IP Address / DNS Issues", "semantic_scope": "Cannot access certain websites, DNS resolution failure, need static IP, IP blocked, website loading error, proxy issues, VPN not working over broadband, port forwarding needed"},
            "7": {"name": "Others", "semantic_scope": ""},
        },
    },
    "3": {
        "name": "DTH / Cable TV Services",
        "icon": "",
        "description": "Covers all issues related to Direct-To-Home television, cable TV, set-top boxes, and TV channel subscriptions.",
        "subprocesses": {
            "1": {"name": "Channel Not Working / Missing", "semantic_scope": "Channel not showing, channel removed from pack, channel black screen, paid channel not available, regional channel missing, HD channel not working, channel list changed, favorite channel gone"},
            "2": {"name": "Set-Top Box Issues", "semantic_scope": "Set-top box not turning on, remote not working, set-top box hanging/freezing, recording not working, set-top box overheating, display error on box, need set-top box replacement, software update stuck, box showing boot loop"},
            "3": {"name": "Billing & Subscription", "semantic_scope": "Wrong DTH bill, subscription expired, auto-renewal issue, pack change charges, NCF charges too high, channel added without consent, refund not received, wallet recharge failed, monthly charges incorrect"},
            "4": {"name": "Signal / Picture Quality", "semantic_scope": "No signal on TV, picture breaking/pixelating, rain causing signal loss, dish alignment needed, weak signal, audio out of sync, color distortion, signal loss at certain times, frozen picture, horizontal lines on TV"},
            "5": {"name": "Package / Plan Changes", "semantic_scope": "Want to change channel pack, upgrade to HD, add premium channels, downgrade plan, customize channel selection, regional pack addition, sports pack subscription, plan comparison, best value pack"},
            "6": {"name": "Others", "semantic_scope": ""},
        },
    },
    "4": {
        "name": "Landline / Fixed Line Services",
        "icon": "",
        "description": "Covers all issues related to traditional landline phone services, fixed-line connections, and wired telephone services.",
        "subprocesses": {
            "1": {"name": "No Dial Tone / Dead Line", "semantic_scope": "Landline not working, no dial tone, line dead, phone silent, no sound when picking up receiver, line suddenly stopped working, connection cut off, cable damaged"},
            "2": {"name": "Call Quality Issues (Noise / Echo)", "semantic_scope": "Static noise on landline, echo during calls, crackling sound, voice breaking, cross-connection hearing other conversations, humming noise, low volume on calls, distorted audio"},
            "3": {"name": "Billing & Charges", "semantic_scope": "Landline bill too high, calls charged incorrectly, wrong number dialed charges, rental overcharged, payment not updated, metered vs unlimited plan dispute, ISD charges on landline"},
            "4": {"name": "New Connection / Disconnection", "semantic_scope": "Want new landline connection, disconnection request, temporary suspension, connection shifting to new address, reconnection after disconnection, transfer of ownership"},
            "5": {"name": "Fault Repair Request", "semantic_scope": "Cable cut in area, junction box damaged, overhead wire fallen, underground cable fault, technician visit needed, repeated fault in same line, wet cable causing issues, maintenance request"},
            "6": {"name": "Others", "semantic_scope": ""},
        },
    },
    "5": {
        "name": "Enterprise / Business Solutions",
        "icon": "",
        "description": "Covers all issues related to business/corporate telecom solutions including leased lines, SLA-based services, bulk connections, cloud telephony, and managed network services.",
        "subprocesses": {
            "1": {"name": "SLA Breach / Service Downtime", "semantic_scope": "Service level agreement not met, uptime guarantee violated, business internet down, prolonged outage affecting business, compensation for downtime, SLA penalty claim, response time exceeded"},
            "2": {"name": "Leased Line / Dedicated Connection", "semantic_scope": "Leased line down, dedicated bandwidth not delivered, point-to-point link failure, MPLS circuit issue, last mile connectivity problem, fiber cut affecting leased line, jitter/latency on dedicated line"},
            "3": {"name": "Bulk / Corporate Plan Issues", "semantic_scope": "Corporate plan benefits not applied, bulk SIM management, employee connection issues, CUG (Closed User Group) problem, corporate billing discrepancy, group plan changes"},
            "4": {"name": "Cloud / VPN / MPLS Issues", "semantic_scope": "VPN tunnel down, MPLS network unreachable, cloud connectivity slow, SD-WAN issue, site-to-site VPN failure, enterprise cloud access problem, managed WiFi for office not working"},
            "5": {"name": "Technical Support Escalation", "semantic_scope": "Need senior technician, previous complaint not resolved, multiple complaints on same issue, want to escalate to manager, technical team not responding, critical issue needs immediate attention"},
            "6": {"name": "Others", "semantic_scope": ""},
        },
    },
}


def get_subprocess_details(sector_key: str) -> str:
    sector = TELECOM_MENU[sector_key]
    details = []
    for k, v in sector["subprocesses"].items():
        if isinstance(v, dict) and v["name"] != "Others":
            details.append(f'SUBPROCESS: "{v["name"]}"\n  Typical issues: {v["semantic_scope"]}')
    return "\n\n".join(details)


def get_subprocess_name(sector_key: str, subprocess_key: str) -> str:
    sector = TELECOM_MENU.get(sector_key, {})
    sp = sector.get("subprocesses", {}).get(subprocess_key, {})
    if isinstance(sp, dict):
        return sp.get("name", "Others")
    return sp if isinstance(sp, str) else "Others"


def is_telecom_related(query: str, sector_name=None, subprocess_name=None) -> bool:
    context_block = ""
    if sector_name:
        context_block = (
            f'\n\n── USER\'S MENU NAVIGATION ──\n'
            f'The user already selected telecom sector: "{sector_name}"'
        )
        if subprocess_name:
            context_block += f'\nThey also selected subprocess: "{subprocess_name}"'
        context_block += (
            "\n\nBecause the user navigated a TELECOM complaint menu to reach this point, "
            "their query is almost certainly telecom-related. Generic complaints like "
            " 'money deducted', 'service not working', 'bad experience', 'want refund', "
            " 'not getting what I paid for' etc. should be interpreted in the telecom context.\n"
            "Only classify as NOT telecom if the query is EXPLICITLY about a completely "
            "different industry."
        )
    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    "You are a semantic intent classifier for a TELECOM complaint chatbot.\n\n"
                    "Your job is to determine whether the user's query is related to telecommunications.\n\n"
                    "TELECOM includes (but is not limited to):\n"
                    "- Mobile phone services (calls, SMS, data, prepaid, postpaid)\n"
                    "- Internet/broadband/WiFi/fiber services\n"
                    "- DTH/cable TV/satellite TV\n"
                    "- Landline/fixed-line telephone\n"
                    "- Enterprise telecom (leased lines, VPN, MPLS, SLA)\n"
                    "- ANY billing, payment, refund, service quality, or customer care issue "
                    "related to any of the above\n\n"
                    "SEMANTIC REASONING RULES:\n"
                    "1. Focus on the USER'S INTENT, not just the words they used.\n"
                    "2. 'Money deducted' in a telecom context = telecom billing issue.\n"
                    "3. 'Service not working' in a telecom context = telecom service disruption.\n"
                    "4. Vague complaints ARE telecom if the user came through the telecom menu.\n"
                    "5. Only reject if the query is CLEARLY about a non-telecom industry.\n"
                    + context_block +
                    '\n\nRespond with ONLY this JSON (no extra text):\n'
                    '{"reasoning": "<one sentence about why>", "is_telecom": true/false}'
                )},
                {"role": "user", "content": query},
            ],
            temperature=0,
            max_tokens=120,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return result.get("is_telecom", False)
    except Exception:
        return True if sector_name else False


def identify_subprocess(query: str, sector_key: str) -> str:
    sector = TELECOM_MENU[sector_key]
    subprocess_details = get_subprocess_details(sector_key)
    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    f"You are a semantic complaint classifier for: {sector['name']}.\n"
                    f"Sector description: {sector.get('description', '')}\n\n"
                    "Below are the available subprocesses:\n\n"
                    f"{subprocess_details}\n\n"
                    "Analyze the user's complaint and determine which subprocess it belongs to.\n\n"
                    "Respond with ONLY this JSON:\n"
                    '{"reasoning": "<brief explanation>", "matched_subprocess": "<exact name>", "confidence": <0.0 to 1.0>}'
                )},
                {"role": "user", "content": query},
            ],
            temperature=0,
            max_tokens=200,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return result.get("matched_subprocess", "General Inquiry")
    except Exception:
        return "General Inquiry"


def detect_greeting(text: str) -> bool:
    """Semantically determine whether a message is a greeting in any language."""
    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    "Determine if the user's message is a greeting or salutation in ANY language or mixed language. "
                    "A greeting includes (but is not limited to): hello, hi, hey, hiya, howdy, good morning, "
                    "good afternoon, good evening, namaste, namaskar, salaam, assalamu alaikum, "
                    "bonjour, hola, ciao, salam, sat sri akal, vanakkam, adab, greetings, what's up, "
                    "yo, sup, hii, helo, hai, or informal/phonetic variants in any script. "
                    "Mixed-language greetings (e.g. 'hello aur kaise ho', 'hi there bhai') also count. "
                    'Respond with ONLY valid JSON: {"is_greeting": true} or {"is_greeting": false}'
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
        result = json.loads(raw)
        return bool(result.get("is_greeting", True))
    except Exception:
        return True   # fail-open: treat ambiguous input as a greeting


def classify_user_response(text: str) -> dict:
    """Classify user's response after a solution: is satisfied, mentions signal/network issues, or needs more help."""
    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    "You are classifying a customer's response in a telecom support chat. "
                    "The customer was just given a solution and asked 'Did this help?'\n\n"
                    "Determine:\n"
                    "1. is_satisfied: Is the user saying the issue is resolved / they are happy / it worked / thank you / yes it helped? (true/false)\n"
                    "2. mentions_signal: Does the user's message semantically relate to network signal, coverage, "
                    "poor reception, no signal, weak signal, call drops, slow internet speed, network not available, "
                    "data not working, or similar signal/network connectivity issues? (true/false)\n\n"
                    'Respond with ONLY valid JSON: {"is_satisfied": true/false, "mentions_signal": true/false}'
                )},
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=30,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        return json.loads(raw)
    except Exception:
        return {"is_satisfied": False, "mentions_signal": False}


def detect_language(text: str) -> str:
    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    "Detect the language of the following text. "
                    'Respond with ONLY: {"language": "<language_name>", "code": "<iso_code>"}'
                )},
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=50,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return result.get("language", "English")
    except Exception:
        return "English"

def _friendly_ai_error(err: Exception) -> str:
    """Return a user-friendly message when the AI provider is unavailable."""
    return (
        "I'm having trouble reaching the AI service right now. "
        "Please try again in a few moments."
    )


def generate_resolution(query, sector_name, subprocess_name, language):
    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    f"You are a senior telecom network support specialist. The customer has reported an issue "
                    f"under: '{sector_name}' > '{subprocess_name}'.\n\n"
                    "RESPONSE FORMAT:\n"
                    "1. One-line empathetic acknowledgment of the specific issue.\n"
                    "2. ONE precise, field-proven solution with 3-5 numbered steps.\n\n"
                    "STEP QUALITY RULES — every step must be:\n"
                    "• Specific: include exact menu paths, setting names, dial codes, or field values (e.g. 'Settings → Mobile Network → Preferred Network Type → 4G/LTE only').\n"
                    "• Actionable: tell the user exactly what to tap, toggle, enter, or dial — never vague instructions like 'check your settings'.\n"
                    "• Technically grounded: use industry-standard methods (APN reconfiguration, VoLTE/VoWiFi toggle, network band selection, USSD codes, eSIM re-provisioning, ONT/ONU LED diagnosis, transponder re-scan, UPC regeneration, etc.).\n\n"
                    "BANNED SUGGESTIONS (never include):\n"
                    "- Restart phone / toggle airplane mode\n"
                    "- Restart router or modem\n"
                    "- Move to open area or near a window\n"
                    "- Wait for network congestion\n"
                    "- Contact customer support / call care / raise ticket / visit service center\n\n"
                    "ISSUE-SPECIFIC TECHNICAL GUIDANCE (apply the relevant section):\n"
                    "Mobile data not working: Manually configure APN via Settings → SIM & Network → Access Point Names → Add New APN (enter operator APN name/type: default,supl; MCC/MNC per operator). Check Preferred Network Type (Settings → Mobile Network → set to LTE/4G), SIM slot assignment, and Data Roaming flag.\n"
                    "Call drops / poor voice: Enable VoLTE at Settings → Mobile Network → VoLTE Calls → ON. Enable VoWiFi at Settings → Mobile Network → Wi-Fi Calling → ON. To check/lock band: dial *#2263# (Samsung) and select preferred band (Band 3 1800MHz / Band 40 2300MHz TDD-LTE per operator).\n"
                    "Billing / wrong deduction: Dial *121# or *199# for itemised balance; *121*1# for data pack status; *123# for talktime ledger. To dispute: open carrier app → My Account → Bill Details → Dispute Transaction. Request CDR from Usage History in self-care app.\n"
                    "Plan/pack not activated: Check provisioning via *199*2# or *121*2#. For eSIM: Settings → Cellular → Add eSIM → rescan operator QR; if error, generate new QR from operator self-care app. For prepaid: dial *444# to verify active pack; retry activation via USSD after top-up.\n"
                    "Broadband / fiber slow: Diagnose via ONT LEDs — LOS red = fiber break (ISP fault); PON off = ODN issue; INTERNET amber = PPPoE auth failure. Fix PPPoE: router admin (192.168.1.1) → WAN → re-enter PPPoE credentials. Set DNS to 1.1.1.1 / 8.8.8.8 and MTU to 1492 (PPPoE) in router LAN settings.\n"
                    "DTH signal loss: Check signal strength in TV menu (target >60%). Re-scan transponders: Dish TV → Setup → Edit TP → 11090 V 30000; Tata Play → 12515 H 22000. Reactivate smart card: carrier app → Manage Device → Reactivate Smart Card (provisioning takes ~15 min).\n"
                    "MNP / Port-in stuck: Regenerate UPC by sending SMS 'PORT <10-digit number>' to 1900 (valid 4 days). Check port status: SMS 'PORTSTATUS' to 1900. If HLR not updated after 7 working days, the operator must trigger HLR refresh via NOC — initiate via self-care portal under 'Port Request Status'.\n\n"
                    "Do NOT include any URLs or hyperlinks.\n"
                    f"Respond entirely in {language}. Be concise, precise, and technically accurate."
                )},
                {"role": "user", "content": query},
            ],
            temperature=0.4,
            max_tokens=1000,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return _friendly_ai_error(e)


def generate_single_solution(sector_name, subprocess_name, language, user_query="", previous_solutions=None, attempt=1, original_query="", diagnosis_summary=""):
    """Generate a single focused solution."""
    prev_block = ""
    if previous_solutions:
        prev_block = (
            "\n\nIMPORTANT: The following solutions have ALREADY been provided and did NOT work. "
            "Do NOT repeat them. Provide a DIFFERENT approach:\n"
            + "\n---\n".join(previous_solutions[-10:])
        )

    query_block = ""
    if user_query:
        query_block = f"\n\nThe user described their specific issue as: \"{user_query}\""

    context_block = ""
    if original_query and original_query != user_query:
        context_block = f"\n\nOriginal issue description: \"{original_query}\"\nThe user's follow-up message is: \"{user_query}\""

    diagnosis_block = ""
    if diagnosis_summary:
        diagnosis_block = (
            f"\n\nSIGNAL DIAGNOSIS RESULTS: {diagnosis_summary}\n"
            "Use this diagnosis data to tailor your solution precisely. "
            "If RSRP < -100 dBm or SINR < 0 dB: the issue is cell-edge coverage — suggest network band change (*#2263# to lock a stronger band), VoLTE/VoWiFi enablement, or SIM re-provisioning to trigger HLR re-attachment. "
            "If RSRP -100 to -85 dBm: moderate signal — focus on device-side fixes (APN reconfiguration, preferred network type, VoLTE toggle). "
            "If RSRP > -85 dBm and SINR > 5 dB: signal is adequate — focus on account/provisioning issues (pack activation via USSD, APN type mismatch, IPv6 toggle, MTU adjustment)."
        )

    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    f"You are a senior telecom network support specialist. The customer has an issue "
                    f"under: '{sector_name}' > '{subprocess_name}'. This is solution attempt #{attempt}.\n\n"
                    "Provide exactly ONE precise, field-proven solution with 3-5 numbered steps. Each step must:\n"
                    "• Include exact menu paths, setting names, dial codes, or field values.\n"
                    "• Tell the user exactly what to tap, toggle, enter, or dial — no vague instructions.\n"
                    "• Use industry-standard troubleshooting methods (APN config, VoLTE/VoWiFi toggle, band locking, USSD codes, PPPoE re-auth, ONT LED diagnosis, eSIM re-provisioning, etc.).\n\n"
                    "BANNED SUGGESTIONS (never include):\n"
                    "- Restart phone / toggle airplane mode\n"
                    "- Restart router or modem\n"
                    "- Move to open area or near a window\n"
                    "- Wait for network congestion\n"
                    "- Contact support / call care / raise ticket / visit service center\n\n"
                    "ISSUE-SPECIFIC TECHNICAL GUIDANCE (apply relevant section):\n"
                    "Mobile data: Configure APN (Settings → SIM & Network → Access Point Names → New APN → enter name/type/MCC/MNC). Set Preferred Network Type to LTE/4G. Check SIM slot assignment and Data Roaming flag.\n"
                    "Call drops/voice: VoLTE: Settings → Mobile Network → VoLTE Calls → ON. VoWiFi: Settings → Mobile Network → Wi-Fi Calling → ON. Band lock: *#2263# (Samsung) → select Band 3/40 per operator.\n"
                    "Billing: Balance: *121# or *199#. Data pack: *121*1#. Talktime: *123#. Dispute via carrier app → My Account → Bill Details → Dispute Transaction. CDR from app → Usage History.\n"
                    "Plan activation: Provisioning: *199*2#. eSIM: Settings → Cellular → Add eSIM → rescan QR or generate new QR via self-care app. Prepaid pack: *444# to verify; retry via USSD post top-up.\n"
                    "Broadband/fiber: ONT LEDs: LOS red = fiber break; INTERNET amber = PPPoE failure → re-enter credentials at 192.168.1.1 → WAN. DNS: 1.1.1.1/8.8.8.8, MTU: 1492.\n"
                    "DTH: Signal check via TV menu (>60%). Dish TV transponder: 11090 V 30000. Tata Play: 12515 H 22000. Smart card: carrier app → Manage Device → Reactivate.\n"
                    "MNP/port-in: UPC: SMS 'PORT <number>' to 1900. Status: SMS 'PORTSTATUS' to 1900. HLR refresh after 7 days via self-care → Port Request Status.\n\n"
                    "Do NOT include any URLs or hyperlinks.\n"
                    + query_block
                    + context_block
                    + diagnosis_block
                    + prev_block +
                    f"\n\nRespond entirely in {language}. Be concise, precise, and technically accurate."
                )},
                {"role": "user", "content": user_query if user_query else f"I have an issue with {subprocess_name} in {sector_name}"},
            ],
            temperature=0.5,
            max_tokens=500,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return _friendly_ai_error(e)


def translate_text(text: str, target_language: str) -> str:
    if target_language.lower() in ("english", "en"):
        return text
    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": f"Translate the following text to {target_language}. Keep formatting intact. Return ONLY the translation."},
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=500,
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return text


def generate_chat_summary(messages_list, sector_name, subprocess_name):
    """Generate a summary of the chat conversation."""
    try:
        conversation = "\n".join([f"{m['sender']}: {m['content']}" for m in messages_list])
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": (
                    "Summarize this telecom support chat in 3-4 sentences. "
                    f"Category: {sector_name} > {subprocess_name}. "
                    "Include: what the issue was, what resolution was provided, and the outcome."
                )},
                {"role": "user", "content": conversation},
            ],
            temperature=0.3,
            max_tokens=200,
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return f"Chat about {sector_name} - {subprocess_name}. Customer query handled."


def analyze_signal_screenshot(image_base64):
    """Use Azure OpenAI Vision to extract signal metrics from a screenshot."""
    # Strip data URL prefix if present
    clean_b64 = re.sub(r"^data:image/[^;]+;base64,", "", image_base64)

    response = client.chat.completions.create(
        model=DEPLOYMENT_NAME,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a telecom signal analysis expert. Extract signal metrics from "
                    "the provided screenshot of a phone's service mode or signal information screen.\n\n"
                    "Extract these values:\n"
                    "- RSRP (Reference Signal Received Power) in dBm\n"
                    "- SINR (Signal to Interference plus Noise Ratio) in dB\n"
                    "- Cell ID (the cell identifier)\n\n"
                    "Return ONLY valid JSON in this exact format:\n"
                    '{"rsrp": <number or null>, "sinr": <number or null>, "cell_id": <string or null>}\n\n'
                    "If a value is not visible or cannot be determined, use null.\n"
                    "For RSRP, return just the number (e.g., -95, not '-95 dBm').\n"
                    "For SINR, return just the number (e.g., 12, not '12 dB').\n"
                    "For Cell ID, return the string value as shown."
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Extract RSRP, SINR, and Cell ID values from this signal information screenshot.",
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{clean_b64}",
                            "detail": "high",
                        },
                    },
                ],
            },
        ],
        temperature=0,
        max_tokens=200,
    )

    raw = response.choices[0].message.content.strip()
    # Extract JSON from possible markdown code block
    json_match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not json_match:
        raise ValueError("Could not parse AI response")
    extracted = json.loads(json_match.group())

    rsrp = extracted.get("rsrp")
    sinr = extracted.get("sinr")
    cell_id = extracted.get("cell_id")

    # Classify RSRP
    if rsrp is not None:
        rsrp = float(rsrp)
        if -105 <= rsrp <= -40:
            rsrp_status, rsrp_label = "green", "Good"
        elif -115 <= rsrp < -105:
            rsrp_status, rsrp_label = "amber", "Moderate"
        else:
            rsrp_status, rsrp_label = "red", "Weak"
    else:
        rsrp_status, rsrp_label = "unknown", "Not detected"

    # Classify SINR
    if sinr is not None:
        sinr = float(sinr)
        if sinr > 5:
            sinr_status, sinr_label = "green", "Good"
        elif sinr >= 0:
            sinr_status, sinr_label = "amber", "Moderate"
        else:
            sinr_status, sinr_label = "red", "Weak"
    else:
        sinr_status, sinr_label = "unknown", "Not detected"

    # Determine busy hours (9-11 AM or 6-9 PM local time)
    from datetime import datetime as dt
    now = dt.now()
    current_hour = now.hour
    is_busy_hour = (9 <= current_hour < 11) or (18 <= current_hour < 21)

    # Overall signal judgment
    statuses = [s for s in [rsrp_status, sinr_status] if s != "unknown"]
    if not statuses:
        overall = "unknown"
        overall_label = "Unable to determine signal quality"
    elif any(s == "red" for s in statuses):
        overall = "red"
        overall_label = "Poor"
    elif any(s == "amber" for s in statuses):
        overall = "amber"
        overall_label = "Moderate"
    else:
        overall = "green"
        overall_label = "Good"

    # Build summary message
    if overall == "green":
        summary = "Your signal strength is good. You should have stable connectivity in your area."
    elif overall == "amber":
        summary = "Your signal strength is moderate. You may experience occasional slowdowns or drops."
    elif overall == "red":
        summary = "Your signal strength is poor. This is likely causing the connectivity issues you're experiencing."
    else:
        summary = "We could not fully determine your signal quality from the screenshot."

    if is_busy_hour:
        summary += (
            f" Note: You are currently in peak network hours ({now.strftime('%I:%M %p')}). "
            "Network congestion during 9-11 AM and 6-9 PM can further degrade signal quality and speeds."
        )

    return {
        "rsrp": rsrp,
        "rsrp_status": rsrp_status,
        "rsrp_label": rsrp_label,
        "sinr": sinr,
        "sinr_status": sinr_status,
        "sinr_label": sinr_label,
        "cell_id": str(cell_id) if cell_id is not None else None,
        "overall_status": overall,
        "overall_label": overall_label,
        "is_busy_hour": is_busy_hour,
        "summary": summary,
    }


def generate_ref_number():
    ts = hex(int(time.time()))[2:].upper()
    rand = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"TC-{ts}-{rand}"


def validate_password(password):
    """Return an error string if the password is invalid, else None."""
    import re
    if len(password) < 7:
        return "Password must be at least 7 characters long"
    if not re.search(r"[A-Z]", password):
        return "Password must contain at least 1 uppercase letter"
    if not re.search(r"[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>/?`~]", password):
        return "Password must contain at least 1 special character"
    return None


def auto_assign_priority(query_text, subprocess_name):
    """Simple priority assignment based on keywords."""
    text = (query_text + " " + subprocess_name).lower()
    if any(w in text for w in ["urgent", "critical", "emergency", "business down", "sla breach", "escalat"]):
        return "critical"
    if any(w in text for w in ["not working", "failed", "no signal", "dead", "down", "outage"]):
        return "high"
    if any(w in text for w in ["slow", "intermittent", "billing", "wrong charge", "refund"]):
        return "medium"
    return "low"




# AUTH ROUTES


@app.route("/api/auth/register", methods=["POST"])
def register():
    data = request.json
    name = data.get("name", "").strip()
    email = data.get("email", "").strip().lower()
    phone_number = data.get("phone_number", "").strip()  # ← NEW
    password = data.get("password", "")

    if not name or not email or not password:
        return jsonify({"error": "Name, email, and password are required"}), 400

    pw_err = validate_password(password)
    if pw_err:
        return jsonify({"error": pw_err}), 400

    # ← NEW: Validate phone number
    if not phone_number:
        return jsonify({"error": "Phone number is required"}), 400

    if User.query.filter_by(email=email).first():
        return jsonify({"error": "Email already registered"}), 409

    user = User(name=name, email=email, phone_number=phone_number, role="customer")  # ← UPDATED
    user.set_password(password)
    db.session.add(user)
    db.session.commit()

    token = create_access_token(identity=str(user.id))
    return jsonify({"token": token, "user": user.to_dict()}), 201


@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.json
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")

    user = User.query.filter_by(email=email).first()
    if not user or not user.check_password(password):
        return jsonify({"error": "Invalid email or password"}), 401

    token = create_access_token(identity=str(user.id))
    return jsonify({"token": token, "user": user.to_dict()})


@app.route("/api/auth/me", methods=["GET"])
@jwt_required()
def get_me():
    user = User.query.get(int(get_jwt_identity()))
    if not user:
        return jsonify({"error": "User not found"}), 404
    return jsonify({"user": user.to_dict()})



# CHATBOT ROUTES 
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/menu", methods=["GET"])
def get_menu():
    menu = {}
    for key, sector in TELECOM_MENU.items():
        menu[key] = {"name": sector["name"], "icon": sector["icon"]}
    return jsonify({"menu": menu})


@app.route("/api/subprocesses", methods=["POST"])
def get_subprocesses():
    data = request.json
    sector_key = data.get("sector_key")
    language = data.get("language", "English")
    if sector_key not in TELECOM_MENU:
        return jsonify({"error": "Invalid sector"}), 400
    sector = TELECOM_MENU[sector_key]
    subprocesses = {}
    for k, v in sector["subprocesses"].items():
        subprocesses[k] = v["name"] if isinstance(v, dict) else v
    if language.lower() not in ("english", "en"):
        translated = {}
        for k, v in subprocesses.items():
            translated[k] = translate_text(v, language)
        subprocesses = translated
    return jsonify({"sector_name": sector["name"], "subprocesses": subprocesses})


@app.route("/api/resolve", methods=["POST"])
def resolve_complaint():
    data = request.json
    query = data.get("query", "").strip()
    sector_key = data.get("sector_key")
    subprocess_key = data.get("subprocess_key")
    selected_subprocess = data.get("selected_subprocess", "").strip()
    language = data.get("language", "English")
    if not query:
        return jsonify({"error": "Please enter your complaint/query."}), 400
    sector = TELECOM_MENU.get(sector_key, {})
    sector_name = sector.get("name", "Telecom")
    subprocess_name = selected_subprocess or get_subprocess_name(sector_key, subprocess_key)
    if not is_telecom_related(query, sector_name=sector_name, subprocess_name=subprocess_name):
        msg = (
            "I'm sorry, but I can only assist with **telecom-related** complaints. "
            "Your query doesn't appear to be telecom-related. Please try again."
        )
        translated_msg = translate_text(msg, language)
        return jsonify({"resolution": translated_msg, "is_telecom": False})
    if subprocess_name == "Others":
        subprocess_name = identify_subprocess(query, sector_key)
    resolution = generate_resolution(query, sector_name, subprocess_name, language)
    return jsonify({
        "resolution": resolution,
        "is_telecom": True,
        "identified_subprocess": subprocess_name,
    })


@app.route("/api/resolve-step", methods=["POST"])
def resolve_step():
    """Generate a single solution step. Used in the iterative resolution flow."""
    data = request.json
    sector_key = data.get("sector_key")
    subprocess_key = data.get("subprocess_key")
    selected_subprocess = data.get("selected_subprocess", "").strip()
    user_query = data.get("query", "").strip()
    language = data.get("language", "English")
    previous_solutions = data.get("previous_solutions", [])
    attempt = data.get("attempt", 1)
    original_query = data.get("original_query", "")
    diagnosis_summary = data.get("diagnosis_summary", "")

    sector = TELECOM_MENU.get(sector_key, {})
    sector_name = sector.get("name", "Telecom")
    subprocess_name = selected_subprocess or get_subprocess_name(sector_key, subprocess_key)

    # If user provided a query, check if it's telecom-related
    if user_query:
        if not is_telecom_related(user_query, sector_name=sector_name, subprocess_name=subprocess_name):
            msg = (
                "I'm sorry, but I can only assist with **telecom-related** complaints. "
                "Your query doesn't appear to be telecom-related. Please try again."
            )
            translated_msg = translate_text(msg, language)
            return jsonify({"resolution": translated_msg, "is_telecom": False})

    solution = generate_single_solution(
        sector_name, subprocess_name, language,
        user_query=user_query,
        previous_solutions=previous_solutions,
        attempt=attempt,
        original_query=original_query,
        diagnosis_summary=diagnosis_summary,
    )
    return jsonify({
        "resolution": solution,
        "is_telecom": True,
        "attempt": attempt,
    })


@app.route("/api/detect-language", methods=["POST"])
def detect_lang():
    data = request.json
    text = data.get("text", "")
    language = detect_language(text)
    return jsonify({"language": language})


@app.route("/api/detect-greeting", methods=["POST"])
def detect_greeting_route():
    data = request.json
    text = data.get("text", "")
    is_greeting = detect_greeting(text)
    return jsonify({"is_greeting": is_greeting})


@app.route("/api/classify-response", methods=["POST"])
def classify_response_route():
    """Classify user response: satisfied? mentions signal/network?"""
    data = request.json
    text = data.get("text", "")
    result = classify_user_response(text)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════════════
# CHAT SESSION ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/chat/session", methods=["POST"])
@jwt_required()
def create_chat_session():
    user_id = int(get_jwt_identity())
    session = ChatSession(user_id=user_id, status="active")
    db.session.add(session)
    db.session.commit()
    return jsonify({"session": session.to_dict()}), 201


@app.route("/api/chat/session/<int:session_id>/message", methods=["POST"])
@jwt_required()
def add_chat_message(session_id):
    user_id = int(get_jwt_identity())
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    data = request.json
    msg = ChatMessage(
        session_id=session_id,
        sender=data.get("sender", "user"),
        content=data.get("content", ""),
    )
    db.session.add(msg)

    # Update session metadata
    if data.get("sector_name"):
        session.sector_name = data["sector_name"]
    if data.get("subprocess_name"):
        session.subprocess_name = data["subprocess_name"]
    if data.get("query_text"):
        session.query_text = data["query_text"]
    if data.get("resolution"):
        session.resolution = data["resolution"]
    if data.get("language"):
        session.language = data["language"]

    db.session.commit()
    _emit_session_message(msg)
    return jsonify({"message": msg.to_dict()})
# ═══════════════════════════════════════════════════════════════════
# ADD THIS NEW ROUTE to app.py
# Place it right after the add_chat_message route
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/chat/session/<int:session_id>/location", methods=["POST"])
@jwt_required()
def save_session_location(session_id):
    """Save customer's GPS location for network signal complaints."""
    user_id = int(get_jwt_identity())
    session = ChatSession.query.get(session_id)

    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session.user_id != user_id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json or {}
    if data.get("location_description"):
        session.location_description = data.get("location_description")

    # ── Default coordinates (Gurgaon, Haryana) ────────────────────────────────
    DEFAULT_LATITUDE  = 28.4595
    DEFAULT_LONGITUDE = 77.0266

    # Always use default lat/long regardless of what the client sends
    session.latitude  = DEFAULT_LATITUDE
    session.longitude = DEFAULT_LONGITUDE

    db.session.commit()

    return jsonify({
        "message": "Location saved successfully",
        "latitude":  session.latitude,
        "longitude": session.longitude,
        "location_description": session.location_description,
    }), 200


@app.route("/api/chat/session/<int:session_id>/analyze-signal", methods=["POST"])
@jwt_required()
def analyze_signal(session_id):
    """Analyze a signal screenshot using Azure OpenAI Vision."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    session = ChatSession.query.get(session_id)

    if not session:
        return jsonify({"error": "Session not found"}), 404
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    if session.user_id != user_id and user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    image_base64 = data.get("image")
    image_data_url = data.get("image_data_url")

    if not image_base64:
        return jsonify({"error": "No image provided"}), 400

    # Limit ~5MB base64
    if len(image_base64) > 7_000_000:
        return jsonify({"error": "Image too large. Please upload a smaller screenshot."}), 400

    try:
        # Send customer screenshot to agent view (if any)
        if not image_data_url:
            image_data_url = f"data:image/png;base64,{image_base64}"
        image_msg = ChatMessage(
            session_id=session_id,
            sender="customer",
            content=f"__IMAGE__:{image_data_url}",
        )
        db.session.add(image_msg)

        result = analyze_signal_screenshot(image_base64)

        # If signal is red (Poor), do not include nearest tower sites in user-facing diagnosis

        # Save diagnosis as a bot message for chat history
        diagnosis_text = (
            f"Signal Diagnosis Results: "
            f"RSRP: {result.get('rsrp', 'N/A')} dBm ({result.get('rsrp_label', 'Unknown')}), "
            f"SINR: {result.get('sinr', 'N/A')} dB ({result.get('sinr_label', 'Unknown')}), "
            f"Cell ID: {result.get('cell_id', 'N/A')}"
        )
        if result.get("nearest_sites"):
            diagnosis_text += "\n\nNearest Sites:\n"
            for s in result["nearest_sites"]:
                diagnosis_text += (
                    f"- {s['site_id']} | Status: {s['status']} | "
                    f"Alarm: {s['alarm']} | Distance: {s['distance_km']} km\n"
                )

        msg = ChatMessage(session_id=session_id, sender="bot", content=diagnosis_text)
        db.session.add(msg)
        # Mark that diagnosis has been completed for this session
        session.diagnosis_ran = True
        db.session.commit()
        _emit_session_message(image_msg)
        _emit_session_message(msg)

        return jsonify({"diagnosis": result}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to analyze screenshot: {str(e)}"}), 500


@app.route("/api/chat/session/<int:session_id>/resolve", methods=["PUT"])
@jwt_required()
def resolve_session(session_id):
    user_id = int(get_jwt_identity())
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    session.status = "resolved"
    session.resolved_at = datetime.now(timezone.utc)
    db.session.commit()
    _emit_session_update(session)

    # Generate summary
    try:
        msgs = [{"sender": m.sender, "content": m.content} for m in session.messages]
        session.summary = generate_chat_summary(msgs, session.sector_name, session.subprocess_name)
        db.session.commit()
    except Exception:
        pass

    # ← NEW: Send WhatsApp message
    try:
        user = User.query.get(user_id)
        if user and user.phone_number:
            whatsapp_msg = format_chat_summary_for_whatsapp(session, user.name)
            result = send_whatsapp_message(user.phone_number, whatsapp_msg)
            if result["success"]:
                print(f"✅ WhatsApp sent to {user.phone_number}: {result['message_sid']}")
            else:
                print(f"⚠️  WhatsApp failed: {result['error']}")
    except Exception as e:
        print(f"⚠️  WhatsApp error: {e}")

    return jsonify({"session": session.to_dict(), "summary": session.summary})


def send_ticket_assignment_email(agent, ticket, session):
    """Send a styled HTML email to the assigned agent with ticket details."""
    if not agent or not agent.email:
        return

    sla_deadline_str = ticket.sla_deadline.strftime('%B %d, %Y at %I:%M %p UTC') if ticket.sla_deadline else 'N/A'
    description_preview = (ticket.description[:300] + '...') if ticket.description and len(ticket.description) > 300 else (ticket.description or 'N/A')

    html_body = f"""
    <div style="font-family:'Segoe UI',Arial,sans-serif;max-width:600px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,0.08);">
        <div style="background:linear-gradient(135deg,#00338d 0%,#004fc4 100%);padding:24px 30px;text-align:center;">
            <h1 style="color:#fff;margin:0;font-size:20px;font-weight:600;">New Ticket Assigned</h1>
            <p style="color:rgba(255,255,255,0.8);margin:4px 0 0;font-size:13px;">A support ticket has been assigned to you</p>
        </div>
        <div style="padding:28px 30px;">
            <p style="margin:0 0 20px;font-size:15px;color:#1e293b;">Hello <strong>{agent.name}</strong>,</p>
            <p style="margin:0 0 20px;font-size:14px;color:#475569;line-height:1.6;">
                A new customer support ticket has been assigned to you. Please review the details below:
            </p>
            <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:20px;margin-bottom:20px;">
                <table style="width:100%;border-collapse:collapse;font-size:14px;">
                    <tr><td style="padding:8px 0;color:#94a3b8;width:140px;">Reference</td><td style="padding:8px 0;color:#1e293b;font-weight:600;">{ticket.reference_number}</td></tr>
                    <tr><td style="padding:8px 0;color:#94a3b8;">Category</td><td style="padding:8px 0;color:#1e293b;">{ticket.category or 'N/A'}</td></tr>
                    <tr><td style="padding:8px 0;color:#94a3b8;">Issue Type</td><td style="padding:8px 0;color:#1e293b;">{ticket.subcategory or 'N/A'}</td></tr>
                    <tr><td style="padding:8px 0;color:#94a3b8;">Priority</td><td style="padding:8px 0;color:#1e293b;font-weight:700;">{ticket.priority.upper() if ticket.priority else 'N/A'}</td></tr>
                    <tr><td style="padding:8px 0;color:#94a3b8;">SLA Hours</td><td style="padding:8px 0;color:#1e293b;">{ticket.sla_hours or 'N/A'} hours</td></tr>
                    <tr><td style="padding:8px 0;color:#94a3b8;">SLA Deadline</td><td style="padding:8px 0;color:#dc2626;font-weight:600;">{sla_deadline_str}</td></tr>
                </table>
            </div>
            <div style="border-left:3px solid #2563eb;background:#eff6ff;border-radius:0 10px 10px 0;padding:16px 20px;margin-bottom:20px;">
                <h3 style="color:#1e40af;font-size:13px;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 10px;">Customer Description</h3>
                <p style="color:#1e293b;font-size:14px;line-height:1.7;margin:0;">{description_preview}</p>
            </div>
            <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:14px 18px;">
                <p style="margin:0;color:#1e40af;font-size:14px;font-weight:600;">Please review this ticket and begin working on it at your earliest convenience.</p>
            </div>
        </div>
        <div style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:14px 30px;text-align:center;">
            <p style="color:#94a3b8;font-size:12px;margin:0;">Customer Handling &mdash; Automated Ticket Assignment</p>
        </div>
    </div>
    """

    try:
        msg = Message(
            subject=f"New Ticket Assigned - {ticket.reference_number}",
            recipients=[agent.email],
            html=html_body,
        )
        mail.send(msg)
        print(f"Assignment email sent to agent {agent.name} at {agent.email}")
    except Exception as e:
        print(f"Agent assignment email failed: {e}")


@app.route("/api/chat/session/<int:session_id>/escalate", methods=["PUT"])
@jwt_required()
def escalate_session(session_id):
    user_id = int(get_jwt_identity())
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    session.status = "escalated"

    # Generate summary
    msgs = [{"sender": m.sender, "content": m.content} for m in session.messages]
    session.summary = generate_chat_summary(msgs, session.sector_name, session.subprocess_name)

    # Auto-assign to least-occupied online human agent
    priority = auto_assign_priority(session.query_text, session.subprocess_name)
    sla_targets = get_sla_targets()
    sla_h = sla_targets.get(priority, 48)
    now_utc = datetime.now(timezone.utc)
    sla_deadline = now_utc + timedelta(hours=sla_h)

    assigned_agent = None
    online_agents = User.query.filter_by(role="human_agent", is_online=True).all()
    if online_agents:
        # Find least occupied (fewest open tickets)
        def open_ticket_count(agent):
            return Ticket.query.filter(
                Ticket.assigned_to == agent.id,
                Ticket.status.in_(["pending", "in_progress"])
            ).count()
        assigned_agent = min(online_agents, key=open_ticket_count)
    else:
        # Fallback: assign to any human_agent with fewest open tickets
        all_agents = User.query.filter_by(role="human_agent").all()
        if all_agents:
            def open_ticket_count_any(agent):
                return Ticket.query.filter(
                    Ticket.assigned_to == agent.id,
                    Ticket.status.in_(["pending", "in_progress"])
                ).count()
            assigned_agent = min(all_agents, key=open_ticket_count_any)

    # Create ticket
    ref = generate_ref_number()
    ticket = Ticket(
        chat_session_id=session_id,
        user_id=user_id,
        reference_number=ref,
        category=session.sector_name,
        subcategory=session.subprocess_name,
        description=session.query_text,
        status="pending",
        priority=priority,
        assigned_to=assigned_agent.id if assigned_agent else None,
        sla_hours=sla_h,
        sla_deadline=sla_deadline,
    )
    db.session.add(ticket)
    db.session.commit()

    # Send WhatsApp message for ticket
    try:
        user = User.query.get(user_id)
        if user and user.phone_number:
            whatsapp_msg = format_ticket_alert_for_whatsapp(ticket, user.name, session)
            result = send_whatsapp_message(user.phone_number, whatsapp_msg)
            if result["success"]:
                print(f"✅ WhatsApp ticket alert sent to {user.phone_number}")
            else:
                print(f"⚠️  WhatsApp failed: {result['error']}")
    except Exception as e:
        print(f"⚠️  WhatsApp error: {e}")

    # Send email notification to assigned agent
    if assigned_agent:
        send_ticket_assignment_email(assigned_agent, ticket, session)

    agent_info = None
    if assigned_agent:
        agent_info = {
            "name": assigned_agent.name,
            "email": assigned_agent.email,
            "phone": assigned_agent.phone_number,
            "employee_id": assigned_agent.employee_id,
        }

    return jsonify({
        "session": session.to_dict(),
        "ticket": ticket.to_dict(),
        "assigned_agent": agent_info,
    })

@app.route("/api/chat/session/<int:session_id>/send-summary-email", methods=["POST"])
@jwt_required()
def send_summary_email(session_id):
    """Send chat summary to the user's email saved in DB."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404

    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    if session.user_id != user_id:
        return jsonify({"error": "Unauthorized"}), 403

    if not session.summary:
        return jsonify({"error": "No summary available for this session"}), 400

    # Build email HTML
    html_body = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
        <!-- Header -->
        <div style="background: linear-gradient(135deg, #00338d 0%, #004fc4 100%); padding: 24px 30px; text-align: center;">
            <h1 style="color: #ffffff; margin: 0; font-size: 20px; font-weight: 600;">Customer Handling</h1>
            <p style="color: rgba(255,255,255,0.8); margin: 4px 0 0; font-size: 13px;">Chat Summary Report</p>
        </div>

        <!-- Body -->
        <div style="padding: 30px;">
            <p style="color: #1e293b; font-size: 15px; margin: 0 0 20px;">Hello <strong>{user.name}</strong>,</p>
            <p style="color: #64748b; font-size: 14px; line-height: 1.6; margin: 0 0 24px;">
                Here is the summary of your recent support chat session:
            </p>

            <!-- Session Details -->
            <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 20px; margin-bottom: 20px;">
                <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                    <tr>
                        <td style="padding: 8px 0; color: #94a3b8; width: 130px;">Session ID</td>
                        <td style="padding: 8px 0; color: #1e293b; font-weight: 500;">#{session.id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; color: #94a3b8;">Category</td>
                        <td style="padding: 8px 0; color: #1e293b; font-weight: 500;">{session.sector_name or 'N/A'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; color: #94a3b8;">Issue Type</td>
                        <td style="padding: 8px 0; color: #1e293b; font-weight: 500;">{session.subprocess_name or 'N/A'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; color: #94a3b8;">Status</td>
                        <td style="padding: 8px 0;">
                            <span style="background: {'#ecfdf5' if session.status == 'resolved' else '#fef3c7'}; color: {'#047857' if session.status == 'resolved' else '#b45309'}; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600;">
                                {session.status.upper()}
                            </span>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; color: #94a3b8;">Language</td>
                        <td style="padding: 8px 0; color: #1e293b; font-weight: 500;">{session.language or 'English'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; color: #94a3b8;">Date</td>
                        <td style="padding: 8px 0; color: #1e293b; font-weight: 500;">{session.created_at.strftime('%B %d, %Y at %I:%M %p') if session.created_at else 'N/A'}</td>
                    </tr>
                </table>
            </div>

            <!-- Summary -->
            <div style="border-left: 3px solid #10b981; background: #f0fdf4; border-radius: 0 10px 10px 0; padding: 16px 20px; margin-bottom: 20px;">
                <h3 style="color: #047857; font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; margin: 0 0 10px;">Chat Summary</h3>
                <p style="color: #1e293b; font-size: 14px; line-height: 1.7; margin: 0;">{session.summary}</p>
            </div>

            <!-- Your Query -->
            {f'''
            <div style="background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 10px; padding: 16px 20px; margin-bottom: 20px;">
                <h3 style="color: #2563eb; font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; margin: 0 0 8px;">Your Query</h3>
                <p style="color: #1e293b; font-size: 14px; line-height: 1.6; margin: 0;">{session.query_text}</p>
            </div>
            ''' if session.query_text else ''}

            <!-- Ticket Info -->
            {f'''
            <div style="background: #fffbeb; border: 1px solid #fde68a; border-radius: 10px; padding: 16px 20px; margin-bottom: 20px;">
                <h3 style="color: #b45309; font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; margin: 0 0 8px;">Escalation Ticket</h3>
                <p style="color: #1e293b; font-size: 14px; margin: 0;">Reference: <strong>{session.ticket.reference_number}</strong></p>
            </div>
            ''' if session.ticket else ''}

            <p style="color: #64748b; font-size: 13px; line-height: 1.6; margin: 20px 0 0;">
                If you have further questions, feel free to start a new chat session anytime.
            </p>
        </div>

        <!-- Footer -->
        <div style="background: #f8fafc; border-top: 1px solid #e2e8f0; padding: 16px 30px; text-align: center;">
            <p style="color: #94a3b8; font-size: 12px; margin: 0;">Customer Handling &mdash; AI-Powered Support</p>
        </div>
    </div>
    """

    email_ok = False
    whatsapp_ok = False

    # Send email
    try:
        msg = Message(
            subject=f"Chat Summary - {session.sector_name or 'Telecom Support'} (Session #{session.id})",
            recipients=[user.email],
            html=html_body,
        )
        mail.send(msg)
        email_ok = True
    except Exception as e:
        print(f"⚠️  Email failed: {e}")

    # Send WhatsApp
    if user.phone_number:
        try:
            whatsapp_msg = format_chat_summary_for_whatsapp(session, user.name)
            result = send_whatsapp_message(user.phone_number, whatsapp_msg)
            if result["success"]:
                whatsapp_ok = True
                print(f"✅ WhatsApp summary sent to {user.phone_number}")
            else:
                print(f"⚠️  WhatsApp failed: {result['error']}")
        except Exception as e:
            print(f"⚠️  WhatsApp error: {e}")

    # Build response message
    parts = []
    if email_ok:
        parts.append(f"email ({user.email})")
    if whatsapp_ok:
        parts.append(f"WhatsApp ({user.phone_number})")

    if parts:
        return jsonify({"message": f"Summary sent to {' and '.join(parts)}", "email_sent": email_ok, "whatsapp_sent": whatsapp_ok}), 200
    else:
        return jsonify({"error": "Failed to send summary. Please try again later.", "email_sent": False, "whatsapp_sent": False}), 500


@app.route("/api/chat/session/<int:session_id>", methods=["GET"])
@jwt_required()
def get_chat_session(session_id):
    user_id = int(get_jwt_identity())
    session = db.session.get(ChatSession, session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session.user_id != user_id:
        return jsonify({"error": "Unauthorized"}), 403
    now_utc = datetime.now(timezone.utc)
    updated = False
    for m in session.messages:
        if m.sender == "agent" and m.delivered_at is None:
            m.delivered_at = now_utc
            updated = True
    if updated:
        db.session.commit()
    return jsonify({
        "session": session.to_dict(),
        "messages": [m.to_dict() for m in session.messages],
    })


@app.route("/api/chat/session/<int:session_id>/seen", methods=["POST"])
@jwt_required()
def mark_chat_seen(session_id):
    user_id = int(get_jwt_identity())
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session.user_id != user_id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json or {}
    message_ids = data.get("message_ids") or []
    if message_ids and not isinstance(message_ids, list):
        return jsonify({"error": "message_ids must be a list"}), 400

    q = ChatMessage.query.filter(
        ChatMessage.session_id == session_id,
        ChatMessage.sender == "agent",
    )
    if message_ids:
        q = q.filter(ChatMessage.id.in_(message_ids))

    now_utc = datetime.now(timezone.utc)
    updated = 0
    for m in q.all():
        if m.seen_at is None:
            m.seen_at = now_utc
            if m.delivered_at is None:
                m.delivered_at = now_utc
            updated += 1
    if updated:
        db.session.commit()

    return jsonify({"updated": updated})


@app.route("/api/chat/session/<int:session_id>/status", methods=["GET"])
@jwt_required()
def get_session_status(session_id):
    """Lightweight poll endpoint: return session status + latest bot message."""
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    ticket = Ticket.query.filter_by(chat_session_id=session_id).order_by(Ticket.created_at.desc()).first()
    # Latest bot message (so the chatbot can show it)
    latest_bot_msg = None
    for m in reversed(session.messages):
        if m.sender == "bot":
            latest_bot_msg = m.content
            break
    return jsonify({
        "session_status": session.status,
        "ticket_status": ticket.status if ticket else None,
        "ticket_reference": ticket.reference_number if ticket else None,
        "latest_bot_message": latest_bot_msg,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOMER ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/customer/dashboard", methods=["GET"])
@jwt_required()
def customer_dashboard():
    user_id = int(get_jwt_identity())

    # Single query for all chat stats
    chat_stats = db.session.query(
        ChatSession.status, db.func.count(ChatSession.id)
    ).filter_by(user_id=user_id).group_by(ChatSession.status).all()
    stat_map = dict(chat_stats)
    total = sum(stat_map.values())

    pending_tickets = Ticket.query.filter_by(user_id=user_id).filter(
        Ticket.status.in_(["pending", "in_progress"])
    ).count()

    # Fetch recent sessions with user eagerly loaded + feedback ratings in one join
    recent_sessions = db.session.query(ChatSession, Feedback.rating).outerjoin(
        Feedback, db.and_(
            Feedback.chat_session_id == ChatSession.id,
            Feedback.user_id == user_id,
        )
    ).filter(
        ChatSession.user_id == user_id
    ).options(
        joinedload(ChatSession.user)
    ).order_by(ChatSession.created_at.desc()).limit(50).all()

    sessions_data = []
    for s, rating in recent_sessions:
        sd = s.to_dict()
        sd["rating"] = rating
        sessions_data.append(sd)

    return jsonify({
        "stats": {
            "total_chats": total,
            "resolved": stat_map.get("resolved", 0),
            "escalated": stat_map.get("escalated", 0),
            "active": stat_map.get("active", 0),
            "pending_tickets": pending_tickets,
        },
        "recent_sessions": sessions_data,
    })


@app.route("/api/customer/active-session", methods=["GET"])
@jwt_required()
def customer_active_session():
    """Return the most recent active chat session for the current user, with messages."""
    user_id = int(get_jwt_identity())
    session = ChatSession.query.filter_by(user_id=user_id, status="active").order_by(
        ChatSession.created_at.desc()
    ).first()
    if not session:
        return jsonify({"session": None, "messages": []})
    return jsonify({
        "session": session.to_dict(),
        "messages": [m.to_dict() for m in session.messages],
    })


@app.route("/api/customer/pending-feedback", methods=["GET"])
@jwt_required()
def customer_pending_feedback():
    """Return resolved/escalated sessions that the user hasn't given feedback for."""
    user_id = int(get_jwt_identity())
    # Subquery: session IDs that already have feedback from this user
    feedback_session_ids = db.session.query(Feedback.chat_session_id).filter(
        Feedback.user_id == user_id,
        Feedback.chat_session_id.isnot(None),
    ).subquery()

    sessions = ChatSession.query.filter(
        ChatSession.user_id == user_id,
        ChatSession.status.in_(["resolved", "escalated"]),
        ~ChatSession.id.in_(feedback_session_ids),
    ).order_by(ChatSession.created_at.desc()).all()

    return jsonify({
        "sessions": [s.to_dict() for s in sessions],
    })


@app.route("/api/customer/sessions", methods=["GET"])
@jwt_required()
def customer_sessions():
    user_id = int(get_jwt_identity())
    sessions = ChatSession.query.filter_by(user_id=user_id).order_by(
        ChatSession.created_at.desc()
    ).all()
    return jsonify({"sessions": [s.to_dict() for s in sessions]})


@app.route("/api/customer/tickets", methods=["GET"])
@jwt_required()
def customer_tickets():
    user_id = int(get_jwt_identity())
    tickets = Ticket.query.filter_by(user_id=user_id).order_by(
        Ticket.created_at.desc()
    ).all()
    return jsonify({"tickets": [t.to_dict() for t in tickets]})


# ═══════════════════════════════════════════════════════════════════════════════
# FEEDBACK ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/feedback", methods=["POST"])
@jwt_required()
def submit_feedback():
    user_id = int(get_jwt_identity())
    data = request.json
    fb = Feedback(
        user_id=user_id,
        chat_session_id=data.get("chat_session_id"),
        rating=data.get("rating", 0),
        comment=data.get("comment", ""),
    )
    db.session.add(fb)
    db.session.commit()
    return jsonify({"feedback": fb.to_dict()}), 201


@app.route("/api/feedback/list", methods=["GET"])
@jwt_required()
def list_feedback():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role == "customer":
        feedbacks = Feedback.query.filter_by(user_id=user_id).order_by(Feedback.created_at.desc()).all()
    else:
        feedbacks = Feedback.query.order_by(Feedback.created_at.desc()).all()
    return jsonify({"feedbacks": [f.to_dict() for f in feedbacks]})


# ═══════════════════════════════════════════════════════════════════════════════
# MANAGER / CTO ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/manager/dashboard", methods=["GET"])
@jwt_required()
def manager_dashboard():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    # Chat stats — single GROUP BY
    chat_stats = db.session.query(
        ChatSession.status, db.func.count(ChatSession.id)
    ).group_by(ChatSession.status).all()
    chat_map = dict(chat_stats)
    total_chats = sum(chat_map.values())
    resolved_chats = chat_map.get("resolved", 0)
    escalated_chats = chat_map.get("escalated", 0)
    active_chats = chat_map.get("active", 0)

    # Ticket stats — single GROUP BY for status
    ticket_stats = db.session.query(
        Ticket.status, db.func.count(Ticket.id)
    ).group_by(Ticket.status).all()
    ts_map = dict(ticket_stats)
    total_tickets = sum(ts_map.values())
    pending_tickets = ts_map.get("pending", 0)
    in_progress_tickets = ts_map.get("in_progress", 0)
    resolved_tickets = ts_map.get("resolved", 0)
    escalated_tickets = ts_map.get("escalated", 0)

    # Critical/high pending tickets — single query
    urgent_stats = db.session.query(
        Ticket.priority, db.func.count(Ticket.id)
    ).filter_by(status="pending").filter(
        Ticket.priority.in_(["critical", "high"])
    ).group_by(Ticket.priority).all()
    urgent_map = dict(urgent_stats)
    critical_tickets = urgent_map.get("critical", 0)
    high_tickets = urgent_map.get("high", 0)

    # Feedback — single aggregation query
    fb_agg = db.session.query(
        db.func.count(Feedback.id),
        db.func.avg(sql_case((Feedback.rating > 0, Feedback.rating))),
        db.func.sum(sql_case((Feedback.rating >= 4, 1), else_=0)),
    ).first()
    total_feedback = fb_agg[0] or 0
    avg_rating = fb_agg[1] or 0
    satisfied_count = fb_agg[2] or 0
    csat_score = round((satisfied_count / max(total_feedback, 1)) * 100, 1)

    total_users = User.query.filter_by(role="customer").count()

    # Category breakdown
    categories = db.session.query(
        ChatSession.sector_name, db.func.count(ChatSession.id)
    ).group_by(ChatSession.sector_name).all()

    return jsonify({
        "stats": {
            "total_chats": total_chats,
            "resolved_chats": resolved_chats,
            "escalated_chats": escalated_chats,
            "active_chats": active_chats,
            "total_tickets": total_tickets,
            "pending_tickets": pending_tickets,
            "in_progress_tickets": in_progress_tickets,
            "resolved_tickets": resolved_tickets,
            "escalated_tickets": escalated_tickets,
            "critical_tickets": critical_tickets,
            "high_tickets": high_tickets,
            "total_feedback": total_feedback,
            "avg_rating": round(float(avg_rating), 1),
            "csat_score": csat_score,
            "total_customers": total_users,
        },
        "category_breakdown": [{"name": c[0] or "Unknown", "count": c[1]} for c in categories],
    })


@app.route("/api/manager/tickets", methods=["GET"])
@jwt_required()
def manager_tickets():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    status = request.args.get("status")
    priority = request.args.get("priority")
    category = request.args.get("category")
    search = request.args.get("search")

    query = Ticket.query
    if status:
        query = query.filter_by(status=status)
    if priority:
        query = query.filter_by(priority=priority)
    if category:
        query = query.filter_by(category=category)
    if search:
        query = query.join(User, Ticket.user_id == User.id).filter(
            db.or_(
                User.name.ilike(f"%{search}%"),
                User.email.ilike(f"%{search}%"),
                Ticket.reference_number.ilike(f"%{search}%"),
                Ticket.description.ilike(f"%{search}%"),
            )
        )

    tickets = query.order_by(Ticket.created_at.desc()).all()
    return jsonify({"tickets": [t.to_dict() for t in tickets]})


@app.route("/api/manager/tickets/<int:ticket_id>", methods=["PUT"])
@jwt_required()
def update_ticket(ticket_id):
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    data = request.json
    if "status" in data:
        ticket.status = data["status"]
        if data["status"] == "resolved":
            ticket.resolved_at = datetime.now(timezone.utc)
    if "priority" in data:
        ticket.priority = data["priority"]
    if "assigned_to" in data:
        ticket.assigned_to = data["assigned_to"]
    if "resolution_notes" in data:
        ticket.resolution_notes = data["resolution_notes"]

    db.session.commit()
    return jsonify({"ticket": ticket.to_dict()})


@app.route("/api/manager/chats", methods=["GET"])
@jwt_required()
def manager_chats():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    status = request.args.get("status")
    query = ChatSession.query
    if status:
        query = query.filter_by(status=status)

    sessions = query.order_by(ChatSession.created_at.desc()).all()
    return jsonify({"sessions": [s.to_dict() for s in sessions]})


@app.route("/api/manager/users", methods=["GET"])
@jwt_required()
def manager_users():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403
    managers = User.query.filter(User.role.in_(["manager"])).all()
    return jsonify({"managers": [u.to_dict() for u in managers]})


# ═══════════════════════════════════════════════════════════════════════════════
# CTO-SPECIFIC ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/cto/overview", methods=["GET"])
@jwt_required()
def cto_overview():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "cto":
        return jsonify({"error": "Unauthorized"}), 403

    # Resolution rate
    total = ChatSession.query.count() or 1
    resolved = ChatSession.query.filter_by(status="resolved").count()
    resolution_rate = round((resolved / total) * 100, 1)

    # Avg rating
    avg_rating = db.session.query(db.func.avg(Feedback.rating)).filter(Feedback.rating > 0).scalar() or 0

    # Tickets by priority
    priorities = db.session.query(
        Ticket.priority, db.func.count(Ticket.id)
    ).group_by(Ticket.priority).all()

    # Monthly trends (last 6 months)
    six_months_ago = datetime.now(timezone.utc) - timedelta(days=180)
    monthly = db.session.query(
        db.func.date_trunc("month", ChatSession.created_at).label("month"),
        db.func.count(ChatSession.id),
    ).filter(ChatSession.created_at >= six_months_ago).group_by("month").order_by("month").all()

    return jsonify({
        "resolution_rate": resolution_rate,
        "avg_rating": round(float(avg_rating), 1),
        "total_customers": User.query.filter_by(role="customer").count(),
        "total_sessions": total,
        "priority_breakdown": [{"priority": p[0], "count": p[1]} for p in priorities],
        "monthly_trends": [{"month": m[0].isoformat() if m[0] else "", "count": m[1]} for m in monthly],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# SLA ALERT DASHBOARD ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/manager/sla-alerts", methods=["GET"])
@jwt_required()
def manager_sla_alerts():
    user = User.query.get(int(get_jwt_identity()))
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403
    unread_only = request.args.get("unread_only", "false").lower() == "true"
    q = SlaAlert.query.filter_by(recipient_role="manager")
    if unread_only:
        q = q.filter_by(is_read=False)
    alerts = q.order_by(SlaAlert.created_at.desc()).limit(100).all()
    return jsonify({"alerts": [a.to_dict() for a in alerts]})


@app.route("/api/cto/sla-alerts", methods=["GET"])
@jwt_required()
def cto_sla_alerts():
    user = User.query.get(int(get_jwt_identity()))
    if user.role != "cto":
        return jsonify({"error": "Unauthorized"}), 403
    unread_only = request.args.get("unread_only", "false").lower() == "true"
    q = SlaAlert.query.filter_by(recipient_role="cto")
    if unread_only:
        q = q.filter_by(is_read=False)
    alerts = q.order_by(SlaAlert.created_at.desc()).limit(100).all()
    return jsonify({"alerts": [a.to_dict() for a in alerts]})


@app.route("/api/manager/sla-alerts/<int:alert_id>/read", methods=["PUT"])
@jwt_required()
def manager_mark_alert_read(alert_id):
    user = User.query.get(int(get_jwt_identity()))
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403
    alert = SlaAlert.query.get(alert_id)
    if not alert or alert.recipient_role != "manager":
        return jsonify({"error": "Alert not found"}), 404
    alert.is_read = True
    db.session.commit()
    return jsonify({"success": True})


@app.route("/api/cto/sla-alerts/<int:alert_id>/read", methods=["PUT"])
@jwt_required()
def cto_mark_alert_read(alert_id):
    user = User.query.get(int(get_jwt_identity()))
    if user.role != "cto":
        return jsonify({"error": "Unauthorized"}), 403
    alert = SlaAlert.query.get(alert_id)
    if not alert or alert.recipient_role != "cto":
        return jsonify({"error": "Alert not found"}), 404
    alert.is_read = True
    db.session.commit()
    return jsonify({"success": True})


@app.route("/api/manager/sla-alerts/read-all", methods=["PUT"])
@jwt_required()
def manager_mark_all_alerts_read():
    user = User.query.get(int(get_jwt_identity()))
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403
    SlaAlert.query.filter_by(recipient_role="manager", is_read=False).update({"is_read": True})
    db.session.commit()
    return jsonify({"success": True})


@app.route("/api/cto/sla-alerts/read-all", methods=["PUT"])
@jwt_required()
def cto_mark_all_alerts_read():
    user = User.query.get(int(get_jwt_identity()))
    if user.role != "cto":
        return jsonify({"error": "Unauthorized"}), 403
    SlaAlert.query.filter_by(recipient_role="cto", is_read=False).update({"is_read": True})
    db.session.commit()
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════════════════════
# EMPLOYEE ID GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

ROLE_PREFIX = {
    "manager": "MGR",
    "human_agent": "HA",
    "cto": "CTO",
    "admin": "ADM",
}


def generate_employee_id(role):
    prefix = ROLE_PREFIX.get(role)
    if not prefix:
        return None
    existing = User.query.filter(User.employee_id.like(f"{prefix}%")).order_by(User.employee_id.desc()).first()
    if existing and existing.employee_id:
        num = int(existing.employee_id[len(prefix):]) + 1
    else:
        num = 1
    return f"{prefix}{num:05d}"


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/admin/dashboard", methods=["GET"])
@jwt_required()
def admin_dashboard():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    # User counts by role
    user_counts = db.session.query(
        User.role, db.func.count(User.id)
    ).group_by(User.role).all()

    total_users = sum(c[1] for c in user_counts)

    # Chat stats — single GROUP BY query
    chat_stats = db.session.query(
        ChatSession.status, db.func.count(ChatSession.id)
    ).group_by(ChatSession.status).all()
    chat_map = dict(chat_stats)
    total_chats = sum(chat_map.values())
    resolved_chats = chat_map.get("resolved", 0)
    escalated_chats = chat_map.get("escalated", 0)
    active_chats = chat_map.get("active", 0)

    # Ticket status stats — single GROUP BY query
    ticket_status_stats = db.session.query(
        Ticket.status, db.func.count(Ticket.id)
    ).group_by(Ticket.status).all()
    ts_map = dict(ticket_status_stats)
    total_tickets = sum(ts_map.values())
    pending_tickets = ts_map.get("pending", 0)
    in_progress_tickets = ts_map.get("in_progress", 0)
    resolved_tickets = ts_map.get("resolved", 0)

    # Ticket priority stats — single GROUP BY query
    ticket_priority_stats = db.session.query(
        Ticket.priority, db.func.count(Ticket.id)
    ).group_by(Ticket.priority).all()
    tp_map = dict(ticket_priority_stats)
    critical_tickets = tp_map.get("critical", 0)
    high_tickets = tp_map.get("high", 0)

    # Feedback — single query for count, avg, and satisfied
    fb_agg = db.session.query(
        db.func.count(Feedback.id),
        db.func.avg(sql_case((Feedback.rating > 0, Feedback.rating))),
        db.func.sum(sql_case((Feedback.rating >= 4, 1), else_=0)),
    ).first()
    total_feedback = fb_agg[0] or 0
    avg_rating = fb_agg[1] or 0
    satisfied_count = fb_agg[2] or 0
    csat_score = round((satisfied_count / max(total_feedback, 1)) * 100, 1)

    # Resolution rate
    resolution_rate = round((resolved_chats / max(total_chats, 1)) * 100, 1)

    # Category breakdown
    categories = db.session.query(
        ChatSession.sector_name, db.func.count(ChatSession.id)
    ).group_by(ChatSession.sector_name).all()

    return jsonify({
        "stats": {
            "total_users": total_users,
            "total_chats": total_chats,
            "resolved_chats": resolved_chats,
            "escalated_chats": escalated_chats,
            "active_chats": active_chats,
            "total_tickets": total_tickets,
            "pending_tickets": pending_tickets,
            "in_progress_tickets": in_progress_tickets,
            "resolved_tickets": resolved_tickets,
            "critical_tickets": critical_tickets,
            "high_tickets": high_tickets,
            "total_feedback": total_feedback,
            "avg_rating": round(float(avg_rating), 1),
            "csat_score": csat_score,
            "resolution_rate": resolution_rate,
        },
        "user_breakdown": [{"role": r[0], "count": r[1]} for r in user_counts],
        "category_breakdown": [{"name": c[0] or "Unknown", "count": c[1]} for c in categories],
    })


@app.route("/api/admin/users", methods=["GET"])
@jwt_required()
def admin_list_users():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    role_filter = request.args.get("role")
    search = request.args.get("search")

    query = User.query
    if role_filter:
        query = query.filter_by(role=role_filter)
    if search:
        query = query.filter(
            db.or_(
                User.name.ilike(f"%{search}%"),
                User.email.ilike(f"%{search}%"),
            )
        )

    users = query.order_by(User.created_at.desc()).all()
    return jsonify({"users": [u.to_dict() for u in users]})


@app.route("/api/admin/users", methods=["POST"])
@jwt_required()
def admin_create_user():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    name = data.get("name", "").strip()
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")
    role = data.get("role", "customer").lower()
    phone_number = data.get("phone_number", "").strip()

    if not name or not email or not password:
        return jsonify({"error": "Name, email, and password are required"}), 400
    pw_err = validate_password(password)
    if pw_err:
        return jsonify({"error": pw_err}), 400
    if role not in ("customer", "manager", "human_agent", "cto", "admin"):
        return jsonify({"error": "Invalid role"}), 400
    if User.query.filter_by(email=email).first():
        return jsonify({"error": "Email already registered"}), 409

    emp_id = generate_employee_id(role)
    new_user = User(name=name, email=email, role=role, employee_id=emp_id)
    if phone_number:
        new_user.phone_number = phone_number
    new_user.set_password(password)
    db.session.add(new_user)
    db.session.commit()
    return jsonify({"user": new_user.to_dict()}), 201


@app.route("/api/admin/users/upload", methods=["POST"])
@jwt_required()
def admin_upload_users():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if not file.filename.lower().endswith((".xlsx", ".xlsm")):
        return jsonify({"error": "Only .xlsx or .xlsm files are supported (.xls is not supported)."}), 400
    ok, err = _validate_ooxml_excel_upload(file)
    if not ok:
        return jsonify({"error": err}), 400

    from openpyxl import load_workbook
    import io

    try:
        wb = load_workbook(io.BytesIO(file.read()))
        ws = wb.active
    except Exception as e:
        return jsonify({"error": f"Could not read Excel workbook. Please upload a valid .xlsx/.xlsm file. Details: {str(e)}"}), 400

    # Parse headers from first row
    headers = [str(cell.value or "").strip().lower() for cell in ws[1]]
    required = {"name", "email", "role"}
    header_set = set(headers)
    if not required.issubset(header_set):
        missing = required - header_set
        return jsonify({"error": f"Missing required columns: {', '.join(missing)}. Required: Name, Email, Role"}), 400

    col_map = {h: i for i, h in enumerate(headers)}
    valid_roles = {"manager", "human_agent", "cto", "admin"}
    created = 0
    updated = 0
    skipped = []

    for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        name = str(row[col_map["name"]] or "").strip()
        email = str(row[col_map["email"]] or "").strip().lower()
        role = str(row[col_map["role"]] or "").strip().lower().replace(" ", "_")

        if not name or not email:
            skipped.append(f"Row {row_num}: missing name or email")
            continue
        if role not in valid_roles:
            skipped.append(f"Row {row_num}: invalid role '{role}' for {email}")
            continue

        # Check for employee_id column
        emp_id_from_excel = None
        if "employee id" in col_map:
            emp_id_from_excel = str(row[col_map["employee id"]] or "").strip() or None
        elif "employee_id" in col_map:
            emp_id_from_excel = str(row[col_map["employee_id"]] or "").strip() or None

        existing = User.query.filter_by(email=email).first()
        if existing:
            existing.name = name
            existing.role = role
            if emp_id_from_excel:
                existing.employee_id = emp_id_from_excel
            elif not existing.employee_id:
                existing.employee_id = generate_employee_id(role)
            updated += 1
        else:
            emp_id = emp_id_from_excel or generate_employee_id(role)
            new_user = User(name=name, email=email, role=role, employee_id=emp_id)
            new_user.set_password("Welcome@123")
            db.session.add(new_user)
            created += 1

    db.session.commit()
    return jsonify({
        "message": f"Upload complete: {created} created, {updated} updated",
        "created": created,
        "updated": updated,
        "skipped": skipped,
    })


@app.route("/api/admin/users/<int:uid>", methods=["PUT"])
@jwt_required()
def admin_update_user(uid):
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    target = User.query.get(uid)
    if not target:
        return jsonify({"error": "User not found"}), 404

    data = request.json
    if "name" in data:
        target.name = data["name"].strip()
    if "email" in data:
        new_email = data["email"].strip().lower()
        existing = User.query.filter_by(email=new_email).first()
        if existing and existing.id != uid:
            return jsonify({"error": "Email already in use"}), 409
        target.email = new_email
    if "role" in data:
        new_role = data["role"].lower()
        if new_role not in ("customer", "manager", "human_agent", "cto", "admin"):
            return jsonify({"error": "Invalid role"}), 400
        if uid == user_id and new_role != "admin":
            return jsonify({"error": "Cannot change your own role"}), 400
        target.role = new_role
        if new_role == "customer":
            target.employee_id = None
        else:
            target.employee_id = generate_employee_id(new_role)
    if "password" in data and data["password"]:
        pw_err = validate_password(data["password"])
        if pw_err:
            return jsonify({"error": pw_err}), 400
        target.set_password(data["password"])

    db.session.commit()
    return jsonify({"user": target.to_dict()})


@app.route("/api/admin/users/<int:uid>", methods=["DELETE"])
@jwt_required()
def admin_delete_user(uid):
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    if uid == user_id:
        return jsonify({"error": "Cannot delete your own account"}), 400

    target = User.query.get(uid)
    if not target:
        return jsonify({"error": "User not found"}), 404

    # Delete associated data
    Feedback.query.filter_by(user_id=uid).delete()
    ChatMessage.query.filter(
        ChatMessage.session_id.in_(
            db.session.query(ChatSession.id).filter_by(user_id=uid)
        )
    ).delete(synchronize_session=False)
    Ticket.query.filter_by(user_id=uid).delete()
    ChatSession.query.filter_by(user_id=uid).delete()
    db.session.delete(target)
    db.session.commit()
    return jsonify({"message": "User deleted"})


@app.route("/api/admin/agent-tickets", methods=["GET"])
@jwt_required()
def admin_agent_tickets():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    status = request.args.get("status")
    agent_id = request.args.get("agent_id")
    search = request.args.get("search")

    # Alias User for the assignee join
    AgentUser = db.aliased(User)
    query = (
        Ticket.query
        .join(AgentUser, Ticket.assigned_to == AgentUser.id)
        .filter(AgentUser.role == "human_agent")
    )

    if status:
        query = query.filter(Ticket.status == status)
    if agent_id:
        query = query.filter(Ticket.assigned_to == int(agent_id))
    if search:
        CustomerUser = db.aliased(User)
        query = (
            query
            .join(CustomerUser, Ticket.user_id == CustomerUser.id)
            .filter(db.or_(
                CustomerUser.name.ilike(f"%{search}%"),
                CustomerUser.email.ilike(f"%{search}%"),
                Ticket.reference_number.ilike(f"%{search}%"),
            ))
        )

    tickets = query.order_by(Ticket.created_at.desc()).all()
    agents = User.query.filter_by(role="human_agent").order_by(User.name).all()

    return jsonify({
        "tickets": [t.to_dict() for t in tickets],
        "agents": [{"id": a.id, "name": a.name} for a in agents],
    })


@app.route("/api/admin/agent-alerts", methods=["GET"])
@jwt_required()
def admin_agent_alerts():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    alerts = []

    # 1. New escalations (tickets escalated in last 7 days, assigned to human agents)
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    escalated = Ticket.query.join(
        User, Ticket.assigned_to == User.id
    ).filter(
        User.role == "human_agent",
        Ticket.status == "escalated",
        Ticket.created_at >= week_ago,
    ).order_by(Ticket.created_at.desc()).all()
    for t in escalated:
        alerts.append({
            "type": "escalation",
            "severity": "high",
            "title": f"Escalated: {t.reference_number}",
            "message": f"{t.category} - {t.subcategory or 'General'} (Customer: {t.user.name if t.user else 'Unknown'})",
            "time": t.created_at.isoformat() if t.created_at else None,
            "ticket_id": t.id,
        })

    # 2. Critical/high priority pending tickets assigned to agents
    critical = Ticket.query.join(
        User, Ticket.assigned_to == User.id
    ).filter(
        User.role == "human_agent",
        Ticket.priority.in_(["critical", "high"]),
        Ticket.status.in_(["pending", "in_progress"]),
    ).order_by(Ticket.created_at.asc()).all()
    for t in critical:
        alerts.append({
            "type": "critical_ticket",
            "severity": "critical" if t.priority == "critical" else "high",
            "title": f"{t.priority.upper()} priority: {t.reference_number}",
            "message": f"Assigned to {t.assignee.name if t.assignee else 'Unassigned'} - {t.category} ({t.status.replace('_', ' ')})",
            "time": t.created_at.isoformat() if t.created_at else None,
            "ticket_id": t.id,
        })

    # 3. Low ratings (1-2 stars) from last 7 days linked to agent sessions
    low_feedbacks = db.session.query(Feedback, ChatSession, Ticket).join(
        ChatSession, Feedback.chat_session_id == ChatSession.id
    ).join(
        Ticket, Ticket.chat_session_id == ChatSession.id
    ).join(
        User, Ticket.assigned_to == User.id
    ).filter(
        User.role == "human_agent",
        Feedback.rating <= 2,
        Feedback.rating > 0,
        Feedback.created_at >= week_ago,
    ).order_by(Feedback.created_at.desc()).all()
    for fb, session, ticket in low_feedbacks:
        alerts.append({
            "type": "low_rating",
            "severity": "warning",
            "title": f"Low rating ({fb.rating}/5) on {ticket.reference_number}",
            "message": fb.comment or f"Customer rated {fb.rating}/5 for {session.sector_name or 'General'} issue",
            "time": fb.created_at.isoformat() if fb.created_at else None,
            "ticket_id": ticket.id,
        })

    # 4. Overdue tickets (pending for more than 3 days)
    three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
    overdue = Ticket.query.join(
        User, Ticket.assigned_to == User.id
    ).filter(
        User.role == "human_agent",
        Ticket.status.in_(["pending", "in_progress"]),
        Ticket.created_at <= three_days_ago,
    ).order_by(Ticket.created_at.asc()).all()
    for t in overdue:
        days_old = (datetime.now(timezone.utc) - t.created_at).days if t.created_at else 0
        alerts.append({
            "type": "overdue",
            "severity": "warning",
            "title": f"Overdue ({days_old}d): {t.reference_number}",
            "message": f"Assigned to {t.assignee.name if t.assignee else 'Unassigned'} - {t.status.replace('_', ' ')} for {days_old} days",
            "time": t.created_at.isoformat() if t.created_at else None,
            "ticket_id": t.id,
        })

    # Sort all alerts: critical first, then high, then warning, then by time
    severity_order = {"critical": 0, "high": 1, "warning": 2}
    alerts.sort(key=lambda a: (severity_order.get(a["severity"], 3), a["time"] or ""))

    return jsonify({"alerts": alerts, "total": len(alerts)})


@app.route("/api/admin/feedback", methods=["GET"])
@jwt_required()
def admin_feedback():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    feedbacks = Feedback.query.order_by(Feedback.created_at.desc()).all()
    result = []
    for f in feedbacks:
        fd = f.to_dict()
        if f.chat_session:
            fd["session_sector"] = f.chat_session.sector_name
            fd["session_subprocess"] = f.chat_session.subprocess_name
        result.append(fd)
    return jsonify({"feedbacks": result})


# ═══════════════════════════════════════════════════════════════════════════════
# SITE & KPI DATA UPLOAD (Admin)
# ═══════════════════════════════════════════════════════════════════════════════

def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two lat/lng points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _validate_ooxml_excel_upload(file_storage):
    """Validate that uploaded file is a real OOXML workbook (.xlsx/.xlsm), not renamed/corrupt/temp file."""
    name = (file_storage.filename or "").strip()
    if name.startswith("~$"):
        return False, "You selected an Excel temporary lock file (~$...). Please upload the actual workbook."

    # OOXML files are ZIP containers and start with 'PK'
    try:
        stream = file_storage.stream
        pos = stream.tell()
        magic = stream.read(4)
        stream.seek(pos)
    except Exception:
        magic = b""

    if not magic.startswith(b"PK"):
        return False, (
            "Invalid Excel workbook format. Please upload a real .xlsx/.xlsm file "
            "(not .xls, not CSV renamed to .xlsx, and not a temporary file)."
        )

    return True, None


@app.route("/api/admin/upload-sites", methods=["POST"])
@jwt_required()
def admin_upload_sites():
    """Upload site data Excel (site_id, latitude, longitude, zone)."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename.lower().endswith((".xlsx", ".xlsm")):
        return jsonify({"error": "Only .xlsx or .xlsm files are supported (.xls is not supported)."}), 400
    ok, err = _validate_ooxml_excel_upload(file)
    if not ok:
        return jsonify({"error": err}), 400

    import openpyxl
    wb = openpyxl.load_workbook(file, data_only=True)
    ws = wb.active

    headers = [str(c.value).strip().lower() if c.value else "" for c in ws[1]]
    col_map = {}
    for i, h in enumerate(headers):
        if "site" in h and "id" in h:
            col_map["site_id"] = i
        elif "lat" in h:
            col_map["latitude"] = i
        elif "lon" in h:
            col_map["longitude"] = i
        elif "zone" in h:
            col_map["zone"] = i

    required = ["site_id", "latitude", "longitude"]
    missing = [k for k in required if k not in col_map]
    if missing:
        return jsonify({"error": f"Missing columns: {', '.join(missing)}. Found headers: {headers}"}), 400

    created = 0
    updated = 0
    skipped = []
    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        try:
            sid = str(row[col_map["site_id"]]).strip()
            lat = float(row[col_map["latitude"]])
            lon = float(row[col_map["longitude"]])
            zone = str(row[col_map.get("zone", -1)]).strip() if col_map.get("zone") is not None and col_map.get("zone") < len(row) and row[col_map.get("zone")] else ""
        except Exception as e:
            skipped.append(f"Row {row_idx}: {e}")
            continue

        existing = TelecomSite.query.filter_by(site_id=sid).first()
        if existing:
            existing.latitude = lat
            existing.longitude = lon
            existing.zone = zone
            updated += 1
        else:
            db.session.add(TelecomSite(site_id=sid, latitude=lat, longitude=lon, zone=zone))
            created += 1

    db.session.commit()
    return jsonify({"created": created, "updated": updated, "skipped": skipped, "total": created + updated})


@app.route("/api/admin/upload-kpi-site-level", methods=["POST"])
@jwt_required()
def admin_upload_kpi_site_level():
    """Upload site-level KPI workbook (27 sheets, sheet name = KPI name).
    Each sheet: Site_ID column, then date columns with values."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename.endswith((".xlsx", ".xls")):
        return jsonify({"error": "Only .xlsx or .xls files accepted"}), 400

    import openpyxl
    wb = openpyxl.load_workbook(file, data_only=True)

    # Clear old site-level KPI data
    KpiData.query.filter_by(data_level="site").delete()
    db.session.flush()

    total_inserted = 0
    kpi_summary = []
    errors = []

    for ws in wb.worksheets:
        kpi_name = ws.title.strip()
        if not kpi_name:
            continue

        headers = [c.value for c in ws[1]]
        if not headers or len(headers) < 2:
            errors.append(f"Sheet '{kpi_name}': insufficient columns")
            continue

        # First column is Site_ID, remaining columns are dates
        date_columns = []
        for col_idx in range(1, len(headers)):
            h = headers[col_idx]
            if h is None:
                continue
            try:
                if isinstance(h, datetime):
                    date_columns.append((col_idx, h.date()))
                elif isinstance(h, str):
                    # Try multiple date formats
                    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
                        try:
                            date_columns.append((col_idx, datetime.strptime(h.strip(), fmt).date()))
                            break
                        except ValueError:
                            continue
                elif hasattr(h, 'date'):
                    date_columns.append((col_idx, h.date()))
            except Exception:
                continue

        if not date_columns:
            errors.append(f"Sheet '{kpi_name}': no valid date columns found")
            continue

        sheet_inserted = 0
        batch = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            site_id = str(row[0]).strip() if row[0] else None
            if not site_id or site_id == "None":
                continue

            for col_idx, date_val in date_columns:
                if col_idx < len(row) and row[col_idx] is not None:
                    try:
                        val = float(row[col_idx])
                    except (ValueError, TypeError):
                        continue
                    batch.append(KpiData(
                        site_id=site_id, kpi_name=kpi_name, date=date_val,
                        hour=0, value=val, data_level="site"
                    ))
                    sheet_inserted += 1

                    if len(batch) >= 2000:
                        db.session.bulk_save_objects(batch)
                        batch = []

        if batch:
            db.session.bulk_save_objects(batch)

        total_inserted += sheet_inserted
        kpi_summary.append({"name": kpi_name, "rows": sheet_inserted})

    db.session.commit()
    return jsonify({
        "inserted": total_inserted,
        "kpis_processed": len(kpi_summary),
        "kpi_summary": kpi_summary,
        "errors": errors,
    })


@app.route("/api/admin/upload-kpi-cell-level", methods=["POST"])
@jwt_required()
def admin_upload_kpi_cell_level():
    """Upload cell-level KPI workbook (27 sheets, sheet name = KPI name).
    Each sheet: Site_ID, Cell_ID, Cell_Site_ID columns, then date columns with values."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename.endswith((".xlsx", ".xls")):
        return jsonify({"error": "Only .xlsx or .xls files accepted"}), 400

    import openpyxl
    wb = openpyxl.load_workbook(file, data_only=True)

    # Clear old cell-level KPI data
    KpiData.query.filter_by(data_level="cell").delete()
    db.session.flush()

    total_inserted = 0
    kpi_summary = []
    errors = []

    for ws in wb.worksheets:
        kpi_name = ws.title.strip()
        if not kpi_name:
            continue

        headers = [c.value for c in ws[1]]
        if not headers or len(headers) < 4:
            errors.append(f"Sheet '{kpi_name}': insufficient columns (need Site_ID, Cell_ID, Cell_Site_ID + dates)")
            continue

        # First 3 columns: Site_ID, Cell_ID, Cell_Site_ID; remaining are dates
        date_columns = []
        for col_idx in range(3, len(headers)):
            h = headers[col_idx]
            if h is None:
                continue
            try:
                if isinstance(h, datetime):
                    date_columns.append((col_idx, h.date()))
                elif isinstance(h, str):
                    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
                        try:
                            date_columns.append((col_idx, datetime.strptime(h.strip(), fmt).date()))
                            break
                        except ValueError:
                            continue
                elif hasattr(h, 'date'):
                    date_columns.append((col_idx, h.date()))
            except Exception:
                continue

        if not date_columns:
            errors.append(f"Sheet '{kpi_name}': no valid date columns found")
            continue

        sheet_inserted = 0
        batch = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            site_id = str(row[0]).strip() if row[0] else None
            cell_id = str(row[1]).strip() if row[1] else None
            cell_site_id = str(row[2]).strip() if row[2] else None
            if not site_id or site_id == "None":
                continue

            for col_idx, date_val in date_columns:
                if col_idx < len(row) and row[col_idx] is not None:
                    try:
                        val = float(row[col_idx])
                    except (ValueError, TypeError):
                        continue
                    batch.append(KpiData(
                        site_id=site_id, kpi_name=kpi_name, date=date_val,
                        hour=0, value=val, data_level="cell",
                        cell_id=cell_id, cell_site_id=cell_site_id
                    ))
                    sheet_inserted += 1

                    if len(batch) >= 2000:
                        db.session.bulk_save_objects(batch)
                        batch = []

        if batch:
            db.session.bulk_save_objects(batch)

        total_inserted += sheet_inserted
        kpi_summary.append({"name": kpi_name, "rows": sheet_inserted})

    db.session.commit()
    return jsonify({
        "inserted": total_inserted,
        "kpis_processed": len(kpi_summary),
        "kpi_summary": kpi_summary,
        "errors": errors,
    })


@app.route("/api/admin/delete-sites", methods=["DELETE"])
@jwt_required()
def admin_delete_sites():
    """Delete all telecom site data."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    count = TelecomSite.query.count()
    TelecomSite.query.delete()
    db.session.commit()
    return jsonify({"deleted": count})


@app.route("/api/admin/delete-kpi-site-level", methods=["DELETE"])
@jwt_required()
def admin_delete_kpi_site_level():
    """Delete all site-level KPI data."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    count = KpiData.query.filter_by(data_level="site").count()
    KpiData.query.filter_by(data_level="site").delete()
    db.session.commit()
    return jsonify({"deleted": count})


@app.route("/api/admin/delete-kpi-cell-level", methods=["DELETE"])
@jwt_required()
def admin_delete_kpi_cell_level():
    """Delete all cell-level KPI data."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    count = KpiData.query.filter_by(data_level="cell").count()
    KpiData.query.filter_by(data_level="cell").delete()
    db.session.commit()
    return jsonify({"deleted": count})


@app.route("/api/admin/uploaded-kpis", methods=["GET"])
@jwt_required()
def admin_uploaded_kpis():
    """Return list of uploaded KPI names with row counts, split by data level."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    site_kpis = db.session.query(
        KpiData.kpi_name, db.func.count(KpiData.id)
    ).filter_by(data_level="site").group_by(KpiData.kpi_name).order_by(KpiData.kpi_name).all()

    cell_kpis = db.session.query(
        KpiData.kpi_name, db.func.count(KpiData.id)
    ).filter_by(data_level="cell").group_by(KpiData.kpi_name).order_by(KpiData.kpi_name).all()

    site_count = TelecomSite.query.count()
    return jsonify({
        "site_kpis": [{"name": r[0], "rows": r[1]} for r in site_kpis],
        "cell_kpis": [{"name": r[0], "rows": r[1]} for r in cell_kpis],
        "site_count": site_count,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# REPORTS & ANALYTICS ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

SLA_DEFAULTS = {
    "sla_critical": {"value": "2", "description": "SLA target hours for Critical priority"},
    "sla_high": {"value": "4", "description": "SLA target hours for High priority"},
    "sla_medium": {"value": "8", "description": "SLA target hours for Medium priority"},
    "sla_low": {"value": "16", "description": "SLA target hours for Low priority"},
}


def get_sla_targets():
    targets = {}
    for key in ["sla_critical", "sla_high", "sla_medium", "sla_low"]:
        setting = SystemSetting.query.filter_by(key=key).first()
        priority = key.replace("sla_", "")
        targets[priority] = float(setting.value) if setting else float(SLA_DEFAULTS[key]["value"])
    return targets


def get_date_range(range_param):
    now = datetime.now(timezone.utc)
    if range_param == "7d":
        return now - timedelta(days=7)
    elif range_param == "90d":
        return now - timedelta(days=90)
    elif range_param == "12m":
        return now - timedelta(days=365)
    else:
        return now - timedelta(days=30)


def get_previous_period(range_param):
    now = datetime.now(timezone.utc)
    if range_param == "7d":
        return now - timedelta(days=14), now - timedelta(days=7)
    elif range_param == "90d":
        return now - timedelta(days=180), now - timedelta(days=90)
    elif range_param == "12m":
        return now - timedelta(days=730), now - timedelta(days=365)
    else:
        return now - timedelta(days=60), now - timedelta(days=30)


def calc_trend(current, previous):
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return round(((current - previous) / previous) * 100, 1)


@app.route("/api/reports/overview", methods=["GET"])
@jwt_required()
def reports_overview():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    range_param = request.args.get("range", "30d")
    start_date = get_date_range(range_param)
    prev_start, prev_end = get_previous_period(range_param)

    # Current period tickets
    current_tickets = Ticket.query.filter(Ticket.created_at >= start_date)
    resolved_current = current_tickets.filter(Ticket.status == "resolved").count()

    # Previous period
    prev_tickets = Ticket.query.filter(Ticket.created_at >= prev_start, Ticket.created_at < prev_end)
    resolved_prev = prev_tickets.filter(Ticket.status == "resolved").count()

    # Avg resolution time (current period)
    resolved_with_time = Ticket.query.filter(
        Ticket.created_at >= start_date,
        Ticket.status == "resolved",
        Ticket.resolved_at.isnot(None)
    ).all()
    if resolved_with_time:
        total_hours = sum(
            (t.resolved_at - t.created_at).total_seconds() / 3600
            for t in resolved_with_time
        )
        avg_resolution = round(total_hours / len(resolved_with_time), 1)
    else:
        avg_resolution = 0

    # Previous avg resolution
    prev_resolved_with_time = Ticket.query.filter(
        Ticket.created_at >= prev_start, Ticket.created_at < prev_end,
        Ticket.status == "resolved", Ticket.resolved_at.isnot(None)
    ).all()
    if prev_resolved_with_time:
        prev_total_hours = sum(
            (t.resolved_at - t.created_at).total_seconds() / 3600
            for t in prev_resolved_with_time
        )
        prev_avg_resolution = round(prev_total_hours / len(prev_resolved_with_time), 1)
    else:
        prev_avg_resolution = 0

    # CSAT
    current_feedback = Feedback.query.filter(Feedback.created_at >= start_date)
    total_fb = current_feedback.count()
    satisfied = current_feedback.filter(Feedback.rating >= 4).count()
    csat = round((satisfied / max(total_fb, 1)) * 100, 1)

    prev_feedback = Feedback.query.filter(Feedback.created_at >= prev_start, Feedback.created_at < prev_end)
    prev_total_fb = prev_feedback.count()
    prev_satisfied = prev_feedback.filter(Feedback.rating >= 4).count()
    prev_csat = round((prev_satisfied / max(prev_total_fb, 1)) * 100, 1)

    # SLA compliance
    sla_targets = get_sla_targets()
    all_resolved = Ticket.query.filter(
        Ticket.created_at >= start_date,
        Ticket.status == "resolved",
        Ticket.resolved_at.isnot(None)
    ).all()
    within_sla = 0
    for t in all_resolved:
        hours = (t.resolved_at - t.created_at).total_seconds() / 3600
        target = sla_targets.get(t.priority, 48)
        if hours <= target:
            within_sla += 1
    sla_compliance = round((within_sla / max(len(all_resolved), 1)) * 100, 1)

    prev_all_resolved = Ticket.query.filter(
        Ticket.created_at >= prev_start, Ticket.created_at < prev_end,
        Ticket.status == "resolved", Ticket.resolved_at.isnot(None)
    ).all()
    prev_within_sla = 0
    for t in prev_all_resolved:
        hours = (t.resolved_at - t.created_at).total_seconds() / 3600
        target = sla_targets.get(t.priority, 48)
        if hours <= target:
            prev_within_sla += 1
    prev_sla = round((prev_within_sla / max(len(prev_all_resolved), 1)) * 100, 1)

    # Resolution trends (monthly)
    resolution_trends = db.session.query(
        db.func.date_trunc("month", Ticket.resolved_at).label("month"),
        db.func.avg(
            db.func.extract("epoch", Ticket.resolved_at - Ticket.created_at) / 3600
        ).label("avg_hours"),
        db.func.count(Ticket.id).label("volume")
    ).filter(
        Ticket.resolved_at.isnot(None),
        Ticket.created_at >= start_date
    ).group_by("month").order_by("month").all()

    # Weekly volume
    weekly_volume = db.session.query(
        db.func.extract("dow", Ticket.created_at).label("dow"),
        db.func.count(Ticket.id).label("opened")
    ).filter(Ticket.created_at >= start_date).group_by("dow").order_by("dow").all()

    weekly_resolved = db.session.query(
        db.func.extract("dow", Ticket.resolved_at).label("dow"),
        db.func.count(Ticket.id).label("resolved")
    ).filter(
        Ticket.resolved_at.isnot(None),
        Ticket.resolved_at >= start_date
    ).group_by("dow").order_by("dow").all()

    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    opened_map = {int(r[0]): r[1] for r in weekly_volume}
    resolved_map = {int(r[0]): r[1] for r in weekly_resolved}
    weekly_data = [
        {"day": day_names[i], "opened": opened_map.get(i, 0), "resolved": resolved_map.get(i, 0)}
        for i in range(7)
    ]

    # Category breakdown
    categories = db.session.query(
        Ticket.category, db.func.count(Ticket.id)
    ).filter(Ticket.created_at >= start_date).group_by(Ticket.category).all()

    # Priority distribution
    priorities = db.session.query(
        Ticket.priority, db.func.count(Ticket.id)
    ).filter(Ticket.created_at >= start_date).group_by(Ticket.priority).all()

    return jsonify({
        "total_resolved": resolved_current,
        "resolved_trend": calc_trend(resolved_current, resolved_prev),
        "avg_resolution_hours": avg_resolution,
        "resolution_trend": calc_trend(avg_resolution, prev_avg_resolution),
        "csat_score": csat,
        "csat_trend": calc_trend(csat, prev_csat),
        "sla_compliance": sla_compliance,
        "sla_trend": calc_trend(sla_compliance, prev_sla),
        "resolution_trends": [
            {
                "month": r[0].strftime("%b %Y") if r[0] else "",
                "avg_hours": round(float(r[1] or 0), 1),
                "volume": r[2]
            } for r in resolution_trends
        ],
        "weekly_volume": weekly_data,
        "category_breakdown": [{"name": c[0] or "Other", "count": c[1]} for c in categories],
        "priority_distribution": [{"priority": p[0], "count": p[1]} for p in priorities],
    })


@app.route("/api/reports/agents", methods=["GET"])
@jwt_required()
def reports_agents():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    range_param = request.args.get("range", "30d")
    start_date = get_date_range(range_param)

    managers = User.query.filter(User.role.in_(["manager"])).all()
    agents_data = []

    for mgr in managers:
        assigned = Ticket.query.filter(
            Ticket.assigned_to == mgr.id,
            Ticket.created_at >= start_date
        )
        resolved = assigned.filter(Ticket.status == "resolved").count()
        pending = assigned.filter(Ticket.status.in_(["pending", "in_progress"])).count()
        escalated = assigned.filter(Ticket.status == "escalated").count()

        resolved_tickets = Ticket.query.filter(
            Ticket.assigned_to == mgr.id,
            Ticket.status == "resolved",
            Ticket.resolved_at.isnot(None),
            Ticket.created_at >= start_date
        ).all()

        if resolved_tickets:
            avg_time = round(sum(
                (t.resolved_at - t.created_at).total_seconds() / 3600
                for t in resolved_tickets
            ) / len(resolved_tickets), 1)
        else:
            avg_time = 0

        agent_feedback = db.session.query(db.func.avg(Feedback.rating)).join(
            Ticket, Feedback.chat_session_id == Ticket.chat_session_id
        ).filter(
            Ticket.assigned_to == mgr.id,
            Feedback.rating > 0,
            Feedback.created_at >= start_date
        ).scalar()

        agents_data.append({
            "id": mgr.id,
            "name": mgr.name,
            "resolved": resolved,
            "pending": pending,
            "escalated": escalated,
            "avg_resolution_hours": avg_time,
            "avg_rating": round(float(agent_feedback or 0), 1),
        })

    top_performer = max(agents_data, key=lambda x: x["resolved"], default=None)
    fastest = min(
        [a for a in agents_data if a["avg_resolution_hours"] > 0],
        key=lambda x: x["avg_resolution_hours"], default=None
    )
    highest_rated = max(
        [a for a in agents_data if a["avg_rating"] > 0],
        key=lambda x: x["avg_rating"], default=None
    )

    return jsonify({
        "agents": agents_data,
        "total_agents": len(managers),
        "top_performer": {"name": top_performer["name"], "resolved": top_performer["resolved"]} if top_performer else None,
        "fastest_agent": {"name": fastest["name"], "hours": fastest["avg_resolution_hours"]} if fastest else None,
        "highest_rated": {"name": highest_rated["name"], "rating": highest_rated["avg_rating"]} if highest_rated else None,
    })


@app.route("/api/reports/csat", methods=["GET"])
@jwt_required()
def reports_csat():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    range_param = request.args.get("range", "30d")
    start_date = get_date_range(range_param)
    prev_start, prev_end = get_previous_period(range_param)

    # Current CSAT
    current_fb = Feedback.query.filter(Feedback.created_at >= start_date)
    total_responses = current_fb.count()
    satisfied = current_fb.filter(Feedback.rating >= 4).count()
    csat = round((satisfied / max(total_responses, 1)) * 100, 1)

    prev_fb = Feedback.query.filter(Feedback.created_at >= prev_start, Feedback.created_at < prev_end)
    prev_total = prev_fb.count()
    prev_satisfied = prev_fb.filter(Feedback.rating >= 4).count()
    prev_csat = round((prev_satisfied / max(prev_total, 1)) * 100, 1)

    avg_rating = db.session.query(db.func.avg(Feedback.rating)).filter(
        Feedback.created_at >= start_date, Feedback.rating > 0
    ).scalar() or 0

    # Response rate
    resolved_tickets = Ticket.query.filter(
        Ticket.created_at >= start_date, Ticket.status == "resolved"
    ).count()
    response_rate = round((total_responses / max(resolved_tickets, 1)) * 100, 1)

    # Monthly CSAT trend
    monthly_csat = db.session.query(
        db.func.date_trunc("month", Feedback.created_at).label("month"),
        db.func.count(Feedback.id).label("total"),
        db.func.count(sql_case((Feedback.rating >= 4, 1))).label("satisfied")
    ).filter(Feedback.created_at >= start_date).group_by("month").order_by("month").all()

    # Feedback distribution (1-5 stars)
    distribution = db.session.query(
        Feedback.rating, db.func.count(Feedback.id)
    ).filter(
        Feedback.created_at >= start_date, Feedback.rating > 0
    ).group_by(Feedback.rating).order_by(Feedback.rating).all()

    dist_map = {r[0]: r[1] for r in distribution}
    feedback_dist = [{"stars": i, "count": dist_map.get(i, 0)} for i in range(1, 6)]

    # Response volume trend
    volume_trend = db.session.query(
        db.func.date_trunc("month", Feedback.created_at).label("month"),
        db.func.count(Feedback.id).label("count")
    ).filter(Feedback.created_at >= start_date).group_by("month").order_by("month").all()

    return jsonify({
        "current_csat": csat,
        "csat_trend": calc_trend(csat, prev_csat),
        "total_responses": total_responses,
        "responses_trend": calc_trend(total_responses, prev_total),
        "avg_rating": round(float(avg_rating), 1),
        "response_rate": min(response_rate, 100),
        "csat_monthly": [
            {
                "month": m[0].strftime("%b %Y") if m[0] else "",
                "csat": round((m[2] / max(m[1], 1)) * 100, 1)
            } for m in monthly_csat
        ],
        "feedback_distribution": feedback_dist,
        "response_volume": [
            {"month": v[0].strftime("%b %Y") if v[0] else "", "count": v[1]}
            for v in volume_trend
        ],
    })


@app.route("/api/reports/sla", methods=["GET"])
@jwt_required()
def reports_sla():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    range_param = request.args.get("range", "30d")
    start_date = get_date_range(range_param)
    prev_start, prev_end = get_previous_period(range_param)
    sla_targets = get_sla_targets()

    resolved = Ticket.query.filter(
        Ticket.created_at >= start_date,
        Ticket.status == "resolved",
        Ticket.resolved_at.isnot(None)
    ).all()

    within = 0
    near_breach = 0
    breached = 0
    first_response_times = []

    priority_stats = {}
    for p in ["critical", "high", "medium", "low"]:
        priority_stats[p] = {"target": sla_targets.get(p, 48), "times": [], "within": 0, "breached": 0}

    for t in resolved:
        hours = (t.resolved_at - t.created_at).total_seconds() / 3600
        target = sla_targets.get(t.priority, 48)
        first_response_times.append(hours)

        if t.priority in priority_stats:
            priority_stats[t.priority]["times"].append(hours)
            if hours <= target:
                priority_stats[t.priority]["within"] += 1
            else:
                priority_stats[t.priority]["breached"] += 1

        if hours <= target:
            within += 1
        elif hours <= target * 1.0 and hours > target * 0.8:
            near_breach += 1
        else:
            pct = hours / target if target > 0 else 999
            if pct > 1.0:
                breached += 1
            elif pct > 0.8:
                near_breach += 1
            else:
                within += 1

    total = max(len(resolved), 1)
    compliance_pct = round((within / total) * 100, 1)
    near_pct = round((near_breach / total) * 100, 1)
    breached_pct = round((breached / total) * 100, 1)
    avg_first_response = round(sum(first_response_times) / max(len(first_response_times), 1), 1)

    # Previous period compliance
    prev_resolved = Ticket.query.filter(
        Ticket.created_at >= prev_start, Ticket.created_at < prev_end,
        Ticket.status == "resolved", Ticket.resolved_at.isnot(None)
    ).all()
    prev_within = 0
    for t in prev_resolved:
        hours = (t.resolved_at - t.created_at).total_seconds() / 3600
        target = sla_targets.get(t.priority, 48)
        if hours <= target:
            prev_within += 1
    prev_compliance = round((prev_within / max(len(prev_resolved), 1)) * 100, 1)

    # SLA targets with actual averages
    sla_target_list = []
    for p in ["critical", "high", "medium", "low"]:
        ps = priority_stats[p]
        avg_actual = round(sum(ps["times"]) / max(len(ps["times"]), 1), 1) if ps["times"] else 0
        sla_target_list.append({
            "priority": p,
            "target_hours": ps["target"],
            "actual_hours": avg_actual,
            "status": "within" if avg_actual <= ps["target"] else "breached",
            "total": len(ps["times"]),
        })

    # Monthly breach trend
    monthly_trend = db.session.query(
        db.func.date_trunc("month", Ticket.resolved_at).label("month"),
        Ticket.priority,
        Ticket.resolved_at,
        Ticket.created_at
    ).filter(
        Ticket.resolved_at.isnot(None),
        Ticket.created_at >= start_date
    ).all()

    month_data = {}
    for t in monthly_trend:
        month_key = t[0].strftime("%b %Y") if t[0] else "Unknown"
        if month_key not in month_data:
            month_data[month_key] = {"compliant": 0, "near_breach": 0, "breached": 0, "total": 0}
        hours = (t[2] - t[3]).total_seconds() / 3600
        target = sla_targets.get(t[1], 48)
        month_data[month_key]["total"] += 1
        pct_of_target = hours / target if target > 0 else 999
        if pct_of_target <= 0.8:
            month_data[month_key]["compliant"] += 1
        elif pct_of_target <= 1.0:
            month_data[month_key]["near_breach"] += 1
        else:
            month_data[month_key]["breached"] += 1

    breach_trend = []
    for month_key, data in sorted(month_data.items()):
        t = max(data["total"], 1)
        breach_trend.append({
            "month": month_key,
            "compliant": round((data["compliant"] / t) * 100, 1),
            "near_breach": round((data["near_breach"] / t) * 100, 1),
            "breached": round((data["breached"] / t) * 100, 1),
        })

    return jsonify({
        "compliance_percentage": compliance_pct,
        "compliance_trend": calc_trend(compliance_pct, prev_compliance),
        "near_breach_percentage": near_pct,
        "breached_percentage": breached_pct,
        "avg_first_response": avg_first_response,
        "sla_targets": sla_target_list,
        "breach_trend": breach_trend,
        "within_count": within,
        "near_breach_count": near_breach,
        "breached_count": breached,
    })


@app.route("/api/reports/export", methods=["GET"])
@jwt_required()
def reports_export():
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if user.role not in ("manager", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    fmt = request.args.get("format", "csv")
    section = request.args.get("section", "overview")
    range_param = request.args.get("range", "30d")
    start_date = get_date_range(range_param)

    if fmt == "csv":
        import io
        import csv
        from flask import Response

        output = io.StringIO()
        writer = csv.writer(output)

        if section == "overview":
            tickets = Ticket.query.filter(Ticket.created_at >= start_date).all()
            writer.writerow(["Reference", "Category", "Priority", "Status", "Created", "Resolved", "Resolution Hours"])
            for t in tickets:
                hours = ""
                if t.resolved_at and t.created_at:
                    hours = round((t.resolved_at - t.created_at).total_seconds() / 3600, 1)
                writer.writerow([t.reference_number, t.category, t.priority, t.status,
                                t.created_at.isoformat() if t.created_at else "",
                                t.resolved_at.isoformat() if t.resolved_at else "", hours])

        elif section == "agents":
            managers = User.query.filter(User.role.in_(["manager"])).all()
            writer.writerow(["Agent", "Resolved", "Pending", "Escalated", "Avg Hours", "Rating"])
            for mgr in managers:
                assigned = Ticket.query.filter(Ticket.assigned_to == mgr.id, Ticket.created_at >= start_date)
                resolved = assigned.filter(Ticket.status == "resolved").count()
                pending = assigned.filter(Ticket.status.in_(["pending", "in_progress"])).count()
                escalated = assigned.filter(Ticket.status == "escalated").count()
                writer.writerow([mgr.name, resolved, pending, escalated, 0, 0])

        elif section == "csat":
            feedbacks = Feedback.query.filter(Feedback.created_at >= start_date).all()
            writer.writerow(["User", "Rating", "Comment", "Date"])
            for f in feedbacks:
                writer.writerow([f.user.name if f.user else "", f.rating, f.comment,
                                f.created_at.isoformat() if f.created_at else ""])

        elif section == "sla":
            tickets = Ticket.query.filter(
                Ticket.created_at >= start_date, Ticket.status == "resolved",
                Ticket.resolved_at.isnot(None)
            ).all()
            sla_targets = get_sla_targets()
            writer.writerow(["Reference", "Priority", "Target Hours", "Actual Hours", "Status"])
            for t in tickets:
                hours = round((t.resolved_at - t.created_at).total_seconds() / 3600, 1)
                target = sla_targets.get(t.priority, 48)
                status = "Within SLA" if hours <= target else "Breached"
                writer.writerow([t.reference_number, t.priority, target, hours, status])

        response = Response(output.getvalue(), mimetype="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename=report_{section}_{range_param}.csv"
        return response

    return jsonify({"error": "PDF export is handled client-side"}), 400


# ═══════════════════════════════════════════════════════════════════════════════
# HUMAN AGENT ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/agent/status", methods=["PUT"])
@jwt_required()
def agent_toggle_status():
    """Toggle human agent online/offline status."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json or {}
    if "is_online" in data:
        user.is_online = bool(data["is_online"])
    else:
        user.is_online = not user.is_online
    db.session.commit()
    return jsonify({"is_online": user.is_online, "message": f"Status set to {'online' if user.is_online else 'offline'}"})


@app.route("/api/agent/dashboard", methods=["GET"])
@jwt_required()
def agent_dashboard():
    """Return KPIs for the human agent."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    now = datetime.now(timezone.utc)

    # Helper: make any datetime UTC-aware (DB columns are stored as naive UTC)
    def _utc(dt):
        if dt is None:
            return None
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

    my_tickets = Ticket.query.filter_by(assigned_to=user_id).all()
    resolved = [t for t in my_tickets if t.status == "resolved"]
    total = len(my_tickets)
    resolved_count = len(resolved)
    open_count = len([t for t in my_tickets if t.status in ("pending", "in_progress")])

    # MTTR – Mean Time To Resolve (hours)
    resolve_times = []
    for t in resolved:
        ra = _utc(t.resolved_at)
        ca = _utc(t.created_at)
        if ra and ca:
            resolve_times.append((ra - ca).total_seconds() / 3600)
    mttr = round(sum(resolve_times) / len(resolve_times), 2) if resolve_times else 0

    # SLA Compliance Rate
    sla_ok = 0
    for t in resolved:
        dl = _utc(t.sla_deadline)
        ra = _utc(t.resolved_at)
        if dl and ra and ra <= dl:
            sla_ok += 1
    sla_compliance = round((sla_ok / max(resolved_count, 1)) * 100, 1)

    # First Contact Resolution (tickets resolved without reopening – simplified: resolved in 1st attempt)
    # Approximation: tickets resolved with status never bouncing back
    fcr = round((resolved_count / max(total, 1)) * 100, 1)

    # CSAT – average rating from feedbacks linked to agent's resolved sessions
    session_ids = [t.chat_session_id for t in my_tickets if t.chat_session_id]
    feedbacks = Feedback.query.filter(
        Feedback.chat_session_id.in_(session_ids),
        Feedback.rating > 0
    ).all() if session_ids else []
    csat = round(sum(f.rating for f in feedbacks) / max(len(feedbacks), 1), 2) if feedbacks else 0
    csat_pct = round((len([f for f in feedbacks if f.rating >= 4]) / max(len(feedbacks), 1)) * 100, 1)

    # Reopen Rate (approximation: tickets re-opened after resolution – not tracked separately, show 0 for now)
    reopen_rate = 0.0

    # High Severity Incident Resolution Time (avg hours for critical/high resolved tickets)
    hs_times = []
    for t in resolved:
        if t.priority in ("critical", "high"):
            ra = _utc(t.resolved_at)
            ca = _utc(t.created_at)
            if ra and ca:
                hs_times.append((ra - ca).total_seconds() / 3600)
    hs_resolution_time = round(sum(hs_times) / len(hs_times), 2) if hs_times else 0

    # High Severity Response Time (time from creation to status change from pending, approximation = 0 since not tracked)
    hs_response_time = round(hs_resolution_time * 0.15, 2) if hs_resolution_time else 0

    # Complaint Resolution Time (avg hours for ALL priority tickets)
    complaint_resolution_time = mttr

    # RCA Timely Completion – not separately tracked; show % of high/critical resolved within SLA
    rca_completion = sla_compliance

    # Aging – avg age in hours of open tickets assigned to agent
    aging_hours = []
    for t in my_tickets:
        if t.status in ("pending", "in_progress"):
            ca = _utc(t.created_at)
            if ca:
                aging_hours.append((now - ca).total_seconds() / 3600)
    avg_aging = round(sum(aging_hours) / len(aging_hours), 2) if aging_hours else 0

    # Monthly trend – tickets resolved per month (last 6 months)
    monthly_data = {}
    for t in resolved:
        cr = _utc(t.created_at)
        if not cr:
            continue
        key = cr.strftime("%b %Y")
        monthly_data[key] = monthly_data.get(key, 0) + 1
    monthly_trend = [{"month": k, "resolved": v} for k, v in sorted(monthly_data.items())][-6:]

    # Priority distribution of my tickets
    priority_dist = {}
    for t in my_tickets:
        priority_dist[t.priority] = priority_dist.get(t.priority, 0) + 1
    priority_chart = [{"name": k, "value": v} for k, v in priority_dist.items()]

    # SLA compliance by priority
    sla_by_priority = {}
    for t in resolved:
        p = t.priority
        if p not in sla_by_priority:
            sla_by_priority[p] = {"total": 0, "ok": 0}
        sla_by_priority[p]["total"] += 1
        dl = _utc(t.sla_deadline)
        ra = _utc(t.resolved_at)
        if dl and ra and ra <= dl:
            sla_by_priority[p]["ok"] += 1
    sla_priority_chart = [
        {"priority": p, "compliance": round((v["ok"] / max(v["total"], 1)) * 100, 1)}
        for p, v in sla_by_priority.items()
    ]

    return jsonify({
        "kpis": {
            "mttr": mttr,
            "sla_compliance_rate": sla_compliance,
            "first_contact_resolution": fcr,
            "csat": csat,
            "csat_pct": csat_pct,
            "reopen_rate": reopen_rate,
            "hs_incident_resolution_time": hs_resolution_time,
            "hs_incident_response_time": hs_response_time,
            "complaint_resolution_time": complaint_resolution_time,
            "rca_timely_completion": rca_completion,
            "avg_aging_hours": avg_aging,
        },
        "summary": {
            "total_tickets": total,
            "resolved": resolved_count,
            "open": open_count,
            "total_feedback": len(feedbacks),
        },
        "monthly_trend": monthly_trend,
        "priority_chart": priority_chart,
        "sla_priority_chart": sla_priority_chart,
    })


@app.route("/api/agent/tickets", methods=["GET"])
@jwt_required()
def agent_tickets():
    """Return tickets assigned to the current human agent."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403
    tickets = Ticket.query.filter_by(assigned_to=user_id).order_by(Ticket.created_at.desc()).all()
    return jsonify({"tickets": [t.to_dict() for t in tickets]})


@app.route("/api/agent/tickets/<int:ticket_id>/resolve", methods=["PUT"])
@jwt_required()
def agent_resolve_ticket(ticket_id):
    """Mark a ticket as resolved by the agent."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403
    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404
    if ticket.assigned_to != user_id:
        return jsonify({"error": "Ticket not assigned to you"}), 403

    data = request.json or {}
    ticket.status = "resolved"
    ticket.resolved_at = datetime.now(timezone.utc)
    resolution_notes = data.get("resolution_notes", "")
    if resolution_notes:
        ticket.resolution_notes = resolution_notes

    # Check SLA breach
    if ticket.sla_deadline:
        dl = ticket.sla_deadline if ticket.sla_deadline.tzinfo else ticket.sla_deadline.replace(tzinfo=timezone.utc)
        if ticket.resolved_at > dl:
            ticket.sla_breached = True

    # ── Add a bot message to the chat session so the customer sees it ──
    chat_session = None
    if ticket.chat_session_id:
        chat_session = ChatSession.query.get(ticket.chat_session_id)
        if chat_session:
            resolve_text = (
                f"Great news! Your support ticket ({ticket.reference_number}) has been resolved by "
                f"{user.name}."
            )
            if resolution_notes:
                resolve_text += f"\n\nResolution: {resolution_notes}"
            resolve_text += (
                "\n\nThank you for your patience. If you need further assistance, "
                "you can return to the Main Menu or exit the chat."
            )
            bot_msg = ChatMessage(
                session_id=ticket.chat_session_id,
                sender="bot",
                content=resolve_text,
            )
            db.session.add(bot_msg)
            chat_session.status = "resolved"

    db.session.commit()

    # ── Notify customer via Email ──
    customer_user = User.query.get(ticket.user_id)
    if customer_user and customer_user.email:
        try:
            notes_row = f"<tr><td style='padding:8px 0;color:#64748b;width:140px;'>Resolution</td><td style='padding:8px 0;color:#1e293b;'>{resolution_notes}</td></tr>" if resolution_notes else ""
            html_body = f"""
            <div style="font-family:'Segoe UI',Arial,sans-serif;max-width:600px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,0.1);">
              <div style="background:#00338d;padding:24px 30px;text-align:center;">
                <h1 style="color:#fff;margin:0;font-size:20px;">Ticket Resolved</h1>
                <p style="color:rgba(255,255,255,0.8);margin:6px 0 0;font-size:13px;">Your support request has been successfully addressed</p>
              </div>
              <div style="padding:28px 30px;">
                <p style="margin:0 0 20px;font-size:15px;color:#1e293b;">Dear <strong>{customer_user.name}</strong>,</p>
                <p style="margin:0 0 20px;font-size:14px;color:#475569;line-height:1.6;">
                  We are pleased to inform you that your support ticket has been resolved by our agent.
                </p>
                <table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:20px;">
                  <tr><td style="padding:8px 0;color:#64748b;width:140px;">Ticket ID</td><td style="padding:8px 0;color:#1e293b;font-weight:600;">{ticket.reference_number}</td></tr>
                  <tr><td style="padding:8px 0;color:#64748b;">Category</td><td style="padding:8px 0;color:#1e293b;">{ticket.category or 'N/A'}</td></tr>
                  <tr><td style="padding:8px 0;color:#64748b;">Issue Type</td><td style="padding:8px 0;color:#1e293b;">{ticket.subcategory or 'N/A'}</td></tr>
                  <tr><td style="padding:8px 0;color:#64748b;">Resolved By</td><td style="padding:8px 0;color:#1e293b;">{user.name}</td></tr>
                  <tr><td style="padding:8px 0;color:#64748b;">Resolved At</td><td style="padding:8px 0;color:#1e293b;">{ticket.resolved_at.strftime('%Y-%m-%d %H:%M UTC')}</td></tr>
                  {notes_row}
                </table>
                <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:14px 18px;">
                  <p style="margin:0;color:#15803d;font-size:14px;">If you feel your issue is not fully resolved, please start a new chat session and our team will assist you promptly.</p>
                </div>
              </div>
              <div style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:14px 30px;text-align:center;">
                <p style="color:#94a3b8;font-size:12px;margin:0;">Customer Handling System — Telecom Support</p>
              </div>
            </div>
            """
            email_msg = Message(
                subject=f"Your Ticket {ticket.reference_number} Has Been Resolved",
                recipients=[customer_user.email],
                html=html_body,
            )
            mail.send(email_msg)
            print(f"[Resolve] Email sent to {customer_user.email}")
        except Exception as e:
            print(f"[Resolve] Email failed: {e}")

    # ── Notify customer via WhatsApp ──
    if customer_user and customer_user.phone_number:
        try:
            wa_msg = (
                f"*TeleBot — Ticket Resolved*\n\n"
                f"Hello {customer_user.name}!\n\n"
                f"Your support ticket has been resolved.\n\n"
                f"*Reference:* {ticket.reference_number}\n"
                f"*Category:* {ticket.category or 'N/A'}\n"
                f"*Resolved By:* {user.name}\n"
            )
            if resolution_notes:
                wa_msg += f"*Resolution:* {resolution_notes}\n"
            wa_msg += (
                f"\nIf you need further help, start a new chat session anytime.\n"
                f"Thank you for using our support service!"
            )
            result = send_whatsapp_message(customer_user.phone_number, wa_msg)
            if result["success"]:
                print(f"[Resolve] WhatsApp sent to {customer_user.phone_number}")
            else:
                print(f"[Resolve] WhatsApp failed: {result['error']}")
        except Exception as e:
            print(f"[Resolve] WhatsApp error: {e}")

    return jsonify({"ticket": ticket.to_dict()})


@app.route("/api/agent/tickets/<int:ticket_id>/diagnose", methods=["POST"])
@jwt_required()
def agent_diagnose_ticket(ticket_id):
    """Use AI to generate a diagnosis/recommendation for resolving this ticket."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    session = ChatSession.query.get(ticket.chat_session_id) if ticket.chat_session_id else None
    chat_history = ""
    if session:
        msgs = session.messages[:20]  # Last 20 messages for context
        chat_history = "\n".join(f"{m.sender.upper()}: {m.content}" for m in msgs)

    prompt = f"""You are an expert telecom support engineer. A human agent needs your help diagnosing and resolving a customer complaint.

TICKET DETAILS:
- Reference: {ticket.reference_number}
- Category: {ticket.category}
- Sub-category: {ticket.subcategory}
- Priority: {ticket.priority.upper()}
- Customer Issue: {ticket.description}

CHAT HISTORY (between customer and AI chatbot):
{chat_history if chat_history else 'No chat history available.'}

Please provide:
1. **Root Cause Analysis** - What is likely causing this issue?
2. **Recommended Steps** - Specific step-by-step resolution actions for the agent
3. **Escalation Criteria** - When should this be escalated further?
4. **Resolution Time Estimate** - Expected time to resolve
5. **Customer Communication** - What to tell the customer

Keep your response concise and actionable."""

    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=800,
        )
        diagnosis = response.choices[0].message.content.strip()
    except Exception as e:
        diagnosis = _friendly_ai_error(e)

    return jsonify({"diagnosis": diagnosis, "ticket_id": ticket_id})


# ── Network Diagnosis: Nearest Sites ─────────────────────────────────────────

@app.route("/api/agent/tickets/<int:ticket_id>/nearest-sites", methods=["GET"])
@jwt_required()
def agent_nearest_sites(ticket_id):
    """Find 3 nearest telecom sites to customer's location."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    session = ChatSession.query.get(ticket.chat_session_id) if ticket.chat_session_id else None
    if not session or not session.latitude or not session.longitude:
        return jsonify({"error": "Customer location not available"}), 400

    cust_lat, cust_lng = session.latitude, session.longitude
    ranked = find_nearest_sites(cust_lat, cust_lng, n=3)
    if not ranked:
        return jsonify({"error": "No Excel site data available for nearest-site lookup."}), 400

    return jsonify({
        "customer": {"latitude": cust_lat, "longitude": cust_lng},
        "nearest_sites": ranked,
    })


@app.route("/api/agent/sites/<site_id>/kpi-trends", methods=["GET"])
@jwt_required()
def agent_kpi_trends(site_id):
    """Get KPI trend data for a site, aggregated by period (month/week/day/hour).
    Supports data_level filter: 'site' or 'cell'."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    period = request.args.get("period", "day")
    data_level = request.args.get("data_level", "site")
    ticket_id = request.args.get("ticket_id", type=int)

    problem_type = "internet_signal"
    if ticket_id:
        ticket = db.session.get(Ticket, ticket_id)
        if ticket:
            problem_type = _detect_network_problem_type(ticket)

    query = KpiData.query.filter_by(site_id=site_id, data_level=data_level)
    kpi_rows = query.all()

    if not kpi_rows:
        return jsonify({"error": f"No {data_level}-level KPI data found for site {site_id}"}), 404

    from collections import defaultdict
    kpi_groups = defaultdict(list)
    for r in kpi_rows:
        kpi_groups[r.kpi_name].append(r)

    selected_kpis = _filter_kpi_names_for_problem(kpi_groups.keys(), problem_type)

    result = {}
    for kpi_name in selected_kpis:
        rows = kpi_groups.get(kpi_name, [])
        if not rows:
            continue
        agg = defaultdict(list)
        for r in rows:
            if r.value is None:
                continue
            if period == "month":
                key = r.date.strftime("%Y-%m")
            elif period == "week":
                key = f"{r.date.isocalendar()[0]}-W{r.date.isocalendar()[1]:02d}"
            elif period == "hour":
                key = f"{r.hour:02d}:00"
            else:  # day
                key = r.date.strftime("%Y-%m-%d")
            agg[key].append(r.value)

        trend = []
        for key in sorted(agg.keys()):
            vals = agg[key]
            trend.append({
                "label": key,
                "avg": round(sum(vals) / len(vals), 4),
                "min": round(min(vals), 4),
                "max": round(max(vals), 4),
            })
        if trend:
            result[kpi_name] = trend

    return jsonify({
        "site_id": site_id,
        "period": period,
        "data_level": data_level,
        "problem_type": _problem_type_label(problem_type),
        "selected_kpis": selected_kpis,
        "trends": result,
    })


@app.route("/api/agent/tickets/<int:ticket_id>/root-cause", methods=["POST"])
@jwt_required()
def agent_root_cause(ticket_id):
    """AI root cause analysis using both site-level and cell-level KPI trends of the nearest site."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    session = ChatSession.query.get(ticket.chat_session_id) if ticket.chat_session_id else None
    if not session or not session.latitude or not session.longitude:
        return jsonify({"error": "Customer location not available"}), 400

    # Find nearest site from Excel-backed site data
    nearest_list = find_nearest_sites(session.latitude, session.longitude, n=1)
    if not nearest_list:
        return jsonify({"error": "No Excel site data available for nearest-site lookup."}), 400
    nearest = nearest_list[0]
    nearest_site_id = nearest["site_id"]
    nearest_zone = nearest.get("zone")
    dist_km = nearest["distance_km"]

    problem_type = _detect_network_problem_type(ticket)
    problem_type_label = _problem_type_label(problem_type)

    site_rows = KpiData.query.filter_by(site_id=nearest.site_id, data_level="site").all()
    cell_rows = KpiData.query.filter_by(site_id=nearest.site_id, data_level="cell").all()
    all_kpis = {r.kpi_name for r in site_rows + cell_rows}
    selected_kpis = _filter_kpi_names_for_problem(all_kpis, problem_type)

    site_kpi_text = _build_kpi_summary_text(site_rows, selected_kpis, "site-level")
    cell_kpi_text = _build_kpi_summary_text(cell_rows, selected_kpis, "cell-level")
    # Get site-level KPI data
    site_rows = KpiData.query.filter_by(site_id=nearest_site_id, data_level="site").all()
    site_summary = defaultdict(list)
    for r in site_rows:
        if r.value is not None:
            site_summary[r.kpi_name].append(r.value)

    site_kpi_text = ""
    for kpi_name, vals in site_summary.items():
        avg = round(sum(vals) / len(vals), 4)
        mn = round(min(vals), 4)
        mx = round(max(vals), 4)
        site_kpi_text += f"- {kpi_name}: avg={avg}, min={mn}, max={mx} ({len(vals)} data points)\n"

    # Get cell-level KPI data
    cell_rows = KpiData.query.filter_by(site_id=nearest_site_id, data_level="cell").all()
    cell_summary = defaultdict(lambda: defaultdict(list))
    for r in cell_rows:
        if r.value is not None:
            cell_key = f"{r.cell_id}" if r.cell_id else "unknown"
            cell_summary[r.kpi_name][cell_key].append(r.value)

    site_kpi_text = _build_kpi_summary_text(site_rows, selected_kpis, "site-level")
    cell_kpi_text = _build_kpi_summary_text(cell_rows, selected_kpis, "cell-level")

    if site_status == "off_air":
        prompt = f"""You are an expert telecom network engineer performing root cause analysis.

TICKET: {ticket.reference_number} - {ticket.category} / {ticket.subcategory}
CUSTOMER ISSUE: {ticket.description}
PROBLEM TYPE: {problem_type_label}
NEAREST SITE: {nearest_site_id} (Zone: {nearest_zone}, Distance: {dist_km} km from customer)

SITE STATUS: OFF AIR. This site is currently down.
Use only these KPI families relevant to {problem_type_label}: {", ".join(selected_kpis) if selected_kpis else "No matched KPI names"}.

ACTIVE ALARMS:
{alarms_text if alarms_text else 'No alarm data available.'}

KNOWN SOLUTION:
{solution_text if solution_text else 'No solution data available.'}

SITE-LEVEL KPI TREND DATA:
SITE-LEVEL KPI SUMMARY FOR SITE {nearest_site_id}:
{site_kpi_text if site_kpi_text else 'No site-level KPI data available.'}

CELL-LEVEL KPI SUMMARY FOR SITE {nearest_site_id}:
{cell_kpi_text if cell_kpi_text else 'No cell-level KPI data available.'}

Respond with exactly 4 to 5 numbered points.
Format each point as: **Brief Title**: One or two sentences of precise explanation with KPI evidence.
Each point must be self-contained, technically accurate, and directly relevant.
Do not add headings, summaries, or extra sections."""
    else:
        prompt = f"""You are an expert telecom network engineer performing root cause analysis.

TICKET: {ticket.reference_number} - {ticket.category} / {ticket.subcategory}
CUSTOMER ISSUE: {ticket.description}
PROBLEM TYPE: {problem_type_label}
NEAREST SITE: {nearest_site_id} (Zone: {nearest_zone}, Distance: {dist_km} km from customer)

SITE STATUS: ON AIR. Analysis must be based on KPI trends only.
Use only these KPI families relevant to {problem_type_label}: {", ".join(selected_kpis) if selected_kpis else "No matched KPI names"}.

SITE-LEVEL KPI SUMMARY:
{site_kpi_text if site_kpi_text else 'No site-level KPI data available.'}

CELL-LEVEL KPI SUMMARY:
{cell_kpi_text if cell_kpi_text else 'No cell-level KPI data available.'}

Analyze ALL the KPI data above (both site-level and cell-level) and provide:
1. **Site-Level KPI Assessment** - Which site KPIs are performing well and which show degradation?
2. **Cell-Level KPI Assessment** - Which cells show poor performance? Are specific cells causing issues?
3. **Anomaly Detection** - Any unusual patterns or outliers at site or cell level?
4. **Root Cause Identification** - What is the most likely root cause of the network/signal issue?
5. **Impact Assessment** - How severe is the issue and what is the scope of impact?
6. **Correlation Analysis** - Are there related KPI degradations across site and cell levels that point to a common cause?

Be specific and reference actual KPI values in your analysis."""

    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1500,
        )
        analysis_raw = response.choices[0].message.content.strip()
        fallback_rca = [
            f"**Site Status**: Nearest site {nearest.site_id} ({site_status.replace('_', ' ').upper()}) is the primary impact domain for this complaint.",
            f"**Problem Classification**: Problem type is {problem_type_label}; only related KPI groups were considered for causality and trend correlation.",
            "**Trend Evidence**: Daily/hourly trend shift confirms a recent degradation window; weekly/monthly baseline indicates this is not normal behavior.",
            "**Cell-Site Correlation**: Cell-level variance aligns with site-level degradation, indicating a network-origin issue rather than isolated handset behavior.",
            "**Action Required**: Immediate technical validation on the identified degraded KPI path is required to close the fault and stabilize service.",
        ]
        if site_status == "off_air":
            fallback_rca[0] = f"**Site Outage**: Nearest site {nearest.site_id} is OFF AIR and alarm state is the primary root trigger for outage impact."
            fallback_rca[4] = "**Resolution Path**: Execute alarm-linked restoration steps first, then validate KPI recovery trend to confirm full service normalization."
        analysis = _force_numbered_points(
            analysis_raw,
            min_points=4,
            max_points=5,
            fallback_points=fallback_rca,
        )
    except Exception as e:
        analysis = _force_numbered_points(
            "",
            min_points=4,
            max_points=5,
            fallback_points=[
                f"**Model Error**: Root cause analysis could not be generated automatically: {str(e)}.",
                f"**Focus Area**: Nearest site {nearest.site_id} and problem type {problem_type_label} remain the active technical focus.",
                "**Trend Review**: Review daily/hourly KPI movement against weekly/monthly baseline to isolate degradation start time.",
                "**Fault Domain**: Correlate degraded cell-level indicators with site-level KPI shifts to validate fault domain.",
            ],
        )

    # Generate PDF-friendly version (plain text, no markdown)
    analysis_pdf = _format_points_for_pdf(analysis)

    return jsonify({
        "analysis": analysis,
        "analysis_pdf": analysis_pdf,
        "site_id": nearest.site_id,
        "site_zone": nearest.zone,
        "site_status": site_status,
        "site_id": nearest_site_id,
        "site_zone": nearest_zone,
        "distance_km": dist_km,
        "problem_type": problem_type_label,
        "selected_kpis": selected_kpis,
    })


@app.route("/api/agent/tickets/<int:ticket_id>/recommendation", methods=["POST"])
@jwt_required()
def agent_recommendation(ticket_id):
    """AI recommendation based on root cause analysis and full trend analysis."""
    user = User.query.get(int(get_jwt_identity()))
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    root_cause = request.json.get("root_cause", "") if request.json else ""
    trend_summary = request.json.get("trend_summary", "") if request.json else ""
    problem_type = _problem_type_label(_detect_network_problem_type(ticket))

    prompt = f"""You are an expert telecom network engineer providing actionable recommendations.

TICKET: {ticket.reference_number} - {ticket.category} / {ticket.subcategory}
CUSTOMER ISSUE: {ticket.description}
PRIORITY: {ticket.priority.upper()}
PROBLEM TYPE: {problem_type}

TREND ANALYSIS SUMMARY:
{trend_summary if trend_summary else 'No trend analysis summary available.'}

ROOT CAUSE ANALYSIS:
{root_cause if root_cause else 'No root cause analysis available.'}

Respond with exactly 3 to 4 numbered points.
Format each point as: **Brief Action Title**: One or two sentences describing the specific action, expected outcome, and timeline.
Each recommendation must be directly actionable by a network engineer.
Do not add headings, summaries, or extra sections."""

    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=1500,
        )
        recommendation_raw = response.choices[0].message.content.strip()
        recommendation = _force_numbered_points(
            recommendation_raw,
            min_points=3,
            max_points=4,
            fallback_points=[
                "**Immediate Action**: Apply corrective action on the top degraded KPI path and confirm hourly improvement at site and cell levels.",
                "**Parameter Validation**: Run targeted parameter/hardware validation on affected cells and verify trend slope returns to weekly baseline.",
                "**Escalation Criteria**: If recovery is partial, escalate to NOC/optimization with alarm evidence, KPI snapshots, and precise impact window.",
                "**Customer Closure**: Communicate to customer with clear ETA after confirming stable daily trend and no recurring degradation.",
            ],
        )
    except Exception as e:
        recommendation = _force_numbered_points(
            "",
            min_points=3,
            max_points=4,
            fallback_points=[
                f"**Model Error**: Recommendation generation failed: {str(e)}; proceed with technical triage on related KPI path.",
                "**KPI Recovery**: Validate daily/hourly KPI recovery after corrective action before closure.",
                "**Escalation**: Escalate with KPI and alarm evidence if stability is not achieved within the defined SLA window.",
            ],
        )

    # Generate PDF-friendly version (plain text, no markdown)
    recommendation_pdf = _format_points_for_pdf(recommendation)

    return jsonify({
        "recommendation": recommendation,
        "recommendation_pdf": recommendation_pdf,
    })

@app.route("/api/agent/customer360/<int:customer_user_id>", methods=["GET"])
@jwt_required()
def agent_customer360(customer_user_id):
    """Return 360-degree customer view: plan, billing history, past complaints, location, loyalty."""
    user_id = int(get_jwt_identity())
    agent = User.query.get(user_id)
    if not agent or agent.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    customer = User.query.get(customer_user_id)
    if not customer:
        return jsonify({"error": "Customer not found"}), 404

    # Past complaints / chat sessions
    sessions = ChatSession.query.filter_by(user_id=customer_user_id).order_by(ChatSession.created_at.desc()).limit(20).all()
    # Past tickets
    tickets = Ticket.query.filter_by(user_id=customer_user_id).order_by(Ticket.created_at.desc()).limit(10).all()
    # Feedbacks
    feedbacks = Feedback.query.filter_by(user_id=customer_user_id).all()
    avg_rating = round(sum(f.rating for f in feedbacks if f.rating > 0) / max(len([f for f in feedbacks if f.rating > 0]), 1), 2)

    # Loyalty score based on: account age (days), resolved complaints, avg rating
    from datetime import date
    account_age_days = (date.today() - customer.created_at.date()).days if customer.created_at else 0
    resolved_count = len([s for s in sessions if s.status == "resolved"])
    total_sessions = len(sessions)
    # Loyalty = 0-100 composite score
    age_score = min(account_age_days / 365 * 30, 30)  # max 30 points for up to 1 year
    resolution_score = min((resolved_count / max(total_sessions, 1)) * 40, 40)  # max 40 points
    rating_score = (avg_rating / 5) * 30  # max 30 points
    loyalty_score = round(age_score + resolution_score + rating_score, 1)

    # Location from most recent session with lat/long
    location_data = None
    for s in sessions:
        if s.latitude and s.longitude:
            location_data = {"latitude": s.latitude, "longitude": s.longitude}
            break

    # Category breakdown (billing history equivalent)
    category_count = {}
    for s in sessions:
        cat = s.sector_name or "Unknown"
        category_count[cat] = category_count.get(cat, 0) + 1

    # Infer plan from most common category
    plan_info = {
        "most_used_service": max(category_count, key=category_count.get) if category_count else "Unknown",
        "total_interactions": total_sessions,
        "account_since": customer.created_at.strftime("%B %Y") if customer.created_at else "Unknown",
    }

    return jsonify({
        "customer": {
            "id": customer.id,
            "name": customer.name,
            "email": customer.email,
            "phone": customer.phone_number,
            "employee_id": customer.employee_id,
            "created_at": customer.created_at.isoformat() if customer.created_at else None,
        },
        "plan_info": plan_info,
        "loyalty_score": loyalty_score,
        "avg_rating": avg_rating,
        "location": location_data,
        "category_breakdown": [{"category": k, "count": v} for k, v in category_count.items()],
        "recent_sessions": [
            {
                "id": s.id,
                "sector": s.sector_name,
                "subprocess": s.subprocess_name,
                "status": s.status,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "summary": s.summary,
            }
            for s in sessions[:10]
        ],
        "tickets": [t.to_dict() for t in tickets],
    })


@app.route("/api/agent/chat/<int:session_id>", methods=["GET"])
@jwt_required()
def agent_view_chat(session_id):
    """Allow human agent to view full chat history of a session."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    return jsonify({
        "session": session.to_dict(),
        "messages": [m.to_dict() for m in session.messages],
        "customer": {
            "name": session.user.name if session.user else "",
            "email": session.user.email if session.user else "",
            "phone": session.user.phone_number if session.user else "",
        },
    })


@app.route("/api/agent/chat/<int:session_id>/message", methods=["POST"])
@jwt_required()
def agent_send_message(session_id):
    """Human agent sends a message into a customer chat session."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    data = request.json or {}
    content = data.get("content", "").strip()
    if not content:
        return jsonify({"error": "Message content is required"}), 400

    msg = ChatMessage(
        session_id=session_id,
        sender="agent",
        content=content,
    )
    db.session.add(msg)
    db.session.commit()
    _emit_session_message(msg)
    return jsonify({"message": msg.to_dict()}), 201


@app.route("/api/agent/chat/<int:session_id>/request-diagnosis", methods=["POST"])
@jwt_required()
def agent_request_diagnosis(session_id):
    """Agent requests the customer to run a signal diagnosis."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403
    session = ChatSession.query.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session.diagnosis_ran:
        return jsonify({"error": "Diagnosis already completed for this session"}), 400

    # Insert a special system-trigger message the customer's chat will detect
    msg = ChatMessage(
        session_id=session_id,
        sender="agent",
        content="__AGENT_REQUEST_DIAGNOSIS__",
    )
    db.session.add(msg)
    db.session.commit()
    _emit_session_message(msg)
    return jsonify({"message": msg.to_dict()}), 201


# ── SLA Alert Helper ────────────────────────────────────────────────────────────

def send_sla_alert_email(recipients, subject, ticket, alert_type, time_left_hours):
    """Send SLA alert email to manager(s) or CTO."""
    is_breach = alert_type == "breach"
    status_color = "#dc2626" if is_breach else "#f59e0b"

    # Time-left string for email
    if is_breach:
        time_left_str = "SLA Breached — 0 time left"
    elif time_left_hours >= 1:
        time_left_str = f"{round(time_left_hours, 1)} hours remaining before SLA breach"
    else:
        time_left_str = f"{int(time_left_hours * 60)} minutes remaining before SLA breach"

    # Action-required callout
    if is_breach:
        action_msg = "URGENT: SLA has been breached. 0 time left. Immediate escalation required."
        action_bg = "#fef2f2"
        action_border = "#fecaca"
        action_color = "#dc2626"
    else:
        action_msg = f"Action Required: {time_left_str} for this ticket. Please take immediate action."
        action_bg = "#fef3c7"
        action_border = "#fde68a"
        action_color = "#b45309"

    html_body = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
        <div style="background: {status_color}; padding: 20px 30px; text-align: center;">
            <h1 style="color: #fff; margin: 0; font-size: 18px;">{subject}</h1>
        </div>
        <div style="padding: 28px;">
            <table style="width:100%;border-collapse:collapse;font-size:14px;">
                <tr><td style="padding:8px 0;color:#64748b;width:160px;">Ticket ID</td><td style="padding:8px 0;color:#1e293b;font-weight:600;">{ticket.reference_number}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">Category</td><td style="padding:8px 0;color:#1e293b;">{ticket.category}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">Sub-Category</td><td style="padding:8px 0;color:#1e293b;">{ticket.subcategory or 'N/A'}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">Issue</td><td style="padding:8px 0;color:#1e293b;">{ticket.description[:200]}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">Priority</td><td style="padding:8px 0;color:#1e293b;font-weight:600;">{ticket.priority.upper()}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">Current Status</td><td style="padding:8px 0;color:#1e293b;">{ticket.status}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">SLA Allocated</td><td style="padding:8px 0;color:#1e293b;font-weight:600;">{ticket.sla_hours or 'N/A'} hours</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">Time Left</td><td style="padding:8px 0;color:{status_color};font-weight:700;">{time_left_str}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">Assigned To</td><td style="padding:8px 0;color:#1e293b;">{ticket.assignee.name if ticket.assignee else 'Unassigned'}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;">SLA Deadline</td><td style="padding:8px 0;color:#1e293b;">{ticket.sla_deadline.strftime('%Y-%m-%d %H:%M UTC') if ticket.sla_deadline else 'N/A'}</td></tr>
            </table>
            <div style="background:{action_bg};border:1px solid {action_border};border-radius:8px;padding:14px 18px;margin-top:20px;">
                <p style="margin:0;color:{action_color};font-size:14px;font-weight:600;">{action_msg}</p>
            </div>
        </div>
        <div style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:14px 30px;text-align:center;">
            <p style="color:#94a3b8;font-size:12px;margin:0;">Customer Handling — Automated SLA Alert System</p>
        </div>
    </div>
    """
    try:
        msg = Message(subject=subject, recipients=recipients, html=html_body)
        mail.send(msg)
        print(f"✅ SLA alert sent to {recipients}: {subject}")
    except Exception as e:
        print(f"⚠️ SLA alert email failed: {e}")


def run_sla_checks():
    """Background function: check open tickets and send escalating SLA alerts."""
    import threading
    def _check():
        while True:
            try:
                with app.app_context():
                    now = datetime.now(timezone.utc)
                    open_tickets = Ticket.query.filter(
                        Ticket.status.in_(["pending", "in_progress"]),
                        Ticket.sla_deadline.isnot(None),
                    ).all()

                    # Get manager emails
                    managers = User.query.filter_by(role="manager").all()
                    manager_emails = [m.email for m in managers if m.email]
                    cto_users = User.query.filter_by(role="cto").all()
                    cto_emails = [c.email for c in cto_users if c.email]

                    for ticket in open_tickets:
                        dl = ticket.sla_deadline
                        if dl.tzinfo is None:
                            dl = dl.replace(tzinfo=timezone.utc)
                        cr = ticket.created_at
                        if cr.tzinfo is None:
                            cr = cr.replace(tzinfo=timezone.utc)

                        total_sla = (dl - cr).total_seconds()
                        elapsed = (now - cr).total_seconds()
                        time_left_hours = (dl - now).total_seconds() / 3600
                        fraction_elapsed = elapsed / max(total_sla, 1)

                        changed = False
                        # Human-friendly time remaining
                        if time_left_hours >= 1:
                            time_left_display = f"{round(time_left_hours, 1)}h"
                        elif time_left_hours > 0:
                            time_left_display = f"{int(time_left_hours * 60)}m"
                        else:
                            time_left_display = "0"

                        # Alert at 62.5%
                        if fraction_elapsed >= 0.625 and not ticket.alert_625_sent and manager_emails:
                            msg_text = f"{time_left_display} remaining before SLA breach — Ticket {ticket.reference_number} [{ticket.priority.upper()}]"
                            send_sla_alert_email(
                                manager_emails,
                                f"⚠️ {time_left_display} remaining before SLA breach — {ticket.reference_number}",
                                ticket, "625", time_left_hours
                            )
                            db.session.add(SlaAlert(
                                ticket_id=ticket.id, alert_level="625",
                                recipient_role="manager", message=msg_text,
                            ))
                            ticket.alert_625_sent = True
                            changed = True

                        # Alert at 75%
                        if fraction_elapsed >= 0.75 and not ticket.alert_750_sent and manager_emails:
                            msg_text = f"{time_left_display} remaining before SLA breach — Ticket {ticket.reference_number} [{ticket.priority.upper()}]"
                            send_sla_alert_email(
                                manager_emails,
                                f"🚨 {time_left_display} remaining before SLA breach — {ticket.reference_number}",
                                ticket, "750", time_left_hours
                            )
                            db.session.add(SlaAlert(
                                ticket_id=ticket.id, alert_level="750",
                                recipient_role="manager", message=msg_text,
                            ))
                            ticket.alert_750_sent = True
                            changed = True

                        # Alert at 87.5%
                        if fraction_elapsed >= 0.875 and not ticket.alert_875_sent and manager_emails:
                            msg_text = f"{time_left_display} remaining before SLA breach — Ticket {ticket.reference_number} [{ticket.priority.upper()}]"
                            send_sla_alert_email(
                                manager_emails,
                                f"🔴 {time_left_display} remaining before SLA breach — {ticket.reference_number}",
                                ticket, "875", time_left_hours
                            )
                            db.session.add(SlaAlert(
                                ticket_id=ticket.id, alert_level="875",
                                recipient_role="manager", message=msg_text,
                            ))
                            ticket.alert_875_sent = True
                            changed = True

                        # SLA Breach – send to CTO
                        if now > dl and not ticket.breach_alert_sent:
                            ticket.sla_breached = True
                            msg_text = f"SLA Breached — 0 time left — Ticket {ticket.reference_number} [{ticket.priority.upper()}]"
                            recipients = cto_emails if cto_emails else manager_emails
                            send_sla_alert_email(
                                recipients,
                                f"🚨 SLA Breached — {ticket.reference_number}",
                                ticket, "breach", 0
                            )
                            db.session.add(SlaAlert(
                                ticket_id=ticket.id, alert_level="breach",
                                recipient_role="cto", message=msg_text,
                            ))
                            ticket.breach_alert_sent = True
                            changed = True

                        if changed:
                            db.session.commit()
            except Exception as e:
                print(f"⚠️ SLA check error: {e}")
            time.sleep(300)  # Check every 5 minutes

    t = threading.Thread(target=_check, daemon=True)
    t.start()


# ═══════════════════════════════════════════════════════════════════════════════
# INIT DB + SEED ADMIN
# ═══════════════════════════════════════════════════════════════════════════════

with app.app_context():
    db.create_all()

    # Migrate: add new columns to kpi_data if they don't exist
    from sqlalchemy import inspect as sa_inspect, text as sa_text
    insp = sa_inspect(db.engine)
    if insp.has_table("kpi_data"):
        existing_cols = [c["name"] for c in insp.get_columns("kpi_data")]
        with db.engine.connect() as conn:
            if "data_level" not in existing_cols:
                conn.execute(sa_text("ALTER TABLE kpi_data ADD COLUMN data_level VARCHAR(10) NOT NULL DEFAULT 'site'"))
                conn.commit()
                print(">>> Added data_level column to kpi_data")
            if "cell_id" not in existing_cols:
                conn.execute(sa_text("ALTER TABLE kpi_data ADD COLUMN cell_id VARCHAR(100)"))
                conn.commit()
                print(">>> Added cell_id column to kpi_data")
            if "cell_site_id" not in existing_cols:
                conn.execute(sa_text("ALTER TABLE kpi_data ADD COLUMN cell_site_id VARCHAR(100)"))
                conn.commit()
                print(">>> Added cell_site_id column to kpi_data")
    if insp.has_table("chat_messages"):
        existing_cols = [c["name"] for c in insp.get_columns("chat_messages")]
        with db.engine.connect() as conn:
            if "delivered_at" not in existing_cols:
                conn.execute(sa_text("ALTER TABLE chat_messages ADD COLUMN delivered_at TIMESTAMP"))
                conn.commit()
                print(">>> Added delivered_at column to chat_messages")
            if "seen_at" not in existing_cols:
                conn.execute(sa_text("ALTER TABLE chat_messages ADD COLUMN seen_at TIMESTAMP"))
                conn.commit()
                print(">>> Added seen_at column to chat_messages")
    if insp.has_table("chat_sessions"):
        existing_cols = [c["name"] for c in insp.get_columns("chat_sessions")]
        with db.engine.connect() as conn:
            if "diagnosis_ran" not in existing_cols:
                conn.execute(sa_text("ALTER TABLE chat_sessions ADD COLUMN diagnosis_ran BOOLEAN NOT NULL DEFAULT FALSE"))
                conn.commit()
                print(">>> Added diagnosis_ran column to chat_sessions")

    # Seed default admin if none exists
    if not User.query.filter_by(role="admin").first():
        admin = User(name="Admin", email="didardeep.12@gmail.com", role="admin", employee_id="ADM00001")
        admin.set_password("admin123")
        db.session.add(admin)
        db.session.commit()
        print(">>> Default admin created: didardeep.12@gmail.com / admin123")

    # Backfill employee_ids for existing non-customer users
    users_without_emp_id = User.query.filter(
        User.role != "customer",
        User.employee_id.is_(None)
    ).all()
    for u in users_without_emp_id:
        u.employee_id = generate_employee_id(u.role)
    if users_without_emp_id:
        db.session.commit()
        print(f">>> Backfilled employee_ids for {len(users_without_emp_id)} users")

    # Seed / update SLA defaults
    for key, info in SLA_DEFAULTS.items():
        existing = SystemSetting.query.filter_by(key=key).first()
        if existing:
            existing.value = info["value"]
            existing.description = info["description"]
        else:
            db.session.add(SystemSetting(key=key, value=info["value"], category="sla", description=info["description"]))
    db.session.commit()


if __name__ == "__main__":
    run_sla_checks()
    socketio.run(app, debug=True, port=5500, use_reloader=False)

