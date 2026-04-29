"""
network_diagnosis.py
====================
Tower site lookup, KPI trend analysis, AI root cause analysis, and
parameter recommendations for the human agent diagnosis workflow.

Imported and registered by app.py — do not run directly.

Routes registered via register_routes(app):
  GET  /api/agent/tickets/:id/nearest-sites
  GET  /api/agent/sites/:site_id/kpi-trends
  POST /api/agent/tickets/:id/diagnose
  POST /api/agent/tickets/:id/root-cause
  POST /api/agent/tickets/:id/recommendation
"""

import re
import math
from collections import defaultdict
from flask import request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity


# ─── Injected from app.py ─────────────────────────────────────────────────────
_client = None
_deployment = None
_db = None
_models = {}   # {"User", "Ticket", "ChatSession", "KpiData", "TelecomSite"}


def init(client, deployment_name, db, models):
    """
    Called once from app.py after Flask app and DB are ready.
    models: dict with keys User, Ticket, ChatSession, KpiData, TelecomSite
    """
    global _client, _deployment, _db, _models
    _client = client
    _deployment = deployment_name
    _db = db
    _models = models


def _friendly_error(err):
    return (
        "I'm having trouble reaching the AI service right now. "
        "Please try again in a few moments."
    )


# ─── KPI classification constants ─────────────────────────────────────────────

NETWORK_PROBLEM_KPI_KEYWORDS = {
    "internet_signal": [
        "throughput", "tput", "data rate", "pdcp", "user throughput",
        "latency", "delay", "rtt", "packet loss",
        "prb", "resource block", "utilization", "congestion",
        "data volume", "traffic volume", "dl volume", "ul volume",
        "active ue", "connected ue",
        "sinr", "rsrp", "rsrq", "cqi",
        "availability",
    ],
    "call_failure": [
        "cssr", "call setup", "setup success", "accessibility",
        "sdcch", "paging", "attach", "blocking", "asr", "volte",
        "moc", "mtc",
    ],
    "call_drop": [
        "drop", "call drop", "tch drop", "retainability",
        "rlf", "radio link failure",
        "handover", "ho success", "ho failure",
        "speech", "voice",
    ],
}

NETWORK_PROBLEM_KPI_PRIMARY = {
    "internet_signal": [
        "throughput", "tput", "data rate", "pdcp", "user throughput",
        "latency", "packet loss", "prb", "utilization",
        "sinr", "rsrp", "rsrq", "cqi",
    ],
    "call_failure": [
        "cssr", "call setup", "setup success", "accessibility",
        "sdcch", "paging", "blocking", "asr", "volte", "attach",
    ],
    "call_drop": [
        "drop", "call drop", "tch drop", "retainability",
        "rlf", "radio link failure", "handover", "ho success", "ho failure",
    ],
}

NETWORK_PROBLEM_KPI_STRONG = {
    "internet_signal": ["throughput", "latency", "prb", "sinr", "rsrp", "rsrq", "cqi", "data volume"],
    "call_failure": ["cssr", "call setup", "accessibility", "sdcch", "paging", "blocking"],
    "call_drop": ["drop", "retainability", "rlf", "tch drop", "handover"],
}


# ─── Problem type detection ───────────────────────────────────────────────────

def _normalize_problem_text(ticket):
    return " ".join([
        (ticket.category or "").strip().lower(),
        (ticket.subcategory or "").strip().lower(),
        (ticket.description or "").strip().lower(),
    ])


def _detect_network_problem_type(ticket):
    text = _normalize_problem_text(ticket)
    if any(x in text for x in ["call drop", "calls drop", "dropped call", "call disconnect", "drop rate"]):
        return "call_drop"
    if any(x in text for x in ["call failure", "calls fail", "unable to make call", "call not connecting", "call setup"]):
        return "call_failure"
    if "call / sms failures" in text:
        return "call_failure"
    return "internet_signal"


def _problem_type_label(problem_type):
    return {
        "internet_signal": "Internet Speed",
        "call_failure": "Call Failure",
        "call_drop": "Call Drop",
    }.get(problem_type, "Internet Speed")


# ─── KPI filtering ────────────────────────────────────────────────────────────

def _filter_kpi_names_for_problem(kpi_names, problem_type):
    primary = NETWORK_PROBLEM_KPI_PRIMARY.get(problem_type, NETWORK_PROBLEM_KPI_PRIMARY["internet_signal"])
    strong = NETWORK_PROBLEM_KPI_STRONG.get(problem_type, [])
    keys = NETWORK_PROBLEM_KPI_KEYWORDS.get(problem_type, NETWORK_PROBLEM_KPI_KEYWORDS["internet_signal"])

    scored = []
    for name in kpi_names:
        lname = (name or "").lower()
        primary_score = sum(2 for k in primary if k in lname)
        strong_bonus = 3 if any(k in lname for k in strong) else 0
        secondary_score = sum(1 for k in keys if k in lname)
        total = primary_score + strong_bonus + secondary_score
        if total > 0:
            scored.append((total, name))

    if scored:
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [n for _, n in scored[:5]]
    return sorted(kpi_names)[:5]


# ─── KPI aggregation helpers ──────────────────────────────────────────────────

def _period_key_for_row(row, period):
    if period == "month":
        return row.date.strftime("%Y-%m")
    if period == "week":
        iso = row.date.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if period == "hour":
        return f"{row.date.strftime('%Y-%m-%d')} {row.hour:02d}:00"
    return row.date.strftime("%Y-%m-%d")


def _build_period_stats(rows, period):
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
    return (
        f"{latest_key}: avg={round(sum(latest_vals)/len(latest_vals), 4)}, "
        f"overall_avg={avg_all}, min={round(min(flat_vals), 4)}, max={round(max(flat_vals), 4)}"
    )


def _build_kpi_summary_text(rows, selected_kpis, data_level_label):
    grouped = defaultdict(list)
    for r in rows:
        if r.kpi_name in selected_kpis and r.value is not None:
            grouped[r.kpi_name].append(r)
    if not grouped:
        return f"No {data_level_label} KPI data available."
    parts = []
    for kpi_name in sorted(grouped.keys()):
        kpi_rows = grouped[kpi_name]
        parts.append(
            f"- {kpi_name}\n"
            f"  Monthly: {_build_period_stats(kpi_rows, 'month')}\n"
            f"  Weekly: {_build_period_stats(kpi_rows, 'week')}\n"
            f"  Daily: {_build_period_stats(kpi_rows, 'day')}\n"
            f"  Hourly: {_build_period_stats(kpi_rows, 'hour')}"
        )
    return "\n".join(parts)


def _kpi_degradation_points(rows, selected_kpis, max_points=3):
    grouped = defaultdict(list)
    for r in rows:
        if r.kpi_name in selected_kpis and r.value is not None:
            grouped[r.kpi_name].append(r)
    points = []
    for kpi_name, kpi_rows in grouped.items():
        kpi_rows.sort(key=lambda r: (r.date, r.hour))
        vals = [float(r.value) for r in kpi_rows if r.value is not None]
        if len(vals) < 2:
            continue
        overall = sum(vals) / len(vals)
        latest_vals = [float(r.value) for r in kpi_rows[-min(8, len(kpi_rows)):] if r.value is not None]
        latest = sum(latest_vals) / max(len(latest_vals), 1)
        if overall == 0:
            continue
        delta_pct = (latest - overall) / overall * 100
        if abs(delta_pct) < 20:
            continue
        direction = "drop" if delta_pct < 0 else "increase"
        points.append((
            abs(delta_pct),
            f"**KPI Shift**: {kpi_name} shows a {direction} to {round(latest, 3)} "
            f"vs baseline {round(overall, 3)} ({round(delta_pct, 1)}%)."
        ))
    points.sort(key=lambda x: -x[0])
    return [p for _, p in points[:max_points]]


def _detect_significant_drops(trend_data, drop_threshold_pct=15.0):
    if len(trend_data) < 3:
        return []
    all_avgs = [p["avg"] for p in trend_data if p["avg"] is not None]
    if not all_avgs or sum(all_avgs) / len(all_avgs) == 0:
        return []
    drops = []
    for i, point in enumerate(trend_data):
        val = point.get("avg")
        if val is None or i < 2:
            continue
        prev_vals = [trend_data[j]["avg"] for j in range(i) if trend_data[j]["avg"] is not None]
        if not prev_vals:
            continue
        baseline = sum(prev_vals) / len(prev_vals)
        if baseline == 0:
            continue
        pct_change = (val - baseline) / baseline * 100
        if pct_change <= -drop_threshold_pct:
            drops.append({"label": point["label"], "value": val, "drop_pct": round(pct_change, 1), "type": "drop"})
        elif pct_change >= drop_threshold_pct * 2:
            drops.append({"label": point["label"], "value": val, "drop_pct": round(pct_change, 1), "type": "spike"})
    return drops


# ─── AI text formatting helpers ───────────────────────────────────────────────

def _normalize_ai_lines(text):
    lines = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        line = re.sub(r"^\s*(?:[-*•]|\d+[.)]\s*)\s*", "", line).strip()
        line = re.sub(r"^\*{0,2}[Cc]rux\*{0,2}\s*:?\s*", "", line).strip()
        if re.match(r"^\*\*[^*]+\*\*$", line):
            line = re.sub(r"^\*\*(.*?)\*\*$", r"\1", line).strip()
        if not line:
            continue
        lines.append(line)
    unique, seen = [], set()
    for line in lines:
        key = line.lower()
        if key not in seen:
            seen.add(key)
            unique.append(line)
    if unique and not re.search(r'[.!?)>]$', unique[-1].rstrip()):
        unique = unique[:-1]
    return unique


def _force_numbered_points(raw_text, min_points, max_points, prefix="", fallback_points=None):
    lines = _normalize_ai_lines(raw_text)
    picked = []
    for line in lines:
        if len(line) < 12:
            continue
        picked.append(line)
        if len(picked) >= max_points:
            break
    for line in (fallback_points or []):
        if len(picked) >= min_points:
            break
        if line and line not in picked:
            picked.append(line)
    picked = picked[:max_points]
    if not picked:
        for line in (fallback_points or []):
            if line and len(line) >= 12:
                picked.append(line)
            if len(picked) >= max_points:
                break
    if not picked:
        picked = ["Unable to generate recommendations — please ensure root cause analysis has been run first."]
    return "\n".join(f"{idx + 1}. {line}" for idx, line in enumerate(picked))


def _strip_markdown_for_pdf(text):
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"\*(.*?)\*", r"\1", text)
    return text


def _format_points_for_pdf(raw_text):
    lines = _normalize_ai_lines(raw_text)
    plain_lines = []
    for i, line in enumerate(lines[:5], 1):
        plain_lines.append(f"{i}. {_strip_markdown_for_pdf(line)}")
    return "\n\n".join(plain_lines)


def _filter_rca_lines(lines):
    generic_patterns = [
        r"\bsite status\b", r"\bproblem classification\b", r"\bproblem type\b",
        r"\bprimary impact domain\b", r"\bonly related kpi\b",
        r"\btrend evidence\b", r"\baction required\b", r"\bfocus area\b",
    ]
    out = []
    for line in lines:
        l = line.lower()
        if any(re.search(p, l) for p in generic_patterns):
            continue
        if re.search(r"\b(kpi|throughput|latency|prb|sinr|rsrp|rsrq|cqi|drop|handover|cssr|paging|rlf|alarm|off air|on air)\b", l):
            out.append(line)
            continue
        if re.search(r"\d+(\.\d+)?", l):
            out.append(line)
    return out


# ─── Parameter recommendation builder ────────────────────────────────────────

def _value_or_na(val, unit=""):
    return "N/A" if (val is None or val == "") else f"{val}{unit}"


def _adjust_value(val, delta, min_val=None):
    if val is None:
        return None
    try:
        new_val = float(val) + float(delta)
        if min_val is not None:
            new_val = max(new_val, min_val)
        return round(new_val, 2)
    except Exception:
        return None


def _infer_issue_flags(text):
    t = (text or "").lower()
    flags = set()
    if any(k in t for k in ["prb", "utilization", "congestion", "overload", "traffic spike"]):
        flags.add("congestion")
    if any(k in t for k in ["sinr", "rsrp", "rsrq", "weak signal", "coverage", "low signal"]):
        flags.add("coverage")
    if any(k in t for k in ["interference", "pilot pollution", "overshoot", "over-shoot"]):
        flags.add("interference")
    if any(k in t for k in ["latency", "packet loss", "delay", "jitter"]):
        flags.add("latency")
    if any(k in t for k in ["handover", "rlf", "drop", "call drop"]):
        flags.add("handover")
    if any(k in t for k in ["call setup", "cssr", "paging", "accessibility", "sdcch", "call failure"]):
        flags.add("access")
    if "off air" in t or "off_air" in t:
        flags.add("off_air")
    return flags


def _build_parameter_recommendations(problem_type, root_cause, trend_summary, nearest):
    text = f"{root_cause}\n{trend_summary}"
    flags = _infer_issue_flags(text)
    bw = nearest.get("bandwidth_mhz") if nearest else None
    gain = nearest.get("antenna_gain_dbi") if nearest else None
    eirp = nearest.get("rf_power_eirp_dbm") if nearest else None
    height = nearest.get("antenna_height_agl_m") if nearest else None
    tilt = nearest.get("e_tilt_degree") if nearest else None
    crs = nearest.get("crs_gain") if nearest else None
    recs = []

    if "off_air" in flags:
        recs.append(
            f"**Restore Site to ON AIR**: Resolve active alarms first and validate recovery against KPI trends; "
            f"then re-check RF parameters (EIRP {_value_or_na(eirp,' dBm')}, E-tilt {_value_or_na(tilt,'°')}) for post-recovery tuning."
        )
    if "congestion" in flags and bw is not None:
        recs.append(f"**Increase Bandwidth**: Bandwidth is {bw} MHz; expand to {bw + (10 if bw <= 10 else 5)} MHz to reduce PRB congestion.")
    elif "congestion" in flags:
        recs.append("**Increase Bandwidth**: Bandwidth value missing; increase carrier bandwidth (+5 to +10 MHz) to reduce PRB congestion.")
    if "interference" in flags and tilt is not None and eirp is not None:
        recs.append(
            f"**Reduce Overshoot/Interference**: E-tilt is {tilt}° and EIRP is {eirp} dBm; "
            f"increase tilt to {_adjust_value(tilt, 1, min_val=0)}° and lower EIRP to {_adjust_value(eirp, -1)} dBm."
        )
    elif "coverage" in flags and tilt is not None and eirp is not None:
        recs.append(
            f"**Improve Coverage/RSS**: E-tilt is {tilt}° and EIRP is {eirp} dBm; "
            f"reduce tilt to {_adjust_value(tilt, -1, min_val=0)}° and raise EIRP to {_adjust_value(eirp, 1)} dBm."
        )
    if "handover" in flags and crs is not None:
        recs.append(f"**Stabilize Handover**: CRS Gain is {crs}; raise to {_adjust_value(crs, 3)} to reduce RLF/call drops.")
    elif "access" in flags and gain is not None:
        recs.append(f"**Improve Call Accessibility**: Antenna Gain is {gain} dBi; increase to {_adjust_value(gain, 1)} dBi.")
    if "latency" in flags and height is not None:
        recs.append(f"**Optimize Antenna Height**: Height is {height} m AGL; adjust to {_adjust_value(height, 2)} m.")

    padding = []
    if tilt is not None and eirp is not None:
        padding.append(f"**RF Parameter Alignment**: Validate E-tilt ({tilt}°) and EIRP ({eirp} dBm) against the RCA-identified degradation window.")
    if bw is not None:
        padding.append(f"**Capacity Confirmation**: Confirm bandwidth at {bw} MHz is adequate; if PRB >70%, expand to {round(bw + 5, 1)} MHz.")
    if crs is not None:
        padding.append(f"**CRS Gain Verification**: CRS Gain is {crs}; validate reference signal power across all sectors.")
    if height is not None:
        padding.append(f"**Antenna Height Review**: Height is {height} m AGL; verify line-of-sight and adjust if obstruction detected.")
    if gain is not None:
        padding.append(f"**Antenna Gain Audit**: Gain is {gain} dBi; cross-check with drive test data.")
    padding += [
        "**Drive Test Validation**: Conduct a drive test post-parameter changes to confirm KPI recovery.",
        "**Neighbor Cell Audit**: Review neighbor cell list for missing or unoptimized neighbors.",
        "**Scheduler Tuning**: Adjust CQI-based scheduler to prioritize users with degraded signal quality.",
    ]
    for pad in padding:
        if len(recs) >= 5:
            break
        if pad not in recs:
            recs.append(pad)
    return recs[:5]


# ─── Haversine / nearest site ─────────────────────────────────────────────────

def _haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_nearest_sites(lat, lon, n=3):
    """Return n nearest telecom sites from the DB."""
    if lat is None or lon is None:
        return []
    TelecomSite = _models["TelecomSite"]
    rows = TelecomSite.query.all()
    if not rows:
        return []
    best_by_site = {}
    for site in rows:
        dist = _haversine(lat, lon, site.latitude, site.longitude)
        existing = best_by_site.get(site.site_id)
        if existing is None or dist < existing[0]:
            best_by_site[site.site_id] = (dist, site)
    scored = sorted(best_by_site.values(), key=lambda x: x[0])
    results = []
    for dist, site in scored[:n]:
        status = (site.site_status or "on_air").lower()
        results.append({
            "site_id": site.site_id,
            "site_name": site.site_name or site.site_id,
            "cell_id": site.cell_id or "",
            "zone": site.zone or "",
            "city": site.city or "",
            "state": site.state or "",
            "latitude": site.latitude,
            "longitude": site.longitude,
            "site_status": status,
            "alarms": site.alarms or "",
            "solution": site.solution or site.standard_solution_step or "",
            "standard_solution_step": site.standard_solution_step or "",
            "bandwidth_mhz": site.bandwidth_mhz,
            "antenna_gain_dbi": site.antenna_gain_dbi,
            "rf_power_eirp_dbm": site.rf_power_eirp_dbm,
            "antenna_height_agl_m": site.antenna_height_agl_m,
            "e_tilt_degree": site.e_tilt_degree,
            "crs_gain": site.crs_gain,
            "distance_km": round(dist, 2),
            "status": status,
            "alarm": site.alarms or "",
        })
    return results


# ─── Flask routes ─────────────────────────────────────────────────────────────

def register_routes(app):
    """Register all network diagnosis routes on the Flask app."""

    User = _models["User"]
    Ticket = _models["Ticket"]
    ChatSession = _models["ChatSession"]
    KpiData = _models["KpiData"]

    @app.route("/api/agent/tickets/<int:ticket_id>/diagnose", methods=["POST"])
    @jwt_required()
    def agent_diagnose_ticket(ticket_id):
        """AI general diagnosis for a ticket — used by agent before running full RCA."""
        user = _db.session.get(User, int(get_jwt_identity()))
        if not user or user.role != "human_agent":
            return jsonify({"error": "Unauthorized"}), 403
        ticket = _db.session.get(Ticket, ticket_id)
        if not ticket:
            return jsonify({"error": "Ticket not found"}), 404
        session = _db.session.get(ChatSession, ticket.chat_session_id) if ticket.chat_session_id else None
        chat_history = ""
        if session:
            msgs = session.messages[:20]
            chat_history = "\n".join(f"{m.sender.upper()}: {m.content}" for m in msgs)
        prompt = f"""You are an expert telecom support engineer helping a human agent resolve a customer complaint.

TICKET DETAILS:
- Reference: {ticket.reference_number}
- Category: {ticket.category}
- Sub-category: {ticket.subcategory}
- Priority: {ticket.priority.upper()}
- Customer Issue: {ticket.description}

CHAT HISTORY (customer and AI chatbot):
{chat_history if chat_history else 'No chat history available.'}

Please provide:
1. **Root Cause Analysis** - What is likely causing this issue?
2. **Recommended Steps** - Specific step-by-step resolution actions for the agent
3. **Escalation Criteria** - When should this be escalated further?
4. **Resolution Time Estimate** - Expected time to resolve
5. **Customer Communication** - What to tell the customer

Keep your response concise and actionable."""
        try:
            response = _client.chat.completions.create(
                model=_deployment,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=800,
            )
            diagnosis = response.choices[0].message.content.strip()
        except Exception as e:
            diagnosis = _friendly_error(e)
        return jsonify({"diagnosis": diagnosis, "ticket_id": ticket_id})

    @app.route("/api/agent/tickets/<int:ticket_id>/nearest-sites", methods=["GET"])
    @jwt_required()
    def agent_nearest_sites(ticket_id):
        """Find 3 nearest telecom towers to the customer's location."""
        user = _db.session.get(User, int(get_jwt_identity()))
        if not user or user.role != "human_agent":
            return jsonify({"error": "Unauthorized"}), 403
        ticket = _db.session.get(Ticket, ticket_id)
        if not ticket:
            return jsonify({"error": "Ticket not found"}), 404

        # Include customer's tier so the CR form can auto-fill customer_type
        customer_user = _db.session.get(User, ticket.user_id) if ticket.user_id else None
        customer_type = (customer_user.user_type or "bronze") if customer_user else "bronze"

        session = _db.session.get(ChatSession, ticket.chat_session_id) if ticket.chat_session_id else None
        lat = session.latitude if session and session.latitude else None
        lon = session.longitude if session and session.longitude else None

        # Fallback: default to Siem Reap — Svay Dankum Commune
        DEFAULT_LAT, DEFAULT_LON = 13.3633, 103.8564
        location_source = "customer"
        if lat is None or lon is None:
            lat, lon = DEFAULT_LAT, DEFAULT_LON
            location_source = "default"

        ranked = find_nearest_sites(lat, lon, n=3)
        if not ranked:
            return jsonify({"error": "No site data available for nearest-site lookup."}), 400
        return jsonify({
            "customer": {"latitude": lat, "longitude": lon},
            "nearest_sites": ranked,
            "customer_type": customer_type,
            "location_source": location_source,
        })

    @app.route("/api/agent/sites/<site_id>/kpi-trends", methods=["GET"])
    @jwt_required()
    def agent_kpi_trends(site_id):
        """KPI trend data for a site, aggregated by period (month/week/day/hour)."""
        user = _db.session.get(User, int(get_jwt_identity()))
        if not user or user.role != "human_agent":
            return jsonify({"error": "Unauthorized"}), 403
        period = request.args.get("period", "day")
        data_level = request.args.get("data_level", "site")
        ticket_id = request.args.get("ticket_id", type=int)
        problem_type = "internet_signal"
        if ticket_id:
            ticket = _db.session.get(Ticket, ticket_id)
            if ticket:
                problem_type = _detect_network_problem_type(ticket)
        # STRICT: when the agent picks the "site" tab they get ONLY site-level
        # rows. When they pick the "cell" tab they get ONLY cell-level rows.
        # No cross-level synthesis — that was producing confusing cell numbers
        # under a site-level header.
        kpi_rows = KpiData.query.filter_by(site_id=site_id, data_level=data_level).all()
        if not kpi_rows:
            return jsonify({"error": f"No {data_level}-level KPI data found for site {site_id}"}), 404
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
            has_hour_variation = any((r.hour or 0) != 0 for r in rows)
            for r in rows:
                if r.value is None:
                    continue
                if period == "month":
                    key = r.date.strftime("%Y-%m")
                elif period == "week":
                    key = f"{r.date.isocalendar()[0]}-W{r.date.isocalendar()[1]:02d}"
                elif period == "hour":
                    key = f"{r.hour:02d}:00" if has_hour_variation else r.date.strftime("%Y-%m-%d")
                else:
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
        drops_map = {}
        for kpi_name, trend_data in result.items():
            drops = _detect_significant_drops(trend_data)
            if drops:
                drops_map[kpi_name] = drops
        return jsonify({
            "site_id": site_id,
            "period": period,
            "data_level": data_level,
            "problem_type": _problem_type_label(problem_type),
            "selected_kpis": selected_kpis,
            "trends": result,
            "significant_drops": drops_map,
        })

    @app.route("/api/agent/tickets/<int:ticket_id>/root-cause", methods=["POST"])
    @jwt_required()
    def agent_root_cause(ticket_id):
        """AI root cause analysis using site and cell KPI trends of the nearest tower."""
        user = _db.session.get(User, int(get_jwt_identity()))
        if not user or user.role != "human_agent":
            return jsonify({"error": "Unauthorized"}), 403
        ticket = _db.session.get(Ticket, ticket_id)
        if not ticket:
            return jsonify({"error": "Ticket not found"}), 404
        session = _db.session.get(ChatSession, ticket.chat_session_id) if ticket.chat_session_id else None
        if not session or not session.latitude or not session.longitude:
            return jsonify({"error": "Customer location not available"}), 400

        nearest_list = find_nearest_sites(session.latitude, session.longitude, n=1)
        if not nearest_list:
            return jsonify({"error": "No site data available for nearest-site lookup."}), 400
        nearest = nearest_list[0]
        nearest_site_id = nearest["site_id"]
        nearest_zone = nearest.get("zone")
        dist_km = nearest["distance_km"]
        site_status = (nearest.get("site_status") or "on_air").lower()
        alarms_text = nearest.get("alarms") or "None"
        solution_text = nearest.get("solution") or "No action required"
        std_solution_text = nearest.get("standard_solution_step") or ""
        site_params_text = "\n".join([
            f"- Bandwidth (MHz): {nearest.get('bandwidth_mhz')}",
            f"- Antenna Gain (dBi): {nearest.get('antenna_gain_dbi')}",
            f"- RF Power (EIRP) [dBm]: {nearest.get('rf_power_eirp_dbm')}",
            f"- Antenna height (AGL) (M): {nearest.get('antenna_height_agl_m')}",
            f"- E-tilt (Degree): {nearest.get('e_tilt_degree')}",
            f"- CRS Gain: {nearest.get('crs_gain')}",
        ])

        problem_type = _detect_network_problem_type(ticket)
        problem_type_label = _problem_type_label(problem_type)

        site_rows = KpiData.query.filter_by(site_id=nearest_site_id, data_level="site").all()
        cell_rows = KpiData.query.filter_by(site_id=nearest_site_id, data_level="cell").all()
        all_kpis = {r.kpi_name for r in site_rows + cell_rows}
        selected_kpis = _filter_kpi_names_for_problem(all_kpis, problem_type)
        site_kpi_text = _build_kpi_summary_text(site_rows, selected_kpis, "site-level")
        cell_kpi_text = _build_kpi_summary_text(cell_rows, selected_kpis, "cell-level")
        kpis_str = ", ".join(selected_kpis) if selected_kpis else "none matched"

        if site_status == "off_air":
            prompt = f"""You are a senior telecom network engineer performing root cause analysis.

TICKET: {ticket.reference_number} — {ticket.category} / {ticket.subcategory}
CUSTOMER REPORTED ISSUE: {ticket.description}
PROBLEM TYPE: {problem_type_label}
NEAREST SITE: {nearest_site_id} (Zone: {nearest_zone}, Distance: {dist_km} km from customer)

=== SITE STATUS: OFF AIR ===
ACTIVE ALARMS:\n{alarms_text}
KNOWN SOLUTION:\n{solution_text}
STANDARD SOLUTION STEP:\n{std_solution_text if std_solution_text else 'N/A'}
SITE RF PARAMETERS:\n{site_params_text}
SITE-LEVEL KPI TREND DATA (KPIs: {kpis_str}):\n{site_kpi_text}
CELL-LEVEL KPI TREND DATA:\n{cell_kpi_text}

INSTRUCTIONS:
- Write exactly 4–5 numbered points. Format: **Concise Title**: 1-2 sentences with specific technical evidence.
- Point 1: Primary root cause linking alarm to {problem_type_label} degradation with a specific KPI value.
- Point 2: What KPI trend shows before/during the outage and magnitude of drop.
- Point 3: RF/parameter context using actual values and how they contribute.
- Point 4: Link alarm's standard solution step to expected KPI recovery.
- Point 5: Secondary impact on adjacent cells visible in cell-level KPI trends.
- Use actual numbers. Do not add headings or extra sections."""
        else:
            prompt = f"""You are a senior telecom network engineer performing root cause analysis.

TICKET: {ticket.reference_number} — {ticket.category} / {ticket.subcategory}
CUSTOMER REPORTED ISSUE: {ticket.description}
PROBLEM TYPE: {problem_type_label}
NEAREST SITE: {nearest_site_id} (Zone: {nearest_zone}, Distance: {dist_km} km from customer)

=== SITE STATUS: ON AIR ===
SITE RF PARAMETERS:\n{site_params_text}
SITE-LEVEL KPI TREND DATA (KPIs: {kpis_str}):\n{site_kpi_text}
CELL-LEVEL KPI TREND DATA:\n{cell_kpi_text}

INSTRUCTIONS:
- Write exactly 4–5 numbered points. Format: **Concise Title**: 1-2 sentences with specific technical evidence.
- Point 1: PRIMARY degraded KPI — state recent value vs baseline (actual numbers) and what it reflects.
- Point 2: SECONDARY degraded KPI — state values and chain of impact to customer's {problem_type_label}.
- Point 3: RF parameter context (bandwidth, E-tilt, EIRP, height, CRS gain) and how values amplify degradation.
- Point 4: Temporal pattern — when degradation started, continuous or intermittent, peak vs off-peak.
- Point 5: Cell-level vs site-level discrepancy narrowing fault to specific cell/sector.
- Be precise: use actual KPI values. Do not add headings or extra sections."""

        try:
            response = _client.chat.completions.create(
                model=_deployment,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=1500,
            )
            analysis_raw = response.choices[0].message.content.strip()
            cleaned_lines = _filter_rca_lines(_normalize_ai_lines(analysis_raw))
            fallback_rca = []
            if site_status == "off_air":
                fallback_rca.append(f"**Site Outage**: Nearest site {nearest_site_id} is OFF AIR; alarms indicate outage as primary cause.")
            fallback_rca += _kpi_degradation_points(site_rows + cell_rows, selected_kpis, max_points=3)
            if len(fallback_rca) < 4:
                fallback_rca += [
                    f"**KPI Evidence**: Trend shifts across {problem_type_label}-related KPIs indicate measurable degradation.",
                    "**Correlation**: Site-level and cell-level KPI movement aligns with the customer's symptom timeline.",
                    "**Impact Scope**: Degradation appears localized to the nearest site/cells based on trend evidence.",
                    "**Next Check**: Validate alarms and RF parameters for contributing factors.",
                ]
                fallback_rca = fallback_rca[:4]
            analysis = _force_numbered_points(
                "\n".join(cleaned_lines) if cleaned_lines else "",
                min_points=4, max_points=5, fallback_points=fallback_rca,
            )
        except Exception as e:
            fallback_rca = _kpi_degradation_points(site_rows + cell_rows, selected_kpis, max_points=4)
            if not fallback_rca:
                fallback_rca = [
                    f"**Model Error**: RCA could not be generated: {str(e)}.",
                    f"**KPI Evidence**: Review KPI shifts for {problem_type_label} and isolate the largest deviations.",
                    "**Correlation**: Correlate degraded cell indicators with site KPIs to confirm fault domain.",
                    "**Next Check**: Validate alarms and site status if KPI evidence is weak.",
                ]
            analysis = _force_numbered_points(
                "\n".join(fallback_rca), min_points=4, max_points=5, fallback_points=fallback_rca,
            )

        return jsonify({
            "analysis": analysis,
            "analysis_pdf": _format_points_for_pdf(analysis),
            "site_id": nearest_site_id,
            "site_zone": nearest_zone,
            "site_status": site_status,
            "distance_km": dist_km,
            "problem_type": problem_type_label,
            "selected_kpis": selected_kpis,
        })

    @app.route("/api/agent/tickets/<int:ticket_id>/recommendation", methods=["POST"])
    @jwt_required()
    def agent_recommendation(ticket_id):
        """AI parameter recommendations based on root cause and KPI trend analysis.

        Dynamically scans every numeric RF column in telecom_sites (plus any
        vendor-specific fields in extra_params) so the LLM can pick the most
        appropriate parameter for the RCA rather than being limited to a
        hard-coded subset.
        """
        user = _db.session.get(User, int(get_jwt_identity()))
        if not user or user.role != "human_agent":
            return jsonify({"error": "Unauthorized"}), 403
        ticket = _db.session.get(Ticket, ticket_id)
        if not ticket:
            return jsonify({"error": "Ticket not found"}), 404
        session = _db.session.get(ChatSession, ticket.chat_session_id) if ticket.chat_session_id else None
        nearest = None
        if session and session.latitude and session.longitude:
            nearest_list = find_nearest_sites(session.latitude, session.longitude, n=1)
            if nearest_list:
                nearest = nearest_list[0]

        root_cause = (request.json or {}).get("root_cause", "")
        trend_summary = (request.json or {}).get("trend_summary", "")
        problem_type = _problem_type_label(_detect_network_problem_type(ticket))

        # ── Chat history (customer's original complaint words) ───────────────
        chat_history = ""
        if session:
            try:
                msgs = session.messages[:20]
                chat_history = "\n".join(f"{m.sender.upper()}: {m.content}" for m in msgs)
            except Exception:
                chat_history = ""

        # ── Dynamically discover every numeric RF column in telecom_sites ────
        from sqlalchemy import inspect as sa_inspect
        TelecomSite = _models.get("TelecomSite")
        RF_SKIP = {"id", "latitude", "longitude", "created_at", "updated_at"}
        TEXT_SKIP = {"site_id","site_name","cell_id","cell_site_id","site_abs_id",
                     "zone","city","state","country","province","commune",
                     "site_status","alarms","solution","standard_solution_step",
                     "vendor_name","technology","extra_params"}
        rf_live = {}
        site_vendor = site_tech = ""
        nearest_site_id = nearest.get("site_id") if nearest else None
        if nearest_site_id and TelecomSite is not None:
            try:
                _insp = sa_inspect(_db.engine)
                cols = [c["name"] for c in _insp.get_columns("telecom_sites")]
                num_cols = [c for c in cols if c not in RF_SKIP and c not in TEXT_SKIP]
                if num_cols:
                    from sqlalchemy import text as _sa_text
                    sel_parts = ", ".join([f"AVG({c}) AS {c}" for c in num_cols])
                    with _db.engine.connect() as conn:
                        row = conn.execute(
                            _sa_text(f"SELECT {sel_parts} FROM telecom_sites WHERE site_id=:sid"),
                            {"sid": nearest_site_id},
                        ).mappings().first()
                    if row:
                        rf_live = {k: v for k, v in dict(row).items() if v is not None}
                site_obj = TelecomSite.query.filter_by(site_id=nearest_site_id).first()
                if site_obj:
                    if site_obj.extra_params:
                        for k, v in site_obj.extra_params.items():
                            if v is not None and k not in rf_live:
                                rf_live[k] = v
                    site_vendor = site_obj.vendor_name or ""
                    site_tech = site_obj.technology or ""
            except Exception:
                pass

        def _fmt_val(v):
            try:
                fv = float(v)
                if fv == int(fv): return f"{int(fv)}"
                return f"{round(fv, 3)}"
            except (TypeError, ValueError):
                return str(v)

        rf_lines = [f"{k} = {_fmt_val(v)}" for k, v in sorted(rf_live.items())]
        rf_full = "\n".join(rf_lines) if rf_lines else "No RF parameters found in telecom_sites for this site."

        site_context = "Nearest site data not available."
        if nearest:
            site_context = (
                f"NEAREST SITE: {nearest.get('site_id')} (Zone: {nearest.get('zone')}, "
                f"Distance: {nearest.get('distance_km')} km)\n"
                f"SITE STATUS: {nearest.get('site_status') or 'on_air'}\n"
                f"VENDOR: {site_vendor or 'Unknown'} | TECHNOLOGY: {site_tech or 'Unknown'}"
            )

        prompt = f"""You are a SENIOR RAN OPTIMISATION ENGINEER on shift at the {site_vendor or 'Ericsson / Nokia'} 24x7 NOC, with 20+ years of hands-on experience triaging customer-experience tickets on live {site_tech or 'LTE'} networks. A customer complaint has been escalated to you — your job is to read what the user actually reported, study the RCA, scan the live RF parameter snapshot for the nearest site straight from the operator's telecom_sites database, and produce a field-deployable change-order an O&M / drive-test team can execute today. THINK EXACTLY AS A HUMAN RAN EXPERT WOULD on the NOC shift handover: reason from facts, quote actual DB values, never invent numbers, never speak in generalities. Your output is consumed by a CR (Change Request) workflow — be precise enough that a change-implementer can apply it without further interpretation.

TICKET: {ticket.reference_number} — {ticket.category} / {ticket.subcategory}
PRIORITY: {ticket.priority.upper()}
PROBLEM TYPE: {problem_type}
CUSTOMER REPORTED ISSUE: {ticket.description}

CUSTOMER CHAT HISTORY (read this to understand exactly what the user reported):
{chat_history if chat_history else 'No chat history available.'}

{site_context}

COMPLETE RF PARAMETER DATABASE (every live numeric value currently stored for this site — treat each entry as a Managed-Object attribute on the {site_vendor or 'eNodeB'}; pick FROM THIS LIST only, never invent parameter names not present here. Common entries include bandwidth_mhz, e_tilt_degree, m_tilt_degree, rf_power_eirp_dbm, antenna_gain_dbi, antenna_height_agl_m, crs_gain, azimuth_degree, pci_cell_id, frequency_band, plus vendor-specific extra_params such as qRxLevMin, cellIndividualOffset, primaryDlPower):
{rf_full}

=== TREND ANALYSIS EVIDENCE ===
{trend_summary if trend_summary else 'No trend analysis summary available.'}

=== ROOT CAUSE ANALYSIS FINDINGS ===
{root_cause if root_cause else 'No root cause analysis available.'}

NOC PREDICTIVE-ANALYSIS METHODOLOGY YOU FOLLOW:
  STEP 1 — Map the customer's reported symptom (call drop, slow internet, signal loss, call setup failure) to the candidate root-cause class (coverage hole, interference, congestion, handover, access).
  STEP 2 — Cross-check that hypothesis against the KPI trend evidence and the RCA findings above.
  STEP 3 — Map the confirmed root cause to the RF lever that fixes it. On Ericsson the typical MOs are EUtranCellFDD / SectorCarrier / AntennaSubunit (digitalTilt, mechTilt, primaryDlPower, qRxLevMin, qOffsetFreq, cellIndividualOffset). On Nokia these map to LNCEL / LNCEL_FDD / MOD (elTilt, maxTxPower, cellIndividualOffset, qRxLevMin, qHyst). Look up the equivalent attribute in the RF database above and use that exact name.
  STEP 4 — Quote the current DB value verbatim, propose a typical-NOC-step delta (e.g., +1° E-tilt, -1 dB EIRP, +3 dB CRS gain) within 3GPP / {site_vendor or 'vendor'} {site_tech or ''} safe bounds.
  STEP 5 — State the expected customer-experience improvement and the 48-72h verification window.

=== YOUR TASK (think like a senior {site_vendor or 'Ericsson / Nokia'} NOC RAN expert) ===
1. Read the customer's complaint and the RCA findings carefully.
2. Scan the FULL RF parameter list above. Identify which parameter(s) will MOST DIRECTLY cure the customer's reported issue and the RCA-identified root cause.
3. For each parameter you propose to change: quote its EXACT name as stored in the DB, the EXACT current value from the list (read verbatim), and an EXACT safe new value within 3GPP / {site_vendor or 'vendor'} {site_tech or ''} safe bounds.
4. Tie each change back to BOTH the root cause AND the customer's reported symptom — show the cause→effect chain in plain RF terms.

=== FORMATTING RULES (strict) ===
- Plain text only. Each recommendation formatted as: **Action Title**: body text.
- Write exactly 4–5 numbered recommendations. Most impactful first.
- Every RF-parameter recommendation MUST include an explicit "Previous value: X → New value: Y" line, with X read directly from the RF parameter database above.
- State the EXPECTED KPI OUTCOME for each (which KPI improves, by how much roughly, in what timeframe).
- Be precise: "Adjust <parameter_name> from Y to Z" — never "consider adjusting".
- Do not repeat the root cause verbatim. Focus on the fix and its expected effect.
- Do not add headings, summaries, or extra sections."""

        try:
            response = _client.chat.completions.create(
                model=_deployment,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=1500,
            )
            recommendation_raw = response.choices[0].message.content.strip()
            fallback_points = _build_parameter_recommendations(problem_type, root_cause, trend_summary, nearest or {})
            if recommendation_raw and len(recommendation_raw) > 80:
                recommendation = _force_numbered_points(recommendation_raw, min_points=4, max_points=5, fallback_points=fallback_points)
            else:
                recommendation = _force_numbered_points("\n".join(fallback_points), min_points=4, max_points=5, fallback_points=fallback_points)
        except Exception as e:
            fallback_points = _build_parameter_recommendations(problem_type, root_cause, trend_summary, nearest or {})
            fallback_points.insert(0, f"**Model Error**: Recommendation generation failed: {str(e)}.")
            recommendation = _force_numbered_points("\n".join(fallback_points), min_points=4, max_points=5, fallback_points=fallback_points)

        return jsonify({
            "recommendation": recommendation,
            "recommendation_pdf": _format_points_for_pdf(recommendation),
        })
