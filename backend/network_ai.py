"""
network_ai.py
=============
Flask Blueprint: Network AI Chat & Query Engine
Extracted from network_analytics.py — handles all AI/LLM query logic,
rule-based NL→SQL fallback, and session CRUD for the Network AI Chat page.

Mount in app.py:
    from network_ai import network_ai_bp
    app.register_blueprint(network_ai_bp)
"""

import os
import json
import math
import logging
from datetime import datetime, timezone

from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text as sa_text

from models import db, User, NetworkAiSession, NetworkAiMessage

_LOG = logging.getLogger("network_ai")
NETWORK_AI_VERSION = "2025-03-26-v5"  # bump this to confirm new file is loaded

# ─────────────────────────────────────────────────────────────────────────────
network_ai_bp = Blueprint("network_ai", __name__)


# ─── Shared helpers (imported lazily from network_analytics) ─────────────────

def _sql(query: str, params: dict = None) -> list:
    """Execute raw SQL and return list of dicts."""
    with db.engine.connect() as conn:
        result = conn.execute(sa_text(query), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def _get_dynamic_time_filter():
    """Import the time filter helper from network_analytics at call time."""
    from network_analytics import _dynamic_time_filter
    return _dynamic_time_filter


def _ensure_ai_session_tables():
    """Create network_ai_sessions / network_ai_messages tables if they don't exist."""
    try:
        NetworkAiSession.__table__.create(db.engine, checkfirst=True)
        NetworkAiMessage.__table__.create(db.engine, checkfirst=True)
    except Exception:
        pass
    # ── CHANGE: migration-safe — add new columns if table already exists ──
    try:
        with db.engine.connect() as conn:
            from sqlalchemy import inspect as sa_inspect
            cols = {c["name"] for c in sa_inspect(db.engine).get_columns("network_ai_sessions")}
            if "session_context" not in cols:
                conn.execute(sa_text("ALTER TABLE network_ai_sessions ADD COLUMN session_context JSON DEFAULT '{}'"))
                conn.commit()
            if "conversation_summary" not in cols:
                conn.execute(sa_text("ALTER TABLE network_ai_sessions ADD COLUMN conversation_summary TEXT"))
                conn.commit()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/network/ai-query
# ─────────────────────────────────────────────────────────────────────────────
@network_ai_bp.route("/api/network/ai-query", methods=["POST"])
@jwt_required()
def ai_query():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user:
        return jsonify({"error": "Forbidden"}), 403

    _LOG.info("network_ai version: %s", NETWORK_AI_VERSION)
    body    = request.get_json(silent=True) or {}
    prompt  = str(body.get("prompt", "")).strip()
    context = body.get("context", {})
    filters = context.get("filters", {})
    time_range = filters.get("time_range", "24h")
    time_filter = _get_dynamic_time_filter()(time_range)

    if not prompt:
        return jsonify({"error": "No prompt provided"}), 400

    # ── Session context (optional) ────────────────────────────────────────────
    session_id = body.get("session_id")
    ai_session = None
    conversation_history = []

    if session_id:
        _ensure_ai_session_tables()
        ai_session = db.session.get(NetworkAiSession, int(session_id))
        if ai_session and ai_session.user_id == int(uid):
            # ── CHANGE: load last 10 messages (5 turns) + prepend summary ──
            recent_msgs = (NetworkAiMessage.query
                          .filter_by(session_id=session_id)
                          .order_by(NetworkAiMessage.created_at.desc())
                          .limit(10)
                          .all())
            recent_msgs.reverse()
            # Prepend rolling summary so LLM retains full history context
            if getattr(ai_session, 'conversation_summary', None):
                conversation_history.append({
                    "role": "system",
                    "content": f"Conversation so far: {ai_session.conversation_summary}",
                })
            for m in recent_msgs:
                if m.role == "user":
                    conversation_history.append({
                        "role": "user",
                        "content": m.content,
                    })
                else:
                    cj = m.content_json or {}
                    context_parts = [m.content]

                    if cj.get("chart_type") == "multi_chart" and cj.get("charts"):
                        for i, ch in enumerate(cj["charts"], 1):
                            ch_sql = ch.get("sql", "")
                            context_parts.append(
                                f"[Chart {i}: title='{ch.get('title','')}', "
                                f"chart_type={ch.get('chart_type','line')}, "
                                f"y_axes={ch.get('y_axes',[])}]"
                            )
                            if ch_sql:
                                context_parts.append(f"[Chart {i} SQL: {ch_sql[:400]}]")
                    else:
                        if cj.get("title"):
                            context_parts.append(f"[Chart title: {cj['title']}]")
                        if cj.get("chart_type"):
                            context_parts.append(f"[Chart type: {cj['chart_type']}]")
                        if cj.get("x_axis"):
                            context_parts.append(f"[x_axis: {cj['x_axis']}]")
                        if cj.get("y_axes"):
                            context_parts.append(f"[y_axes: {cj['y_axes']}]")
                        if cj.get("sql"):
                            context_parts.append(f"[SQL used: {cj['sql'][:400]}]")

                    conversation_history.append({
                        "role": "assistant",
                        "content": "\n".join(context_parts),
                    })
            user_msg = NetworkAiMessage(
                session_id=session_id, role="user", content=prompt,
            )
            db.session.add(user_msg)
            ai_session.last_message_at = datetime.now(timezone.utc)
            db.session.commit()
        else:
            ai_session = None

    # ── Try LLM providers ─────────────────────────────────────────────────────
    provider = None
    ai_result = None

    SCHEMA_HINT = """
Tables:
1. kpi_data(id, site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
   - data_level = 'site' for site-level, 'cell' for cell-level
   - IMPORTANT: kpi_name values are EXACT strings. Use these EXACTLY as listed below.

   === RAN KPI Names (EXACT values in kpi_name column) ===
   'LTE RRC Setup Success Rate'        -- RRC success rate, accessibility
   'LTE Call Setup Success Rate'        -- Call setup success, CSSR
   'LTE E-RAB Setup Success Rate'       -- E-RAB setup
   'E-RAB Call Drop Rate_1'             -- Call drop rate, CDR
   'CSFB Access Success Rate'           -- CSFB fallback
   'LTE Intra-Freq HO Success Rate'    -- Intra-frequency handover
   'Intra-eNB HO Success Rate'         -- Intra-eNB handover
   'Inter-eNBX2HO Success Rate'        -- Inter-eNB X2 handover
   'Inter-eNBS1HO Success Rate'        -- Inter-eNB S1 handover
   'LTE DL - Cell Ave Throughput'       -- DL cell throughput (Mbps)
   'LTE UL - Cell Ave Throughput'       -- UL cell throughput
   'LTE DL - Usr Ave Throughput'        -- DL user throughput (Mbps)
   'LTE UL - User Ave Throughput'       -- UL user throughput
   'Average Latency Downlink'           -- Latency (ms)
   'DL Data Total Volume'               -- DL data volume (GB)
   'UL Data Total Volume'               -- UL data volume
   'VoLTE Traffic Erlang'               -- VoLTE traffic in Erlang
   'VoLTE Traffic UL'                   -- VoLTE UL traffic
   'VoLTE Traffic DL'                   -- VoLTE DL traffic
   'Ave RRC Connected Ue'               -- Average connected users
   'Max RRC Connected Ue'               -- Max connected users
   'Average Act UE DL Per Cell'         -- Active DL users per cell
   'Average Act UE UL Per Cell'         -- Active UL users per cell
   'Availability'                       -- Site availability %
   'Average NI of Carrier-'             -- Noise/interference
   'DL PRB Utilization (1BH)'           -- DL PRB utilization %, congestion
   'UL PRB Utilization (1BH)'           -- UL PRB utilization %

2. telecom_sites(site_id, cell_id, latitude, longitude, zone, city, state, technology, site_status, alarms)
   - JOIN with kpi_data: kpi_data k JOIN telecom_sites ts ON k.site_id = ts.site_id
   - zone = cluster/region name, city = city name, state = state name
   - technology = 'LTE', '4G', '5G', etc.
   - site_status = 'on_air' or 'off_air'

3. flexible_kpi_uploads — EAV (Entity-Attribute-Value) table for Core and Revenue data
   Columns: id, kpi_type, site_id, column_name, column_type, num_value, str_value
   - This is NOT a flat table. Each ROW stores ONE metric for ONE site.
   - column_type = 'numeric' → value is in num_value column
   - column_type = 'text'    → value is in str_value column
   - Alias the table as f: FROM flexible_kpi_uploads f

   === Revenue data (kpi_type = 'revenue') ===
   column_name values for revenue:
     'subscribers'      — subscriber count per site (numeric)
     'revenue_jan_l'    — January revenue in Lakhs (numeric)
     'revenue_feb_l'    — February revenue in Lakhs (numeric)
     'revenue_mar_l'    — March revenue in Lakhs (numeric)
     ... through 'revenue_dec_l' — pattern: revenue_<mon>_l
     'opex_jan_l'       — January OPEX in Lakhs (numeric)
     'opex_feb_l'       — February OPEX in Lakhs (numeric)
     ... through 'opex_dec_l' — pattern: opex_<mon>_l
     'zone'             — geographic zone (text, in str_value)
     'technology'       — technology type (text, in str_value)
     'site_category'    — site classification (text, in str_value)

   Example: Get total revenue per site:
     SELECT f.site_id, SUM(f.num_value) AS total_revenue
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue'
       AND f.column_name LIKE 'revenue\\_%' AND f.column_type = 'numeric'
     GROUP BY f.site_id ORDER BY total_revenue DESC

   Example: Get subscribers for a specific site:
     SELECT f.site_id, f.num_value AS subscribers
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue' AND f.column_name = 'subscribers'
       AND f.site_id = 'GUR_LTE_1500'

   Example: Get revenue for a specific month (March):
     SELECT f.site_id, f.num_value AS revenue_mar
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue' AND f.column_name = 'revenue_mar_l'

   Example: Compare revenue vs OPEX for a site:
     SELECT f.site_id, f.column_name, f.num_value
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue'
       AND f.column_name IN ('revenue_jan_l','revenue_feb_l','revenue_mar_l','opex_jan_l','opex_feb_l','opex_mar_l')
       AND f.site_id = 'GUR_LTE_1500'

   === Core KPI data (kpi_type = 'core') ===
   column_name values for core:
     'auth_success_rate'           — Authentication Success Rate (numeric, %)
     'cpu_utilization'             — CPU Utilization (numeric, %)
     'attach_success_rate'         — Attach Success Rate (numeric, %)
     'pdp_bearer_setup_success_rate' — PDP Bearer Setup Success Rate (numeric, %)

   Example: Get core KPIs for a site:
     SELECT f.site_id, f.column_name, f.num_value
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'core' AND f.site_id = 'GUR_LTE_1500'

=== Natural Language → KPI Mapping Guide ===
User says "call drop" / "drop rate" / "CDR" / "call failure" → 'E-RAB Call Drop Rate_1' (kpi_data)
User says "throughput" / "speed" / "download speed" → 'LTE DL - Usr Ave Throughput' or 'LTE DL - Cell Ave Throughput' (kpi_data)
User says "PRB" / "congestion" / "load" / "utilization" → 'DL PRB Utilization (1BH)' (kpi_data)
User says "availability" / "uptime" / "downtime" → 'Availability' (kpi_data)
User says "connected users" / "RRC users" / "active users" → 'Ave RRC Connected Ue' (kpi_data)
User says "handover" / "HO" → 'LTE Intra-Freq HO Success Rate' (kpi_data)
User says "VoLTE" / "voice" → 'VoLTE Traffic Erlang' (kpi_data)
User says "latency" / "delay" / "ping" → 'Average Latency Downlink' (kpi_data)
User says "data volume" / "traffic volume" → 'DL Data Total Volume' (kpi_data)
User says "call setup" / "CSSR" → 'LTE Call Setup Success Rate' (kpi_data)
User says "RRC" / "accessibility" / "access" → 'LTE RRC Setup Success Rate' (kpi_data)
User says "noise" / "interference" → 'Average NI of Carrier-' (kpi_data)
User says "revenue" / "income" / "earning" → revenue_data OR flexible_kpi_uploads with kpi_type='revenue'
User says "OPEX" / "operating cost" / "expenditure" → revenue_data (opex_jan/feb/mar) OR flexible_kpi_uploads
User says "subscribers" / "subscriber count" / "customer count" → revenue_data.subscribers OR flexible_kpi_uploads
User says "ARPU" → revenue / subscribers (compute from revenue_data)
User says "core KPI" / "authentication" / "CPU" / "attach" / "PDP" → core_kpi_data table
User says "transport" / "backhaul" / "microwave" / "fiber" / "jitter" → transport_kpi_data table
User says "link capacity" / "link utilization" / "backhaul latency" → transport_kpi_data table
User says "network issue" / "worst cell" / "tickets" / "AI tickets" → network_issue_tickets table
User says "last 7 days" → AND k.date >= CURRENT_DATE - INTERVAL '7 days' AND k.date <= CURRENT_DATE
User says "last month" → AND k.date >= CURRENT_DATE - INTERVAL '1 month' AND k.date <= CURRENT_DATE
ALWAYS add AND k.date <= CURRENT_DATE when any date range is used, to exclude future data.

4. transport_kpi_data — Transport/backhaul network KPI data
   Columns: id, site_id, zone, backhaul_type, link_capacity, avg_util, peak_util,
            packet_loss, avg_latency, jitter, availability, error_rate, tput_efficiency, alarms
   - backhaul_type = 'microwave', 'fiber', 'copper'
   - avg_util / peak_util = utilization percentages
   - availability = link uptime %

   Example: Get transport KPIs for a site:
     SELECT t.site_id, t.backhaul_type, t.link_capacity, t.avg_util, t.packet_loss, t.avg_latency, t.jitter, t.availability
     FROM transport_kpi_data t WHERE t.site_id = 'GUR_LTE_1500'

   Example: Sites with high packet loss:
     SELECT t.site_id, t.packet_loss, t.avg_latency, t.backhaul_type
     FROM transport_kpi_data t WHERE t.packet_loss > 1 ORDER BY t.packet_loss DESC

5. core_kpi_data — Core network KPIs with date (flat table, NOT EAV)
   Columns: id, site_id, date, auth_sr, cpu_util, attach_sr, pdp_sr
   - auth_sr = authentication success rate (%)
   - cpu_util = CPU utilization (%)
   - attach_sr = device attach success rate (%)
   - pdp_sr = PDP bearer setup success rate (%)

   Example: Core KPIs for a site over time:
     SELECT c.date::text AS date, c.auth_sr, c.cpu_util, c.attach_sr, c.pdp_sr
     FROM core_kpi_data c WHERE c.site_id = 'GUR_LTE_1500' ORDER BY c.date

6. revenue_data — Revenue per site (flat table, one row per site)
   Columns: id, site_id, zone, technology, subscribers, rev_jan, rev_feb, rev_mar,
            opex_jan, opex_feb, opex_mar, site_category
   - rev_jan/feb/mar = monthly revenue (NOT daily — there is no date column)
   - opex_jan/feb/mar = monthly OPEX

   Example: Revenue and subscribers per site:
     SELECT r.site_id, r.subscribers, r.rev_jan, r.rev_feb, r.rev_mar, r.zone
     FROM revenue_data r ORDER BY r.subscribers DESC

   Example: Top revenue sites:
     SELECT r.site_id, (r.rev_jan + r.rev_feb + r.rev_mar) AS total_revenue, r.subscribers
     FROM revenue_data r ORDER BY total_revenue DESC LIMIT 10

7. network_issue_tickets — Auto-generated tickets for worst-performing cells
   Columns: id, site_id, cells_affected, category, priority, priority_score, sla_hours,
            avg_drop_rate, avg_cssr, avg_tput, violations, status, zone, location,
            assigned_agent, root_cause, recommendation, created_at
   - status = 'open', 'in_progress', 'resolved'
   - priority = 'Critical', 'High', 'Medium', 'Low'
   - violations = number of KPI threshold breaches

   Example: Open network issue tickets:
     SELECT n.site_id, n.priority, n.avg_drop_rate, n.avg_cssr, n.avg_tput, n.violations, n.status, n.zone
     FROM network_issue_tickets n WHERE n.status IN ('open','in_progress') ORDER BY n.priority_score DESC

=== TABLE SELECTION RULE ===
- RAN performance KPIs (drop rate, throughput, PRB, latency, etc.) → query kpi_data table
- Revenue, OPEX, subscribers, ARPU → query revenue_data table FIRST; fallback to flexible_kpi_uploads with kpi_type='revenue'
- Core network KPIs (authentication, CPU, attach, PDP) → query core_kpi_data table FIRST; fallback to flexible_kpi_uploads with kpi_type='core'
- Transport/backhaul KPIs (link capacity, jitter, packet loss, backhaul) → query transport_kpi_data table
- Network issue tickets (worst cells, AI tickets, open issues) → query network_issue_tickets table
- Site info (zone, city, state, technology, location) → query telecom_sites table
NEVER query kpi_data for revenue/subscriber/OPEX data — it does not exist there.
If a table returns 0 rows, try the alternative table (e.g., revenue_data → flexible_kpi_uploads).
"""

    # ── Dynamic table availability — tell LLM which tables actually exist ──
    try:
        _tbl_rows = _sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name IN "
            "('revenue_data','core_kpi_data','transport_kpi_data',"
            "'network_issue_tickets','flexible_kpi_uploads','kpi_data','telecom_sites')"
        )
        _available_tables = {r['table_name'] for r in _tbl_rows}
    except Exception:
        _available_tables = set()

    _tbl_note = "\n=== AVAILABLE TABLES (real-time, checked just now) ===\n"
    for _tn, _fb in [
        ('kpi_data', None),
        ('telecom_sites', None),
        ('revenue_data', "flexible_kpi_uploads WHERE kpi_type='revenue'"),
        ('core_kpi_data', "flexible_kpi_uploads WHERE kpi_type='core'"),
        ('transport_kpi_data', None),
        ('network_issue_tickets', None),
        ('flexible_kpi_uploads', None),
    ]:
        if _tn in _available_tables:
            _tbl_note += f"  {_tn}: EXISTS — use it\n"
        elif _fb:
            _tbl_note += f"  {_tn}: DOES NOT EXIST — use {_fb} instead\n"
        else:
            _tbl_note += f"  {_tn}: DOES NOT EXIST — data not uploaded yet\n"
    SCHEMA_HINT += _tbl_note

    # ── CHANGE: read session_context for dynamic prompt injection ──
    _sctx = (getattr(ai_session, 'session_context', None) or {}) if ai_session else {}
    _active_sites = ", ".join(_sctx.get("active_sites", [])) or "none yet"
    _active_kpis  = ", ".join(_sctx.get("active_kpis", []))  or "none yet"
    _active_days  = _sctx.get("active_days")
    _active_days_str = f"last {_active_days} days" if _active_days else "not set"
    _last_chart   = _sctx.get("last_chart_type", "bar")

    LLM_SYSTEM = f"""You are a telecom network analytics SQL generator. Your ONLY job is to convert the user's natural-language query into an EXACT, STRICT SQL query that fetches PRECISELY what was asked — nothing more, nothing less.

READ THE USER QUERY VERY CAREFULLY. Understand EVERY part of it before generating SQL.

The user may write in English, Hindi, Hinglish, or any language. Understand the intent and translate to SQL.

{SCHEMA_HINT}

═══════════════════════════════════════════════════════════
ACTIVE SESSION STATE (always inherit this for follow-ups):
═══════════════════════════════════════════════════════════
- Sites currently in focus: {_active_sites}
- KPIs currently in focus:  {_active_kpis}
- Time range in use:        {_active_days_str}
- Last chart type:          {_last_chart}

If the user's query does not mention a new site, inherit the active_sites above.
If the user's query does not mention a new KPI, inherit the active_kpis above.
If the user's query does not mention a new time range, inherit the active_days above.

═══════════════════════════════════════════════════════════
CRITICAL RULE #00 — CONVERSATION CONTEXT & FOLLOW-UPS:
═══════════════════════════════════════════════════════════

You will receive the full conversation history. Each assistant message contains:
- The plain text response
- [Chart title: ...] — what was shown
- [Chart type: ...] — chart type used
- [SQL used: ...] — the exact SQL that was run (for single charts)
- [Chart N SQL: ...] — the SQL for each sub-chart (for multi-chart responses)

**You MUST use this history to handle follow-up queries correctly.**

Follow-up patterns and how to handle them:

1. KPI SWITCH — "what about throughput?" / "show drop rate instead"
   → User wants the SAME site and time range as the previous chart, but a DIFFERENT KPI.
   → Extract site_id and INTERVAL from the previous SQL, swap the kpi_name.

2. SITE SWITCH — "show the same for GUR_LTE_1400" / "i want to see for site id GUR_LTE_0001" / "what about site X" / "now show for X"
   → ANY prompt that names a new site ID WITHOUT specifying a new KPI = site switch.
   → Keep the EXACT same KPI(s) and INTERVAL from the previous SQL/charts. Only swap the site_id.
   → "i want to see for site id GUR_LTE_0001" with no new KPI = show same KPIs for GUR_LTE_0001.
   → If the previous response was multi_chart (multiple sites), generate a SINGLE composed chart
     for just the new site, preserving ALL the same KPIs and INTERVAL from the previous charts.
   → CRITICAL: NEVER default to PRB or any other KPI when the user only switches the site.

3. TIME RANGE CHANGE — "extend to 30 days" / "show last 10 days instead"
   → Keep everything the same, only change the INTERVAL value in the SQL.

4. CHART TYPE CHANGE — "show as bar chart" / "convert to pie"
   → Re-use the EXACT same SQL, only change chart_type in the response.

5. ADD A KPI — "also show PRB" / "overlay throughput"
   → Extend the previous SQL using UNION ALL or CASE WHEN to include the new KPI.

6. VAGUE / AMBIGUOUS — "yes", "ok", "show more", short prompts with no new site/KPI
   → Re-run the previous SQL with the same parameters.

**IMPORTANT RULES for follow-ups:**
- ALWAYS inherit site_id from previous SQL if the user doesn't mention a new one.
- ALWAYS inherit the time range (INTERVAL) from previous SQL if not specified.
- ALWAYS inherit kpi_name(s) from previous SQL if the user doesn't mention a new KPI.
- If the current prompt is completely self-contained (has site ID + KPI + time range), treat it as a FRESH query.
- A prompt like "i want to see for site id X" with NO new KPI mentioned = SITE SWITCH → inherit all KPIs.

═══════════════════════════════════════════════════════════
CRITICAL RULE #0 — MULTI-PART / COMPOUND QUERIES:
═══════════════════════════════════════════════════════════

Users often ask for MULTIPLE things in ONE query. You MUST handle ALL parts.

**How to detect multi-part queries:**
- Multiple site IDs mentioned: "site A ... and site B ..."
- Multiple KPIs mentioned: "CSSR ... and throughput ..."
- Words like "and", "also", "along with", "as well as", "plus", "both"

**MULTIPLE SITES with SAME KPI(s) → one chart PER SITE:**
When the user asks for the same KPI(s) across multiple sites, generate multi_chart with
one entry per site. Each site gets its own chart containing all the requested KPIs.

Example: "show E-RAB drop rate and CSSR last 18 days for GUR_LTE_1500 and GUR_LTE_0001"
→ TWO CHARTS: Chart 1 = GUR_LTE_1500 (both KPIs), Chart 2 = GUR_LTE_0001 (both KPIs)
→ Each chart: composed chart_type, UNION ALL SQL filtering by that site.

Example SQL for one site with two KPIs:
SELECT k.date::text AS date, k.site_id, AVG(k.value) AS value, 'E-RAB Call Drop Rate_1' AS kpi_name
FROM kpi_data k WHERE k.kpi_name = 'E-RAB Call Drop Rate_1' AND k.site_id = 'GUR_LTE_1500'
  AND k.data_level='site' AND k.value IS NOT NULL AND k.date >= CURRENT_DATE - INTERVAL '18 days' AND k.date <= CURRENT_DATE
GROUP BY k.date, k.site_id
UNION ALL
SELECT k.date::text AS date, k.site_id, AVG(k.value) AS value, 'LTE Call Setup Success Rate' AS kpi_name
FROM kpi_data k WHERE k.kpi_name = 'LTE Call Setup Success Rate' AND k.site_id = 'GUR_LTE_1500'
  AND k.data_level='site' AND k.value IS NOT NULL AND k.date >= CURRENT_DATE - INTERVAL '18 days' AND k.date <= CURRENT_DATE
GROUP BY k.date, k.site_id
ORDER BY date

**NEVER ignore part of the user's query. If they ask for 2 sites, return charts for BOTH.**

═══════════════════════════════════════════════════════════
STRICTNESS RULES — FOLLOW THESE EXACTLY:
═══════════════════════════════════════════════════════════

1. ONLY query the EXACT KPI(s) the user asked about. Do NOT add extra KPIs.
2. ONLY filter by what the user specified (site, date range, zone).
3. TODAY's date is {datetime.now().strftime('%Y-%m-%d')}. ALWAYS cap with AND k.date <= CURRENT_DATE.
4. EXTRACT THE EXACT NUMBER OF DAYS mentioned. "last 18 days" = 18, NOT 7 or 30.
5. KPI names are CASE-SENSITIVE — copy EXACTLY from the list above.
6. Site IDs are EXACT — copy every character. NEVER truncate in SQL (only title has 60-char limit).
7. ALWAYS: WHERE k.data_level='site' AND k.value IS NOT NULL. NEVER use data_level='cell'.
8. JOIN telecom_sites only when you need zone/geo data.
9. In UNION ALL queries, NEVER put ORDER BY or LIMIT inside individual SELECT branches. Put ONE ORDER BY at the very end AFTER the last UNION ALL branch.
10. "a week" / "1 week" = INTERVAL '7 days'. "a month" = INTERVAL '30 days'. Always convert to days.
11. When user says "for both" or "for site A and site B", you MUST include ALL mentioned sites — never drop any.

═══════════════════════════════════════════════════════════
CHART TYPE — MUST MATCH THE DATA SHAPE:
═══════════════════════════════════════════════════════════

- "line"     → Time series, 1 site, 1 KPI
- "composed" → Multiple KPIs or multiple series on same time axis
- "bar"      → Ranking/comparison of sites (x=site_id)
- "area"     → Network-wide aggregated trend
- "pie"      → Distribution/proportion
- "scatter"  → Correlation between 2 KPIs
- "radar"    → Multi-KPI profile, few sites

═══════════════════════════════════════════════════════════
RESPONSE FORMAT:
═══════════════════════════════════════════════════════════

**For SINGLE chart:**
{{
  "sql": "SELECT ...",
  "title": "Short title (max 60 chars)",
  "response": "1-2 sentence description",
  "chart_type": "line|bar|composed|area|pie|scatter|radar",
  "x_axis": "column_name",
  "y_axes": ["metric_col"],
  "chart_config": {{"x_label":"","y_label":"","threshold":null,"threshold_dir":"above|below","color_scheme":"sequential"}},
  "filter_update": {{}}
}}

**For MULTI-PART queries (different sites, or incompatible time ranges/units):**
{{
  "multi_chart": true,
  "title": "Overall title (max 80 chars)",
  "response": "1-2 sentences describing all charts",
  "charts": [
    {{"sql":"...","title":"Chart 1 title","chart_type":"line|composed","x_axis":"date","y_axes":["metric"]}},
    {{"sql":"...","title":"Chart 2 title","chart_type":"line|composed","x_axis":"date","y_axes":["metric"]}}
  ],
  "filter_update": {{}}
}}

**Use multi_chart when:** user mentions 2+ site IDs (one chart per site), OR 2 incompatible time ranges.
**Use single composed chart when:** two KPIs for the SAME site on the same time axis.

Respond ONLY with valid JSON (no markdown, no code fences, no extra text)."""

    def _strip_json(raw):
        raw = raw.strip()
        if "```" in raw:
            parts = raw.split("```")
            for part in parts[1:]:
                part = part.strip()
                if part.lower().startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    raw = part
                    break
        idx = raw.find("{")
        if idx > 0:
            raw = raw[idx:]
        depth = 0
        end = -1
        for i, c in enumerate(raw):
            if c == "{": depth += 1
            elif c == "}": depth -= 1
            if depth == 0 and c == "}":
                end = i
                break
        if end > 0:
            raw = raw[:end+1]
        return raw.strip()

    def _parse_ai_result(raw_text):
        parsed = json.loads(_strip_json(raw_text))
        if parsed.get("multi_chart") and parsed.get("charts"):
            if "title" not in parsed:
                parsed["title"] = prompt[:70]
            if "response" not in parsed:
                parsed["response"] = parsed.get("title", "Results")
            for chart in parsed["charts"]:
                if chart.get("sql"):
                    chart["sql"] = _sanitize_llm_sql(chart["sql"])
            return parsed
        ct = parsed.get("chart_type") or parsed.get("chart") or parsed.get("query_type") or "bar"
        parsed["chart_type"] = ct
        parsed["query_type"] = ct
        if "title" not in parsed:
            parsed["title"] = prompt[:60]
        if "response" not in parsed:
            parsed["response"] = parsed.get("title", "Results")
        if "x_axis" not in parsed:
            parsed["x_axis"] = "date" if ct in ("line","area","composed") else "site_id"
        if "y_axes" not in parsed:
            parsed["y_axes"] = []
        if "chart_config" not in parsed:
            parsed["chart_config"] = {}
        if parsed.get("sql"):
            parsed["sql"] = _sanitize_llm_sql(parsed["sql"])
        return parsed

    # ── CHANGE: comprehensive SQL sanitizer for all known LLM mistakes ──
    def _sanitize_llm_sql(sql: str) -> str:
        """Fix common LLM-generated SQL mistakes before execution."""
        import re
        if not sql or not sql.strip():
            return sql

        # 1. Fix truncated site IDs (LLM cuts off last chars)
        prompt_site_ids = re.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', prompt)
        for correct_id in prompt_site_ids:
            for trim in range(1, 4):
                truncated = correct_id[:-trim]
                if len(truncated) < 6:
                    break
                pattern = r"(site_id\s*=\s*')(" + re.escape(truncated) + r")(')"
                replacement = r"\g<1>" + correct_id + r"\g<3>"
                fixed = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
                if fixed != sql:
                    _LOG.info("[SQL-fix] truncated site ID: '%s' → '%s'", truncated, correct_id)
                    sql = fixed

        # 2. Fix ORDER BY inside UNION ALL branches (PostgreSQL syntax error)
        if re.search(r'\bUNION\s+ALL\b', sql, re.IGNORECASE):
            parts = re.split(r'\bUNION\s+ALL\b', sql, flags=re.IGNORECASE)
            if len(parts) > 1:
                cleaned = []
                for part in parts:
                    part = re.sub(r'\s+ORDER\s+BY\s+[\w\s,.]+$', '', part.strip(), flags=re.IGNORECASE)
                    cleaned.append(part)
                sql = '\nUNION ALL\n'.join(cleaned) + '\nORDER BY date'
                _LOG.info("[SQL-fix] moved ORDER BY to end of UNION ALL")

        # 3. Fix data_level='cell' → 'site' (prompt rule #7 says ALWAYS 'site')
        if re.search(r"data_level\s*=\s*'cell'", sql, re.IGNORECASE):
            sql = re.sub(r"data_level\s*=\s*'cell'", "data_level = 'site'", sql, flags=re.IGNORECASE)
            _LOG.info("[SQL-fix] changed data_level='cell' → 'site'")

        # 4. Ensure k.date <= CURRENT_DATE cap exists when date range is used
        if re.search(r"CURRENT_DATE\s*-\s*INTERVAL", sql, re.IGNORECASE):
            if not re.search(r"date\s*<=\s*CURRENT_DATE", sql, re.IGNORECASE):
                sql = re.sub(
                    r"(CURRENT_DATE\s*-\s*INTERVAL\s*'[^']+'\s*)",
                    r"\1AND k.date <= CURRENT_DATE ",
                    sql, count=1, flags=re.IGNORECASE,
                )
                _LOG.info("[SQL-fix] added missing k.date <= CURRENT_DATE cap")

        # 5. Ensure k.value IS NOT NULL exists
        if 'kpi_data' in sql.lower() and 'is not null' not in sql.lower():
            sql = re.sub(
                r"(data_level\s*=\s*'site')",
                r"\1 AND k.value IS NOT NULL",
                sql, count=1, flags=re.IGNORECASE,
            )
            _LOG.info("[SQL-fix] added missing k.value IS NOT NULL")

        # 6. Fix LIMIT inside UNION ALL branches (PostgreSQL syntax error)
        if re.search(r'\bUNION\s+ALL\b', sql, re.IGNORECASE):
            parts = re.split(r'\bUNION\s+ALL\b', sql, flags=re.IGNORECASE)
            if len(parts) > 1:
                cleaned = []
                for part in parts:
                    part = re.sub(r'\s+LIMIT\s+\d+\s*$', '', part.strip(), flags=re.IGNORECASE)
                    cleaned.append(part)
                # Preserve the ORDER BY we already placed at end
                if re.search(r'\bORDER\s+BY\b', cleaned[-1], re.IGNORECASE):
                    sql = '\nUNION ALL\n'.join(cleaned)
                else:
                    sql = '\nUNION ALL\n'.join(cleaned) + '\nORDER BY date'

        return sql

    user_prompt = f"User query: {prompt}"
    if filters.get("cluster"):
        user_prompt += f"\nActive filter — Zone: {filters['cluster']}"
    if filters.get("time_range") and filters["time_range"] != "24h":
        user_prompt += f"\nActive filter — Time range: {filters['time_range']}"

    llm_messages = [{"role": "system", "content": LLM_SYSTEM}]
    llm_messages.extend(conversation_history)
    llm_messages.append({"role": "user", "content": user_prompt})

    from app import client as _llm_client, DEPLOYMENT_NAME as _llm_model
    _LOG.info("AI provider: global client (%s)", _llm_model)

    # ── PRE-LLM INTERCEPTOR ────────────────────────────────────────────────────
    # Handle certain query patterns with rule-based logic BEFORE calling the LLM.
    # This ensures follow-ups (site switch, time change, chart type) and
    # multi-site trend queries are handled correctly and consistently,
    # regardless of which LLM provider is configured.
    import re as _re_pre

    def _get_prev_context_for_intercept():
        """Load the most recent assistant message's content_json from the session."""
        if not (ai_session and session_id):
            return None
        try:
            last_asst = (NetworkAiMessage.query
                         .filter_by(session_id=session_id, role="assistant")
                         .order_by(NetworkAiMessage.created_at.desc())
                         .first())
            return last_asst.content_json if last_asst else None
        except Exception:
            return None

    _p_lower = prompt.lower().strip()
    _prompt_sites = _re_pre.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', prompt)
    # ── CHANGE: use unified time parser instead of just regex for "last N days" ──
    _prompt_days_int = _parse_time_to_days(_p_lower)

    # 1. Follow-up detection — run rule-based BEFORE LLM so context is never lost
    if _is_followup(_p_lower):
        _prev_ctx = _get_prev_context_for_intercept()
        if _prev_ctx:
            _fu = _handle_followup(prompt, _p_lower, _prev_ctx, time_filter)
            if _fu:
                ai_result = _fu
                provider  = {"provider": "rule-based-followup"}
                _LOG.info("Follow-up intercepted before LLM: site-switch / chart-change / time-change")

    # 2. Multi-site trend queries — rule-based reliably generates one chart per site
    #    with ALL requested KPIs, which LLMs often get wrong.
    # ── CHANGE: detect "a week", "a month" etc. via _parse_time_to_days ──
    if not ai_result and len(_prompt_sites) >= 2:
        _is_trend_pre = (
            bool(_prompt_days_int) or
            any(w in _p_lower for w in ('trend', 'over time', 'history', 'daily', 'last', 'week', 'month', 'year'))
        )
        if _is_trend_pre:
            ai_result = _rule_based_query(prompt, time_filter, prev_context=None)
            provider  = {"provider": "rule-based-multisite"}
            _LOG.info("Multi-site trend intercepted before LLM: %s", _prompt_sites)

    # 3. LLM handles all queries (revenue, core, transport, tickets, KPIs).
    #    Dynamic schema (AVAILABLE TABLES section) tells LLM which tables exist.
    #    Rule-based handlers remain as fallback if LLM SQL fails at execution.
    if not ai_result:
        try:
            _llm_resp = _llm_client.chat.completions.create(
                model=_llm_model,
                messages=llm_messages,
                temperature=0.1,
                max_tokens=2000,
            )
            raw_content = _llm_resp.choices[0].message.content
            if raw_content:
                ai_result = _parse_ai_result(raw_content)
                provider = {"provider": _llm_model}
                _LOG.info("AI query handled by %s", _llm_model)
        except json.JSONDecodeError as je:
            _LOG.warning("LLM returned bad JSON: %s", str(je)[:200])
        except Exception as e:
            _LOG.warning("LLM call failed, falling back to rule-based: %s", str(e)[:200])

    # ── Fallback: rule-based query engine ─────────────────────────────────────
    if not ai_result:
        prev_context = None
        if ai_session and session_id:
            try:
                last_asst = (NetworkAiMessage.query
                             .filter_by(session_id=session_id, role="assistant")
                             .order_by(NetworkAiMessage.created_at.desc())
                             .first())
                if last_asst and last_asst.content_json:
                    prev_context = last_asst.content_json
            except Exception:
                pass
        ai_result = _rule_based_query(prompt, time_filter, prev_context=prev_context)
        if not provider:
            provider = {"provider": "rule-based"}
        _LOG.info("AI query handled by rule-based fallback")

    # ── Helper functions ──────────────────────────────────────────────────────
    def _sql_with_timeout(query, timeout_sec=10):
        with db.engine.connect() as conn:
            conn.execute(sa_text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))
            result = conn.execute(sa_text(query))
            cols = list(result.keys())
            return [dict(zip(cols, row)) for row in result.fetchall()]

    def _serial(v):
        if v is None: return None
        if hasattr(v, "isoformat"): return v.isoformat()
        if isinstance(v, float) and math.isnan(v): return None
        try:    return float(v)
        except: return str(v)

    # ── MULTI-CHART: execute each chart's SQL separately ───────────────────────
    if ai_result.get("multi_chart") and ai_result.get("charts"):
        charts_out = []
        for chart_spec in ai_result["charts"]:
            c_sql = chart_spec.get("sql", "")
            c_error = None
            c_rows = []
            try:
                c_rows = _sql_with_timeout(c_sql, timeout_sec=15)
                if not c_rows:
                    _LOG.warning("Multi-chart SQL returned 0 rows — SQL: %s", c_sql[:300])
                    c_error = "Query returned no data. The site ID or KPI may not exist in the database."
            except Exception as e:
                _LOG.warning("Multi-chart SQL failed: %s — SQL: %s", e, c_sql[:300])
                c_error = str(e)
                c_rows = []
            c_cols = list(c_rows[0].keys()) if c_rows else []
            c_safe = [{k: _serial(v) for k, v in r.items()} for r in c_rows]
            chart_entry = {
                "title":      chart_spec.get("title", ""),
                "chart_type": chart_spec.get("chart_type", "line"),
                "x_axis":     chart_spec.get("x_axis", c_cols[0] if c_cols else "date"),
                "y_axes":     chart_spec.get("y_axes", c_cols[1:] if c_cols else []),
                "data":       c_safe,
                "columns":    c_cols,
                "row_count":  len(c_rows),
                "sql":        c_sql,
            }
            if c_error:
                chart_entry["error"] = c_error
            charts_out.append(chart_entry)

        resp_text = ai_result.get("response", f"Here are {len(charts_out)} charts.")
        resp_title = ai_result.get("title", prompt[:70])

        if ai_session:
            try:
                if ai_session.title == "New Chat":
                    ai_session.title = (resp_title or prompt[:60])[:200]
                assistant_msg = NetworkAiMessage(
                    session_id=ai_session.id, role="assistant",
                    content=resp_text,
                    content_json={
                        "title": resp_title, "chart_type": "multi_chart",
                        "charts": charts_out, "response": resp_text,
                        "provider": provider["provider"] if provider else "rule-based",
                    },
                )
                db.session.add(assistant_msg)
                db.session.commit()
            except Exception as e:
                _LOG.error("Failed to persist AI message: %s", e)
            # ── CHANGE: update session context + maybe summarize ──
            _all_sqls = " ".join(ch.get("sql", "") for ch in charts_out)
            _update_session_context(ai_session, _all_sqls, "multi_chart")
            _maybe_summarize_conversation(ai_session)

        return jsonify({
            "response":     resp_text,
            "query_type":   "multi_chart",
            "chart_type":   "multi_chart",
            "title":        resp_title,
            "charts":       charts_out,
            "data":         [],
            "columns":      [],
            "row_count":    sum(c["row_count"] for c in charts_out),
            "provider":     provider["provider"] if provider else "rule-based",
            "session_id":   ai_session.id if ai_session else None,
        })

    # ── SINGLE CHART: execute SQL normally ─────────────────────────────────────
    sql = ai_result.get("sql", "")
    if not sql or not sql.strip().upper().startswith("SELECT"):
        return jsonify({"error": "Could not generate a safe query"}), 400

    try:
        rows = _sql_with_timeout(sql, timeout_sec=15)
    except Exception as e:
        _LOG.warning("AI SQL execution failed: %s — SQL: %s", e, sql[:200])
        try:
            fallback = _rule_based_query(prompt, time_filter)
            sql2 = fallback.get("sql", "")
            if sql2 and sql2.strip().upper().startswith("SELECT"):
                rows = _sql_with_timeout(sql2, timeout_sec=10)
                ai_result.update(fallback)
                sql = sql2
                if not provider:
                    provider = {"provider": "rule-based-fallback"}
            else:
                rows = []
        except Exception:
            rows = []

    columns = list(rows[0].keys()) if rows else []
    has_geo = any(r.get("lat") or r.get("latitude") for r in rows)
    safe_rows = [{k: _serial(v) for k, v in r.items()} for r in rows]
    y_axes = ai_result.get("y_axes") or [
        c for c in columns[1:5]
        if c not in ("lat", "lng", "latitude", "longitude", "site_id", "cell_id", "cluster", "region", "technology")
    ]

    resp_text = ai_result.get("response", f"Found {len(rows)} results.")
    resp_title = ai_result.get("title", prompt[:70])
    resp_chart = ai_result.get("chart_type", ai_result.get("query_type", "bar"))

    if ai_session:
        try:
            if ai_session.title == "New Chat":
                ai_session.title = (resp_title or prompt[:60])[:200]
            assistant_msg = NetworkAiMessage(
                session_id=ai_session.id,
                role="assistant",
                content=resp_text,
                content_json={
                    "title": resp_title,
                    "data": safe_rows,
                    "columns": columns,
                    "x_axis": ai_result.get("x_axis", columns[0] if columns else ""),
                    "y_axes": y_axes,
                    "chart_type": resp_chart,
                    "chart_config": ai_result.get("chart_config", {}),
                    "row_count": len(rows),
                    "sql": sql,
                    "provider": provider["provider"] if provider else "rule-based",
                    "response": resp_text,
                },
            )
            db.session.add(assistant_msg)
            db.session.commit()
        except Exception as e:
            _LOG.error("Failed to persist AI message: %s", e)
        # ── CHANGE: update session context + maybe summarize ──
        _update_session_context(ai_session, sql, resp_chart)
        _maybe_summarize_conversation(ai_session)

    return jsonify({
        "response":      resp_text,
        "query_type":    ai_result.get("query_type", "bar"),
        "chart_type":    resp_chart,
        "chart_config":  ai_result.get("chart_config", {}),
        "title":         resp_title,
        "x_axis":        ai_result.get("x_axis", columns[0] if columns else ""),
        "y_axes":        y_axes,
        "data":          safe_rows,
        "columns":       columns,
        "row_count":     len(rows),
        "sql":           sql,
        "filter_update": ai_result.get("filter_update", {}),
        "tab":           ai_result.get("tab"),
        "show_map":      has_geo or ai_result.get("show_map", False),
        "provider":      provider["provider"] if provider else "rule-based",
        "session_id":    ai_session.id if ai_session else None,
    })


# ─────────────────────────────────────────────────────────────────────────────
# CHANGE: Session context tracking + conversation summarization
# ─────────────────────────────────────────────────────────────────────────────

def _extract_session_context(sql_str, chart_type="bar"):
    """Parse active sites, KPIs, days, chart type from an executed SQL string.
    Returns a dict suitable for storing in ai_session.session_context."""
    import re
    ctx = {
        "active_sites": [],
        "active_kpis": [],
        "active_days": None,
        "last_chart_type": chart_type or "bar",
        "last_sql": (sql_str or "")[:2000],
    }
    if not sql_str:
        return ctx
    ctx["active_sites"] = list(dict.fromkeys(re.findall(r"site_id\s*=\s*'([^']+)'", sql_str)))
    ctx["active_kpis"]  = list(dict.fromkeys(re.findall(r"kpi_name\s*=\s*'([^']+)'", sql_str)))
    days_m = re.search(r"INTERVAL\s+'(\d+)\s+days?'", sql_str)
    if days_m:
        ctx["active_days"] = int(days_m.group(1))
    return ctx


def _update_session_context(ai_session, sql_str, chart_type="bar"):
    """Update ai_session.session_context from the SQL that was just executed.
    MERGES new sites/KPIs with existing ones so earlier context isn't lost.
    Wrapped in try/except so it never breaks the main response."""
    if not ai_session:
        return
    try:
        new_ctx = _extract_session_context(sql_str, chart_type)
        old_ctx = (getattr(ai_session, 'session_context', None) or {})

        # ── CHANGE: merge instead of replace — keep last 8 sites/KPIs seen ──
        merged_sites = list(dict.fromkeys(
            old_ctx.get("active_sites", []) + new_ctx.get("active_sites", [])
        ))[-8:]  # keep last 8 unique sites
        merged_kpis = list(dict.fromkeys(
            old_ctx.get("active_kpis", []) + new_ctx.get("active_kpis", [])
        ))[-6:]  # keep last 6 unique KPIs

        new_ctx["active_sites"] = merged_sites
        new_ctx["active_kpis"]  = merged_kpis
        # Days and chart type always use the latest
        ai_session.session_context = new_ctx
        db.session.commit()
    except Exception as e:
        _LOG.warning("Failed to update session_context: %s", e)
        try:
            db.session.rollback()
        except Exception:
            pass


def _maybe_summarize_conversation(ai_session):
    """Generate a rolling summary — first at 8 messages, then every 8 after.
    This ensures the summary exists BEFORE old messages drop out of the history window.
    Wrapped in try/except so it never breaks the main response."""
    if not ai_session:
        return
    try:
        msg_count = NetworkAiMessage.query.filter_by(session_id=ai_session.id).count()
        # ── CHANGE: trigger at 8 instead of 10, so summary exists before messages drop ──
        if msg_count < 8 or msg_count % 8 != 0:
            return

        # Grab last 12 messages for summarization (wider window for better summary)
        last_10 = (NetworkAiMessage.query
                   .filter_by(session_id=ai_session.id)
                   .order_by(NetworkAiMessage.created_at.desc())
                   .limit(12).all())
        last_10.reverse()
        convo_text = "\n".join(
            f"{m.role}: {m.content[:300]}" for m in last_10
        )

        summary_prompt = (
            "Summarize this telecom analytics conversation in 3-4 sentences. "
            "Focus on: which sites were analyzed, which KPIs were discussed, "
            "what time ranges were used, and what insights were found.\n\n"
            f"Conversation:\n{convo_text}"
        )
        summary_msgs = [
            {"role": "system", "content": "You are a concise summarizer."},
            {"role": "user", "content": summary_prompt},
        ]

        _cfg = lambda k, default="": current_app.config.get(k, "") or os.environ.get(k, "") or default
        azure_key   = _cfg("AZURE_OPENAI_API_KEY")
        azure_ep    = _cfg("AZURE_OPENAI_ENDPOINT")
        azure_dep   = _cfg("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
        azure_ver   = _cfg("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
        gemini_key  = _cfg("GEMINI_API_KEY")
        openai_key  = _cfg("OPENAI_API_KEY")

        summary_text = None

        if azure_key and azure_ep:
            from openai import AzureOpenAI as _AzOAI
            c = _AzOAI(api_key=azure_key, api_version=azure_ver, azure_endpoint=azure_ep, timeout=15.0)
            r = c.chat.completions.create(model=azure_dep, messages=summary_msgs, temperature=0.3, max_tokens=300)
            summary_text = r.choices[0].message.content
        elif gemini_key:
            from openai import OpenAI as _OAI
            c = _OAI(api_key=gemini_key, base_url="https://generativelanguage.googleapis.com/v1beta/openai/", timeout=15.0)
            r = c.chat.completions.create(model=_cfg("OPENAI_MODEL", "gemini-2.0-flash"), messages=summary_msgs, temperature=0.3, max_tokens=300)
            summary_text = r.choices[0].message.content
        elif openai_key:
            from openai import OpenAI as _OAI2
            c = _OAI2(api_key=openai_key, timeout=15.0)
            r = c.chat.completions.create(model="gpt-4o-mini", messages=summary_msgs, temperature=0.3, max_tokens=300)
            summary_text = r.choices[0].message.content

        if summary_text:
            ai_session.conversation_summary = summary_text.strip()
            db.session.commit()
            _LOG.info("Conversation summary updated for session %d (%d msgs)", ai_session.id, msg_count)
    except Exception as e:
        _LOG.warning("Summarization failed (non-fatal): %s", e)
        try:
            db.session.rollback()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Shared time-range parser
# ─────────────────────────────────────────────────────────────────────────────

# ── CHANGE: unified time parser that handles "a week", "1 month", "last 18 days", etc. ──
def _parse_time_to_days(text: str):
    """Extract a day count from natural-language time references.
    Returns int (number of days) or None if no time reference found."""
    import re
    t = text.lower().strip()

    # "last 18 days", "past 7 days", "recent 30 days"
    m = re.search(r'(?:last|past|recent)\s+(\d+)\s*days?', t)
    if m:
        return int(m.group(1))

    # "18 days", "7 day" (bare number + days)
    m = re.search(r'(\d+)\s*days?', t)
    if m and not re.search(r'(top|bottom|worst|best)\s+' + m.group(1), t):
        return int(m.group(1))

    # Word-based: "a week", "one week", "1 week", "2 weeks", "a month", etc.
    _WORD_MAP = {
        'week': 7, 'weeks': 7,
        'month': 30, 'months': 30,
        'year': 365, 'years': 365,
        'quarter': 90, 'quarters': 90,
        'fortnight': 14, 'fortnights': 14,
    }
    # "last week", "a week", "1 week", "one week", "2 weeks", "last 3 months"
    m = re.search(
        r'(?:last|past|a|an|one|1|(\d+))\s+(week|weeks|month|months|year|years|quarter|quarters|fortnight|fortnights)',
        t
    )
    if m:
        multiplier = int(m.group(1)) if m.group(1) else 1
        unit = m.group(2)
        return multiplier * _WORD_MAP.get(unit, 7)

    # "for a week", "for one month"
    m = re.search(
        r'for\s+(?:a|an|one|1|(\d+))\s+(week|weeks|month|months|year|years|quarter|quarters)',
        t
    )
    if m:
        multiplier = int(m.group(1)) if m.group(1) else 1
        unit = m.group(2)
        return multiplier * _WORD_MAP.get(unit, 7)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Follow-up / Conversation Context Handling
# ─────────────────────────────────────────────────────────────────────────────

_FU_KPI_WORDS = {
    'cssr', 'call setup', 'rrc', 'drop', 'cdr', 'erab', 'e-rab', 'e rab',
    'throughput', 'tput', 'dl throughput', 'speed', 'prb', 'congestion',
    'availability', 'latency', 'delay', 'volte', 'handover', 'volume',
    'traffic', 'connected', 'users',
}

_NEW_QUERY_WORDS = {
    'top', 'bottom', 'worst', 'best', 'compare zones', 'zone wise',
    'overall', 'network wide', 'all sites', 'show me', 'give me',
}


def _is_followup(prompt_lower: str) -> bool:
    """
    Returns True if this prompt is modifying/extending the previous chart
    rather than starting a completely new query.
    """
    import re
    p = prompt_lower.strip()
    words = p.split()

    has_site     = bool(re.findall(r'[a-z]{2,}[_\-][a-z]{2,}[_\-]\d{3,}', p))
    has_kpi      = any(kw in p for kw in _FU_KPI_WORDS)
    # ── CHANGE: use unified time parser so "a week", "a month" are detected ──
    has_time_ref = bool(_parse_time_to_days(p))

    # 1. Truly self-contained: site + explicit time reference → always fresh
    if has_site and has_time_ref:
        return False

    # 2. Explicit ranking/network-wide → always fresh
    if re.search(r'(top|bottom|worst|best)\s+\d+', p):
        return False

    # 3. Multiple different site IDs → multi-site fresh query
    #    (handled separately by the pre-LLM interceptor)
    _all_sites = re.findall(r'[a-z]{2,}[_\-][a-z]{2,}[_\-]\d{3,}', p)
    if len(_all_sites) >= 2:
        return False

    # 4. Explicit modification keywords → definitely follow-up
    #    Covers: "not cssr", "instead of", "only line", "switch to erab", etc.
    mod_keywords = [
        ' not ', 'instead', 'rather than', 'in place of',
        'only line', 'only bar', 'only area', 'just line', 'just bar',
        'switch to', 'change to', 'show as', 'display as', 'convert to',
        'the graph', 'the chart', 'this graph', 'this chart', 'that chart',
        'same', 'previous', 'last one', 'above', 'earlier',
        'the data', 'the result', 'instead', 'rather', 'in place',
        'swap', 'replace', 'for this', 'for that',
        'scale', 'zoom', 'resize', 'bigger', 'smaller',
        'enlarge', 'expand', 'more days', 'fewer days', 'extend', 'shorten',
        'add', 'also show', 'overlay', 'combine',
        'remove', 'hide', 'exclude', 'colour', 'color',
        'bar chart', 'line chart', 'pie chart', 'area chart',
        'line graph', 'bar graph',
        'make it', 'turn it',
    ]
    if any(kw in p for kw in mod_keywords):
        return True

    # 5. Site-only (no KPI, no time) → site-switch follow-up
    #    e.g. "show me GUR_LTE_0001" / "i want to see for site id GUR_LTE_0001"
    _SITE_SWITCH_BLOCKERS = {'top', 'bottom', 'worst', 'best', 'compare', 'zone', 'all sites', 'network'}
    if has_site and not has_kpi and not has_time_ref:
        if not any(nw in p for nw in _SITE_SWITCH_BLOCKERS):
            return True

    # 6. Very short prompts with no site and no KPI → vague continuation
    if len(words) <= 5 and not has_kpi and not has_site:
        return True

    # 7. Polite one-word confirmations
    if p in ('yes', 'ok', 'sure', 'do it', 'go ahead', 'please do',
             'please', 'thanks', 'thank you', 'good', 'nice', 'great'):
        return True

    # 8. KPI-only prompt (no site, no time) → KPI switch on same site
    if has_kpi and not has_site and not has_time_ref and len(words) <= 10:
        _NEW_QUERY_BLOCKERS = {'top', 'bottom', 'worst', 'best', 'compare', 'zone', 'all sites', 'network wide', 'overall'}
        if not any(nw in p for nw in _NEW_QUERY_BLOCKERS):
            return True

    return False



def _handle_followup(prompt_orig: str, p: str, prev: dict, time_filter: str) -> dict:
    import re

    prev_sql    = prev.get("sql", "")
    prev_title  = prev.get("title", "")
    prev_chart  = prev.get("chart_type", "bar")
    prev_y      = prev.get("y_axes", [])
    prev_x      = prev.get("x_axis", "date")
    prev_response = prev.get("response", "")
    prev_cfg    = prev.get("chart_config", {}) or {}
    prev_charts = prev.get("charts", [])

    if not prev_sql and not prev_charts:
        return None

    # Extract context from main SQL
    prev_sites    = re.findall(r"site_id\s*=\s*'([^']+)'", prev_sql)
    prev_kpi_names = re.findall(r"kpi_name\s*=\s*'([^']+)'", prev_sql)
    prev_days_m   = re.search(r"INTERVAL\s+'(\d+)\s+days?'", prev_sql)
    prev_days     = int(prev_days_m.group(1)) if prev_days_m else None

    # ── FIX: When prev was multi_chart, harvest KPIs, sites, and days from ALL sub-charts ──
    if prev_charts:
        for ch in prev_charts:
            ch_sql = ch.get("sql", "")
            for kpi in re.findall(r"kpi_name\s*=\s*'([^']+)'", ch_sql):
                if kpi not in prev_kpi_names:
                    prev_kpi_names.append(kpi)
            for site in re.findall(r"site_id\s*=\s*'([^']+)'", ch_sql):
                if site not in prev_sites:
                    prev_sites.append(site)
            if not prev_days:
                m = re.search(r"INTERVAL\s+'(\d+)\s+days?'", ch_sql)
                if m:
                    prev_days = int(m.group(1))

    KPI_MAP = {
        'cssr': ('LTE Call Setup Success Rate', 'cssr'),
        'call setup': ('LTE Call Setup Success Rate', 'cssr'),
        'rrc': ('LTE RRC Setup Success Rate', 'rrc_sr'),
        'erab': ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'e-rab': ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'e rab': ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'drop': ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'cdr': ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'throughput': ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'tput': ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'dl throughput': ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'speed': ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'prb': ('DL PRB Utilization (1BH)', 'dl_prb'),
        'congestion': ('DL PRB Utilization (1BH)', 'dl_prb'),
        'availability': ('Availability', 'availability'),
        'latency': ('Average Latency Downlink', 'latency'),
        'delay': ('Average Latency Downlink', 'latency'),
        'volte': ('VoLTE Traffic Erlang', 'volte_erl'),
        'handover': ('LTE Intra-Freq HO Success Rate', 'ho_sr'),
        'volume': ('DL Data Total Volume', 'dl_volume'),
        'traffic': ('DL Data Total Volume', 'dl_volume'),
        'connected': ('Ave RRC Connected Ue', 'avg_rrc_ue'),
        'users': ('Ave RRC Connected Ue', 'avg_rrc_ue'),
    }

    def _detect_kpi(text):
        t = text.lower()
        import re as _re_dk
        # First, identify KPIs the user wants to EXCLUDE ("not cssr", "instead of cssr")
        excluded_kpis = set()
        for kw, (kn, al) in KPI_MAP.items():
            if (_re_dk.search(r'\bnot\s+' + _re_dk.escape(kw), t) or
                    _re_dk.search(r'instead\s+of\s+' + _re_dk.escape(kw), t) or
                    _re_dk.search(r'not\s+this\s+' + _re_dk.escape(kw), t)):
                excluded_kpis.add(kn)
        # Return the first KPI that is NOT excluded
        for kw in sorted(KPI_MAP.keys(), key=len, reverse=True):
            if kw in t and KPI_MAP[kw][0] not in excluded_kpis:
                return KPI_MAP[kw]
        return None

    new_sites  = re.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', prompt_orig)
    new_kpi    = _detect_kpi(p)
    # ── CHANGE: use unified time parser for "a week", "a month", etc. ──
    new_days   = _parse_time_to_days(p)

    # ── FIX: Site switch — new site mentioned, no new KPI → inherit ALL previous KPIs ──
    # This handles: "i want to see for site id GUR_LTE_0001" after a multi-chart response.
    if new_sites and not new_kpi and prev_kpi_names:
        site = new_sites[0]
        days = new_days or prev_days or 14
        date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE"

        if len(prev_kpi_names) == 1:
            kpi_name = prev_kpi_names[0]
            alias = 'value'
            for kw, (kn, al) in KPI_MAP.items():
                if kn == kpi_name:
                    alias = al
                    break
            return {
                "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {alias}
                    FROM kpi_data k
                    WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                      AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                    GROUP BY k.date ORDER BY k.date""",
                "query_type": "line", "chart_type": "line",
                "title": f"{kpi_name} — {site} (last {days}d)",
                "x_axis": "date", "y_axes": [alias],
                "response": f"Showing {kpi_name} for site {site} over last {days} days.",
            }
        else:
            # Multiple inherited KPIs → composed chart with UNION ALL
            parts_sql = []
            for kpi_name in prev_kpi_names[:3]:
                parts_sql.append(f"""SELECT k.date::text AS date, k.site_id,
                       AVG(k.value) AS value, '{kpi_name}' AS kpi_name
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                  AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date, k.site_id""")
            kpi_short = " & ".join(
                k.replace("LTE ", "").replace("E-RAB ", "")[:18]
                for k in prev_kpi_names[:3]
            )
            return {
                "sql": "\nUNION ALL\n".join(parts_sql) + "\nORDER BY date",
                "query_type": "composed", "chart_type": "composed",
                "title": f"{site} — {kpi_short} (last {days}d)",
                "x_axis": "date", "y_axes": ["value"],
                "response": f"Showing {', '.join(prev_kpi_names[:3])} for site {site} over last {days} days.",
            }

    # KPI switch — new KPI, same site
    if new_kpi and not new_sites and prev_sites:
        kpi_name, alias = new_kpi
        days = new_days or prev_days or 14
        site = prev_sites[0]
        date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE"
        return {
            "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {alias}
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                  AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date ORDER BY k.date""",
            "query_type": "line", "chart_type": "line",
            "title": f"{kpi_name} — {site} (last {days}d)",
            "x_axis": "date", "y_axes": [alias],
            "response": f"Switched to {kpi_name} for site {site} over last {days} days.",
        }

    # Chart type switch
    new_chart = None
    if 'bar' in p and ('chart' in p or 'graph' in p or 'as bar' in p):
        new_chart = 'bar'
    elif 'line' in p and ('chart' in p or 'graph' in p or 'as line' in p):
        new_chart = 'line'
    elif 'pie' in p:
        new_chart = 'pie'
    elif 'area' in p and ('chart' in p or 'graph' in p):
        new_chart = 'area'
    elif 'table' in p:
        new_chart = 'bar'

    if new_chart and prev_sql:
        return {
            "sql": prev_sql,
            "query_type": new_chart, "chart_type": new_chart,
            "title": prev_title, "x_axis": prev_x, "y_axes": prev_y,
            "response": f"Changed chart to {new_chart} view.",
        }

    # Time range change
    if new_days and prev_sql:
        updated_sql   = re.sub(r"INTERVAL\s+'(\d+)\s+days?'", f"INTERVAL '{new_days} days'", prev_sql)
        updated_title = re.sub(r'\(last \d+d\)', f'(last {new_days}d)', prev_title)
        if updated_title == prev_title:
            updated_title = prev_title + f" (last {new_days}d)"

        if prev_charts:
            new_charts = []
            for ch in prev_charts:
                ch_sql   = re.sub(r"INTERVAL\s+'(\d+)\s+days?'", f"INTERVAL '{new_days} days'", ch.get("sql", ""))
                ch_title = re.sub(r'\(last \d+d\)', f'(last {new_days}d)', ch.get("title", ""))
                new_charts.append({**ch, "sql": ch_sql, "title": ch_title})
            return {
                "multi_chart": True, "charts": new_charts,
                "sql": new_charts[0]["sql"],
                "query_type": "multi_chart", "chart_type": "multi_chart",
                "title": " & ".join(c["title"] for c in new_charts)[:80],
                "x_axis": "date", "y_axes": ["value"],
                "response": f"Updated to last {new_days} days.",
            }
        return {
            "sql": updated_sql,
            "query_type": prev_chart, "chart_type": prev_chart,
            "title": updated_title, "x_axis": prev_x, "y_axes": prev_y,
            "response": f"Updated to show last {new_days} days.",
        }

    # Y-axis scale change
    if any(w in p for w in ['scale', 'zoom', 'resize', 'bigger', 'smaller',
                             'enlarge', 'expand', 'y axis', 'y-axis', 'range',
                             'difference', 'interval', 'step', 'tick']):
        scale_num = None
        sm = re.search(r'(?:to|of|at|interval|difference|step|every)\s*(\d+)', p)
        if sm:
            scale_num = int(sm.group(1))
        elif re.search(r'(\d+)\s*(?:difference|interval|step|tick|unit|gap)', p):
            scale_num = int(re.search(r'(\d+)\s*(?:difference|interval|step|tick|unit|gap)', p).group(1))

        cfg = dict(prev_cfg)
        if scale_num:
            cfg["y_tick_interval"] = scale_num
            resp_msg = f"Changed Y-axis scale to intervals of {scale_num}."
        else:
            cfg.pop("y_tick_interval", None)
            resp_msg = "Re-rendered chart with auto-scaled axes."

        if prev_charts:
            for ch in prev_charts:
                ch_cfg = dict(ch.get("chart_config", {}) or {})
                if scale_num:
                    ch_cfg["y_tick_interval"] = scale_num
                ch["chart_config"] = ch_cfg
            return {
                "multi_chart": True, "charts": prev_charts,
                "sql": prev_charts[0].get("sql", ""),
                "query_type": "multi_chart", "chart_type": "multi_chart",
                "title": prev_title, "x_axis": "date", "y_axes": ["value"],
                "chart_config": cfg, "response": resp_msg,
            }
        return {
            "sql": prev_sql,
            "query_type": prev_chart, "chart_type": prev_chart,
            "title": prev_title, "x_axis": prev_x, "y_axes": prev_y,
            "chart_config": cfg, "response": resp_msg,
        }

    # Add/overlay a KPI
    if any(w in p for w in ['add', 'include', 'also show', 'overlay', 'combine']):
        add_kpi = _detect_kpi(p)
        if add_kpi and prev_sites and prev_kpi_names:
            kpi_name, alias = add_kpi
            site = prev_sites[0]
            days = prev_days or 14
            date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE"
            prev_alias = prev_y[0] if prev_y else 'value'
            prev_kpi   = prev_kpi_names[0]
            new_sql = f"""SELECT k.date::text AS date, k.site_id,
                       AVG(k.value) AS value, '{prev_kpi}' AS kpi_name
                FROM kpi_data k
                WHERE k.kpi_name = '{prev_kpi}' AND k.site_id = '{site}'
                  AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date, k.site_id
            UNION ALL
            SELECT k.date::text AS date, k.site_id,
                       AVG(k.value) AS value, '{kpi_name}' AS kpi_name
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                  AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date, k.site_id
            ORDER BY date"""
            return {
                "sql": new_sql,
                "query_type": "composed", "chart_type": "composed",
                "title": f"{site} — {prev_alias} & {alias} (last {days}d)",
                "x_axis": "date", "y_axes": ["value"],
                "response": f"Added {kpi_name} alongside {prev_kpi} for {site}.",
            }

    # Default: re-show previous result
    if prev_charts:
        return {
            "multi_chart": True, "charts": prev_charts,
            "sql": prev_charts[0].get("sql", ""),
            "query_type": "multi_chart", "chart_type": "multi_chart",
            "title": prev_title, "x_axis": "date", "y_axes": ["value"],
            "response": prev_response or "Here are the previous results.",
        }
    return {
        "sql": prev_sql,
        "query_type": prev_chart, "chart_type": prev_chart,
        "title": prev_title, "x_axis": prev_x, "y_axes": prev_y,
        "response": prev_response or "Here are the previous results.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rule-based NL → SQL fallback engine
# ─────────────────────────────────────────────────────────────────────────────
def _rule_based_query(prompt: str, time_filter: str = '1=1', prev_context: dict = None) -> dict:
    import re
    p = prompt.lower()

    if prev_context and _is_followup(p):
        result = _handle_followup(prompt, p, prev_context, time_filter)
        if result:
            return result

    site_ids = re.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', prompt)
    site_ids = list(dict.fromkeys(site_ids))

    KPI_MAP = {
        'cssr':            ('LTE Call Setup Success Rate', 'cssr'),
        'call setup':      ('LTE Call Setup Success Rate', 'cssr'),
        'call success':    ('LTE Call Setup Success Rate', 'cssr'),
        'rrc':             ('LTE RRC Setup Success Rate', 'rrc_sr'),
        'accessibility':   ('LTE RRC Setup Success Rate', 'rrc_sr'),
        'erab':            ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'e-rab':           ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'e rab':           ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'drop rate':       ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'call drop':       ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'cdr':             ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'call failure':    ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'throughput':      ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'tput':            ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'dl throughput':   ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'speed':           ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'download speed':  ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'mbps':            ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'prb':             ('DL PRB Utilization (1BH)', 'dl_prb'),
        'congestion':      ('DL PRB Utilization (1BH)', 'dl_prb'),
        'congested':       ('DL PRB Utilization (1BH)', 'dl_prb'),
        'utilization':     ('DL PRB Utilization (1BH)', 'dl_prb'),
        'overloaded':      ('DL PRB Utilization (1BH)', 'dl_prb'),
        'availability':    ('Availability', 'availability'),
        'uptime':          ('Availability', 'availability'),
        'downtime':        ('Availability', 'availability'),
        'latency':         ('Average Latency Downlink', 'latency'),
        'delay':           ('Average Latency Downlink', 'latency'),
        'ping':            ('Average Latency Downlink', 'latency'),
        'volte':           ('VoLTE Traffic Erlang', 'volte_erl'),
        'voice traffic':   ('VoLTE Traffic Erlang', 'volte_erl'),
        'handover':        ('LTE Intra-Freq HO Success Rate', 'ho_sr'),
        'ho success':      ('LTE Intra-Freq HO Success Rate', 'ho_sr'),
        'volume':          ('DL Data Total Volume', 'dl_volume'),
        'data volume':     ('DL Data Total Volume', 'dl_volume'),
        'traffic volume':  ('DL Data Total Volume', 'dl_volume'),
        'connected':       ('Ave RRC Connected Ue', 'avg_rrc_ue'),
        'connected users': ('Ave RRC Connected Ue', 'avg_rrc_ue'),
        'active users':    ('Ave RRC Connected Ue', 'avg_rrc_ue'),
        'noise':           ('Average NI of Carrier-', 'noise_interference'),
        'interference':    ('Average NI of Carrier-', 'noise_interference'),
    }

    def _detect_kpis(text):
        found = []
        seen = set()
        t = text.lower()
        for kw in sorted(KPI_MAP.keys(), key=len, reverse=True):
            if kw in t and KPI_MAP[kw][0] not in seen:
                found.append(KPI_MAP[kw])
                seen.add(KPI_MAP[kw][0])
        return found

    # ── CHANGE: use unified time parser to handle "a week", "a month", etc. ──
    def _extract_days(text):
        return _parse_time_to_days(text)

    # _try_split_compound REMOVED — caused incorrect KPI-site pairing on "and" keyword

    try:
        cnt = _sql("SELECT COUNT(*) AS n FROM kpi_data")
        USE_KD = int((cnt[0].get("n") or 0) if cnt else 0) > 0
    except Exception:
        USE_KD = False

    if not USE_KD:
        return _rule_based_legacy(p, time_filter)

    GEO_JOIN = "LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)"

    detected_kpis = _detect_kpis(p)
    days          = _extract_days(p)
    # ── CHANGE: also detect "week", "month", "year" as trend signals ──
    is_trend      = (bool(days) or 'trend' in p or 'over time' in p or
                     'history' in p or 'daily' in p or 'last' in p or
                     'week' in p or 'month' in p or 'year' in p)

    # ── Non-kpi_data table intercepts (revenue, core, transport, tickets) ──
    # These tables have different schemas from kpi_data, so rule-based is more reliable.

    _REVENUE_WORDS = {'revenue', 'income', 'earning', 'opex', 'expenditure', 'operating cost',
                      'subscriber', 'subscribers', 'customer count', 'arpu'}
    _CORE_WORDS = {'core kpi', 'core network', 'authentication', 'auth success', 'cpu utilization',
                   'attach success', 'attach rate', 'pdp bearer', 'pdp setup',
                   'auth sr', 'attach sr', 'pdp sr', 'cpu util'}
    _TRANSPORT_WORDS = {'transport', 'backhaul', 'microwave', 'fiber', 'jitter',
                        'link capacity', 'link utilization', 'backhaul latency',
                        'packet loss', 'tput efficiency', 'error rate'}
    _TICKET_WORDS = {'network issue', 'worst cell', 'ai ticket', 'network ticket',
                     'open issue', 'open ticket', 'issue ticket', 'fault'}

    _is_revenue   = any(w in p for w in _REVENUE_WORDS)
    _is_core      = any(w in p for w in _CORE_WORDS)
    _is_transport = any(w in p for w in _TRANSPORT_WORDS)
    _is_ticket    = any(w in p for w in _TICKET_WORDS)

    if _is_revenue or _is_core or _is_transport or _is_ticket:
        site_filter_f = ""
        site_filter_r = ""
        site_filter_t = ""
        site_filter_n = ""
        if site_ids:
            in_clause = ", ".join(f"'{s}'" for s in site_ids[:4])
            site_filter_f = f"AND f.site_id IN ({in_clause})"
            site_filter_r = f"AND r.site_id IN ({in_clause})"
            site_filter_t = f"AND t.site_id IN ({in_clause})"
            site_filter_n = f"AND n.site_id IN ({in_clause})"

        # Check which optional tables actually exist
        def _tbl_exists(name):
            try:
                rows = _sql(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = :tbl",
                    {"tbl": name},
                )
                return len(rows) > 0
            except Exception:
                return False

        # ── Revenue queries (revenue_data → flexible_kpi_uploads fallback) ──
        if _is_revenue:
            _has_rev_tbl = _tbl_exists("revenue_data")

            # Extract "top N" / "bottom N" from prompt
            _top_m = re.search(r'(?:top|best|highest)\s+(\d+)', p)
            _bot_m = re.search(r'(?:bottom|worst|lowest|least)\s+(\d+)', p)
            _rev_n = 10  # default
            _rev_order = "DESC"
            if _top_m:
                _rev_n = min(int(_top_m.group(1)), 100)
                _rev_order = "DESC"
            elif _bot_m:
                _rev_n = min(int(_bot_m.group(1)), 100)
                _rev_order = "ASC"
            elif any(w in p for w in ('worst', 'low', 'bottom', 'least')):
                _rev_order = "ASC"
            _limit = f"LIMIT {_rev_n}"

            if not _has_rev_tbl:
                # Fallback: use flexible_kpi_uploads EAV table
                if any(w in p for w in ('subscriber', 'subscribers', 'customer count')):
                    return {
                        "sql": f"""SELECT f.site_id, f.num_value AS subscribers
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'revenue' AND f.column_name = 'subscribers'
                              AND f.num_value IS NOT NULL {site_filter_f}
                            ORDER BY f.num_value {_rev_order} {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — Subscribers",
                        "x_axis": "site_id", "y_axes": ["subscribers"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by subscriber count.",
                    }
                elif any(w in p for w in ('opex', 'expenditure', 'operating cost')):
                    return {
                        "sql": f"""SELECT f.site_id, SUM(f.num_value) AS total_opex
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'revenue' AND f.column_name LIKE 'opex\\_%'
                              AND f.column_type = 'numeric' {site_filter_f}
                            GROUP BY f.site_id
                            ORDER BY total_opex {_rev_order} {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — OPEX",
                        "x_axis": "site_id", "y_axes": ["total_opex"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Highest'} {_rev_n} sites by total OPEX.",
                    }
                elif 'arpu' in p:
                    return {
                        "sql": f"""SELECT rev.site_id,
                                   ROUND(CAST(rev.total_rev AS NUMERIC) / NULLIF(sub.subscribers, 0), 2) AS arpu
                            FROM (
                                SELECT f.site_id, SUM(f.num_value) AS total_rev
                                FROM flexible_kpi_uploads f
                                WHERE f.kpi_type = 'revenue' AND f.column_name LIKE 'revenue\\_%'
                                  AND f.column_type = 'numeric' {site_filter_f}
                                GROUP BY f.site_id
                            ) rev
                            JOIN (
                                SELECT f.site_id, f.num_value AS subscribers
                                FROM flexible_kpi_uploads f
                                WHERE f.kpi_type = 'revenue' AND f.column_name = 'subscribers'
                                  AND f.num_value > 0
                            ) sub ON rev.site_id = sub.site_id
                            ORDER BY arpu {_rev_order} {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — ARPU",
                        "x_axis": "site_id", "y_axes": ["arpu"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by ARPU.",
                    }
                else:
                    # Default: aggregate total revenue per site
                    return {
                        "sql": f"""SELECT f.site_id, SUM(f.num_value) AS total_revenue
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'revenue' AND f.column_name LIKE 'revenue\\_%'
                              AND f.column_type = 'numeric' {site_filter_f}
                            GROUP BY f.site_id
                            ORDER BY total_revenue {_rev_order} NULLS LAST {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} Sites by Revenue",
                        "x_axis": "site_id", "y_axes": ["total_revenue"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by total revenue.",
                    }

            # revenue_data table exists — use flat table queries
            if any(w in p for w in ('subscriber', 'subscribers', 'customer count')):
                return {
                    "sql": f"""SELECT r.site_id, r.subscribers
                        FROM revenue_data r
                        WHERE r.subscribers IS NOT NULL {site_filter_r}
                        ORDER BY r.subscribers {_rev_order} {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — Subscribers",
                    "x_axis": "site_id", "y_axes": ["subscribers"],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by subscriber count.",
                }
            elif any(w in p for w in ('opex', 'expenditure', 'operating cost')):
                return {
                    "sql": f"""SELECT r.site_id, r.opex_jan, r.opex_feb, r.opex_mar,
                               (COALESCE(r.opex_jan,0)+COALESCE(r.opex_feb,0)+COALESCE(r.opex_mar,0)) AS total_opex
                        FROM revenue_data r
                        WHERE r.site_id IS NOT NULL {site_filter_r}
                        ORDER BY total_opex {_rev_order} {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — OPEX",
                    "x_axis": "site_id", "y_axes": ["opex_jan", "opex_feb", "opex_mar"],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Highest'} {_rev_n} sites by total OPEX.",
                }
            elif 'arpu' in p:
                return {
                    "sql": f"""SELECT r.site_id,
                               ROUND(CAST((COALESCE(r.rev_jan,0) + COALESCE(r.rev_feb,0) + COALESCE(r.rev_mar,0))
                                   AS NUMERIC) / NULLIF(r.subscribers, 0), 2) AS arpu
                        FROM revenue_data r
                        WHERE r.subscribers > 0 {site_filter_r}
                        ORDER BY arpu {_rev_order} {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — ARPU",
                    "x_axis": "site_id", "y_axes": ["arpu"],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by ARPU.",
                }
            elif site_ids and is_trend:
                # Revenue trend — unpivot monthly columns into time series
                site_filter_lat = " OR ".join(f"r.site_id = '{s}'" for s in site_ids[:5])
                return {
                    "sql": f"""SELECT r.site_id, t.month_name, t.revenue, t.opex
                        FROM revenue_data r
                        CROSS JOIN LATERAL (VALUES
                            ('Jan', 1, r.rev_jan, r.opex_jan),
                            ('Feb', 2, r.rev_feb, r.opex_feb),
                            ('Mar', 3, r.rev_mar, r.opex_mar)
                        ) AS t(month_name, month_ord, revenue, opex)
                        WHERE ({site_filter_lat})
                        ORDER BY r.site_id, t.month_ord""",
                    "query_type": "composed", "chart_type": "composed",
                    "title": f"Revenue Trend — {', '.join(site_ids[:3])}",
                    "x_axis": "month_name", "y_axes": ["revenue", "opex"],
                    "response": f"Monthly revenue & OPEX for {', '.join(site_ids[:3])}. Revenue data is monthly (Jan-Mar).",
                }
            else:
                return {
                    "sql": f"""SELECT r.site_id, r.subscribers,
                               r.rev_jan, r.rev_feb, r.rev_mar,
                               (COALESCE(r.rev_jan,0) + COALESCE(r.rev_feb,0) + COALESCE(r.rev_mar,0)) AS total_revenue,
                               r.zone, r.technology
                        FROM revenue_data r
                        WHERE r.site_id IS NOT NULL {site_filter_r}
                        ORDER BY total_revenue {_rev_order} NULLS LAST {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} Sites by Revenue",
                    "x_axis": "site_id", "y_axes": ["total_revenue"],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by total revenue.",
                }

        # ── Shared: extract top/bottom N for non-revenue handlers too ──
        _top_m2 = re.search(r'(?:top|best|highest)\s+(\d+)', p)
        _bot_m2 = re.search(r'(?:bottom|worst|lowest|least)\s+(\d+)', p)
        _hn = 10
        _horder = "DESC"
        if _top_m2:
            _hn = min(int(_top_m2.group(1)), 100)
        elif _bot_m2:
            _hn = min(int(_bot_m2.group(1)), 100)
            _horder = "ASC"
        elif any(w in p for w in ('worst', 'low', 'bottom', 'least')):
            _horder = "ASC"
        _hlimit = f"LIMIT {_hn}"

        # ── Core KPI queries (core_kpi_data → flexible_kpi_uploads fallback) ──
        if _is_core:
            if _tbl_exists("core_kpi_data"):
                date_clause = ""
                if days:
                    date_clause = f"AND c.date >= CURRENT_DATE - INTERVAL '{days} days' AND c.date <= CURRENT_DATE"
                if site_ids:
                    return {
                        "sql": f"""SELECT c.date::text AS date, c.auth_sr, c.cpu_util, c.attach_sr, c.pdp_sr
                            FROM core_kpi_data c
                            WHERE c.site_id = '{site_ids[0]}' {date_clause}
                            ORDER BY c.date""",
                        "query_type": "composed", "chart_type": "composed",
                        "title": f"Core KPIs — {site_ids[0]}",
                        "x_axis": "date", "y_axes": ["auth_sr", "cpu_util", "attach_sr", "pdp_sr"],
                        "response": f"Showing core network KPIs for {site_ids[0]}.",
                    }
                else:
                    _core_sort = "auth_sr ASC" if _horder == "ASC" else "auth_sr DESC"
                    return {
                        "sql": f"""SELECT c.site_id, AVG(c.auth_sr) AS auth_sr, AVG(c.cpu_util) AS cpu_util,
                                   AVG(c.attach_sr) AS attach_sr, AVG(c.pdp_sr) AS pdp_sr
                            FROM core_kpi_data c
                            WHERE c.site_id IS NOT NULL {date_clause}
                            GROUP BY c.site_id ORDER BY {_core_sort} NULLS LAST {_hlimit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _horder=='ASC' else 'Top'} {_hn} — Core KPIs",
                        "x_axis": "site_id", "y_axes": ["auth_sr", "cpu_util", "attach_sr", "pdp_sr"],
                        "response": f"{'Worst' if _horder=='ASC' else 'Top'} {_hn} sites by core KPIs.",
                    }
            else:
                # Fallback to flexible_kpi_uploads EAV
                return {
                    "sql": f"""SELECT f.site_id, f.column_name, f.num_value
                        FROM flexible_kpi_uploads f
                        WHERE f.kpi_type = 'core' AND f.column_type = 'numeric'
                          {site_filter_f}
                        ORDER BY f.site_id, f.column_name""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": "Core KPIs" + (f" — {', '.join(site_ids[:2])}" if site_ids else ""),
                    "x_axis": "site_id", "y_axes": ["num_value"],
                    "response": "Showing core network KPIs (from uploaded data).",
                }

        # ── Transport/backhaul queries ──
        if _is_transport:
            if _tbl_exists("transport_kpi_data"):
                _trans_sort = "packet_loss" if any(w in p for w in ('packet loss', 'loss')) else \
                              "avg_latency" if any(w in p for w in ('latency', 'delay')) else \
                              "jitter" if 'jitter' in p else "packet_loss"
                if site_ids:
                    return {
                        "sql": f"""SELECT t.site_id, t.backhaul_type, t.link_capacity, t.avg_util,
                                   t.peak_util, t.packet_loss, t.avg_latency, t.jitter, t.availability
                            FROM transport_kpi_data t
                            WHERE t.site_id IS NOT NULL {site_filter_t}
                            ORDER BY t.site_id""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Transport KPIs — {', '.join(site_ids[:2])}",
                        "x_axis": "site_id", "y_axes": ["avg_util", "packet_loss", "avg_latency", "jitter"],
                        "response": f"Transport/backhaul KPIs for {', '.join(site_ids[:2])}.",
                    }
                else:
                    return {
                        "sql": f"""SELECT t.site_id, t.backhaul_type, t.link_capacity, t.avg_util,
                                   t.peak_util, t.packet_loss, t.avg_latency, t.jitter, t.availability
                            FROM transport_kpi_data t
                            WHERE t.site_id IS NOT NULL
                            ORDER BY {_trans_sort} DESC NULLS LAST {_hlimit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Top {_hn} — Transport Issues (by {_trans_sort})",
                        "x_axis": "site_id", "y_axes": ["packet_loss", "avg_latency", "jitter"],
                        "response": f"Top {_hn} sites with worst transport KPIs (by {_trans_sort}).",
                    }
            else:
                return {
                    "sql": "", "query_type": "bar", "chart_type": "bar",
                    "title": "Transport KPIs", "x_axis": "site_id", "y_axes": [],
                    "response": "No transport data available. Please upload transport KPI data via the admin panel.",
                }

        # ── Network issue ticket queries ──
        if _is_ticket:
            if _tbl_exists("network_issue_tickets"):
                if site_ids:
                    return {
                        "sql": f"""SELECT n.site_id, n.priority, n.avg_drop_rate, n.avg_cssr, n.avg_tput,
                                   n.violations, n.status, n.zone, n.created_at::text
                            FROM network_issue_tickets n
                            WHERE n.status IN ('open','in_progress') {site_filter_n}
                            ORDER BY n.priority_score DESC""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Network Tickets — {', '.join(site_ids[:2])}",
                        "x_axis": "site_id", "y_axes": ["avg_drop_rate", "avg_cssr", "avg_tput"],
                        "response": f"Network issue tickets for {', '.join(site_ids[:2])}.",
                    }
                else:
                    return {
                        "sql": f"""SELECT n.site_id, n.priority, n.avg_drop_rate, n.avg_cssr, n.avg_tput,
                                   n.violations, n.status, n.zone, n.created_at::text
                            FROM network_issue_tickets n
                            WHERE n.status IN ('open','in_progress')
                            ORDER BY n.priority_score DESC {_hlimit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Top {_hn} Network Issue Tickets",
                        "x_axis": "site_id", "y_axes": ["avg_drop_rate", "avg_cssr", "avg_tput"],
                        "response": f"Showing {_hn} highest-priority open tickets.",
                    }
            else:
                return {
                    "sql": "", "query_type": "bar", "chart_type": "bar",
                    "title": "Network Tickets", "x_axis": "site_id", "y_axes": [],
                    "response": "No network issue tickets found. The AI ticket system generates tickets during the daily 08:00 IST scan.",
                }

    # ── FIX: Multiple sites + trend → multi_chart (one chart per site, each with ALL KPIs) ──
    # Previously this incorrectly paired kpi[i] with site[i].
    if len(site_ids) >= 2 and is_trend:
        date_clause = (
            f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE"
            if days else "AND k.date <= CURRENT_DATE"
        )
        charts = []
        for site in site_ids[:4]:
            if not detected_kpis:
                # Default KPIs when none specified
                use_kpis = [
                    ('E-RAB Call Drop Rate_1', 'drop_rate'),
                    ('LTE Call Setup Success Rate', 'cssr'),
                ]
            else:
                use_kpis = detected_kpis[:3]

            if len(use_kpis) == 1:
                kpi_name, alias = use_kpis[0]
                charts.append({
                    "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {alias}
                        FROM kpi_data k
                        WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                          AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                        GROUP BY k.date ORDER BY k.date""",
                    "chart_type": "line",
                    "title": f"{kpi_name} — {site}" + (f" (last {days}d)" if days else ""),
                    "x_axis": "date",
                    "y_axes": [alias],
                })
            else:
                # Multiple KPIs per site → composed chart using UNION ALL
                parts_sql = []
                for kpi_name, alias in use_kpis:
                    parts_sql.append(
                        f"""SELECT k.date::text AS date, k.site_id,
                               AVG(k.value) AS value, '{kpi_name}' AS kpi_name
                        FROM kpi_data k
                        WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                          AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                        GROUP BY k.date, k.site_id"""
                    )
                kpi_labels = " & ".join(
                    kpi[0].replace("LTE ", "").replace("E-RAB ", "")[:18]
                    for kpi in use_kpis
                )
                charts.append({
                    "sql": "\nUNION ALL\n".join(parts_sql) + "\nORDER BY date",
                    "chart_type": "composed",
                    "title": f"{site} — {kpi_labels}" + (f" (last {days}d)" if days else ""),
                    "x_axis": "date",
                    "y_axes": ["value"],
                })

        chart_labels = [c["title"] for c in charts]
        return {
            "multi_chart": True,
            "charts":      charts,
            "sql":         charts[0]["sql"],
            "query_type":  "multi_chart",
            "chart_type":  "multi_chart",
            "title":       " & ".join(chart_labels)[:80],
            "x_axis":      "date",
            "y_axes":      ["value"],
            "response": (
                f"Here are {len(charts)} charts as requested: {', '.join(chart_labels)}"
                + (f" (last {days}d)." if days else ".")
            ),
        }

    # compound split removed

    # ── Single site, single KPI ───────────────────────────────────────────────
    if site_ids and detected_kpis and is_trend:
        if len(site_ids) == 1 and len(detected_kpis) == 1:
            kpi_name, alias = detected_kpis[0]
            site        = site_ids[0]
            date_clause = (
                f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE"
                if days else "AND k.date <= CURRENT_DATE"
            )
            return {
                "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {alias}
                    FROM kpi_data k
                    WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                      AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                    GROUP BY k.date ORDER BY k.date""",
                "query_type": "line", "chart_type": "line",
                "title": f"{kpi_name} — {site}" + (f" (last {days}d)" if days else ""),
                "x_axis": "date", "y_axes": [alias],
                "response": f"Trend of {kpi_name} for site {site}" + (f" over last {days} days." if days else "."),
            }

        # Single site, multiple KPIs
        if len(site_ids) == 1 and len(detected_kpis) >= 2:
            site        = site_ids[0]
            date_clause = (
                f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE"
                if days else "AND k.date <= CURRENT_DATE"
            )
            parts_sql = []
            for kpi_name, alias in detected_kpis[:3]:
                parts_sql.append(f"""SELECT k.date::text AS date, k.site_id,
                       AVG(k.value) AS value, '{kpi_name}' AS kpi_name
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                  AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date, k.site_id""")
            return {
                "sql": "\nUNION ALL\n".join(parts_sql) + "\nORDER BY date",
                "query_type": "composed", "chart_type": "composed",
                "title": f"{site} — " + " & ".join(k[1] for k in detected_kpis[:3]),
                "x_axis": "date", "y_axes": ["value"],
                "response": f"Showing {', '.join(k[0] for k in detected_kpis[:3])} for {site}.",
            }

    if site_ids and detected_kpis and not is_trend:
        site  = site_ids[0]
        cases = ", ".join(f"AVG(CASE WHEN k.kpi_name = '{kn}' THEN k.value END) AS {al}" for kn, al in detected_kpis[:5])
        in_cl = ", ".join(f"'{kn}'" for kn, _ in detected_kpis[:5])
        return {
            "sql": f"""SELECT k.site_id, {cases}
                FROM kpi_data k
                WHERE k.site_id = '{site}' AND k.data_level = 'site' AND k.value IS NOT NULL
                  AND k.kpi_name IN ({in_cl})
                GROUP BY k.site_id""",
            "query_type": "bar", "chart_type": "bar",
            "title": f"KPIs for {site}",
            "x_axis": "site_id", "y_axes": [al for _, al in detected_kpis[:5]],
            "response": f"Showing requested KPIs for site {site}.",
        }

    if is_trend and detected_kpis:
        kpi_name, alias = detected_kpis[0]
        date_clause = (
            f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE"
            if days else "AND k.date <= CURRENT_DATE"
        )
        return {
            "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {alias},
                       MIN(k.value) AS min_val, MAX(k.value) AS max_val
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi_name}' AND k.data_level = 'site'
                  AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date ORDER BY k.date LIMIT 60""",
            "query_type": "line", "chart_type": "line",
            "title": f"{kpi_name} Daily Trend",
            "x_axis": "date", "y_axes": [alias, "min_val", "max_val"],
            "response": f"Daily trend of {kpi_name} across the network.",
        }

    N    = 10
    nums = re.findall(r'\b(\d+)\b', p)
    if nums:
        N = min(int(nums[0]), 100)

    def _kd_site_query(kpi_names_and_aliases, order_col, order_dir="DESC"):
        case_parts      = []
        kpi_names_for_in = []
        for kpi_name, alias in kpi_names_and_aliases:
            case_parts.append(f"AVG(CASE WHEN k.kpi_name = '{kpi_name}' THEN k.value END) AS {alias}")
            kpi_names_for_in.append(f"'{kpi_name}'")
        cases     = ",\n                   ".join(case_parts)
        in_clause = ", ".join(kpi_names_for_in)
        return f"""SELECT k.site_id, MAX(ts.zone) AS cluster,
                       AVG(ts.latitude) AS lat, AVG(ts.longitude) AS lng,
                       {cases}
                FROM kpi_data k {GEO_JOIN}
                WHERE k.data_level = 'site' AND k.value IS NOT NULL
                  AND k.kpi_name IN ({in_clause})
                GROUP BY k.site_id
                ORDER BY {order_col} {order_dir} NULLS LAST LIMIT {N}"""

    if 'rrc' in p or 'accessibility' in p:
        sql = _kd_site_query([("LTE RRC Setup Success Rate","lte_rrc_setup_sr"),("LTE E-RAB Setup Success Rate","erab_setup_sr"),("LTE Call Setup Success Rate","lte_call_setup_sr"),("DL PRB Utilization (1BH)","dl_prb_util")],"lte_rrc_setup_sr","ASC")
        return {"sql":sql,"query_type":"bar","title":f"RRC / Accessibility — Bottom {N}","x_axis":"site_id","y_axes":["lte_rrc_setup_sr","erab_setup_sr"],"response":f"Showing {N} sites with lowest RRC Setup Success Rate."}

    if 'volte' in p:
        sql = _kd_site_query([("VoLTE Traffic Erlang","volte_traffic_erl"),("VoLTE Traffic DL","volte_dl"),("VoLTE Traffic UL","volte_ul")],"volte_traffic_erl","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} Sites — VoLTE Traffic","x_axis":"site_id","y_axes":["volte_traffic_erl"],"response":f"Showing {N} sites by VoLTE Erlang traffic."}

    if 'handover' in p or ' ho ' in p or 'hsr' in p:
        sql = _kd_site_query([("LTE Intra-Freq HO Success Rate","intra_freq_ho_sr"),("LTE RRC Setup Success Rate","lte_rrc_setup_sr"),("DL PRB Utilization (1BH)","dl_prb_util")],"intra_freq_ho_sr","ASC")
        return {"sql":sql,"query_type":"bar","title":f"Bottom {N} — HO Success Rate","x_axis":"site_id","y_axes":["intra_freq_ho_sr"],"response":f"Showing {N} sites with worst Handover Success Rate."}

    if 'drop rate' in p or 'call drop' in p or 'call failure' in p or 'cdr' in p:
        sql = _kd_site_query([("E-RAB Call Drop Rate_1","erab_drop_rate"),("DL PRB Utilization (1BH)","dl_prb_util"),("LTE DL - Cell Ave Throughput","dl_cell_tput")],"erab_drop_rate","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} Call Drop Offenders","x_axis":"site_id","y_axes":["erab_drop_rate","dl_prb_util"],"response":f"Showing {N} sites with highest E-RAB call drop rate."}

    if 'prb' in p or 'congestion' in p or 'congested' in p or 'overload' in p:
        sql = _kd_site_query([("DL PRB Utilization (1BH)","dl_prb_util"),("UL PRB Utilization (1BH)","ul_prb_util"),("LTE DL - Cell Ave Throughput","dl_cell_tput"),("Ave RRC Connected Ue","avg_rrc_ue")],"dl_prb_util","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} Congested Sites (PRB)","x_axis":"site_id","y_axes":["dl_prb_util","ul_prb_util","dl_cell_tput"],"response":f"Showing top {N} sites by DL PRB Utilization."}

    if 'throughput' in p or 'tput' in p or 'speed' in p or 'mbps' in p:
        order = "ASC" if any(w in p for w in ['worst','low','bad','poor']) else "DESC"
        sql = _kd_site_query([("LTE DL - Cell Ave Throughput","dl_cell_tput"),("LTE UL - Cell Ave Throughput","ul_cell_tput"),("DL PRB Utilization (1BH)","dl_prb_util")],"dl_cell_tput",order)
        return {"sql":sql,"query_type":"bar","title":f"{'Bottom' if order=='ASC' else 'Top'} {N} — DL Throughput","x_axis":"site_id","y_axes":["dl_cell_tput","ul_cell_tput"],"response":f"{'Worst' if order=='ASC' else 'Best'} {N} sites by throughput."}

    if 'cssr' in p or 'call setup' in p or 'setup success' in p:
        sql = _kd_site_query([("LTE Call Setup Success Rate","lte_cssr"),("LTE E-RAB Setup Success Rate","erab_setup_sr"),("E-RAB Call Drop Rate_1","erab_drop_rate")],"lte_cssr","ASC")
        return {"sql":sql,"query_type":"bar","title":f"Bottom {N} — Call Setup Success","x_axis":"site_id","y_axes":["lte_cssr","erab_setup_sr"],"response":f"Showing {N} sites with lowest CSSR."}

    if 'zone' in p or 'cluster' in p or 'cbd' in p or 'urban' in p or 'compare' in p or 'comparison' in p:
        return {"sql":f"""SELECT ts.zone AS cluster, COUNT(DISTINCT k.site_id) AS sites,
                       AVG(CASE WHEN k.kpi_name='DL PRB Utilization (1BH)' THEN k.value END) AS avg_prb,
                       AVG(CASE WHEN k.kpi_name='LTE DL - Cell Ave Throughput' THEN k.value END) AS avg_tput,
                       AVG(CASE WHEN k.kpi_name='E-RAB Call Drop Rate_1' THEN k.value END) AS avg_drop,
                       AVG(CASE WHEN k.kpi_name='LTE RRC Setup Success Rate' THEN k.value END) AS avg_rrc_sr
                FROM kpi_data k {GEO_JOIN}
                WHERE k.data_level='site' AND k.value IS NOT NULL AND ts.zone IS NOT NULL
                  AND k.kpi_name IN ('DL PRB Utilization (1BH)','LTE DL - Cell Ave Throughput','E-RAB Call Drop Rate_1','LTE RRC Setup Success Rate')
                GROUP BY ts.zone ORDER BY avg_prb DESC NULLS LAST""",
                "query_type":"bar","title":"Zone-wise KPI Comparison","x_axis":"cluster","y_axes":["avg_prb","avg_tput","avg_drop"],"response":"Zone-level KPI comparison."}

    if 'availability' in p or 'downtime' in p or 'uptime' in p:
        sql = _kd_site_query([("Availability","availability"),("DL PRB Utilization (1BH)","dl_prb_util")],"availability","ASC")
        return {"sql":sql,"query_type":"bar","title":"Sites with Lowest Availability","x_axis":"site_id","y_axes":["availability"],"response":"Sites with lowest availability."}

    if 'latency' in p or 'delay' in p or 'ping' in p:
        sql = _kd_site_query([("Average Latency Downlink","avg_latency"),("LTE DL - Usr Ave Throughput","dl_usr_tput")],"avg_latency","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} High Latency Sites","x_axis":"site_id","y_axes":["avg_latency"],"response":f"Showing {N} sites with highest latency."}

    if 'volume' in p or 'data volume' in p:
        sql = _kd_site_query([("DL Data Total Volume","dl_volume"),("UL Data Total Volume","ul_volume"),("DL PRB Utilization (1BH)","dl_prb_util")],"dl_volume","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} by Data Volume","x_axis":"site_id","y_axes":["dl_volume","ul_volume"],"response":f"Showing {N} sites with highest data volume."}

    if 'connected' in p or re.search(r'\busers?\b', p) or re.search(r'\bue\b', p):
        sql = _kd_site_query([("Ave RRC Connected Ue","avg_rrc_ue"),("Max RRC Connected Ue","max_rrc_ue"),("DL PRB Utilization (1BH)","dl_prb_util")],"avg_rrc_ue","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} by Connected Users","x_axis":"site_id","y_axes":["avg_rrc_ue","max_rrc_ue"],"response":f"Showing {N} sites with most users."}

    sql = _kd_site_query([("DL PRB Utilization (1BH)","dl_prb_util"),("LTE DL - Cell Ave Throughput","dl_cell_tput"),("E-RAB Call Drop Rate_1","erab_drop_rate"),("LTE RRC Setup Success Rate","lte_rrc_setup_sr")],"dl_prb_util","DESC")
    return {"sql":sql,"query_type":"bar","title":f"Top {N} Sites by PRB Utilization","x_axis":"site_id","y_axes":["dl_prb_util","dl_cell_tput","erab_drop_rate"],"response":f"Top {N} sites by PRB utilization."}


def _rule_based_legacy(p: str, time_filter: str) -> dict:
    """Legacy fallback using network_kpi_timeseries table."""
    import re
    N    = 10
    nums = re.findall(r'\b(\d+)\b', p)
    if nums:
        N = min(int(nums[0]), 100)
    TBL  = "network_kpi_timeseries"
    base = f"FROM {TBL} WHERE {time_filter}"

    if 'rrc' in p or 'accessibility' in p or 'access' in p:
        return {"sql": f"SELECT site_id, cluster, AVG(lte_rrc_setup_sr) as lte_rrc_setup_sr, AVG(erab_setup_sr) as erab_setup_sr, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY lte_rrc_setup_sr ASC NULLS LAST LIMIT {N}", "query_type":"bar","title":f"RRC — Bottom {N} Sites","x_axis":"site_id","y_axes":["lte_rrc_setup_sr","erab_setup_sr"],"response":f"Bottom {N} sites by RRC SR."}
    if 'volte' in p:
        return {"sql": f"SELECT site_id, cluster, AVG(volte_traffic_erl) as volte_traffic_erl, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY volte_traffic_erl DESC NULLS LAST LIMIT {N}", "query_type":"bar","title":f"VoLTE — Top {N}","x_axis":"site_id","y_axes":["volte_traffic_erl"],"response":f"Top {N} sites by VoLTE Erlang."}
    if 'drop rate' in p or 'call drop' in p or 'call failure' in p or 'cdr' in p:
        return {"sql": f"SELECT site_id, cluster, AVG(COALESCE(erab_drop_rate,call_drop_rate,0)) as erab_drop_rate, AVG(COALESCE(dl_prb_util,prb_utilization)) as avg_prb, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY erab_drop_rate DESC NULLS LAST LIMIT {N}", "query_type":"mixed","title":f"Call Drop — Top {N}","x_axis":"site_id","y_axes":["erab_drop_rate","avg_prb"],"response":f"Top {N} call drop sites."}
    if 'zone' in p or 'compar' in p or 'cluster' in p:
        return {"sql": f"SELECT cluster, COUNT(DISTINCT site_id) as sites, AVG(COALESCE(dl_prb_util,prb_utilization)) as avg_prb, AVG(COALESCE(dl_cell_tput,throughput_dl)) as avg_tput {base} GROUP BY cluster ORDER BY avg_prb DESC", "query_type":"bar","title":"Zone Comparison","x_axis":"cluster","y_axes":["avg_prb","avg_tput"],"response":"Zone KPI comparison."}
    return {"sql": f"SELECT site_id, cluster, AVG(COALESCE(dl_prb_util,prb_utilization)) as avg_prb, AVG(COALESCE(dl_cell_tput,throughput_dl)) as avg_tput, AVG(COALESCE(erab_drop_rate,call_drop_rate,0)) as avg_drop, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY avg_prb DESC NULLS LAST LIMIT {N}", "query_type":"mixed","title":f"Top {N} Sites by PRB","x_axis":"site_id","y_axes":["avg_prb","avg_tput"],"response":f"Top {N} sites by PRB utilization."}


# ─────────────────────────────────────────────────────────────────────────────
# Network AI Session CRUD endpoints
# ─────────────────────────────────────────────────────────────────────────────

@network_ai_bp.route("/api/network/ai-sessions", methods=["GET"])
@jwt_required()
def list_ai_sessions():
    _ensure_ai_session_tables()
    uid = int(get_jwt_identity())
    sessions = (NetworkAiSession.query
                .filter_by(user_id=uid)
                .order_by(NetworkAiSession.last_message_at.desc())
                .limit(50)
                .all())
    return jsonify({"sessions": [s.to_dict() for s in sessions]})


@network_ai_bp.route("/api/network/ai-sessions", methods=["POST"])
@jwt_required()
def create_ai_session():
    _ensure_ai_session_tables()
    uid = int(get_jwt_identity())
    session = NetworkAiSession(user_id=uid, title="New Chat")
    db.session.add(session)
    db.session.commit()
    return jsonify({"session": session.to_dict()}), 201


@network_ai_bp.route("/api/network/ai-sessions/<int:session_id>/messages", methods=["GET"])
@jwt_required()
def get_ai_session_messages(session_id):
    uid = int(get_jwt_identity())
    session = db.session.get(NetworkAiSession, session_id)
    if not session or session.user_id != uid:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "session": session.to_dict(),
        "messages": [m.to_dict() for m in session.messages],
    })


@network_ai_bp.route("/api/network/ai-sessions/<int:session_id>", methods=["PUT"])
@jwt_required()
def update_ai_session(session_id):
    uid = int(get_jwt_identity())
    session = db.session.get(NetworkAiSession, session_id)
    if not session or session.user_id != uid:
        return jsonify({"error": "Not found"}), 404
    body = request.get_json(silent=True) or {}
    if "title" in body:
        session.title = body["title"][:200]
    db.session.commit()
    return jsonify({"session": session.to_dict()})


@network_ai_bp.route("/api/network/ai-sessions/<int:session_id>", methods=["DELETE"])
@jwt_required()
def delete_ai_session(session_id):
    uid = int(get_jwt_identity())
    session = db.session.get(NetworkAiSession, session_id)
    if not session or session.user_id != uid:
        return jsonify({"error": "Not found"}), 404
    NetworkAiMessage.query.filter_by(session_id=session_id).delete()
    db.session.delete(session)
    db.session.commit()
    return jsonify({"success": True})