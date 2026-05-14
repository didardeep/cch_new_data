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
import re
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

# Shared process-level schema metadata cache. app.py warms this on startup and
# upload/delete flows invalidate it after changing KPI data.
_schema_cache = {
    "populated": False,
}


def invalidate_schema_cache():
    """Clear cached AI schema metadata after network data changes."""
    _schema_cache.clear()
    _schema_cache["populated"] = False


def _populate_schema_cache():
    """Discover KPI names and data dates from DB, cache for reuse."""
    if _schema_cache.get("populated"):
        return
    try:
        # Discover KPI names from kpi_data
        rows = _sql("SELECT DISTINCT kpi_name FROM kpi_data WHERE kpi_name IS NOT NULL ORDER BY kpi_name")
        _schema_cache["kpi_names_list"] = [r["kpi_name"] for r in rows] if rows else []
        _schema_cache["kpi_data_has_data"] = bool(rows)

        # Discover max date for date substitution
        date_rows = _sql("SELECT MAX(date)::text AS max_date FROM kpi_data")
        if date_rows and date_rows[0].get("max_date"):
            _schema_cache["kpi_max_date"] = date_rows[0]["max_date"]

        # MV max date
        try:
            mv_rows = _sql("SELECT MAX(date)::text AS max_date FROM mv_daily_site_kpi")
            if mv_rows and mv_rows[0].get("max_date"):
                _schema_cache["mv_max_date"] = mv_rows[0]["max_date"]
                _schema_cache["mv_has_data"] = True
            else:
                _schema_cache["mv_has_data"] = False
        except Exception:
            _schema_cache["mv_has_data"] = False

        # Discover flexible_kpi_uploads data availability and dates
        try:
            flex_rows = _sql("""
                SELECT kpi_type,
                       COUNT(*) AS cnt,
                       MAX(row_date)::text AS max_date,
                       MIN(row_date)::text AS min_date
                FROM flexible_kpi_uploads
                WHERE kpi_type IS NOT NULL
                GROUP BY kpi_type
            """)
            flex_info = {}
            for r in (flex_rows or []):
                flex_info[r["kpi_type"]] = {
                    "count": r["cnt"],
                    "max_date": r.get("max_date"),
                    "min_date": r.get("min_date"),
                }
            _schema_cache["flex_kpi_info"] = flex_info
            _schema_cache["flex_has_revenue"] = "revenue" in flex_info
            _schema_cache["flex_has_core"] = "core" in flex_info
            # Discover actual column names for revenue
            if flex_info.get("revenue"):
                col_rows = _sql("""
                    SELECT DISTINCT column_name FROM flexible_kpi_uploads
                    WHERE kpi_type='revenue' AND column_type='numeric'
                    AND column_name IS NOT NULL
                    ORDER BY column_name LIMIT 30
                """)
                _schema_cache["flex_revenue_columns"] = [r["column_name"] for r in col_rows] if col_rows else []
            # Use flex dates as fallback when kpi_data is empty
            if not _schema_cache.get("kpi_max_date"):
                for kt in ("revenue", "core"):
                    if flex_info.get(kt, {}).get("max_date"):
                        _schema_cache["kpi_max_date"] = flex_info[kt]["max_date"]
                        break
        except Exception:
            _schema_cache["flex_kpi_info"] = {}
            _schema_cache["flex_has_revenue"] = False
            _schema_cache["flex_has_core"] = False

        _schema_cache["populated"] = True
        print(f"[AI-DEBUG] Schema cache populated: {len(_schema_cache.get('kpi_names_list', []))} KPIs, "
              f"max_date={_schema_cache.get('kpi_max_date', 'N/A')}, "
              f"mv_max_date={_schema_cache.get('mv_max_date', 'N/A')}, "
              f"kpi_data_has_data={_schema_cache.get('kpi_data_has_data')}, "
              f"mv_has_data={_schema_cache.get('mv_has_data')}, "
              f"flex_has_revenue={_schema_cache.get('flex_has_revenue')}, "
              f"flex_has_core={_schema_cache.get('flex_has_core')}", flush=True)
        _LOG.info("Schema cache populated: %d KPI names, max_date=%s",
                  len(_schema_cache.get("kpi_names_list", [])),
                  _schema_cache.get("kpi_max_date", "unknown"))
    except Exception as e:
        print(f"[AI-DEBUG] Schema cache FAILED to populate: {str(e)[:200]}", flush=True)
        _LOG.warning("Failed to populate schema cache: %s", e)
        # Mark as populated even on failure to avoid retrying every request
        _schema_cache["populated"] = True


def refresh_materialized_views():
    """Refresh AI-related materialized views, best-effort.

    The merged KPI materialized view is owned by network_analytics; keep this
    wrapper here so app.py can treat Network AI startup/upload hooks uniformly.
    """
    try:
        from network_analytics import refresh_kpi_data_merged
        refresh_kpi_data_merged()
    except Exception as exc:
        _LOG.warning("Network AI materialized view refresh skipped: %s", exc)


def ensure_db_optimizations():
    """Ensure database objects used by Network AI exist.

    This is intentionally best-effort because these indexes/materialized views
    are performance helpers; the app should still start if a deployment user
    lacks DDL permissions or a table has not been created yet.
    """
    _ensure_ai_session_tables()
    try:
        from network_analytics import _ensure_kpi_data_stats_table, _ensure_kpi_indexes
        _ensure_kpi_indexes()
        _ensure_kpi_data_stats_table()
    except Exception as exc:
        _LOG.warning("Network AI DB optimization setup skipped: %s", exc)
    refresh_materialized_views()

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
    _populate_schema_cache()
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
            recent_msgs = (NetworkAiMessage.query
                          .filter_by(session_id=session_id)
                          .order_by(NetworkAiMessage.created_at.desc())
                          .limit(20)
                          .all())
            recent_msgs.reverse()
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

    # ── Build dynamic schema hint with actual DB state ──────────────────────
    _kpi_max = _schema_cache.get("kpi_max_date", "unknown")
    _mv_max  = _schema_cache.get("mv_max_date", _kpi_max)
    _db_kpis = _schema_cache.get("kpi_names_list", [])
    _kpi_data_has_data = _schema_cache.get("kpi_data_has_data", False)
    _mv_has_data = _schema_cache.get("mv_has_data", False)
    _flex_has_revenue = _schema_cache.get("flex_has_revenue", False)
    _flex_has_core = _schema_cache.get("flex_has_core", False)
    _flex_info = _schema_cache.get("flex_kpi_info", {})
    _flex_rev_cols = _schema_cache.get("flex_revenue_columns", [])

    # Build a compact list of actual KPI names from the DB for the LLM
    _db_kpi_block = ""
    if _db_kpis:
        _db_kpi_block = "\n   Actual KPI names discovered in this database:\n"
        for _kn in _db_kpis[:60]:
            _db_kpi_block += f"   '{_kn}'\n"

    # Build data availability section
    _avail_lines = ["\n=== DATA AVAILABILITY (CRITICAL — read before generating SQL) ==="]
    if _kpi_data_has_data:
        _avail_lines.append(f"   kpi_data: HAS DATA (latest date: {_kpi_max})")
    else:
        _avail_lines.append("   kpi_data: EMPTY — do NOT query this table or mv_daily_site_kpi for RAN KPIs.")
    if _mv_has_data:
        _avail_lines.append(f"   mv_daily_site_kpi: HAS DATA (latest date: {_mv_max})")
    else:
        _avail_lines.append("   mv_daily_site_kpi: EMPTY — do NOT query this table.")
    if _flex_has_revenue:
        rev_info = _flex_info.get("revenue", {})
        _avail_lines.append(f"   flexible_kpi_uploads (revenue): HAS DATA (rows: {rev_info.get('count', '?')}, dates: {rev_info.get('min_date', '?')} to {rev_info.get('max_date', '?')})")
        if _flex_rev_cols:
            _avail_lines.append(f"   Revenue column_name values: {_flex_rev_cols[:10]}")
    else:
        _avail_lines.append("   flexible_kpi_uploads (revenue): NO REVENUE DATA")
    if _flex_has_core:
        core_info = _flex_info.get("core", {})
        _avail_lines.append(f"   flexible_kpi_uploads (core): HAS DATA (rows: {core_info.get('count', '?')}, dates: {core_info.get('min_date', '?')} to {core_info.get('max_date', '?')})")
    else:
        _avail_lines.append("   flexible_kpi_uploads (core): NO CORE KPI DATA")

    if not _kpi_data_has_data and not _mv_has_data:
        _avail_lines.append("")
        _avail_lines.append("   ** WARNING: RAN KPI tables are empty. Queries for throughput, call drop rate,")
        _avail_lines.append("   PRB utilization, handover, etc. will return 0 rows. Only revenue and core KPI")
        _avail_lines.append("   queries from flexible_kpi_uploads will work. If user asks for RAN KPIs,")
        _avail_lines.append("   respond with a message explaining that RAN KPI data has not been uploaded yet.")
        _avail_lines.append("   For 'utilization' queries when kpi_data is empty, check if flexible_kpi_uploads")
        _avail_lines.append("   has relevant data (e.g., kpi_type='core' AND column_name ILIKE '%util%').")
    _data_avail_block = "\n".join(_avail_lines)

    SCHEMA_HINT = f"""
Tables:
1. kpi_data(id, site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
   - data_level = 'site' for site-level, 'cell' for cell-level
   - IMPORTANT: kpi_name values are EXACT strings. Use these EXACTLY as listed below.
   - Latest date in kpi_data: {_kpi_max}

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
{_db_kpi_block}
2. mv_daily_site_kpi(site_id, kpi_name, date, avg_value, min_value, max_value, sample_count)
   - PRE-AGGREGATED materialized view of kpi_data grouped by site+kpi+date.
   - MUCH faster than kpi_data for site-level daily queries. PREFER this table.
   - Does NOT have data_level column — do NOT add WHERE data_level=... on this table.
   - Latest date in mv_daily_site_kpi: {_mv_max}
   - Use avg_value (not value) for averages, min_value/max_value for extremes.

3. telecom_sites(site_id, cell_id, latitude, longitude, zone, technology, vendor_name, cluster)
   - JOIN: kpi_data k JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
   - zone column has zone/area names, cluster has cluster names
   - technology: '4G', '5G', etc.

4. flexible_kpi_uploads(id, site_id, kpi_name, kpi_type, column_name, column_type, num_value, str_value, row_date)
   - kpi_type = 'core' for core KPIs (Authentication Success Rate, CPU Utilization, Attach Success Rate, etc.)
   - kpi_type = 'revenue' for revenue data
   - For revenue queries: WHERE kpi_type='revenue' AND column_type='numeric'
   - Revenue value is in num_value column, metric name is in column_name
   - Date column is row_date (NOT date)
   - Example revenue query:
     SELECT site_id, SUM(num_value) AS total_revenue
     FROM flexible_kpi_uploads
     WHERE kpi_type='revenue' AND column_name ILIKE '%revenue%' AND column_type='numeric'
     GROUP BY site_id

5. revenue_data(site_id, total_revenue, date) — alternative revenue table (may not exist)
{_data_avail_block}

=== CROSS-TABLE QUERIES (revenue + KPI data) ===
Revenue is in flexible_kpi_uploads or revenue_data, NEVER in kpi_data.
To combine revenue with KPI data, use CTEs:
  WITH rev AS (
    SELECT site_id, SUM(num_value) AS total_revenue
    FROM flexible_kpi_uploads
    WHERE kpi_type='revenue' AND column_name ILIKE '%revenue%' AND column_type='numeric'
    GROUP BY site_id
  ),
  kpi AS (
    SELECT site_id, AVG(avg_value) AS avg_kpi
    FROM mv_daily_site_kpi
    WHERE kpi_name='DL PRB Utilization (1BH)'
      AND date >= (SELECT MAX(date) FROM mv_daily_site_kpi) - INTERVAL '7 days'
    GROUP BY site_id
  )
  SELECT COALESCE(rev.site_id, kpi.site_id) AS site_id,
         rev.total_revenue, kpi.avg_kpi
  FROM rev FULL JOIN kpi ON LOWER(rev.site_id) = LOWER(kpi.site_id)
  WHERE rev.total_revenue IS NOT NULL AND kpi.avg_kpi IS NOT NULL
  ORDER BY rev.total_revenue DESC NULLS LAST, site_id ASC
  LIMIT 10

=== Natural Language → KPI Mapping Guide ===
User says "call drop" / "drop rate" / "CDR" / "call failure" → 'E-RAB Call Drop Rate_1'
User says "throughput" / "speed" / "download speed" → 'LTE DL - Usr Ave Throughput' (user) or 'LTE DL - Cell Ave Throughput' (cell)
User says "PRB" / "congestion" / "load" / "utilization" → 'DL PRB Utilization (1BH)'
User says "availability" / "uptime" / "downtime" → 'Availability'
User says "connected users" / "RRC users" / "active users" → 'Ave RRC Connected Ue'
User says "handover" / "HO" → 'LTE Intra-Freq HO Success Rate'
User says "VoLTE" / "voice" → 'VoLTE Traffic Erlang'
User says "latency" / "delay" / "ping" → 'Average Latency Downlink'
User says "data volume" / "traffic volume" → 'DL Data Total Volume'
User says "call setup" / "CSSR" → 'LTE Call Setup Success Rate'
User says "RRC" / "accessibility" / "access" → 'LTE RRC Setup Success Rate'
User says "noise" / "interference" → 'Average NI of Carrier-'
User says "revenue" / "income" / "earnings" / "ARPU" → flexible_kpi_uploads WHERE kpi_type='revenue'

=== DATE HANDLING — CRITICAL ===
{"The latest date with data is " + _kpi_max + ". CURRENT_DATE may be far ahead of the actual data." if _kpi_max != "unknown" else "The latest date in kpi_data is UNKNOWN (table may be empty). For flexible_kpi_uploads, use: row_date >= (SELECT MAX(row_date) FROM flexible_kpi_uploads) - INTERVAL '7 days'."}
{("- If user says 'last 7 days', use: date >= '" + _kpi_max + "'::date - INTERVAL '7 days' AND date <= '" + _kpi_max + "'::date") if _kpi_max != "unknown" else "- If user says 'last 7 days' and querying kpi_data/mv_daily_site_kpi, use: date >= (SELECT MAX(date) FROM mv_daily_site_kpi) - INTERVAL '7 days'"}
{("- If user says 'last month', use: date >= '" + _kpi_max + "'::date - INTERVAL '1 month' AND date <= '" + _kpi_max + "'::date") if _kpi_max != "unknown" else "- If user says 'last month' and querying kpi_data/mv_daily_site_kpi, use subquery: date >= (SELECT MAX(date) FROM mv_daily_site_kpi) - INTERVAL '1 month'"}
- If user doesn't specify a time range, default to last 7 days.
- For mv_daily_site_kpi, use the same pattern with date column.
- For flexible_kpi_uploads, use row_date instead of date. Anchor with: row_date >= (SELECT MAX(row_date) FROM flexible_kpi_uploads) - INTERVAL '7 days'
- {"NEVER use CURRENT_DATE — always anchor to '" + _kpi_max + "'::date (the latest actual data date)." if _kpi_max != "unknown" else "Use subqueries like (SELECT MAX(date) FROM table) to anchor date ranges dynamically."}
"""

    LLM_SYSTEM = f"""You are a telecom network analytics SQL generator. Your ONLY job is to convert the user's natural-language query into an EXACT, STRICT SQL query that fetches PRECISELY what was asked — nothing more, nothing less.

READ THE USER QUERY VERY CAREFULLY. Understand EVERY part of it before generating SQL.

The user may write in English, Hindi, Hinglish, or any language. Understand the intent and translate to SQL.

{SCHEMA_HINT}

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

2. SITE SWITCH — "show the same for SITE_A" / "i want to see for site id SITE_A" / "what about site X" / "now show for X"
   → ANY prompt that names a new site ID WITHOUT specifying a new KPI = site switch.
   → Keep the EXACT same KPI(s) and INTERVAL from the previous SQL/charts. Only swap the site_id.
   → "i want to see for site id SITE_A" with no new KPI = show same KPIs for SITE_A.
   → If the previous response was multi_chart (multiple sites), generate a SINGLE composed chart
     for just the new site, preserving ALL the same KPIs and INTERVAL from the previous charts.
   → CRITICAL: NEVER default to PRB or any other KPI when the user only switches the site.

3. TIME RANGE CHANGE — "extend to 30 days" / "show last 10 days instead"
   → Keep everything the same, only change the INTERVAL value in the SQL.

4. CHART TYPE CHANGE — "show as bar chart" / "convert to pie"
   → Re-use the EXACT same SQL, only change chart_type in the response.

5. ADD A KPI — "also show PRB" / "overlay throughput"
   → Extend the previous SQL using UNION ALL or CASE WHEN to include the new KPI.

6. COUNT CORRECTION — "I asked for 5 not 4" / "only 4 sites" / "show me 5" / "need more results"
   → User is unhappy with the number of results. The PREVIOUS SQL logic is correct but LIMIT is wrong.
   → Take the EXACT previous SQL, ONLY change the LIMIT clause to the desired count.
   → Do NOT rewrite the query logic, filters, or ordering — keep everything identical.
   → If the user says "only 4, I asked for 5", change LIMIT 4 → LIMIT 5 (or add LIMIT 5).
   → The sites/results returned must be a SUPERSET of the previous results, not a different set.

7. VAGUE / AMBIGUOUS — "yes", "ok", "show more", short prompts with no new site/KPI
   → Re-run the previous SQL with the same parameters.

**IMPORTANT RULES for follow-ups:**
- ALWAYS inherit site_id from previous SQL if the user doesn't mention a new one.
- ALWAYS inherit the time range (INTERVAL) from previous SQL if not specified.
- ALWAYS inherit kpi_name(s) from previous SQL if the user doesn't mention a new KPI.
- If the current prompt is completely self-contained (has site ID + KPI + time range), treat it as a FRESH query.
- A prompt like "i want to see for site id X" with NO new KPI mentioned = SITE SWITCH → inherit all KPIs.
- CONSISTENCY: When user asks to correct/adjust results (more rows, different LIMIT), MODIFY the previous SQL minimally — do NOT rewrite from scratch. The corrected results must be consistent with (superset of) the original results.

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

Example: "show E-RAB drop rate and CSSR last 18 days for SITE_A and SITE_A"
→ TWO CHARTS: Chart 1 = SITE_A (both KPIs), Chart 2 = SITE_A (both KPIs)
→ Each chart: composed chart_type, UNION ALL SQL filtering by that site.

Example SQL for one site with two KPIs (using mv_daily_site_kpi — preferred):
SELECT mv.date::text AS date, mv.site_id, AVG(mv.avg_value) AS value, 'E-RAB Call Drop Rate_1' AS kpi_name
FROM mv_daily_site_kpi mv WHERE mv.kpi_name = 'E-RAB Call Drop Rate_1' AND mv.site_id = 'SITE_A'
  AND mv.avg_value IS NOT NULL AND mv.date >= '{_kpi_max}'::date - INTERVAL '18 days' AND mv.date <= '{_kpi_max}'::date
GROUP BY mv.date, mv.site_id
UNION ALL
SELECT mv.date::text AS date, mv.site_id, AVG(mv.avg_value) AS value, 'LTE Call Setup Success Rate' AS kpi_name
FROM mv_daily_site_kpi mv WHERE mv.kpi_name = 'LTE Call Setup Success Rate' AND mv.site_id = 'SITE_A'
  AND mv.avg_value IS NOT NULL AND mv.date >= '{_kpi_max}'::date - INTERVAL '18 days' AND mv.date <= '{_kpi_max}'::date
GROUP BY mv.date, mv.site_id
ORDER BY date

**NEVER ignore part of the user's query. If they ask for 2 sites, return charts for BOTH.**

═══════════════════════════════════════════════════════════
STRICTNESS RULES — FOLLOW THESE EXACTLY:
═══════════════════════════════════════════════════════════

1. ONLY query the EXACT KPI(s) the user asked about. Do NOT add extra KPIs.
2. ONLY filter by what the user specified (site, date range, zone).
3. The latest date with actual data is '{_kpi_max}'. NEVER use CURRENT_DATE — always anchor dates to '{_kpi_max}'::date.
   Example: "last 7 days" → date >= '{_kpi_max}'::date - INTERVAL '7 days' AND date <= '{_kpi_max}'::date
4. EXTRACT THE EXACT NUMBER OF DAYS mentioned. "last 18 days" = 18, NOT 7 or 30.
5. KPI names are CASE-SENSITIVE — copy EXACTLY from the list above. If unsure, pick the closest match from the KPI list.
6. Site IDs are EXACT — copy every character. NEVER truncate in SQL (only title has 60-char limit).
7. PREFER mv_daily_site_kpi over kpi_data for site-level daily queries — it's pre-aggregated, much faster.
   Use avg_value (not value). Do NOT add data_level filter on MVs.
   Fall back to kpi_data only if you need hourly data or cell-level data.
8. For kpi_data: add WHERE data_level='site' AND value IS NOT NULL.
9. JOIN telecom_sites only when you need zone/geo/technology/vendor data.
10. For revenue data: query flexible_kpi_uploads with kpi_type='revenue', NOT kpi_data.

═══════════════════════════════════════════════════════════
QUERY OPTIMIZATION — ALWAYS APPLY:
═══════════════════════════════════════════════════════════

1. ALWAYS add LIMIT — max 500 rows. For "top N" queries use the user's N but cap at 500.
2. ALWAYS include a date filter. If user doesn't specify, default to last 7 days.
3. For time-series/trend charts: GROUP BY date (not hour) unless user asks for hourly. This reduces row count.
4. For "top/worst N sites" queries: use a subquery or CTE to first identify the sites, then fetch details. Do NOT scan the entire table.
5. Prefer mv_daily_site_kpi over kpi_data for site-level daily aggregations — it's pre-aggregated and much faster.
6. Use AVG/MAX aggregation instead of returning raw rows when the user asks for "average", "peak", etc.

═══════════════════════════════════════════════════════════
DETERMINISTIC RESULTS — CRITICAL FOR CONSISTENCY:
═══════════════════════════════════════════════════════════

1. EVERY ORDER BY MUST include site_id as a tiebreaker at the end.
   Example: ORDER BY avg_value DESC NULLS LAST, site_id ASC LIMIT 5
   Without the tiebreaker, sites with equal values appear in random order on each run.

2. For "top/worst N" by MULTIPLE metrics (e.g. "high revenue AND high utilization"):
   Compute a combined ranking score so the ordering is unambiguous.
   Example approach — normalize each metric to a 0-1 scale, sum or average:
   ```sql
   ORDER BY (COALESCE(total_revenue,0) / NULLIF(MAX(total_revenue) OVER (), 0)
           + COALESCE(avg_util,0) / NULLIF(MAX(avg_util) OVER (), 0)) DESC,
           site_id ASC
   LIMIT 5
   ```
   Do NOT order by just one of the two metrics when the user asked for both.

3. ALWAYS use DESC NULLS LAST for "top" and ASC NULLS LAST for "worst/bottom" to push NULLs to the end.

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
**Use single composed/bar chart when:**
- Multiple KPIs for the SAME site(s) on the same axis — ALWAYS single chart, NOT multi_chart.
- User says "combine", "one chart", "together", "all in one", "same chart", "combined".
- User says "show all KPIs for each site" — use CASE WHEN pivot, single bar chart.
- "worst N sites where KPI_A > X OR KPI_B < Y" — single bar chart with violations column.
NEVER split multiple KPIs into separate charts when user asks for them together. Use the MULTI-KPI PIVOT pattern.

**CROSS-TABLE QUERIES (revenue + KPI data):**
See the CTE example in the schema section above. Key rules:
- Revenue is in flexible_kpi_uploads (kpi_type='revenue') or revenue_data — NEVER in kpi_data.
- Use CTE with FULL JOIN or INNER JOIN to combine revenue with KPI data.
- INNER JOIN is better when you want sites that have BOTH metrics (e.g., "high revenue AND high utilization").
- FULL JOIN when you want all sites even if one metric is missing.
- For "top N" across multiple metrics, use a combined score in ORDER BY (see DETERMINISTIC RESULTS rules).
- Always add WHERE ... IS NOT NULL after the JOIN to filter out sites missing either metric.

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
                    chart["sql"] = _fix_site_ids_in_sql(chart["sql"])
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
            parsed["sql"] = _fix_site_ids_in_sql(parsed["sql"])
        return parsed

    def _fix_site_ids_in_sql(sql: str) -> str:
        import re
        prompt_site_ids = re.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', prompt)
        if not prompt_site_ids:
            return sql
        for correct_id in prompt_site_ids:
            for trim in range(1, 4):
                truncated = correct_id[:-trim]
                if len(truncated) < 6:
                    break
                pattern = r"(site_id\s*=\s*')(" + re.escape(truncated) + r")(')"
                replacement = r"\g<1>" + correct_id + r"\g<3>"
                fixed = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
                if fixed != sql:
                    _LOG.info("Fixed truncated site ID in SQL: '%s' → '%s'", truncated, correct_id)
                    sql = fixed
        return sql

    user_prompt = f"User query: {prompt}"
    if filters.get("cluster"):
        user_prompt += f"\nActive filter — Zone: {filters['cluster']}"
    if filters.get("time_range") and filters["time_range"] != "24h":
        user_prompt += f"\nActive filter — Time range: {filters['time_range']}"

    llm_messages = [{"role": "system", "content": LLM_SYSTEM}]
    llm_messages.extend(conversation_history)
    llm_messages.append({"role": "user", "content": user_prompt})

    _cfg = lambda k, default="": current_app.config.get(k, "") or os.environ.get(k, "") or default

    azure_key        = _cfg("AZURE_OPENAI_API_KEY")
    azure_endpoint   = _cfg("AZURE_OPENAI_ENDPOINT")
    azure_deployment = _cfg("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
    azure_version    = _cfg("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
    if not (azure_key and azure_endpoint):
        _LOG.warning("Azure OpenAI not configured — check AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT")
        print("[AI-DEBUG] WARNING: Azure OpenAI not configured! Check AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT env vars or app config.", flush=True)

    print(f"[AI-DEBUG] prompt={prompt[:80]!r}, provider=azure, schema_cache_populated={_schema_cache.get('populated')}, kpi_count={len(_schema_cache.get('kpi_names_list', []))}", flush=True)

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
    _prompt_days  = _re_pre.search(r'last\s+(\d+)\s*days?', _p_lower)

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
    if not ai_result and len(_prompt_sites) >= 2:
        _is_trend_pre = (
            bool(_prompt_days) or
            any(w in _p_lower for w in ('trend', 'over time', 'history', 'daily', 'last'))
        )
        if _is_trend_pre:
            ai_result = _rule_based_query(prompt, time_filter, prev_context=None)
            provider  = {"provider": "rule-based-multisite"}
            _LOG.info("Multi-site trend intercepted before LLM: %s", _prompt_sites)

    # ── Azure OpenAI — sole LLM provider ─────────────────────────────────────
    if not ai_result and azure_key and azure_endpoint:
        try:
            from openai import AzureOpenAI as _AzureOpenAI
            az_client = _AzureOpenAI(
                api_key=azure_key, api_version=azure_version,
                azure_endpoint=azure_endpoint, timeout=25.0,
            )
            az_resp = az_client.chat.completions.create(
                model=azure_deployment,
                messages=llm_messages,
                temperature=0,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )
            raw_content = az_resp.choices[0].message.content
            print(f"[AI-DEBUG] Azure raw response ({len(raw_content) if raw_content else 0} chars): {(raw_content or '')[:200]}", flush=True)
            if raw_content:
                ai_result = _parse_ai_result(raw_content)
                provider = {"provider": f"azure-{azure_deployment}"}
                print(f"[AI-DEBUG] Azure parsed OK. Has sql={bool(ai_result.get('sql'))}, multi_chart={ai_result.get('multi_chart')}", flush=True)
                _LOG.info("AI query handled by Azure OpenAI (%s)", azure_deployment)
        except json.JSONDecodeError as je:
            print(f"[AI-DEBUG] AZURE returned bad JSON: {str(je)[:150]}", flush=True)
            _LOG.warning("Azure LLM returned bad JSON, using rule-based fallback: %s", str(je)[:100])
        except Exception as e:
            print(f"[AI-DEBUG] AZURE FAILED: {str(e)[:300]}", flush=True)
            _LOG.warning("Azure LLM failed: %s", str(e)[:200])

    # ── Fallback: rule-based query engine ─────────────────────────────────────
    if not ai_result:
        print(f"[AI-DEBUG] No LLM result — falling back to rule-based engine", flush=True)
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
        print(f"[AI-DEBUG] Rule-based result: has sql={bool(ai_result.get('sql'))}, multi_chart={ai_result.get('multi_chart')}, title={ai_result.get('title', '')[:50]}", flush=True)
        _LOG.info("AI query handled by rule-based fallback")
    else:
        print(f"[AI-DEBUG] LLM result obtained via {provider}. has sql={bool(ai_result.get('sql'))}, multi_chart={ai_result.get('multi_chart')}", flush=True)

    # ── Helper functions ──────────────────────────────────────────────────────
    def _sql_with_timeout(query, timeout_sec=10):
        with db.engine.connect() as conn:
            with conn.begin():
                conn.execute(sa_text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))
                result = conn.execute(sa_text(query))
                cols = list(result.keys())
                return [dict(zip(cols, row)) for row in result.fetchall()]

    def _add_safety_limits(sql, max_rows=500):
        """Cap or add LIMIT to prevent runaway queries."""
        import re as _re_sl
        upper = sql.upper().strip()
        # If there's an existing LIMIT, cap it
        m = _re_sl.search(r'\bLIMIT\s+(\d+)', upper)
        if m:
            existing = int(m.group(1))
            if existing > max_rows:
                sql = _re_sl.sub(
                    r'\bLIMIT\s+\d+',
                    f'LIMIT {max_rows}',
                    sql, count=1, flags=_re_sl.IGNORECASE,
                )
        elif 'UNION' not in upper and not upper.startswith("WITH"):
            # Add LIMIT only to simple queries (not UNION or CTE)
            sql = sql.rstrip().rstrip(';') + f' LIMIT {max_rows}'
        return sql

    def _add_date_bounds(sql):
        """Ensure queries on large tables have date filters to prevent full scans.
        Skip CTE/complex queries — they already have targeted date logic from the LLM."""
        import re as _re_db
        upper = sql.upper().strip()
        # Don't touch CTEs or multi-table queries — too risky to inject blindly
        if upper.startswith("WITH") or 'FLEXIBLE_KPI' in upper or upper.count("SELECT") > 2:
            return sql
        tables_needing_dates = ['KPI_DATA', 'MV_DAILY_SITE_KPI', 'MV_ZONE_DAILY_KPI']
        has_date_filter = bool(_re_db.search(r'\b(date|row_date)\s*(>=|<=|>|<|BETWEEN)', upper))
        if has_date_filter:
            return sql
        for tbl in tables_needing_dates:
            if tbl in upper:
                # Add a 30-day default window
                date_clause = "date >= (SELECT MAX(date) - INTERVAL '30 days' FROM mv_daily_site_kpi)"
                if 'WHERE' in upper:
                    # Insert after the first WHERE
                    sql = _re_db.sub(
                        r'\bWHERE\b',
                        f'WHERE {date_clause} AND',
                        sql, count=1, flags=_re_db.IGNORECASE,
                    )
                else:
                    sql = sql.rstrip().rstrip(';') + f' WHERE {date_clause}'
                break
        return sql

    def _downsample_for_chart(rows, max_points=300):
        """Evenly sample rows for chart rendering if too many."""
        if len(rows) <= max_points:
            return rows
        step = len(rows) / max_points
        sampled = []
        i = 0.0
        while i < len(rows):
            sampled.append(rows[int(i)])
            i += step
        # Always include the last row
        if sampled[-1] is not rows[-1]:
            sampled.append(rows[-1])
        return sampled

    def _serial(v):
        if v is None: return None
        if hasattr(v, "isoformat"): return v.isoformat()
        if isinstance(v, float) and math.isnan(v): return None
        try:    return float(v)
        except: return str(v)

    # ── POST-LLM SQL VALIDATION & REPAIR ─────────────────────────────────────
    def _fix_kpi_names_in_sql(sql):
        """Find KPI name literals in SQL and fuzzy-match to actual DB KPI names."""
        import re as _re_kpi
        if not _db_kpis:
            return sql
        # Extract all string literals that look like KPI names (in kpi_name = '...' or kpi_name IN (...))
        _literals = _re_kpi.findall(r"kpi_name\s*(?:=|ILIKE)\s*'([^']+)'", sql, _re_kpi.IGNORECASE)
        # Also extract from IN(...) lists
        _in_matches = _re_kpi.findall(r"kpi_name\s+IN\s*\(([^)]+)\)", sql, _re_kpi.IGNORECASE)
        for _in_m in _in_matches:
            _literals.extend(_re_kpi.findall(r"'([^']+)'", _in_m))
        if not _literals:
            return sql
        _db_kpis_lower = {k.lower(): k for k in _db_kpis}
        for lit in _literals:
            if lit.lower() in _db_kpis_lower:
                # Exact match (case-insensitive) — fix casing if needed
                correct = _db_kpis_lower[lit.lower()]
                if lit != correct:
                    sql = sql.replace(f"'{lit}'", f"'{correct}'")
                continue
            # Fuzzy match: find best substring match
            lit_lower = lit.lower()
            best, best_score = None, 0
            for db_kpi in _db_kpis:
                db_lower = db_kpi.lower()
                # Score: count of matching words
                lit_words = set(lit_lower.replace('_', ' ').replace('-', ' ').split())
                db_words  = set(db_lower.replace('_', ' ').replace('-', ' ').split())
                common = len(lit_words & db_words)
                # Also check substring containment
                if lit_lower in db_lower or db_lower in lit_lower:
                    common += 3
                if common > best_score:
                    best_score = common
                    best = db_kpi
            if best and best_score >= 2:
                print(f"[AI-DEBUG] KPI name fix: '{lit}' → '{best}' (score={best_score})", flush=True)
                sql = sql.replace(f"'{lit}'", f"'{best}'")
            else:
                print(f"[AI-DEBUG] KPI name '{lit}' not found in DB (best match: '{best}', score={best_score})", flush=True)
        return sql

    def _validate_table_refs(sql):
        """Check that SQL only references known tables."""
        import re as _re_tbl
        upper = sql.upper()
        _KNOWN_TABLES = {
            'KPI_DATA', 'MV_DAILY_SITE_KPI', 'MV_ZONE_DAILY_KPI',
            'TELECOM_SITES', 'FLEXIBLE_KPI_UPLOADS', 'REVENUE_DATA',
            'CORE_COMPONENT_KPI', 'NETWORK_KPI_TIMESERIES', 'TRANSPORT_KPI_DATA',
        }
        # Extract FROM/JOIN table references
        _refs = _re_tbl.findall(r'(?:FROM|JOIN)\s+(\w+)', upper)
        for ref in _refs:
            if ref not in _KNOWN_TABLES and ref not in ('SUB', 'REV', 'KPI', 'PER_SKD', 'T', 'K', 'MV', 'TS', 'R', 'S'):
                print(f"[AI-DEBUG] WARNING: Unknown table '{ref}' in SQL", flush=True)
        return True  # warn only, don't block

    # Apply KPI name fixing to all SQL in the result
    if ai_result:
        if ai_result.get("sql"):
            ai_result["sql"] = _fix_kpi_names_in_sql(ai_result["sql"])
            _validate_table_refs(ai_result["sql"])
        if ai_result.get("charts"):
            for _ch in ai_result["charts"]:
                if _ch.get("sql"):
                    _ch["sql"] = _fix_kpi_names_in_sql(_ch["sql"])

    # ── MULTI-CHART: execute each chart's SQL separately ───────────────────────
    if ai_result.get("multi_chart") and ai_result.get("charts"):
        print(f"[AI-DEBUG] Processing MULTI-CHART response with {len(ai_result['charts'])} charts", flush=True)
        charts_out = []
        for chart_spec in ai_result["charts"]:
            c_sql = chart_spec.get("sql", "")
            c_error = None
            c_rows = []
            # Apply safety limits and date bounds
            if c_sql:
                c_sql = _add_date_bounds(c_sql)
                c_sql = _add_safety_limits(c_sql, max_rows=500)
                chart_spec["sql"] = c_sql
            try:
                c_rows = _sql_with_timeout(c_sql, timeout_sec=15)
                if not c_rows:
                    _LOG.warning("Multi-chart SQL returned 0 rows — SQL: %s", c_sql[:300])
                    c_error = "Query returned no data. The site ID or KPI may not exist in the database."
            except Exception as e:
                _LOG.warning("Multi-chart SQL failed: %s — SQL: %s", e, c_sql[:300])
                c_error = str(e)
                c_rows = []
            c_rows = _downsample_for_chart(c_rows, max_points=300)
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
    if not sql or not sql.strip().upper().startswith(("SELECT", "WITH")):
        # LLM returned a result without valid SQL — fall back to rule-based
        print(f"[AI-DEBUG] ai_result has NO valid SQL. keys={list(ai_result.keys())}, sql repr={(sql or '(empty)')[:100]!r}. Falling back to rule-based.", flush=True)
        _LOG.warning("AI result has no valid SQL (keys: %s), falling back to rule-based", list(ai_result.keys()))
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
        provider = {"provider": "rule-based-nosql-fallback"}
        sql = ai_result.get("sql", "")
        print(f"[AI-DEBUG] Rule-based fallback result: has sql={bool(sql)}, sql[:100]={(sql or '(empty)')[:100]!r}", flush=True)
        # If even rule-based can't produce SQL, return a helpful text response
        if not sql or not sql.strip().upper().startswith(("SELECT", "WITH")):
            print(f"[AI-DEBUG] Even rule-based produced no SQL. Returning text-only response.", flush=True)
            text_resp = ai_result.get("response", "I couldn't generate a query for that. Try asking about a specific KPI like throughput, drop rate, or availability.")
            # Save assistant message so conversation history is preserved
            if ai_session:
                try:
                    assistant_msg = NetworkAiMessage(
                        session_id=ai_session.id, role="assistant",
                        content=text_resp,
                        content_json={"chart_type": "text", "response": text_resp},
                    )
                    db.session.add(assistant_msg)
                    db.session.commit()
                except Exception as e:
                    _LOG.error("Failed to persist text-only AI message: %s", e)
            return jsonify({
                "response": text_resp,
                "query_type": "text",
                "chart_type": "text",
                "title": "",
                "data": [],
                "columns": [],
                "row_count": 0,
                "provider": "rule-based",
                "session_id": ai_session.id if ai_session else None,
            })

    # Apply safety limits and date bounds
    sql = _add_date_bounds(sql)
    sql = _add_safety_limits(sql, max_rows=500)
    print(f"[AI-DEBUG] Final SQL to execute ({len(sql)} chars): {sql[:300]}", flush=True)

    try:
        rows = _sql_with_timeout(sql, timeout_sec=15)
        print(f"[AI-DEBUG] SQL returned {len(rows)} rows", flush=True)
    except Exception as e:
        print(f"[AI-DEBUG] SQL EXECUTION FAILED: {str(e)[:300]}", flush=True)
        _LOG.warning("AI SQL execution failed: %s — SQL: %s", e, sql[:200])
        try:
            fallback = _rule_based_query(prompt, time_filter)
            sql2 = fallback.get("sql", "")
            if sql2 and sql2.strip().upper().startswith(("SELECT", "WITH")):
                rows = _sql_with_timeout(sql2, timeout_sec=10)
                ai_result.update(fallback)
                sql = sql2
                if not provider:
                    provider = {"provider": "rule-based-fallback"}
                print(f"[AI-DEBUG] Fallback SQL returned {len(rows)} rows", flush=True)
            else:
                rows = []
        except Exception as e2:
            print(f"[AI-DEBUG] Fallback SQL also failed: {str(e2)[:200]}", flush=True)
            rows = []

    # ── CTE partial-result recovery ──────────────────────────────────────────
    # When a WITH ... SELECT query returns 0 rows (e.g. revenue JOIN kpi fails
    # because kpi_data is empty), try extracting each CTE and running it alone
    # so the user at least sees the data that IS available.
    if not rows and sql.strip().upper().startswith("WITH"):
        import re as _re_cte
        cte_pattern = _re_cte.compile(
            r"(\w+)\s+AS\s*\(\s*(SELECT\s.+?)\s*\)\s*(?:,|SELECT)",
            _re_cte.IGNORECASE | _re_cte.DOTALL,
        )
        cte_matches = cte_pattern.findall(sql)
        if cte_matches:
            print(f"[AI-DEBUG] 0-row CTE query — attempting {len(cte_matches)} CTEs individually", flush=True)
            for cte_name, cte_sql in cte_matches:
                try:
                    cte_sql_clean = _add_safety_limits(cte_sql.strip().rstrip(","), max_rows=100)
                    partial = _sql_with_timeout(cte_sql_clean, timeout_sec=10)
                    if partial:
                        print(f"[AI-DEBUG] CTE '{cte_name}' returned {len(partial)} rows — using as partial result", flush=True)
                        rows = partial
                        ai_result["response"] = (
                            f"The combined query returned no results because some data tables are empty. "
                            f"Showing partial results from the '{cte_name}' portion of the query."
                        )
                        ai_result["title"] = f"Partial: {ai_result.get('title', cte_name)}"
                        sql = cte_sql_clean
                        break
                    else:
                        print(f"[AI-DEBUG] CTE '{cte_name}' also returned 0 rows", flush=True)
                except Exception as cte_err:
                    print(f"[AI-DEBUG] CTE '{cte_name}' failed: {str(cte_err)[:150]}", flush=True)

    rows = _downsample_for_chart(rows, max_points=300)
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

    # ── When 0 rows returned, override the LLM's optimistic text ──────────
    if not rows:
        # Diagnose which tables are empty using cached info (no extra DB calls)
        diag_parts = []
        upper_sql = sql.upper()
        if ("MV_DAILY_SITE_KPI" in upper_sql or "KPI_DATA" in upper_sql) and not _kpi_data_has_data:
            diag_parts.append("RAN KPI data (kpi_data / mv_daily_site_kpi) tables are empty — please upload RAN KPI data")
        if "FLEXIBLE_KPI" in upper_sql and not _flex_has_revenue and "REVENUE" in upper_sql:
            diag_parts.append("Revenue data has not been uploaded yet")
        if diag_parts:
            resp_text = f"The query ran successfully but returned no results. Reason: {'; '.join(diag_parts)}."
        else:
            resp_text = "The query ran successfully but returned 0 rows. The site IDs, KPI names, or date range may not match any data in the database."
        print(f"[AI-DEBUG] 0-row diagnosis: {diag_parts or 'no specific cause found'}", flush=True)

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

    result_json = {
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
    }
    if not rows:
        result_json["error"] = resp_text
    return jsonify(result_json)


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
    has_days     = bool(re.search(r'last\s+\d+\s*days?', p))
    has_time_ref = has_days or bool(re.search(
        r'last\s+(year|month|week|quarter|6\s*months?|3\s*months?)'
        r'|\d+\s*(month|year|week)s?|this\s+(year|month|week)', p
    ))

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
        'only 1', 'only 2', 'only 3', 'only 4', 'only 5', 'only 6', 'only 7', 'only 8', 'only 9',
        'asked for', 'i need', 'want more', 'show more', 'more results',
        'not enough', 'missing', 'where are the', 'rest of',
    ]
    if any(kw in p for kw in mod_keywords):
        return True

    # 5. Site-only (no KPI, no time) → site-switch follow-up
    #    e.g. "show me SITE_A" / "i want to see for site id SITE_A"
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
    new_days_m = re.search(r'(?:last|past|recent)\s+(\d+)\s*days?', p)
    new_days   = int(new_days_m.group(1)) if new_days_m else None
    if not new_days:
        dm = re.search(r'(\d+)\s*days?', p)
        if dm and not re.search(r'(top|bottom|worst|best)\s+' + dm.group(1), p):
            new_days = int(dm.group(1))

    # ── FIX: Site switch — new site mentioned, no new KPI → inherit ALL previous KPIs ──
    # This handles: "i want to see for site id SITE_A" after a multi-chart response.
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

    # Count correction — "I asked for 5 not 4" / "only 4, show 5" / "show more results"
    # Adjusts LIMIT in previous SQL while keeping the query logic identical.
    _count_words = ['only', 'asked for', 'i need', 'want more', 'show more', 'more results',
                    'not enough', 'missing', 'where are the', 'rest of', 'should be']
    if any(w in p for w in _count_words):
        desired = None
        # Try to extract the desired count from the prompt
        _num_m = re.search(r'(?:asked for|need|want|show|top|get)\s+(\d+)', p)
        if not _num_m:
            _num_m = re.search(r'(\d+)\s+(?:sites?|results?|rows?|records?|items?)', p)
        if not _num_m:
            # "only 4" → user probably wants more; try to find any number
            _num_m = re.search(r'(?:only|just)\s+(\d+)', p)
            if _num_m:
                # "only 4" means they got 4, probably want the original requested count
                # Check if there's another number that's what they actually want
                _want_m = re.search(r'(?:asked for|need|want|should be|not)\s+(\d+)', p)
                if _want_m:
                    desired = int(_want_m.group(1))
                else:
                    # Bump by a few to give them more
                    desired = int(_num_m.group(1)) + 3
            else:
                desired = None
        else:
            desired = int(_num_m.group(1))

        if desired and desired > 0 and (prev_sql or prev_charts):
            _target_sql = prev_sql
            _target_charts = prev_charts

            if _target_sql:
                # Update or add LIMIT in the previous SQL
                if re.search(r'LIMIT\s+\d+', _target_sql, re.IGNORECASE):
                    new_sql = re.sub(r'LIMIT\s+\d+', f'LIMIT {desired}', _target_sql, flags=re.IGNORECASE)
                else:
                    new_sql = _target_sql.rstrip().rstrip(';') + f'\nLIMIT {desired}'
                return {
                    "sql": new_sql,
                    "query_type": prev_chart, "chart_type": prev_chart,
                    "title": re.sub(r'(?:top|bottom)\s+\d+', f'Top {desired}', prev_title, flags=re.IGNORECASE) if re.search(r'(?:top|bottom)\s+\d+', prev_title, re.IGNORECASE) else prev_title,
                    "x_axis": prev_x, "y_axes": prev_y,
                    "chart_config": prev_cfg,
                    "response": f"Updated to show {desired} results.",
                }
            elif _target_charts:
                # Multi-chart: update LIMIT in each sub-chart SQL
                updated_charts = []
                for ch in _target_charts:
                    ch_sql = ch.get("sql", "")
                    if re.search(r'LIMIT\s+\d+', ch_sql, re.IGNORECASE):
                        ch_sql = re.sub(r'LIMIT\s+\d+', f'LIMIT {desired}', ch_sql, flags=re.IGNORECASE)
                    else:
                        ch_sql = ch_sql.rstrip().rstrip(';') + f'\nLIMIT {desired}'
                    updated_ch = dict(ch)
                    updated_ch["sql"] = ch_sql
                    updated_ch["title"] = re.sub(r'(?:top|bottom)\s+\d+', f'Top {desired}', ch.get("title", ""), flags=re.IGNORECASE) if re.search(r'(?:top|bottom)\s+\d+', ch.get("title", ""), re.IGNORECASE) else ch.get("title", "")
                    updated_charts.append(updated_ch)
                return {
                    "multi_chart": True, "charts": updated_charts,
                    "sql": updated_charts[0].get("sql", ""),
                    "query_type": "multi_chart", "chart_type": "multi_chart",
                    "title": prev_title, "x_axis": "date", "y_axes": ["value"],
                    "response": f"Updated to show {desired} results per chart.",
                }

    # Combine / merge multi-chart into single chart
    _combine_words = ['combine', 'merge', 'single chart', 'one chart', 'same chart', 'together', 'into one', 'in one']
    if any(w in p for w in _combine_words) and prev_charts and len(prev_charts) >= 2:
        # Merge all previous chart SQLs into a single UNION ALL query
        union_parts = []
        chart_labels = []
        for ch in prev_charts[:4]:
            ch_sql = ch.get("sql", "").strip().rstrip(";")
            ch_title = ch.get("title", "")
            if ch_sql:
                # Wrap each chart's SQL to add a label column
                union_parts.append(f"SELECT sub.*, '{ch_title[:40]}' AS chart_label FROM ({ch_sql}) sub")
                chart_labels.append(ch_title[:30])
        if union_parts:
            combined_sql = "\nUNION ALL\n".join(union_parts)
            return {
                "sql": combined_sql,
                "query_type": "composed", "chart_type": "composed",
                "title": "Combined: " + " & ".join(chart_labels)[:60],
                "x_axis": "site_id",
                "y_axes": prev_charts[0].get("y_axes", ["value"]),
                "response": f"Combined {len(prev_charts)} charts into a single view.",
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
        'cssr':          ('LTE Call Setup Success Rate', 'cssr'),
        'call setup':    ('LTE Call Setup Success Rate', 'cssr'),
        'rrc':           ('LTE RRC Setup Success Rate', 'rrc_sr'),
        'erab':          ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'e-rab':         ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'e rab':         ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'drop':          ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'cdr':           ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'throughput':    ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'tput':          ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'dl throughput': ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'speed':         ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'prb':           ('DL PRB Utilization (1BH)', 'dl_prb'),
        'congestion':    ('DL PRB Utilization (1BH)', 'dl_prb'),
        'availability':  ('Availability', 'availability'),
        'availab':       ('Availability', 'availability'),
        'latency':       ('Average Latency Downlink', 'latency'),
        'delay':         ('Average Latency Downlink', 'latency'),
        'volte':         ('VoLTE Traffic Erlang', 'volte_erl'),
        'handover':      ('LTE Intra-Freq HO Success Rate', 'ho_sr'),
        'volume':        ('DL Data Total Volume', 'dl_volume'),
        'traffic':       ('DL Data Total Volume', 'dl_volume'),
        'connected':     ('Ave RRC Connected Ue', 'avg_rrc_ue'),
        'users':         ('Ave RRC Connected Ue', 'avg_rrc_ue'),
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

    def _extract_days(text):
        m = re.search(r'last\s+(\d+)\s*days?', text.lower())
        return int(m.group(1)) if m else None

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
    is_trend      = (bool(days) or 'trend' in p or 'over time' in p or
                     'history' in p or 'daily' in p or 'last' in p)

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

    # ── Dynamic KPI chip queries: match user keywords against ACTUAL DB KPIs ──
    # NO hardcoded KPI names — discover from _schema_cache at runtime.
    _all_db_kpis = _schema_cache.get("kpi_names_list", [])

    def _find_db_kpis(keywords):
        """Find actual KPI names from DB that match any of the keywords."""
        matched = []
        for kn in _all_db_kpis:
            kn_lower = kn.lower()
            for kw in keywords:
                if kw in kn_lower:
                    alias = re.sub(r'[^a-z0-9]+', '_', kn_lower).strip('_')[:30]
                    matched.append((kn, alias))
                    break
        return matched

    if ('drop' in p or 'cdr' in p) and not detected_kpis:
        _kpis = _find_db_kpis(['drop rate', 'drop_rate', 'cdr'])
        if _kpis:
            sql = _kd_site_query(_kpis[:3], _kpis[0][1], "DESC")
            return {"sql":sql,"query_type":"bar","title":f"Top {N} — Drop Rate","x_axis":"site_id","y_axes":[a for _,a in _kpis[:3]],"response":f"Showing {N} sites with highest drop rate."}

    if ('throughput' in p or 'tput' in p or 'speed' in p or 'mbps' in p) and not detected_kpis:
        _kpis = _find_db_kpis(['throughput', 'tput', 'ave throughput'])
        if _kpis:
            order = "ASC" if any(w in p for w in ['worst','low','bad','poor','bottom']) else "DESC"
            sql = _kd_site_query(_kpis[:3], _kpis[0][1], order)
            return {"sql":sql,"query_type":"bar","title":f"{'Bottom' if order=='ASC' else 'Top'} {N} — Throughput","x_axis":"site_id","y_axes":[a for _,a in _kpis[:2]],"response":f"{'Worst' if order=='ASC' else 'Best'} {N} sites by throughput."}

    if ('prb' in p or 'congestion' in p or 'congested' in p) and not detected_kpis:
        _kpis = _find_db_kpis(['prb util', 'prb_util', 'prb utilization'])
        if _kpis:
            sql = _kd_site_query(_kpis[:3], _kpis[0][1], "DESC")
            return {"sql":sql,"query_type":"bar","title":f"Top {N} Congested Sites","x_axis":"site_id","y_axes":[a for _,a in _kpis[:3]],"response":f"Top {N} sites by PRB Utilization."}

    if ('cssr' in p or 'call setup' in p or 'setup success' in p) and not detected_kpis:
        _kpis = _find_db_kpis(['call setup', 'setup success', 'cssr'])
        if _kpis:
            sql = _kd_site_query(_kpis[:3], _kpis[0][1], "ASC")
            return {"sql":sql,"query_type":"bar","title":f"Bottom {N} — Call Setup","x_axis":"site_id","y_axes":[a for _,a in _kpis[:2]],"response":f"Showing {N} sites with lowest CSSR."}

    if ('rrc' in p or 'accessibility' in p) and not detected_kpis:
        _kpis = _find_db_kpis(['rrc', 'accessibility', 'e-rab setup'])
        if _kpis:
            sql = _kd_site_query(_kpis[:3], _kpis[0][1], "ASC")
            return {"sql":sql,"query_type":"bar","title":f"Bottom {N} — Accessibility","x_axis":"site_id","y_axes":[a for _,a in _kpis[:2]],"response":f"Showing {N} sites with lowest accessibility."}

    if ('zone' in p or 'cluster' in p or 'compar' in p) and not detected_kpis:
        # Use first 4 KPIs from DB for zone comparison
        _kpis = _all_db_kpis[:4] if _all_db_kpis else []
        if _kpis:
            _cases = ", ".join(f"AVG(CASE WHEN k.kpi_name='{kn}' THEN k.value END) AS {re.sub(r'[^a-z0-9]+','_',kn.lower()).strip('_')[:25]}" for kn in _kpis)
            _in_cl = ", ".join(f"'{kn}'" for kn in _kpis)
            _aliases = [re.sub(r'[^a-z0-9]+','_',kn.lower()).strip('_')[:25] for kn in _kpis]
            return {"sql":f"""SELECT ts.zone AS cluster, COUNT(DISTINCT k.site_id) AS sites, {_cases}
                FROM kpi_data k {GEO_JOIN}
                WHERE k.data_level='site' AND k.value IS NOT NULL AND ts.zone IS NOT NULL
                  AND k.kpi_name IN ({_in_cl})
                GROUP BY ts.zone ORDER BY {_aliases[0]} DESC NULLS LAST""",
                "query_type":"bar","title":"Zone-wise KPI Comparison","x_axis":"cluster","y_axes":_aliases[:3],"response":"Zone-level KPI comparison."}

    if 'availab' in p or 'downtime' in p or 'uptime' in p:
        sql = _kd_site_query([("Availability","availability"),("DL PRB Utilization (1BH)","dl_prb_util")],"availability","ASC")
        return {"sql":sql,"query_type":"bar","title":"Sites with Lowest Availability","x_axis":"site_id","y_axes":["availability"],"response":"Sites with lowest availability."}

    if 'latency' in p or 'delay' in p or 'ping' in p:
        sql = _kd_site_query([("Average Latency Downlink","avg_latency"),("LTE DL - Usr Ave Throughput","dl_usr_tput")],"avg_latency","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} High Latency Sites","x_axis":"site_id","y_axes":["avg_latency"],"response":f"Showing {N} sites with highest latency."}

    if 'volume' in p or 'data volume' in p:
        sql = _kd_site_query([("DL Data Total Volume","dl_volume"),("UL Data Total Volume","ul_volume"),("DL PRB Utilization (1BH)","dl_prb_util")],"dl_volume","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} by Data Volume","x_axis":"site_id","y_axes":["dl_volume","ul_volume"],"response":f"Showing {N} sites with highest data volume."}

    if 'user' in p or 'ue' in p or 'connected' in p:
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
    if 'drop' in p or 'cdr' in p:
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
