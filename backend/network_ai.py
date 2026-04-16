"""
network_ai.py  — v6 (2026-04-16)
==================================
Flask Blueprint: Network AI Chat & Query Engine

Architecture:
  User NL Query
    → Query Router (analytical / informational / follow-up)
    → LLM SQL Generation (Azure OpenAI) with schema-aware prompt
    → SQL Validation & Safety Guards
    → Query Execution (PostgreSQL, timeout, pagination)
    → Structured JSON + Visualization Output
    → WebSocket streaming for intermediate states

Features:
  • LLM generates SQL only — PostgreSQL handles all computation
  • No SELECT *, no DROP/DELETE/UPDATE — strict read-only safety
  • In-memory LRU cache for schema discovery and query results
  • WebSocket streaming: understanding → generating → executing → rendering
  • Database optimization: indexes, materialized views
  • Query router: analytical → SQL path, informational → direct LLM
  • Comprehensive SQL guardrails against hallucinated SQL

Mount in app.py:
    from network_ai import network_ai_bp
    app.register_blueprint(network_ai_bp)
"""

import os
import re
import json
import math
import time
import hashlib
import logging
import threading
from datetime import datetime, timezone
from functools import lru_cache

from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text as sa_text

from models import db, User, NetworkAiSession, NetworkAiMessage

_LOG = logging.getLogger("network_ai")
NETWORK_AI_VERSION = "2026-04-16-v6"

# ─────────────────────────────────────────────────────────────────────────────
network_ai_bp = Blueprint("network_ai", __name__)

# ─── Query Result Cache (TTL-based in-memory) ───────────────────────────────
_query_cache = {}          # {hash: {"data": ..., "ts": time.time()}}
_cache_lock = threading.Lock()
_CACHE_TTL = 300           # 5 minutes for query results
_SCHEMA_CACHE_TTL = 600    # 10 minutes for schema discovery

# ─── Allowed tables for SQL validation ───────────────────────────────────────
_ALLOWED_TABLES = frozenset({
    'kpi_data', 'telecom_sites', 'flexible_kpi_uploads',
    'revenue_data', 'core_kpi_data', 'transport_kpi_data',
    'network_issue_tickets',
    'site_kpi_summary',  # ML-categorized pre-aggregated KPI data
})
_BLOCKED_SQL_PATTERNS = re.compile(
    r'\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|EXEC|EXECUTE)\b',
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# SQL Safety & Validation
# ─────────────────────────────────────────────────────────────────────────────

def _strip_sql_strings(sql: str) -> str:
    """Remove all string literals from SQL so keyword checks don't match
    words inside values like 'E-RAB Call Drop Rate_1'."""
    return re.sub(r"'[^']*'", "''", sql)


def _validate_sql(sql: str) -> tuple:
    """Validate generated SQL for safety. Returns (is_safe, error_message).
    Rules: only SELECT allowed, no dangerous operations, must have LIMIT,
    no SELECT *, must reference allowed tables only."""
    if not sql or not sql.strip():
        return False, "Empty SQL query"

    normalized = sql.strip()

    # Must start with SELECT (or WITH for CTEs)
    if not re.match(r'^(SELECT|WITH)\b', normalized, re.IGNORECASE):
        return False, "Only SELECT queries are allowed"

    # Strip string literals before checking for blocked keywords.
    # This prevents false positives like 'E-RAB Call Drop Rate_1' matching DROP
    stripped = _strip_sql_strings(normalized)

    # Block dangerous operations (only check SQL keywords, not string content)
    if _BLOCKED_SQL_PATTERNS.search(stripped):
        match = _BLOCKED_SQL_PATTERNS.search(stripped)
        return False, f"Blocked SQL operation: {match.group(0)}"

    # Ensure LIMIT exists (prevent unbounded queries on 22.5M rows)
    if 'LIMIT' not in normalized.upper():
        normalized = normalized.rstrip(';') + ' LIMIT 500'

    # Check that only allowed tables are referenced (skip subquery aliases)
    from_tables = re.findall(r'\bFROM\s+(\w+)', stripped, re.IGNORECASE)
    join_tables = re.findall(r'\bJOIN\s+(\w+)', stripped, re.IGNORECASE)
    all_tables = set(t.lower() for t in from_tables + join_tables)
    # Remove SQL keywords, common aliases, and subquery refs
    _skip = {'select', 'where', 'group', 'order', 'having', 'limit',
             'union', 'all', 'as', 'lateral', 'each', 'unnest',
             'k', 'f', 'r', 'c', 't', 'ts', 'n', 's'}  # common table aliases
    all_tables -= _skip

    invalid_tables = all_tables - _ALLOWED_TABLES
    if invalid_tables:
        return False, f"Unknown table(s): {', '.join(invalid_tables)}"

    return True, normalized


def _add_safety_limits(sql: str, max_rows: int = 500) -> str:
    """Ensure SQL has a LIMIT clause and is bounded."""
    if not sql:
        return sql
    normalized = sql.strip().rstrip(';')
    if 'LIMIT' not in normalized.upper():
        normalized += f' LIMIT {max_rows}'
    return normalized


# ─────────────────────────────────────────────────────────────────────────────
# Query Router — Classify queries before processing
# ─────────────────────────────────────────────────────────────────────────────

_INFORMATIONAL_PATTERNS = [
    r'^(?:what|how|why|explain|describe|tell me about|define|meaning of)\b',
    r'\b(?:what is|what are|how does|how do|explain|describe)\b.*\b(?:kpi|metric|parameter|concept|term)\b',
    r'\b(?:difference between|compare concept|meaning of)\b',
]

_ANALYTICAL_INDICATORS = [
    'show', 'display', 'plot', 'chart', 'graph', 'trend', 'compare',
    'top', 'bottom', 'worst', 'best', 'highest', 'lowest',
    'average', 'sum', 'count', 'total', 'aggregate',
    'last', 'days', 'week', 'month', 'daily', 'hourly',
    'site', 'cell', 'zone', 'revenue', 'throughput', 'drop rate',
    'cssr', 'prb', 'availability', 'latency', 'volte',
    # ML category indicators
    'healthy', 'degraded', 'critical', 'anomaly', 'anomalies', 'outlier',
    'health score', 'health', 'tier', 'performer', 'underperformer',
    'top performer', 'classification', 'category', 'categorize',
]


def _classify_query(prompt: str) -> str:
    """Classify a user query into: 'analytical', 'informational', or 'followup'.
    - analytical: needs SQL generation → data fetch → chart
    - informational: general telecom knowledge → direct LLM response
    - followup: modifies previous result"""
    p = prompt.lower().strip()

    # Check for informational patterns
    for pattern in _INFORMATIONAL_PATTERNS:
        if re.search(pattern, p):
            # But if it also has analytical indicators, it's analytical
            if any(ind in p for ind in ['show', 'display', 'plot', 'chart', 'trend',
                                         'top', 'bottom', 'worst', 'best', 'last', 'days']):
                return 'analytical'
            # Check if it mentions specific sites
            if re.search(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', p):
                return 'analytical'
            return 'informational'

    # Check for analytical indicators
    if any(ind in p for ind in _ANALYTICAL_INDICATORS):
        return 'analytical'

    # Check for site IDs
    if re.search(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', p):
        return 'analytical'

    # Default to analytical for ambiguous queries
    return 'analytical'


def _handle_informational_query(prompt: str) -> dict:
    """Handle informational queries with direct LLM response (no SQL)."""
    from app import client as _llm_client, DEPLOYMENT_NAME as _llm_model

    system_msg = """You are a telecom network expert. Answer the user's question about
telecom concepts, KPIs, network architecture, or industry terminology.

Be concise (2-4 sentences). Use bullet points for lists. Include the standard value ranges
where applicable (e.g., "Call Drop Rate should be below 1.5%").

If the question is actually asking for data analysis (e.g., "what is the drop rate for site X"),
respond with: {"redirect": "analytical"} to redirect to the SQL engine."""

    try:
        resp = _llm_client.chat.completions.create(
            model=_llm_model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=500,
        )
        content = resp.choices[0].message.content.strip()

        # Check if LLM redirected to analytical
        if '{"redirect"' in content or '"redirect"' in content:
            return None  # Signal to route to analytical path

        return {
            "type": "informational",
            "response": content,
            "chart_type": "none",
            "data": [],
            "columns": [],
            "row_count": 0,
        }
    except Exception as e:
        _LOG.warning("Informational query failed: %s", e)
        return None  # Fallback to analytical


# ─────────────────────────────────────────────────────────────────────────────
# Cache Layer
# ─────────────────────────────────────────────────────────────────────────────

def _cache_key(sql: str) -> str:
    """Generate cache key from SQL query."""
    return hashlib.md5(sql.strip().lower().encode()).hexdigest()


def _cache_get(key: str, ttl: int = None) -> dict:
    """Get cached result if still valid."""
    ttl = ttl or _CACHE_TTL
    with _cache_lock:
        entry = _query_cache.get(key)
        if entry and (time.time() - entry["ts"]) < ttl:
            _LOG.debug("Cache hit: %s", key[:8])
            return entry["data"]
        return None


def _cache_set(key: str, data: dict):
    """Store result in cache."""
    with _cache_lock:
        # Evict old entries if cache too large (max 200 entries)
        if len(_query_cache) > 200:
            oldest = sorted(_query_cache.items(), key=lambda x: x[1]["ts"])[:50]
            for k, _ in oldest:
                _query_cache.pop(k, None)
        _query_cache[key] = {"data": data, "ts": time.time()}


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket Streaming for intermediate states
# ─────────────────────────────────────────────────────────────────────────────

def _emit_progress(session_id: int, stage: str, message: str = ""):
    """Emit a WebSocket event for query progress.
    Stages: understanding → generating → executing → rendering → complete"""
    try:
        from flask_socketio import emit as ws_emit
        ws_emit('ai_progress', {
            'session_id': session_id,
            'stage': stage,
            'message': message,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }, room=f'ai_session_{session_id}', namespace='/')
    except Exception:
        pass  # WebSocket is optional, never break the flow


# ─────────────────────────────────────────────────────────────────────────────
# Database Optimization — Indexes & Materialized Views
# ─────────────────────────────────────────────────────────────────────────────

def ensure_db_optimizations():
    """Create indexes and materialized views for optimal query performance.
    Safe to call multiple times (idempotent with IF NOT EXISTS)."""
    _LOG.info("Ensuring database optimizations...")

    _indexes = [
        # kpi_data indexes for common query patterns
        "CREATE INDEX IF NOT EXISTS idx_kpi_site_name_date ON kpi_data(site_id, kpi_name, date)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_level_name ON kpi_data(data_level, kpi_name)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_site_level_date ON kpi_data(site_id, kpi_name, data_level, date)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_date ON kpi_data(date DESC)",
        # telecom_sites indexes
        "CREATE INDEX IF NOT EXISTS idx_sites_zone ON telecom_sites(zone)",
        "CREATE INDEX IF NOT EXISTS idx_sites_status ON telecom_sites(site_status)",
        "CREATE INDEX IF NOT EXISTS idx_sites_city ON telecom_sites(city)",
        # flexible_kpi_uploads indexes
        "CREATE INDEX IF NOT EXISTS idx_flex_type_col ON flexible_kpi_uploads(kpi_type, column_name)",
        "CREATE INDEX IF NOT EXISTS idx_flex_site ON flexible_kpi_uploads(site_id)",
        # network_issue_tickets indexes
        "CREATE INDEX IF NOT EXISTS idx_nit_status ON network_issue_tickets(status)",
        "CREATE INDEX IF NOT EXISTS idx_nit_priority ON network_issue_tickets(priority_score DESC)",
    ]

    _materialized_views = [
        # Pre-aggregated daily site KPI summary
        """CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_site_kpi AS
        SELECT site_id, kpi_name, date,
               AVG(value) AS avg_value,
               MIN(value) AS min_value,
               MAX(value) AS max_value,
               COUNT(*) AS sample_count
        FROM kpi_data
        WHERE data_level = 'site' AND value IS NOT NULL
        GROUP BY site_id, kpi_name, date""",

        # Pre-aggregated zone-level KPI summary
        """CREATE MATERIALIZED VIEW IF NOT EXISTS mv_zone_kpi_summary AS
        SELECT ts.zone, k.kpi_name,
               AVG(k.value) AS avg_value,
               MIN(k.value) AS min_value,
               MAX(k.value) AS max_value,
               COUNT(DISTINCT k.site_id) AS site_count
        FROM kpi_data k
        JOIN telecom_sites ts ON k.site_id = ts.site_id
        WHERE k.data_level = 'site' AND k.value IS NOT NULL
        GROUP BY ts.zone, k.kpi_name""",
    ]

    _mv_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_mv_daily_site ON mv_daily_site_kpi(site_id, kpi_name, date)",
        "CREATE INDEX IF NOT EXISTS idx_mv_zone_kpi ON mv_zone_kpi_summary(zone, kpi_name)",
    ]

    try:
        with db.engine.connect() as conn:
            # Create indexes
            for idx_sql in _indexes:
                try:
                    conn.execute(sa_text(idx_sql))
                except Exception as e:
                    _LOG.debug("Index skip: %s", str(e)[:80])
            conn.commit()

            # Create materialized views
            for mv_sql in _materialized_views:
                try:
                    conn.execute(sa_text(mv_sql))
                except Exception as e:
                    _LOG.debug("MV skip: %s", str(e)[:80])
            conn.commit()

            # Create MV indexes
            for mv_idx in _mv_indexes:
                try:
                    conn.execute(sa_text(mv_idx))
                except Exception as e:
                    _LOG.debug("MV index skip: %s", str(e)[:80])
            conn.commit()

        _LOG.info("Database optimizations complete.")
    except Exception as e:
        _LOG.warning("DB optimization failed (non-fatal): %s", e)


def refresh_materialized_views():
    """Refresh all materialized views. Call periodically or after data uploads."""
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_site_kpi"))
            conn.execute(sa_text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_zone_kpi_summary"))
            conn.commit()
        _LOG.info("Materialized views refreshed successfully.")
    except Exception as e:
        _LOG.debug("MV refresh skip: %s", str(e)[:80])

# ─── Module-level schema discovery cache (populated on first ai_query call) ──
_schema_cache = {
    "populated": False,
    "available_tables": set(),
    # revenue_data
    "rev_data_cols": [], "rev_rev_cols": [], "rev_opex_cols": [], "rev_has_subscribers": False,
    # core_kpi_data
    "core_data_cols": [], "core_metric_cols": [],
    # transport_kpi_data
    "transport_data_cols": [], "transport_metric_cols": [],
    # network_issue_tickets
    "ticket_cols": [],
    # data freshness — actual max date in kpi_data (may lag behind CURRENT_DATE)
    "kpi_max_date": None,   # e.g. "2026-03-22"
    "kpi_max_date_recent": None,  # max date among KPIs that have recent data
}

# ─── Shared helpers (imported lazily from network_analytics) ─────────────────

def _sql(query: str, params: dict = None) -> list:
    """Execute raw SQL and return list of dicts."""
    with db.engine.connect() as conn:
        result = conn.execute(sa_text(query), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def _kpi_date_ref():
    """Return a SQL expression for the latest KPI data date.
    If the data lags behind today, returns e.g. '2026-03-22'::date.
    Otherwise returns CURRENT_DATE."""
    _max = _schema_cache.get("kpi_max_date_recent")
    return f"'{_max}'::date" if _max else "CURRENT_DATE"


def _kpi_date_clause(days_val, alias='k'):
    """Build a date range WHERE clause using the actual data max date."""
    _ref = _kpi_date_ref()
    if days_val:
        return f"AND {alias}.date >= {_ref} - INTERVAL '{days_val} days' AND {alias}.date <= {_ref}"
    return f"AND {alias}.date <= {_ref}"


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
    _query_start = time.time()
    body    = request.get_json(silent=True) or {}
    prompt  = str(body.get("prompt", "")).strip()
    context = body.get("context", {})
    filters = context.get("filters", {})
    time_range = filters.get("time_range", "24h")
    time_filter = _get_dynamic_time_filter()(time_range)

    if not prompt:
        return jsonify({"error": "No prompt provided"}), 400

    # ── Query Router: classify the query type ──────────────────────────────
    query_type = _classify_query(prompt)
    _LOG.info("[ROUTER] Query classified as: %s | prompt: %s", query_type, prompt[:80])

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

    # ── WebSocket: signal query started ──────────────────────────────────────
    _ws_sid = ai_session.id if ai_session else None
    _emit_progress(_ws_sid, 'understanding', 'Analyzing your question...')

    # ── Handle informational queries (no SQL needed) ─────────────────────────
    if query_type == 'informational':
        _emit_progress(_ws_sid, 'generating', 'Preparing answer...')
        info_result = _handle_informational_query(prompt)
        if info_result:
            _emit_progress(_ws_sid, 'complete', 'Done')
            resp_text = info_result["response"]
            # Persist to session
            if ai_session:
                try:
                    if ai_session.title == "New Chat":
                        ai_session.title = prompt[:60]
                    assistant_msg = NetworkAiMessage(
                        session_id=ai_session.id, role="assistant",
                        content=resp_text,
                        content_json={"type": "informational", "response": resp_text},
                    )
                    db.session.add(assistant_msg)
                    db.session.commit()
                except Exception as e:
                    _LOG.error("Failed to persist informational msg: %s", e)
            _elapsed = round(time.time() - _query_start, 2)
            _LOG.info("[COMPLETE] Informational query in %.2fs", _elapsed)
            return jsonify({
                "response": resp_text,
                "query_type": "informational",
                "chart_type": "none",
                "title": prompt[:60],
                "data": [],
                "columns": [],
                "row_count": 0,
                "provider": "llm-informational",
                "session_id": _ws_sid,
                "elapsed_seconds": _elapsed,
            })
        # If info_result is None, LLM redirected to analytical — continue below

    _emit_progress(_ws_sid, 'generating', 'Generating SQL query...')

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
     'revenue_total'    — TOTAL revenue (sum of all months, numeric) — USE THIS for total revenue queries
     'opex_jan_l'       — January OPEX in Lakhs (numeric)
     'opex_feb_l'       — February OPEX in Lakhs (numeric)
     'opex_mar_l'       — March OPEX in Lakhs (numeric)
     'opex_total'       — TOTAL OPEX (sum of all months, numeric) — USE THIS for total OPEX queries
     'difference'       — Revenue minus OPEX (numeric)
     'zone'             — geographic zone (text, in str_value)
     'technology'       — technology type (text, in str_value)
     'site_category'    — site classification (text, in str_value)

   IMPORTANT: column_name values may have mixed casing (e.g. 'Subscribers', 'Revenue_Jan_L').
   ALWAYS use ILIKE (case-insensitive) for column_name matching, NEVER use = or LIKE.
   IMPORTANT: For total revenue, use 'revenue_total' directly — do NOT SUM all revenue columns
              (that would double-count because revenue_total = jan + feb + mar).

   Example: Get total revenue per site (use revenue_total, NOT SUM of monthly):
     SELECT f.site_id, f.num_value AS total_revenue
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue'
       AND f.column_name ILIKE '%revenue_total%' AND f.column_type = 'numeric'
     ORDER BY total_revenue DESC

   Example: Get subscribers for a specific site:
     SELECT f.site_id, f.num_value AS subscribers
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%subscriber%'
       AND f.site_id = 'GUR_LTE_1500'

   Example: Get revenue for a specific month (March):
     SELECT f.site_id, f.num_value AS revenue_mar
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%revenue%mar%'

   Example: Compare revenue vs OPEX for a site:
     SELECT f.site_id, f.column_name, f.num_value
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'revenue'
       AND (f.column_name ILIKE '%revenue%' OR f.column_name ILIKE '%opex%')
       AND f.site_id = 'GUR_LTE_1500'

   === Core KPI data (kpi_type = 'core') ===
   IMPORTANT: Core data uses kpi_name (NOT column_name) for the metric type.
   kpi_name values: 'Authentication Success Rate', 'CPU Utilization', 'Attach Success Rate', 'PDP Bearer Setup Success Rate'
   column_name contains DATES (e.g., '2026_03_07_000000'), num_value contains the metric value.
   ALWAYS use ILIKE for kpi_name matching.

   Example: Get average core KPIs per site:
     SELECT f.site_id,
       AVG(CASE WHEN f.kpi_name ILIKE '%auth%' THEN f.num_value END) AS auth_sr,
       AVG(CASE WHEN f.kpi_name ILIKE '%cpu%' THEN f.num_value END) AS cpu_util,
       AVG(CASE WHEN f.kpi_name ILIKE '%attach%' THEN f.num_value END) AS attach_sr,
       AVG(CASE WHEN f.kpi_name ILIKE '%pdp%' THEN f.num_value END) AS pdp_sr
     FROM flexible_kpi_uploads f
     WHERE f.kpi_type = 'core' AND f.column_type = 'numeric'
     GROUP BY f.site_id

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
User says "site info" / "location" / "which city" / "which zone" / "site status" / "alarms" → telecom_sites table
User says "last 7 days" → AND k.date >= CURRENT_DATE - INTERVAL '7 days' AND k.date <= CURRENT_DATE
User says "last month" → AND k.date >= CURRENT_DATE - INTERVAL '1 month' AND k.date <= CURRENT_DATE
ALWAYS add AND k.date <= CURRENT_DATE when any date range is used, to exclude future data.

4. transport_kpi_data — Transport/backhaul network KPI data
   NOTE: Actual columns are discovered at runtime — see "ACTUAL COLUMNS in transport_kpi_data" section below.
   Typical columns: site_id, zone, backhaul_type, link_capacity, avg_util, peak_util,
            packet_loss, avg_latency, jitter, availability, error_rate, tput_efficiency, alarms
   Do NOT assume specific column names — use ONLY the columns listed in the runtime discovery section.

   Example: Get transport KPIs for a site:
     SELECT t.* FROM transport_kpi_data t WHERE t.site_id = 'GUR_LTE_1500'

5. core_kpi_data — Core network KPIs with date (flat table, NOT EAV)
   NOTE: Actual columns are discovered at runtime — see "ACTUAL COLUMNS in core_kpi_data" section below.
   Typical columns: site_id, date, auth_sr, cpu_util, attach_sr, pdp_sr
   Do NOT assume specific column names — use ONLY the columns listed in the runtime discovery section.
   If the runtime section is not present, fall back to flexible_kpi_uploads with kpi_type='core'.

   Example: Core KPIs for a site over time:
     SELECT c.* FROM core_kpi_data c WHERE c.site_id = 'GUR_LTE_1500' ORDER BY c.date

6. revenue_data — Revenue per site (flat table, one row per site)
   NOTE: Actual columns are discovered at runtime — see "ACTUAL COLUMNS in revenue_data" section below.
   The table may have monthly revenue columns (like rev_jan, rev_feb, ...) and OPEX columns.
   Do NOT assume specific column names — use ONLY the columns listed in the runtime discovery section.
   If the runtime section is not present, fall back to flexible_kpi_uploads with kpi_type='revenue'.

7. network_issue_tickets — Auto-generated tickets for worst-performing cells
   NOTE: Actual columns are discovered at runtime — see "ACTUAL COLUMNS in network_issue_tickets" section below.
   Typical columns: site_id, cells_affected, category, priority, priority_score, sla_hours,
            avg_drop_rate, avg_cssr, avg_tput, violations, status, zone, location,
            assigned_agent, root_cause, recommendation, created_at
   Do NOT assume specific column names — use ONLY the columns listed in the runtime discovery section.

   Example: Open network issue tickets:
     SELECT n.* FROM network_issue_tickets n WHERE n.status IN ('open','in_progress') ORDER BY n.priority_score DESC

8. site_kpi_summary — ML-categorized pre-aggregated daily KPI summary per site
   Columns: site_id, date, kpi_name, avg_value, min_value, max_value, stddev_value, sample_count,
            health_label, health_confidence, is_anomaly, anomaly_score, site_tier, health_score, zone
   - health_label = 'healthy', 'degraded', or 'critical' (ML K-Means per KPI)
   - is_anomaly = true/false (ML Isolation Forest outlier detection)
   - site_tier = 'top_performer', 'good', 'average', or 'underperformer' (ML K-Means multi-KPI)
   - health_score = 0-100 composite score (higher = healthier)
   - health_confidence = 0-1 (how confident the ML model is in the health_label)
   - anomaly_score = negative float (more negative = more anomalous)

   USE THIS TABLE for questions about:
   - site health, performance categories, degraded/critical sites
   - anomalies, outliers, unusual behavior
   - site rankings, top/worst performers, performance tiers
   - health scores, health trends over time
   - comparing site performance across zones

   Example: Get worst-performing sites today:
     SELECT s.site_id, s.zone, s.health_score, s.site_tier, s.health_label, s.kpi_name, s.avg_value
     FROM site_kpi_summary s
     WHERE s.date = (SELECT MAX(date) FROM site_kpi_summary)
       AND s.health_label = 'critical'
     ORDER BY s.health_score ASC LIMIT 20

   Example: Anomalies detected in last 7 days:
     SELECT s.site_id, s.date::text, s.kpi_name, s.avg_value, s.anomaly_score, s.health_label
     FROM site_kpi_summary s
     WHERE s.is_anomaly = true
       AND s.date >= (SELECT MAX(date) FROM site_kpi_summary) - INTERVAL '7 days'
     ORDER BY s.anomaly_score ASC LIMIT 50

   Example: Site health trend over time:
     SELECT s.date::text, s.health_score, s.health_label
     FROM site_kpi_summary s
     WHERE s.site_id = 'GUR_LTE_1500' AND s.kpi_name = 'E-RAB Call Drop Rate_1'
     ORDER BY s.date LIMIT 30

   Example: Zone-wise health comparison:
     SELECT s.zone, s.kpi_name,
            AVG(s.health_score) AS avg_health,
            COUNT(*) FILTER (WHERE s.health_label = 'critical') AS critical_count,
            COUNT(*) FILTER (WHERE s.is_anomaly = true) AS anomaly_count
     FROM site_kpi_summary s
     WHERE s.date = (SELECT MAX(date) FROM site_kpi_summary)
     GROUP BY s.zone, s.kpi_name
     ORDER BY avg_health ASC LIMIT 30

   Example: Top performing sites:
     SELECT s.site_id, s.zone, AVG(s.health_score) AS avg_health, s.site_tier
     FROM site_kpi_summary s
     WHERE s.date >= (SELECT MAX(date) FROM site_kpi_summary) - INTERVAL '7 days'
     GROUP BY s.site_id, s.zone, s.site_tier
     HAVING s.site_tier = 'top_performer'
     ORDER BY avg_health DESC LIMIT 20

=== TABLE SELECTION RULE ===
- Health/performance categories, anomalies, site tiers, rankings → query site_kpi_summary table (PREFERRED for categorized insights)
- RAN performance KPIs (raw values, specific hours, cell-level) → query kpi_data table
- Revenue, OPEX, subscribers, ARPU → query revenue_data table FIRST; fallback to flexible_kpi_uploads with kpi_type='revenue'
- Core network KPIs (authentication, CPU, attach, PDP) → query core_kpi_data table FIRST; fallback to flexible_kpi_uploads with kpi_type='core'
- Transport/backhaul KPIs (link capacity, jitter, packet loss, backhaul) → query transport_kpi_data table
- Network issue tickets (worst cells, AI tickets, open issues) → query network_issue_tickets table
- Site info (zone, city, state, technology, location) → query telecom_sites table
NEVER query kpi_data for revenue/subscriber/OPEX data — it does not exist there.
If a table returns 0 rows, try the alternative table (e.g., revenue_data → flexible_kpi_uploads).

=== WHEN TO USE site_kpi_summary vs kpi_data ===
PREFER site_kpi_summary when the user asks about:
- "healthy", "degraded", "critical" sites/KPIs
- "anomalies", "outliers", "unusual" readings
- "top performers", "worst sites", "underperformers", site "tiers"
- "health score", "performance score"
- general aggregated trends (daily averages)
- zone-level comparisons
PREFER kpi_data when the user asks about:
- hourly data, specific hours of the day
- cell-level data (data_level = 'cell')
- raw unaggregated values
- data that may not yet be in the summary (just uploaded)

=== CRITICAL: column_name matching in flexible_kpi_uploads ===
Column names may have MIXED CASING (e.g., 'Subscribers', 'Revenue_Jan_L', 'OPEX_Feb (L)').
ALWAYS use ILIKE for column_name matching:
  CORRECT: f.column_name ILIKE '%revenue%'
  WRONG:   f.column_name LIKE 'revenue%'  -- case-sensitive, will miss 'Revenue_Jan'
  WRONG:   f.column_name = 'subscribers'   -- exact match, will miss 'Subscribers'
"""

    # ── Dynamic table availability — tell LLM which tables actually exist ──
    try:
        _tbl_rows = _sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name IN "
            "('revenue_data','core_kpi_data','transport_kpi_data',"
            "'network_issue_tickets','flexible_kpi_uploads','kpi_data','telecom_sites',"
            "'site_kpi_summary')"
        )
        _available_tables = {r['table_name'] for r in _tbl_rows}
    except Exception:
        _available_tables = set()

    _tbl_note = "\n=== AVAILABLE TABLES (real-time, checked just now) ===\n"
    for _tn, _fb in [
        ('kpi_data', None),
        ('telecom_sites', None),
        ('site_kpi_summary', "kpi_data (raw, no ML categories)"),
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

    # ── Dynamic revenue column discovery — tell LLM the EXACT column names ──
    if 'flexible_kpi_uploads' in _available_tables:
        try:
            _rev_cols = _sql(
                "SELECT DISTINCT column_name, column_type "
                "FROM flexible_kpi_uploads WHERE kpi_type = 'revenue' "
                "ORDER BY column_name LIMIT 40"
            )
            if _rev_cols:
                _rcn = "\n=== ACTUAL COLUMN NAMES in flexible_kpi_uploads (kpi_type='revenue') ===\n"
                _rcn += "These are the REAL column_name values — use these EXACT values in queries.\n"
                _rcn += "IMPORTANT: ALWAYS use ILIKE (not LIKE or =) for column_name matching.\n"
                for _rc in _rev_cols:
                    _rcn += f"  '{_rc['column_name']}' ({_rc['column_type']})\n"
                _rcn += "Example: WHERE f.column_name ILIKE '%revenue%' (not LIKE or =)\n"
                _rcn += "Example: WHERE f.column_name ILIKE '%subscriber%' (not = 'subscribers')\n"
                SCHEMA_HINT += _rcn
        except Exception:
            pass

    # ── Dynamic schema discovery → populate module-level _schema_cache ──
    global _schema_cache
    _schema_cache["available_tables"] = _available_tables
    _LOG.info("[DISCOVERY] Available tables: %s", _available_tables)

    if 'revenue_data' in _available_tables:
        try:
            _rdc = _sql(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'revenue_data' "
                "AND column_name NOT IN ('id', 'uploaded_at') "
                "ORDER BY ordinal_position"
            )
            _schema_cache["rev_data_cols"] = [r['column_name'] for r in _rdc]
            _skip = {'id', 'uploaded_at', 'site_id', 'zone', 'technology', 'site_category', 'subscribers'}
            _schema_cache["rev_rev_cols"] = [c for c in _schema_cache["rev_data_cols"] if 'rev' in c.lower() and c.lower() not in _skip]
            _schema_cache["rev_opex_cols"] = [c for c in _schema_cache["rev_data_cols"] if 'opex' in c.lower()]
            _schema_cache["rev_has_subscribers"] = any(c.lower() == 'subscribers' for c in _schema_cache["rev_data_cols"])
            if _schema_cache["rev_data_cols"]:
                _rds = "\n=== ACTUAL COLUMNS in revenue_data table (discovered at runtime) ===\n"
                _rds += f"  All columns: {', '.join(_schema_cache['rev_data_cols'])}\n"
                if _schema_cache["rev_rev_cols"]:
                    _rds += f"  Revenue columns: {', '.join(_schema_cache['rev_rev_cols'])}\n"
                    _total_ex = ' + '.join(f'COALESCE(r.{c},0)' for c in _schema_cache["rev_rev_cols"])
                    _rds += f"  Total revenue: SELECT r.site_id, ({_total_ex}) AS total_revenue FROM revenue_data r\n"
                if _schema_cache["rev_opex_cols"]:
                    _rds += f"  OPEX columns: {', '.join(_schema_cache['rev_opex_cols'])}\n"
                if _schema_cache["rev_has_subscribers"]:
                    _rds += "  Subscribers column: subscribers\n"
                _rds += "Use ONLY these column names — they are the real ones from the database.\n"
                SCHEMA_HINT += _rds
        except Exception:
            pass

    if 'core_kpi_data' in _available_tables:
        try:
            _cdc = _sql(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'core_kpi_data' "
                "AND column_name NOT IN ('id', 'uploaded_at') "
                "ORDER BY ordinal_position"
            )
            _schema_cache["core_data_cols"] = [r['column_name'] for r in _cdc]
            _core_skip = {'id', 'uploaded_at', 'site_id', 'date', 'zone', 'city', 'state'}
            _schema_cache["core_metric_cols"] = [c for c in _schema_cache["core_data_cols"] if c.lower() not in _core_skip]
            if _schema_cache["core_data_cols"]:
                _cds = "\n=== ACTUAL COLUMNS in core_kpi_data table (discovered at runtime) ===\n"
                _cds += f"  All columns: {', '.join(_schema_cache['core_data_cols'])}\n"
                if _schema_cache["core_metric_cols"]:
                    _cds += f"  Metric columns: {', '.join(_schema_cache['core_metric_cols'])}\n"
                    _cds += f"  Example: SELECT c.site_id, c.date::text, {', '.join('c.' + c for c in _schema_cache['core_metric_cols'][:4])} FROM core_kpi_data c\n"
                _cds += "Use ONLY these column names — they are the real ones from the database.\n"
                SCHEMA_HINT += _cds
        except Exception:
            pass

    if 'transport_kpi_data' in _available_tables:
        try:
            _tdc = _sql(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'transport_kpi_data' "
                "AND column_name NOT IN ('id', 'uploaded_at') "
                "ORDER BY ordinal_position"
            )
            _schema_cache["transport_data_cols"] = [r['column_name'] for r in _tdc]
            _transport_skip = {'id', 'uploaded_at', 'site_id', 'zone'}
            _schema_cache["transport_metric_cols"] = [c for c in _schema_cache["transport_data_cols"]
                                                       if c.lower() not in _transport_skip
                                                       and c.lower() != 'backhaul_type']
            if _schema_cache["transport_data_cols"]:
                _tds = "\n=== ACTUAL COLUMNS in transport_kpi_data table (discovered at runtime) ===\n"
                _tds += f"  All columns: {', '.join(_schema_cache['transport_data_cols'])}\n"
                if _schema_cache["transport_metric_cols"]:
                    _tds += f"  Metric columns: {', '.join(_schema_cache['transport_metric_cols'])}\n"
                _tds += "Use ONLY these column names — they are the real ones from the database.\n"
                SCHEMA_HINT += _tds
        except Exception:
            pass

    if 'network_issue_tickets' in _available_tables:
        try:
            _ntc = _sql(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'network_issue_tickets' "
                "AND column_name NOT IN ('id') "
                "ORDER BY ordinal_position"
            )
            _schema_cache["ticket_cols"] = [r['column_name'] for r in _ntc]
            if _schema_cache["ticket_cols"]:
                _nts = "\n=== ACTUAL COLUMNS in network_issue_tickets table (discovered at runtime) ===\n"
                _nts += f"  All columns: {', '.join(_schema_cache['ticket_cols'])}\n"
                _nts += "Use ONLY these column names — they are the real ones from the database.\n"
                SCHEMA_HINT += _nts
        except Exception:
            pass

    # ── Data freshness — discover actual max date in kpi_data ──
    # Many KPIs may lag behind CURRENT_DATE. Using CURRENT_DATE in WHERE clauses
    # would return 0 rows. We discover the actual max date and use it as reference.
    if 'kpi_data' in _available_tables:
        try:
            _date_info = _sql(
                "SELECT MAX(date)::text AS max_date FROM kpi_data WHERE data_level = 'site'"
            )
            if _date_info and _date_info[0].get('max_date'):
                _schema_cache["kpi_max_date"] = _date_info[0]['max_date']
                # Also get the "main" max date (excluding KPIs that may have future/synthetic data)
                _main_dates = _sql(
                    "SELECT MAX(date)::text AS max_date FROM kpi_data "
                    "WHERE data_level = 'site' AND kpi_name NOT IN ('Site Revenue','Site Users')"
                )
                if _main_dates and _main_dates[0].get('max_date'):
                    _schema_cache["kpi_max_date_recent"] = _main_dates[0]['max_date']
                else:
                    _schema_cache["kpi_max_date_recent"] = _schema_cache["kpi_max_date"]
            _LOG.info("[DISCOVERY] kpi_max_date=%s kpi_max_date_recent=%s",
                      _schema_cache["kpi_max_date"], _schema_cache["kpi_max_date_recent"])
            # Tell LLM about the actual date range so it doesn't use CURRENT_DATE blindly
            if _schema_cache["kpi_max_date_recent"]:
                _date_hint = f"\n=== DATA FRESHNESS (CRITICAL — read carefully) ===\n"
                _date_hint += f"The LATEST date with RAN KPI data is: {_schema_cache['kpi_max_date_recent']}\n"
                _date_hint += f"Today's date is: {datetime.now().strftime('%Y-%m-%d')}\n"
                _date_hint += "IMPORTANT: If these dates differ, the data is NOT up-to-date.\n"
                _date_hint += f"Use '{_schema_cache['kpi_max_date_recent']}'::date instead of CURRENT_DATE as the end date.\n"
                _date_hint += f"Example: AND k.date >= '{_schema_cache['kpi_max_date_recent']}'::date - INTERVAL '7 days' "
                _date_hint += f"AND k.date <= '{_schema_cache['kpi_max_date_recent']}'::date\n"
                _date_hint += "NEVER use CURRENT_DATE if it is more recent than the latest data date.\n"
                SCHEMA_HINT += _date_hint
        except Exception as e:
            _LOG.warning("[DISCOVERY] Failed to get kpi_data date range: %s", e)

    _schema_cache["populated"] = True
    _LOG.info("[DISCOVERY] rev_data_cols=%s rev_rev_cols=%s rev_opex=%s rev_subs=%s",
              _schema_cache["rev_data_cols"][:5], _schema_cache["rev_rev_cols"][:5],
              _schema_cache["rev_opex_cols"][:3], _schema_cache["rev_has_subscribers"])
    _LOG.info("[DISCOVERY] core_metric=%s transport_metric=%s ticket_cols=%d",
              _schema_cache["core_metric_cols"][:4], _schema_cache["transport_metric_cols"][:4],
              len(_schema_cache["ticket_cols"]))

    # ── Dynamic flexible_kpi_uploads core column discovery ──
    if 'flexible_kpi_uploads' in _available_tables:
        try:
            _core_eav_cols = _sql(
                "SELECT DISTINCT column_name, column_type "
                "FROM flexible_kpi_uploads WHERE kpi_type = 'core' "
                "ORDER BY column_name LIMIT 40"
            )
            if _core_eav_cols:
                _ccn = "\n=== ACTUAL COLUMN NAMES in flexible_kpi_uploads (kpi_type='core') ===\n"
                _ccn += "These are the REAL column_name values — use these EXACT values in queries.\n"
                _ccn += "IMPORTANT: ALWAYS use ILIKE (not LIKE or =) for column_name matching.\n"
                for _cc in _core_eav_cols:
                    _ccn += f"  '{_cc['column_name']}' ({_cc['column_type']})\n"
                _ccn += "Example: WHERE f.kpi_type = 'core' AND f.column_name ILIKE '%auth%'\n"
                SCHEMA_HINT += _ccn
        except Exception:
            pass

    # ── Dynamic site_kpi_summary discovery — tell LLM about ML categories ──
    # Auto-trigger ML pipeline if table is empty (first-time or after data refresh)
    _sks_row_count = 0
    if 'site_kpi_summary' in _available_tables:
        try:
            _sks_info = _sql(
                "SELECT COUNT(*) AS row_count, "
                "MIN(date)::text AS min_date, MAX(date)::text AS max_date, "
                "COUNT(DISTINCT site_id) AS site_count, "
                "COUNT(DISTINCT kpi_name) AS kpi_count, "
                "COUNT(*) FILTER (WHERE health_label = 'critical') AS critical_count, "
                "COUNT(*) FILTER (WHERE health_label = 'degraded') AS degraded_count, "
                "COUNT(*) FILTER (WHERE health_label = 'healthy') AS healthy_count, "
                "COUNT(*) FILTER (WHERE is_anomaly = true) AS anomaly_count "
                "FROM site_kpi_summary"
            )
            _sks_row_count = _sks_info[0].get('row_count', 0) if _sks_info else 0
            if _sks_row_count > 0:
                _si = _sks_info[0]
                _sks_hint = "\n=== ML-CATEGORIZED SUMMARY (site_kpi_summary) ===\n"
                _sks_hint += f"Total rows: {_si['row_count']} | Sites: {_si['site_count']} | KPIs: {_si['kpi_count']}\n"
                _sks_hint += f"Date range: {_si['min_date']} to {_si['max_date']}\n"
                _sks_hint += f"Health distribution: {_si['healthy_count']} healthy, {_si['degraded_count']} degraded, {_si['critical_count']} critical\n"
                _sks_hint += f"Anomalies detected: {_si['anomaly_count']}\n"
                _sks_hint += "PREFER this table for performance/health queries — it has ML-generated labels.\n"
                _sks_hint += f"For date references in this table, use '{_si['max_date']}'::date as the latest date.\n"
                SCHEMA_HINT += _sks_hint
                _LOG.info("[DISCOVERY] site_kpi_summary: %d rows, %s to %s, %d anomalies",
                          _si['row_count'], _si['min_date'], _si['max_date'], _si['anomaly_count'])
        except Exception as e:
            _LOG.debug("[DISCOVERY] site_kpi_summary check failed: %s", e)

    # Auto-run ML pipeline in background if summary table is empty
    if _sks_row_count == 0 and 'kpi_data' in _available_tables:
        try:
            from ml_pipeline import run_ml_pipeline_async, get_pipeline_status
            _ml_st = get_pipeline_status()
            if not _ml_st.get("running"):
                _LOG.info("[AUTO-ML] site_kpi_summary is empty — triggering ML pipeline in background")
                run_ml_pipeline_async(current_app._get_current_object())
        except Exception as e:
            _LOG.debug("[AUTO-ML] Failed to auto-trigger ML pipeline: %s", e)

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
12. NEVER filter by the `hour` column. All site-level data is stored with hour=0 — filtering by hour will always return 0 rows.
13. NEVER invent or guess site_ids. Use only site_ids the user explicitly stated or that appeared in prior conversation context.

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

        # 4. Replace CURRENT_DATE with actual max data date if data is stale
        _max_d = _schema_cache.get("kpi_max_date_recent")
        if _max_d and 'kpi_data' in sql.lower():
            sql = re.sub(r'\bCURRENT_DATE\b', f"'{_max_d}'::date", sql, flags=re.IGNORECASE)
            _LOG.info("[SQL-fix] replaced CURRENT_DATE with actual max date %s", _max_d)

        # 4b. Ensure date cap exists when date range is used
        _date_ref_rx = re.escape(f"'{_max_d}'::date") if _max_d else r"CURRENT_DATE"
        if re.search(_date_ref_rx + r"\s*-\s*INTERVAL", sql, re.IGNORECASE):
            if not re.search(r"date\s*<=\s*" + _date_ref_rx, sql, re.IGNORECASE):
                _cap = f"'{_max_d}'::date" if _max_d else "CURRENT_DATE"
                sql = re.sub(
                    r"(" + _date_ref_rx + r"\s*-\s*INTERVAL\s*'[^']+'\s*)",
                    r"\1AND k.date <= " + _cap + " ",
                    sql, count=1, flags=re.IGNORECASE,
                )
                _LOG.info("[SQL-fix] added missing date cap")

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

    # 2b. Non-KPI table queries (revenue, core, transport, tickets, site info)
    #     Rule-based handlers use dynamic schema discovery and are more reliable
    #     than LLM for these tables. Intercept BEFORE LLM to avoid wrong SQL.
    if not ai_result:
        _REVENUE_KW = {'revenue', 'income', 'earning', 'opex', 'expenditure', 'operating cost',
                        'subscriber', 'subscribers', 'customer count', 'arpu'}
        _CORE_KW = {'core kpi', 'core network', 'authentication', 'auth success', 'cpu utilization',
                     'attach success', 'attach rate', 'pdp bearer', 'pdp setup',
                     'auth sr', 'attach sr', 'pdp sr', 'cpu util'}
        _TRANSPORT_KW = {'transport', 'backhaul', 'microwave', 'fiber', 'jitter',
                          'link capacity', 'link utilization', 'backhaul latency',
                          'packet loss', 'tput efficiency', 'error rate'}
        _TICKET_KW = {'network issue', 'worst cell', 'ai ticket', 'network ticket',
                       'open issue', 'open ticket', 'issue ticket', 'fault'}
        _SITE_KW = {'site info', 'site detail', 'site status', 'which city',
                     'which zone', 'which state', 'site location', 'on air', 'off air',
                     'alarm', 'alarms', 'critical alarm', 'site technology'}
        _is_non_kpi = (any(w in _p_lower for w in _REVENUE_KW) or
                       any(w in _p_lower for w in _CORE_KW) or
                       any(w in _p_lower for w in _TRANSPORT_KW) or
                       any(w in _p_lower for w in _TICKET_KW) or
                       any(w in _p_lower for w in _SITE_KW))
        if _is_non_kpi:
            ai_result = _rule_based_query(prompt, time_filter, prev_context=None)
            provider = {"provider": "rule-based-intercept"}
            _LOG.info("Non-KPI query intercepted before LLM (revenue/core/transport/ticket/site)")

    # 3. LLM handles KPI and general queries.
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
        """Execute SQL with timeout and safety validation."""
        # Validate SQL safety before execution
        is_safe, result_or_error = _validate_sql(query)
        if not is_safe:
            _LOG.warning("[SQL-BLOCKED] %s — SQL: %s", result_or_error, query[:200])
            raise ValueError(f"SQL validation failed: {result_or_error}")
        query = _add_safety_limits(result_or_error)  # result_or_error is normalized SQL when safe

        # Check cache first
        ck = _cache_key(query)
        cached = _cache_get(ck)
        if cached is not None:
            _LOG.info("[CACHE-HIT] Returning cached result for: %s", query[:80])
            return cached

        with db.engine.connect() as conn:
            conn.execute(sa_text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))
            result = conn.execute(sa_text(query))
            cols = list(result.keys())
            rows = [dict(zip(cols, row)) for row in result.fetchall()]

        # Cache the result
        _cache_set(ck, rows)
        return rows

    def _serial(v):
        if v is None: return None
        if hasattr(v, "isoformat"): return v.isoformat()
        if isinstance(v, float) and math.isnan(v): return None
        try:    return float(v)
        except: return str(v)

    _emit_progress(_ws_sid, 'executing', 'Running query...')

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

        _emit_progress(_ws_sid, 'complete', 'Done')
        _elapsed = round(time.time() - _query_start, 2)
        _LOG.info("[COMPLETE] Multi-chart query in %.2fs | %d charts | provider=%s",
                  _elapsed, len(charts_out), provider.get("provider") if provider else "rule-based")
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
            "elapsed_seconds": _elapsed,
        })

    # ── SINGLE CHART: execute SQL normally ─────────────────────────────────────
    sql = ai_result.get("sql", "")
    if not sql or not sql.strip().upper().startswith("SELECT"):
        return jsonify({"error": "Could not generate a safe query"}), 400

    try:
        rows = _sql_with_timeout(sql, timeout_sec=15)
    except Exception as e:
        _LOG.warning("AI SQL execution failed: %s — SQL: %s", e, sql[:200])
        rows = []

    # ── 0-rows fallback: if LLM SQL returned nothing, try rule-based ──
    if not rows:
        try:
            fallback = _rule_based_query(prompt, time_filter)
            sql2 = fallback.get("sql", "")
            if sql2 and sql2 != sql and sql2.strip().upper().startswith("SELECT"):
                rows = _sql_with_timeout(sql2, timeout_sec=10)
                if rows:
                    ai_result.update(fallback)
                    sql = sql2
                    provider = {"provider": "rule-based-fallback"}
                    _LOG.info("Rule-based fallback returned %d rows", len(rows))
        except Exception:
            pass

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

    _emit_progress(_ws_sid, 'complete', 'Done')
    _elapsed = round(time.time() - _query_start, 2)
    _LOG.info("[COMPLETE] Single-chart query in %.2fs | %d rows | provider=%s | chart=%s",
              _elapsed, len(rows), provider.get("provider") if provider else "rule-based", resp_chart)
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
        "elapsed_seconds": _elapsed,
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
        date_clause = _kpi_date_clause(days)

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
        date_clause = _kpi_date_clause(days)
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
            date_clause = _kpi_date_clause(days)
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

    # ── Read discovered schema from module-level cache ──
    _rev_data_cols = _schema_cache.get("rev_data_cols", [])
    _rev_rev_cols = _schema_cache.get("rev_rev_cols", [])
    _rev_opex_cols = _schema_cache.get("rev_opex_cols", [])
    _rev_has_subscribers = _schema_cache.get("rev_has_subscribers", False)
    _core_data_cols = _schema_cache.get("core_data_cols", [])
    _core_metric_cols = _schema_cache.get("core_metric_cols", [])
    _transport_data_cols = _schema_cache.get("transport_data_cols", [])
    _transport_metric_cols = _schema_cache.get("transport_metric_cols", [])
    _ticket_cols = _schema_cache.get("ticket_cols", [])

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
    _SITE_INFO_WORDS = {'site info', 'site detail', 'site status', 'which city',
                        'which zone', 'which state', 'site location', 'on air', 'off air',
                        'alarm', 'alarms', 'critical alarm', 'site technology'}

    _is_revenue   = any(w in p for w in _REVENUE_WORDS)
    _is_core      = any(w in p for w in _CORE_WORDS)
    _is_transport = any(w in p for w in _TRANSPORT_WORDS)
    _is_ticket    = any(w in p for w in _TICKET_WORDS)
    _is_site_info = any(w in p for w in _SITE_INFO_WORDS)

    if _is_revenue or _is_core or _is_transport or _is_ticket or _is_site_info:
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
            _LOG.info("[REVENUE] has_rev_tbl=%s rev_cols=%s opex_cols=%s subs=%s use_eav=%s",
                      _has_rev_tbl, _rev_rev_cols[:3], _rev_opex_cols[:3], _rev_has_subscribers,
                      not _has_rev_tbl or (not _rev_rev_cols and not _rev_has_subscribers))

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

            # Use EAV if: revenue_data doesn't exist, OR it exists but discovery found no revenue columns
            _use_eav_revenue = not _has_rev_tbl or (not _rev_rev_cols and not _rev_has_subscribers)
            if _use_eav_revenue:
                # Fallback: use flexible_kpi_uploads EAV table
                # NOTE: ILIKE for case-insensitive matching — column_name casing depends on uploaded CSV headers
                # IMPORTANT: Use 'revenue_total' directly if it exists to avoid double-counting
                #   (SUM of revenue_jan + feb + mar + revenue_total = 2x actual)
                if any(w in p for w in ('subscriber', 'subscribers', 'customer count')):
                    return {
                        "sql": f"""SELECT f.site_id, f.num_value AS subscribers
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%subscriber%'
                              AND f.column_type = 'numeric' AND f.num_value IS NOT NULL {site_filter_f}
                            ORDER BY f.num_value {_rev_order} {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — Subscribers",
                        "x_axis": "site_id", "y_axes": ["subscribers"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by subscriber count.",
                    }
                elif any(w in p for w in ('opex', 'expenditure', 'operating cost')):
                    return {
                        "sql": f"""SELECT f.site_id, f.num_value AS total_opex
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%opex_total%'
                              AND f.column_type = 'numeric' {site_filter_f}
                            ORDER BY total_opex {_rev_order} {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — OPEX",
                        "x_axis": "site_id", "y_axes": ["total_opex"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Highest'} {_rev_n} sites by total OPEX.",
                    }
                elif 'arpu' in p:
                    return {
                        "sql": f"""SELECT rev.site_id,
                                   ROUND((CAST(rev.total_rev AS NUMERIC) / NULLIF(sub.subscribers, 0))::numeric, 2) AS arpu
                            FROM (
                                SELECT f.site_id, f.num_value AS total_rev
                                FROM flexible_kpi_uploads f
                                WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%revenue_total%'
                                  AND f.column_type = 'numeric' {site_filter_f}
                            ) rev
                            JOIN (
                                SELECT f.site_id, f.num_value AS subscribers
                                FROM flexible_kpi_uploads f
                                WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%subscriber%'
                                  AND f.column_type = 'numeric' AND f.num_value > 0
                            ) sub ON rev.site_id = sub.site_id
                            ORDER BY arpu {_rev_order} {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — ARPU",
                        "x_axis": "site_id", "y_axes": ["arpu"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by ARPU.",
                    }
                else:
                    # Default: use revenue_total if it exists, else SUM monthly columns (exclude total to avoid double-count)
                    return {
                        "sql": f"""SELECT f.site_id, f.num_value AS total_revenue
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%revenue_total%'
                              AND f.column_type = 'numeric' {site_filter_f}
                            ORDER BY total_revenue {_rev_order} NULLS LAST {_limit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} Sites by Revenue",
                        "x_axis": "site_id", "y_axes": ["total_revenue"],
                        "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by total revenue.",
                    }

            # revenue_data table exists — build SQL from ACTUAL discovered columns
            # (no hardcoded column names — adapts to whatever schema the table has)
            _total_rev_expr = " + ".join(f"COALESCE(r.{c},0)" for c in _rev_rev_cols) if _rev_rev_cols else "0"
            _total_opex_expr = " + ".join(f"COALESCE(r.{c},0)" for c in _rev_opex_cols) if _rev_opex_cols else "0"
            _rev_cols_select = ", ".join(f"r.{c}" for c in _rev_rev_cols) if _rev_rev_cols else "NULL AS no_rev_cols"
            _opex_cols_select = ", ".join(f"r.{c}" for c in _rev_opex_cols) if _rev_opex_cols else "NULL AS no_opex_cols"

            if _rev_has_subscribers and any(w in p for w in ('subscriber', 'subscribers', 'customer count')):
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
            elif _rev_opex_cols and any(w in p for w in ('opex', 'expenditure', 'operating cost')):
                return {
                    "sql": f"""SELECT r.site_id, {_opex_cols_select},
                               ({_total_opex_expr}) AS total_opex
                        FROM revenue_data r
                        WHERE r.site_id IS NOT NULL {site_filter_r}
                        ORDER BY total_opex {_rev_order} {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — OPEX",
                    "x_axis": "site_id", "y_axes": _rev_opex_cols[:6],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Highest'} {_rev_n} sites by total OPEX.",
                }
            elif 'arpu' in p and _rev_rev_cols and _rev_has_subscribers:
                return {
                    "sql": f"""SELECT r.site_id,
                               ROUND((({_total_rev_expr})::numeric / NULLIF(r.subscribers, 0))::numeric, 2) AS arpu
                        FROM revenue_data r
                        WHERE r.subscribers > 0 {site_filter_r}
                        ORDER BY arpu {_rev_order} {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} — ARPU",
                    "x_axis": "site_id", "y_axes": ["arpu"],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by ARPU.",
                }
            elif site_ids and is_trend and _rev_rev_cols:
                # Revenue trend — unpivot monthly columns dynamically
                site_filter_lat = " OR ".join(f"r.site_id = '{s}'" for s in site_ids[:5])
                # Build LATERAL VALUES from discovered rev + opex columns, paired by index
                _lat_rows = []
                for i, rc in enumerate(_rev_rev_cols):
                    _label = rc.replace('rev_', '').replace('_', ' ').title()
                    _oc = _rev_opex_cols[i] if i < len(_rev_opex_cols) else "NULL"
                    _oc_ref = f"r.{_oc}" if _oc != "NULL" else "NULL"
                    _lat_rows.append(f"('{_label}', {i+1}, r.{rc}, {_oc_ref})")
                if _lat_rows:
                    _lat_values = ",\n                            ".join(_lat_rows)
                    return {
                        "sql": f"""SELECT r.site_id, t.month_name, t.revenue, t.opex
                            FROM revenue_data r
                            CROSS JOIN LATERAL (VALUES
                            {_lat_values}
                            ) AS t(month_name, month_ord, revenue, opex)
                            WHERE ({site_filter_lat})
                            ORDER BY r.site_id, t.month_ord""",
                        "query_type": "composed", "chart_type": "composed",
                        "title": f"Revenue Trend — {', '.join(site_ids[:3])}",
                        "x_axis": "month_name", "y_axes": ["revenue", "opex"],
                        "response": f"Monthly revenue & OPEX for {', '.join(site_ids[:3])}.",
                    }
            # Default: show revenue with all discovered columns
            if _rev_rev_cols:
                _sub_col = "r.subscribers, " if _rev_has_subscribers else ""
                _meta_cols = []
                for _mc in ('zone', 'technology'):
                    if _mc in _rev_data_cols:
                        _meta_cols.append(f"r.{_mc}")
                _meta_select = (", " + ", ".join(_meta_cols)) if _meta_cols else ""
                return {
                    "sql": f"""SELECT r.site_id, {_sub_col}{_rev_cols_select},
                               ({_total_rev_expr}) AS total_revenue{_meta_select}
                        FROM revenue_data r
                        WHERE r.site_id IS NOT NULL {site_filter_r}
                        ORDER BY total_revenue {_rev_order} NULLS LAST {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} Sites by Revenue",
                    "x_axis": "site_id", "y_axes": ["total_revenue"],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by total revenue.",
                }
            else:
                # revenue_data exists but no recognized rev columns — fall back to EAV
                return {
                    "sql": f"""SELECT f.site_id, f.num_value AS total_revenue
                        FROM flexible_kpi_uploads f
                        WHERE f.kpi_type = 'revenue' AND f.column_name ILIKE '%revenue_total%'
                          AND f.column_type = 'numeric' {site_filter_f}
                        ORDER BY total_revenue {_rev_order} NULLS LAST {_limit}""",
                    "query_type": "bar", "chart_type": "bar",
                    "title": f"{'Bottom' if _rev_order=='ASC' else 'Top'} {_rev_n} Sites by Revenue",
                    "x_axis": "site_id", "y_axes": ["total_revenue"],
                    "response": f"{'Lowest' if _rev_order=='ASC' else 'Top'} {_rev_n} sites by total revenue (from uploaded data).",
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
            if _tbl_exists("core_kpi_data") and _core_metric_cols:
                # Build SQL from ACTUAL discovered columns (no hardcoded names)
                _core_select = ", ".join(f"c.{c}" for c in _core_metric_cols)
                _core_avg_select = ", ".join(f"AVG(c.{c}) AS {c}" for c in _core_metric_cols)
                _core_sort_col = _core_metric_cols[0]  # first metric for sorting
                date_clause = _kpi_date_clause(days, alias='c') if days else ""
                if site_ids:
                    _has_date = 'date' in [c.lower() for c in _core_data_cols]
                    if _has_date:
                        return {
                            "sql": f"""SELECT c.date::text AS date, {_core_select}
                                FROM core_kpi_data c
                                WHERE LOWER(c.site_id) = LOWER('{site_ids[0]}') {date_clause}
                                ORDER BY c.date""",
                            "query_type": "composed", "chart_type": "composed",
                            "title": f"Core KPIs — {site_ids[0]}",
                            "x_axis": "date", "y_axes": _core_metric_cols[:6],
                            "response": f"Showing core network KPIs for {site_ids[0]}.",
                        }
                    else:
                        return {
                            "sql": f"""SELECT c.site_id, {_core_select}
                                FROM core_kpi_data c
                                WHERE LOWER(c.site_id) = LOWER('{site_ids[0]}') {date_clause}""",
                            "query_type": "bar", "chart_type": "bar",
                            "title": f"Core KPIs — {site_ids[0]}",
                            "x_axis": "site_id", "y_axes": _core_metric_cols[:6],
                            "response": f"Showing core network KPIs for {site_ids[0]}.",
                        }
                else:
                    _core_sort = f"{_core_sort_col} {'ASC' if _horder == 'ASC' else 'DESC'}"
                    return {
                        "sql": f"""SELECT c.site_id, {_core_avg_select}
                            FROM core_kpi_data c
                            WHERE c.site_id IS NOT NULL {date_clause}
                            GROUP BY c.site_id ORDER BY {_core_sort} NULLS LAST {_hlimit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _horder=='ASC' else 'Top'} {_hn} — Core KPIs",
                        "x_axis": "site_id", "y_axes": _core_metric_cols[:6],
                        "response": f"{'Worst' if _horder=='ASC' else 'Top'} {_hn} sites by core KPIs.",
                    }
            else:
                # Fallback to flexible_kpi_uploads EAV
                # Core data uses kpi_name for metric type (e.g., 'CPU Utilization')
                # and column_name for dates. Pivot by kpi_name to show per-site metrics.
                if site_ids:
                    return {
                        "sql": f"""SELECT f.site_id, f.kpi_name, AVG(f.num_value) AS avg_value
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'core' AND f.column_type = 'numeric'
                              {site_filter_f}
                            GROUP BY f.site_id, f.kpi_name
                            ORDER BY f.site_id, f.kpi_name""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": "Core KPIs — " + ', '.join(site_ids[:2]),
                        "x_axis": "kpi_name", "y_axes": ["avg_value"],
                        "response": f"Core network KPIs for {', '.join(site_ids[:2])}.",
                    }
                else:
                    return {
                        "sql": f"""SELECT f.site_id,
                               AVG(CASE WHEN f.kpi_name ILIKE '%auth%' THEN f.num_value END) AS auth_sr,
                               AVG(CASE WHEN f.kpi_name ILIKE '%cpu%' THEN f.num_value END) AS cpu_util,
                               AVG(CASE WHEN f.kpi_name ILIKE '%attach%' THEN f.num_value END) AS attach_sr,
                               AVG(CASE WHEN f.kpi_name ILIKE '%pdp%' THEN f.num_value END) AS pdp_sr
                            FROM flexible_kpi_uploads f
                            WHERE f.kpi_type = 'core' AND f.column_type = 'numeric'
                            GROUP BY f.site_id
                            ORDER BY auth_sr {'ASC' if _horder == 'ASC' else 'DESC'} NULLS LAST {_hlimit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"{'Bottom' if _horder=='ASC' else 'Top'} {_hn} — Core KPIs",
                        "x_axis": "site_id", "y_axes": ["auth_sr", "cpu_util", "attach_sr", "pdp_sr"],
                        "response": f"{'Worst' if _horder=='ASC' else 'Top'} {_hn} sites by core KPIs.",
                    }

        # ── Transport/backhaul queries ──
        if _is_transport:
            if _tbl_exists("transport_kpi_data") and _transport_data_cols:
                # Build SQL from ACTUAL discovered columns (no hardcoded names)
                _all_t_select = ", ".join(f"t.{c}" for c in _transport_data_cols)
                # Dynamic sort: find a matching column by keyword
                _trans_sort = None
                for _kw, _col_hint in [('packet loss', 'packet_loss'), ('loss', 'packet_loss'),
                                        ('latency', 'latency'), ('delay', 'latency'),
                                        ('jitter', 'jitter'), ('error', 'error'),
                                        ('utilization', 'util'), ('capacity', 'capacity')]:
                    if _kw in p:
                        _trans_sort = next((c for c in _transport_metric_cols if _col_hint in c.lower()), None)
                        if _trans_sort:
                            break
                if not _trans_sort:
                    _trans_sort = next((c for c in _transport_metric_cols if 'packet_loss' in c.lower()),
                                      _transport_metric_cols[0] if _transport_metric_cols else 'site_id')
                # Determine y_axes from discovered metric columns
                _t_y_axes = _transport_metric_cols[:5] if _transport_metric_cols else ['site_id']
                if site_ids:
                    return {
                        "sql": f"""SELECT {_all_t_select}
                            FROM transport_kpi_data t
                            WHERE t.site_id IS NOT NULL {site_filter_t}
                            ORDER BY t.site_id""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Transport KPIs — {', '.join(site_ids[:2])}",
                        "x_axis": "site_id", "y_axes": _t_y_axes,
                        "response": f"Transport/backhaul KPIs for {', '.join(site_ids[:2])}.",
                    }
                else:
                    return {
                        "sql": f"""SELECT {_all_t_select}
                            FROM transport_kpi_data t
                            WHERE t.site_id IS NOT NULL
                            ORDER BY {_trans_sort} DESC NULLS LAST {_hlimit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Top {_hn} — Transport Issues (by {_trans_sort})",
                        "x_axis": "site_id", "y_axes": _t_y_axes,
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
            if _tbl_exists("network_issue_tickets") and _ticket_cols:
                # Build SQL from ACTUAL discovered columns (no hardcoded names)
                _skip_ticket = {'id', 'updated_at'}
                _ticket_select_cols = [c for c in _ticket_cols if c.lower() not in _skip_ticket]
                # Cast timestamp columns to text for JSON serialization
                _ticket_select_parts = []
                for c in _ticket_select_cols:
                    if 'created_at' in c.lower() or 'deadline' in c.lower() or 'updated_at' in c.lower():
                        _ticket_select_parts.append(f"n.{c}::text AS {c}")
                    else:
                        _ticket_select_parts.append(f"n.{c}")
                _ticket_select = ", ".join(_ticket_select_parts)
                # Determine y_axes from numeric-looking columns
                _ticket_y_candidates = [c for c in _ticket_cols if any(k in c.lower() for k in
                    ('drop_rate', 'cssr', 'tput', 'violations', 'priority_score', 'sla', 'rrc', 'revenue'))]
                _ticket_y = _ticket_y_candidates[:4] if _ticket_y_candidates else ['site_id']
                # Check if status column exists
                _has_status = 'status' in [c.lower() for c in _ticket_cols]
                _status_filter = "WHERE n.status IN ('open','in_progress')" if _has_status else "WHERE 1=1"
                # Check if priority_score exists for ordering
                _has_pscore = 'priority_score' in [c.lower() for c in _ticket_cols]
                _ticket_order = "n.priority_score DESC" if _has_pscore else "n.site_id"
                if site_ids:
                    return {
                        "sql": f"""SELECT {_ticket_select}
                            FROM network_issue_tickets n
                            {_status_filter} {site_filter_n}
                            ORDER BY {_ticket_order}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Network Tickets — {', '.join(site_ids[:2])}",
                        "x_axis": "site_id", "y_axes": _ticket_y,
                        "response": f"Network issue tickets for {', '.join(site_ids[:2])}.",
                    }
                else:
                    return {
                        "sql": f"""SELECT {_ticket_select}
                            FROM network_issue_tickets n
                            {_status_filter}
                            ORDER BY {_ticket_order} {_hlimit}""",
                        "query_type": "bar", "chart_type": "bar",
                        "title": f"Top {_hn} Network Issue Tickets",
                        "x_axis": "site_id", "y_axes": _ticket_y,
                        "response": f"Showing {_hn} highest-priority open tickets.",
                    }
            else:
                return {
                    "sql": "", "query_type": "bar", "chart_type": "bar",
                    "title": "Network Tickets", "x_axis": "site_id", "y_axes": [],
                    "response": "No network issue tickets found. The AI ticket system generates tickets during the daily 08:00 IST scan.",
                }

        # ── Site info / alarms queries (telecom_sites table) ──
        if _is_site_info:
            if _tbl_exists("telecom_sites"):
                _is_alarm = any(w in p for w in ('alarm', 'alarms', 'critical alarm'))
                if _is_alarm:
                    if site_ids:
                        return {
                            "sql": f"""SELECT ts.site_id, ts.zone, ts.city, ts.state, ts.technology,
                                       ts.site_status, ts.alarms
                                FROM telecom_sites ts
                                WHERE ts.alarms IS NOT NULL AND ts.alarms != '0'
                                  AND ts.site_id IN ({", ".join(f"'{s}'" for s in site_ids[:4])})
                                GROUP BY ts.site_id, ts.zone, ts.city, ts.state, ts.technology, ts.site_status, ts.alarms
                                ORDER BY ts.alarms DESC""",
                            "query_type": "bar", "chart_type": "bar",
                            "title": f"Alarms — {', '.join(site_ids[:2])}",
                            "x_axis": "site_id", "y_axes": ["alarms"],
                            "response": f"Alarm information for {', '.join(site_ids[:2])}.",
                        }
                    else:
                        return {
                            "sql": f"""SELECT ts.site_id, ts.zone, ts.city, ts.technology,
                                       ts.site_status, ts.alarms
                                FROM telecom_sites ts
                                WHERE ts.alarms IS NOT NULL AND ts.alarms != '0'
                                GROUP BY ts.site_id, ts.zone, ts.city, ts.technology, ts.site_status, ts.alarms
                                ORDER BY ts.alarms DESC {_hlimit}""",
                            "query_type": "bar", "chart_type": "bar",
                            "title": f"Top {_hn} Sites with Alarms",
                            "x_axis": "site_id", "y_axes": ["alarms"],
                            "response": f"Showing {_hn} sites with highest alarm counts.",
                        }
                else:
                    # General site info query
                    if site_ids:
                        return {
                            "sql": f"""SELECT DISTINCT ts.site_id, ts.zone, ts.city, ts.state,
                                       ts.technology, ts.site_status, ts.latitude, ts.longitude, ts.alarms
                                FROM telecom_sites ts
                                WHERE ts.site_id IN ({", ".join(f"'{s}'" for s in site_ids[:4])})
                                ORDER BY ts.site_id""",
                            "query_type": "bar", "chart_type": "bar",
                            "title": f"Site Info — {', '.join(site_ids[:2])}",
                            "x_axis": "site_id", "y_axes": ["zone", "city", "technology"],
                            "response": f"Site details for {', '.join(site_ids[:2])}.",
                        }
                    else:
                        # Show sites by status or general overview
                        _is_off = any(w in p for w in ('off air', 'down', 'inactive'))
                        _status_filt = "AND ts.site_status = 'off_air'" if _is_off else ""
                        return {
                            "sql": f"""SELECT ts.site_id, ts.zone, ts.city, ts.state,
                                       ts.technology, ts.site_status, ts.alarms
                                FROM telecom_sites ts
                                WHERE ts.site_id IS NOT NULL {_status_filt}
                                GROUP BY ts.site_id, ts.zone, ts.city, ts.state, ts.technology, ts.site_status, ts.alarms
                                ORDER BY ts.site_id {_hlimit}""",
                            "query_type": "bar", "chart_type": "bar",
                            "title": f"{'Off-Air' if _is_off else 'All'} Sites Overview",
                            "x_axis": "site_id", "y_axes": ["zone", "technology", "site_status"],
                            "response": f"Showing {'off-air' if _is_off else 'top'} sites.",
                        }
            else:
                return {
                    "sql": "", "query_type": "bar", "chart_type": "bar",
                    "title": "Site Info", "x_axis": "site_id", "y_axes": [],
                    "response": "No telecom_sites data available. Please upload site data first.",
                }

    # ── FIX: Multiple sites + trend → multi_chart (one chart per site, each with ALL KPIs) ──
    # Previously this incorrectly paired kpi[i] with site[i].
    if len(site_ids) >= 2 and is_trend:
        date_clause = _kpi_date_clause(days)
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
            date_clause = _kpi_date_clause(days)
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
            date_clause = _kpi_date_clause(days)
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
        date_clause = _kpi_date_clause(days)
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