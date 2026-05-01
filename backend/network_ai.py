"""
network_ai.py  — v7 (2026-05-01)
==================================
Flask Blueprint: Network AI Chat & Query Engine

Architecture:
  User NL Query
    → Query Router (analytical / informational)
    → Dynamic schema discovery (KPI names, tables, columns from live DB)
    → LLM SQL Generation with schema-aware prompt (no hardcoded KPI names)
    → SQL Validation & Safety Guards
    → Parallel Query Execution (PostgreSQL, timeout, cache)
    → Structured JSON + Visualization Output
    → WebSocket streaming for intermediate states

Features:
  • LLM-first: no hardcoded KPI names, no rule-based interceptors
  • Dynamic KPI discovery from live database at query time
  • Materialized views as primary query targets (10-100x faster)
  • No SELECT *, no DROP/DELETE/UPDATE — strict read-only safety
  • In-memory LRU cache for schema discovery and query results
  • WebSocket streaming: understanding → generating → executing → complete
  • Database optimization: indexes, materialized views
  • Parallel multi-chart SQL execution (ThreadPoolExecutor)
  • Works with any uploaded dataset without code changes

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
NETWORK_AI_VERSION = "2026-05-01-v7"

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
    'site_kpi_summary',       # ML-categorized pre-aggregated KPI data
    'mv_daily_site_kpi',      # pre-aggregated daily site KPI summary (fast)
    'mv_zone_kpi_summary',    # pre-aggregated zone-level KPI summary (fast)
    'mv_zone_daily_kpi',      # pre-aggregated zone+date daily KPI summary (fast)
    'kpi_data_merged',        # materialized view with site+cell fallback
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

        # Zone-level daily KPI with date — enables zone trend queries
        """CREATE MATERIALIZED VIEW IF NOT EXISTS mv_zone_daily_kpi AS
        SELECT COALESCE(ts.zone, 'Unknown') AS zone,
               k.kpi_name, k.date,
               AVG(k.avg_value) AS avg_val,
               COUNT(DISTINCT k.site_id) AS site_count
        FROM mv_daily_site_kpi k
        LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
        GROUP BY COALESCE(ts.zone, 'Unknown'), k.kpi_name, k.date""",
    ]

    _mv_indexes = [
        # UNIQUE index required for REFRESH MATERIALIZED VIEW CONCURRENTLY (non-blocking).
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_site_uniq ON mv_daily_site_kpi(site_id, kpi_name, date)",
        "CREATE INDEX IF NOT EXISTS idx_mv_daily_site ON mv_daily_site_kpi(site_id, kpi_name, date)",
        "CREATE INDEX IF NOT EXISTS idx_mv_zone_kpi ON mv_zone_kpi_summary(zone, kpi_name)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_zone_daily_uniq ON mv_zone_daily_kpi(zone, kpi_name, date)",
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
    """Refresh all AI materialized views. Call periodically or after data uploads."""
    for _mv in ["mv_daily_site_kpi", "mv_zone_kpi_summary", "mv_zone_daily_kpi"]:
        try:
            with db.engine.connect() as conn:
                conn.execute(sa_text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {_mv}"))
                conn.commit()
        except Exception:
            try:
                with db.engine.connect() as conn:
                    conn.execute(sa_text(f"REFRESH MATERIALIZED VIEW {_mv}"))
                    conn.commit()
            except Exception as _e:
                _LOG.debug("MV refresh skip %s: %s", _mv, str(_e)[:80])
    _LOG.info("Materialized views refreshed successfully.")

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
    """Build a date range WHERE clause using the actual data max date.
    Always applies a lower bound — defaults to 90 days when the user
    did not specify a period, preventing full-history table scans.
    """
    _ref = _kpi_date_ref()
    _days = days_val or 90  # default window prevents unbounded scans
    return f"AND {alias}.date >= {_ref} - INTERVAL '{_days} days' AND {alias}.date <= {_ref}"


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

    # ── Dynamic KPI name discovery (no hardcoded values) ──────────────────────
    # Discovers actual KPI names from the live database so the chatbot works
    # with any uploaded dataset without requiring code changes.
    _kpi_names_list = []
    try:
        _kn = _sql(
            "SELECT DISTINCT kpi_name FROM mv_daily_site_kpi "
            "ORDER BY kpi_name LIMIT 100"
        )
        _kpi_names_list = [r['kpi_name'] for r in _kn if r.get('kpi_name')]
    except Exception:
        pass
    if not _kpi_names_list:
        try:
            _kn = _sql(
                "SELECT DISTINCT kpi_name FROM kpi_data "
                "WHERE data_level = 'site' AND kpi_name IS NOT NULL "
                "ORDER BY kpi_name LIMIT 100"
            )
            _kpi_names_list = [r['kpi_name'] for r in _kn if r.get('kpi_name')]
        except Exception:
            pass

    SCHEMA_HINT = """
=== FAST MATERIALIZED VIEWS — USE THESE AS PRIMARY QUERY TARGETS ===
These views are pre-aggregated and 10-100x faster than querying raw kpi_data.

mv_daily_site_kpi(site_id TEXT, kpi_name TEXT, date DATE,
                  avg_value FLOAT, min_value FLOAT, max_value FLOAT, sample_count INT)
  Daily KPI averages per site. Use for: trends, top-N, comparisons, threshold queries.
  JOIN telecom_sites ts ON LOWER(mv.site_id) = LOWER(ts.site_id) for zone/city/location.
  Example: SELECT site_id, avg_value FROM mv_daily_site_kpi
           WHERE kpi_name = '...' AND date >= CURRENT_DATE - INTERVAL '30 days'
           ORDER BY avg_value DESC LIMIT 20

mv_zone_daily_kpi(zone TEXT, kpi_name TEXT, date DATE, avg_val FLOAT, site_count INT)
  Daily zone-level KPI averages. Use for zone trends and comparisons over time.
  No JOIN needed for zone questions.
  Example: SELECT zone, AVG(avg_val) FROM mv_zone_daily_kpi
           WHERE kpi_name = '...' AND date >= CURRENT_DATE - INTERVAL '30 days'
           GROUP BY zone ORDER BY AVG(avg_val) DESC LIMIT 20

mv_zone_kpi_summary(zone TEXT, kpi_name TEXT,
                    avg_value FLOAT, min_value FLOAT, max_value FLOAT, site_count INT)
  All-time zone aggregate (no date column). Use for zone snapshots when date is not needed.

MANDATORY QUERY RULES (violating these causes slow or empty results):
1. ALWAYS prefer mv_daily_site_kpi over kpi_data for any site-level KPI query.
2. ALWAYS add a date filter: date >= CURRENT_DATE - INTERVAL '30 days' (or shorter).
3. ALWAYS add LIMIT 1000 to every query.
4. Only use raw kpi_data for HOURLY data (WHERE hour = X) or cell-level data.
5. For zone trends over time: use mv_zone_daily_kpi.
   For zone snapshot (no time): use mv_zone_kpi_summary.

======================================================================
Tables (FALLBACK — use kpi_data only when MVs unavailable or hourly/cell data needed):
1. kpi_data(id, site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
   - data_level = 'site' for site-level, 'cell' for cell-level
   - kpi_name is CASE-SENSITIVE. Exact strings are in the "RAN KPI Names" section (discovered at runtime).
   - For fuzzy matching use ILIKE: WHERE kpi_name ILIKE '%drop%'

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

=== Natural Language → Data Source Mapping ===
For RAN KPI queries (any network performance metric):
  → Use mv_daily_site_kpi (preferred for site/daily data) or kpi_data (for hourly/cell data).
  → Match kpi_name with ILIKE for concept search: WHERE kpi_name ILIKE '%drop%'
  → For exact names check the "RAN KPI Names" section (discovered from live database above).
User asks about revenue / income / OPEX / subscribers / ARPU:
  → revenue_data (flat table) OR flexible_kpi_uploads WHERE kpi_type = 'revenue'
User asks about core network / authentication / CPU / attach / PDP bearer:
  → core_kpi_data (flat) OR flexible_kpi_uploads WHERE kpi_type = 'core'
User asks about transport / backhaul / microwave / jitter / packet loss / link utilization:
  → transport_kpi_data table
User asks about network issues / worst cells / AI tickets / open tickets:
  → network_issue_tickets table
User asks about site info / location / zone / city / state / status / alarms:
  → telecom_sites table
Time references:
  "last 7 days" / "last week" → INTERVAL '7 days'
  "last month" / "30 days"    → INTERVAL '30 days'
  "last year"                 → INTERVAL '365 days'
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

    # ── Inject discovered KPI names (replaces hardcoded list) ─────────────────
    if _kpi_names_list:
        _kpi_section = "\n=== RAN KPI Names (discovered from live database — EXACT strings) ===\n"
        _kpi_section += "These are the ACTUAL kpi_name values in your database, discovered at runtime.\n"
        _kpi_section += "kpi_name is CASE-SENSITIVE. Copy exactly, or use ILIKE for fuzzy: kpi_name ILIKE '%drop%'\n"
        for _k in _kpi_names_list:
            _kpi_section += f"  '{_k}'\n"
        SCHEMA_HINT += _kpi_section

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

    # ── Materialized view availability — query pg_matviews (MVs don't appear in information_schema) ──
    try:
        _mv_status_rows = _sql(
            "SELECT matviewname FROM pg_matviews "
            "WHERE schemaname = 'public' AND matviewname IN "
            "('mv_daily_site_kpi','mv_zone_daily_kpi','mv_zone_kpi_summary','kpi_data_merged')"
        )
        _mv_ready = {r['matviewname'] for r in _mv_status_rows}
    except Exception:
        _mv_ready = set()
    _mv_note = "\n=== MATERIALIZED VIEW STATUS (fast pre-aggregated tables) ===\n"
    for _mvn in ['mv_daily_site_kpi', 'mv_zone_daily_kpi', 'mv_zone_kpi_summary', 'kpi_data_merged']:
        if _mvn in _mv_ready:
            _mv_note += f"  {_mvn}: READY — use this\n"
        else:
            _mv_note += f"  {_mvn}: NOT YET CREATED — fall back to kpi_data\n"
    SCHEMA_HINT += _mv_note

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

    # ── KPI value ranges — inject actual min/avg/median/max so LLM knows realistic thresholds ──
    # Without this, the LLM has no idea if "< 8 Mbps" is a tight or loose filter,
    # leading to threshold queries that confidently return 0 rows with no explanation.
    if 'kpi_data' in _available_tables:
        try:
            _kpi_ranges = _sql(
                """SELECT kpi_name,
                          ROUND(MIN(value)::numeric, 2)    AS min_val,
                          ROUND(AVG(value)::numeric, 2)    AS avg_val,
                          ROUND(MAX(value)::numeric, 2)    AS max_val,
                          ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value)::numeric, 2) AS p25,
                          ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value)::numeric, 2) AS p75,
                          COUNT(DISTINCT site_id) AS site_count
                   FROM kpi_data
                   WHERE data_level = 'site' AND value IS NOT NULL
                   GROUP BY kpi_name
                   ORDER BY kpi_name"""
            )
            if _kpi_ranges:
                _schema_cache["kpi_ranges"] = {r["kpi_name"]: r for r in _kpi_ranges}
                _rng_hint = "\n=== KPI VALUE RANGES (actual data — use for threshold calibration) ===\n"
                _rng_hint += "Format: kpi_name → min | p25 | avg | p75 | max | sites\n"
                for r in _kpi_ranges:
                    _rng_hint += (
                        f"  '{r['kpi_name']}': "
                        f"min={r['min_val']} | p25={r['p25']} | avg={r['avg_val']} "
                        f"| p75={r['p75']} | max={r['max_val']} | {r['site_count']} sites\n"
                    )
                _rng_hint += (
                    "CRITICAL: When a threshold query returns 0 rows, it means NO site breaches "
                    "that threshold — check ranges above and inform the user.\n"
                )
                SCHEMA_HINT += _rng_hint
                _LOG.info("[DISCOVERY] KPI ranges loaded for %d KPIs", len(_kpi_ranges))
        except Exception as e:
            _LOG.warning("[DISCOVERY] KPI ranges query failed (non-fatal): %s", e)

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

6. VAGUE / AMBIGUOUS — "yes", "ok", "show more", short prompts with no new site/KPI
   → Re-run the previous SQL with the same parameters.

**IMPORTANT RULES for follow-ups:**
- ALWAYS inherit site_id from previous SQL if the user doesn't mention a new one.
- ALWAYS inherit the time range (INTERVAL) from previous SQL if not specified.
- ALWAYS inherit kpi_name(s) from previous SQL if the user doesn't mention a new KPI.
- If the current prompt is completely self-contained (has site ID + KPI + time range), treat it as a FRESH query.
- A prompt like "i want to see for site id X" with NO new KPI mentioned = SITE SWITCH → inherit all KPIs.

═══════════════════════════════════════════════════════════
CRITICAL RULE #0b — THRESHOLD-BASED WORST SITE QUERIES:
═══════════════════════════════════════════════════════════

When the user asks for "worst N sites where KPI1 > X OR KPI2 < Y OR KPI3 < Z":
→ NEVER generate 3 separate SQL queries (multi_chart). Generate ONE SQL query.
→ ALWAYS use PIVOT (CASE WHEN) approach to compute all KPI averages in one query.
→ Use HAVING clause with OR conditions to filter sites meeting ANY threshold.
→ Add a "violations" count column to rank sites by how many thresholds they breach.

CORRECT approach for "show 5 worst sites where E-RAB Drop > 1.5% OR CSSR < 98.5% OR DL Usr Tput < 8":
```sql
SELECT k.site_id,
       AVG(CASE WHEN k.kpi_name = 'E-RAB Call Drop Rate_1' THEN k.value END) AS drop_rate,
       AVG(CASE WHEN k.kpi_name = 'LTE Call Setup Success Rate' THEN k.value END) AS cssr,
       AVG(CASE WHEN k.kpi_name = 'LTE DL - Usr Ave Throughput' THEN k.value END) AS dl_usr_tput,
       (CASE WHEN AVG(CASE WHEN k.kpi_name = 'E-RAB Call Drop Rate_1' THEN k.value END) > 1.5 THEN 1 ELSE 0 END +
        CASE WHEN AVG(CASE WHEN k.kpi_name = 'LTE Call Setup Success Rate' THEN k.value END) < 98.5 THEN 1 ELSE 0 END +
        CASE WHEN AVG(CASE WHEN k.kpi_name = 'LTE DL - Usr Ave Throughput' THEN k.value END) < 8 THEN 1 ELSE 0 END
       ) AS violations
FROM kpi_data k
WHERE k.data_level = 'site' AND k.value IS NOT NULL
  AND k.kpi_name IN ('E-RAB Call Drop Rate_1', 'LTE Call Setup Success Rate', 'LTE DL - Usr Ave Throughput')
GROUP BY k.site_id
HAVING AVG(CASE WHEN k.kpi_name = 'E-RAB Call Drop Rate_1' THEN k.value END) > 1.5
    OR AVG(CASE WHEN k.kpi_name = 'LTE Call Setup Success Rate' THEN k.value END) < 98.5
    OR AVG(CASE WHEN k.kpi_name = 'LTE DL - Usr Ave Throughput' THEN k.value END) < 8
ORDER BY violations DESC, drop_rate DESC NULLS LAST
LIMIT 5
```
chart_type: "bar", x_axis: "site_id", y_axes: ["drop_rate","cssr","dl_usr_tput","violations"]


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

Example SQL for one site with two KPIs:
SELECT k.date::text AS date, k.site_id, AVG(k.value) AS value, 'E-RAB Call Drop Rate_1' AS kpi_name
FROM kpi_data k WHERE k.kpi_name = 'E-RAB Call Drop Rate_1' AND k.site_id = 'SITE_A'
  AND k.data_level='site' AND k.value IS NOT NULL AND k.date >= CURRENT_DATE - INTERVAL '18 days' AND k.date <= CURRENT_DATE
GROUP BY k.date, k.site_id
UNION ALL
SELECT k.date::text AS date, k.site_id, AVG(k.value) AS value, 'LTE Call Setup Success Rate' AS kpi_name
FROM kpi_data k WHERE k.kpi_name = 'LTE Call Setup Success Rate' AND k.site_id = 'SITE_A'
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


    # ── LLM Call ──────────────────────────────────────────────────────────────
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
        _LOG.warning("LLM call failed: %s", str(e)[:200])

    if not ai_result:
        _emit_progress(_ws_sid, "complete", "Error")
        return jsonify({"error": "Could not generate a query. Please try rephrasing your question."}), 400

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

    # ── MULTI-CHART: execute each chart's SQL in PARALLEL ──────────────────────
    if ai_result.get("multi_chart") and ai_result.get("charts"):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _exec_chart(chart_spec):
            c_sql = chart_spec.get("sql", "")
            try:
                rows = _sql_with_timeout(c_sql, timeout_sec=15)
                if not rows:
                    _LOG.warning("Multi-chart SQL returned 0 rows — SQL: %s", c_sql[:300])
                    return chart_spec, rows, "Query returned no data. The site ID or KPI may not exist in the database."
                return chart_spec, rows, None
            except Exception as e:
                _LOG.warning("Multi-chart SQL failed: %s — SQL: %s", e, c_sql[:300])
                return chart_spec, [], str(e)

        charts_out = []
        chart_specs = ai_result["charts"]
        results_map = {}
        with ThreadPoolExecutor(max_workers=min(len(chart_specs), 4)) as _pool:
            futures = {_pool.submit(_exec_chart, spec): i for i, spec in enumerate(chart_specs)}
            for fut in as_completed(futures):
                idx = futures[fut]
                results_map[idx] = fut.result()

        # Rebuild charts in original order
        for idx, chart_spec in enumerate(chart_specs):
            chart_spec, c_rows, c_error = results_map.get(idx, (chart_spec, [], "Execution failed"))
            c_sql = chart_spec.get("sql", "")
            c_cols = list(c_rows[0].keys()) if c_rows else []
            c_safe = [{k: _serial(v) for k, v in r.items()} for r in c_rows]

            # Validate y_axes against actual result columns
            _mc_skip = {
                "lat", "lng", "latitude", "longitude", "site_id", "cell_id",
                "cluster", "region", "zone", "technology", "kpi_name", "date", "hour",
            }
            _mc_llm_y = chart_spec.get("y_axes", [])
            _mc_y = [c for c in _mc_llm_y if c in c_cols]
            if not _mc_y:
                _mc_y = [
                    c for c in c_cols
                    if c not in _mc_skip
                    and any(isinstance(r.get(c), (int, float)) for r in c_rows[:5])
                ][:5]
            if not _mc_y:
                _mc_y = [c for c in c_cols if c not in _mc_skip][:3]

            # Validate x_axis
            _mc_llm_x = chart_spec.get("x_axis", "date")
            _mc_x = _mc_llm_x if _mc_llm_x in c_cols else (c_cols[0] if c_cols else "date")

            # Correct chart type from actual data shape
            _mc_has_kpi = "kpi_name" in c_cols
            _mc_has_val = "value" in c_cols
            _mc_has_date = _mc_x in ("date", "hour") or "date" in str(_mc_x).lower()
            _mc_chart = chart_spec.get("chart_type", "line")
            if _mc_has_kpi and _mc_has_val:
                _mc_chart = "composed"
            elif _mc_has_date and len(_mc_y) >= 2:
                _mc_chart = "composed"
            elif _mc_has_date and len(_mc_y) == 1:
                _mc_chart = "line"

            chart_entry = {
                "title":      chart_spec.get("title", ""),
                "chart_type": _mc_chart,
                "x_axis":     _mc_x,
                "y_axes":     _mc_y,
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

    # ── 0-rows explanation: if still empty, check KPI ranges and explain why ──
    # This prevents the chart from silently showing nothing.
    # If we know the value ranges, we can tell the user their threshold is too tight.
    if not rows and sql:
        try:
            _kpi_ranges = _schema_cache.get("kpi_ranges", {})
            _threshold_kpis = []
            import re as _re_0r
            # Detect HAVING/WHERE conditions in the SQL to find the KPIs + thresholds
            _having_clauses = _re_0r.findall(
                r"kpi_name\s*=\s*'([^']+)'[^)]*(?:>|<)\s*([\d.]+)",
                sql, _re_0r.IGNORECASE
            )
            for _kn, _val in _having_clauses:
                if _kn in _kpi_ranges:
                    r = _kpi_ranges[_kn]
                    _threshold_kpis.append(
                        f"{_kn}: actual range [{r['min_val']} – {r['max_val']}], "
                        f"avg={r['avg_val']} (your threshold: {_val})"
                    )
            if _threshold_kpis:
                ai_result["response"] = (
                    "No sites matched the specified thresholds. "
                    "Based on actual data ranges:\n"
                    + "\n".join(f"• {t}" for t in _threshold_kpis)
                    + "\n\nTry relaxing the threshold values."
                )
                _LOG.info("[0-ROWS] Threshold explanation injected for %d KPIs", len(_threshold_kpis))
        except Exception as _e0r:
            _LOG.debug("[0-ROWS] Explanation injection failed: %s", _e0r)

    columns = list(rows[0].keys()) if rows else []
    has_geo = any(r.get("lat") or r.get("latitude") for r in rows)
    safe_rows = [{k: _serial(v) for k, v in r.items()} for r in rows]

    # ── Fix 1: Validate y_axes against actual query result columns ──────────
    # LLM often hallucinates column names that don't exist in the result.
    # Only keep suggestions that exist; fallback to actual numeric columns.
    _SKIP_COLS = {
        "lat", "lng", "latitude", "longitude", "site_id", "cell_id",
        "cluster", "region", "zone", "technology", "kpi_name",
        "date", "hour", "color", "status",
    }
    _llm_y = ai_result.get("y_axes") or []
    _validated_y = [c for c in _llm_y if c in columns]
    if not _validated_y:
        # Derive from actual result: prefer columns that contain numeric values
        _validated_y = [
            c for c in columns
            if c not in _SKIP_COLS
            and any(isinstance(r.get(c), (int, float)) for r in rows[:5])
        ][:5]
    if not _validated_y:
        # Last resort: anything not in skip list
        _validated_y = [c for c in columns if c not in _SKIP_COLS][:3]
    y_axes = _validated_y

    # ── Fix 5: Validate x_axis against actual columns ────────────────────────
    _llm_x = ai_result.get("x_axis", "")
    x_axis_final = _llm_x if _llm_x in columns else (columns[0] if columns else "")

    # ── Fix 2: Correct chart type based on actual data shape ─────────────────
    def _correct_chart_type(ai_chart, cols, data_rows, x_col, y_cols):
        if not data_rows:
            return ai_chart
        has_date_x = x_col in ("date", "hour") or "date" in str(x_col).lower()
        has_kpi_col = "kpi_name" in cols
        has_value_col = "value" in cols
        n_rows = len(data_rows)
        n_series = len(y_cols)

        # UNION ALL multi-KPI result — kpi_name + value columns = always composed
        if has_kpi_col and has_value_col:
            return "composed"
        # Time series with 1 series → line
        if has_date_x and n_series == 1 and n_rows > 5:
            return "line"
        # Time series with 2+ series → composed
        if has_date_x and n_series >= 2:
            return "composed"
        # Site/category comparison → bar
        if x_col in ("site_id", "cell_id", "zone", "cluster", "kpi_name", "category"):
            return "bar"
        # Pie only for small distributions
        if ai_chart == "pie" and n_rows <= 12:
            return "pie"
        return ai_chart

    resp_text = ai_result.get("response", f"Found {len(rows)} results.")
    resp_title = ai_result.get("title", prompt[:70])
    resp_chart = ai_result.get("chart_type", ai_result.get("query_type", "bar"))
    resp_chart = _correct_chart_type(resp_chart, columns, rows, x_axis_final, y_axes)
    _LOG.info("[CHART-CORRECT] final chart_type=%s x_axis=%s y_axes=%s",
              resp_chart, x_axis_final, y_axes)

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
                    "x_axis": x_axis_final,
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
        # ── update session context + maybe summarize ──
        _update_session_context(ai_session, sql, resp_chart)
        _maybe_summarize_conversation(ai_session)

    _emit_progress(_ws_sid, 'complete', 'Done')
    _elapsed = round(time.time() - _query_start, 2)
    _LOG.info("[COMPLETE] Single-chart query in %.2fs | %d rows | provider=%s | chart=%s",
              _elapsed, len(rows), provider.get("provider") if provider else "rule-based", resp_chart)
    return jsonify({
        "response":      resp_text,
        "query_type":    resp_chart,
        "chart_type":    resp_chart,
        "chart_config":  ai_result.get("chart_config", {}),
        "title":         resp_title,
        "x_axis":        x_axis_final,
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