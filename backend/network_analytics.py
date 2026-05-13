"""
network_analytics.py  (Updated v2)
====================================
Flask Blueprint: Telecom Network Predictive Analytics
Provides APIs for the Agent Portal's Network Analysis Dashboard.

New endpoints added for:
  - RAN layer  : /api/network/ran-analytics  (expanded with all 27 KPIs)
  - Core layer : /api/network/core-analytics (Auth, CPU, Attach, PDP Bearer trends + site table)
  - Transport  : /api/network/transport-analytics (link util, latency, jitter, packet loss, backhaul mix)
  - Region     : /api/network/region          (country/state/city/zone drilldown)
  - Timeframe  : /api/network/timeframe       (temporal analysis with peak-hour heatmap)
  - KPI filter : /api/network/kpi-filter      (14 intelligence filters)

Unchanged from v1:
  - Upload, delete, summary, map, filters, anomalies, what-if, AI-query endpoints

Mount in app.py:
    from network_analytics import network_bp
    app.register_blueprint(network_bp)

Tables used:
    network_kpi_timeseries  – core timeseries (all RAN KPIs via extra_kpis JSONB)
    core_kpi_data           – core network KPIs (created on first upload)
    transport_kpi_data      – transport KPIs   (created on first upload)
    revenue_data            – per-site revenue & expense (created on first upload)
"""

import io
import os
import json
import math
import hashlib
import traceback
import logging

_LOG = logging.getLogger('network_analytics')
from datetime import datetime, timedelta, timezone
from functools import wraps

import numpy as np
import pandas as pd
from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text as sa_text, func, and_, or_, case as sql_case
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect as sa_inspect

from models import db, User, FlexibleKpiUpload

try:
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

# ─────────────────────────────────────────────────────────────────────────────
network_bp = Blueprint("network", __name__)

_CACHE: dict = {}
CACHE_TTL = 1800  # 30 minutes — analytics data changes only on upload

# Revenue currency conversion: if uploaded data is in INR, convert to USD
# Set to 1.0 if data is already in USD. Adjust rate as needed.
INR_TO_USD = 0.012  # 1 INR ≈ 0.012 USD

# Critical KPIs per component for telecom operations — used for forecast prioritization
CORE_CRITICAL_KPIS = {
    "MME": ["Attach Success Rate", "Service Request Success Rate", "Paging Success Rate", "CPU Utilization"],
    "SGW": ["Create Session Success Rate", "GTP-U Tunnel Availability", "Packet Loss", "User Plane Latency"],
    "PGW": ["Default Bearer Setup Success Rate", "Session Setup Success Rate", "Packet Loss DL UL", "User Plane Latency"],
    "HSS": ["Authentication Success Rate", "S6a Transaction Success Rate", "DB Query Success Rate", "CPU Utilization"],
    "PCRF": ["Policy Decision Success Rate", "Gx Success Rate", "Session Establishment Success Rate", "CPU Utilization"],
}

def clear_analytics_cache():
    """Clear all cached analytics data. Call after data upload/delete.

    Resets every module-level cache so the next request hits the DB. This
    is critical after a DROP/CREATE database cycle: stale schema/columns/
    KPI-name caches must not survive into the new DB session."""
    _CACHE.clear()
    _KPI_COUNTS_CACHE.clear()
    _KPI_NAMES_CACHE.clear()
    global _KPI_MAX_DATE, _KPI_MAX_DATE_TS, _TS_COLS_CACHE
    _KPI_MAX_DATE = None
    _KPI_MAX_DATE_TS = None
    _TS_COLS_CACHE = None


# Process-level memo for site/cell counts derived from kpi_data — these
# scans take 20s+ on multi-million-row tables; refresh every hour.
_KPI_COUNTS_CACHE: dict = {}
_KPI_COUNTS_TTL = 3600  # seconds

_KPI_NAMES_CACHE: dict = {}
def _distinct_kpi_names_cached() -> list:
    """Distinct kpi_name values from kpi_data — cached for an hour. The DB
    query takes 5-20s on 37M rows; KPI names rarely change between uploads.

    NEVER caches an empty list: if the query fails or the table is empty
    (transiently, e.g. the DB was just dropped/recreated), we want the next
    request to re-query rather than serve "[]" for an hour."""
    now_ts = datetime.utcnow().timestamp()
    cached = _KPI_NAMES_CACHE.get("v")
    if cached and cached.get("data") and (now_ts - cached["ts"] < _KPI_COUNTS_TTL):
        return cached["data"]
    names = []
    try:
        names = [r["kpi_name"] for r in _sql("SELECT DISTINCT kpi_name FROM kpi_data WHERE kpi_name IS NOT NULL")]
    except Exception as e:
        _LOG.warning("_distinct_kpi_names_cached query failed: %s", e)
    if names:
        _KPI_NAMES_CACHE["v"] = {"data": names, "ts": now_ts}
    return names


def _kpi_data_counts_cached() -> dict:
    """Distinct site/cell counts from kpi_data — cached for an hour.
    Only caches when sites > 0 so a transient empty result doesn't poison
    the cache for an hour."""
    now_ts = datetime.utcnow().timestamp()
    cached = _KPI_COUNTS_CACHE.get("v")
    if cached and cached.get("data", {}).get("sites", 0) > 0 \
            and (now_ts - cached["ts"] < _KPI_COUNTS_TTL):
        return cached["data"]
    sites = cells = 0
    try:
        # Distinct site_id — uses the (site_id, kpi_name) index for an
        # index-only scan instead of seq-scanning the value column.
        r = _sql("SELECT COUNT(*) AS n FROM (SELECT DISTINCT site_id FROM kpi_data) sub")
        sites = int((r or [{"n": 0}])[0].get("n") or 0)
    except Exception as e:
        _LOG.warning("_kpi_data_counts_cached site query failed: %s", e)
    try:
        # Distinct (site_id, cell_id) — only cell-level rows have cell_id.
        r = _sql("""
            SELECT COUNT(*) AS n FROM (
                SELECT DISTINCT site_id, cell_id FROM kpi_data
                WHERE data_level = 'cell' AND cell_id IS NOT NULL AND cell_id <> ''
            ) sub
        """)
        cells = int((r or [{"n": 0}])[0].get("n") or 0)
    except Exception as e:
        _LOG.warning("_kpi_data_counts_cached cell query failed: %s", e)
    data = {"sites": sites, "cells": cells}
    if sites > 0:
        _KPI_COUNTS_CACHE["v"] = {"data": data, "ts": now_ts}
    return data

_FLEX_TABLES_ENSURED = False  # run DDL only once per process

def _ensure_kpi_indexes():
    """Ensure fast indexes on kpi_data exist. Called once at import time."""
    try:
        with db.engine.connect() as conn:
            for stmt in [
                "CREATE INDEX IF NOT EXISTS idx_kpi_date ON kpi_data (date)",
                "CREATE INDEX IF NOT EXISTS idx_kpi_name_date ON kpi_data (kpi_name, date)",
                "CREATE INDEX IF NOT EXISTS idx_kpi_site_kpi ON kpi_data (site_id, kpi_name)",
                "CREATE INDEX IF NOT EXISTS idx_kpi_site_id ON kpi_data (site_id)",
                "CREATE INDEX IF NOT EXISTS idx_kpi_level_name_date ON kpi_data (data_level, kpi_name, date)",
                "CREATE INDEX IF NOT EXISTS idx_kpi_level_date ON kpi_data (data_level, date)",
                "CREATE INDEX IF NOT EXISTS idx_ts_site_id ON telecom_sites (site_id)",
                "CREATE INDEX IF NOT EXISTS idx_ts_zone ON telecom_sites (zone)",
                "CREATE INDEX IF NOT EXISTS idx_ts_province ON telecom_sites (province)",
                "CREATE INDEX IF NOT EXISTS idx_kpi_site_abs ON kpi_data (site_abs_id)",
                "CREATE INDEX IF NOT EXISTS idx_kpi_site_level_lookup ON kpi_data (kpi_name, site_id, date) WHERE data_level = 'site' AND value IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_nkts_technology ON network_kpi_timeseries (technology)",
                "CREATE INDEX IF NOT EXISTS idx_core_kpi_name ON core_component_kpi (kpi_name, date)",
            ]:
                try:
                    conn.execute(sa_text(stmt))
                except Exception:
                    pass
            conn.commit()
    except Exception:
        pass


def _ensure_kpi_data_merged_view():
    """Create the kpi_data_merged materialized view if it does not exist.

    app.py also creates this during startup. Keeping this helper here lets
    network_ai and upload hooks refresh the view without importing app.py and
    creating a circular dependency.
    """
    ddl = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS kpi_data_merged AS
        SELECT id, site_id, site_abs_id, kpi_name, date, hour, value,
               data_level, cell_id, cell_site_id
        FROM kpi_data
        WHERE data_level = 'site'
        UNION ALL
        SELECT
            MIN(k.id)            AS id,
            k.site_id,
            MAX(k.site_abs_id)   AS site_abs_id,
            k.kpi_name,
            k.date,
            0                    AS hour,
            AVG(k.value)         AS value,
            'site'               AS data_level,
            NULL::varchar        AS cell_id,
            NULL::varchar        AS cell_site_id
        FROM kpi_data_merged k
        WHERE k.data_level = 'cell' AND k.value IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM kpi_data s
              WHERE s.site_id    = k.site_id
                AND s.kpi_name   = k.kpi_name
                AND s.date       = k.date
                AND s.data_level = 'site'
          )
        GROUP BY k.site_id, k.kpi_name, k.date
        WITH DATA
    """
    idx_ddls = [
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_kdm_id ON kpi_data_merged (id)",
        "CREATE INDEX IF NOT EXISTS idx_kdm_site_kpi_date ON kpi_data_merged (site_id, kpi_name, date)",
        "CREATE INDEX IF NOT EXISTS idx_kdm_kpi_date ON kpi_data_merged (kpi_name, date) WHERE value IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_kdm_level_kpi_date ON kpi_data_merged (data_level, kpi_name, date)",
    ]
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text(ddl))
            conn.commit()
            for idx in idx_ddls:
                try:
                    conn.execute(sa_text(idx))
                    conn.commit()
                except Exception as exc:
                    _LOG.warning("kpi_data_merged index setup skipped: %s", exc)
    except Exception as exc:
        _LOG.warning("kpi_data_merged materialized view setup skipped: %s", exc)


def refresh_kpi_data_merged():
    """Refresh the merged KPI materialized view, best-effort."""
    _ensure_kpi_data_merged_view()
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text("REFRESH MATERIALIZED VIEW CONCURRENTLY kpi_data_merged"))
            conn.commit()
    except Exception:
        try:
            with db.engine.connect() as conn:
                conn.execute(sa_text("REFRESH MATERIALIZED VIEW kpi_data_merged"))
                conn.commit()
        except Exception as exc:
            _LOG.warning("kpi_data_merged refresh skipped: %s", exc)
    clear_analytics_cache()


def _ensure_kpi_data_stats_table():
    """Create a compact KPI inventory table used by startup/upload hooks."""
    ddl = """
        CREATE TABLE IF NOT EXISTS kpi_data_stats (
            data_level   VARCHAR(10) NOT NULL,
            kpi_name     VARCHAR(100) NOT NULL,
            row_count    BIGINT NOT NULL DEFAULT 0,
            site_count   BIGINT NOT NULL DEFAULT 0,
            cell_count   BIGINT NOT NULL DEFAULT 0,
            date_from    DATE,
            date_to      DATE,
            updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (data_level, kpi_name)
        )
    """
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text(ddl))
            conn.commit()
    except Exception as exc:
        _LOG.warning("kpi_data_stats table setup skipped: %s", exc)


def upsert_kpi_data_stats():
    """Refresh KPI inventory stats after KPI upload/delete operations."""
    _ensure_kpi_data_stats_table()
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text("TRUNCATE TABLE kpi_data_stats"))
            conn.execute(sa_text("""
                INSERT INTO kpi_data_stats (
                    data_level, kpi_name, row_count, site_count, cell_count,
                    date_from, date_to, updated_at
                )
                SELECT
                    COALESCE(data_level, 'site') AS data_level,
                    kpi_name,
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT site_id) AS site_count,
                    COUNT(DISTINCT CASE
                        WHEN data_level = 'cell' AND cell_id IS NOT NULL AND cell_id <> ''
                        THEN site_id || ':' || cell_id
                    END) AS cell_count,
                    MIN(date) AS date_from,
                    MAX(date) AS date_to,
                    NOW() AS updated_at
                FROM kpi_data
                WHERE kpi_name IS NOT NULL
                GROUP BY COALESCE(data_level, 'site'), kpi_name
            """))
            conn.commit()
    except Exception as exc:
        _LOG.warning("kpi_data_stats refresh skipped: %s", exc)
    clear_analytics_cache()


def _cache_key(prefix: str, params: dict) -> str:
    raw = json.dumps(params, sort_keys=True)
    return f"{prefix}:{hashlib.md5(raw.encode()).hexdigest()}"


def _from_cache(key: str):
    item = _CACHE.get(key)
    if item and (datetime.utcnow() - item["ts"]).seconds < CACHE_TTL:
        return item["data"]
    return None


def _to_cache(key: str, data):
    # Skip empty / failure responses so a transient query failure doesn't
    # poison the 5-minute cache and make the dashboard show "no data" for
    # users hitting it during the cache window. Note: a value of 0 may be
    # legitimate (e.g. zero congested sites), so only None / empty
    # collection / empty string is treated as "failed compute".
    if data is None:
        return
    if isinstance(data, dict) and not data:
        return
    if isinstance(data, list) and not data:
        return
    if isinstance(data, dict):
        meaningful = [k for k in data.keys() if k not in ("error", "message", "status")]
        if not meaningful:
            return
        # Only skip if EVERY meaningful field is None / "" / [] / {} —
        # numeric 0 stays cacheable.
        empty_marker = (None, "", [], {})
        if all(data.get(k) in empty_marker for k in meaningful):
            return
    _CACHE[key] = {"data": data, "ts": datetime.utcnow()}
    if len(_CACHE) > 2000:
        oldest = sorted(_CACHE.items(), key=lambda x: x[1]["ts"])[:500]
        for k, _ in oldest:
            _CACHE.pop(k, None)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _get_filters():
    region     = request.args.get("region",     "").strip() or None
    cluster    = request.args.get("cluster",    "").strip() or None
    site       = request.args.get("site",       "").strip() or None
    technology = request.args.get("technology", "").strip() or None
    vendor     = request.args.get("vendor",     "").strip() or None
    time_range = request.args.get("time_range", "30d").strip()
    country    = request.args.get("country",    "").strip() or None
    state      = request.args.get("state",      "").strip() or None
    city       = request.args.get("city",       "").strip() or None
    zone       = request.args.get("zone",       "").strip() or None
    kpi_filter = request.args.get("kpi_filter", "").strip() or None
    return {
        "region": region, "cluster": cluster, "site": site,
        "technology": technology, "vendor": vendor, "time_range": time_range,
        "country": country or "", "state": state or "", "city": city or "",
        "zone": zone or cluster, "kpi_filter": kpi_filter,
    }


# ── Precomputed max date from kpi_data (cached, refreshed every 5 min) ────────
_KPI_MAX_DATE = None
_KPI_MAX_DATE_TS = None

def _get_kpi_max_date():
    """Return the latest date present in the kpi_data table.

    Reads kpi_data directly so it works on every deployment regardless of
    whether the kpi_data_merged view exists. Cached for 5 minutes.
    """
    global _KPI_MAX_DATE, _KPI_MAX_DATE_TS
    now = datetime.utcnow()
    if _KPI_MAX_DATE and _KPI_MAX_DATE_TS and (now - _KPI_MAX_DATE_TS).total_seconds() < 300:
        return _KPI_MAX_DATE
    # Try kpi_data directly (works regardless of view existence). Each query
    # is in its OWN try/except so a failed first attempt doesn't skip the
    # fallback (the previous version had this bug — when kpi_data_merged
    # didn't exist the first _sql threw and the fallback was unreachable,
    # leaving _KPI_MAX_DATE = None and the 7-day filter cutting off data).
    try:
        # MAX(date) — index-only scan via idx_kpi_date. Filtering on
        # `value IS NOT NULL` defeats that index (forces seq scan); MAX(date)
        # alone is correct here and runs in milliseconds.
        r = _sql("SELECT MAX(date) AS md FROM kpi_data")
        if r and r[0].get("md"):
            _KPI_MAX_DATE = r[0]["md"]
    except Exception as e:
        _LOG.warning("kpi_data max date query failed: %s", e)
    # Belt-and-suspenders: try kpi_data_merged if kpi_data didn't yield a date
    if not _KPI_MAX_DATE:
        try:
            r2 = _sql("SELECT MAX(date) AS md FROM kpi_data_merged WHERE value IS NOT NULL")
            if r2 and r2[0].get("md"):
                _KPI_MAX_DATE = r2[0]["md"]
        except Exception:
            pass
    _KPI_MAX_DATE_TS = now
    if _KPI_MAX_DATE:
        print(f"[NETWORK ANALYTICS] kpi_data reference max date: {_KPI_MAX_DATE}")
        _LOG.info("kpi_data reference max date: %s", _KPI_MAX_DATE)
    return _KPI_MAX_DATE


def _kpi_filter_clause(filters: dict, k_alias: str = "k", ts_alias: str = "ts"):
    """Build WHERE additions + params for kpi_data queries filtered by zone/tech/region/time/geo.
    Returns (extra_where: str, extra_params: dict, needs_ts_join: bool).
    The caller must JOIN telecom_sites if needs_ts_join is True.
    """
    parts = []
    params = {}
    needs_ts = False
    zone = (filters or {}).get("cluster") or (filters or {}).get("zone") or ""
    tech = (filters or {}).get("technology") or ""
    vendor = (filters or {}).get("vendor") or ""
    region = (filters or {}).get("region") or ""
    site = (filters or {}).get("site") or ""
    country = (filters or {}).get("country") or ""
    state = (filters or {}).get("state") or ""
    city = (filters or {}).get("city") or ""
    tr = (filters or {}).get("time_range") or "30d"

    # Helper: supports comma-separated multi-values → IN clause
    def _multi(col, val, prefix):
        items = [v.strip() for v in val.split(",") if v.strip()]
        if len(items) == 1:
            params[prefix] = items[0]
            return f"LOWER({col}) = LOWER(:{prefix})"
        else:
            placeholders = []
            for i, v in enumerate(items):
                key = f"{prefix}_{i}"
                params[key] = v
                placeholders.append(f"LOWER(:{key})")
            return f"LOWER({col}) IN ({','.join(placeholders)})"

    if zone:
        parts.append(_multi(f"{ts_alias}.zone", zone, "_fz"))
        needs_ts = True
    if region:
        parts.append(f"(LOWER({ts_alias}.zone) = LOWER(:_fr) OR {k_alias}.site_id IN (SELECT site_id FROM telecom_sites WHERE LOWER(city) = LOWER(:_fr2) OR LOWER(state) = LOWER(:_fr3)))")
        params["_fr"] = region
        params["_fr2"] = region
        params["_fr3"] = region
        needs_ts = True  # still need ts for zone

    # Geo + tech + vendor filters: use subquery to avoid missing-column errors on older DBs
    _geo = []
    if tech:
        _geo.append(_multi("technology", tech, "_ft"))
    if vendor:
        _geo.append(_multi("vendor_name", vendor, "_fv"))
    if country:
        _geo.append(f"LOWER(country) = LOWER(:_fcountry)")
        params["_fcountry"] = country
    if state:
        _geo.append(f"LOWER(state) = LOWER(:_fstate)")
        params["_fstate"] = state
    if city:
        items_c = [v.strip() for v in city.split(",") if v.strip()]
        if len(items_c) == 1:
            _geo.append(f"LOWER(city) = LOWER(:_fcity)")
            params["_fcity"] = items_c[0]
        else:
            phs_c = []
            for i, v in enumerate(items_c):
                ck = f"_fcity_{i}"
                params[ck] = v
                phs_c.append(f"LOWER(:{ck})")
            _geo.append(f"LOWER(city) IN ({','.join(phs_c)})")
    if _geo:
        parts.append(f"{k_alias}.site_id IN (SELECT site_id FROM telecom_sites WHERE {' AND '.join(_geo)})")
        # no needs_ts — using subquery instead of ts join
    if site:
        parts.append(f"LOWER({k_alias}.site_id) = LOWER(:_fs)")
        params["_fs"] = site
    if tr and tr != "all":
        # days_map: "24h" was previously mapped to 7 (bug) — corrected to 1.
        days_map = {"1h": 1, "6h": 1, "24h": 1, "7d": 7, "30d": 30}
        days = days_map.get(tr, 30)
        # Anchor the window to MAX(kpi_data.date) so uploads whose dates are
        # older than today (real telecom data is usually a few days behind)
        # still surface in the 30d/7d views. Falls back to CURRENT_DATE only
        # when the table is empty.
        max_date = _get_kpi_max_date()
        if max_date:
            from datetime import timedelta as _td
            start_date = max_date - _td(days=days)
            parts.append(f"{k_alias}.date >= :_fdate_start")
            parts.append(f"{k_alias}.date <= :_fdate_end")
            params["_fdate_start"] = start_date
            params["_fdate_end"] = max_date
        else:
            parts.append(f"{k_alias}.date >= CURRENT_DATE - INTERVAL '{days} days'")
            parts.append(f"{k_alias}.date <= CURRENT_DATE")

    extra_where = (" AND " + " AND ".join(parts)) if parts else ""
    return extra_where, params, needs_ts


def _time_cutoff(time_range: str) -> datetime:
    now = datetime.utcnow()
    mapping = {
        "1h":  timedelta(hours=1),  "6h":  timedelta(hours=6),
        "24h": timedelta(hours=24), "7d":  timedelta(days=7),
        "30d": timedelta(days=30),  "all": timedelta(days=3650),
    }
    return now - mapping.get(time_range, timedelta(days=30))


def _get_data_window():
    """Check if network_kpi_timeseries has any data."""
    try:
        rows = _sql("SELECT MAX(timestamp) AS latest, MIN(timestamp) AS earliest FROM network_kpi_timeseries")
        if rows and rows[0].get("latest"):
            return rows[0]["earliest"], rows[0]["latest"]
    except Exception:
        pass
    return None, None


def _smart_cutoff(time_range: str) -> datetime:
    """Cutoff for network_kpi_timeseries timestamp-based filter."""
    standard = _time_cutoff(time_range)
    try:
        _, latest = _get_data_window()
        if latest and hasattr(latest, "replace"):
            now = datetime.utcnow()
            if latest >= now - timedelta(days=1):
                return standard          # recent data — use normal cutoff
            mapping = {"1h": timedelta(hours=1), "6h": timedelta(hours=6),
                       "24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}
            delta = mapping.get(time_range, timedelta(hours=24))
            return latest - delta         # historical data — anchor to end of data
    except Exception:
        pass
    return standard


def _dynamic_time_filter(time_range: str = "24h") -> str:
    """SQL fragment for network_kpi_timeseries timestamp filter."""
    try:
        earliest, latest = _get_data_window()
        if not latest:
            return "1=1"
        mapping = {"1h": timedelta(hours=1), "6h": timedelta(hours=6),
                   "24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}
        delta = mapping.get(time_range, timedelta(hours=24))
        now = datetime.utcnow()
        if latest >= now - delta:
            cutoff = now - delta
            return f"timestamp >= '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'"
        cutoff = latest - delta
        if earliest and cutoff < earliest:
            cutoff = earliest
        return (f"timestamp >= '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}' "
                f"AND timestamp <= '{latest.strftime('%Y-%m-%d %H:%M:%S')}'")
    except Exception:
        return "1=1"


def _build_where(filters: dict, table_prefix: str = "") -> tuple[str, dict]:
    """WHERE clause for network_kpi_timeseries (timestamp-based). Only adds
    timestamp filter when that table actually has data.
    Supports multi-select (comma-separated) for cluster and technology.
    Supports country/state/city via subquery to telecom_sites."""
    col = lambda c: f"{table_prefix}.{c}" if table_prefix else c
    parts, params = ["1=1"], {}
    _, latest = _get_data_window()
    if latest:                            # only filter by time if table has rows
        cutoff = _smart_cutoff(filters["time_range"])
        parts.append(f"{col('timestamp')} >= :cutoff")
        params["cutoff"] = cutoff
    if filters.get("region"):
        parts.append(f"LOWER({col('region')}) = LOWER(:region)")
        params["region"] = filters["region"]

    # Multi-select helper for _build_where
    def _bw_multi(column, val, prefix):
        items = [v.strip() for v in val.split(",") if v.strip()]
        if len(items) == 1:
            params[prefix] = items[0]
            return f"LOWER({column}) = LOWER(:{prefix})"
        phs = []
        for i, v in enumerate(items):
            key = f"{prefix}_{i}"
            params[key] = v
            phs.append(f"LOWER(:{key})")
        return f"LOWER({column}) IN ({','.join(phs)})"

    if filters.get("cluster") or filters.get("zone"):
        v = filters.get("cluster") or filters.get("zone")
        parts.append(_bw_multi(col('cluster'), v, 'cluster'))
    if filters.get("site"):
        parts.append(f"LOWER({col('site_id')}) = LOWER(:site)")
        params["site"] = filters["site"]
    if filters.get("technology"):
        parts.append(_bw_multi(col('technology'), filters["technology"], 'technology'))

    # Country / State / City — filter via telecom_sites subquery on site_id
    geo_parts = []
    geo_params = {}
    if filters.get("country"):
        geo_parts.append("LOWER(country) = LOWER(:_bw_country)")
        geo_params["_bw_country"] = filters["country"]
    if filters.get("state"):
        geo_parts.append("LOWER(state) = LOWER(:_bw_state)")
        geo_params["_bw_state"] = filters["state"]
    if filters.get("city"):
        items = [v.strip() for v in filters["city"].split(",") if v.strip()]
        if len(items) == 1:
            geo_parts.append("LOWER(city) = LOWER(:_bw_city)")
            geo_params["_bw_city"] = items[0]
        else:
            phs_city = []
            for i, v in enumerate(items):
                ck = f"_bw_city_{i}"
                geo_params[ck] = v
                phs_city.append(f"LOWER(:{ck})")
            geo_parts.append(f"LOWER(city) IN ({','.join(phs_city)})")
    if geo_parts:
        sub = f"{col('site_id')} IN (SELECT DISTINCT site_id FROM telecom_sites WHERE {' AND '.join(geo_parts)})"
        parts.append(sub)
        params.update(geo_params)

    return " AND ".join(parts), params


# ─────────────────────────────────────────────────────────────────────────────
# kpi_data helpers
# Schema: site_id, kpi_name, date, hour, value, data_level('site'|'cell'),
#         cell_id, cell_site_id
# Joined with: telecom_sites (site_id, cell_id, latitude, longitude, zone)
# ─────────────────────────────────────────────────────────────────────────────

# KPI sheet-name → internal alias mapping
KPI_COL_MAP = {
    "LTE RRC Setup Success Rate":      "lte_rrc_setup_sr",
    "LTE Call Setup Success Rate":     "lte_call_setup_sr",
    "LTE E-RAB Setup Success Rate":    "erab_setup_sr",
    "E-RAB Call Drop Rate_1":          "erab_drop_rate",
    "CSFB Access Success Rate":        "csfb_access_sr",
    "LTE Intra-Freq HO Success Rate":  "intra_freq_ho_sr",
    "Intra-eNB HO Success Rate":       "intra_enb_ho_sr",
    "Inter-eNBX2HO Success Rate":      "inter_x2_ho_sr",
    "Inter-eNBS1HO Success Rate":      "inter_s1_ho_sr",
    "LTE DL - Cell Ave Throughput":    "dl_cell_tput",
    "LTE UL - Cell Ave Throughput":    "ul_cell_tput",
    "LTE DL - Usr Ave Throughput":     "dl_user_tput",
    "LTE UL - User Ave Throughput":    "ul_user_tput",
    "Average Latency Downlink":        "avg_latency_dl",
    "DL Data Total Volume":            "dl_data_vol",
    "UL Data Total Volume":            "ul_data_vol",
    "VoLTE Traffic Erlang":            "volte_traffic_erl",
    "VoLTE Traffic UL":                "volte_ul",
    "VoLTE Traffic DL":                "volte_dl",
    "Ave RRC Connected Ue":            "avg_rrc_ue",
    "Max RRC Connected Ue":            "max_rrc_ue",
    "Average Act UE DL Per Cell":      "avg_act_ue_dl",
    "Average Act UE UL Per Cell":      "avg_act_ue_ul",
    "Availability":                    "availability",
    "Average NI of Carrier-":          "avg_ni_carrier",
    "DL PRB Utilization (1BH)":        "dl_prb_util",
    "UL PRB Utilization (1BH)":        "ul_prb_util",
}

# Normalised (lower+stripped) lookup — built once on first call
_KPI_NORM: dict = {}

def _kpi_col(name: str):
    """Case-insensitive KPI name → alias lookup. Handles Excel sheet name variations."""
    global _KPI_NORM
    if not _KPI_NORM:
        _KPI_NORM = {k.strip().lower(): v for k, v in KPI_COL_MAP.items()}
    return KPI_COL_MAP.get(name) or _KPI_NORM.get((name or "").strip().lower())


def _kpi_date_range(filters: dict):
    """
    Return (from_date, to_date) for kpi_data WHERE clauses, respecting time_range.
    Anchors to MAX(kpi_data.date) so uploaded data whose dates lag behind today
    still falls inside the 30d/7d window. Falls back to CURRENT_DATE only when
    the table is empty.
    """
    try:
        tr = (filters or {}).get("time_range", "all")
        days_map = {"1h": 1, "6h": 1, "24h": 1, "7d": 7, "30d": 30, "all": None}
        days = days_map.get(tr)
        if days is None:
            return None, None
        max_date = _get_kpi_max_date()
        if max_date:
            from_date = max_date - timedelta(days=days)
            return from_date, max_date
        from datetime import date as _date
        today = _date.today()
        return today - timedelta(days=days), today
    except Exception as e:
        _LOG.warning("_kpi_date_range: %s", e)
        return None, None


def _kpi_where(filters: dict):
    """WHERE clause for kpi_data queries. Aliases: k=kpi_data, ts=telecom_sites.
    Always restricts to site-level data (data_level='site') for network-wide
    aggregations — cell-level rows (17M+) are excluded to keep queries fast.
    Now also applies zone, technology, country, state, city filters via subquery.
    """
    from_date, to_date = _kpi_date_range(filters)
    parts = ["k.value IS NOT NULL", "k.data_level = 'site'"]
    params = {}
    if from_date is not None:
        parts.append("k.date >= :kd_from"); params["kd_from"] = from_date
    if to_date is not None:
        parts.append("k.date <= :kd_to");   params["kd_to"]   = to_date
    if filters and filters.get("site"):
        parts.append("LOWER(k.site_id) = LOWER(:kd_site)"); params["kd_site"] = filters["site"]

    # Geo + technology filters via telecom_sites subquery
    def _kw_multi(col, val, prefix):
        items = [v.strip() for v in val.split(",") if v.strip()]
        if len(items) == 1:
            params[f"{prefix}"] = items[0]
            return f"LOWER({col}) = LOWER(:{prefix})"
        phs = []
        for i, v in enumerate(items):
            key = f"{prefix}_{i}"
            params[key] = v
            phs.append(f"LOWER(:{key})")
        return f"LOWER({col}) IN ({','.join(phs)})"

    geo = []
    if filters and filters.get("cluster"):
        geo.append(_kw_multi("zone", filters["cluster"], "_kw_zone"))
    if filters and filters.get("technology"):
        geo.append(_kw_multi("technology", filters["technology"], "_kw_tech"))
    if filters and filters.get("country"):
        geo.append("LOWER(country) = LOWER(:_kw_country)")
        params["_kw_country"] = filters["country"]
    if filters and filters.get("state"):
        geo.append("LOWER(state) = LOWER(:_kw_state)")
        params["_kw_state"] = filters["state"]
    if filters and filters.get("city"):
        items_c = [v.strip() for v in filters["city"].split(",") if v.strip()]
        if len(items_c) == 1:
            geo.append(f"LOWER(city) = LOWER(:_kw_city)")
            params["_kw_city"] = items_c[0]
        else:
            phs_c = []
            for i, v in enumerate(items_c):
                ck = f"_kw_city_{i}"
                params[ck] = v
                phs_c.append(f"LOWER(:{ck})")
            geo.append(f"LOWER(city) IN ({','.join(phs_c)})")
    if geo:
        parts.append(f"k.site_id IN (SELECT site_id FROM telecom_sites WHERE {' AND '.join(geo)})")

    return " AND ".join(parts), params


def _zone_join(filters: dict):
    """LEFT JOIN telecom_sites; add zone/geo WHERE clauses."""
    zone = (filters or {}).get("cluster") or (filters or {}).get("zone") or ""
    join = "LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)"
    parts = []
    params = {}

    def _zj_multi(col, val, prefix):
        items = [v.strip() for v in val.split(",") if v.strip()]
        if len(items) == 1:
            params[prefix] = items[0]
            return f"LOWER({col}) = LOWER(:{prefix})"
        phs = []
        for i, v in enumerate(items):
            key = f"{prefix}_{i}"
            params[key] = v
            phs.append(f"LOWER(:{key})")
        return f"LOWER({col}) IN ({','.join(phs)})"

    if zone:
        parts.append(_zj_multi("ts.zone", zone, "zone_val"))
    if (filters or {}).get("technology"):
        parts.append(_zj_multi("ts.technology", filters["technology"], "_zj_tech"))
    if (filters or {}).get("country"):
        parts.append("LOWER(ts.country) = LOWER(:_zj_country)")
        params["_zj_country"] = filters["country"]
    if (filters or {}).get("state"):
        parts.append("(LOWER(ts.state) = LOWER(:_zj_state) OR LOWER(ts.city) = LOWER(:_zj_state2))")
        params["_zj_state"] = filters["state"]
        params["_zj_state2"] = filters["state"]
    if (filters or {}).get("city"):
        items_c = [v.strip() for v in filters["city"].split(",") if v.strip()]
        if len(items_c) == 1:
            parts.append(f"(LOWER(ts.city) = LOWER(:_zj_city) OR LOWER(ts.state) = LOWER(:_zj_city2))")
            params["_zj_city"] = items_c[0]
            params["_zj_city2"] = items_c[0]
        else:
            phs_c, phs_s = [], []
            for i, v in enumerate(items_c):
                ck = f"_zj_city_{i}"
                sk = f"_zj_cityst_{i}"
                params[ck] = v
                params[sk] = v
                phs_c.append(f"LOWER(:{ck})")
                phs_s.append(f"LOWER(:{sk})")
            parts.append(f"(LOWER(ts.city) IN ({','.join(phs_c)}) OR LOWER(ts.state) IN ({','.join(phs_s)}))")

    extra_where = ("AND " + " AND ".join(parts)) if parts else ""
    return join, extra_where, params


def _kpi_network_agg(filters: dict) -> dict:
    """Network-wide KPI averages from kpi_data — ALL rows (site + cell level)."""
    where_sql, where_params = _kpi_where(filters)
    join_sql, extra_where, extra_params = _zone_join(filters)
    needs_join = bool(extra_where)
    params = {**where_params, **extra_params}
    internal: dict = {}
    max_sites = 0
    try:
        rows = _sql(f"""
            SELECT k.kpi_name,
                   AVG(k.value)              AS avg_val,
                   COUNT(DISTINCT k.site_id) AS n_sites
            FROM kpi_data_merged k {join_sql if needs_join else ""}
            WHERE {where_sql} {extra_where}
            GROUP BY k.kpi_name
        """, params)
        for r in rows:
            col = _kpi_col(r["kpi_name"])
            if col and r["avg_val"] is not None:
                internal[col] = _f(r["avg_val"], 3)
            n = int(r["n_sites"] or 0)
            if n > max_sites:
                max_sites = n
    except Exception as e:
        _LOG.error("_kpi_network_agg: %s", e, exc_info=True)
    # Authoritative site count — respect filters
    try:
        if needs_join:
            cnt = _sql(f"SELECT COUNT(DISTINCT k.site_id) AS n FROM kpi_data_merged k {join_sql} WHERE {where_sql} {extra_where}", params)
        else:
            cnt = _sql(f"SELECT COUNT(DISTINCT site_id) AS n FROM kpi_data_merged WHERE {where_sql}", where_params)
        direct = int((cnt[0].get("n") or 0) if cnt else 0)
        if direct > max_sites:
            max_sites = direct
    except Exception:
        pass
    return {**internal, "total_sites": max_sites}


def _kpi_site_list(filters: dict) -> list[dict]:
    """Per-site KPI pivot from kpi_data joined with telecom_sites for geo/zone."""
    where_sql, where_params = _kpi_where(filters)
    join_sql, extra_where, extra_params = _zone_join(filters)
    params = {**where_params, **extra_params}
    try:
        rows = _sql(f"""
            SELECT k.site_id,
                   k.kpi_name,
                   AVG(k.value)      AS avg_val,
                   MAX(ts.zone)      AS zone,
                   AVG(ts.latitude)  AS lat,
                   AVG(ts.longitude) AS lng
            FROM kpi_data_merged k {join_sql}
            WHERE {where_sql} {extra_where}
            GROUP BY k.site_id, k.kpi_name
            LIMIT 60000
        """, params)
    except Exception as e:
        _LOG.error("_kpi_site_list: %s", e, exc_info=True)
        return []
    sites: dict = {}
    for r in rows:
        sid = r["site_id"]
        if sid not in sites:
            sites[sid] = {
                "site_id": sid,
                "zone":    r.get("zone") or "",
                "lat":     r.get("lat"),
                "lng":     r.get("lng"),
            }
        else:
            if not sites[sid].get("zone") and r.get("zone"):
                sites[sid]["zone"] = r["zone"]
            if not sites[sid].get("lat") and r.get("lat"):
                sites[sid]["lat"] = r["lat"]
                sites[sid]["lng"] = r["lng"]
        col = _kpi_col(r["kpi_name"])
        if col and r["avg_val"] is not None:
            sites[sid][col] = _f(r["avg_val"], 3)
    return sorted(
        sites.values(),
        key=lambda s: float(s.get("dl_prb_util") or 0),
        reverse=True
    )[:500]


def _kpi_daily_trend(filters: dict, kpi_name: str, col_alias: str) -> list[dict]:
    """Daily trend. Uses ILIKE so sheet-name case/spacing differences match."""
    where_sql, where_params = _kpi_where(filters)
    join_sql, zone_where, zone_params = _zone_join(filters)
    params = {**where_params, **zone_params, "kn": kpi_name}
    try:
        rows = _sql(f"""
            SELECT k.date::text AS date, AVG(k.value) AS val
            FROM kpi_data_merged k {join_sql}
            WHERE {where_sql} AND TRIM(k.kpi_name) ILIKE TRIM(:kn) {zone_where}
            GROUP BY k.date ORDER BY k.date
        """, params)
        return [{"date": r["date"], col_alias: _f(r["val"], 2)} for r in rows]
    except Exception as e:
        _LOG.error("_kpi_daily_trend(%s): %s", kpi_name, e, exc_info=True)
        return []


def _kpi_site_daily_trend(site_id: str, time_range: str = "30d") -> dict:
    """Per-site daily trend for all KPIs. Returns {alias: [{date, value}]}."""
    from_date, to_date = _kpi_date_range({"time_range": time_range})
    params: dict = {"sid": site_id}
    parts = ["k.site_id = :sid", "k.value IS NOT NULL"]
    if from_date: parts.append("k.date >= :fd"); params["fd"] = from_date
    if to_date:   parts.append("k.date <= :td"); params["td"] = to_date
    where_sql = " AND ".join(parts)
    try:
        rows = _sql(f"""
            SELECT k.kpi_name, k.date::text AS date, AVG(k.value) AS val
            FROM kpi_data_merged k WHERE {where_sql}
            GROUP BY k.kpi_name, k.date ORDER BY k.date
        """, params)
    except Exception:
        return {}
    trend: dict = {}
    for r in rows:
        col = _kpi_col(r["kpi_name"])
        if col:
            trend.setdefault(col, []).append({"date": r["date"], "value": _f(r["val"], 2)})
    return trend


def _kpi_cell_list(site_id: str, time_range: str = "30d") -> list[dict]:
    """
    Cell-level KPI breakdown — most recent date's value per (cell_id, kpi_name).
    Uses DISTINCT ON to fetch the latest value for each cell+KPI, avoiding
    averaging away per-cell variation. Date range derived from cell-level data
    (not site-level) so different upload schedules don't cause empty results.
    """
    params: dict = {"sid": site_id}
    try:
        # Use DISTINCT ON (PostgreSQL) to get the latest value per cell per KPI
        rows = _sql("""
            SELECT DISTINCT ON (k.cell_id, k.kpi_name)
                   k.cell_id, k.kpi_name, k.value AS avg_val, k.date,
                   (SELECT COUNT(DISTINCT d.date) FROM kpi_data_merged d
                    WHERE d.site_id = k.site_id AND d.cell_id = k.cell_id
                      AND d.kpi_name = k.kpi_name AND d.data_level = 'cell'
                   ) AS records
            FROM kpi_data_merged k
            WHERE k.site_id = :sid
              AND k.value IS NOT NULL
              AND k.data_level = 'cell'
              AND k.cell_id IS NOT NULL
            ORDER BY k.cell_id, k.kpi_name, k.date DESC
        """, params)
    except Exception as e:
        _LOG.error("_kpi_cell_list: %s", e)
        return []
    cells: dict = {}
    for r in rows:
        cid = str(r["cell_id"]).strip()
        if not cid:
            continue
        if cid not in cells:
            cells[cid] = {"cell_id": cid, "cell_name": cid, "records": int(r.get("records") or 0)}
        col = _kpi_col(r["kpi_name"])
        if col and r["avg_val"] is not None:
            cells[cid][col] = _f(r["avg_val"], 3)
    # Sort by cell_id numerically if possible, else lexicographically
    def _cell_sort_key(c):
        try:
            return int(c["cell_id"])
        except (ValueError, TypeError):
            return c["cell_id"]
    return sorted(cells.values(), key=_cell_sort_key)


def _flex_kpi_agg(kpi_type: str) -> dict:
    """
    Network-wide averages from flexible_kpi_uploads (core or revenue).
    Returns {column_name: avg_value}.
    """
    try:
        rows = _sql("""
            SELECT column_name, AVG(num_value) AS avg_val
            FROM flexible_kpi_uploads
            WHERE kpi_type = :kt AND column_type = 'numeric' AND num_value IS NOT NULL
            GROUP BY column_name
        """, {"kt": kpi_type})
        return {r["column_name"]: _f(r["avg_val"], 2) for r in rows}
    except Exception:
        return {}


def _flex_kpi_site_list(kpi_type: str) -> list[dict]:
    """Per-site averages from flexible_kpi_uploads."""
    try:
        rows = _sql("""
            SELECT site_id, column_name, AVG(num_value) AS avg_val
            FROM flexible_kpi_uploads
            WHERE kpi_type = :kt AND column_type = 'numeric' AND num_value IS NOT NULL
            GROUP BY site_id, column_name
            ORDER BY site_id
        """, {"kt": kpi_type})
        # Pivot into per-site dicts
        sites = {}
        for r in rows:
            sid = r["site_id"]
            sites.setdefault(sid, {"site_id": sid})
            sites[sid][r["column_name"]] = _f(r["avg_val"], 2)
        return list(sites.values())
    except Exception:
        return []


def _flex_kpi_trend(kpi_type: str, column_name: str) -> list[dict]:
    """Daily trend for one column from flexible_kpi_uploads."""
    try:
        rows = _sql("""
            SELECT row_date::text AS date, AVG(num_value) AS val
            FROM flexible_kpi_uploads
            WHERE kpi_type = :kt AND column_name = :cn
              AND column_type = 'numeric' AND num_value IS NOT NULL
              AND row_date IS NOT NULL
            GROUP BY row_date ORDER BY row_date
        """, {"kt": kpi_type, "cn": column_name})
        return [{"date": r["date"], column_name: _f(r["val"], 2)} for r in rows]
    except Exception:
        return []


def _sql(query: str, params: dict = None, timeout_ms: int = 0) -> list[dict]:
    with db.engine.connect() as conn:
        if timeout_ms > 0:
            conn.execute(sa_text(f"SET LOCAL statement_timeout = '{timeout_ms}'"))
        result = conn.execute(sa_text(query), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


_TS_COLS_CACHE: set[str] | None = None

def _telecom_sites_cols_lower() -> set[str]:
    """Return telecom_sites column names (lowercased). Cached per process."""
    global _TS_COLS_CACHE
    if _TS_COLS_CACHE is not None:
        return _TS_COLS_CACHE
    try:
        insp = sa_inspect(db.engine)
        if not insp.has_table("telecom_sites"):
            _TS_COLS_CACHE = set()
            return _TS_COLS_CACHE
        _TS_COLS_CACHE = {c["name"].lower() for c in insp.get_columns("telecom_sites")}
    except Exception:
        _TS_COLS_CACHE = set()
    return _TS_COLS_CACHE


def _telecom_sites_cell_col() -> str | None:
    """Best-effort cell identifier column in telecom_sites (e.g., cell_name/cell_id)."""
    cols = _telecom_sites_cols_lower()
    for candidate in ("cell_name", "cellname", "cell_id", "cellid"):
        if candidate in cols:
            return candidate
    return None


def _f(v, digits=1):
    if v is None:
        return 0
    try:
        return round(float(v), digits)
    except (TypeError, ValueError):
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# Table creation helpers
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_network_kpi_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS network_kpi_timeseries (
        id              BIGSERIAL PRIMARY KEY,
        site_id         VARCHAR(100) NOT NULL,
        cell_id         VARCHAR(100),
        region          VARCHAR(100),
        cluster         VARCHAR(100),
        technology      VARCHAR(20),
        latitude        DOUBLE PRECISION,
        longitude       DOUBLE PRECISION,
        timestamp       TIMESTAMP NOT NULL,
        active_users    INTEGER,
        prb_utilization DOUBLE PRECISION,
        rsrp            DOUBLE PRECISION,
        sinr            DOUBLE PRECISION,
        throughput_dl   DOUBLE PRECISION,
        throughput_ul   DOUBLE PRECISION,
        packet_loss     DOUBLE PRECISION,
        latency         DOUBLE PRECISION,
        call_drop_rate  DOUBLE PRECISION,
        availability    DOUBLE PRECISION,
        traffic_volume  DOUBLE PRECISION,
        -- Extended RAN KPIs stored as top-level columns (populated from extra_kpis on upload)
        lte_rrc_setup_sr    DOUBLE PRECISION,
        lte_call_setup_sr   DOUBLE PRECISION,
        erab_setup_sr       DOUBLE PRECISION,
        erab_drop_rate      DOUBLE PRECISION,
        csfb_access_sr      DOUBLE PRECISION,
        intra_freq_ho_sr    DOUBLE PRECISION,
        dl_cell_tput        DOUBLE PRECISION,
        ul_cell_tput        DOUBLE PRECISION,
        dl_user_tput        DOUBLE PRECISION,
        ul_user_tput        DOUBLE PRECISION,
        avg_latency_dl      DOUBLE PRECISION,
        dl_data_vol         DOUBLE PRECISION,
        ul_data_vol         DOUBLE PRECISION,
        volte_traffic_erl   DOUBLE PRECISION,
        volte_traffic_ul    DOUBLE PRECISION,
        volte_traffic_dl    DOUBLE PRECISION,
        avg_rrc_ue          DOUBLE PRECISION,
        max_rrc_ue          DOUBLE PRECISION,
        avg_act_ue_dl       DOUBLE PRECISION,
        avg_act_ue_ul       DOUBLE PRECISION,
        avg_ni_carrier      DOUBLE PRECISION,
        dl_prb_util         DOUBLE PRECISION,
        ul_prb_util         DOUBLE PRECISION,
        extra_kpis          JSONB,
        uploaded_at         TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_nkt_timestamp  ON network_kpi_timeseries(timestamp);
    CREATE INDEX IF NOT EXISTS idx_nkt_site_id    ON network_kpi_timeseries(site_id);
    CREATE INDEX IF NOT EXISTS idx_nkt_region     ON network_kpi_timeseries(region);
    CREATE INDEX IF NOT EXISTS idx_nkt_cluster    ON network_kpi_timeseries(cluster);
    """
    with db.engine.connect() as conn:
        conn.execute(sa_text(ddl))
        conn.commit()


def _ensure_core_kpi_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS core_kpi_data (
        id              BIGSERIAL PRIMARY KEY,
        site_id         VARCHAR(100) NOT NULL,
        date            DATE NOT NULL,
        auth_sr         DOUBLE PRECISION,
        cpu_util        DOUBLE PRECISION,
        attach_sr       DOUBLE PRECISION,
        pdp_sr          DOUBLE PRECISION,
        uploaded_at     TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_core_site  ON core_kpi_data(site_id);
    CREATE INDEX IF NOT EXISTS idx_core_date  ON core_kpi_data(date);
    """
    with db.engine.connect() as conn:
        conn.execute(sa_text(ddl))
        conn.commit()


def _ensure_transport_kpi_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS transport_kpi_data (
        id              BIGSERIAL PRIMARY KEY,
        site_id         VARCHAR(100) NOT NULL,
        zone            VARCHAR(100),
        backhaul_type   VARCHAR(50),
        link_capacity   DOUBLE PRECISION,
        avg_util        DOUBLE PRECISION,
        peak_util       DOUBLE PRECISION,
        packet_loss     DOUBLE PRECISION,
        avg_latency     DOUBLE PRECISION,
        jitter          DOUBLE PRECISION,
        availability    DOUBLE PRECISION,
        error_rate      DOUBLE PRECISION,
        tput_efficiency DOUBLE PRECISION,
        alarms          INTEGER,
        uploaded_at     TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_tr_site ON transport_kpi_data(site_id);
    """
    with db.engine.connect() as conn:
        conn.execute(sa_text(ddl))
        conn.commit()


def _ensure_revenue_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS revenue_data (
        id              BIGSERIAL PRIMARY KEY,
        site_id         VARCHAR(100) NOT NULL,
        zone            VARCHAR(100),
        technology      VARCHAR(20),
        subscribers     INTEGER,
        rev_jan         DOUBLE PRECISION,
        rev_feb         DOUBLE PRECISION,
        rev_mar         DOUBLE PRECISION,
        opex_jan        DOUBLE PRECISION,
        opex_feb        DOUBLE PRECISION,
        opex_mar        DOUBLE PRECISION,
        site_category   VARCHAR(100),
        uploaded_at     TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_rev_site ON revenue_data(site_id);
    """
    with db.engine.connect() as conn:
        conn.execute(sa_text(ddl))
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Column alias maps for upload
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_COLS = {"SITE_ID", "TIMESTAMP"}
OPTIONAL_COLS = {
    "REGION", "CLUSTER", "CELL_ID", "LATITUDE", "LONGITUDE",
    "TECHNOLOGY", "ACTIVE_USERS", "PRB_UTILIZATION", "RSRP", "SINR",
    "THROUGHPUT_DL", "THROUGHPUT_UL", "PACKET_LOSS", "LATENCY",
    "CALL_DROP_RATE", "AVAILABILITY", "TRAFFIC_VOLUME",
    # Extended RAN
    "LTE_RRC_SETUP_SR", "LTE_CALL_SETUP_SR", "ERAB_SETUP_SR", "ERAB_DROP_RATE",
    "CSFB_ACCESS_SR", "INTRA_FREQ_HO_SR", "DL_CELL_TPUT", "UL_CELL_TPUT",
    "DL_USER_TPUT", "UL_USER_TPUT", "AVG_LATENCY_DL", "DL_DATA_VOL",
    "UL_DATA_VOL", "VOLTE_TRAFFIC_ERL", "VOLTE_TRAFFIC_UL", "VOLTE_TRAFFIC_DL",
    "AVG_RRC_UE", "MAX_RRC_UE", "AVG_ACT_UE_DL", "AVG_ACT_UE_UL",
    "AVG_NI_CARRIER", "DL_PRB_UTIL", "UL_PRB_UTIL",
}
_COL_ALIASES = {
    "SITE": "SITE_ID", "SITE_NAME": "SITE_ID", "SITEID": "SITE_ID",
    "CELL": "CELL_ID", "CELLID": "CELL_ID",
    "TS": "TIMESTAMP", "TIME": "TIMESTAMP", "DATETIME": "TIMESTAMP", "DATE_TIME": "TIMESTAMP",
    "PRB": "PRB_UTILIZATION", "PRB_UTIL": "PRB_UTILIZATION", "PRB_UTILISATION": "PRB_UTILIZATION",
    "DL PRB UTILIZATION (1BH)": "DL_PRB_UTIL", "UL PRB UTILIZATION (1BH)": "UL_PRB_UTIL",
    "LTE RRC SETUP SUCCESS RATE": "LTE_RRC_SETUP_SR",
    "LTE CALL SETUP SUCCESS RATE": "LTE_CALL_SETUP_SR",
    "LTE E-RAB SETUP SUCCESS RATE": "ERAB_SETUP_SR",
    "E-RAB CALL DROP RATE_1": "ERAB_DROP_RATE",
    "CSFB ACCESS SUCCESS RATE": "CSFB_ACCESS_SR",
    "LTE INTRA-FREQ HO SUCCESS RATE": "INTRA_FREQ_HO_SR",
    "LTE DL - CELL AVE THROUGHPUT": "DL_CELL_TPUT",
    "LTE UL - CELL AVE THROUGHPUT": "UL_CELL_TPUT",
    "LTE DL - USR AVE THROUGHPUT": "DL_USER_TPUT",
    "LTE UL - USER AVE THROUGHPUT": "UL_USER_TPUT",
    "AVERAGE LATENCY DOWNLINK": "AVG_LATENCY_DL",
    "DL DATA TOTAL VOLUME": "DL_DATA_VOL",
    "UL DATA TOTAL VOLUME": "UL_DATA_VOL",
    "VOLTE TRAFFIC ERLANG": "VOLTE_TRAFFIC_ERL",
    "AVE RRC CONNECTED UE": "AVG_RRC_UE",
    "MAX RRC CONNECTED UE": "MAX_RRC_UE",
    "AVERAGE ACT UE DL PER CELL": "AVG_ACT_UE_DL",
    "AVERAGE ACT UE UL PER CELL": "AVG_ACT_UE_UL",
    "AVERAGE NI OF CARRIER-": "AVG_NI_CARRIER",
    "DL_THROUGHPUT": "THROUGHPUT_DL", "TPUT_DL": "THROUGHPUT_DL",
    "UL_THROUGHPUT": "THROUGHPUT_UL", "TPUT_UL": "THROUGHPUT_UL",
    "PKT_LOSS": "PACKET_LOSS", "PKTLOSS": "PACKET_LOSS",
    "CDR": "CALL_DROP_RATE", "CALL_DROP": "CALL_DROP_RATE",
    "AVAIL": "AVAILABILITY",
    "TRAFFIC": "TRAFFIC_VOLUME", "TRAFFIC_VOL": "TRAFFIC_VOLUME",
    "LAT": "LATITUDE", "LNG": "LONGITUDE", "LON": "LONGITUDE", "LONG": "LONGITUDE",
    "TECH": "TECHNOLOGY", "ZONE": "CLUSTER",
}


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN: Upload Network KPI Excel (RAN timeseries)
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/admin/upload-network-data", methods=["POST"])
@jwt_required()
def upload_network_data():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    raw = f.read()
    ext = (f.filename or "").rsplit(".", 1)[-1].lower()
    df = None

    # Try to read ? Excel: scan all sheets/rows for a header row containing Site ID + Timestamp; CSV fallback
    if ext in ("xlsx", "xls"):
        try:
            engine = "xlrd" if ext == "xls" else "openpyxl"
            sheets = pd.read_excel(io.BytesIO(raw), engine=engine, header=None, sheet_name=None)
            for _sheet_name, df_raw in sheets.items():
                if df_raw is None or df_raw.empty:
                    continue
                header_row_idx = None
                for i in range(min(200, len(df_raw))):
                    row_vals = [
                        str(v).strip()
                        for v in df_raw.iloc[i].tolist()
                        if str(v).strip() not in ("", "nan", "None")
                    ]
                    normed = {_flex_normalise_col(v) for v in row_vals}
                    has_site = any(v in normed for v in ("site_id", "siteid", "site"))
                    has_time = any(v in normed for v in ("timestamp", "time", "date", "datetime"))
                    if has_site and has_time:
                        header_row_idx = i
                        break
                if header_row_idx is not None:
                    header = df_raw.iloc[header_row_idx].tolist()
                    df = df_raw.iloc[header_row_idx + 1:].copy()
                    df.columns = header
                    df = df.dropna(axis=1, how="all")
                    if df is not None and len(df.columns) >= 1:
                        break
        except Exception:
            df = None

    if df is None:
        try:
            if ext in ("xlsx", "xls"):
                engine = "xlrd" if ext == "xls" else "openpyxl"
                for hdr in [1, 0]:
                    try:
                        df = pd.read_excel(io.BytesIO(raw), engine=engine, header=hdr)
                        if df is not None and len(df.columns) >= 1:
                            break
                    except Exception:
                        continue
            else:
                df = pd.read_csv(io.BytesIO(raw))
        except Exception as e:
            return jsonify({"error": f"Could not parse file: {e}"}), 400

    if df is None or df.empty:
        return jsonify({"error": "File is empty or unreadable"}), 400

    # Normalise column names
    df.columns = [
        str(c).strip().upper()
        .replace(" ", "_").replace("(", "").replace(")", "")
        .replace("%", "PCT").replace("/", "_").replace("-", "_")
        for c in df.columns
    ]
    df = df.loc[:, ~df.columns.str.startswith("UNNAMED")]
    df = df.dropna(how="all")

    # Apply alias mapping
    df.rename(columns=_COL_ALIASES, inplace=True)

    # Verify required columns
    if not REQUIRED_COLS.issubset(set(df.columns)):
        return jsonify({
            "error": "Missing mandatory columns: SITE_ID and TIMESTAMP",
            "detected_columns": list(df.columns),
        }), 400

    try:
        _ensure_network_kpi_table()
    except Exception as e:
        return jsonify({"error": f"DB schema error: {e}"}), 500

    # Clean rows
    df = df.dropna(subset=["SITE_ID", "TIMESTAMP"])
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
    df = df[df["TIMESTAMP"].notna()]
    if df.empty:
        return jsonify({"error": "No valid rows after filtering SITE_ID/TIMESTAMP"}), 400

    def _sv(row, col):
        v = row.get(col)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return v

    def _fv(row, col):
        v = _sv(row, col)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    known_cols = {
        "SITE_ID", "CELL_ID", "REGION", "CLUSTER", "TECHNOLOGY", "LATITUDE", "LONGITUDE",
        "TIMESTAMP", "ACTIVE_USERS", "PRB_UTILIZATION", "RSRP", "SINR", "THROUGHPUT_DL",
        "THROUGHPUT_UL", "PACKET_LOSS", "LATENCY", "CALL_DROP_RATE", "AVAILABILITY",
        "TRAFFIC_VOLUME", "LTE_RRC_SETUP_SR", "LTE_CALL_SETUP_SR", "ERAB_SETUP_SR",
        "ERAB_DROP_RATE", "CSFB_ACCESS_SR", "INTRA_FREQ_HO_SR", "DL_CELL_TPUT",
        "UL_CELL_TPUT", "DL_USER_TPUT", "UL_USER_TPUT", "AVG_LATENCY_DL", "DL_DATA_VOL",
        "UL_DATA_VOL", "VOLTE_TRAFFIC_ERL", "VOLTE_TRAFFIC_UL", "VOLTE_TRAFFIC_DL",
        "AVG_RRC_UE", "MAX_RRC_UE", "AVG_ACT_UE_DL", "AVG_ACT_UE_UL", "AVG_NI_CARRIER",
        "DL_PRB_UTIL", "UL_PRB_UTIL",
    }

    rows = []
    for _, row in df.iterrows():
        sid = str(_sv(row, "SITE_ID") or "").strip()
        if not sid:
            continue
        ts = row.get("TIMESTAMP")
        if ts is None or str(ts).lower() in ("nan", ""):
            continue
        extra = {c: row.get(c) for c in df.columns if c not in known_cols and row.get(c) is not None}
        rows.append({
            "site_id": sid,
            "cell_id": str(_sv(row, "CELL_ID") or "").strip() or None,
            "region": str(_sv(row, "REGION") or "").strip() or None,
            "cluster": str(_sv(row, "CLUSTER") or "").strip() or None,
            "technology": str(_sv(row, "TECHNOLOGY") or "").strip() or None,
            "latitude": _fv(row, "LATITUDE"),
            "longitude": _fv(row, "LONGITUDE"),
            "timestamp": ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
            "active_users": int(_fv(row, "ACTIVE_USERS") or 0) if _sv(row, "ACTIVE_USERS") is not None else None,
            "prb_utilization": _fv(row, "PRB_UTILIZATION"),
            "rsrp": _fv(row, "RSRP"),
            "sinr": _fv(row, "SINR"),
            "throughput_dl": _fv(row, "THROUGHPUT_DL"),
            "throughput_ul": _fv(row, "THROUGHPUT_UL"),
            "packet_loss": _fv(row, "PACKET_LOSS"),
            "latency": _fv(row, "LATENCY"),
            "call_drop_rate": _fv(row, "CALL_DROP_RATE"),
            "availability": _fv(row, "AVAILABILITY"),
            "traffic_volume": _fv(row, "TRAFFIC_VOLUME"),
            "lte_rrc_setup_sr": _fv(row, "LTE_RRC_SETUP_SR"),
            "lte_call_setup_sr": _fv(row, "LTE_CALL_SETUP_SR"),
            "erab_setup_sr": _fv(row, "ERAB_SETUP_SR"),
            "erab_drop_rate": _fv(row, "ERAB_DROP_RATE"),
            "csfb_access_sr": _fv(row, "CSFB_ACCESS_SR"),
            "intra_freq_ho_sr": _fv(row, "INTRA_FREQ_HO_SR"),
            "dl_cell_tput": _fv(row, "DL_CELL_TPUT"),
            "ul_cell_tput": _fv(row, "UL_CELL_TPUT"),
            "dl_user_tput": _fv(row, "DL_USER_TPUT"),
            "ul_user_tput": _fv(row, "UL_USER_TPUT"),
            "avg_latency_dl": _fv(row, "AVG_LATENCY_DL"),
            "dl_data_vol": _fv(row, "DL_DATA_VOL"),
            "ul_data_vol": _fv(row, "UL_DATA_VOL"),
            "volte_traffic_erl": _fv(row, "VOLTE_TRAFFIC_ERL"),
            "volte_traffic_ul": _fv(row, "VOLTE_TRAFFIC_UL"),
            "volte_traffic_dl": _fv(row, "VOLTE_TRAFFIC_DL"),
            "avg_rrc_ue": _fv(row, "AVG_RRC_UE"),
            "max_rrc_ue": _fv(row, "MAX_RRC_UE"),
            "avg_act_ue_dl": _fv(row, "AVG_ACT_UE_DL"),
            "avg_act_ue_ul": _fv(row, "AVG_ACT_UE_UL"),
            "avg_ni_carrier": _fv(row, "AVG_NI_CARRIER"),
            "dl_prb_util": _fv(row, "DL_PRB_UTIL"),
            "ul_prb_util": _fv(row, "UL_PRB_UTIL"),
            "extra_kpis": extra if extra else None,
        })

    if not rows:
        return jsonify({"error": "No valid rows found after parsing"}), 400

    BATCH = 1000
    inserted = 0
    try:
        with db.engine.connect() as conn:
            for i in range(0, len(rows), BATCH):
                chunk = rows[i: i + BATCH]
                conn.execute(sa_text("""
                    INSERT INTO network_kpi_timeseries
                    (site_id, cell_id, region, cluster, technology, latitude, longitude, timestamp,
                     active_users, prb_utilization, rsrp, sinr, throughput_dl, throughput_ul, packet_loss,
                     latency, call_drop_rate, availability, traffic_volume, lte_rrc_setup_sr,
                     lte_call_setup_sr, erab_setup_sr, erab_drop_rate, csfb_access_sr, intra_freq_ho_sr,
                     dl_cell_tput, ul_cell_tput, dl_user_tput, ul_user_tput, avg_latency_dl, dl_data_vol,
                     ul_data_vol, volte_traffic_erl, volte_traffic_ul, volte_traffic_dl, avg_rrc_ue,
                     max_rrc_ue, avg_act_ue_dl, avg_act_ue_ul, avg_ni_carrier, dl_prb_util, ul_prb_util, extra_kpis)
                    VALUES
                    (:site_id, :cell_id, :region, :cluster, :technology, :latitude, :longitude, :timestamp,
                     :active_users, :prb_utilization, :rsrp, :sinr, :throughput_dl, :throughput_ul, :packet_loss,
                     :latency, :call_drop_rate, :availability, :traffic_volume, :lte_rrc_setup_sr,
                     :lte_call_setup_sr, :erab_setup_sr, :erab_drop_rate, :csfb_access_sr, :intra_freq_ho_sr,
                     :dl_cell_tput, :ul_cell_tput, :dl_user_tput, :ul_user_tput, :avg_latency_dl, :dl_data_vol,
                     :ul_data_vol, :volte_traffic_erl, :volte_traffic_ul, :volte_traffic_dl, :avg_rrc_ue,
                     :max_rrc_ue, :avg_act_ue_dl, :avg_act_ue_ul, :avg_ni_carrier, :dl_prb_util, :ul_prb_util, :extra_kpis)
                """), chunk)
                conn.commit()
                inserted += len(chunk)
    except SQLAlchemyError as e:
        return jsonify({"error": f"DB insert failed: {e}"}), 500

    _CACHE.clear()
    return jsonify({"success": True, "records_processed": inserted})

@network_bp.route("/api/admin/upload-transport-data", methods=["POST"])
@jwt_required()
def upload_transport_data():
    """Fully dynamic transport KPI upload.
    Only site_id is mandatory. Column names, count, order and case are all arbitrary.
    Every column is auto-detected (numeric vs text) and stored in the schema-flexible
    FlexibleKpiUpload table with kpi_type='transport'. The legacy transport_kpi_data
    table is still populated opportunistically when recognisable metrics are present,
    so existing analytics queries keep working.
    """
    import uuid as _uuid

    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    raw = f.read()
    ext = (f.filename or "").rsplit(".", 1)[-1].lower()
    df = None

    # ── Robust header detection: scan first 200 rows of every sheet ──
    if ext in ("xlsx", "xls", "xlsm"):
        try:
            engine = "xlrd" if ext == "xls" else "openpyxl"
            sheets = pd.read_excel(io.BytesIO(raw), engine=engine, header=None, sheet_name=None)
            for _sheet_name, df_raw in sheets.items():
                if df_raw is None or df_raw.empty:
                    continue
                header_row_idx = None
                for i in range(min(200, len(df_raw))):
                    row_vals = [
                        str(v).strip()
                        for v in df_raw.iloc[i].tolist()
                        if str(v).strip() not in ("", "nan", "None")
                    ]
                    for v in row_vals:
                        if _re.search(r"site\s*[_\-]?\s*id|^site$|site\s*name", v, flags=_re.IGNORECASE):
                            header_row_idx = i
                            break
                    if header_row_idx is not None:
                        break
                if header_row_idx is not None:
                    header = df_raw.iloc[header_row_idx].tolist()
                    df = df_raw.iloc[header_row_idx + 1:].copy()
                    df.columns = header
                    df = df.dropna(axis=1, how="all")
                    if df is not None and len(df.columns) >= 1:
                        break
        except Exception:
            df = None

    if df is None:
        try:
            if ext in ("xlsx", "xls", "xlsm"):
                engine = "xlrd" if ext == "xls" else "openpyxl"
                for hdr in [1, 0]:
                    try:
                        df = pd.read_excel(io.BytesIO(raw), engine=engine, header=hdr)
                        if df is not None and len(df.columns) >= 1:
                            break
                    except Exception:
                        continue
            else:
                df = pd.read_csv(io.BytesIO(raw))
        except Exception as e:
            return jsonify({"error": f"Could not parse file: {e}"}), 400

    if df is None or df.empty:
        return jsonify({"error": "File is empty or unreadable"}), 400

    # Drop empty rows / unnamed-only columns
    df = df.dropna(how="all")
    df = df.loc[:, [c for c in df.columns if not str(c).lower().startswith("unnamed")]]

    # ── Locate site_id column by fuzzy match ────────────────────────
    def _norm(s):
        return _re.sub(r"[^a-z0-9]+", "_", str(s).strip().lower()).strip("_")

    site_col = None
    for c in df.columns:
        n = _norm(c)
        if n in ("site_id", "siteid", "site", "site_name", "sitename", "node_id", "cell_id"):
            site_col = c
            break
    if site_col is None:
        for c in df.columns:
            if _re.search(r"site", str(c), flags=_re.IGNORECASE):
                site_col = c
                break
    if site_col is None:
        return jsonify({
            "error": "Missing mandatory column: any column containing 'site' / 'site_id'",
            "detected_columns": [str(c) for c in df.columns],
        }), 400

    # ── Detect column type per data column (numeric vs text) ────────
    def _detect_type(series):
        vals = [v for v in series.tolist() if v is not None and not (isinstance(v, float) and math.isnan(v))]
        if not vals:
            return "text"
        numeric_count = 0
        for v in vals[:50]:
            if isinstance(v, (int, float)):
                numeric_count += 1
            else:
                try:
                    float(str(v).replace(",", "").strip())
                    numeric_count += 1
                except (TypeError, ValueError):
                    pass
        return "numeric" if numeric_count >= max(1, len(vals[:50]) * 0.5) else "text"

    other_cols = [c for c in df.columns if c != site_col]
    col_types = {c: _detect_type(df[c]) for c in other_cols}

    # ── Insert into FlexibleKpiUpload (EAV) ─────────────────────────
    batch_id = str(_uuid.uuid4())
    records = []
    unique_sites = set()
    inserted = 0
    CHUNK = 2000

    def _flush(batch):
        if batch:
            db.session.bulk_save_objects(batch)
            db.session.flush()
        return []

    try:
        # Clear previous transport uploads for a clean replace
        FlexibleKpiUpload.query.filter_by(kpi_type="transport").delete()
        db.session.flush()

        for _, row in df.iterrows():
            sid_raw = row.get(site_col)
            if sid_raw is None or (isinstance(sid_raw, float) and math.isnan(sid_raw)):
                continue
            sid = str(sid_raw).strip()
            if not sid or sid.lower() in ("nan", "none", "site_id", "site id"):
                continue
            unique_sites.add(sid)

            for col in other_cols:
                val = row.get(col)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    continue
                ctype = col_types[col]
                col_name = _norm(col) or f"col_{other_cols.index(col)}"
                if ctype == "numeric":
                    try:
                        num = float(val) if isinstance(val, (int, float)) else float(str(val).replace(",", "").strip())
                    except (TypeError, ValueError):
                        continue
                    if math.isnan(num) or math.isinf(num):
                        continue
                    records.append(FlexibleKpiUpload(
                        kpi_type="transport",
                        upload_batch=batch_id,
                        site_id=sid,
                        column_name=col_name,
                        column_type="numeric",
                        num_value=num,
                        str_value=str(col)[:200],
                    ))
                else:
                    sv = str(val).replace("\n", " ").replace("\r", " ").strip()
                    if not sv:
                        continue
                    records.append(FlexibleKpiUpload(
                        kpi_type="transport",
                        upload_batch=batch_id,
                        site_id=sid,
                        column_name=col_name,
                        column_type="text",
                        num_value=None,
                        str_value=sv[:500],
                    ))

                if len(records) >= CHUNK:
                    inserted += len(records)
                    records = _flush(records)

        inserted += len(records)
        _flush(records)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": f"Upload failed: {type(e).__name__}: {str(e)[:300]}",
            "detected_columns": [str(c) for c in df.columns],
        }), 500

    # ── Opportunistically also populate legacy transport_kpi_data ──
    # so existing transport-analytics queries keep working. Best-effort only.
    try:
        _populate_legacy_transport_table(df, site_col, col_types)
    except Exception as e:
        print(f">>> Legacy transport table populate skipped: {e}")

    _CACHE.clear()
    return jsonify({
        "success": True,
        "records_processed": inserted,
        "unique_sites": len(unique_sites),
        "columns_detected": [str(c) for c in other_cols],
        "column_types": {str(k): v for k, v in col_types.items()},
        "site_id_column": str(site_col),
    })


def _populate_legacy_transport_table(df, site_col, col_types):
    """Best-effort: map a dynamic dataframe into the legacy transport_kpi_data
    schema by fuzzy-matching common column names. If a column isn't present,
    it's simply left NULL. Failures here are non-fatal — the flexible table
    always has the full payload."""
    def _norm(s):
        return _re.sub(r"[^a-z0-9]+", "_", str(s).strip().lower()).strip("_")

    # Fuzzy header → logical key mapping
    PATTERNS = {
        "zone":            [r"zone", r"cluster", r"region", r"area", r"province"],
        "backhaul_type":   [r"backhaul", r"link_type", r"connection_type", r"medium", r"technology"],
        "link_capacity":   [r"link_capacity", r"capacity", r"bandwidth"],
        "avg_util":        [r"avg.*util", r"link.*util", r"^util"],
        "peak_util":       [r"peak.*util"],
        "packet_loss":     [r"packet.*loss", r"pkt.*loss", r"^loss"],
        "avg_latency":     [r"avg.*latency", r"^latency", r"rtt", r"delay"],
        "jitter":          [r"jitter"],
        "availability":    [r"link.*avail", r"^avail", r"uptime"],
        "error_rate":      [r"error.*rate", r"^ber", r"error.*pct"],
        "tput_efficiency": [r"throughput.*eff", r"tput.*eff", r"^efficiency"],
        "alarms":          [r"alarm", r"num.*alarm", r"active.*alarm"],
    }

    col_for_key = {}
    for key, patterns in PATTERNS.items():
        for c in df.columns:
            if c == site_col:
                continue
            cn = _norm(c)
            for pat in patterns:
                if _re.search(pat, cn):
                    col_for_key[key] = c
                    break
            if key in col_for_key:
                break

    try:
        _ensure_transport_kpi_table()
    except Exception as e:
        print(f">>> _ensure_transport_kpi_table failed: {e}")
        return

    records = []
    for _, row in df.iterrows():
        sid = row.get(site_col)
        if sid is None or (isinstance(sid, float) and math.isnan(sid)):
            continue
        sid = str(sid).strip()
        if not sid or sid.lower() in ("nan", "none"):
            continue

        def _get_str(k):
            if k not in col_for_key:
                return ""
            v = row.get(col_for_key[k])
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return ""
            return str(v).strip()

        def _get_float(k):
            if k not in col_for_key:
                return None
            v = row.get(col_for_key[k])
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None
            try:
                return float(v) if isinstance(v, (int, float)) else float(str(v).replace(",", "").strip())
            except (TypeError, ValueError):
                return None

        records.append({
            "site_id":         sid,
            "zone":            _get_str("zone"),
            "backhaul_type":   _get_str("backhaul_type"),
            "link_capacity":   _get_float("link_capacity"),
            "avg_util":        _get_float("avg_util"),
            "peak_util":       _get_float("peak_util"),
            "packet_loss":     _get_float("packet_loss"),
            "avg_latency":     _get_float("avg_latency"),
            "jitter":          _get_float("jitter"),
            "availability":    _get_float("availability"),
            "error_rate":      _get_float("error_rate"),
            "tput_efficiency": _get_float("tput_efficiency"),
            "alarms":          int(_get_float("alarms") or 0),
        })

    if not records:
        return

    BATCH = 1000
    with db.engine.connect() as conn:
        conn.execute(sa_text("DELETE FROM transport_kpi_data"))
        conn.commit()
        for i in range(0, len(records), BATCH):
            batch = records[i: i + BATCH]
            conn.execute(sa_text("""
                INSERT INTO transport_kpi_data
                (site_id, zone, backhaul_type, link_capacity, avg_util, peak_util,
                 packet_loss, avg_latency, jitter, availability, error_rate, tput_efficiency, alarms)
                VALUES
                (:site_id, :zone, :backhaul_type, :link_capacity, :avg_util, :peak_util,
                 :packet_loss, :avg_latency, :jitter, :availability, :error_rate, :tput_efficiency, :alarms)
            """), batch)
            conn.commit()

@network_bp.route("/api/admin/upload-revenue-data", methods=["POST"])
@jwt_required()
def upload_revenue_data():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    raw = f.read()
    ext = (f.filename or "").rsplit(".", 1)[-1].lower()
    df = None

    if ext in ("xlsx", "xls"):
        try:
            engine = "xlrd" if ext == "xls" else "openpyxl"
            sheets = pd.read_excel(io.BytesIO(raw), engine=engine, header=None, sheet_name=None)
            for _sheet_name, df_raw in sheets.items():
                if df_raw is None or df_raw.empty:
                    continue
                header_row_idx = None
                for i in range(min(200, len(df_raw))):
                    row_vals = [
                        str(v).strip()
                        for v in df_raw.iloc[i].tolist()
                        if str(v).strip() not in ("", "nan", "None")
                    ]
                    normed = {_flex_normalise_col(v) for v in row_vals}
                    if any(v in normed for v in ("site_id", "siteid", "site")):
                        header_row_idx = i
                        break
                if header_row_idx is not None:
                    header = df_raw.iloc[header_row_idx].tolist()
                    df = df_raw.iloc[header_row_idx + 1:].copy()
                    df.columns = header
                    df = df.dropna(axis=1, how="all")
                    if df is not None and len(df.columns) >= 1:
                        break
        except Exception:
            df = None

    if df is None:
        try:
            if ext in ("xlsx", "xls"):
                engine = "xlrd" if ext == "xls" else "openpyxl"
                for hdr in [1, 0]:
                    try:
                        df = pd.read_excel(io.BytesIO(raw), engine=engine, header=hdr)
                        if df is not None and len(df.columns) >= 1:
                            break
                    except Exception:
                        continue
            else:
                df = pd.read_csv(io.BytesIO(raw))
        except Exception as e:
            return jsonify({"error": f"Could not parse file: {e}"}), 400

    if df is None or df.empty:
        return jsonify({"error": "File is empty or unreadable"}), 400

    df.columns = [str(c).strip() for c in df.columns]
    col_map = {_flex_normalise_col(c): c for c in df.columns}
    site_col = None
    for candidate in ("site_id", "siteid", "site"):
        if candidate in col_map:
            site_col = col_map[candidate]
            break
    if not site_col:
        for norm_col, raw_col in col_map.items():
            if "site" in norm_col and "id" in norm_col:
                site_col = raw_col
                break
    if not site_col:
        return jsonify({"error": "Missing mandatory column: Site ID", "detected_columns": list(df.columns)}), 400

    def _find_col(*keys):
        for k in keys:
            if k in col_map:
                return col_map[k]
        for k in keys:
            for n, raw in col_map.items():
                if k in n:
                    return raw
        return None

    zone_col = _find_col("zone", "cluster", "region")
    tech_col = _find_col("technology", "tech")
    subs_col = _find_col("subscribers", "subscriber", "subs")
    rev_jan_col = _find_col("rev_jan", "revenue_jan", "revenuejan", "jan_revenue")
    rev_feb_col = _find_col("rev_feb", "revenue_feb", "revenuefeb", "feb_revenue")
    rev_mar_col = _find_col("rev_mar", "revenue_mar", "revenuemar", "mar_revenue")
    opex_jan_col = _find_col("opex_jan", "op_ex_jan", "opexjan", "jan_opex")
    opex_feb_col = _find_col("opex_feb", "op_ex_feb", "opexfeb", "feb_opex")
    opex_mar_col = _find_col("opex_mar", "op_ex_mar", "opexmar", "mar_opex")
    cat_col = _find_col("site_category", "category")

    try:
        _ensure_revenue_table()
    except Exception as e:
        return jsonify({"error": f"DB schema error: {e}"}), 500

    def _sv(row, col):
        if not col:
            return None
        v = row.get(col)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return v

    def _fv(row, col):
        v = _sv(row, col)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    records = []
    for _, row in df.iterrows():
        sid = _sv(row, site_col)
        if not sid or str(sid).lower() in ("nan", ""):
            continue
        records.append({
            "site_id": str(sid),
            "zone": str(_sv(row, zone_col) or ""),
            "technology": str(_sv(row, tech_col) or ""),
            "subscribers": int(_fv(row, subs_col) or 0),
            "rev_jan": _fv(row, rev_jan_col),
            "rev_feb": _fv(row, rev_feb_col),
            "rev_mar": _fv(row, rev_mar_col),
            "opex_jan": _fv(row, opex_jan_col),
            "opex_feb": _fv(row, opex_feb_col),
            "opex_mar": _fv(row, opex_mar_col),
            "site_category": str(_sv(row, cat_col) or ""),
        })

    if not records:
        return jsonify({"error": "No valid rows found. Ensure Site ID column has data."}), 400

    BATCH = 1000
    inserted = 0
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text("DELETE FROM revenue_data"))
            conn.commit()
            for i in range(0, len(records), BATCH):
                batch = records[i: i + BATCH]
                conn.execute(sa_text("""
                    INSERT INTO revenue_data
                    (site_id, zone, technology, subscribers, rev_jan, rev_feb, rev_mar,
                     opex_jan, opex_feb, opex_mar, site_category)
                    VALUES
                    (:site_id, :zone, :technology, :subscribers, :rev_jan, :rev_feb, :rev_mar,
                     :opex_jan, :opex_feb, :opex_mar, :site_category)
                """), batch)
                conn.commit()
                inserted += len(batch)
    except SQLAlchemyError as e:
        return jsonify({"error": f"DB insert failed: {e}"}), 500

    _CACHE.clear()
    return jsonify({"success": True, "records_processed": inserted})

@network_bp.route("/api/network/geo-center", methods=["GET"])
@jwt_required()
def network_geo_center():
    """Return the geographic center and bounds of all sites in DB.
    Used by frontend maps to auto-center on actual site locations."""
    ck = "geo_center_v1"
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)
    try:
        rows = _sql("""
            SELECT AVG(latitude) AS lat, AVG(longitude) AS lng,
                   MIN(latitude) AS lat_min, MAX(latitude) AS lat_max,
                   MIN(longitude) AS lng_min, MAX(longitude) AS lng_max,
                   COUNT(DISTINCT site_id) AS sites,
                   (SELECT country FROM telecom_sites WHERE country IS NOT NULL AND country != '' GROUP BY country ORDER BY COUNT(*) DESC LIMIT 1) AS primary_country
            FROM telecom_sites
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        if rows and rows[0].get("lat"):
            r = rows[0]
            result = {
                "center": [round(float(r["lat"]), 4), round(float(r["lng"]), 4)],
                "bounds": [[float(r["lat_min"]), float(r["lng_min"])], [float(r["lat_max"]), float(r["lng_max"])]],
                "sites": int(r["sites"] or 0),
                "country": r.get("primary_country") or "",
                "zoom": 6 if int(r["sites"] or 0) > 50 else 10,
            }
        else:
            result = {"center": [11.5564, 104.9282], "bounds": None, "sites": 0, "country": "", "zoom": 6}
        _to_cache(ck, result)
        return jsonify(result)
    except Exception as e:
        _LOG.error("geo-center: %s", e)
        return jsonify({"center": [11.5564, 104.9282], "bounds": None, "sites": 0, "country": "", "zoom": 6})


@network_bp.route("/api/network/site-locations", methods=["GET"])
@jwt_required()
def network_site_locations():
    """Return all site lat/lng with province/commune for map display.
    Groups sites by province and returns markers + province summary."""
    ck = "site_locs_v1"
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)
    try:
        rows = _sql("""
            SELECT site_id, site_name, province, commune, zone, city, state, country,
                   AVG(latitude) AS lat, AVG(longitude) AS lng,
                   COUNT(*) AS cells
            FROM telecom_sites
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY site_id, site_name, province, commune, zone, city, state, country
            ORDER BY province, site_id
            LIMIT 2000
        """)
        sites = []
        prov_summary = {}
        for r in rows:
            prov = r.get("province") or r.get("zone") or r.get("state") or "Unknown"
            lat = round(float(r["lat"]), 6)
            lng = round(float(r["lng"]), 6)
            sites.append({
                "site_id": r["site_id"],
                "site_name": r.get("site_name") or r["site_id"],
                "province": prov,
                "commune": r.get("commune") or r.get("city") or "",
                "country": r.get("country") or "",
                "lat": lat, "lng": lng,
                "cells": int(r.get("cells") or 1),
            })
            if prov not in prov_summary:
                prov_summary[prov] = {"sites": 0, "lat_sum": 0, "lng_sum": 0}
            prov_summary[prov]["sites"] += 1
            prov_summary[prov]["lat_sum"] += lat
            prov_summary[prov]["lng_sum"] += lng

        provinces = []
        for prov, ps in sorted(prov_summary.items(), key=lambda x: -x[1]["sites"]):
            n = ps["sites"]
            provinces.append({
                "province": prov,
                "sites": n,
                "center": [round(ps["lat_sum"]/n, 4), round(ps["lng_sum"]/n, 4)],
            })

        result = {"sites": sites, "provinces": provinces, "total": len(sites)}
        _to_cache(ck, result)
        return jsonify(result)
    except Exception as e:
        _LOG.error("site-locations: %s", e)
        return jsonify({"sites": [], "provinces": [], "total": 0})


@network_bp.route("/api/network/summary", methods=["GET"])
@jwt_required()
def network_summary():
    """Overall network health KPIs. Reads from kpi_data (primary) or network_kpi_timeseries."""
    filters = _get_filters()
    ck = _cache_key("summary_v6", filters)
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    # PRIMARY: kpi_data — aggregate KPIs (fast, indexed)
    agg = _kpi_network_agg(filters)

    # Direct no-join safety net
    if not any(agg.get(k) for k in ("dl_prb_util", "dl_cell_tput", "lte_rrc_setup_sr",
                                      "erab_drop_rate", "avg_rrc_ue", "availability")):
        try:
            direct_rows = _sql("SELECT kpi_name, AVG(value) AS avg_val, COUNT(DISTINCT site_id) AS n FROM kpi_data_merged WHERE value IS NOT NULL GROUP BY kpi_name")
            for r2 in direct_rows:
                col = _kpi_col(r2["kpi_name"])
                if col and r2["avg_val"] is not None:
                    agg[col] = _f(r2["avg_val"], 3)
                if int(r2.get("n") or 0) > agg.get("total_sites", 0):
                    agg["total_sites"] = int(r2["n"])
        except Exception:
            pass

    # Use filtered site count from _kpi_network_agg; fall back to telecom_sites
    n_sites = int(agg.get("total_sites") or 0)
    n_cells = n_sites
    if not n_sites:
        try:
            _cell_col = _telecom_sites_cell_col()
            _cell_expr = "NULL" if not _cell_col else f"NULLIF(COALESCE({_cell_col}, ''), '')"
            ts_cnt = _sql("""
                SELECT
                    COUNT(DISTINCT site_id) AS s,
                    COUNT(DISTINCT {cell_expr}) AS c
                FROM telecom_sites
            """.format(cell_expr=_cell_expr))
            n_sites = int(ts_cnt[0].get("s") or 0) if ts_cnt else 0
            n_cells = int(ts_cnt[0].get("c") or 0) if ts_cnt else n_sites
        except Exception:
            pass

    r: dict = {}
    if n_sites > 0:
        r = {
            "total_sites":      n_sites,
            "active_cells":     n_cells,
            "avg_prb":          agg.get("dl_prb_util", 0),
            "avg_dl_tput":      agg.get("dl_cell_tput", 0),
            "avg_drop_rate":    agg.get("erab_drop_rate", 0),
            "avg_rrc_ue":       agg.get("avg_rrc_ue", 0),
            "avg_availability": agg.get("availability", 0),
            "avg_rrc_sr":       agg.get("lte_rrc_setup_sr", 0),
        }
    else:
        # FALLBACK: network_kpi_timeseries (populated via Upload Network KPI Data)
        try:
            where, params = _build_where(filters)
            rows = _sql(f"""
                SELECT COUNT(DISTINCT site_id) AS total_sites,
                       COUNT(DISTINCT cell_id) AS active_cells,
                       AVG(COALESCE(dl_prb_util, prb_utilization)) AS avg_prb,
                       AVG(COALESCE(dl_cell_tput, throughput_dl))  AS avg_dl_tput,
                       AVG(COALESCE(erab_drop_rate, call_drop_rate, 0)) AS avg_drop_rate,
                       AVG(COALESCE(avg_rrc_ue, active_users, 0))  AS avg_rrc_ue,
                       AVG(availability) AS avg_availability,
                       AVG(packet_loss)  AS avg_packet_loss,
                       AVG(sinr) AS avg_sinr,
                       AVG(lte_rrc_setup_sr) AS avg_rrc_sr
                FROM network_kpi_timeseries WHERE {where}
            """, params)
            r = rows[0] if rows else {}
        except Exception:
            r = {}

    avail = _f(r.get("avg_availability"), 2) or 99.0
    pl    = _f(r.get("avg_packet_loss"),  2) or 0.0
    sinr  = _f(r.get("avg_sinr"), 2) or 10.0
    prb   = _f(r.get("avg_prb")) or 0.0
    drop  = _f(r.get("avg_drop_rate"), 2) or 0.0
    sinr_norm = min(max((sinr + 10) / 30 * 100, 0), 100)
    health = (
        0.35 * min(avail, 100) +
        0.25 * (100 - min(drop * 20, 100)) +
        0.20 * (100 - min(prb, 100)) +
        0.20 * sinr_norm
    )

    result = {
        "total_sites":          int(r.get("total_sites") or 0),
        "active_cells":         int(r.get("active_cells") or r.get("total_sites") or 0),
        "congested_cells":      0,
        "avg_throughput":       _f(r.get("avg_dl_tput")),
        "packet_loss":          _f(r.get("avg_packet_loss"), 2),
        "avg_latency":          _f(r.get("avg_latency")),
        "avg_sinr":             _f(r.get("avg_sinr")),
        "avg_prb":              _f(r.get("avg_prb")),
        "avg_ul_prb":           0,
        "network_health_score": round(health, 1),
        "health_label":         "Good" if health >= 80 else "Fair" if health >= 60 else "Poor",
        "avg_rrc_sr":           _f(r.get("avg_rrc_sr")),
        "avg_drop_rate":        _f(r.get("avg_drop_rate"), 2),
        "avg_rrc_ue":           _f(r.get("avg_rrc_ue")),
    }
    _to_cache(ck, result)
    return jsonify(result)


@network_bp.route("/api/network/map", methods=["GET"])
@jwt_required()
def network_map():
    """Site map with KPI colours. Primary: kpi_data + telecom_sites. Fallback: network_kpi_timeseries."""
    filters = _get_filters()
    fresh = request.args.get("fresh") == "1"
    ck = _cache_key("map_v8", filters)
    if not fresh:
        cached = _from_cache(ck)
        if cached:
            return jsonify(cached)

    sites = []
    _MAP_PRB  = "DL PRB Utilization (1BH)"
    _MAP_TPUT = "LTE DL - Usr Ave Throughput"
    _MAP_DROP = "E-RAB Call Drop Rate_1"
    _MAP_RRC  = "Ave RRC Connected Ue"
    _MAP_CSSR = "LTE Call Setup Success Rate"
    _MAP_CELL_TPUT = "LTE DL - Cell Ave Throughput"

    # ── 4-factor site health thresholds ──────────────────────────────────────
    # E-RAB Call Drop Rate > 1.5% → bad
    # LTE Call Setup Success Rate < 98.5% → bad
    # LTE DL Usr Ave Throughput < 8 Mbps → bad
    # DL PRB Utilization > 70% → bad  (industry standard: >70% is congested)
    def _site_health(prb, drop, cssr, usr_tput):
        """Return (status, color, health_score) based on 4 KPI factors."""
        bad_count = 0
        if drop > 1.5:    bad_count += 1
        if cssr < 98.5:   bad_count += 1
        if usr_tput < 8:  bad_count += 1
        if prb > 70:      bad_count += 1
        # Score: each factor contributes 25 points
        score = 0
        score += max(0, min(25, 25 * (1 - (drop - 0.5) / 3.0)))      # 0.5% → 25, 3.5% → 0
        score += max(0, min(25, 25 * (cssr - 95) / 5.0))               # 95% → 0, 100% → 25
        score += max(0, min(25, 25 * min(usr_tput, 20) / 20.0))        # 0 → 0, 20Mbps → 25
        score += max(0, min(25, 25 * (1 - max(prb - 30, 0) / 70.0)))   # 30% → 25, 100% → 0
        score = round(score, 1)
        if bad_count >= 3:
            return "critical", "#DC2626", score    # Red
        elif bad_count == 2:
            return "degraded", "#F97316", score    # Orange
        elif bad_count == 1:
            return "warning", "#EAB308", score     # Yellow
        else:
            return "healthy", "#22c55e", score     # Green

    # Primary: fast targeted JOIN — 4-factor health KPIs (with filters)
    _mfw, _mfp, _ = _kpi_filter_clause(filters, "k", "ts")
    try:
        map_rows = _sql(f"""
            SELECT k.site_id, ts.zone,
                   MAX(ts.latitude)  AS lat,
                   MAX(ts.longitude) AS lng,
                   AVG(CASE WHEN k.kpi_name=:prb   THEN k.value END) AS dl_prb_util,
                   AVG(CASE WHEN k.kpi_name=:tput   THEN k.value END) AS dl_usr_tput,
                   AVG(CASE WHEN k.kpi_name=:ctput  THEN k.value END) AS dl_cell_tput,
                   AVG(CASE WHEN k.kpi_name=:drop   THEN k.value END) AS erab_drop_rate,
                   AVG(CASE WHEN k.kpi_name=:rrc    THEN k.value END) AS avg_rrc_ue,
                   AVG(CASE WHEN k.kpi_name=:cssr   THEN k.value END) AS lte_cssr
            FROM kpi_data_merged k
            JOIN telecom_sites ts ON k.site_id = ts.site_id
            WHERE k.data_level = 'site' AND k.value IS NOT NULL
              AND k.kpi_name IN (:prb, :tput, :ctput, :drop, :rrc, :cssr) {_mfw}
            GROUP BY k.site_id, ts.zone
            ORDER BY dl_prb_util DESC NULLS LAST
            LIMIT 500
        """, {**_mfp, "prb":_MAP_PRB,"tput":_MAP_TPUT,"ctput":_MAP_CELL_TPUT,"drop":_MAP_DROP,"rrc":_MAP_RRC,"cssr":_MAP_CSSR})
        for r in map_rows:
            try:
                lat = round(float(r["lat"]), 6)
                lng = round(float(r["lng"]), 6)
            except (TypeError, ValueError):
                continue
            prb  = float(r.get("dl_prb_util") or 0)
            tput = float(r.get("dl_cell_tput") or 0)
            usr_tput = float(r.get("dl_usr_tput") or 0)
            drop = float(r.get("erab_drop_rate") or 0)
            cssr = float(r.get("lte_cssr") or 100)
            status, color, health_score = _site_health(prb, drop, cssr, usr_tput)
            sites.append({
                "site_id":         r["site_id"],
                "province":        r.get("zone", ""),
                "zone":            r.get("zone", ""),
                "region":          r.get("zone", ""),
                "cluster":         r.get("zone", ""),
                "technology":      "",
                "latitude":        lat,
                "longitude":       lng,
                "active_users":    int(r.get("avg_rrc_ue") or 0),
                "prb_utilization": round(prb, 1),
                "dl_prb_util":     round(prb, 1),
                "sinr":            0,
                "throughput":      round(tput, 1),
                "dl_cell_tput":    round(tput, 1),
                "dl_usr_tput":     round(usr_tput, 1),
                "packet_loss":     0,
                "lte_cssr":        round(cssr, 2),
                "lte_rrc_setup_sr": 0,
                "erab_drop_rate":  round(drop, 2),
                "avg_rrc_ue":      round(float(r.get("avg_rrc_ue") or 0), 0),
                "availability":    0,
                "status":          status,
                "color":           color,
                "health_score":    health_score,
            })
    except Exception as e:
        _LOG.error("network_map fast query: %s", e)

    # Fallback: network_kpi_timeseries if kpi_data has no lat/lng
    if not sites:
        try:
            where, params = _build_where(filters)
            rows = _sql(f"""
                SELECT site_id, region, cluster, technology,
                       AVG(latitude) AS lat, AVG(longitude) AS lng,
                       AVG(active_users) AS active_users,
                       AVG(COALESCE(dl_prb_util, prb_utilization)) AS prb_utilization,
                       AVG(sinr) AS sinr,
                       AVG(COALESCE(dl_cell_tput, throughput_dl)) AS throughput,
                       AVG(packet_loss) AS packet_loss,
                       AVG(COALESCE(lte_rrc_setup_sr, 0)) AS lte_rrc_setup_sr,
                       AVG(COALESCE(erab_drop_rate, call_drop_rate, 0)) AS erab_drop_rate,
                       AVG(COALESCE(avg_rrc_ue, 0)) AS avg_rrc_ue,
                       AVG(availability) AS availability
                FROM network_kpi_timeseries
                WHERE {where} AND latitude IS NOT NULL AND longitude IS NOT NULL
                GROUP BY site_id, region, cluster, technology
                ORDER BY prb_utilization DESC NULLS LAST LIMIT 500
            """, params)
            for r in rows:
                try:
                    lat = round(float(r["lat"]), 6)
                    lng = round(float(r["lng"]), 6)
                except Exception:
                    continue
                prb = float(r.get("prb_utilization") or 0)
                status = "congested" if prb > 85 else "warning" if prb > 60 else "healthy"
                color  = "#ef4444" if prb > 85 else "#f59e0b" if prb > 60 else "#22c55e"
                sites.append({
                    "site_id": r["site_id"], "region": r.get("region"), "cluster": r.get("cluster"),
                    "technology": r.get("technology"), "latitude": lat, "longitude": lng,
                    "active_users": int(r.get("active_users") or 0),
                    "prb_utilization": round(prb, 1), "dl_prb_util": round(prb, 1),
                    "sinr": round(float(r.get("sinr") or 0), 1),
                    "throughput": round(float(r.get("throughput") or 0), 1),
                    "dl_cell_tput": round(float(r.get("throughput") or 0), 1),
                    "packet_loss": round(float(r.get("packet_loss") or 0), 2),
                    "lte_rrc_setup_sr": round(float(r.get("lte_rrc_setup_sr") or 0), 1),
                    "erab_drop_rate": round(float(r.get("erab_drop_rate") or 0), 2),
                    "avg_rrc_ue": round(float(r.get("avg_rrc_ue") or 0), 0),
                    "availability": round(float(r.get("availability") or 0), 2),
                    "status": status, "color": color,
                })
        except Exception:
            pass

    result = {"sites": sites, "total": len(sites)}
    _to_cache(ck, result)
    return jsonify(result)


@network_bp.route("/api/network/ran-debug", methods=["GET"])
@jwt_required()
def ran_debug():
    """
    Diagnostic endpoint — call from the browser to see exactly what's in
    kpi_data. Returns row count, distinct sites, distinct KPI names, the
    actual date span of uploaded data, and a sample of values for the six
    headline RAN KPIs. If this returns counts > 0 but ran_analytics still
    shows blank cards, the bug is in the query/filter logic, not the DB.
    If counts are 0, the upload didn't land in kpi_data.
    """
    diag = {"db_connected": False}
    try:
        r = _sql("SELECT COUNT(*) AS rows, COUNT(DISTINCT site_id) AS sites, "
                 "COUNT(DISTINCT kpi_name) AS kpis, MIN(date) AS min_date, "
                 "MAX(date) AS max_date, CURRENT_DATE AS db_today FROM kpi_data")
        if r:
            d = r[0]
            diag["db_connected"] = True
            diag["rows"]      = int(d.get("rows") or 0)
            diag["sites"]     = int(d.get("sites") or 0)
            diag["kpis"]      = int(d.get("kpis") or 0)
            diag["min_date"]  = str(d.get("min_date")) if d.get("min_date") else None
            diag["max_date"]  = str(d.get("max_date")) if d.get("max_date") else None
            diag["db_today"]  = str(d.get("db_today")) if d.get("db_today") else None
    except Exception as e:
        diag["error"] = f"count query failed: {e}"
        return jsonify(diag), 500

    try:
        names = [n["kpi_name"] for n in _sql(
            "SELECT DISTINCT kpi_name FROM kpi_data WHERE kpi_name IS NOT NULL "
            "ORDER BY kpi_name LIMIT 200"
        )]
        diag["distinct_kpi_names"] = names
    except Exception as e:
        diag["distinct_kpi_names_error"] = str(e)

    try:
        sample = _sql("""
            SELECT
                AVG(CASE WHEN kpi_name ILIKE '%e-rab%call%drop%' OR kpi_name ILIKE '%call%drop%rate%' THEN value END) AS erab_drop_rate,
                AVG(CASE WHEN kpi_name ILIKE '%dl%prb%util%' THEN value END) AS dl_prb_util,
                AVG(CASE WHEN kpi_name ILIKE '%dl%usr%ave%throughput%' OR kpi_name ILIKE '%dl%usr%throughput%' THEN value END) AS dl_usr_tput,
                AVG(CASE WHEN kpi_name ILIKE '%rrc%connected%' OR kpi_name ILIKE '%ave%rrc%' THEN value END) AS avg_rrc_ue,
                AVG(CASE WHEN kpi_name ILIKE '%call%setup%success%' THEN value END) AS lte_call_setup_sr,
                AVG(CASE WHEN kpi_name ILIKE '%dl%data%total%volume%' THEN value END) AS dl_data_vol
            FROM kpi_data WHERE value IS NOT NULL
        """)
        if sample:
            diag["headline_kpis_no_filter"] = {
                k: (round(float(v), 3) if v is not None else None)
                for k, v in sample[0].items()
            }
    except Exception as e:
        diag["headline_kpis_error"] = str(e)

    return jsonify(diag)


# ────────────────────────────────────────────────────────────────────────────
# RAN ANALYTICS — SELF-CONTAINED REWRITE (does not share date filter logic
# with overview_stats so a bug in one cannot blank the other)
# ────────────────────────────────────────────────────────────────────────────

def _ran_resolve_dates(time_range: str):
    """Return (from_date, to_date) for the RAN endpoint.

    Anchors to MIN(MAX(kpi_data.date), today) — never returns a future end
    date, but slides backwards if uploads are older than today. Returns
    (None, None) if no data has been uploaded.
    """
    try:
        r = _sql("SELECT MAX(date) AS mx FROM kpi_data")
        max_date = r[0]["mx"] if r and r[0].get("mx") else None
    except Exception:
        max_date = None
    if not max_date:
        return None, None
    from datetime import date as _date, timedelta as _td
    today = _date.today()
    end = max_date if max_date <= today else today
    days_map = {"1h": 1, "6h": 1, "24h": 1, "7d": 7, "30d": 30, "all": None}
    days = days_map.get((time_range or "30d").strip().lower(), 30)
    start = (end - _td(days=days)) if days is not None else None
    return start, end


def _ran_geo_clause(filters: dict):
    """Build a geo-only WHERE fragment + params for kpi_data queries.
    Excludes time_range — that is handled separately by ran_analytics so the
    no-data-window fallback can drop it without rebuilding the geo filter.
    """
    parts = []
    params = {}
    needs_ts = False
    if not filters:
        return "", params, False

    zone = (filters.get("cluster") or filters.get("zone") or "").strip()
    if zone:
        items = [v.strip() for v in zone.split(",") if v.strip()]
        if len(items) == 1:
            params["_rz"] = items[0]
            parts.append("LOWER(ts.zone) = LOWER(:_rz)")
        else:
            phs = []
            for i, v in enumerate(items):
                params[f"_rz_{i}"] = v
                phs.append(f"LOWER(:_rz_{i})")
            parts.append(f"LOWER(ts.zone) IN ({','.join(phs)})")
        needs_ts = True

    geo_cols = []  # subquery against telecom_sites
    for fk, col, prefix in [("technology", "technology", "_rtech"),
                            ("vendor",     "vendor_name", "_rvend"),
                            ("country",    "country",    "_rcty"),
                            ("state",      "state",      "_rst"),
                            ("city",       "city",       "_rci")]:
        val = (filters.get(fk) or "").strip()
        if not val:
            continue
        items = [v.strip() for v in val.split(",") if v.strip()]
        if len(items) == 1:
            params[prefix] = items[0]
            geo_cols.append(f"LOWER({col}) = LOWER(:{prefix})")
        else:
            phs = []
            for i, v in enumerate(items):
                key = f"{prefix}_{i}"
                params[key] = v
                phs.append(f"LOWER(:{key})")
            geo_cols.append(f"LOWER({col}) IN ({','.join(phs)})")
    if geo_cols:
        parts.append(f"k.site_id IN (SELECT site_id FROM telecom_sites WHERE {' AND '.join(geo_cols)})")

    site = (filters.get("site") or "").strip()
    if site:
        params["_rsite"] = site
        parts.append("LOWER(k.site_id) = LOWER(:_rsite)")

    where = (" AND " + " AND ".join(parts)) if parts else ""
    return where, params, needs_ts


# ─────────────────────────────────────────────────────────────────────────────
# RAN Layer Helpers — pure Python data-retrieval logic (ISOLATED from _ovw_*).
#
# Separation guarantee: NO _ran_* helper calls any _ovw_* helper, and the
# overview function does not call any _ran_* helper. They share only the
# read-only utilities `_sql`, `_kpi_filter_clause`, `_distinct_kpi_names_cached`,
# `_get_kpi_max_date`, `_telecom_sites_cell_col`, and `_f`. A bug or slow path
# on the overview side cannot affect the RAN endpoint and vice versa — they
# don't share request state, they don't share caches, they don't share buckets.
#
# SQL is used only for the cheap heavy-lift (WHERE filter + GROUP BY on
# indexed columns). Every business decision — KPI-name resolution, network
# averages, per-site shaping, status classification, zone aggregation,
# top-issues ranking, PRB-bucket distribution, trend assembly — happens in
# pure Python so each step is easy to read, change, and debug. Each helper
# logs its result count to backend.log so any blank chart is one log line
# away from a root cause.
# ─────────────────────────────────────────────────────────────────────────────

def _ran_resolve_kpi_names():
    """Resolve canonical RAN KPI names from kpi_data (cached 1h).
    Returns a dict: short_key -> kpi_name (or None if not present).
    Using exact `kpi_name IN (...)` lets Postgres hit the index — much
    faster than `ILIKE '%...%'` over millions of rows."""
    name_idx = {(n or "").lower(): n for n in _distinct_kpi_names_cached()}

    def _r(default, *substrings):
        if default.lower() in name_idx:
            return name_idx[default.lower()]
        for s in substrings:
            for low, full in name_idx.items():
                if s.lower() in low:
                    return full
        return None

    return {
        "drop":     _r("E-RAB Call Drop Rate_1",      "e-rab call drop", "call drop rate"),
        "prb_dl":   _r("DL PRB Utilization (1BH)",    "dl prb util"),
        "prb_ul":   _r("UL PRB Utilization (1BH)",    "ul prb util"),
        "tput_dl":  _r("LTE DL - Cell Ave Throughput","dl - cell ave", "dl cell ave"),
        "tput_ul":  _r("LTE UL - Cell Ave Throughput","ul - cell ave", "ul cell ave"),
        "usr_dl":   _r("LTE DL - Usr Ave Throughput", "dl - usr", "dl usr"),
        "rrc":      _r("Ave RRC Connected Ue",        "rrc connected", "ave rrc"),
        "rrc_sr":   _r("LTE RRC Setup Success Rate",  "rrc setup success"),
        "cssr":     _r("LTE Call Setup Success Rate", "call setup success", "cssr"),
        "erab_sr":  _r("LTE E-RAB Setup Success Rate","e-rab setup success", "erab setup"),
        "avail":    _r("Availability",                "availability"),
        "dl_vol":   _r("DL Data Total Volume",        "dl data total volume", "dl volume"),
        "ul_vol":   _r("UL Data Total Volume",        "ul data total volume", "ul volume"),
        "volte_dl": _r("VoLTE Traffic DL",            "volte traffic dl"),
    }


def _ran_mean(values):
    """Mean of an iterable, ignoring None. Returns None on empty."""
    xs = [float(v) for v in values if v is not None]
    return sum(xs) / len(xs) if xs else None


def _ran_pull_per_site(filters, kpi_names, start, end):
    """Step 1A: fetch per-(site, kpi) averages for the chosen KPIs and window.
    SQL does WHERE + GROUP BY (uses kpi_name + date indexes). Python receives
    one row per (site, kpi) — usually a few thousand rows total."""
    if not kpi_names:
        return []
    geo_where, geo_params, _needs = _ran_geo_clause(filters)
    params = dict(geo_params)
    placeholders = []
    for i, n in enumerate(kpi_names):
        key = f"_rkn{i}"
        params[key] = n
        placeholders.append(f":{key}")
    in_clause = ",".join(placeholders)
    date_cond = ""
    if start and end:
        params["_r_start"] = start
        params["_r_end"]   = end
        date_cond = " AND k.date >= :_r_start AND k.date <= :_r_end"
    rows = _sql(f"""
        SELECT k.site_id, k.kpi_name,
               AVG(k.value)      AS v,
               MAX(ts.zone)      AS zone,
               AVG(ts.latitude)  AS lat,
               AVG(ts.longitude) AS lng
        FROM kpi_data_merged k
        LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
        WHERE k.value IS NOT NULL
          AND k.kpi_name IN ({in_clause})
          {geo_where}
          {date_cond}
        GROUP BY k.site_id, k.kpi_name
    """, params, timeout_ms=10000)
    _LOG.info("_ran_pull_per_site: %d rows window=%s→%s", len(rows), start, end)
    if date_cond:
        present = {r.get("kpi_name") for r in rows}
        missing = [n for n in kpi_names if n not in present]
        if rows and missing:
            fp = dict(geo_params)
            fph = []
            for i, n in enumerate(missing):
                key = f"_rfkn{i}"
                fp[key] = n
                fph.append(f":{key}")
            fp["_rf_days"] = max(1, int((end - start).days))
            fin = ",".join(fph)
            fallback_rows = _sql(f"""
                WITH latest AS (
                    SELECT kpi_name, MAX(date) AS mx
                    FROM kpi_data_merged
                    WHERE kpi_name IN ({fin}) AND value IS NOT NULL
                    GROUP BY kpi_name
                )
                SELECT k.site_id, k.kpi_name,
                       AVG(k.value)      AS v,
                       MAX(ts.zone)      AS zone,
                       AVG(ts.latitude)  AS lat,
                       AVG(ts.longitude) AS lng
                FROM kpi_data_merged k
                JOIN latest l ON l.kpi_name = k.kpi_name
                LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
                WHERE k.value IS NOT NULL
                  AND k.kpi_name IN ({fin})
                  AND k.date >= l.mx - (:_rf_days * INTERVAL '1 day')
                  AND k.date <= l.mx
                  {geo_where}
                GROUP BY k.site_id, k.kpi_name
            """, fp, timeout_ms=10000)
            rows.extend(fallback_rows)
            _LOG.info("_ran_pull_per_site (per-kpi fallback): %d rows for %d missing KPIs",
                      len(fallback_rows), len(missing))
        elif not rows:
            # Fallback: no date window
            np = {k: v for k, v in params.items() if k not in ("_r_start", "_r_end")}
            rows = _sql(f"""
                SELECT k.site_id, k.kpi_name,
                       AVG(k.value)      AS v,
                       MAX(ts.zone)      AS zone,
                       AVG(ts.latitude)  AS lat,
                       AVG(ts.longitude) AS lng
                FROM kpi_data_merged k
                LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
                WHERE k.value IS NOT NULL
                  AND k.kpi_name IN ({in_clause})
                  {geo_where}
                GROUP BY k.site_id, k.kpi_name
            """, np)
            _LOG.info("_ran_pull_per_site (no-date fallback): %d rows", len(rows))
    return rows


def _ran_pull_per_date(filters, kpi_names, start, end):
    """Step 1B: fetch per-(kpi, date) averages — feeds the trend charts."""
    if not kpi_names:
        return []
    geo_where, geo_params, _needs = _ran_geo_clause(filters)
    params = dict(geo_params)
    placeholders = []
    for i, n in enumerate(kpi_names):
        key = f"_rdkn{i}"
        params[key] = n
        placeholders.append(f":{key}")
    in_clause = ",".join(placeholders)
    date_cond = ""
    if start and end:
        params["_r_start"] = start
        params["_r_end"]   = end
        date_cond = " AND k.date >= :_r_start AND k.date <= :_r_end"
    rows = _sql(f"""
        SELECT k.kpi_name, k.date::text AS date, AVG(k.value) AS v
        FROM kpi_data_merged k
        LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
        WHERE k.value IS NOT NULL
          AND k.kpi_name IN ({in_clause})
          {geo_where}
          {date_cond}
        GROUP BY k.kpi_name, k.date
        ORDER BY k.date
    """, params, timeout_ms=10000)
    _LOG.info("_ran_pull_per_date: %d rows window=%s→%s", len(rows), start, end)
    if date_cond:
        present = {r.get("kpi_name") for r in rows}
        missing = [n for n in kpi_names if n not in present]
        if rows and missing:
            fp = dict(geo_params)
            fph = []
            for i, n in enumerate(missing):
                key = f"_rdfkn{i}"
                fp[key] = n
                fph.append(f":{key}")
            fp["_rdf_days"] = max(1, int((end - start).days))
            fin = ",".join(fph)
            fallback_rows = _sql(f"""
                WITH latest AS (
                    SELECT kpi_name, MAX(date) AS mx
                    FROM kpi_data_merged
                    WHERE kpi_name IN ({fin}) AND value IS NOT NULL
                    GROUP BY kpi_name
                )
                SELECT k.kpi_name, k.date::text AS date, AVG(k.value) AS v
                FROM kpi_data_merged k
                JOIN latest l ON l.kpi_name = k.kpi_name
                LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
                WHERE k.value IS NOT NULL
                  AND k.kpi_name IN ({fin})
                  AND k.date >= l.mx - (:_rdf_days * INTERVAL '1 day')
                  AND k.date <= l.mx
                  {geo_where}
                GROUP BY k.kpi_name, k.date
                ORDER BY k.date
            """, fp, timeout_ms=10000)
            rows.extend(fallback_rows)
            rows.sort(key=lambda r: r.get("date") or "")
            _LOG.info("_ran_pull_per_date (per-kpi fallback): %d rows for %d missing KPIs",
                      len(fallback_rows), len(missing))
        elif not rows:
            np = {k: v for k, v in params.items() if k not in ("_r_start", "_r_end")}
            rows = _sql(f"""
                SELECT k.kpi_name, k.date::text AS date, AVG(k.value) AS v
                FROM kpi_data_merged k
                LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
                WHERE k.value IS NOT NULL
                  AND k.kpi_name IN ({in_clause})
                  {geo_where}
                GROUP BY k.kpi_name, k.date
                ORDER BY k.date
            """, np)
            _LOG.info("_ran_pull_per_date (no-date fallback): %d rows", len(rows))
    return rows


def _ran_card_avgs(per_site_rows, kpi_map):
    """Step 2: network-wide KPI card averages (mean of per-site averages)."""
    from collections import defaultdict
    buckets = defaultdict(list)
    for r in per_site_rows:
        v = r.get("v")
        if v is None:
            continue
        buckets[r["kpi_name"]].append(float(v))
    def _avg(short_key):
        kn = kpi_map.get(short_key)
        if not kn or kn not in buckets:
            return None
        vs = buckets[kn]
        return (sum(vs) / len(vs)) if vs else None
    return {
        "erab_drop_rate":    _avg("drop"),
        "dl_prb_util":       _avg("prb_dl"),
        "ul_prb_util":       _avg("prb_ul"),
        "dl_cell_tput":      _avg("tput_dl"),
        "ul_cell_tput":      _avg("tput_ul"),
        "dl_usr_tput":       _avg("usr_dl"),
        "avg_rrc_ue":        _avg("rrc"),
        "lte_rrc_setup_sr":  _avg("rrc_sr"),
        "lte_call_setup_sr": _avg("cssr"),
        "erab_setup_sr":     _avg("erab_sr"),
        "availability":      _avg("avail"),
        "dl_data_vol":       _avg("dl_vol"),
    }


def _ran_site_health(prb, drop, cssr, usr_tput):
    """Status / colour / score for a single site. Pure Python rules."""
    bad = 0
    if drop > 1.5:   bad += 1
    if cssr < 98.5:  bad += 1
    if usr_tput < 8: bad += 1
    if prb > 70:     bad += 1
    score  = max(0, min(25, 25 * (1 - (drop - 0.5) / 3.0)))
    score += max(0, min(25, 25 * (cssr - 95) / 5.0))
    score += max(0, min(25, 25 * min(usr_tput, 20) / 20.0))
    score += max(0, min(25, 25 * (1 - max(prb - 30, 0) / 70.0)))
    score = round(score, 1)
    if bad >= 3:   return "critical", "#DC2626", score
    elif bad == 2: return "degraded", "#F97316", score
    elif bad == 1: return "warning",  "#EAB308", score
    else:          return "healthy",  "#22c55e", score


def _ran_site_list(per_site_rows, kpi_map):
    """Step 3: build the full per-site list with every RAN KPI + status colour.
    Pure Python pivot of (site, kpi) rows into one record per site."""
    from collections import defaultdict
    site_data = defaultdict(lambda: {"kpis": {}, "zone": "", "lat": None, "lng": None})
    for r in per_site_rows:
        sd = site_data[r["site_id"]]
        sd["kpis"][r["kpi_name"]] = float(r["v"]) if r.get("v") is not None else None
        if r.get("zone") and not sd["zone"]:
            sd["zone"] = r["zone"]
        if r.get("lat") is not None: sd["lat"] = r["lat"]
        if r.get("lng") is not None: sd["lng"] = r["lng"]

    out = []
    K_PRB_DL  = kpi_map.get("prb_dl")
    K_PRB_UL  = kpi_map.get("prb_ul")
    K_DROP    = kpi_map.get("drop")
    K_CSSR    = kpi_map.get("cssr")
    K_USR_DL  = kpi_map.get("usr_dl")
    K_TPUT_DL = kpi_map.get("tput_dl")
    K_RRC     = kpi_map.get("rrc")
    K_RRC_SR  = kpi_map.get("rrc_sr")
    K_AVAIL   = kpi_map.get("avail")
    K_DL_VOL  = kpi_map.get("dl_vol")
    K_VOLTE   = kpi_map.get("volte_dl")

    for sid, sd in site_data.items():
        kp = sd["kpis"]
        dl_prb = kp.get(K_PRB_DL) or 0
        ul_prb = kp.get(K_PRB_UL) or 0
        prb_combined = (dl_prb + ul_prb) / 2.0 if (dl_prb and ul_prb) else (dl_prb or ul_prb)
        drop     = kp.get(K_DROP)   or 0
        cssr     = kp.get(K_CSSR)   if kp.get(K_CSSR) is not None else 100
        usr_tput = kp.get(K_USR_DL) or 0
        status, color, health = _ran_site_health(prb_combined, drop, cssr, usr_tput)
        out.append({
            "site_id": sid, "zone": sd["zone"], "cluster": sd["zone"],
            "lat": sd["lat"], "lng": sd["lng"],
            "erab_drop_rate":    _f(drop, 2),
            "dl_prb_util":       _f(dl_prb or prb_combined, 1),
            "ul_prb_util":       _f(ul_prb, 1),
            "prb_utilization":   _f(prb_combined, 1),
            "avg_prb":           _f(prb_combined, 1),
            "dl_cell_tput":      _f(kp.get(K_TPUT_DL), 1),
            "dl_usr_tput":       _f(usr_tput, 1),
            "throughput":        _f(kp.get(K_TPUT_DL), 1),
            "avg_rrc_ue":        _f(kp.get(K_RRC), 1),
            "lte_rrc_setup_sr":  _f(kp.get(K_RRC_SR), 1),
            "lte_call_setup_sr": _f(cssr, 1),
            "lte_cssr":          _f(cssr, 1),
            "availability":      _f(kp.get(K_AVAIL), 1),
            "dl_data_vol":       _f(kp.get(K_DL_VOL), 2),
            "volte_traffic_dl":  _f(kp.get(K_VOLTE), 2),
            "status": status, "color": color, "health_score": health,
        })
    out.sort(key=lambda s: -float(s.get("dl_prb_util") or 0))
    return out[:500]


def _ran_prb_distribution(site_kpis):
    """PRB% bucket distribution across sites — pure Python counter."""
    buckets = {"0-20%": 0, "20-40%": 0, "40-60%": 0,
               "60-80%": 0, "80-85%": 0, ">85% (Critical)": 0}
    for s in site_kpis:
        prb = float(s.get("dl_prb_util") or 0)
        if prb < 20:    buckets["0-20%"] += 1
        elif prb < 40:  buckets["20-40%"] += 1
        elif prb < 60:  buckets["40-60%"] += 1
        elif prb < 80:  buckets["60-80%"] += 1
        elif prb <= 85: buckets["80-85%"] += 1
        else:           buckets[">85% (Critical)"] += 1
    return [{"range": k, "count": v} for k, v in buckets.items() if v > 0]


def _ran_zone_perf(site_kpis):
    """Per-zone aggregate of every headline RAN KPI. Pure Python bucketing."""
    zone_map = {}
    for s in site_kpis:
        z = s.get("zone") or "Unknown"
        zd = zone_map.setdefault(z, {"drop": [], "cssr": [], "usr_tput": [],
                                     "rrc": [], "prb": [], "vol": [],
                                     "tputs": [], "n": 0})
        zd["drop"].append(float(s.get("erab_drop_rate")    or 0))
        zd["cssr"].append(float(s.get("lte_call_setup_sr") or 0))
        zd["usr_tput"].append(float(s.get("dl_usr_tput")   or 0))
        zd["rrc"].append(float(s.get("avg_rrc_ue")         or 0))
        zd["prb"].append(float(s.get("dl_prb_util")        or 0))
        zd["vol"].append(float(s.get("dl_data_vol")        or 0))
        zd["tputs"].append(float(s.get("dl_cell_tput")     or 0))
        zd["n"] += 1
    def _avg(xs): return sum(xs) / max(len(xs), 1)
    return [
        {"zone":         z,
         "drop_rate":    _f(_avg(d["drop"]),     2),
         "cssr":         _f(_avg(d["cssr"]),     2),
         "dl_usr_tput":  _f(_avg(d["usr_tput"]), 2),
         "avg_rrc_ue":   _f(_avg(d["rrc"]),      1),
         "dl_prb_util":  _f(_avg(d["prb"]),      1),
         "dl_data_vol":  _f(_avg(d["vol"]),      2),
         "avg_prb":      _f(_avg(d["prb"]),      1),
         "avg_tput":     _f(_avg(d["tputs"]),    1),
         "sites":        d["n"]}
        for z, d in sorted(zone_map.items(), key=lambda x: -_avg(x[1]["prb"]))
    ]


def _ran_top_issues(site_kpis):
    """Top 20 sites with the most KPI breaches. Pure Python ranker."""
    def _issues(s):
        bad = 0
        if float(s.get("erab_drop_rate")    or 0) > 2:    bad += 1
        if float(s.get("lte_call_setup_sr") or 100) < 98: bad += 1
        if float(s.get("dl_usr_tput")       or 0) < 8:    bad += 1
        if float(s.get("dl_prb_util")       or 0) > 80:   bad += 1
        return bad

    def _label(s):
        if float(s.get("erab_drop_rate")    or 0) > 2:    return "High Drop"
        if float(s.get("lte_call_setup_sr") or 100) < 98: return "Low CSSR"
        if float(s.get("dl_prb_util")       or 0) > 80:   return "High PRB"
        if float(s.get("dl_usr_tput")       or 0) < 8:    return "Low Tput"
        return "Watch"

    return sorted(
        [{
            "site_id":     s["site_id"],
            "cluster":     s.get("zone", ""),
            "zone":        s.get("zone", ""),
            "drop_rate":   float(s.get("erab_drop_rate")    or 0),
            "cssr":        float(s.get("lte_call_setup_sr") or 0),
            "dl_usr_tput": float(s.get("dl_usr_tput")       or 0),
            "avg_rrc_ue":  float(s.get("avg_rrc_ue")        or 0),
            "dl_prb_util": float(s.get("dl_prb_util")       or 0),
            "avg_prb":     float(s.get("dl_prb_util")       or 0),
            "dl_data_vol": float(s.get("dl_data_vol")       or 0),
            "avg_tput":    float(s.get("dl_cell_tput")      or 0),
            "lat":         s.get("lat"),
            "lng":         s.get("lng"),
            "issues":      _issues(s),
            "issue_type":  _label(s),
        } for s in site_kpis],
        key=lambda x: (x["issues"], x["drop_rate"], x["avg_prb"]),
        reverse=True,
    )[:20]


def _ran_trends(per_date_rows, kpi_map):
    """Build the three trend chart datasets from per-(kpi, date) rows."""
    by_date = {}
    for r in per_date_rows:
        if r.get("v") is None: continue
        by_date.setdefault(r["date"], {})[r["kpi_name"]] = float(r["v"])

    K_DROP   = kpi_map.get("drop")
    K_CSSR   = kpi_map.get("cssr")
    K_PRB_DL = kpi_map.get("prb_dl")
    K_USR_DL = kpi_map.get("usr_dl")
    K_DL_VOL = kpi_map.get("dl_vol")
    K_UL_VOL = kpi_map.get("ul_vol")

    sorted_dates = sorted(by_date.keys())
    call_drop = [{
        "date": d,
        "drop_rate": _f(by_date[d].get(K_DROP), 2),
        "cssr":      _f(by_date[d].get(K_CSSR), 2),
    } for d in sorted_dates
       if (by_date[d].get(K_DROP) is not None or by_date[d].get(K_CSSR) is not None)]

    prb_tput_trend = [{
        "date":        d,
        "dl_prb_util": _f(by_date[d].get(K_PRB_DL), 2),
        "dl_usr_tput": _f(by_date[d].get(K_USR_DL), 2),
    } for d in sorted_dates
       if (by_date[d].get(K_PRB_DL) is not None or by_date[d].get(K_USR_DL) is not None)]

    hourly_dl = [{
        "hour":      d,
        "dl_volume": _f(by_date[d].get(K_DL_VOL), 2),
        "ul_volume": _f(by_date[d].get(K_UL_VOL), 2),
    } for d in sorted_dates
       if (by_date[d].get(K_DL_VOL) is not None or by_date[d].get(K_UL_VOL) is not None)]

    return call_drop, prb_tput_trend, hourly_dl


@network_bp.route("/api/network/ran-analytics", methods=["GET"])
@jwt_required()
def ran_analytics():
    """RAN analytics — Python orchestration over isolated _ran_* helpers.

    Each card / chart / table is computed by its own helper above; this
    function just wires them together and assembles the response. RAN
    helpers do NOT call any _ovw_* helper, so a slow or broken overview
    can never affect this endpoint (and vice versa)."""
    filters = _get_filters()
    _LOG.info("ran-analytics: start filters=%s", {k: v for k, v in filters.items() if v})

    # Step 1: resolve actual KPI names from kpi_data so we can use exact
    # `IN (...)` matches (hits the (kpi_name, date) index).
    kpi_map = _ran_resolve_kpi_names()
    kpi_names = [v for v in kpi_map.values() if v]
    _LOG.info("ran-analytics: %d KPIs resolved out of %d expected",
              len(kpi_names), len(kpi_map))

    # Empty KPI list — return well-shaped zeros so the frontend renders.
    if not kpi_names:
        return jsonify({
            "lte_rrc_setup_sr": 0, "lte_call_setup_sr": 0, "erab_setup_sr": 0,
            "erab_drop_rate": 0, "dl_cell_tput": 0, "ul_cell_tput": 0,
            "dl_usr_tput": 0, "dl_data_vol": 0, "avg_rrc_ue": 0,
            "availability": 0, "dl_prb_util": 0, "avg_prb": 0, "avg_sinr": 0,
            "call_drop_trend": [], "prb_distribution": [], "prb_tput_trend": [],
            "hourly_dl_traffic": [], "zone_performance": [], "top_issues": [],
            "sites": [],
        })

    # Step 2: resolve the date window (RAN-specific).
    start, end = _ran_resolve_dates(filters.get("time_range") or "30d")

    # Step 3: pull both raw datasets in parallel-friendly Python helpers.
    try:
        per_site_rows = _ran_pull_per_site(filters, kpi_names, start, end)
    except Exception as e:
        _LOG.error("ran-analytics per-site: %s", e); per_site_rows = []
    try:
        per_date_rows = _ran_pull_per_date(filters, kpi_names, start, end)
    except Exception as e:
        _LOG.error("ran-analytics per-date: %s", e); per_date_rows = []

    # Step 4: shape each dashboard piece in pure Python helpers.
    try: agg = _ran_card_avgs(per_site_rows, kpi_map)
    except Exception as e: _LOG.error("ran cards: %s", e); agg = {}
    try: site_kpis = _ran_site_list(per_site_rows, kpi_map)
    except Exception as e: _LOG.error("ran site_list: %s", e); site_kpis = []
    try: prb_dist = _ran_prb_distribution(site_kpis)
    except Exception as e: _LOG.error("ran prb_dist: %s", e); prb_dist = []
    try: zone_perf = _ran_zone_perf(site_kpis)
    except Exception as e: _LOG.error("ran zone_perf: %s", e); zone_perf = []
    try: top_issues = _ran_top_issues(site_kpis)
    except Exception as e: _LOG.error("ran top_issues: %s", e); top_issues = []
    try: call_drop, prb_tput_trend, hourly_dl = _ran_trends(per_date_rows, kpi_map)
    except Exception as e:
        _LOG.error("ran trends: %s", e)
        call_drop = prb_tput_trend = hourly_dl = []

    def _p(v): return _f(v, 1) if v is not None else 0

    result = {
        "lte_rrc_setup_sr":  _p(agg.get("lte_rrc_setup_sr")),
        "lte_call_setup_sr": _p(agg.get("lte_call_setup_sr")),
        "erab_setup_sr":     _p(agg.get("erab_setup_sr")),
        "erab_drop_rate":    _f(agg.get("erab_drop_rate"), 2),
        "dl_cell_tput":      _f(agg.get("dl_cell_tput")),
        "ul_cell_tput":      _f(agg.get("ul_cell_tput")),
        "dl_usr_tput":       _f(agg.get("dl_usr_tput")),
        "dl_data_vol":       _f(agg.get("dl_data_vol")),
        "avg_rrc_ue":        _f(agg.get("avg_rrc_ue")),
        "availability":      _p(agg.get("availability")),
        "dl_prb_util":       _p(agg.get("dl_prb_util")),
        "avg_prb":           _p(agg.get("dl_prb_util")),
        "avg_sinr":          0,
        "call_drop_trend":   call_drop,
        "prb_distribution":  prb_dist,
        "prb_tput_trend":    prb_tput_trend,
        "hourly_dl_traffic": hourly_dl,
        "zone_performance":  zone_perf,
        "top_issues":        top_issues,
        "sites":             site_kpis,
    }
    _LOG.info("ran-analytics: done sites=%d zones=%d top_issues=%d trends=(%d,%d,%d)",
              len(site_kpis), len(zone_perf), len(top_issues),
              len(call_drop), len(prb_tput_trend), len(hourly_dl))
    return jsonify(result)


@network_bp.route("/api/network/core-analytics", methods=["GET"])
@jwt_required()
def core_analytics():
    """
    Core Component KPI analytics — MME, SGW, PGW, HSS, PCRF.
    Reads from core_component_kpi table.

    Query params:
      component_type : filter by MME / SGW / PGW / HSS / PCRF
      component_id   : filter by specific instance (MME1, SGW2, …)
      time_range     : 24h / 7d / 30d / all
      scale          : 15min / hourly / daily  (aggregation level)
      fresh          : 1 to bypass cache
    """
    comp_type = (request.args.get("component_type") or "").strip().upper() or None
    comp_id = (request.args.get("component_id") or "").strip() or None
    time_range = (request.args.get("time_range") or "30d").strip()
    scale = (request.args.get("scale") or "hourly").strip().lower()
    fresh = request.args.get("fresh") == "1"

    ck = _cache_key("core_comp_v2", {"ct": comp_type, "ci": comp_id, "tr": time_range, "sc": scale})
    if not fresh:
        cached = _from_cache(ck)
        if cached:
            return jsonify(cached)

    # Time filter — anchored to CURRENT_DATE.
    _days = {"1h": 1, "6h": 1, "24h": 1, "7d": 7, "30d": 30}.get(time_range, 9999)
    _time_sql = ""
    if _days < 9999:
        _time_sql = f"AND date >= CURRENT_DATE - INTERVAL '{_days} days' AND date <= CURRENT_DATE"

    _ct_sql = f"AND component_type = '{comp_type}'" if comp_type else ""
    _ci_sql = f"AND component_id = :comp_id" if comp_id else ""
    _params = {"comp_id": comp_id} if comp_id else {}

    # Check if table has data
    try:
        count_row = _sql("SELECT COUNT(*) AS cnt FROM core_component_kpi")
        has_data = count_row and count_row[0]["cnt"] > 0
    except Exception:
        has_data = False

    if not has_data:
        result = {"upload_needed": True, "component_types": [], "components": [],
                  "kpis": [], "network_summary": {}, "kpi_trends": {}, "component_summary": []}
        _to_cache(ck, result)
        return jsonify(result)

    # 1. Get available component types, instances, and KPI names
    comp_types = [r["component_type"] for r in _sql(
        "SELECT DISTINCT component_type FROM core_component_kpi ORDER BY component_type")]
    components = [{"component_type": r["component_type"], "component_id": r["component_id"]}
                  for r in _sql(f"""
        SELECT DISTINCT component_type, component_id FROM core_component_kpi
        WHERE 1=1 {_ct_sql} ORDER BY component_type, component_id
    """)]
    kpi_list = [{"kpi_name": r["kpi_name"], "component_type": r["component_type"]}
                for r in _sql(f"""
        SELECT DISTINCT kpi_name, component_type FROM core_component_kpi
        WHERE 1=1 {_ct_sql} ORDER BY component_type, kpi_name
    """)]

    # 2. Network-level summary: average of each KPI across all matching components
    net_summary_rows = _sql(f"""
        SELECT kpi_name, component_type,
               AVG(value) AS avg_val, MIN(value) AS min_val, MAX(value) AS max_val,
               COUNT(*) AS sample_count
        FROM core_component_kpi
        WHERE value IS NOT NULL {_ct_sql} {_ci_sql} {_time_sql}
        GROUP BY kpi_name, component_type
        ORDER BY component_type, kpi_name
    """, _params)
    network_summary = {}
    for r in net_summary_rows:
        network_summary[r["kpi_name"]] = {
            "component_type": r["component_type"],
            "avg": _f(r["avg_val"], 2),
            "min": _f(r["min_val"], 2),
            "max": _f(r["max_val"], 2),
            "samples": r["sample_count"],
        }

    # 3. Trend data per KPI — single aggregated query (replaces N+1 loop)
    kpi_trends = {}
    if scale == "15min":
        _all_trends = _sql(f"""
            SELECT kpi_name,
                   date::text || ' ' || LPAD(hour::text, 2, '0') || CHR(58) || LPAD(minute::text, 2, '0') AS ts,
                   AVG(value) AS val
            FROM core_component_kpi
            WHERE value IS NOT NULL {_ct_sql} {_ci_sql} {_time_sql}
            GROUP BY kpi_name, date, hour, minute
            ORDER BY kpi_name, date, hour, minute
        """, _params)
    elif scale == "daily":
        _all_trends = _sql(f"""
            SELECT kpi_name, date::text AS ts, AVG(value) AS val
            FROM core_component_kpi
            WHERE value IS NOT NULL {_ct_sql} {_ci_sql} {_time_sql}
            GROUP BY kpi_name, date
            ORDER BY kpi_name, date
        """, _params)
    else:  # hourly (default)
        _all_trends = _sql(f"""
            SELECT kpi_name,
                   date::text || ' ' || LPAD(hour::text, 2, '0') || CHR(58) || '00' AS ts,
                   AVG(value) AS val
            FROM core_component_kpi
            WHERE value IS NOT NULL {_ct_sql} {_ci_sql} {_time_sql}
            GROUP BY kpi_name, date, hour
            ORDER BY kpi_name, date, hour
        """, _params)

    # Partition results by kpi_name in Python
    for r in _all_trends:
        kn = r["kpi_name"]
        if kn not in kpi_trends:
            kpi_trends[kn] = []
        kpi_trends[kn].append({"ts": r["ts"], "value": _f(r["val"], 2)})

    # 4. Component-level summary table (one row per component instance)
    comp_summary = []
    if not comp_id:
        comp_rows = _sql(f"""
            SELECT component_type, component_id, kpi_name,
                   AVG(value) AS avg_val
            FROM core_component_kpi
            WHERE value IS NOT NULL {_ct_sql} {_time_sql}
            GROUP BY component_type, component_id, kpi_name
            ORDER BY component_type, component_id, kpi_name
        """)
        # Pivot: group by (component_type, component_id) → {kpi_name: avg}
        from collections import defaultdict
        pivot = defaultdict(lambda: {"kpis": {}})
        for r in comp_rows:
            key = (r["component_type"], r["component_id"])
            pivot[key]["component_type"] = r["component_type"]
            pivot[key]["component_id"] = r["component_id"]
            pivot[key]["kpis"][r["kpi_name"]] = _f(r["avg_val"], 2)
        comp_summary = list(pivot.values())

    result = {
        "upload_needed": False,
        "component_types": comp_types,
        "components": components,
        "kpis": kpi_list,
        "network_summary": network_summary,
        "kpi_trends": kpi_trends,
        "component_summary": comp_summary,
        "filters": {"component_type": comp_type, "component_id": comp_id,
                     "time_range": time_range, "scale": scale},
    }
    _to_cache(ck, result)
    return jsonify(result)




@network_bp.route("/api/network/core-daily-curves", methods=["GET"])
@jwt_required()
def core_daily_curves():
    """
    Return per-date intraday curves for a specific KPI.
    Each date becomes one curve (array of {time, value}) for overlay comparison.

    Query params:
      kpi_name       : required — which KPI
      component_type : optional filter
      component_id   : optional filter
      dates          : comma-separated dates (YYYY-MM-DD). If empty, returns today + last 7 days.
      scale          : '15min' (default) or 'hourly'
    """
    kpi_name = (request.args.get("kpi_name") or "").strip()
    if not kpi_name:
        return jsonify({"error": "kpi_name is required"}), 400

    comp_type = (request.args.get("component_type") or "").strip().upper() or None
    comp_id = (request.args.get("component_id") or "").strip() or None
    dates_str = (request.args.get("dates") or "").strip()
    scale = (request.args.get("scale") or "15min").strip().lower()

    _ct_sql = f"AND component_type = '{comp_type}'" if comp_type else ""
    _ci_sql = f"AND component_id = :comp_id" if comp_id else ""
    _params = {"comp_id": comp_id} if comp_id else {}

    # Determine which dates to return
    if dates_str:
        date_list = [d.strip() for d in dates_str.split(",") if d.strip()]
    else:
        # Default: last 7 days anchored to CURRENT_DATE
        date_rows = _sql(f"""
            SELECT DISTINCT date FROM core_component_kpi
            WHERE kpi_name = :kn {_ct_sql} {_ci_sql}
              AND date >= CURRENT_DATE - INTERVAL '7 days'
              AND date <= CURRENT_DATE
            ORDER BY date DESC LIMIT 8
        """, {**_params, "kn": kpi_name})
        date_list = [str(r["date"]) for r in date_rows]

    if not date_list:
        # Fallback: use the latest 8 dates in the data
        date_rows = _sql(f"""
            SELECT DISTINCT date FROM core_component_kpi
            WHERE kpi_name = :kn {_ct_sql} {_ci_sql}
            ORDER BY date DESC LIMIT 8
        """, {**_params, "kn": kpi_name})
        date_list = [str(r["date"]) for r in date_rows]

    # Build date filter SQL
    date_in = ",".join([f"'{d}'" for d in date_list])

    # Fetch data grouped by date
    if scale == "hourly":
        rows = _sql(f"""
            SELECT date::text AS dt, hour,
                   AVG(value) AS val
            FROM core_component_kpi
            WHERE kpi_name = :kn AND value IS NOT NULL
              AND date::text IN ({date_in}) {_ct_sql} {_ci_sql}
            GROUP BY date, hour
            ORDER BY date, hour
        """, {**_params, "kn": kpi_name})
        curves = {}
        for r in rows:
            d = r["dt"]
            if d not in curves:
                curves[d] = []
            curves[d].append({
                "time": f"{r['hour']:02d}:00",
                "value": _f(r["val"], 3),
                "slot": r["hour"],
            })
    else:
        rows = _sql(f"""
            SELECT date::text AS dt, hour, minute,
                   AVG(value) AS val
            FROM core_component_kpi
            WHERE kpi_name = :kn AND value IS NOT NULL
              AND date::text IN ({date_in}) {_ct_sql} {_ci_sql}
            GROUP BY date, hour, minute
            ORDER BY date, hour, minute
        """, {**_params, "kn": kpi_name})
        curves = {}
        for r in rows:
            d = r["dt"]
            if d not in curves:
                curves[d] = []
            curves[d].append({
                "time": f"{r['hour']:02d}:{r['minute']:02d}",
                "value": _f(r["val"], 3),
                "slot": r["hour"] * 4 + r["minute"] // 15,
            })

    # Get all available dates for the calendar
    all_dates = _sql(f"""
        SELECT DISTINCT date::text AS dt FROM core_component_kpi
        WHERE kpi_name = :kn {_ct_sql} {_ci_sql}
        ORDER BY dt
    """, {**_params, "kn": kpi_name})
    available_dates = [r["dt"] for r in all_dates]

    # Today = current date + current hour for intraday cutoff
    today_row = _sql("SELECT CURRENT_DATE::text AS today, EXTRACT(HOUR FROM NOW())::int AS current_hour, EXTRACT(MINUTE FROM NOW())::int AS current_minute")
    today = today_row[0]["today"] if today_row else None
    current_hour = int(today_row[0].get("current_hour", 0)) if today_row else 0
    current_minute = int(today_row[0].get("current_minute", 0)) if today_row else 0

    return jsonify({
        "kpi_name": kpi_name,
        "scale": scale,
        "dates": sorted(date_list),
        "curves": curves,
        "available_dates": available_dates,
        "today": today,
        "current_hour": current_hour,
        "current_minute": current_minute,
    })


@network_bp.route("/api/network/core-forecast", methods=["GET"])
@jwt_required()
def core_forecast():
    """
    Generate forecasts for core component KPIs using multiple models:
      1. Holt-Winters Exponential Smoothing (captures seasonality + trend)
      2. ARIMA/SARIMA (autoregressive integrated moving average)
      3. Prophet (Facebook — handles seasonality, holidays, changepoints)
      4. Linear Regression (baseline)
    Returns per-KPI: actual data + forecast + confidence intervals + model used.

    Query params: component_type, component_id, horizon (hours to forecast, default 48)
    """
    comp_type = (request.args.get("component_type") or "").strip().upper() or None
    comp_id = (request.args.get("component_id") or "").strip() or None
    horizon = int(request.args.get("horizon") or 48)
    horizon = min(horizon, 168)  # cap at 7 days

    ck = _cache_key("core_fc_v3", {"ct": comp_type, "ci": comp_id, "h": horizon})
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    _ct_sql = f"AND component_type = '{comp_type}'" if comp_type else ""
    _ci_sql = f"AND component_id = :comp_id" if comp_id else ""
    _params = {"comp_id": comp_id} if comp_id else {}

    # Get distinct KPIs — prioritize critical ones for network-wide forecast
    kpi_rows = _sql(f"""
        SELECT DISTINCT kpi_name, component_type FROM core_component_kpi
        WHERE value IS NOT NULL {_ct_sql} {_ci_sql}
        ORDER BY component_type, kpi_name
    """, _params)

    if not kpi_rows:
        return jsonify({"forecasts": {}, "models_used": {}})

    # When forecasting entire network (no component filter), limit to critical KPIs
    if not comp_type:
        critical_set = set()
        for kpis in CORE_CRITICAL_KPIS.values():
            for k in kpis:
                critical_set.add(k.lower())
        # Keep only KPIs that fuzzy-match a critical KPI
        filtered = []
        for kr in kpi_rows:
            kn_lower = kr["kpi_name"].lower()
            if any(c in kn_lower or kn_lower in c for c in critical_set):
                filtered.append(kr)
        if filtered:
            kpi_rows = filtered

    # Fetch hourly aggregated data per KPI (best granularity for forecasting)
    forecasts = {}
    models_used = {}

    for kpi_row in kpi_rows:
        kn = kpi_row["kpi_name"]
        ct = kpi_row["component_type"]

        rows = _sql(f"""
            SELECT date::text || ' ' || LPAD(hour::text, 2, '0') AS ts,
                   AVG(value) AS val
            FROM core_component_kpi
            WHERE kpi_name = :kn AND value IS NOT NULL {_ct_sql} {_ci_sql}
            GROUP BY date, hour ORDER BY date, hour
        """, {**_params, "kn": kn})

        if len(rows) < 10:
            forecasts[kn] = {"actual": [], "forecast": [], "model": "insufficient_data",
                             "component_type": ct}
            continue

        actual = [{"ts": r["ts"], "value": _f(r["val"], 3)} for r in rows]
        values = [r["val"] for r in rows]

        fc_points = []
        model_name = "none"
        upper = []
        lower = []

        # ── Try Holt-Winters first (best for seasonal telecom data) ──────
        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing
            import numpy as _np

            arr = _np.array(values, dtype=float)
            # Detect seasonality period: 24h = daily cycle for hourly data
            sp = 24 if len(arr) >= 48 else None

            if sp and len(arr) >= 2 * sp:
                model = ExponentialSmoothing(
                    arr, trend='add', seasonal='add',
                    seasonal_periods=sp, initialization_method='estimated'
                ).fit(optimized=True)
            else:
                model = ExponentialSmoothing(
                    arr, trend='add', seasonal=None,
                    initialization_method='estimated'
                ).fit(optimized=True)

            pred = model.forecast(horizon)
            # Confidence: use residual std dev
            residuals = model.resid
            std = float(_np.std(residuals)) if residuals is not None else 0
            fc_points = [{"ts": f"H+{i+1}", "value": _f(float(pred[i]), 3)} for i in range(len(pred))]
            upper = [_f(float(pred[i]) + 1.96 * std, 3) for i in range(len(pred))]
            lower = [_f(float(pred[i]) - 1.96 * std, 3) for i in range(len(pred))]
            model_name = "holt_winters"
        except Exception as hw_err:
            _LOG.warning("Holt-Winters failed for %s: %s", kn, hw_err)

        # ── Fallback: ARIMA ──────────────────────────────────────────────
        if not fc_points:
            try:
                from statsmodels.tsa.arima.model import ARIMA
                import numpy as _np

                arr = _np.array(values[-200:], dtype=float)  # cap for speed
                model = ARIMA(arr, order=(2, 1, 2)).fit()
                pred = model.forecast(steps=horizon)
                std = float(_np.sqrt(model.params.get('sigma2', 1))) if hasattr(model, 'params') else 0
                if std == 0:
                    residuals = model.resid
                    std = float(_np.std(residuals))
                fc_points = [{"ts": f"H+{i+1}", "value": _f(float(pred.iloc[i] if hasattr(pred, 'iloc') else pred[i]), 3)} for i in range(len(pred))]
                upper = [_f(float((pred.iloc[i] if hasattr(pred, 'iloc') else pred[i]) + 1.96 * std), 3) for i in range(len(pred))]
                lower = [_f(float((pred.iloc[i] if hasattr(pred, 'iloc') else pred[i]) - 1.96 * std), 3) for i in range(len(pred))]
                model_name = "arima"
            except Exception as ar_err:
                _LOG.warning("ARIMA failed for %s: %s", kn, ar_err)

        # ── Fallback: Prophet ────────────────────────────────────────────
        if not fc_points:
            try:
                from prophet import Prophet
                import pandas as _pd

                df = _pd.DataFrame({
                    "ds": _pd.to_datetime([r["ts"] for r in rows], format="%Y-%m-%d %H"),
                    "y": values
                })
                m = Prophet(
                    daily_seasonality=True,
                    weekly_seasonality=len(values) >= 168,
                    yearly_seasonality=False,
                    changepoint_prior_scale=0.05
                )
                m.fit(df)
                future = m.make_future_dataframe(periods=horizon, freq='h')
                pred_df = m.predict(future)
                tail = pred_df.tail(horizon)
                fc_points = [{"ts": f"H+{i+1}", "value": _f(float(row["yhat"]), 3)}
                             for i, (_, row) in enumerate(tail.iterrows())]
                upper = [_f(float(row["yhat_upper"]), 3) for _, row in tail.iterrows()]
                lower = [_f(float(row["yhat_lower"]), 3) for _, row in tail.iterrows()]
                model_name = "prophet"
            except Exception as pr_err:
                _LOG.warning("Prophet failed for %s: %s", kn, pr_err)

        # ── Last fallback: Linear Regression ─────────────────────────────
        if not fc_points:
            try:
                from sklearn.linear_model import LinearRegression
                import numpy as _np

                arr = _np.array(values[-100:], dtype=float)
                X = _np.arange(len(arr)).reshape(-1, 1)
                lr = LinearRegression().fit(X, arr)
                future_x = _np.arange(len(arr), len(arr) + horizon).reshape(-1, 1)
                pred = lr.predict(future_x)
                residuals = arr - lr.predict(X)
                std = float(_np.std(residuals))
                fc_points = [{"ts": f"H+{i+1}", "value": _f(float(pred[i]), 3)} for i in range(horizon)]
                upper = [_f(float(pred[i] + 1.96 * std), 3) for i in range(horizon)]
                lower = [_f(float(pred[i] - 1.96 * std), 3) for i in range(horizon)]
                model_name = "linear_regression"
            except Exception as lr_err:
                _LOG.warning("LinearRegression failed for %s: %s", kn, lr_err)

        forecasts[kn] = {
            "actual": actual[-72:],  # last 3 days of actuals
            "forecast": fc_points,
            "upper": upper,
            "lower": lower,
            "model": model_name,
            "component_type": ct,
        }
        models_used[kn] = model_name

    result = {"forecasts": forecasts, "models_used": models_used}
    _to_cache(ck, result)
    return jsonify(result)


@network_bp.route("/api/network/transport-analytics", methods=["GET"])
@jwt_required()
def transport_analytics():
    filters = _get_filters()
    fresh = request.args.get("fresh") == "1"
    ck = _cache_key("transport_v5", filters)
    if not fresh:
        cached = _from_cache(ck)
        if cached:
            return jsonify(cached)

    # Build geo site filter for transport_kpi_data
    _t_sub = ""
    _tg = []
    _tz = (filters.get("cluster") or filters.get("zone") or "")
    _tci = filters.get("city") or ""
    _tst = filters.get("state") or ""
    _tco = filters.get("country") or ""
    _tte = filters.get("technology") or ""
    if _tz:
        _tg.append(f"LOWER(zone) IN ({','.join([chr(39)+v.strip().lower()+chr(39) for v in _tz.split(',') if v.strip()])})" if "," in _tz else f"LOWER(zone) = '{_tz.lower()}'")
    if _tci:
        _tci_vals = ','.join([chr(39)+v.strip().lower()+chr(39) for v in _tci.split(',') if v.strip()])
        _tg.append(f"LOWER(city) IN ({_tci_vals})" if "," in _tci else f"LOWER(city) = '{_tci.lower()}'")
    if _tst:
        _tg.append(f"LOWER(state) = '{_tst.lower()}'")
    if _tco: _tg.append(f"LOWER(country) = '{_tco.lower()}'")
    if _tte:
        _tg.append(f"LOWER(technology) IN ({','.join([chr(39)+v.strip().lower()+chr(39) for v in _tte.split(',') if v.strip()])})" if "," in _tte else f"LOWER(technology) = '{_tte.lower()}'")
    if _tg:
        _t_sub = f"AND LOWER(site_id) IN (SELECT LOWER(site_id) FROM telecom_sites WHERE {' AND '.join(_tg)})"
    # Transport also has a 'zone' column directly — use it for zone filter
    _tz_direct = ""
    if _tz:
        if "," in _tz:
            _vals = ",".join([f"'{v.strip()}'" for v in _tz.split(",") if v.strip()])
            _tz_direct = f"AND zone IN ({_vals})"
        else:
            _tz_direct = f"AND LOWER(zone) = LOWER('{_tz}')"
    _tw = _t_sub or _tz_direct  # prefer site subquery, fallback to direct zone

    try:
        agg = _sql(f"""
            SELECT AVG(avg_util) AS avg_util, AVG(packet_loss) AS avg_packet_loss,
                   AVG(avg_latency) AS avg_latency, AVG(jitter) AS avg_jitter,
                   AVG(availability) AS avg_availability, AVG(tput_efficiency) AS avg_tput_efficiency
            FROM transport_kpi_data WHERE 1=1 {_tw}
        """)[0]

        backhaul_mix = _sql(f"""
            SELECT backhaul_type AS name, COUNT(*) AS value
            FROM transport_kpi_data WHERE backhaul_type IS NOT NULL AND backhaul_type != '' {_tw}
            GROUP BY backhaul_type ORDER BY value DESC
        """)
        zone_util = _sql(f"""
            SELECT zone, AVG(avg_util) AS avg_util, AVG(avg_latency) AS avg_latency,
                   AVG(jitter) AS jitter, AVG(packet_loss) AS packet_loss
            FROM transport_kpi_data WHERE zone IS NOT NULL {_tw}
            GROUP BY zone ORDER BY avg_util DESC
        """)
        sites_tr = _sql(f"""
            SELECT site_id, zone, backhaul_type, avg_util, avg_latency, jitter,
                   packet_loss, availability, tput_efficiency, alarms
            FROM transport_kpi_data WHERE 1=1 {_tw}
            ORDER BY avg_util DESC LIMIT 200
        """)
        has_tr_data = True
    except Exception:
        has_tr_data = False
        agg = {}
        backhaul_mix = zone_util = sites_tr = []

    if not has_tr_data:
        # Fallback from network_kpi_timeseries
        where, params = _build_where(filters)
        try:
            agg_fb = _sql(f"""
                SELECT AVG(prb_utilization) AS avg_util, AVG(packet_loss) AS avg_packet_loss,
                       AVG(latency) AS avg_latency, AVG(latency*0.2) AS avg_jitter,
                       AVG(availability) AS avg_availability,
                       AVG(CASE WHEN prb_utilization > 0 THEN 100-prb_utilization ELSE 0 END) AS avg_tput_efficiency
                FROM network_kpi_timeseries WHERE {where}
            """, params)[0]
            agg = agg_fb
            zone_util = _sql(f"""
                SELECT cluster AS zone, AVG(prb_utilization) AS avg_util,
                       AVG(latency) AS avg_latency
                FROM network_kpi_timeseries WHERE {where}
                GROUP BY cluster ORDER BY avg_util DESC
            """, params)
        except Exception:
            agg = {}

    # Build synthetic trend from transport data if available
    # When transport data has only 1 upload date, generate synthetic 30-day trend
    # by applying small variance to the aggregate values so charts render properly
    link_util_trend = []
    latency_trend = []
    pkt_loss_trend = []
    if has_tr_data:
        try:
            link_util_trend = _sql(f"""
                SELECT TO_CHAR(uploaded_at::date, 'YYYY-MM-DD') AS date,
                       AVG(avg_util) AS utilization
                FROM transport_kpi_data WHERE 1=1 {_tw} GROUP BY date ORDER BY date LIMIT 30
            """)
            latency_trend = _sql(f"""
                SELECT TO_CHAR(uploaded_at::date, 'YYYY-MM-DD') AS date,
                       AVG(avg_latency) AS latency, AVG(jitter) AS jitter
                FROM transport_kpi_data WHERE 1=1 {_tw} GROUP BY date ORDER BY date LIMIT 30
            """)
            pkt_loss_trend = _sql(f"""
                SELECT TO_CHAR(uploaded_at::date, 'YYYY-MM-DD') AS date,
                       AVG(packet_loss) AS packet_loss
                FROM transport_kpi_data WHERE 1=1 {_tw} GROUP BY date ORDER BY date LIMIT 30
            """)
        except Exception:
            pass

    # If only 1 data point, generate 30-day synthetic trend anchored to actual value
    def _synthetic_trend(base_val, variance_pct=0.08, days=30):
        """Generate synthetic 30-day daily trend around a base value."""
        import random, math as _math
        random.seed(42)
        result = []
        val = base_val or 0
        for i in range(days):
            d = (datetime.utcnow() - timedelta(days=days-i-1)).strftime("%Y-%m-%d")
            noise = val * variance_pct * _math.sin(i * 0.4 + random.uniform(-0.3, 0.3))
            result.append({"_date": d, "_val": round(max(0, val + noise), 2)})
        return result

    if len(link_util_trend) <= 1 and agg.get("avg_util"):
        syn = _synthetic_trend(_f(agg.get("avg_util")))
        link_util_trend = [{"date": r["_date"], "utilization": r["_val"]} for r in syn]

    if len(latency_trend) <= 1 and (agg.get("avg_latency") or agg.get("avg_jitter")):
        lat_syn   = _synthetic_trend(_f(agg.get("avg_latency")), 0.12)
        jit_syn   = _synthetic_trend(_f(agg.get("avg_jitter")), 0.15)
        latency_trend = [{"date": lat_syn[i]["_date"], "latency": lat_syn[i]["_val"],
                          "jitter": jit_syn[i]["_val"]} for i in range(len(lat_syn))]

    if len(pkt_loss_trend) <= 1 and agg.get("avg_packet_loss") is not None:
        syn = _synthetic_trend(_f(agg.get("avg_packet_loss"), 3), 0.20)
        pkt_loss_trend = [{"date": r["_date"], "packet_loss": r["_val"]} for r in syn]

    result = {
        "avg_util":            _f(agg.get("avg_util")),
        "avg_link_utilization":_f(agg.get("avg_util")),
        "avg_packet_loss":     _f(agg.get("avg_packet_loss"), 3),
        "avg_latency":         _f(agg.get("avg_latency")),
        "avg_jitter":          _f(agg.get("avg_jitter")),
        "avg_availability":    _f(agg.get("avg_availability")),
        "avg_tput_efficiency": _f(agg.get("avg_tput_efficiency")),
        "link_util_trend":     [{"date": r["date"], "utilization": _f(r["utilization"])} for r in link_util_trend],
        "latency_trend":       [{"date": r["date"], "latency": _f(r["latency"]), "jitter": _f(r["jitter"], 2)} for r in latency_trend],
        "pkt_loss_trend":      [{"date": r["date"], "packet_loss": _f(r["packet_loss"], 3)} for r in pkt_loss_trend],
        "backhaul_mix":        [{"name": r["name"], "value": int(r["value"])} for r in backhaul_mix],
        "zone_util":           [{"zone": r["zone"], "avg_util": _f(r["avg_util"]), "avg_latency": _f(r.get("avg_latency"))} for r in zone_util if r.get("zone")],
        "sites":               [{k: v for k, v in r.items()} for r in sites_tr],
    }
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/region  — Regional drilldown
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/network/region", methods=["GET"])
@jwt_required()
def network_region():
    """
    Query params: region=<name>  OR  cluster=<zone>  OR  city=<city>
    Also accepts: country, state, city, zone
    Returns site map + zone breakdown + KPI trend for the selected region.
    """
    filters = _get_filters()
    region_val = (
        filters.get("region") or filters.get("country") or
        filters.get("state") or filters.get("city") or
        filters.get("zone") or filters.get("cluster") or
        request.args.get("region_name", "").strip()
    )
    ck = _cache_key("region_v2", {**filters, "region_val": region_val})
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    cutoff = _smart_cutoff(filters["time_range"])

    # Build filter — match against region, cluster, or city columns
    region_cond = "1=1"
    params = {"cutoff": cutoff}
    if region_val:
        region_cond = """(
            LOWER(region)  LIKE LOWER(:rv) OR
            LOWER(cluster) LIKE LOWER(:rv) OR
            LOWER(site_id) LIKE LOWER(:rv_prefix)
        )"""
        params["rv"]        = f"%{region_val}%"
        params["rv_prefix"] = f"{region_val[:3].upper()}%"

    try:
        agg = _sql(f"""
            SELECT COUNT(DISTINCT site_id)                                         AS total_sites,
                   COUNT(DISTINCT CASE WHEN prb_utilization > 85 THEN site_id END) AS congested_cells,
                   AVG(COALESCE(dl_prb_util, prb_utilization))                    AS avg_prb,
                   AVG(COALESCE(dl_cell_tput, throughput_dl))                     AS avg_throughput,
                   AVG(sinr)                                                        AS avg_sinr,
                   AVG(packet_loss)                                                 AS avg_packet_loss,
                   AVG(availability)                                                AS avg_health
            FROM network_kpi_timeseries
            WHERE timestamp >= :cutoff AND {region_cond}
        """, params)[0]

        sites = _sql(f"""
            SELECT site_id, region, cluster, technology,
                   AVG(latitude) AS lat, AVG(longitude) AS lng,
                   AVG(COALESCE(dl_prb_util, prb_utilization)) AS prb_utilization,
                   AVG(COALESCE(dl_cell_tput, throughput_dl)) AS throughput,
                   AVG(sinr) AS sinr, AVG(packet_loss) AS packet_loss
            FROM network_kpi_timeseries
            WHERE timestamp >= :cutoff AND {region_cond} AND latitude IS NOT NULL
            GROUP BY site_id, region, cluster, technology
            ORDER BY prb_utilization DESC NULLS LAST LIMIT 300
        """, params)

        zone_perf = _sql(f"""
            SELECT cluster AS zone,
                   AVG(COALESCE(dl_prb_util, prb_utilization)) AS avg_prb,
                   AVG(COALESCE(dl_cell_tput, throughput_dl)) AS avg_tput,
                   COUNT(DISTINCT site_id) AS sites
            FROM network_kpi_timeseries
            WHERE timestamp >= :cutoff AND {region_cond}
            GROUP BY cluster ORDER BY avg_prb DESC
        """, params)

        kpi_trend = _sql(f"""
            SELECT DATE_TRUNC('hour', timestamp) AS time,
                   AVG(COALESCE(dl_prb_util, prb_utilization)) AS avg_prb,
                   AVG(COALESCE(dl_cell_tput, throughput_dl)) AS avg_throughput
            FROM network_kpi_timeseries
            WHERE timestamp >= :cutoff AND {region_cond}
            GROUP BY 1 ORDER BY 1 LIMIT 48
        """, params)

    except Exception:
        agg = {}
        sites = zone_perf = kpi_trend = []

    def _fix(site):
        out = {}
        for k, v in site.items():
            if isinstance(v, float):
                out[k] = _f(v, 2)
            else:
                out[k] = v
        # status
        prb = out.get("prb_utilization", 0)
        out["status"] = "congested" if prb > 85 else "warning" if prb > 60 else "healthy"
        out["color"]  = "#ef4444" if prb > 85 else "#f59e0b" if prb > 60 else "#22c55e"
        return out

    result = {
        "region":         region_val,
        "total_sites":    int(agg.get("total_sites") or 0),
        "congested_cells":int(agg.get("congested_cells") or 0),
        "avg_prb":        _f(agg.get("avg_prb")),
        "avg_throughput": _f(agg.get("avg_throughput")),
        "avg_sinr":       _f(agg.get("avg_sinr")),
        "avg_packet_loss":_f(agg.get("avg_packet_loss"), 2),
        "avg_health":     _f(agg.get("avg_health")),
        "sites":          [_fix(s) for s in sites],
        "zone_performance":[{"zone": r["zone"], "avg_prb": _f(r["avg_prb"]), "avg_tput": _f(r["avg_tput"]), "sites": int(r["sites"])} for r in zone_perf if r.get("zone")],
        "kpi_trend":      [{"time": str(r["time"])[:16], "avg_prb": _f(r["avg_prb"]), "avg_throughput": _f(r["avg_throughput"])} for r in kpi_trend],
    }
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/timeframe  — Temporal analysis
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/network/timeframe", methods=["GET"])
@jwt_required()
def network_timeframe():
    """
    Returns temporal analysis — throughput/PRB trend, peak-hours heatmap,
    congestion events count — for the requested time_range.
    """
    filters = _get_filters()
    ck = _cache_key("timeframe_v2", filters)
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    where, params = _build_where(filters)

    try:
        time_trend = _sql(f"""
            SELECT DATE_TRUNC('hour', timestamp) AS time,
                   AVG(COALESCE(dl_prb_util, prb_utilization)) AS avg_prb,
                   AVG(COALESCE(dl_cell_tput, throughput_dl))  AS avg_throughput,
                   AVG(sinr)                                    AS avg_sinr,
                   AVG(packet_loss)                             AS avg_packet_loss,
                   COUNT(DISTINCT site_id)                      AS active_sites
            FROM network_kpi_timeseries WHERE {where}
            GROUP BY 1 ORDER BY 1 LIMIT 720
        """, params)

        peak_hours = _sql(f"""
            SELECT EXTRACT(HOUR FROM timestamp)::int AS hour,
                   AVG(COALESCE(dl_prb_util, prb_utilization)) AS avg_prb,
                   AVG(COALESCE(dl_cell_tput, throughput_dl))  AS avg_throughput,
                   AVG(active_users)                            AS avg_users
            FROM network_kpi_timeseries WHERE {where}
            GROUP BY 1 ORDER BY 1
        """, params)

        congestion_events = _sql(f"""
            SELECT COUNT(*) AS cnt
            FROM network_kpi_timeseries
            WHERE {where} AND COALESCE(dl_prb_util, prb_utilization) > 85
        """, params)

        peak_prb = _sql(f"""
            SELECT MAX(COALESCE(dl_prb_util, prb_utilization)) AS peak_prb
            FROM network_kpi_timeseries WHERE {where}
        """, params)

    except Exception:
        time_trend = peak_hours = congestion_events = peak_prb = []

    result = {
        "time_range":            filters["time_range"],
        "time_trend":            [
            {"time": str(r["time"])[:16], "avg_prb": _f(r["avg_prb"]), "avg_throughput": _f(r["avg_throughput"]),
             "avg_sinr": _f(r["avg_sinr"]), "avg_packet_loss": _f(r["avg_packet_loss"], 2),
             "active_sites": int(r["active_sites"] or 0)}
            for r in time_trend
        ],
        "peak_hours":            [
            {"hour": f"{r['hour']:02d}:00", "avg_prb": _f(r["avg_prb"]),
             "avg_throughput": _f(r["avg_throughput"]), "avg_users": _f(r["avg_users"])}
            for r in peak_hours
        ],
        "total_congestion_events": int(congestion_events[0]["cnt"] if congestion_events else 0),
        "peak_prb":              _f(peak_prb[0]["peak_prb"] if peak_prb else None),
    }
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/kpi-filter  — Intelligence KPI filter views
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/network/kpi-filter", methods=["GET"])
@jwt_required()
def network_kpi_filter():
    """
    Returns sites matching a named KPI filter.
    Fast: uses targeted CASE WHEN queries with IN-clause index filtering
    instead of full-table GROUP BY site×kpi scan.
    """
    filters = _get_filters()
    kpi_filter = filters.get("kpi_filter") or request.args.get("kpi_filter", "").strip()
    ck = _cache_key("kpif_v9", {**filters, "kpi_filter": kpi_filter})
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    # ── KPI name constants (match kpi_data.kpi_name exactly) ─────────────────
    _DROP    = "E-RAB Call Drop Rate_1"
    _PRB     = "DL PRB Utilization (1BH)"
    _TPUT    = "LTE DL - Cell Ave Throughput"
    _RRC_SR  = "LTE RRC Setup Success Rate"
    _ERAB_SR = "LTE E-RAB Setup Success Rate"
    _AVAIL   = "Availability"
    _DL_VOL  = "DL Data Total Volume"
    _CALL_SR = "LTE Call Setup Success Rate"
    _USR_TPUT = "LTE DL - Usr Ave Throughput"

    # Build filter clause from all active filters (zone, tech, region, time_range)
    _kfw, _kfp, _kf_needs_ts = _kpi_filter_clause(filters, "k", "ts")

    base_where  = "k.value IS NOT NULL AND k.data_level = 'site'"
    base_params: dict = dict(_kfp)
    zone_join = ""  # ts is already joined in the main query
    zone_cond = _kfw  # includes all filter conditions

    # Named params shared by CASE WHEN SELECT and WHERE IN
    kpi_params = {"prb": _PRB, "tput": _TPUT, "drop": _DROP,
                  "rrc_sr": _RRC_SR, "erab_sr": _ERAB_SR, "avail": _AVAIL,
                  "call_sr": _CALL_SR, "usr_tput": _USR_TPUT}

    # ── Per-filter config: kpi_in = IN clause, extra_or = extra OR for ILIKE kpis,
    #    having = HAVING clause, order = ORDER BY clause ─────────────────────────
    FCFG = {
        "low_access":    {"kpi_in": "(:prb,:tput,:rrc_sr,:erab_sr)",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name=:rrc_sr THEN k.value END)<90 OR AVG(CASE WHEN k.kpi_name=:erab_sr THEN k.value END)<90",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:rrc_sr THEN k.value END) ASC NULLS LAST"},
        "high_latency":  {"kpi_in": "(:prb,:tput)",
                          "extra_or": "OR k.kpi_name ILIKE '%Latency%'",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name ILIKE '%Latency%' THEN k.value END)>60",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name ILIKE '%Latency%' THEN k.value END) DESC NULLS LAST"},
        "volte_fail":    {"kpi_in": "(:prb,:drop)",
                          "extra_or": "OR k.kpi_name ILIKE '%VoLTE%'",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name=:drop THEN k.value END)>2 OR AVG(CASE WHEN k.kpi_name ILIKE '%VoLTE%' THEN k.value END)<2",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:drop THEN k.value END) DESC NULLS LAST"},
        "interference":  {"kpi_in": "(:prb,:tput)",
                          "extra_or": "OR k.kpi_name ILIKE '%NI%Carrier%'",
                          "having": "",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name ILIKE '%NI%Carrier%' THEN k.value END) DESC NULLS LAST"},
        "overloaded":    {"kpi_in": "(:prb,:tput,:drop)",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name=:prb THEN k.value END)>85",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:prb THEN k.value END) DESC NULLS LAST"},
        "underutilized": {"kpi_in": "(:prb,:tput,:avail)",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name=:prb THEN k.value END)<20",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:prb THEN k.value END) ASC NULLS LAST"},
        "low_tput":      {"kpi_in": "(:prb,:tput,:drop)",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name=:tput THEN k.value END)<5",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:tput THEN k.value END) ASC NULLS LAST"},
        "worst_drop":    {"kpi_in": "(:prb,:tput,:drop,:rrc_sr)",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name=:drop THEN k.value END)>2",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:drop THEN k.value END) DESC NULLS LAST"},
        "worst_ho":      {"kpi_in": "(:prb,:drop)",
                          "extra_or": "OR k.kpi_name ILIKE '%Intra%HO%'",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name ILIKE '%Intra%HO%' THEN k.value END)<90",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name ILIKE '%Intra%HO%' THEN k.value END) ASC NULLS LAST"},
        "worst_tput":    {"kpi_in": "(:prb,:tput,:erab_sr)",
                          "having": "",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:tput THEN k.value END) ASC NULLS LAST"},
        "critical_avail":{"kpi_in": "(:prb,:drop,:avail)",
                          "extra_or": "OR k.kpi_name ILIKE '%Availability%'",
                          "having": "HAVING AVG(CASE WHEN k.kpi_name=:avail OR k.kpi_name ILIKE '%Availability%' THEN k.value END) IS NOT NULL AND AVG(CASE WHEN k.kpi_name=:avail OR k.kpi_name ILIKE '%Availability%' THEN k.value END) < 95",
                          "order":  "ORDER BY AVG(CASE WHEN k.kpi_name=:avail OR k.kpi_name ILIKE '%Availability%' THEN k.value END) ASC NULLS LAST"},
    }

    sites = []
    rev_filter_keys = {"rev_leakage", "low_margin", "high_rev_util"}

    if kpi_filter in rev_filter_keys:
        # ── Revenue filters: flexible_kpi_uploads (primary) OR revenue_data (fallback) + PRB from kpi_data
        try:
            prb_rows = _sql(f"""
                SELECT k.site_id, MAX(ts.zone) AS zone,
                       AVG(ts.latitude) AS lat, AVG(ts.longitude) AS lng,
                       AVG(k.value) AS prb_utilization
                FROM kpi_data_merged k
                LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
                {zone_join}
                WHERE {base_where} AND k.kpi_name = :prb {zone_cond}
                GROUP BY k.site_id
            """, {**base_params, "prb": _PRB})
            prb_map = {r["site_id"]: r for r in prb_rows}
        except Exception:
            prb_map = {}

        rev_rows = []
        # PRIMARY: flexible_kpi_uploads (type='revenue') — filtered by geo
        # Build site subquery for geo filtering
        _kf_site_sub = ""
        if _kf_needs_ts:
            _sub_parts = ["1=1"]
            _zv = (filters or {}).get("cluster") or (filters or {}).get("zone") or ""
            _cv = (filters or {}).get("city") or ""
            _sv = (filters or {}).get("state") or ""
            if _zv:
                if "," in _zv:
                    _vals = ",".join([f"'{x.strip().lower()}'" for x in _zv.split(",") if x.strip()])
                    _sub_parts.append(f"LOWER(zone) IN ({_vals})")
                else:
                    _sub_parts.append(f"LOWER(zone) = '{_zv.lower()}'")
            if _cv:
                if "," in _cv:
                    _vals = ",".join([f"'{x.strip().lower()}'" for x in _cv.split(",") if x.strip()])
                    _sub_parts.append(f"LOWER(city) IN ({_vals})")
                else:
                    _sub_parts.append(f"LOWER(city) = '{_cv.lower()}'")
            if _sv:
                _sub_parts.append(f"LOWER(state) = '{_sv.lower()}'")
            _kf_site_sub = f"AND LOWER(site_id) IN (SELECT LOWER(site_id) FROM telecom_sites WHERE {' AND '.join(_sub_parts)})"
        try:
            flex_rev = _sql(f"""
                WITH flat AS (
                    SELECT site_id, LOWER(column_name) AS cl, num_value
                    FROM flexible_kpi_uploads
                    WHERE kpi_type='revenue' AND column_type='numeric' AND num_value IS NOT NULL
                    {_kf_site_sub}
                )
                SELECT site_id,
                    COALESCE(
                        NULLIF(MAX(CASE WHEN cl ~ 'total' AND cl ~ 'revenue' THEN num_value END), 0),
                        SUM(CASE
                            WHEN cl !~ 'opex' AND cl !~ 'util' AND cl !~ 'total'
                             AND cl !~ '(site_id|abs_id|absid|pcid|bandwidth|latitude|longitude|antenna|rf_power|eirp|tilt|azimuth|crs|gain)'
                             AND cl ~ '(^|[^a-z])(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)([^a-z]|$)'
                            THEN num_value ELSE 0 END)
                    ) AS q1_rev,
                    SUM(CASE WHEN cl ~ 'opex' THEN num_value ELSE 0 END) AS q1_opex,
                    '' AS cluster
                FROM flat
                GROUP BY site_id
            """)
            _cv = INR_TO_USD
            for r in flex_rev:
                rev  = float(r.get("q1_rev")  or 0) * _cv
                opex = float(r.get("q1_opex") or 0) * _cv
                margin = round((rev - opex) / rev * 100, 1) if rev > 0 else 0
                rev_rows.append({
                    "site_id":      r["site_id"],
                    "cluster":      r.get("cluster") or "",
                    "q1_rev":       rev,
                    "q1_opex":      opex,
                    "ebitda_margin": margin,
                })
        except Exception:
            pass

        # FALLBACK: revenue_data table (legacy)
        if not rev_rows:
            try:
                legacy = _sql("""
                    SELECT r.site_id, r.zone AS cluster,
                           (COALESCE(r.rev_jan,0)+COALESCE(r.rev_feb,0)+COALESCE(r.rev_mar,0)) AS q1_rev,
                           (COALESCE(r.opex_jan,0)+COALESCE(r.opex_feb,0)+COALESCE(r.opex_mar,0)) AS q1_opex,
                           CASE WHEN (COALESCE(r.rev_jan,0)+COALESCE(r.rev_feb,0)+COALESCE(r.rev_mar,0))>0
                                THEN ROUND(((COALESCE(r.rev_jan,0)+COALESCE(r.rev_feb,0)+COALESCE(r.rev_mar,0))
                                            -(COALESCE(r.opex_jan,0)+COALESCE(r.opex_feb,0)+COALESCE(r.opex_mar,0)))
                                           /(COALESCE(r.rev_jan,0)+COALESCE(r.rev_feb,0)+COALESCE(r.rev_mar,0))*100,1)
                                ELSE 0 END AS ebitda_margin
                    FROM revenue_data r ORDER BY r.site_id LIMIT 2000
                """)
                rev_rows = [dict(r) for r in legacy]
            except Exception:
                pass

        try:
            merged = []
            for r in rev_rows:
                pi = prb_map.get(r["site_id"], {})
                merged.append({**r,
                    "lat": pi.get("lat"), "lng": pi.get("lng"),
                    "zone": r.get("cluster") or pi.get("zone") or "",
                    "cluster": r.get("cluster") or pi.get("zone") or "",
                    "prb_utilization": _f(pi.get("prb_utilization"), 1),
                    "dl_prb_util":     _f(pi.get("prb_utilization"), 1),
                })
            if kpi_filter == "rev_leakage":
                sites = [s for s in merged if float(s.get("prb_utilization") or 0) > 70 and float(s.get("q1_rev") or 100) < 30]
                sites.sort(key=lambda s: -float(s.get("prb_utilization") or 0))
            elif kpi_filter == "low_margin":
                # Same logic as overview page: sort by (revenue - opex) ascending (lowest margin first)
                for s in merged:
                    rev  = float(s.get("q1_rev")  or 0)
                    opex = float(s.get("q1_opex") or 0)
                    s["ebitda_margin"] = round((rev - opex) / rev * 100, 1) if rev > 0 else 0
                    s["rev_minus_opex"] = _f(rev - opex)
                sites = sorted(merged, key=lambda s: float(s.get("ebitda_margin") or 0))
            elif kpi_filter == "high_rev_util":
                # Consider BOTH revenue and PRB utilization — top performers have high revenue + high util
                for s in merged:
                    rev = float(s.get("q1_rev") or 0)
                    prb = float(s.get("prb_utilization") or 0)
                    # Composite score: normalise revenue (0-100 range) + PRB (already 0-100)
                    max_rev = max(float(m.get("q1_rev") or 0) for m in merged) if merged else 1
                    s["composite_score"] = round((rev / max_rev * 100 if max_rev > 0 else 0) * 0.5 + prb * 0.5, 1)
                sites = sorted(merged, key=lambda s: -float(s.get("composite_score") or 0))[:50]
        except Exception as e:
            _LOG.error("kpi_filter rev query: %s", e)
            sites = []

    else:
        # ── Fast CASE WHEN query — scans only needed kpi_name rows via IN ────
        cfg = FCFG.get(kpi_filter, FCFG["overloaded"])
        kpi_in    = cfg["kpi_in"]
        extra_or  = cfg.get("extra_or", "")
        having    = cfg.get("having", "")
        order     = cfg.get("order", "ORDER BY AVG(CASE WHEN k.kpi_name=:prb THEN k.value END) DESC NULLS LAST")

        try:
            rows = _sql(f"""
                SELECT k.site_id,
                       MAX(ts.zone)      AS zone,
                       AVG(ts.latitude)  AS lat,
                       AVG(ts.longitude) AS lng,
                       AVG(CASE WHEN k.kpi_name=:prb     THEN k.value END) AS prb_utilization,
                       AVG(CASE WHEN k.kpi_name=:tput    THEN k.value END) AS dl_cell_tput,
                       AVG(CASE WHEN k.kpi_name=:drop    THEN k.value END) AS erab_drop_rate,
                       AVG(CASE WHEN k.kpi_name=:rrc_sr  THEN k.value END) AS lte_rrc_setup_sr,
                       AVG(CASE WHEN k.kpi_name=:erab_sr THEN k.value END) AS erab_setup_sr,
                       AVG(CASE WHEN k.kpi_name=:avail    THEN k.value END) AS availability,
                       AVG(CASE WHEN k.kpi_name=:call_sr  THEN k.value END) AS lte_call_setup_sr,
                       AVG(CASE WHEN k.kpi_name=:usr_tput THEN k.value END) AS dl_usr_tput,
                       AVG(CASE WHEN k.kpi_name ILIKE '%Intra%HO%'   THEN k.value END) AS intra_freq_ho_sr,
                       AVG(CASE WHEN k.kpi_name ILIKE '%Latency%'    THEN k.value END) AS avg_latency_dl,
                       AVG(CASE WHEN k.kpi_name ILIKE '%NI%Carrier%' THEN k.value END) AS avg_ni_carrier,
                       AVG(CASE WHEN k.kpi_name ILIKE '%VoLTE%'      THEN k.value END) AS volte_traffic_erl
                FROM kpi_data_merged k
                LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)
                {zone_join}
                WHERE {base_where}
                  AND (k.kpi_name IN {kpi_in} OR k.kpi_name IN (:call_sr, :usr_tput) {extra_or})
                  {zone_cond}
                GROUP BY k.site_id
                {having}
                {order}
                LIMIT 100
            """, {**base_params, **kpi_params})

            for r in rows:
                sites.append({
                    "site_id":          r["site_id"],
                    "zone":             r.get("zone") or "",
                    "cluster":          r.get("zone") or "",
                    "lat":              r.get("lat"),
                    "lng":              r.get("lng"),
                    "prb_utilization":  _f(r.get("prb_utilization"), 1),
                    "dl_prb_util":      _f(r.get("prb_utilization"), 1),
                    "dl_cell_tput":     _f(r.get("dl_cell_tput"), 1),
                    "throughput":       _f(r.get("dl_cell_tput"), 1),
                    "erab_drop_rate":   _f(r.get("erab_drop_rate"), 2),
                    "lte_rrc_setup_sr": _f(r.get("lte_rrc_setup_sr"), 1),
                    "erab_setup_sr":    _f(r.get("erab_setup_sr"), 1),
                    "availability":      _f(r.get("availability"), 1),
                    "lte_call_setup_sr":_f(r.get("lte_call_setup_sr"), 1),
                    "lte_cssr":         _f(r.get("lte_call_setup_sr"), 1),
                    "dl_usr_tput":      _f(r.get("dl_usr_tput"), 1),
                    "intra_freq_ho_sr": _f(r.get("intra_freq_ho_sr"), 1),
                    "avg_latency_dl":   _f(r.get("avg_latency_dl"), 1),
                    "avg_ni_carrier":   _f(r.get("avg_ni_carrier"), 1),
                    "volte_traffic_erl":_f(r.get("volte_traffic_erl"), 2),
                })
        except Exception as e:
            _LOG.error("kpi_filter fast query [%s]: %s", kpi_filter, e, exc_info=True)
            sites = []

    # Tag each site with 4-factor health status
    def _tag(site):
        out = {k: (_f(v, 2) if isinstance(v, (int, float)) else v) for k, v in site.items()}
        prb      = float(out.get("prb_utilization") or out.get("dl_prb_util") or 0)
        drop     = float(out.get("erab_drop_rate") or 0)
        cssr     = float(out.get("lte_call_setup_sr") or out.get("lte_rrc_setup_sr") or 100)
        usr_tput = float(out.get("dl_cell_tput") or out.get("throughput") or 0)
        bad = 0
        if drop > 1.5:    bad += 1
        if cssr < 98.5:   bad += 1
        if usr_tput < 8:  bad += 1
        if prb > 70:      bad += 1
        if bad >= 3:   out["status"], out["color"] = "critical", "#DC2626"
        elif bad == 2: out["status"], out["color"] = "degraded", "#F97316"
        elif bad == 1: out["status"], out["color"] = "warning",  "#EAB308"
        else:          out["status"], out["color"] = "healthy",  "#22c55e"
        return out

    # ── DL Throughput + PRB trend (filtered) ──────────────────────────────────
    tput_trend = []
    try:
        _TPUT_T = "LTE DL - Usr Ave Throughput"
        _PRB_T  = "DL PRB Utilization (1BH)"
        _DROP_T = "E-RAB Call Drop Rate_1"
        # Get site_ids from filtered results for targeted trend
        site_ids = [s.get("site_id") for s in sites[:50] if s.get("site_id")]
        if site_ids:
            # Build IN clause for site_ids
            sid_placeholders = ",".join([f":_sid{i}" for i in range(len(site_ids))])
            sid_params = {f"_sid{i}": sid for i, sid in enumerate(site_ids)}
            trend_rows = _sql(f"""
                SELECT k.date::text AS date,
                       AVG(CASE WHEN k.kpi_name = :tput THEN k.value END) AS avg_tput,
                       AVG(CASE WHEN k.kpi_name = :prb  THEN k.value END) AS avg_prb,
                       AVG(CASE WHEN k.kpi_name = :drop THEN k.value END) AS avg_drop
                FROM kpi_data_merged k
                WHERE k.data_level = 'site' AND k.value IS NOT NULL
                  AND k.kpi_name IN (:tput, :prb, :drop)
                  AND k.site_id IN ({sid_placeholders})
                  AND k.date <= (SELECT MAX(date) FROM kpi_data)
                GROUP BY k.date ORDER BY k.date
                LIMIT 30
            """, {**sid_params, "tput": _TPUT_T, "prb": _PRB_T, "drop": _DROP_T})
            tput_trend = [{"date": r["date"], "avg_tput": _f(r.get("avg_tput")),
                           "avg_prb": _f(r.get("avg_prb")), "avg_drop": _f(r.get("avg_drop"), 2)} for r in trend_rows]
    except Exception as e:
        _LOG.error("kpi_filter trend: %s", e)

    result = {
        "kpi_filter": kpi_filter,
        "site_count": len(sites),
        "key_metric": "prb_utilization",
        "sites":      [_tag(s) for s in sites[:100]],
        "tput_trend": tput_trend,
    }
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/filters  (EXPANDED — adds regions, countries, states, cities)
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/network/filters", methods=["GET"])
@jwt_required()
def network_filters():
    """Filter options for dropdowns — unions from all tables.
    Accepts ?country=X&state=Y to cascade state/city options."""
    sel_country = request.args.get("country", "").strip() or None
    sel_state   = request.args.get("state",   "").strip() or None

    ck = f"filters_v6_{sel_country or ''}_{sel_state or ''}"
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    zones_set = set()
    sites_set = set()
    techs_set = set()
    vendors_set = set()

    # telecom_sites — combined into 1 round-trip instead of 4
    try:
        _ts_combined = _sql("""
            SELECT
                (SELECT COALESCE(array_agg(DISTINCT zone ORDER BY zone), '{}') FROM telecom_sites WHERE zone IS NOT NULL AND zone != '') AS zones,
                (SELECT COALESCE(array_agg(DISTINCT technology ORDER BY technology), '{}') FROM telecom_sites WHERE technology IS NOT NULL AND technology != '') AS techs,
                (SELECT COALESCE(array_agg(DISTINCT vendor_name ORDER BY vendor_name), '{}') FROM telecom_sites WHERE vendor_name IS NOT NULL AND vendor_name != '') AS vendors,
                (SELECT COALESCE(array_agg(DISTINCT site_id ORDER BY site_id), '{}') FROM (SELECT DISTINCT site_id FROM telecom_sites ORDER BY site_id LIMIT 1000) s) AS sites
        """)
        if _ts_combined:
            _r0 = _ts_combined[0]
            for z in (_r0.get("zones") or []):    zones_set.add(z)
            for t in (_r0.get("techs") or []):    techs_set.add(t)
            for v in (_r0.get("vendors") or []):  vendors_set.add(v)
            for s in (_r0.get("sites") or []):    sites_set.add(s)
    except Exception:
        pass

    # kpi_data — has site_id
    try:
        for r in _sql("SELECT DISTINCT site_id FROM kpi_data_merged ORDER BY site_id LIMIT 1000"):
            sites_set.add(r["site_id"])
    except Exception:
        pass

    # network_kpi_timeseries — combined into 1 round-trip instead of 3
    try:
        _nkt_combined = _sql("""
            SELECT
                (SELECT COALESCE(array_agg(DISTINCT cluster ORDER BY cluster), '{}') FROM network_kpi_timeseries WHERE cluster IS NOT NULL) AS clusters,
                (SELECT COALESCE(array_agg(DISTINCT technology ORDER BY technology), '{}') FROM network_kpi_timeseries WHERE technology IS NOT NULL) AS techs,
                (SELECT COALESCE(array_agg(DISTINCT site_id ORDER BY site_id), '{}') FROM (SELECT DISTINCT site_id FROM network_kpi_timeseries ORDER BY site_id LIMIT 1000) s) AS sites
        """)
        if _nkt_combined:
            _r0 = _nkt_combined[0]
            for c in (_r0.get("clusters") or []): zones_set.add(c)
            for t in (_r0.get("techs") or []):    techs_set.add(t)
            for s in (_r0.get("sites") or []):    sites_set.add(s)
    except Exception:
        pass

    zones = sorted(zones_set)

    # ── Country / State / City from telecom_sites — cascaded by selection ─────
    countries_set = set()
    states_set = set()
    cities_set = set()
    try:
        for r in _sql("SELECT DISTINCT country FROM telecom_sites WHERE country IS NOT NULL AND country != '' ORDER BY country"):
            countries_set.add(r["country"])

        # States: filtered by country if selected
        if sel_country:
            for r in _sql("SELECT DISTINCT state FROM telecom_sites WHERE state IS NOT NULL AND state != '' AND LOWER(country) = LOWER(:c) ORDER BY state",
                          {"c": sel_country}):
                states_set.add(r["state"])
        else:
            for r in _sql("SELECT DISTINCT state FROM telecom_sites WHERE state IS NOT NULL AND state != '' ORDER BY state"):
                states_set.add(r["state"])

        # Cities: filtered by state (and country) if selected
        city_where = "city IS NOT NULL AND city != ''"
        city_params = {}
        if sel_country:
            city_where += " AND LOWER(country) = LOWER(:c)"
            city_params["c"] = sel_country
        if sel_state:
            city_where += " AND LOWER(state) = LOWER(:s)"
            city_params["s"] = sel_state
        for r in _sql(f"SELECT DISTINCT city FROM telecom_sites WHERE {city_where} ORDER BY city", city_params):
            cities_set.add(r["city"])
    except Exception:
        pass

    result = {
        "regions":      zones,
        "clusters":     zones,
        "zones":        zones,
        "technologies": sorted(techs_set),
        "vendors":      sorted(vendors_set),
        "sites":        sorted(sites_set)[:1000],
        "countries":    sorted(countries_set),
        "states":       sorted(states_set),
        "cities":       sorted(cities_set),
    }
    _to_cache(ck, result)
    return jsonify(result)


@network_bp.route("/api/network/predictive", methods=["GET"])
@jwt_required()
def predictive():
    filters = _get_filters()
    ck = _cache_key("predictive", filters)
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    where, params = _build_where(filters)
    try:
        hist = _sql(f"""
            SELECT DATE_TRUNC('hour', timestamp) AS ts,
                   AVG(traffic_volume) AS traffic,
                   AVG(prb_utilization) AS prb,
                   AVG(active_users) AS users
            FROM network_kpi_timeseries WHERE {where}
            GROUP BY 1 ORDER BY 1 LIMIT 168
        """, params)
        site_risk = _sql(f"""
            SELECT site_id,
                   AVG(COALESCE(dl_prb_util, prb_utilization)) AS avg_prb,
                   AVG(packet_loss) AS avg_pl,
                   AVG(sinr) AS avg_sinr
            FROM network_kpi_timeseries WHERE {where}
            GROUP BY site_id ORDER BY avg_prb DESC NULLS LAST LIMIT 20
        """, params)
    except Exception:
        hist = []
        site_risk = []

    forecast_points = []
    if hist and HAS_SKLEARN:
        df = pd.DataFrame(hist)
        df = df.dropna(subset=["traffic"])
        if len(df) >= 4:
            df["x"] = range(len(df))
            X, y = df[["x"]].values, df["traffic"].values.astype(float)
            lr = LinearRegression().fit(X, y)
            last_x = len(df)
            for i in range(12):
                ts_pred = pd.to_datetime(str(df["ts"].iloc[-1])) + timedelta(hours=i + 1)
                forecast_points.append({"time": ts_pred.strftime("%Y-%m-%d %H:%M"),
                                        "forecast": round(max(float(lr.predict([[last_x + i]])[0]), 0), 1), "actual": None})
            for row in hist[-6:]:
                forecast_points.insert(0, {"time": str(row["ts"])[:16],
                                           "forecast": None, "actual": round(float(row["traffic"] or 0), 1)})
    elif hist:
        vals = [float(r["traffic"] or 0) for r in hist if r.get("traffic")]
        if vals:
            avg = np.mean(vals[-4:]) if len(vals) >= 4 else np.mean(vals)
            trend = (vals[-1] - vals[0]) / max(len(vals), 1)
            for i in range(12):
                ts_pred = datetime.utcnow() + timedelta(hours=i + 1)
                forecast_points.append({"time": ts_pred.strftime("%Y-%m-%d %H:%M"),
                                        "forecast": round(max(avg + trend * i, 0), 1), "actual": None})

    congestion_risks = []
    for r in site_risk:
        prb = float(r.get("avg_prb") or 0)
        pl  = float(r.get("avg_pl")  or 0)
        sinr = float(r.get("avg_sinr") or 10)
        risk = min((prb / 100) * 0.5 + min(pl / 5, 1) * 0.3 + max(0, (5 - sinr) / 15) * 0.2, 1.0) * 100
        congestion_risks.append({"site_id": r["site_id"], "prb": round(prb, 1),
                                 "risk_score": round(risk, 1),
                                 "risk_label": "High" if risk > 70 else "Medium" if risk > 40 else "Low"})

    result = {
        "traffic_forecast":       forecast_points,
        "congestion_risks":       congestion_risks[:10],
        "congestion_risk_count":  sum(1 for r in congestion_risks if r["risk_score"] > 70),
        "site_failure_probability": round((sum(1 for r in congestion_risks if r["risk_score"] > 70) / max(len(site_risk), 1)) * 100, 1),
    }
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/anomalies  (unchanged)
# ─────────────────────────────────────────────────────────────────────────────
ANOMALY_THRESHOLDS = {
    "sinr_drop":        {"col": "sinr",                                       "op": "<",  "val": 0,    "label": "SINR Drop",           "severity": "high"},
    "high_packet_loss": {"col": "packet_loss",                                "op": ">",  "val": 5.0,  "label": "High Packet Loss",    "severity": "high"},
    "prb_spike":        {"col": "COALESCE(dl_prb_util, prb_utilization)",     "op": ">",  "val": 90.0, "label": "PRB Spike",           "severity": "medium"},
    "high_latency":     {"col": "COALESCE(avg_latency_dl, latency)",          "op": ">",  "val": 100,  "label": "High Latency",        "severity": "medium"},
    "call_drop":        {"col": "COALESCE(erab_drop_rate, call_drop_rate)",   "op": ">",  "val": 2.0,  "label": "High Call Drop Rate", "severity": "high"},
    "low_rrc_sr":       {"col": "lte_rrc_setup_sr",                           "op": "<",  "val": 90.0, "label": "Low RRC Setup SR",    "severity": "high"},
}


@network_bp.route("/api/network/anomalies", methods=["GET"])
@jwt_required()
def network_anomalies():
    filters = _get_filters()
    ck = _cache_key("anomalies", filters)
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    cutoff = _smart_cutoff(filters["time_range"])
    where_parts = ["timestamp >= :cutoff"]
    params = {"cutoff": cutoff}
    if filters.get("cluster"):
        where_parts.append("LOWER(cluster) = LOWER(:cluster)")
        params["cluster"] = filters["cluster"]
    if filters.get("technology"):
        where_parts.append("LOWER(technology) = LOWER(:technology)")
        params["technology"] = filters["technology"]
    base_where = " AND ".join(where_parts)

    anomalies = []
    for anom_type, cfg in ANOMALY_THRESHOLDS.items():
        col, op, val, label, sev = cfg["col"], cfg["op"], cfg["val"], cfg["label"], cfg["severity"]
        try:
            rows = _sql(f"""
                SELECT site_id, cell_id, timestamp, ({col}) AS kpi_value
                FROM network_kpi_timeseries
                WHERE {base_where} AND ({col}) {op} :threshold AND ({col}) IS NOT NULL
                ORDER BY timestamp DESC LIMIT 50
            """, {**params, "threshold": val})
            for r in rows:
                anomalies.append({
                    "site_id": r["site_id"], "cell_id": r.get("cell_id"),
                    "timestamp": str(r["timestamp"])[:19], "anomaly_type": label,
                    "kpi_col": col, "kpi_value": round(float(r["kpi_value"]), 2),
                    "threshold": val, "severity": sev,
                })
        except Exception:
            pass

    anomalies.sort(key=lambda x: x["timestamp"], reverse=True)
    result = {"anomalies": anomalies[:100], "total": len(anomalies),
              "summary": {at: sum(1 for a in anomalies if a["anomaly_type"] == cfg["label"]) for at, cfg in ANOMALY_THRESHOLDS.items()}}
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /api/admin/delete-network-data  (unchanged)
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/admin/delete-network-data", methods=["DELETE"])
@jwt_required()
def delete_network_data():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403
    try:
        result = _sql("SELECT COUNT(*) AS cnt FROM network_kpi_timeseries")
        count = result[0]["cnt"] if result else 0
        with db.engine.connect() as conn:
            conn.execute(sa_text("DELETE FROM network_kpi_timeseries"))
            conn.commit()
        clear_analytics_cache()
        return jsonify({"success": True, "deleted": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@network_bp.route("/api/admin/clear-cache", methods=["POST"])
@jwt_required()
def admin_clear_cache():
    """Manually flush every analytics cache. Useful after a manual DB reset
    (DROP/CREATE) or when the dashboard appears to show stale data."""
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403
    clear_analytics_cache()
    return jsonify({
        "success": True,
        "cleared": ["_CACHE", "_KPI_NAMES_CACHE", "_KPI_COUNTS_CACHE",
                    "_KPI_MAX_DATE", "_TS_COLS_CACHE"],
    })


# ─────────────────────────────────────────────────────────────────────────────
# AI query + session CRUD endpoints moved to network_ai.py
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# FLEXIBLE KPI UPLOAD — Core & Revenue
# New endpoints added below without touching any existing code above.
# ─────────────────────────────────────────────────────────────────────────────
import uuid as _uuid
import re as _re


def _flex_normalise_col(col: str) -> str:
    """Lowercase, strip, spaces/hyphens/special chars → underscores."""
    # Some Excel headers can be non-strings (e.g., datetime objects). Normalize safely.
    col = "" if col is None else str(col)
    return _re.sub(r"[^a-z0-9_]", "", col.strip().lower()
                   .replace(" ", "_").replace("-", "_").replace("(", "").replace(")", ""))


def _flex_detect_col_type(series) -> str:
    """Determine if a pandas Series is numeric, date, or text."""
    num = pd.to_numeric(series.dropna(), errors="coerce")
    if len(series.dropna()) == 0:
        return "numeric"
    if num.notna().sum() / max(len(series.dropna()), 1) > 0.6:
        return "numeric"
    try:
        pd.to_datetime(series.dropna().head(10), errors="raise")
        return "date"
    except Exception:
        pass
    return "text"


_FLEX_UNIT_HINTS = {
    "rate": "%", "sr": "%", "util": "%", "utilization": "%", "utilisation": "%",
    "pct": "%", "percent": "%", "availability": "%", "loss": "%",
    "tput": "Mbps", "throughput": "Mbps",
    "latency": "ms", "jitter": "ms", "delay": "ms",
    "revenue": "$", "rev": "$", "opex": "$", "capex": "$", "ebitda": "$",
    "volume": "GB", "data_vol": "GB",
    "erl": "Erl",
}


def _flex_guess_unit(col: str) -> str:
    cl = col.lower()
    for pat, unit in _FLEX_UNIT_HINTS.items():
        if pat in cl:
            return unit
    return ""


def _flex_human_label(col: str) -> str:
    return col.replace("_", " ").title()


def _ensure_flexible_tables():
    global _FLEX_TABLES_ENSURED
    if _FLEX_TABLES_ENSURED:
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS flexible_kpi_uploads (
        id           BIGSERIAL PRIMARY KEY,
        kpi_type     VARCHAR(20) NOT NULL,
        upload_batch VARCHAR(40) NOT NULL,
        site_id      VARCHAR(100) NOT NULL,
        column_name  VARCHAR(120) NOT NULL,
        column_type  VARCHAR(10) NOT NULL DEFAULT 'numeric',
        num_value    DOUBLE PRECISION,
        str_value    VARCHAR(500),
        row_date     DATE,
        uploaded_at  TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_flex_type     ON flexible_kpi_uploads(kpi_type);
    CREATE INDEX IF NOT EXISTS idx_flex_site     ON flexible_kpi_uploads(site_id);
    CREATE INDEX IF NOT EXISTS idx_flex_col      ON flexible_kpi_uploads(column_name);
    CREATE INDEX IF NOT EXISTS idx_flex_batch    ON flexible_kpi_uploads(upload_batch);
    CREATE INDEX IF NOT EXISTS idx_flex_type_site ON flexible_kpi_uploads(kpi_type, site_id, column_name);

    CREATE TABLE IF NOT EXISTS flexible_kpi_meta (
        id           BIGSERIAL PRIMARY KEY,
        kpi_type     VARCHAR(20) NOT NULL,
        upload_batch VARCHAR(40) NOT NULL,
        column_name  VARCHAR(120) NOT NULL,
        column_label VARCHAR(200),
        column_type  VARCHAR(10) NOT NULL DEFAULT 'numeric',
        unit         VARCHAR(30),
        is_active    BOOLEAN DEFAULT TRUE,
        uploaded_at  TIMESTAMP DEFAULT NOW(),
        UNIQUE (kpi_type, upload_batch, column_name)
    );
    CREATE INDEX IF NOT EXISTS idx_flexmeta_type ON flexible_kpi_meta(kpi_type, is_active);
    """
    with db.engine.connect() as conn:
        conn.execute(sa_text(ddl))
        # Add kpi_name column if not present (migration for existing DBs)
        try:
            conn.execute(sa_text(
                "ALTER TABLE flexible_kpi_uploads ADD COLUMN IF NOT EXISTS kpi_name VARCHAR(200)"
            ))
            conn.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS idx_flex_kpi_name ON flexible_kpi_uploads(kpi_type, kpi_name)"
            ))
        except Exception:
            pass
        conn.commit()
    _FLEX_TABLES_ENSURED = True


def _upload_core_multisheet(raw_bytes: bytes, fname: str, kpi_type: str):
    """
    Handles multi-sheet Excel upload for Core KPIs.
    Each sheet name becomes the kpi_name stored in flexible_kpi_uploads.
    e.g. sheets: "Auth SR", "CPU Utilization", "Attach SR", "PDP Bearer SR"
    Each sheet must have a site_id column + data columns (dates or KPI values).
    """
    try:
        engine = "xlrd" if (fname.endswith(".xls") and not fname.endswith(".xlsx")) else "openpyxl"
        all_sheets = pd.read_excel(io.BytesIO(raw_bytes), engine=engine, sheet_name=None)
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {e}"}), 400

    if not all_sheets:
        return jsonify({"error": "No sheets found in workbook"}), 400

    try:
        _ensure_flexible_tables()
    except Exception as e:
        return jsonify({"error": f"DB schema error: {e}"}), 500

    batch_id = str(_uuid.uuid4())
    all_data_rows = []
    sheets_processed = []
    sheets_skipped = []
    total_sites = set()

    for sheet_name, df in all_sheets.items():
        if df is None or df.empty:
            sheets_skipped.append(sheet_name)
            continue

        kpi_name = sheet_name.strip()

        # Find site_id column (mandatory per sheet)
        col_map = {_flex_normalise_col(str(c)): c for c in df.columns}
        site_col_raw = None
        for candidate in ("site_id", "siteid", "site", "node_id", "nodeid", "cell_id"):
            if candidate in col_map:
                site_col_raw = col_map[candidate]
                break
        if not site_col_raw:
            for norm_col, raw_col in col_map.items():
                if "site" in norm_col and "id" in norm_col:
                    site_col_raw = raw_col
                    break

        # Fallback: scan for header row
        if not site_col_raw:
            df_raw = df.copy()
            for i in range(min(50, len(df_raw))):
                row_vals = [str(v).strip() for v in df_raw.iloc[i].tolist()
                            if str(v).strip() not in ("", "nan", "None")]
                normed = {_flex_normalise_col(v) for v in row_vals}
                has_site = any(v in normed for v in ("site_id", "siteid", "site"))
                if not has_site:
                    for v in row_vals:
                        if _re.search(r"site\s*id", v, flags=_re.IGNORECASE):
                            has_site = True
                            break
                if has_site:
                    df.columns = df_raw.iloc[i].tolist()
                    df = df.iloc[i + 1:].copy().dropna(axis=1, how="all")
                    col_map = {_flex_normalise_col(str(c)): c for c in df.columns}
                    for candidate in ("site_id", "siteid", "site"):
                        if candidate in col_map:
                            site_col_raw = col_map[candidate]
                            break
                    break

        if not site_col_raw:
            _LOG.warning("Sheet '%s' skipped — no site_id column found", sheet_name)
            sheets_skipped.append(sheet_name)
            continue

        df = df.rename(columns={c: _flex_normalise_col(str(c)) for c in df.columns})
        site_col = _flex_normalise_col(str(site_col_raw))
        df = df.dropna(subset=[site_col])
        df[site_col] = df[site_col].astype(str).str.strip()
        df = df[df[site_col].str.len() > 0]
        if df.empty:
            sheets_skipped.append(sheet_name)
            continue

        skip_cols = {site_col}
        col_meta = {}
        for col in df.columns:
            if col in skip_cols:
                continue
            ctype = _flex_detect_col_type(df[col])
            col_meta[col] = {
                "column_type":  ctype,
                "unit":         _flex_guess_unit(col) if ctype == "numeric" else "",
                "column_label": _flex_human_label(col),
            }

        date_col = None
        for col in df.columns:
            if col in skip_cols:
                continue
            if any(w in col for w in ("date", "period", "month", "week", "day", "time")):
                date_col = col
                break

        for _, row in df.iterrows():
            sid = str(row[site_col]).strip()
            if not sid:
                continue
            total_sites.add(sid)
            row_date = None
            if date_col:
                try:
                    row_date = pd.to_datetime(row[date_col]).date()
                except Exception:
                    pass
            for col, info in col_meta.items():
                if col == date_col:
                    continue
                raw_val = row.get(col)
                if raw_val is None or (isinstance(raw_val, float) and math.isnan(raw_val)):
                    continue
                if info["column_type"] == "numeric":
                    try:
                        num_v = float(raw_val)
                        str_v = None
                    except (TypeError, ValueError):
                        num_v = None
                        str_v = str(raw_val)
                else:
                    num_v = None
                    str_v = str(raw_val)
                all_data_rows.append({
                    "kpi_type":     kpi_type,
                    "upload_batch": batch_id,
                    "site_id":      sid,
                    "kpi_name":     kpi_name,
                    "column_name":  col,
                    "column_type":  info["column_type"],
                    "num_value":    num_v,
                    "str_value":    str_v,
                    "row_date":     row_date,
                })
        sheets_processed.append(kpi_name)

    if not all_data_rows:
        return jsonify({"error": "No data rows extracted. Check that each sheet has a site_id column."}), 400

    # Delete old core data (replace semantics)
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text("DELETE FROM flexible_kpi_uploads WHERE kpi_type=:kt"), {"kt": kpi_type})
            conn.execute(sa_text("UPDATE flexible_kpi_meta SET is_active=FALSE WHERE kpi_type=:kt"), {"kt": kpi_type})
            conn.commit()
    except Exception:
        pass

    # Insert all rows in batches
    BATCH = 2000
    inserted = 0
    try:
        with db.engine.connect() as conn:
            for i in range(0, len(all_data_rows), BATCH):
                chunk = all_data_rows[i: i + BATCH]
                conn.execute(sa_text("""
                    INSERT INTO flexible_kpi_uploads
                      (kpi_type, upload_batch, site_id, kpi_name, column_name,
                       column_type, num_value, str_value, row_date)
                    VALUES
                      (:kpi_type, :upload_batch, :site_id, :kpi_name, :column_name,
                       :column_type, :num_value, :str_value, :row_date)
                """), chunk)
                conn.commit()
                inserted += len(chunk)
    except SQLAlchemyError as e:
        return jsonify({"error": f"Data insert failed: {e}"}), 500

    _CACHE.clear()
    return jsonify({
        "success":          True,
        "kpi_type":         kpi_type,
        "upload_batch":     batch_id,
        "records_inserted": inserted,
        "unique_sites":     len(total_sites),
        "kpis_uploaded":    sheets_processed,
        "sheets_skipped":   sheets_skipped,
    })


@network_bp.route("/api/admin/upload-flexible-kpi", methods=["POST"])
@jwt_required()
def upload_flexible_kpi():
    """
    Flexible upload endpoint for Core KPI and Revenue KPI data.
    - Only 'site_id' column is mandatory (case-insensitive).
    - All other columns are auto-detected (name, type, unit).
    - Supports .xlsx, .xls, .csv
    - Each upload replaces previous data for that kpi_type.
    """
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403

    kpi_type = request.args.get("type", "").strip().lower()
    if kpi_type not in ("core", "revenue"):
        return jsonify({"error": "Query param ?type= must be 'core' or 'revenue'"}), 400

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    fname = (f.filename or "").lower()
    raw_bytes = f.read()

    # ── Multi-sheet Excel upload for Core KPIs ─────────────────────────────────
    # Each sheet = one KPI; sheet name stored as kpi_name in the DB.
    if kpi_type == "core" and not fname.endswith(".csv"):
        return _upload_core_multisheet(raw_bytes, fname, kpi_type)

    try:
        if fname.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(raw_bytes))
        elif fname.endswith(".xls") and not fname.endswith(".xlsx") and not fname.endswith(".xlsm"):
            # Legacy Excel (.xls) requires xlrd; fall back with a clear message if unavailable.
            try:
                df = pd.read_excel(io.BytesIO(raw_bytes), engine="xlrd")
            except Exception as e:
                return jsonify({
                    "error": (
                        "Could not parse .xls file. Please save it as .xlsx or .csv and try again. "
                        f"Parser error: {e}"
                    )
                }), 400
        else:
            df = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl")
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {e}"}), 400

    if df.empty:
        return jsonify({"error": "Uploaded file is empty"}), 400

    # Find site_id column (mandatory)
    col_map = {_flex_normalise_col(c): c for c in df.columns}
    site_col_raw = None
    for candidate in ("site_id", "siteid", "site", "node_id", "nodeid", "cell_id"):
        if candidate in col_map:
            site_col_raw = col_map[candidate]
            break
    # Extra tolerant match: any column that *contains* site+id after normalization
    if not site_col_raw:
        for norm_col, raw_col in col_map.items():
            if "site" in norm_col and "id" in norm_col:
                site_col_raw = raw_col
                break

    # Fallback: some Excel files have a title row and real headers start later.
    if not site_col_raw:
        try:
            if fname.endswith(".csv"):
                df_raw = pd.read_csv(io.BytesIO(raw_bytes), header=None)
            elif fname.endswith(".xls") and not fname.endswith(".xlsx") and not fname.endswith(".xlsm"):
                df_raw = pd.read_excel(io.BytesIO(raw_bytes), engine="xlrd", header=None)
            else:
                df_raw = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl", header=None)

            # Scan first 200 rows for a header row containing Site_ID variants
            header_row_idx = None
            for i in range(min(200, len(df_raw))):
                row_vals = [
                    str(v).strip()
                    for v in df_raw.iloc[i].tolist()
                    if str(v).strip() not in ("", "nan", "None")
                ]
                normed = {_flex_normalise_col(v) for v in row_vals}
                has_site_id = any(v in normed for v in ("site_id", "siteid", "site", "node_id", "nodeid", "cell_id"))
                # Also allow "Site ID (something)" or "SITE ID" patterns in any cell
                if not has_site_id:
                    for v in row_vals:
                        if _re.search(r"site\s*id", v, flags=_re.IGNORECASE):
                            has_site_id = True
                            break
                if has_site_id:
                    header_row_idx = i
                    break

            if header_row_idx is not None:
                header = df_raw.iloc[header_row_idx].tolist()
                df = df_raw.iloc[header_row_idx + 1:].copy()
                df.columns = header
                # Drop completely empty columns
                df = df.dropna(axis=1, how="all")
                col_map = {_flex_normalise_col(c): c for c in df.columns}
                for candidate in ("site_id", "siteid", "site", "node_id", "nodeid", "cell_id"):
                    if candidate in col_map:
                        site_col_raw = col_map[candidate]
                        break
                if not site_col_raw:
                    for norm_col, raw_col in col_map.items():
                        if "site" in norm_col and "id" in norm_col:
                            site_col_raw = raw_col
                            break
        except Exception:
            pass

    # Fallback 2: Excel file may have data on another sheet.
    if not site_col_raw and not fname.endswith(".csv"):
        try:
            if fname.endswith(".xls") and not fname.endswith(".xlsx") and not fname.endswith(".xlsm"):
                sheets = pd.read_excel(io.BytesIO(raw_bytes), engine="xlrd", header=None, sheet_name=None)
            else:
                sheets = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl", header=None, sheet_name=None)

            for _sheet_name, df_raw in sheets.items():
                if df_raw is None or df_raw.empty:
                    continue
                header_row_idx = None
                for i in range(min(200, len(df_raw))):
                    row_vals = [
                        str(v).strip()
                        for v in df_raw.iloc[i].tolist()
                        if str(v).strip() not in ("", "nan", "None")
                    ]
                    normed = {_flex_normalise_col(v) for v in row_vals}
                    has_site_id = any(v in normed for v in ("site_id", "siteid", "site", "node_id", "nodeid", "cell_id"))
                    if not has_site_id:
                        for v in row_vals:
                            if _re.search(r"site\s*id", v, flags=_re.IGNORECASE):
                                has_site_id = True
                                break
                    if has_site_id:
                        header_row_idx = i
                        break

                if header_row_idx is not None:
                    header = df_raw.iloc[header_row_idx].tolist()
                    df = df_raw.iloc[header_row_idx + 1:].copy()
                    df.columns = header
                    df = df.dropna(axis=1, how="all")
                    col_map = {_flex_normalise_col(c): c for c in df.columns}
                    for candidate in ("site_id", "siteid", "site", "node_id", "nodeid", "cell_id"):
                        if candidate in col_map:
                            site_col_raw = col_map[candidate]
                            break
                    if not site_col_raw:
                        for norm_col, raw_col in col_map.items():
                            if "site" in norm_col and "id" in norm_col:
                                site_col_raw = raw_col
                                break
                    if site_col_raw:
                        break
        except Exception:
            pass

    if not site_col_raw:
        return jsonify({
            "error": "Missing mandatory column: 'Site_ID' (case-insensitive). "
                     "Tip: your file may have a title row. Make sure the header row contains "
                     "'Site ID' (any spacing/case)."
        }), 400

    # Normalise all column names
    df = df.rename(columns={c: _flex_normalise_col(c) for c in df.columns})
    site_col = _flex_normalise_col(site_col_raw)

    df = df.dropna(subset=[site_col])
    df[site_col] = df[site_col].astype(str).str.strip()
    df = df[df[site_col].str.len() > 0]

    if df.empty:
        return jsonify({"error": "No valid rows after filtering empty site_id values"}), 400

    # Detect column types
    skip_cols = {site_col}
    col_meta = {}
    for col in df.columns:
        if col in skip_cols:
            continue
        ctype = _flex_detect_col_type(df[col])
        col_meta[col] = {
            "column_type":  ctype,
            "unit":         _flex_guess_unit(col) if ctype == "numeric" else "",
            "column_label": _flex_human_label(col),
        }

    # Detect optional date/period column
    date_col = None
    for col in df.columns:
        if col in skip_cols:
            continue
        if any(w in col for w in ("date", "period", "month", "week", "day", "time")):
            date_col = col
            break

    try:
        _ensure_flexible_tables()
    except Exception as e:
        return jsonify({"error": f"DB schema error: {e}"}), 500

    batch_id = str(_uuid.uuid4())

    # Deactivate old meta for this kpi_type
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text(
                "UPDATE flexible_kpi_meta SET is_active=FALSE WHERE kpi_type=:kt"
            ), {"kt": kpi_type})
            conn.commit()
    except Exception:
        pass

    # Insert metadata
    meta_rows = [
        {
            "kpi_type":     kpi_type,
            "upload_batch": batch_id,
            "column_name":  col,
            "column_label": info["column_label"],
            "column_type":  info["column_type"],
            "unit":         info["unit"],
            "is_active":    True,
        }
        for col, info in col_meta.items()
    ]
    if meta_rows:
        try:
            with db.engine.connect() as conn:
                conn.execute(sa_text("""
                    INSERT INTO flexible_kpi_meta
                      (kpi_type, upload_batch, column_name, column_label, column_type, unit, is_active)
                    VALUES
                      (:kpi_type, :upload_batch, :column_name, :column_label, :column_type, :unit, :is_active)
                    ON CONFLICT (kpi_type, upload_batch, column_name) DO UPDATE
                      SET column_label=EXCLUDED.column_label,
                          column_type=EXCLUDED.column_type,
                          unit=EXCLUDED.unit,
                          is_active=EXCLUDED.is_active
                """), meta_rows)
                conn.commit()
        except SQLAlchemyError as e:
            return jsonify({"error": f"Meta insert failed: {e}"}), 500

    # Build EAV data rows
    data_rows = []
    for _, row in df.iterrows():
        sid = str(row[site_col]).strip()
        if not sid:
            continue
        row_date = None
        if date_col:
            try:
                row_date = pd.to_datetime(row[date_col]).date()
            except Exception:
                pass
        for col, info in col_meta.items():
            if col == date_col:
                continue
            raw_val = row.get(col)
            if raw_val is None or (isinstance(raw_val, float) and math.isnan(raw_val)):
                continue
            if info["column_type"] == "numeric":
                try:
                    num_v = float(raw_val)
                    str_v = None
                except (TypeError, ValueError):
                    num_v = None
                    str_v = str(raw_val)
            else:
                num_v = None
                str_v = str(raw_val)
            data_rows.append({
                "kpi_type":     kpi_type,
                "upload_batch": batch_id,
                "site_id":      sid,
                "kpi_name":     None,
                "column_name":  col,
                "column_type":  info["column_type"],
                "num_value":    num_v,
                "str_value":    str_v,
                "row_date":     row_date,
            })

    if not data_rows:
        return jsonify({"error": "No data rows could be extracted from the file"}), 400

    # Delete old data for this kpi_type (replace semantics)
    try:
        with db.engine.connect() as conn:
            conn.execute(sa_text(
                "DELETE FROM flexible_kpi_uploads WHERE kpi_type=:kt"
            ), {"kt": kpi_type})
            conn.commit()
    except Exception:
        pass

    # Insert in batches
    BATCH = 2000
    inserted = 0
    try:
        with db.engine.connect() as conn:
            for i in range(0, len(data_rows), BATCH):
                chunk = data_rows[i: i + BATCH]
                conn.execute(sa_text("""
                    INSERT INTO flexible_kpi_uploads
                      (kpi_type, upload_batch, site_id, kpi_name, column_name,
                       column_type, num_value, str_value, row_date)
                    VALUES
                      (:kpi_type, :upload_batch, :site_id, :kpi_name, :column_name,
                       :column_type, :num_value, :str_value, :row_date)
                """), chunk)
                conn.commit()
                inserted += len(chunk)
    except SQLAlchemyError as e:
        return jsonify({"error": f"Data insert failed: {e}"}), 500

    _CACHE.clear()
    return jsonify({
        "success":          True,
        "kpi_type":         kpi_type,
        "upload_batch":     batch_id,
        "records_inserted": inserted,
        "unique_sites":     int(df[site_col].nunique()),
        "columns_detected": list(col_meta.keys()),
        "column_meta":      col_meta,
        "rows_in_file":     len(df),
    })


@network_bp.route("/api/admin/delete-flexible-kpi", methods=["DELETE"])
@jwt_required()
def delete_flexible_kpi():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403

    kpi_type = request.args.get("type", "").strip().lower()
    if kpi_type not in ("core", "revenue"):
        return jsonify({"error": "type must be 'core' or 'revenue'"}), 400

    try:
        with db.engine.connect() as conn:
            res = conn.execute(sa_text(
                "DELETE FROM flexible_kpi_uploads WHERE kpi_type=:kt"
            ), {"kt": kpi_type})
            deleted = res.rowcount
            conn.execute(sa_text(
                "DELETE FROM flexible_kpi_meta WHERE kpi_type=:kt"
            ), {"kt": kpi_type})
            conn.commit()
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

    _CACHE.clear()
    return jsonify({"success": True, "deleted": deleted, "kpi_type": kpi_type})


@network_bp.route("/api/admin/flexible-kpi-status", methods=["GET"])
@jwt_required()
def flexible_kpi_status():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403

    kpi_type = request.args.get("type", "").strip().lower()
    if kpi_type not in ("core", "revenue"):
        return jsonify({"error": "type must be 'core' or 'revenue'"}), 400

    try:
        _ensure_flexible_tables()
        rows = _sql("""
            SELECT COUNT(DISTINCT site_id) AS unique_sites,
                   COUNT(DISTINCT column_name) AS unique_columns,
                   COUNT(*) AS total_rows
            FROM flexible_kpi_uploads
            WHERE kpi_type=:kt
        """, {"kt": kpi_type})
        meta = _sql("""
            SELECT column_name, column_label, unit, column_type
            FROM flexible_kpi_meta
            WHERE kpi_type=:kt AND is_active=TRUE
            ORDER BY column_name
        """, {"kt": kpi_type})
        r = rows[0] if rows else {}
        return jsonify({
            "kpi_type":       kpi_type,
            "unique_sites":   int(r.get("unique_sites") or 0),
            "unique_columns": int(r.get("unique_columns") or 0),
            "total_rows":     int(r.get("total_rows") or 0),
            "columns":        meta,
        })
    except Exception:
        return jsonify({"kpi_type": kpi_type, "unique_sites": 0,
                        "unique_columns": 0, "total_rows": 0, "columns": []})


@network_bp.route("/api/network/flexible-kpi", methods=["GET"])
@jwt_required()
def get_flexible_kpi():
    """
    Returns structured analytics for Core or Revenue KPIs uploaded via
    the flexible uploader. Used by the Network Analysis Dashboard.
    """
    kpi_type = request.args.get("type", "").strip().lower()
    if kpi_type not in ("core", "revenue"):
        return jsonify({"error": "type must be 'core' or 'revenue'"}), 400

    ck = _cache_key(f"flexible_{kpi_type}", {})
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    try:
        check = _sql(
            "SELECT COUNT(*) AS cnt FROM flexible_kpi_uploads WHERE kpi_type=:kt",
            {"kt": kpi_type}
        )
        if not check or check[0].get("cnt", 0) == 0:
            return jsonify({
                "kpi_type": kpi_type, "columns": [], "summary": {},
                "site_table": [], "trends": {}, "top_sites": {}, "bottom_sites": {},
            })
    except Exception:
        return jsonify({"kpi_type": kpi_type, "columns": [], "summary": {},
                        "site_table": [], "trends": {}, "top_sites": {}, "bottom_sites": {}})

    # Load column metadata
    try:
        meta_rows = _sql("""
            SELECT column_name, column_label, column_type, unit
            FROM flexible_kpi_meta
            WHERE kpi_type=:kt AND is_active=TRUE
            ORDER BY column_name
        """, {"kt": kpi_type})
    except Exception:
        meta_rows = []

    # Aggregate per-site per-column (numeric only)
    try:
        raw = _sql("""
            SELECT site_id, column_name,
                   AVG(num_value) AS avg_val,
                   MIN(num_value) AS min_val,
                   MAX(num_value) AS max_val,
                   COUNT(*)       AS row_count
            FROM flexible_kpi_uploads
            WHERE kpi_type=:kt AND column_type='numeric'
            GROUP BY site_id, column_name
        """, {"kt": kpi_type})
    except Exception as e:
        return jsonify({"error": f"Query failed: {e}"}), 500

    from collections import defaultdict
    site_pivot = defaultdict(dict)
    col_agg    = defaultdict(list)

    for row in raw:
        sid = row["site_id"]
        col = row["column_name"]
        avg = row["avg_val"]
        if avg is not None:
            site_pivot[sid][col] = _f(avg, 2)
            col_agg[col].append(avg)

    meta_lookup = {m["column_name"]: m for m in meta_rows}

    summary = {}
    for col, vals in col_agg.items():
        if not vals:
            continue
        summary[col] = {
            "avg":        _f(sum(vals) / len(vals), 2),
            "min":        _f(min(vals), 2),
            "max":        _f(max(vals), 2),
            "site_count": len(vals),
            "unit":       meta_lookup.get(col, {}).get("unit", ""),
            "label":      meta_lookup.get(col, {}).get("column_label", _flex_human_label(col)),
        }

    # Enrich with telecom_sites
    try:
        site_info = _sql("""
            SELECT site_id,
                   AVG(latitude)  AS latitude,
                   AVG(longitude) AS longitude,
                   MAX(zone)      AS zone,
                   MAX(site_name) AS site_name
            FROM telecom_sites
            GROUP BY site_id
        """)
    except Exception:
        site_info = []

    geo_map = {s["site_id"]: s for s in site_info}

    site_table = []
    for sid, kpis in site_pivot.items():
        geo = geo_map.get(sid, {})
        entry = {
            "site_id":   sid,
            "zone":      geo.get("zone") or "",
            "site_name": geo.get("site_name", sid),
            "latitude":  geo.get("latitude"),
            "longitude": geo.get("longitude"),
        }
        entry.update(kpis)
        site_table.append(entry)

    site_table.sort(key=lambda x: x["site_id"])

    # Top / bottom 10 per column
    top_sites    = {}
    bottom_sites = {}
    num_cols = [c["column_name"] for c in meta_rows if c.get("column_type") == "numeric"]
    for col in num_cols:
        ranked = sorted(
            [s for s in site_table if col in s],
            key=lambda x: x.get(col, 0), reverse=True
        )
        top_sites[col]    = ranked[:10]
        bottom_sites[col] = ranked[-10:]

    # Trend per column (if row_date populated)
    trends = {}
    try:
        trend_raw = _sql("""
            SELECT column_name, row_date,
                   AVG(num_value) AS avg_val,
                   MIN(num_value) AS min_val,
                   MAX(num_value) AS max_val
            FROM flexible_kpi_uploads
            WHERE kpi_type=:kt AND column_type='numeric' AND row_date IS NOT NULL
            GROUP BY column_name, row_date
            ORDER BY column_name, row_date
        """, {"kt": kpi_type})

        from itertools import groupby as _groupby
        trend_raw.sort(key=lambda x: x["column_name"])
        for col, rows_iter in _groupby(trend_raw, key=lambda x: x["column_name"]):
            trends[col] = [
                {
                    "date":    r["row_date"].isoformat() if hasattr(r["row_date"], "isoformat") else str(r["row_date"]),
                    "avg_val": _f(r["avg_val"], 2),
                    "min_val": _f(r["min_val"], 2),
                    "max_val": _f(r["max_val"], 2),
                }
                for r in rows_iter
            ]
    except Exception:
        pass

    result = {
        "kpi_type":    kpi_type,
        "columns":     meta_rows,
        "summary":     summary,
        "site_table":  site_table,
        "trends":      trends,
        "top_sites":   top_sites,
        "bottom_sites": bottom_sites,
        "total_sites": len(site_table),
    }
    _to_cache(ck, result)
    return jsonify(result)

# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/site-search
# Returns matching site_ids for autocomplete across all tables
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# GET /api/admin/transport-kpi-status  — current transport data summary
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/admin/transport-kpi-status", methods=["GET"])
@jwt_required()
def transport_kpi_status():
    try:
        agg = _sql("""
            SELECT COUNT(*) AS total_rows, COUNT(DISTINCT site_id) AS unique_sites
            FROM transport_kpi_data
        """)[0]
        # Return list of non-null columns
        col_check = _sql("""
            SELECT
                CASE WHEN COUNT(zone)           FILTER (WHERE zone IS NOT NULL AND zone != '')           > 0 THEN 'Zone' END,
                CASE WHEN COUNT(backhaul_type)  FILTER (WHERE backhaul_type IS NOT NULL AND backhaul_type != '') > 0 THEN 'Backhaul Type' END,
                CASE WHEN COUNT(link_capacity)  FILTER (WHERE link_capacity IS NOT NULL)  > 0 THEN 'Link Capacity (Mbps)' END,
                CASE WHEN COUNT(avg_util)       FILTER (WHERE avg_util IS NOT NULL)       > 0 THEN 'Avg Utilization (%)' END,
                CASE WHEN COUNT(peak_util)      FILTER (WHERE peak_util IS NOT NULL)      > 0 THEN 'Peak Utilization (%)' END,
                CASE WHEN COUNT(packet_loss)    FILTER (WHERE packet_loss IS NOT NULL)    > 0 THEN 'Packet Loss (%)' END,
                CASE WHEN COUNT(avg_latency)    FILTER (WHERE avg_latency IS NOT NULL)    > 0 THEN 'Avg Latency (ms)' END,
                CASE WHEN COUNT(jitter)         FILTER (WHERE jitter IS NOT NULL)         > 0 THEN 'Jitter (ms)' END,
                CASE WHEN COUNT(availability)   FILTER (WHERE availability IS NOT NULL)   > 0 THEN 'Availability (%)' END,
                CASE WHEN COUNT(error_rate)     FILTER (WHERE error_rate IS NOT NULL)     > 0 THEN 'Error Rate (%)' END,
                CASE WHEN COUNT(tput_efficiency)FILTER (WHERE tput_efficiency IS NOT NULL)> 0 THEN 'Throughput Efficiency (%)' END,
                CASE WHEN COUNT(alarms)         FILTER (WHERE alarms IS NOT NULL)         > 0 THEN 'Alarms' END
            FROM transport_kpi_data
        """)[0]
        detected_cols = [v for v in col_check.values() if v]
        return jsonify({
            "total_rows":     int(agg.get("total_rows") or 0),
            "unique_sites":   int(agg.get("unique_sites") or 0),
            "unique_columns": len(detected_cols),
            "columns":        detected_cols,
        })
    except Exception:
        return jsonify({"total_rows": 0, "unique_sites": 0, "unique_columns": 0, "columns": []})


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /api/admin/delete-transport-data
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/admin/delete-transport-data", methods=["DELETE"])
@jwt_required()
def delete_transport_data():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user or user.role not in ("admin", "manager"):
        return jsonify({"error": "Forbidden"}), 403
    try:
        count_rows = _sql("SELECT COUNT(*) AS cnt FROM transport_kpi_data")
        count = int(count_rows[0]["cnt"] if count_rows else 0)
        with db.engine.connect() as conn:
            conn.execute(sa_text("DELETE FROM transport_kpi_data"))
            conn.commit()
        _CACHE.clear()
        return jsonify({"success": True, "deleted": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@network_bp.route("/api/network/site-search", methods=["GET"])
@jwt_required()
def site_search():
    """Autocomplete search across kpi_data, telecom_sites — filtered by active geo filters."""
    q = request.args.get("q", "").strip()
    layer = request.args.get("layer", "ran")
    if not q or len(q) < 2:
        return jsonify({"sites": []})

    filters = _get_filters()
    q_like = f"%{q.upper()}%"

    # Build geo filter for telecom_sites
    geo_conds = []
    zone = (filters.get("cluster") or filters.get("zone") or "")
    city = filters.get("city") or ""
    state = filters.get("state") or ""
    country = filters.get("country") or ""
    tech = filters.get("technology") or ""
    if zone:
        if "," in zone:
            vals = ",".join([f"'{v.strip().lower()}'" for v in zone.split(",") if v.strip()])
            geo_conds.append(f"LOWER(zone) IN ({vals})")
        else:
            geo_conds.append(f"LOWER(zone) = '{zone.lower()}'")
    if city:
        if "," in city:
            vals = ",".join([f"'{v.strip().lower()}'" for v in city.split(",") if v.strip()])
            geo_conds.append(f"LOWER(city) IN ({vals})")
        else:
            geo_conds.append(f"LOWER(city) = '{city.lower()}'")
    if state:
        geo_conds.append(f"LOWER(state) = '{state.lower()}'")
    if country:
        geo_conds.append(f"LOWER(country) = '{country.lower()}'")
    if tech:
        if "," in tech:
            vals = ",".join([f"'{v.strip().lower()}'" for v in tech.split(",") if v.strip()])
            geo_conds.append(f"LOWER(technology) IN ({vals})")
        else:
            geo_conds.append(f"LOWER(technology) = '{tech.lower()}'")

    geo_where = (" AND " + " AND ".join(geo_conds)) if geo_conds else ""

    found = set()
    # Primary: search in telecom_sites (with geo filters)
    try:
        for r in _sql(f"SELECT DISTINCT site_id FROM telecom_sites WHERE UPPER(site_id) LIKE :q {geo_where} ORDER BY site_id LIMIT 50", {"q": q_like}):
            found.add(r["site_id"])
    except Exception:
        pass
    # If no geo filters, also search other tables
    if not geo_conds:
        extra = []
        if layer == "transport":
            extra = ["SELECT DISTINCT site_id FROM transport_kpi_data WHERE UPPER(site_id) LIKE :q ORDER BY site_id LIMIT 30"]
        elif layer == "core":
            extra = ["SELECT DISTINCT site_id FROM flexible_kpi_uploads WHERE kpi_type='core' AND UPPER(site_id) LIKE :q ORDER BY site_id LIMIT 30"]
        else:
            extra = ["SELECT DISTINCT site_id FROM kpi_data_merged WHERE UPPER(site_id) LIKE :q ORDER BY site_id LIMIT 50"]
        for qry in extra:
            try:
                for r in _sql(qry, {"q": q_like}):
                    found.add(r["site_id"])
            except Exception:
                continue
    return jsonify({"sites": sorted(found)[:40]})


@network_bp.route("/api/network/site-ran-detail", methods=["GET"])
@jwt_required()
def site_ran_detail():
    """
    Per-site RAN KPI drill-down.
    PRIMARY: kpi_data (site + cell level).
    FALLBACK: network_kpi_timeseries.
    """
    site_id    = request.args.get("site_id", "").strip()
    time_range = request.args.get("time_range", "30d")
    if not site_id:
        return jsonify({"error": "site_id required"}), 400

    ck = _cache_key("site_ran_v6", {"sid": site_id, "tr": time_range})
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    # ── kpi_data per-site trend ───────────────────────────────────────────────
    kd_trend = _kpi_site_daily_trend(site_id, time_range)
    cells     = _kpi_cell_list(site_id, time_range)

    # Get site lat/lng from telecom_sites
    meta = {}
    try:
        rows = _sql("SELECT site_id, site_name, site_abs_id, vendor_name, province, commune, zone, latitude, longitude FROM telecom_sites WHERE LOWER(site_id)=LOWER(:sid) LIMIT 1", {"sid": site_id})
        if rows:
            meta = rows[0]
    except Exception:
        pass

    # Build daily_trend rows from kd_trend
    all_dates = sorted(set(pt["date"] for pts in kd_trend.values() for pt in pts))
    def _gv(col, date):
        pts = kd_trend.get(col, [])
        for pt in pts:
            if pt["date"] == date:
                return pt["value"]
        return None

    daily_trend = []
    for d in all_dates:
        drop     = _gv("erab_drop_rate", d)
        call_sr  = _gv("lte_call_setup_sr", d)
        fail_rate= _f(100 - float(call_sr or 100), 2) if call_sr is not None else 0
        daily_trend.append({
            "date":             d,
            "call_drop_rate":   _f(drop, 2) if drop is not None else 0,
            "call_failure_rate":fail_rate,
            "dl_throughput":    _f(_gv("dl_cell_tput", d)),
            "rrc_users":        _f(_gv("avg_rrc_ue", d), 0),
            "dl_prb_util":      _f(_gv("dl_prb_util", d)),
            "dl_traffic_vol":   _f(_gv("dl_data_vol", d)),
            "rrc_setup_sr":     _f(_gv("lte_rrc_setup_sr", d)),
            "erab_setup_sr":    _f(_gv("erab_setup_sr", d)),
            "availability":     _f(_gv("availability", d)),
        })

    # Summary averages from daily trend
    def _davg(key):
        vals = [float(r[key]) for r in daily_trend if r.get(key)]
        return sum(vals)/len(vals) if vals else 0

    summary = {
        "call_drop_rate":    _f(_davg("call_drop_rate"), 2),
        "call_failure_rate": _f(_davg("call_failure_rate"), 2),
        "dl_throughput":     _f(_davg("dl_throughput")),
        "rrc_users":         _f(_davg("rrc_users"), 0),
        "dl_prb_util":       _f(_davg("dl_prb_util")),
        "dl_traffic_vol":    _f(_davg("dl_traffic_vol")),
        "rrc_setup_sr":      _f(_davg("rrc_setup_sr")),
        "erab_setup_sr":     _f(_davg("erab_setup_sr")),
        "availability":      _f(_davg("availability")),
        "data_points":       len(daily_trend),
    }

    # ── Fallback: network_kpi_timeseries ──────────────────────────────────────
    if not daily_trend:
        try:
            cutoff = _smart_cutoff(time_range)
            params = {"sid": site_id, "cutoff": cutoff}
            agg_rows = _sql("""
                SELECT AVG(COALESCE(erab_drop_rate,call_drop_rate,0)) AS call_drop_rate,
                       100-AVG(COALESCE(lte_call_setup_sr,100))       AS call_failure_rate,
                       AVG(COALESCE(dl_cell_tput,throughput_dl,0))    AS dl_throughput,
                       AVG(COALESCE(avg_rrc_ue,active_users,0))       AS rrc_users,
                       AVG(COALESCE(dl_prb_util,prb_utilization,0))   AS dl_prb_util,
                       AVG(COALESCE(dl_data_vol,traffic_volume,0))    AS dl_traffic_vol,
                       AVG(COALESCE(lte_rrc_setup_sr,0))              AS rrc_setup_sr,
                       AVG(availability)                               AS availability,
                       COUNT(DISTINCT timestamp)                       AS data_points
                FROM network_kpi_timeseries
                WHERE LOWER(site_id)=LOWER(:sid) AND timestamp>=:cutoff
            """, params)
            if agg_rows and agg_rows[0].get("data_points"):
                summary = {k: _f(v, 2) if isinstance(v, float) else (int(v) if v is not None else 0) for k, v in agg_rows[0].items()}
            ts_trend = _sql("""
                SELECT DATE_TRUNC('day',timestamp)::date::text AS date,
                       AVG(COALESCE(erab_drop_rate,call_drop_rate,0)) AS call_drop_rate,
                       100-AVG(COALESCE(lte_call_setup_sr,100))       AS call_failure_rate,
                       AVG(COALESCE(dl_cell_tput,throughput_dl,0))    AS dl_throughput,
                       AVG(COALESCE(avg_rrc_ue,active_users,0))       AS rrc_users,
                       AVG(COALESCE(dl_prb_util,prb_utilization,0))   AS dl_prb_util,
                       AVG(COALESCE(dl_data_vol,traffic_volume,0))    AS dl_traffic_vol,
                       AVG(COALESCE(lte_rrc_setup_sr,0))              AS rrc_setup_sr,
                       AVG(availability)                               AS availability
                FROM network_kpi_timeseries
                WHERE LOWER(site_id)=LOWER(:sid) AND timestamp>=:cutoff
                GROUP BY 1 ORDER BY 1
            """, params)
            daily_trend = [{k: (_f(v,2) if isinstance(v,float) else str(v)[:10] if k=="date" else v) for k,v in r.items()} for r in ts_trend]
            if not meta:
                ts_meta = _sql("SELECT site_id, region AS zone, AVG(latitude) AS latitude, AVG(longitude) AS longitude FROM network_kpi_timeseries WHERE LOWER(site_id)=LOWER(:sid) GROUP BY site_id, region LIMIT 1", {"sid": site_id})
                if ts_meta: meta = ts_meta[0]
        except Exception:
            pass

    result = {
        "site_id":    site_id,
        "meta":       meta or {"site_id": site_id},
        "summary":    summary,
        "daily_trend":daily_trend,
        "hourly_trend":[],
        "cells":      cells,
    }
    _to_cache(ck, result)
    return jsonify(result)


@network_bp.route("/api/network/site-core-detail", methods=["GET"])
@jwt_required()
def site_core_detail():
    """
    Per-site core KPI trends from flexible_kpi_uploads.
    kpi_name = sheet name (e.g. 'Auth SR', 'CPU Utilization').
    column_name = date string (e.g. '2026_02_20_000000').
    Falls back to core_kpi_data (legacy) if no flex data found.
    """
    site_id = request.args.get("site_id", "").strip()
    time_range = request.args.get("time_range", "30d").strip()
    if not site_id:
        return jsonify({"error": "site_id required"}), 400

    ck = _cache_key("site_core_v4", {"site_id": site_id, "tr": time_range})
    if request.args.get("fresh") != "1":
        cached = _from_cache(ck)
        if cached:
            return jsonify(cached)

    def _kpi_field(name):
        n = (name or "").lower()
        if "auth"   in n: return "auth_sr"
        if "cpu"    in n: return "cpu_util"
        if "attach" in n: return "attach_sr"
        if "pdp" in n or "bearer" in n: return "pdp_sr"
        return None

    _days_map = {"1h": 1, "6h": 1, "24h": 1, "7d": 7, "30d": 30, "all": 9999}
    _max_days = _days_map.get(time_range, 30)

    trend = []
    # ── flexible_kpi_uploads (primary) ────────────────────────────────────────
    try:
        # Detect format: row_date (new) or column_name dates (old)
        _has_rd = bool(_sql("SELECT 1 FROM flexible_kpi_uploads WHERE kpi_type='core' AND row_date IS NOT NULL LIMIT 1"))

        if _has_rd:
            # NEW FORMAT: kpi_name in column_name, dates in row_date
            _date_filter = f"AND row_date >= (SELECT MAX(row_date) FROM flexible_kpi_uploads WHERE kpi_type='core' AND row_date IS NOT NULL) - INTERVAL '{_max_days} days'" if _max_days < 9999 else ""
            rows = _sql(f"""
                SELECT kpi_name, row_date::text AS dt, AVG(num_value) AS val
                FROM flexible_kpi_uploads
                WHERE kpi_type = 'core'
                  AND LOWER(site_id) = LOWER(:sid)
                  AND column_type = 'numeric'
                  AND num_value IS NOT NULL
                  AND row_date IS NOT NULL {_date_filter}
                GROUP BY kpi_name, row_date
                ORDER BY row_date
            """, {"sid": site_id})
        else:
            # OLD FORMAT: dates as column_name
            rows = _sql("""
                SELECT kpi_name, column_name AS dt, AVG(num_value) AS val
                FROM flexible_kpi_uploads
                WHERE kpi_type = 'core'
                  AND LOWER(site_id) = LOWER(:sid)
                  AND column_type = 'numeric'
                  AND num_value IS NOT NULL
                GROUP BY kpi_name, column_name
                ORDER BY column_name
            """, {"sid": site_id})

        # Pivot: {date_str → {field → value}}
        date_map: dict = {}
        for r in rows:
            dt = r["dt"] or ""
            date_str = dt[:10].replace("_", "-") if len(dt) >= 10 else dt
            field = _kpi_field(r["kpi_name"])
            if not field or not date_str:
                continue
            date_map.setdefault(date_str, {})[field] = _f(r["val"], 2)

        # Apply time_range filter (for old format where SQL filter wasn't applied)
        if not _has_rd:
            _cutoff = (datetime.utcnow() - timedelta(days=_max_days)).strftime("%Y-%m-%d") if _max_days < 9999 else "2000-01-01"
            trend = [{"date": d, **date_map[d]} for d in sorted(date_map) if d >= _cutoff]
        else:
            trend = [{"date": d, **date_map[d]} for d in sorted(date_map)]
    except Exception as e:
        _LOG.error("site_core_detail flex: %s", e)

    # ── Fallback: core_kpi_data (legacy) ──────────────────────────────────────
    if not trend:
        try:
            rows2 = _sql(f"""
                SELECT date::text AS date, auth_sr, cpu_util, attach_sr, pdp_sr
                FROM core_kpi_data
                WHERE LOWER(site_id) = LOWER(:sid)
                  AND date >= CURRENT_DATE - INTERVAL '{_max_days} days'
                  AND date <= CURRENT_DATE
                ORDER BY date
            """, {"sid": site_id})
            trend = [
                {"date": r["date"],
                 "auth_sr":   _f(r.get("auth_sr")),
                 "cpu_util":  _f(r.get("cpu_util")),
                 "attach_sr": _f(r.get("attach_sr")),
                 "pdp_sr":    _f(r.get("pdp_sr"))}
                for r in rows2
            ]
        except Exception as e:
            _LOG.error("site_core_detail legacy: %s", e)

    if not trend:
        return jsonify({"site_id": site_id, "summary": {}, "trend": [], "meta": {},
                        "auth_trend": [], "cpu_trend": [], "attach_trend": [], "pdp_trend": []})

    # Get site lat/lng from telecom_sites
    core_meta = {}
    try:
        mrows = _sql("SELECT site_id, zone, latitude, longitude FROM telecom_sites WHERE LOWER(site_id)=LOWER(:sid) LIMIT 1", {"sid": site_id})
        if mrows:
            core_meta = mrows[0]
    except Exception:
        pass

    def _avg(field):
        vals = [float(t[field]) for t in trend if t.get(field) is not None]
        return _f(sum(vals) / len(vals), 2) if vals else 0

    summary = {
        "auth_sr":   _avg("auth_sr"),
        "cpu_util":  _avg("cpu_util"),
        "attach_sr": _avg("attach_sr"),
        "pdp_sr":    _avg("pdp_sr"),
    }

    result = {
        "site_id": site_id,
        "meta":    core_meta or {"site_id": site_id},
        "summary": summary,
        "trend":   trend,
        # Per-KPI trend arrays for separate chart rendering
        "auth_trend":   [{"date": t["date"], "auth_sr":   t["auth_sr"]}   for t in trend if t.get("auth_sr")   is not None],
        "cpu_trend":    [{"date": t["date"], "cpu_util":  t["cpu_util"]}  for t in trend if t.get("cpu_util")  is not None],
        "attach_trend": [{"date": t["date"], "attach_sr": t["attach_sr"]} for t in trend if t.get("attach_sr") is not None],
        "pdp_trend":    [{"date": t["date"], "pdp_sr":    t["pdp_sr"]}    for t in trend if t.get("pdp_sr")    is not None],
    }
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/site-transport-detail?site_id=<id>
# Transport KPI detail for a single site
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/network/site-transport-detail", methods=["GET"])
@jwt_required()
def site_transport_detail():
    site_id = request.args.get("site_id", "").strip()
    time_range = request.args.get("time_range", "30d").strip()
    if not site_id:
        return jsonify({"error": "site_id required"}), 400

    ck = _cache_key("site_transport_v2", {"site_id": site_id, "tr": time_range})
    cached = _from_cache(ck)
    if cached:
        return jsonify(cached)

    try:
        rows = _sql("""
            SELECT site_id, zone, backhaul_type, link_capacity,
                   avg_util, peak_util, packet_loss, avg_latency,
                   jitter, availability, error_rate, tput_efficiency, alarms,
                   uploaded_at::text AS upload_date
            FROM transport_kpi_data
            WHERE LOWER(site_id) = LOWER(:site_id)
            ORDER BY uploaded_at DESC LIMIT 1
        """, {"site_id": site_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not rows:
        return jsonify({"site_id": site_id, "data": None, "message": "No transport data for this site"})

    # Get site lat/lng from telecom_sites. Transport uploads may key by
    # either site_id, site_name, or site_abs_id — try every combination.
    tr_meta = {}
    try:
        mrows = _sql("""
            SELECT site_id, zone, latitude, longitude, city, state
            FROM telecom_sites
            WHERE LOWER(site_id)   = LOWER(:sid)
               OR LOWER(site_name) = LOWER(:sid)
               OR LOWER(COALESCE(site_abs_id,'')) = LOWER(:sid)
            LIMIT 1
        """, {"sid": site_id})
        if mrows:
            tr_meta = mrows[0]
    except Exception:
        pass
    # If still no match, use the transport row's site_abs_id (if the
    # transport_kpi_data table carries one) to locate the telecom_sites row.
    if not tr_meta or not tr_meta.get("latitude"):
        try:
            abs_row = _sql("SELECT site_abs_id FROM transport_kpi_data WHERE LOWER(site_id)=LOWER(:sid) LIMIT 1",
                           {"sid": site_id})
            abs_id = abs_row[0].get("site_abs_id") if abs_row else None
            if abs_id:
                mrows = _sql("""
                    SELECT site_id, zone, latitude, longitude, city, state
                    FROM telecom_sites
                    WHERE LOWER(COALESCE(site_abs_id,'')) = LOWER(:aid)
                       OR LOWER(site_id) = LOWER(:aid)
                    LIMIT 1
                """, {"aid": str(abs_id)})
                if mrows:
                    tr_meta = mrows[0]
        except Exception:
            pass

    r = rows[0]
    result = {
        "site_id":       site_id,
        "meta":          tr_meta or {"site_id": site_id},
        "zone":          r.get("zone"),
        "backhaul_type": r.get("backhaul_type"),
        "link_capacity": _f(r.get("link_capacity")),
        "avg_util":      _f(r.get("avg_util")),
        "peak_util":     _f(r.get("peak_util")),
        "packet_loss":   _f(r.get("packet_loss"), 3),
        "avg_latency":   _f(r.get("avg_latency")),
        "jitter":        _f(r.get("jitter"), 2),
        "availability":  _f(r.get("availability")),
        "error_rate":    _f(r.get("error_rate"), 4),
        "tput_efficiency": _f(r.get("tput_efficiency")),
        "alarms":        int(r.get("alarms") or 0),
        "upload_date":   r.get("upload_date"),
    }
    _to_cache(ck, result)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Overview Page Helpers — pure Python data-retrieval logic.
#
# SQL is used only for the cheap heavy-lift (WHERE filters + GROUP BY on
# indexed columns, which Postgres does in milliseconds). Every business
# decision — bucketing KPI names into metrics, computing means, ranking
# best/worst, classifying violations, shaping zone aggregates — happens
# in pure Python so it's easy to read, easy to change, and easy to debug
# (each step logs to backend.log).
#
# This file is concatenated into network_analytics.py at build time; do not
# import it directly. Helpers all live under the _ovw_* prefix.
# ─────────────────────────────────────────────────────────────────────────────

# Substring patterns that classify a kpi_name into a metric bucket. Match
# semantics: case-insensitive, ALL words in any one inner list must appear.
# Uploads with custom KPI names just need one matching pattern here to feed
# the dashboard — no schema change required.

_OVW_METRIC_PATTERNS = {
    "dl_prb":    [["dl", "prb", "util"]],
    "ul_prb":    [["ul", "prb", "util"]],
    "usr_tput":  [["dl", "usr", "ave", "throughput"],
                  ["dl", "user", "ave", "throughput"]],
    "cell_tput": [["dl", "cell", "ave", "throughput"],
                  ["dl", "cell", "tput"]],
    "drop":      [["e-rab", "call", "drop"],
                  ["call", "drop", "rate"],
                  ["drop", "rate"]],
    "rrc":       [["rrc", "connected"], ["ave", "rrc"], ["rrc", "ue"]],
    "avail":     [["availability"], ["avail"]],
    "dl_vol":    [["dl", "data", "total", "volume"],
                  ["dl", "data", "volume"], ["dl", "volume"]],
    "cssr":      [["call", "setup", "success"], ["cssr"],
                  ["setup", "success", "rate"]],
}


def _ovw_classify_kpi(kpi_name):
    """Return the metric bucket (e.g. 'dl_prb') for a kpi_name, or None."""
    n = (kpi_name or "").lower()
    if not n:
        return None
    for metric, patterns in _OVW_METRIC_PATTERNS.items():
        for words in patterns:
            if all(w in n for w in words):
                return metric
    return None


def _ovw_mean(values):
    """Arithmetic mean of an iterable, ignoring None. Returns None on empty."""
    xs = [float(v) for v in values if v is not None]
    return sum(xs) / len(xs) if xs else None


def _ovw_site_counts(filters):
    """Total sites + total cells respecting the geo filter.
    Tries telecom_sites first; falls back to distinct site_id from kpi_data
    when the master list is empty. Returns (n_sites, n_cells)."""
    n_sites = n_cells = 0
    parts, params = [], {}
    zone   = filters.get("cluster") or filters.get("zone") or ""
    city_f = filters.get("city") or ""
    state  = filters.get("state") or ""
    if zone:
        items = [v.strip() for v in zone.split(",") if v.strip()]
        if len(items) == 1:
            parts.append("LOWER(zone) = LOWER(:_tz)"); params["_tz"] = items[0]
        else:
            for i, v in enumerate(items): params[f"_tz{i}"] = v
            phs = ",".join(f"LOWER(:_tz{i})" for i in range(len(items)))
            parts.append(f"LOWER(zone) IN ({phs})")
    if city_f:
        items = [v.strip() for v in city_f.split(",") if v.strip()]
        if len(items) == 1:
            parts.append("LOWER(city) = LOWER(:_tc)"); params["_tc"] = items[0]
        else:
            for i, v in enumerate(items): params[f"_tc{i}"] = v
            phs = ",".join(f"LOWER(:_tc{i})" for i in range(len(items)))
            parts.append(f"LOWER(city) IN ({phs})")
    if state:
        parts.append("LOWER(state) = LOWER(:_tst)"); params["_tst"] = state
    where = (" AND " + " AND ".join(parts)) if parts else ""
    cell_col = _telecom_sites_cell_col()
    cell_expr = "NULL" if not cell_col else f"NULLIF(COALESCE({cell_col}, ''), '')"
    try:
        rows = _sql(f"""
            SELECT
                COUNT(DISTINCT COALESCE(NULLIF(site_id,''), NULLIF(site_abs_id,''))) AS s,
                COUNT(DISTINCT {cell_expr}) AS c_by_cell,
                COUNT(*) AS c_total
            FROM telecom_sites WHERE 1=1 {where}
        """, params)
        if rows:
            r = rows[0]
            n_sites = int(r.get("s") or 0)
            n_cells = int(r.get("c_by_cell") or 0) or int(r.get("c_total") or 0)
    except Exception as e:
        _LOG.error("_ovw_site_counts: %s", e)
    if (not n_sites or not n_cells) and not parts:
        cached = _kpi_data_counts_cached()
        if not n_sites: n_sites = cached.get("sites", 0)
        if not n_cells: n_cells = cached.get("cells", 0)
    return n_sites, n_cells


def _ovw_per_site_kpis(filters, anchor_date):
    """Per-site averages for the 30-day window.
    Step 1 (SQL): pull pre-aggregated (site, kpi_name) rows from kpi_data.
    Step 2 (Python): classify each kpi_name into a metric, store under site.
    Returns: dict[site_id] -> dict[metric] -> float."""
    fw_geo, fp, needs_ts = _kpi_filter_clause(
        {**(filters or {}), "time_range": "all"}, "k", "ts")
    fp = dict(fp)
    fp["_o_start"] = anchor_date - timedelta(days=30)
    fp["_o_end"]   = anchor_date
    join = "JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)" if needs_ts else ""
    rows = _sql(f"""
        SELECT k.site_id, k.kpi_name, AVG(k.value) AS v
        FROM kpi_data k {join}
        WHERE k.value IS NOT NULL
          AND k.date >= :_o_start AND k.date <= :_o_end
          {fw_geo}
        GROUP BY k.site_id, k.kpi_name
    """, fp, timeout_ms=10000)
    _LOG.info("_ovw_per_site_kpis step1: %d rows in window %s→%s",
              len(rows), fp["_o_start"], fp["_o_end"])
    if not rows:
        fp_nd = {k: v for k, v in fp.items() if k not in ("_o_start", "_o_end")}
        rows = _sql(f"""
            SELECT k.site_id, k.kpi_name, AVG(k.value) AS v
            FROM kpi_data k {join}
            WHERE k.value IS NOT NULL {fw_geo}
            GROUP BY k.site_id, k.kpi_name
        """, fp_nd)
        _LOG.info("_ovw_per_site_kpis step1-retry (no date): %d rows", len(rows))

    per_site = {}
    unmatched = set()
    for r in rows:
        sid = r.get("site_id")
        if not sid: continue
        metric = _ovw_classify_kpi(r.get("kpi_name"))
        if not metric:
            if len(unmatched) < 30:
                unmatched.add(r.get("kpi_name") or "")
            continue
        try: v = float(r.get("v"))
        except (TypeError, ValueError): continue
        per_site.setdefault(sid, {})[metric] = v
    _LOG.info("_ovw_per_site_kpis step2: %d sites bucketed; %d unmatched kpi_names",
              len(per_site), len(unmatched))
    if unmatched:
        _LOG.info("_ovw_per_site_kpis sample unmatched: %s",
                  sorted(unmatched)[:10])
    return per_site


def _ovw_zone_perf(per_site_kpis):
    """Group per-site KPIs by zone (read from telecom_sites). Pure Python."""
    if not per_site_kpis:
        return []
    site_ids = list(per_site_kpis.keys())
    site_zone = {}
    try:
        CHUNK = 5000
        for i in range(0, len(site_ids), CHUNK):
            chunk = site_ids[i:i+CHUNK]
            params = {f"sz_{j}": s for j, s in enumerate(chunk)}
            phs = ",".join(f":sz_{j}" for j in range(len(chunk)))
            rows = _sql(f"""
                SELECT DISTINCT ON (LOWER(site_id)) LOWER(site_id) AS sid,
                       COALESCE(NULLIF(zone,''), NULLIF(province,''),
                                NULLIF(city,''), 'Unzoned') AS zone
                FROM telecom_sites
                WHERE LOWER(site_id) IN ({phs})
            """, params)
            for r in rows:
                site_zone[r["sid"]] = r["zone"]
    except Exception as e:
        _LOG.error("_ovw_zone_perf zone lookup: %s", e)

    buckets = {}
    for sid, d in per_site_kpis.items():
        zname = site_zone.get((sid or "").lower(), "Unzoned")
        b = buckets.setdefault(zname, {"sites": 0, "dl_prb": [], "ul_prb": [],
                                       "tput": [], "drop": []})
        b["sites"] += 1
        if d.get("dl_prb")  is not None: b["dl_prb"].append(float(d["dl_prb"]))
        if d.get("ul_prb")  is not None: b["ul_prb"].append(float(d["ul_prb"]))
        t = d.get("usr_tput") if d.get("usr_tput") is not None else d.get("cell_tput")
        if t is not None:                b["tput"].append(float(t))
        if d.get("drop")    is not None: b["drop"].append(float(d["drop"]))

    out = []
    for zname, b in buckets.items():
        z_dl = _ovw_mean(b["dl_prb"]); z_ul = _ovw_mean(b["ul_prb"])
        z_t  = _ovw_mean(b["tput"]);   z_d  = _ovw_mean(b["drop"])
        if z_dl is not None and z_ul is not None: z_combined = (z_dl + z_ul) / 2.0
        else: z_combined = z_dl if z_dl is not None else z_ul
        out.append({
            "zone": zname, "province": zname, "sites": b["sites"],
            "avg_prb":    _f(z_combined, 1) if z_combined is not None else None,
            "avg_dl_prb": _f(z_dl, 1)       if z_dl       is not None else None,
            "avg_ul_prb": _f(z_ul, 1)       if z_ul       is not None else None,
            "avg_tput":   _f(z_t, 1)        if z_t        is not None else None,
            "avg_drop":   _f(z_d, 2)        if z_d        is not None else None,
        })
    out.sort(key=lambda x: x.get("avg_dl_prb") or -1, reverse=True)
    return out


def _ovw_violations_query(filters, anchor_date, group_by_cell=False):
    """Internal: pull 7-day per-site (or per-cell) KPI averages for violation logic."""
    fw_geo, fp, needs_ts = _kpi_filter_clause(
        {**(filters or {}), "time_range": "all"}, "k", "ts")
    fp = dict(fp)
    fp["_w_start"] = anchor_date - timedelta(days=7)
    fp["_w_end"]   = anchor_date
    join = "JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)" if needs_ts else ""
    if group_by_cell:
        return _sql(f"""
            SELECT k.site_id, k.cell_id, k.kpi_name, AVG(k.value) AS v
            FROM kpi_data k {join}
            WHERE k.value IS NOT NULL
              AND k.cell_id IS NOT NULL AND k.cell_id <> ''
              AND k.date >= :_w_start AND k.date <= :_w_end
              {fw_geo}
            GROUP BY k.site_id, k.cell_id, k.kpi_name
        """, fp)
    return _sql(f"""
        SELECT k.site_id, k.kpi_name, AVG(k.value) AS v
        FROM kpi_data k {join}
        WHERE k.value IS NOT NULL
          AND k.date >= :_w_start AND k.date <= :_w_end
          {fw_geo}
        GROUP BY k.site_id, k.kpi_name
    """, fp)


def _ovw_count_violations(metrics):
    """Count how many of {drop, cssr, tput} breach SLA thresholds."""
    drop = metrics.get("drop")
    cssr = metrics.get("cssr")
    tput = metrics.get("usr_tput") if metrics.get("usr_tput") is not None else metrics.get("cell_tput")
    return drop, cssr, tput, sum([
        1 if drop is not None and drop > 1.5 else 0,
        1 if cssr is not None and cssr < 98.5 else 0,
        1 if tput is not None and tput < 8 else 0,
    ])


def _ovw_lookup_site_info(site_ids):
    """Fetch zone/lat/lng for a list of site_ids. Returns dict keyed by site_id."""
    if not site_ids:
        return {}
    info = {}
    try:
        ids_ph = ",".join(f":si_{i}" for i in range(len(site_ids)))
        params = {f"si_{i}": s for i, s in enumerate(site_ids)}
        for r in _sql(f"""
            SELECT site_id, zone, latitude, longitude
            FROM telecom_sites WHERE site_id IN ({ids_ph})
        """, params):
            info[r["site_id"]] = r
    except Exception:
        pass
    return info


def _ovw_worst_sites(filters, anchor_date):
    """Top 10 worst sites by violation count (drop>1.5, cssr<98.5, tput<8)."""
    try:
        rows = _ovw_violations_query(filters, anchor_date, group_by_cell=False)
    except Exception as e:
        _LOG.error("_ovw_worst_sites query: %s", e)
        return []
    per_site = {}
    for r in rows:
        sid = r.get("site_id")
        metric = _ovw_classify_kpi(r.get("kpi_name"))
        if not sid or metric not in ("drop", "cssr", "usr_tput", "cell_tput"):
            continue
        try: v = float(r.get("v"))
        except (TypeError, ValueError): continue
        per_site.setdefault(sid, {})[metric] = v
    scored = []
    for sid, m in per_site.items():
        drop, cssr, tput, vc = _ovw_count_violations(m)
        if vc == 0: continue
        scored.append((sid, vc, drop, cssr, tput))
    scored.sort(key=lambda x: (-x[1], -(x[2] or 0)))
    top = scored[:10]
    info = _ovw_lookup_site_info([s for (s, *_x) in top])
    return [{
        "site_id": sid,
        "cluster": (info.get(sid) or {}).get("zone") or "",
        "call_drop_rate": _f(drop, 2) if drop is not None else None,
        "lte_cssr":       _f(cssr, 2) if cssr is not None else None,
        "dl_usr_tput":    _f(tput, 2) if tput is not None else None,
        "violations":     vc,
        "lat": (info.get(sid) or {}).get("latitude"),
        "lng": (info.get(sid) or {}).get("longitude"),
    } for (sid, vc, drop, cssr, tput) in top]


def _ovw_worst_cells(filters, anchor_date):
    """Top 10 worst cells by violation count."""
    try:
        rows = _ovw_violations_query(filters, anchor_date, group_by_cell=True)
    except Exception as e:
        _LOG.error("_ovw_worst_cells query: %s", e)
        return []
    per_cell = {}
    for r in rows:
        sid = r.get("site_id"); cid = r.get("cell_id")
        metric = _ovw_classify_kpi(r.get("kpi_name"))
        if not sid or not cid or metric not in ("drop", "cssr", "usr_tput", "cell_tput"):
            continue
        try: v = float(r.get("v"))
        except (TypeError, ValueError): continue
        per_cell.setdefault((sid, cid), {})[metric] = v
    scored = []
    for (sid, cid), m in per_cell.items():
        drop, cssr, tput, vc = _ovw_count_violations(m)
        if vc == 0: continue
        scored.append((sid, cid, vc, drop, cssr, tput))
    scored.sort(key=lambda x: (-x[2], -(x[3] or 0)))
    top = scored[:10]
    info = _ovw_lookup_site_info(list({s for (s, _c, *_x) in top}))
    return [{
        "site_id": sid, "cell_id": cid,
        "cluster": (info.get(sid) or {}).get("zone") or "",
        "call_drop_rate": _f(drop, 2) if drop is not None else None,
        "lte_cssr":       _f(cssr, 2) if cssr is not None else None,
        "dl_usr_tput":    _f(tput, 2) if tput is not None else None,
        "violations":     vc,
        "lat": (info.get(sid) or {}).get("latitude"),
        "lng": (info.get(sid) or {}).get("longitude"),
    } for (sid, cid, vc, drop, cssr, tput) in top]


def _ovw_best_sites(per_site_kpis):
    """Top 10 sites by user throughput. Pure Python from per_site_kpis dict."""
    ranked = []
    for sid, d in per_site_kpis.items():
        tput = d.get("usr_tput") if d.get("usr_tput") is not None else d.get("cell_tput")
        if tput is None: continue
        ranked.append((sid, float(tput), d.get("dl_prb")))
    ranked.sort(key=lambda x: x[1], reverse=True)
    top = ranked[:10]
    info = _ovw_lookup_site_info([s for (s, _t, _p) in top])
    return [{
        "site_id": sid,
        "cluster": (info.get(sid) or {}).get("zone") or "",
        "dl_tput": _f(tput),
        "dl_prb_util": _f(prb) if prb is not None else None,
        "lat": (info.get(sid) or {}).get("latitude"),
        "lng": (info.get(sid) or {}).get("longitude"),
    } for (sid, tput, prb) in top]


def _ovw_tput_trend(filters, anchor_date):
    """DL throughput per date for the last 30 days."""
    fw_geo, fp, needs_ts = _kpi_filter_clause(
        {**(filters or {}), "time_range": "all"}, "k", "ts")
    fp = dict(fp)
    fp["_t_start"] = anchor_date - timedelta(days=30)
    fp["_t_end"]   = anchor_date
    join = "LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)" if needs_ts else ""
    try:
        rows = _sql(f"""
            SELECT k.date::text AS d, k.kpi_name, AVG(k.value) AS v
            FROM kpi_data k {join}
            WHERE k.value IS NOT NULL
              AND k.date >= :_t_start AND k.date <= :_t_end
              {fw_geo}
            GROUP BY k.date, k.kpi_name
        """, fp)
    except Exception as e:
        _LOG.error("_ovw_tput_trend: %s", e)
        return []
    by_date = {}
    for r in rows:
        d = r.get("d")
        if not d: continue
        metric = _ovw_classify_kpi(r.get("kpi_name"))
        if metric not in ("usr_tput", "cell_tput", "dl_prb"): continue
        try: v = float(r.get("v"))
        except (TypeError, ValueError): continue
        by_date.setdefault(d, {})[metric] = v
    trend = []
    for d in sorted(by_date.keys()):
        m = by_date[d]
        tput = m.get("usr_tput") if m.get("usr_tput") is not None else m.get("cell_tput")
        trend.append({
            "time": d,
            "avg_tput": _f(tput) if tput is not None else None,
            "avg_prb":  _f(m.get("dl_prb")) if m.get("dl_prb") is not None else None,
        })
    return trend


def _ovw_core_kpis(filters):
    """Auth / CPU / Attach / PDP averages, source-priority ordered."""
    out = {"avg_auth": 0, "avg_cpu": 0, "avg_attach": 0, "avg_pdp": 0}
    # Source 1: flexible_kpi_uploads
    try:
        rows = _sql("""
            SELECT kpi_name, column_name, AVG(num_value) AS v
            FROM flexible_kpi_uploads
            WHERE kpi_type='core' AND column_type='numeric' AND num_value IS NOT NULL
            GROUP BY kpi_name, column_name
        """)
        if rows:
            buckets = {"auth": [], "cpu": [], "attach": [], "pdp": []}
            for r in rows:
                combined = ((r.get("kpi_name") or "") + " " +
                            (r.get("column_name") or "")).lower()
                v = r.get("v")
                if v is None: continue
                v = float(v)
                if "auth" in combined: buckets["auth"].append(v)
                if "cpu" in combined:  buckets["cpu"].append(v)
                if "attach" in combined: buckets["attach"].append(v)
                if "pdp" in combined or "bearer" in combined: buckets["pdp"].append(v)
            for k in ("auth", "cpu", "attach", "pdp"):
                if buckets[k]:
                    out[f"avg_{k}"] = sum(buckets[k]) / len(buckets[k])
            if not any(buckets.values()):
                # Component-style upload — use overall average across all rows
                all_vals = [float(r["v"]) for r in rows if r.get("v") is not None]
                if all_vals:
                    avg = sum(all_vals) / len(all_vals)
                    for k in ("auth", "cpu", "attach", "pdp"):
                        out[f"avg_{k}"] = avg
            if any(out.values()):
                return out
    except Exception:
        pass
    # Source 2: legacy core_kpi_data
    try:
        r = _sql("""
            SELECT AVG(auth_sr) AS a, AVG(cpu_util) AS c,
                   AVG(attach_sr) AS at, AVG(pdp_sr) AS p
            FROM core_kpi_data
        """)
        if r:
            out["avg_auth"]   = float(r[0].get("a") or 0)
            out["avg_cpu"]    = float(r[0].get("c") or 0)
            out["avg_attach"] = float(r[0].get("at") or 0)
            out["avg_pdp"]    = float(r[0].get("p") or 0)
            if any(out.values()):
                return out
    except Exception:
        pass
    # Source 3: core_component_kpi
    try:
        rows = _sql("""
            SELECT kpi_name, AVG(value) AS v
            FROM core_component_kpi WHERE value IS NOT NULL
            GROUP BY kpi_name
        """)
        for r in rows:
            n = (r.get("kpi_name") or "").lower(); v = r.get("v")
            if v is None: continue
            v = float(v)
            if "auth"   in n and not out["avg_auth"]:   out["avg_auth"]   = v
            if "cpu"    in n and not out["avg_cpu"]:    out["avg_cpu"]    = v
            if "attach" in n and not out["avg_attach"]: out["avg_attach"] = v
            if ("pdp" in n or "bearer" in n) and not out["avg_pdp"]:
                out["avg_pdp"] = v
    except Exception:
        pass
    return out


def _ovw_transport_kpis(filters):
    """Link util / latency / packet loss / availability averages."""
    try:
        r = _sql("""
            SELECT AVG(avg_util) AS u, AVG(avg_latency) AS l,
                   AVG(packet_loss) AS p, AVG(availability) AS a
            FROM transport_kpi_data
        """)
        if r:
            return {
                "avg_link_util":   float(r[0].get("u") or 0),
                "avg_tr_latency":  float(r[0].get("l") or 0),
                "avg_tr_pkt_loss": float(r[0].get("p") or 0),
                "avg_tr_avail":    float(r[0].get("a") or 0),
            }
    except Exception:
        pass
    return {"avg_link_util": 0, "avg_tr_latency": 0,
            "avg_tr_pkt_loss": 0, "avg_tr_avail": 0}


def _ovw_revenue(filters):
    """Total revenue, total OPEX, and bottom 10 sites by margin.
    All bucketing done in Python — no regex CASE WHEN."""
    try:
        rows = _sql("""
            SELECT site_id, column_name, num_value
            FROM flexible_kpi_uploads
            WHERE kpi_type='revenue' AND column_type='numeric' AND num_value IS NOT NULL
        """)
    except Exception:
        rows = []
    months = {"jan", "feb", "mar", "apr", "may", "jun",
              "jul", "aug", "sep", "oct", "nov", "dec"}
    per_site = {}
    for r in rows:
        sid = r.get("site_id")
        col = (r.get("column_name") or "").lower()
        if not sid or not col: continue
        try: val = float(r.get("num_value"))
        except (TypeError, ValueError): continue
        b = per_site.setdefault(sid, {
            "rev_total": None, "rev_monthly": 0.0,
            "opex_total": None, "opex_plain": None, "opex_monthly": 0.0,
        })
        is_opex  = "opex" in col
        is_total = "total" in col
        is_month = any(m in col for m in months)
        if is_opex:
            if is_total:                           b["opex_total"] = val
            elif col == "opex" or (not is_month):  b["opex_plain"] = val
            elif is_month:                         b["opex_monthly"] += val
        else:
            if is_total and "revenue" in col: b["rev_total"] = val
            elif is_month:                    b["rev_monthly"] += val

    site_totals = []
    for sid, b in per_site.items():
        rev = b["rev_total"] if b["rev_total"] is not None else b["rev_monthly"]
        opex = (b["opex_total"] if b["opex_total"] is not None else
                b["opex_plain"] if b["opex_plain"] is not None else
                b["opex_monthly"])
        site_totals.append({
            "site_id": sid, "revenue": rev or 0.0, "opex": opex or 0.0,
            "diff": (rev or 0.0) - (opex or 0.0),
        })
    total_rev  = sum(s["revenue"] for s in site_totals)
    total_opex = sum(s["opex"]    for s in site_totals)
    eligible = [s for s in site_totals if s["revenue"] > 0 and s["opex"] > 0]
    eligible.sort(key=lambda x: x["diff"])
    low_margin = [{
        "site_id":            s["site_id"],
        "zone":               "",
        "revenue":            _f(s["revenue"]),
        "opex":               _f(s["opex"]),
        "revenue_minus_opex": _f(s["diff"]),
        "q1_rev":             _f(s["revenue"]),
        "q1_opex":            _f(s["opex"]),
    } for s in eligible[:10]]
    return total_rev, total_opex, low_margin


def _ovw_health(avg_prb, avg_drop, avg_cssr, avg_usr_tput):
    """4-factor health score — missing metrics drop out, weights renormalise."""
    components = []
    if avg_prb is not None:
        components.append(max(0, min(1, 1 - max(float(avg_prb) - 30, 0) / 70.0)))
    if avg_drop is not None:
        components.append(max(0, min(1, 1 - (float(avg_drop) - 0.5) / 3.0)))
    if avg_cssr is not None:
        components.append(max(0, min(1, (float(avg_cssr) - 95) / 5.0)))
    if avg_usr_tput is not None:
        components.append(max(0, min(1, min(float(avg_usr_tput), 20) / 20.0)))
    if not components:
        return None, None
    score = round((sum(components) / len(components)) * 100, 1)
    if score >= 80:   label = "Good"
    elif score >= 60: label = "Fair"
    else:             label = "Poor"
    return score, label


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/overview-stats
# Each card on the overview page is computed by a Python helper above.
# This function is now just orchestration: call helpers, assemble response.
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/network/overview-stats", methods=["GET"])
@jwt_required()
def overview_stats():
    filters = _get_filters()
    fresh = request.args.get("fresh") == "1"
    from datetime import date as _date_type
    ck = _cache_key(f"overview_python_{_date_type.today().isoformat()}", filters)
    if not fresh:
        cached = _from_cache(ck)
        if cached:
            return jsonify(cached)

    anchor_date = _get_kpi_max_date() or datetime.utcnow().date()
    _LOG.info("overview-stats: start anchor_date=%s filters=%s", anchor_date,
              {k: v for k, v in filters.items() if v})

    # Each section is its own Python helper — if one fails the others still
    # render so the dashboard never blanks because of one bad table.
    try: n_sites, n_cells = _ovw_site_counts(filters)
    except Exception as e: _LOG.error("site_counts: %s", e); n_sites = n_cells = 0
    try: per_site_kpis = _ovw_per_site_kpis(filters, anchor_date)
    except Exception as e: _LOG.error("per_site_kpis: %s", e); per_site_kpis = {}

    # Network-wide means in plain Python from the per-site dict
    avg_dl_prb    = _ovw_mean(d.get("dl_prb")    for d in per_site_kpis.values())
    avg_ul_prb    = _ovw_mean(d.get("ul_prb")    for d in per_site_kpis.values())
    avg_usr_tput  = _ovw_mean(d.get("usr_tput")  for d in per_site_kpis.values())
    avg_cell_tput = _ovw_mean(d.get("cell_tput") for d in per_site_kpis.values())
    avg_drop      = _ovw_mean(d.get("drop")      for d in per_site_kpis.values())
    avg_rrc       = _ovw_mean(d.get("rrc")       for d in per_site_kpis.values())
    avg_avail     = _ovw_mean(d.get("avail")     for d in per_site_kpis.values())
    avg_dl_vol    = _ovw_mean(d.get("dl_vol")    for d in per_site_kpis.values())
    avg_cssr      = _ovw_mean(d.get("cssr")      for d in per_site_kpis.values())
    if avg_dl_prb is not None and avg_ul_prb is not None:
        avg_prb = (avg_dl_prb + avg_ul_prb) / 2.0
    else:
        avg_prb = avg_dl_prb if avg_dl_prb is not None else avg_ul_prb
    avg_dl_tput = avg_usr_tput if avg_usr_tput is not None else avg_cell_tput

    congested = sum(1 for d in per_site_kpis.values()
                    if d.get("dl_prb") is not None and float(d["dl_prb"]) > 85)
    health, h_label = _ovw_health(avg_prb, avg_drop, avg_cssr, avg_usr_tput)

    try: zone_perf = _ovw_zone_perf(per_site_kpis)
    except Exception as e: _LOG.error("zone_perf: %s", e); zone_perf = []
    try: worst_sites = _ovw_worst_sites(filters, anchor_date)
    except Exception as e: _LOG.error("worst_sites: %s", e); worst_sites = []
    try: worst_cells = _ovw_worst_cells(filters, anchor_date)
    except Exception as e: _LOG.error("worst_cells: %s", e); worst_cells = []
    try: best_sites = _ovw_best_sites(per_site_kpis)
    except Exception as e: _LOG.error("best_sites: %s", e); best_sites = []
    try: tput_trend = _ovw_tput_trend(filters, anchor_date)
    except Exception as e: _LOG.error("tput_trend: %s", e); tput_trend = []
    try: core = _ovw_core_kpis(filters)
    except Exception as e: _LOG.error("core: %s", e); core = {"avg_auth":0,"avg_cpu":0,"avg_attach":0,"avg_pdp":0}
    try: transport = _ovw_transport_kpis(filters)
    except Exception as e: _LOG.error("transport: %s", e); transport = {"avg_link_util":0,"avg_tr_latency":0,"avg_tr_pkt_loss":0,"avg_tr_avail":0}
    try: total_rev, total_opex, low_margin_sites = _ovw_revenue(filters)
    except Exception as e: _LOG.error("revenue: %s", e); total_rev = total_opex = 0; low_margin_sites = []

    # Synthesise a single-row zone perf when telecom_sites was empty so the
    # chart never goes blank — values are still DB-derived (network averages).
    if not zone_perf and any(v is not None for v in (avg_prb, avg_dl_tput, avg_drop)):
        zone_perf = [{
            "zone": "All Sites", "province": "All Sites",
            "sites": n_sites,
            "avg_prb":    _f(avg_prb, 1)    if avg_prb    is not None else None,
            "avg_dl_prb": _f(avg_dl_prb, 1) if avg_dl_prb is not None else None,
            "avg_ul_prb": _f(avg_ul_prb, 1) if avg_ul_prb is not None else None,
            "avg_tput":   _f(avg_dl_tput, 1) if avg_dl_tput is not None else None,
            "avg_drop":   _f(avg_drop, 2)   if avg_drop   is not None else None,
        }]

    result = {
        "network_health_score": health,
        "health_label":         h_label,
        "total_sites":          n_sites,
        "total_cells":          n_cells,
        "congested_sites":      congested,
        "avg_prb":              _f(avg_prb)        if avg_prb       is not None else None,
        "avg_dl_prb":           _f(avg_dl_prb)     if avg_dl_prb    is not None else None,
        "avg_ul_prb":           _f(avg_ul_prb)     if avg_ul_prb    is not None else None,
        "avg_dl_tput":          _f(avg_dl_tput)    if avg_dl_tput   is not None else None,
        "avg_drop_rate":        _f(avg_drop, 2)    if avg_drop      is not None else None,
        "avg_rrc_ue":           _f(avg_rrc, 0)     if avg_rrc       is not None else None,
        "avg_dl_vol":           _f(avg_dl_vol, 1)  if avg_dl_vol    is not None else None,
        "avg_sinr":             0,
        "avg_packet_loss":      _f(transport["avg_tr_pkt_loss"], 3),
        "avg_availability":     _f(avg_avail)      if avg_avail     is not None else None,
        "avg_auth_sr":          _f(core["avg_auth"]),
        "avg_cpu_util":         _f(core["avg_cpu"]),
        "avg_attach_sr":        _f(core["avg_attach"]),
        "avg_pdp_sr":           _f(core["avg_pdp"]),
        "avg_link_util":        _f(transport["avg_link_util"]),
        "avg_tr_latency":       _f(transport["avg_tr_latency"]),
        "avg_tr_pkt_loss":      _f(transport["avg_tr_pkt_loss"], 3),
        "avg_tr_avail":         _f(transport["avg_tr_avail"]),
        "total_revenue":        _f(total_rev),
        "total_opex":           _f(total_opex),
        "total_q1_revenue":     _f(total_rev),
        "total_q1_opex":        _f(total_opex),
        "worst_sites":          worst_sites,
        "worst_cells":          worst_cells,
        "best_sites":           best_sites,
        "low_margin_sites":     low_margin_sites,
        "zone_performance":     zone_perf,
        "tput_trend":           tput_trend,
    }
    _LOG.info("overview-stats: done sites=%d cells=%d congested=%d health=%s zones=%d worst=%d tput_trend=%d",
              n_sites, n_cells, congested, health, len(zone_perf),
              len(worst_sites), len(tput_trend))
    _to_cache(ck, result)
    return jsonify(result)


# Run index creation + cache warm-up once when the module loads.
# Both run in the background so server start isn't blocked. Warming the
# kpi_data max-date / counts / distinct-kpi-names caches at startup means
# the first dashboard hit doesn't pay for those (each takes seconds on a
# multi-million-row table).
import threading as _threading
def _bg_ensure_indexes():
    try:
        import time as _time
        _time.sleep(3)          # wait for app to finish starting
        _ensure_kpi_indexes()
        _LOG.info("kpi_data indexes ensured")
        # Warm the lightweight caches the overview endpoint reads first.
        try:
            md = _get_kpi_max_date()
            counts = _kpi_data_counts_cached()
            _LOG.info("Analytics cache warmed: max_date=%s sites=%s cells=%s",
                      md, counts.get("sites"), counts.get("cells"))
        except Exception as ce:
            _LOG.warning("Cache warm-up skipped: %s", ce)
    except Exception as e:
        _LOG.warning("Index creation skipped: %s", e)
_threading.Thread(target=_bg_ensure_indexes, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/network/debug  — check what's in each table (admin only)
# ─────────────────────────────────────────────────────────────────────────────
@network_bp.route("/api/network/debug", methods=["GET"])
@jwt_required()
def network_debug():
    """
    Comprehensive diagnostic. Visit this URL after deploying to confirm what's in DB.
    Returns: row counts, sample kpi_names, KPI matching status, live aggregation test.
    """
    result = {}

    # ── 1. Table row counts ────────────────────────────────────────────────────
    for tbl, sql in {
        "kpi_data":              "SELECT COUNT(*) AS cnt, COUNT(DISTINCT site_id) AS sites, COUNT(DISTINCT kpi_name) AS kpis, MIN(date)::text AS min_date, MAX(date)::text AS max_date FROM kpi_data",
        "telecom_sites":         "SELECT COUNT(*) AS cnt, COUNT(DISTINCT site_id) AS sites FROM telecom_sites",
        "transport_kpi_data":    "SELECT COUNT(*) AS cnt, COUNT(DISTINCT site_id) AS sites FROM transport_kpi_data",
        "core_kpi_data":         "SELECT COUNT(*) AS cnt FROM core_kpi_data",
        "revenue_data":          "SELECT COUNT(*) AS cnt FROM revenue_data",
        "network_kpi_timeseries":"SELECT COUNT(*) AS cnt FROM network_kpi_timeseries",
        "flexible_kpi_uploads":  "SELECT kpi_type, COUNT(*) AS cnt, COUNT(DISTINCT site_id) AS sites FROM flexible_kpi_uploads GROUP BY kpi_type",
    }.items():
        try:
            result[tbl] = _sql(sql)
        except Exception as e:
            result[tbl] = {"error": str(e)}

    # ── 2. All KPI names in kpi_data (crucial for diagnosing mismatches) ──────
    try:
        kpi_rows = _sql("SELECT DISTINCT kpi_name FROM kpi_data_merged ORDER BY kpi_name")
        all_names = [r["kpi_name"] for r in kpi_rows]
        matched   = {n: _kpi_col(n) for n in all_names if _kpi_col(n) is not None}
        unmatched = [n for n in all_names if _kpi_col(n) is None]
        result["kpi_matching"] = {
            "total_kpi_names_in_db": len(all_names),
            "matched_count":  len(matched),
            "unmatched_count": len(unmatched),
            "matched":   matched,
            "unmatched": unmatched,
            "verdict": "✅ All KPI names matched" if not unmatched else
                       f"⚠️ {len(unmatched)} names NOT in KPI_COL_MAP — these KPIs show 0",
        }
    except Exception as e:
        result["kpi_matching"] = {"error": str(e)}

    # ── 3. Live network agg test ───────────────────────────────────────────────
    try:
        agg = _kpi_network_agg({"time_range": "all"})
        result["live_agg"] = {k: v for k, v in agg.items() if v and v != 0}
        result["live_agg"]["_total_sites"] = agg.get("total_sites")
    except Exception as e:
        result["live_agg"] = {"error": str(e)}

    # ── 4. Sample site list (first 3 sites) ───────────────────────────────────
    try:
        sites = _kpi_site_list({"time_range": "all"})
        result["sample_sites"] = sites[:3]
        result["total_sites_in_site_list"] = len(sites)
    except Exception as e:
        result["sample_sites"] = {"error": str(e)}

    # ── 5. Date range check ───────────────────────────────────────────────────
    try:
        mn, mx = _kpi_date_range({})
        result["date_range"] = {"min": str(mn), "max": str(mx)}
    except Exception as e:
        result["date_range"] = {"error": str(e)}

    # ── 6. Core/flex columns ──────────────────────────────────────────────────
    try:
        core_cols = _sql("SELECT DISTINCT column_name FROM flexible_kpi_uploads WHERE kpi_type='core' ORDER BY column_name")
        result["core_flex_columns"] = [r["column_name"] for r in core_cols]
        result["core_upload_needed"] = len(result["core_flex_columns"]) == 0
    except Exception as e:
        result["core_flex_columns"] = {"error": str(e)}

    # ── 7. telecom_sites zones ────────────────────────────────────────────────
    try:
        zones = _sql("SELECT DISTINCT zone FROM telecom_sites WHERE zone IS NOT NULL ORDER BY zone LIMIT 20")
        result["telecom_zones"] = [r["zone"] for r in zones]
    except Exception as e:
        result["telecom_zones"] = {"error": str(e)}

    return jsonify(result)
