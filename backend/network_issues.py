"""
Network Issues (Worst Cell Offenders) — Auto-ticketing module V2.

Daily at 08:00 IST: detects worst cells, creates site-level tickets,
assigns to online agents, provides cell/site level AI diagnosis.
If server wasn't running at 08:00, creates on first startup.
"""

import logging
import os
from datetime import datetime, timedelta, date as _date
from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text as sa_text
from models import db, User

_LOG = logging.getLogger("network_issues")
_LOG.setLevel(logging.INFO)
if not _LOG.handlers:
    _LOG.addHandler(logging.StreamHandler())

network_issues_bp = Blueprint("network_issues", __name__)

# Track if today's job has run (reset daily)
_LAST_JOB_DATE = None

# Pre-scan cache: stores latest worst-cell scan results for dashboard display
_LATEST_WORST_CELLS = {}
_LATEST_SCAN_TIME = None
# Pre-scan cache for overutilized sites (populated at 07:30 IST daily)
_LATEST_OVERUTILIZED = {}
_LATEST_OVERUTIL_SCAN_TIME = None
# Today's routing log — list of {site_id/group, action, agent_name, priority, timestamp}
_TODAYS_ROUTING_LOG = []


# ─────────────────────────────────────────────────────────────────────────────
# Model
# ─────────────────────────────────────────────────────────────────────────────
class NetworkIssueTicket(db.Model):
    __tablename__ = "network_issue_tickets"

    id              = db.Column(db.Integer, primary_key=True)
    site_id         = db.Column(db.String(100), nullable=False, index=True)
    cells_affected  = db.Column(db.Text, default="")          # comma-separated cell_ids
    cell_site_ids   = db.Column(db.Text, default="")          # comma-separated cell_site_ids
    category        = db.Column(db.String(30), default="Worst")
    priority        = db.Column(db.String(20), default="Low")
    priority_score  = db.Column(db.Float, default=0)
    sla_hours       = db.Column(db.Float, default=16)
    avg_rrc         = db.Column(db.Float, default=0)
    max_rrc         = db.Column(db.Float, default=0)
    revenue_total   = db.Column(db.Float, default=0)
    avg_drop_rate   = db.Column(db.Float, default=0)
    avg_cssr        = db.Column(db.Float, default=0)
    avg_tput        = db.Column(db.Float, default=0)
    violations      = db.Column(db.Integer, default=0)
    status          = db.Column(db.String(30), default="open")
    assigned_agent  = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    root_cause      = db.Column(db.Text, default="")
    recommendation  = db.Column(db.Text, default="")
    zone            = db.Column(db.String(100), default="")
    location        = db.Column(db.String(200), default="")   # city/state of the site
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at      = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deadline_time   = db.Column(db.DateTime, nullable=True)


# ─────────────────────────────────────────────────────────────────────────────
# SQL helper
# ─────────────────────────────────────────────────────────────────────────────
def _sql(query, params=None):
    with db.engine.connect() as conn:
        result = conn.execute(sa_text(query), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def _f(v, d=1):
    try: return round(float(v), d) if v is not None else 0
    except: return 0


def _clean_ai(text):
    """Strip all markdown from AI response."""
    import re
    if not text: return text
    t = text.replace('**', '').replace('***', '')
    t = re.sub(r'^#{1,4}\s*', '', t, flags=re.MULTILINE)
    t = re.sub(r'^[\-─━═]{3,}$', '', t, flags=re.MULTILINE)
    t = re.sub(r'^[•●◦▪]\s*', '', t, flags=re.MULTILINE)
    t = re.sub(r'\n{3,}', '\n\n', t)
    return t.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Priority calculation
# ─────────────────────────────────────────────────────────────────────────────
def _calc_priority(category, revenue_total, avg_rrc_60d, max_rrc_60d, site_utilization=None, users_connected=None):
    """Calculate AI ticket priority using 4 revenue brackets + users connected
    + RRC metrics + site utilization.

    Revenue brackets (p25/p50/p75, derived from all-months average per site).
    Users brackets  (p25/p50/p75, derived from business KPI users columns).

    Site utilization factor:
      High utilization (>= 80%) adds +1 to score (overloaded sites are higher priority)
    """
    severity = 3 if category == "Severe Worst" else 1
    rev = float(revenue_total or 0)

    # 4-bracket revenue scoring using dynamic thresholds
    rev_brackets = _get_revenue_brackets()
    if rev >= rev_brackets["p75"]:
        rev_score = 4
    elif rev >= rev_brackets["p50"]:
        rev_score = 3
    elif rev >= rev_brackets["p25"]:
        rev_score = 2
    else:
        rev_score = 1

    # 4-bracket users-connected scoring using dynamic thresholds
    usr = float(users_connected or 0)
    usr_brackets = _get_users_brackets()
    if usr >= usr_brackets["p75"]:
        usr_score = 4
    elif usr >= usr_brackets["p50"]:
        usr_score = 3
    elif usr >= usr_brackets["p25"]:
        usr_score = 2
    else:
        usr_score = 1

    arrc = float(avg_rrc_60d or 0)
    avg_rrc_score = 3 if arrc >= 1000 else (2 if arrc >= 300 else 1)
    mrrc = float(max_rrc_60d or 0)
    max_rrc_score = 3 if mrrc >= 1500 else (2 if mrrc >= 500 else 1)

    # Site utilization bonus
    util_score = 0
    if site_utilization is not None:
        util = float(site_utilization)
        if util >= 80:
            util_score = 1

    score = severity + rev_score + usr_score + avg_rrc_score + max_rrc_score + util_score
    # Thresholds adjusted for added users bracket (max score now 18)
    if score >= 14: return score, "Critical", 2
    elif score >= 11: return score, "High", 4
    elif score >= 7:  return score, "Medium", 8
    else: return score, "Low", 16


# Cache users brackets — recalculated daily
_USERS_BRACKETS_CACHE = {"data": None, "date": None}

def _get_users_brackets():
    """Calculate users-connected percentile brackets from business KPI data."""
    today = _date.today()
    if _USERS_BRACKETS_CACHE["date"] == today and _USERS_BRACKETS_CACHE["data"]:
        return _USERS_BRACKETS_CACHE["data"]

    defaults = {"p25": 200, "p50": 500, "p75": 1000}
    try:
        rows = _sql("""
            SELECT site_id, AVG(num_value) AS avg_u
            FROM flexible_kpi_uploads
            WHERE kpi_type='business' AND column_type='numeric'
              AND num_value IS NOT NULL AND num_value > 0
              AND (column_name ILIKE '%user%' OR column_name ILIKE '%subscriber%'
                   OR column_name ILIKE '%sub%')
            GROUP BY site_id
            ORDER BY avg_u
        """)
        if not rows or len(rows) < 4:
            return defaults

        values = sorted([float(r["avg_u"]) for r in rows])
        n = len(values)
        result = {
            "p25": values[int(n * 0.25)],
            "p50": values[int(n * 0.50)],
            "p75": values[int(n * 0.75)],
        }
        _USERS_BRACKETS_CACHE["data"] = result
        _USERS_BRACKETS_CACHE["date"] = today
        _LOG.info("Users brackets: P25=%.0f, P50=%.0f, P75=%.0f (from %d sites)",
                  result["p25"], result["p50"], result["p75"], n)
        return result
    except Exception as e:
        _LOG.warning("Users brackets calculation failed: %s", e)
        return defaults


# Cache revenue brackets — recalculated daily
_REVENUE_BRACKETS_CACHE = {"data": None, "date": None}

def _get_revenue_brackets():
    """Calculate revenue percentile brackets from all months avg in flexible_kpi_uploads.
    Uses all numeric revenue columns to compute overall average per site, then percentiles."""
    today = _date.today()
    if _REVENUE_BRACKETS_CACHE["date"] == today and _REVENUE_BRACKETS_CACHE["data"]:
        return _REVENUE_BRACKETS_CACHE["data"]

    defaults = {"p25": 30, "p50": 45, "p75": 60}
    try:
        rows = _sql("""
            SELECT site_id, AVG(num_value) AS avg_rev
            FROM flexible_kpi_uploads
            WHERE kpi_type='revenue' AND column_type='numeric' AND num_value IS NOT NULL
              AND num_value > 0
            GROUP BY site_id
            ORDER BY avg_rev
        """)
        if not rows or len(rows) < 4:
            return defaults

        values = sorted([float(r["avg_rev"]) for r in rows])
        n = len(values)
        result = {
            "p25": values[int(n * 0.25)],
            "p50": values[int(n * 0.50)],
            "p75": values[int(n * 0.75)],
        }
        _REVENUE_BRACKETS_CACHE["data"] = result
        _REVENUE_BRACKETS_CACHE["date"] = today
        _LOG.info("Revenue brackets: P25=%.1f, P50=%.1f, P75=%.1f (from %d sites)", result["p25"], result["p50"], result["p75"], n)
        return result
    except Exception as e:
        _LOG.warning("Revenue brackets calculation failed: %s", e)
        return defaults


# ─────────────────────────────────────────────────────────────────────────────
# Worst cells detection
# A cell is "worst" if ALL 3 conditions fail on 7-day average:
#   1. E-RAB Call Drop Rate_1 > 1.5%
#   2. LTE Call Setup Success Rate < 98.5%
#   3. LTE DL - Usr Ave Throughput < 8 Mbps
# ─────────────────────────────────────────────────────────────────────────────
_DROP_KPI = "E-RAB Call Drop Rate_1"
_CSSR_KPI = "LTE Call Setup Success Rate"
_TPUT_KPI = "LTE DL - Usr Ave Throughput"


def _resolve_kpi_name(default, patterns):
    """Find the actual KPI name stored in kpi_data that best matches a canonical
    name / list of lowercased substring patterns. Falls back to `default`."""
    try:
        rows = _sql("SELECT DISTINCT kpi_name FROM kpi_data WHERE kpi_name IS NOT NULL")
        names = [r["kpi_name"] for r in rows]
    except Exception:
        return default
    if default in names:
        return default
    for pat in patterns:
        pl = pat.lower()
        for n in names:
            if n and pl in n.lower():
                return n
    return default


def _pick_agent_for_site(site_zone, site_city, site_state, domain="RAN"):
    """Route a network-issue ticket to an appropriate agent.

    Selection factors (all applied):
      1. Domain match     — agent.expertise contains an RF/LTE/5G/RAN/CORE/TRANSPORT token
      2. Location match   — agent.location overlaps with site zone/city/state
      3. Bandwidth cap    — prefer agents with open_tickets < bandwidth_capacity
      4. Load balance     — among qualifying agents, pick least loaded
    Falls back to any available human_agent if no perfect match.
    """
    try:
        from app import _open_ticket_count
    except Exception:
        def _open_ticket_count(_): return 0

    DOMAIN_EXPERTISE = {
        "RAN":       {"NETWORK_RF", "NETWORK_OPTIMIZATION", "LTE", "5G", "RAN"},
        "CORE":      {"CORE", "EPC", "5GC", "IMS"},
        "TRANSPORT": {"TRANSPORT", "BACKHAUL", "IP", "MPLS"},
    }
    wanted_exp = DOMAIN_EXPERTISE.get(domain.upper(), DOMAIN_EXPERTISE["RAN"])

    try:
        all_agents = User.query.filter(User.role == "human_agent").all()
    except Exception:
        return None
    if not all_agents:
        return None

    site_zone_l  = (site_zone  or "").lower().strip()
    site_city_l  = (site_city  or "").lower().strip()
    site_state_l = (site_state or "").lower().strip()

    def _score(a):
        score = 0
        exp = (a.expertise or "").upper()
        # Domain match
        if any(tok in exp for tok in wanted_exp):
            score += 10
        # Location match
        loc = (a.location or "").lower()
        if loc and site_zone_l  and (loc in site_zone_l  or site_zone_l  in loc): score += 6
        if loc and site_city_l  and (loc in site_city_l  or site_city_l  in loc): score += 4
        if loc and site_state_l and (loc in site_state_l or site_state_l in loc): score += 2
        # Bandwidth headroom (negative penalty when overloaded)
        cap = a.bandwidth_capacity or 10
        open_n = _open_ticket_count(a.id)
        headroom = max(cap - open_n, 0)
        score += min(headroom, 5)  # up to +5 for headroom
        # Load penalty
        score -= min(open_n, 8)    # up to -8 for heavy load
        return score

    ranked = sorted(all_agents, key=_score, reverse=True)
    # Prefer an agent under their bandwidth cap
    for a in ranked:
        cap = a.bandwidth_capacity or 10
        if _open_ticket_count(a.id) < cap:
            return a
    return ranked[0] if ranked else None


def _resolve_core_kpis():
    """Return the (drop, cssr, tput, prb) KPI names as actually stored in DB."""
    return (
        _resolve_kpi_name("E-RAB Call Drop Rate_1",       ["e-rab call drop", "call drop rate", "erab drop", "drop rate"]),
        _resolve_kpi_name("LTE Call Setup Success Rate",  ["call setup success", "cssr", "setup success"]),
        _resolve_kpi_name("LTE DL - Usr Ave Throughput",  ["dl usr ave", "usr ave throughput", "user throughput"]),
        _resolve_kpi_name("DL PRB Utilization (1BH)",     ["dl prb util", "prb util", "dl_prb"]),
    )


def _get_worst_cells_by_site():
    """Find worst cells: every cell_site_id where ALL 3 KPI thresholds fail
    on last 7 days average from latest available data date. Groups results by site_id."""

    # Last 7 days from current date (per product spec).
    today = _date.today()
    start_date = today - timedelta(days=7)
    # Resolve actual KPI names from DB — handles upload naming drift.
    drop_kpi, cssr_kpi, tput_kpi, _ = _resolve_core_kpis()
    print(f"[WORST CELLS] Scanning cells from {start_date} to {today} — kpis: drop='{drop_kpi}' cssr='{cssr_kpi}' tput='{tput_kpi}'")

    try:
        rows = _sql("""
            SELECT k.site_id, k.cell_id, k.cell_site_id, ts.zone, ts.city, ts.state,
                   AVG(CASE WHEN k.kpi_name=:drop THEN k.value END) AS avg_drop,
                   AVG(CASE WHEN k.kpi_name=:cssr THEN k.value END) AS avg_cssr,
                   AVG(CASE WHEN k.kpi_name=:tput THEN k.value END) AS avg_tput
            FROM kpi_data k
            JOIN telecom_sites ts ON k.site_id = ts.site_id
            WHERE k.value IS NOT NULL AND k.data_level = 'cell'
              AND k.kpi_name IN (:drop, :cssr, :tput)
              AND k.date >= :start_date AND k.date <= :end_date
            GROUP BY k.site_id, k.cell_id, k.cell_site_id, ts.zone, ts.city, ts.state
            HAVING AVG(CASE WHEN k.kpi_name=:drop THEN k.value END) > 1.5
               AND AVG(CASE WHEN k.kpi_name=:cssr THEN k.value END) < 98.5
               AND AVG(CASE WHEN k.kpi_name=:tput THEN k.value END) < 8
            ORDER BY AVG(CASE WHEN k.kpi_name=:drop THEN k.value END) DESC
        """, {"drop": drop_kpi, "cssr": cssr_kpi, "tput": tput_kpi,
              "start_date": start_date, "end_date": today})
    except Exception as e:
        print(f"[WORST CELLS] Query failed: {e}")
        _LOG.error("Failed to get worst cells: %s", e)
        return {}

    print(f"[WORST CELLS] Found {len(rows)} cells failing ALL 3 thresholds")

    # Group cells by site
    sites = {}
    for r in rows:
        sid = r["site_id"]
        if sid not in sites:
            sites[sid] = {"cells": [], "cell_site_ids": [], "zone": r.get("zone") or "",
                          "city": r.get("city") or "", "state": r.get("state") or "",
                          "drops": [], "cssrs": [], "tputs": []}
        sites[sid]["cells"].append(r["cell_id"] or "")
        sites[sid]["cell_site_ids"].append(r.get("cell_site_id") or r["cell_id"] or "")
        sites[sid]["drops"].append(float(r.get("avg_drop") or 0))
        sites[sid]["cssrs"].append(float(r.get("avg_cssr") or 100))
        sites[sid]["tputs"].append(float(r.get("avg_tput") or 999))

    result = {}
    for sid, d in sites.items():
        n = len(d["cells"])
        result[sid] = {
            "cells": d["cells"], "cell_site_ids": d["cell_site_ids"],
            "zone": d["zone"], "city": d["city"], "state": d["state"],
            "avg_drop": _f(sum(d["drops"])/n, 2), "avg_cssr": _f(sum(d["cssrs"])/n, 2),
            "avg_tput": _f(sum(d["tputs"])/n, 2), "violations": 3,
            "category": "Severe Worst",  # All 3 thresholds fail → always Severe Worst
        }
    print(f"[WORST CELLS] Grouped into {len(result)} sites: {list(result.keys())}")
    return result


def _get_site_rrc_and_revenue(site_id):
    """Get RRC, revenue (all months average), users connected, and site utilization
    for priority calculation. Prefers site-level KPI data; falls back to cell-level
    (averaged across cells) for sites that have no site-level uploads."""
    avg_rrc = max_rrc = revenue = 0
    users_connected = 0
    site_utilization = None
    try:
        # Prefer site-level; fall back to cell-level for the same site if none exists.
        r = _sql("""
            WITH levels AS (
                SELECT data_level,
                       AVG(CASE WHEN kpi_name='Ave RRC Connected Ue' THEN value END) AS avg_rrc,
                       MAX(CASE WHEN kpi_name='Max RRC Connected Ue' THEN value END) AS max_rrc
                FROM kpi_data
                WHERE site_id=:sid AND value IS NOT NULL
                  AND kpi_name IN ('Ave RRC Connected Ue','Max RRC Connected Ue')
                  AND date >= CURRENT_DATE - INTERVAL '60 days'
                GROUP BY data_level
            )
            SELECT avg_rrc, max_rrc FROM levels WHERE data_level='site'
            UNION ALL
            SELECT avg_rrc, max_rrc FROM levels
            WHERE data_level='cell'
              AND NOT EXISTS (SELECT 1 FROM levels WHERE data_level='site')
            LIMIT 1
        """, {"sid": site_id})
        if r: avg_rrc = float(r[0].get("avg_rrc") or 0); max_rrc = float(r[0].get("max_rrc") or 0)
    except: pass

    # Revenue: average across monthly revenue columns (exclude opex/util)
    try:
        r = _sql("""SELECT AVG(num_value) AS avg_rev FROM flexible_kpi_uploads
                    WHERE site_id=:sid AND kpi_type='revenue' AND column_type='numeric'
                      AND num_value IS NOT NULL AND num_value > 0
                      AND column_name NOT ILIKE '%opex%'
                      AND column_name NOT ILIKE '%util%'""", {"sid": site_id})
        if r and r[0].get("avg_rev"): revenue = float(r[0]["avg_rev"])
    except: pass

    # Users connected: average across monthly user columns in business KPI
    try:
        r = _sql("""SELECT AVG(num_value) AS avg_u FROM flexible_kpi_uploads
                    WHERE site_id=:sid AND kpi_type='business' AND column_type='numeric'
                      AND num_value IS NOT NULL AND num_value > 0
                      AND (column_name ILIKE '%user%' OR column_name ILIKE '%subscriber%'
                           OR column_name ILIKE '%sub%')""", {"sid": site_id})
        if r and r[0].get("avg_u"): users_connected = float(r[0]["avg_u"])
    except: pass

    # Site utilization: check for utilization column in revenue data
    try:
        r = _sql("""SELECT AVG(num_value) AS util FROM flexible_kpi_uploads
                    WHERE site_id=:sid AND kpi_type='revenue' AND column_type='numeric'
                      AND (column_name ILIKE '%utilization%' OR column_name ILIKE '%util%')
                      AND num_value IS NOT NULL""", {"sid": site_id})
        if r and r[0].get("util"): site_utilization = float(r[0]["util"])
    except: pass

    return avg_rrc, max_rrc, revenue, site_utilization, users_connected


# ─────────────────────────────────────────────────────────────────────────────
# Pre-scan: keeps worst cells fresh for the dashboard
# ─────────────────────────────────────────────────────────────────────────────
def run_pre_scan():
    """07:30 IST daily pre-scan — detects (not creates) worst cells AND
    overutilized sites. Results are cached for the dashboard + 08:00 ticket run.
    Prints verbose logs so they show up in the VS Code terminal."""
    global _LATEST_WORST_CELLS, _LATEST_SCAN_TIME
    global _LATEST_OVERUTILIZED, _LATEST_OVERUTIL_SCAN_TIME
    _stamp_0800 = datetime.combine(_date.today(), datetime.min.time()).replace(hour=8)

    print("=" * 70)
    print(f"[NETWORK ISSUES] 07:30 PRE-SCAN STARTED — {_date.today()}")
    print("=" * 70)

    print("[WORST CELLS] Scanning last 7 days cell-level KPIs...")
    try:
        _LATEST_WORST_CELLS = _get_worst_cells_by_site()
        _LATEST_SCAN_TIME = _stamp_0800
        print(f"[WORST CELLS] ✓ Found {len(_LATEST_WORST_CELLS)} sites with severe worst cells: "
              f"{list(_LATEST_WORST_CELLS.keys())[:20]}{' ...' if len(_LATEST_WORST_CELLS) > 20 else ''}")
    except Exception as e:
        print(f"[WORST CELLS] ✗ Pre-scan failed: {e}")
        _LOG.error("Worst-cells pre-scan failed: %s", e)

    print("[OVERUTILIZED] Scanning last 7 days site-level DL PRB Utilization > 92%...")
    try:
        _LATEST_OVERUTILIZED = _get_overutilized_sites()
        _LATEST_OVERUTIL_SCAN_TIME = _stamp_0800
        print(f"[OVERUTILIZED] ✓ Found {len(_LATEST_OVERUTILIZED)} sites with PRB > 92%: "
              f"{list(_LATEST_OVERUTILIZED.keys())[:20]}{' ...' if len(_LATEST_OVERUTILIZED) > 20 else ''}")
    except Exception as e:
        print(f"[OVERUTILIZED] ✗ Pre-scan failed: {e}")
        _LOG.error("Overutilized pre-scan failed: %s", e)

    print("[NETWORK ISSUES] 07:30 PRE-SCAN COMPLETE (tickets will be created at 08:00)")
    print("=" * 70)


def _get_existing_open_tickets():
    """Get all open/in_progress tickets grouped by site_id.
    Returns { site_id: NetworkIssueTicket } — the most recent ticket per site."""
    tickets = NetworkIssueTicket.query.filter(
        NetworkIssueTicket.status.in_(["open", "in_progress"])
    ).order_by(NetworkIssueTicket.updated_at.desc()).all()
    result = {}
    for t in tickets:
        if t.site_id not in result:  # keep most recent per site
            result[t.site_id] = t
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Daily job — runs at 08:00 IST or on startup if missed
#
# Logic:
# 1. Find all cells where ALL 3 KPIs fail on 7-day average
# 2. Group by site
# 3. For each site:
#    - If an open/in_progress ticket exists for this site with SAME cells
#      → UPDATE the existing ticket (refresh KPIs, set updated_at = today 08:00)
#    - If cells changed OR no existing ticket
#      → CREATE a new ticket, assign to agent
# 4. Timestamp is always 08:00 AM today (even if server runs later)
# ─────────────────────────────────────────────────────────────────────────────
def run_daily_network_issue_job():
    global _LAST_JOB_DATE, _TODAYS_ROUTING_LOG

    # Reset today's routing log so it shows only this run's actions
    _TODAYS_ROUTING_LOG = [e for e in _TODAYS_ROUTING_LOG
                           if e.get("timestamp","")[:10] == _date.today().isoformat()]

    print("=" * 70)
    print(f"[NETWORK ISSUES] 08:00 DAILY JOB STARTED — {_date.today()}")
    print("=" * 70)

    worst_sites = _get_worst_cells_by_site()
    if not worst_sites:
        print("[NETWORK ISSUES] No worst cells found — no tickets to create/update.")
        _LAST_JOB_DATE = _date.today()
        return 0

    # Today at 08:00 AM — the official daily timestamp
    today_08 = datetime.combine(_date.today(), datetime.min.time()).replace(hour=8, minute=0, second=0)

    # Get all existing open/in_progress tickets
    existing_tickets = _get_existing_open_tickets()

    created = updated = 0
    for site_id, data in worst_sites.items():
        avg_rrc, max_rrc, revenue, site_utilization, users_connected = _get_site_rrc_and_revenue(site_id)
        score, priority, sla_hours = _calc_priority(data["category"], revenue, avg_rrc, max_rrc, site_utilization, users_connected)
        location = f"{data['city']}, {data['state']}" if data['city'] else data['zone']
        today_cells = set(data["cells"])

        # Check if we have an existing open ticket for this site
        existing = existing_tickets.get(site_id)
        if existing:
            existing_cells = set(existing.cells_affected.split(",")) if existing.cells_affected else set()
            if existing_cells == today_cells:
                # SAME cells → UPDATE existing ticket
                existing.category = data["category"]
                existing.priority = priority
                existing.priority_score = score
                existing.sla_hours = sla_hours
                existing.avg_rrc = _f(avg_rrc, 1)
                existing.max_rrc = _f(max_rrc, 1)
                existing.revenue_total = _f(revenue, 1)
                existing.avg_drop_rate = data["avg_drop"]
                existing.avg_cssr = data["avg_cssr"]
                existing.avg_tput = data["avg_tput"]
                existing.violations = data["violations"]
                existing.zone = data["zone"]
                existing.location = location
                existing.updated_at = today_08
                updated += 1
                _ag_name = "unassigned"
                try:
                    if existing.assigned_agent:
                        _ag = db.session.get(User, existing.assigned_agent)
                        if _ag: _ag_name = _ag.name or _ag.email
                except Exception: pass
                print(f"  [WORST UPDATE] Ticket #{existing.id}  site={site_id}  cells={len(today_cells)} (same)  priority={priority}  SLA={sla_hours}h  agent={_ag_name}")
                _TODAYS_ROUTING_LOG.append({
                    "type": "worst_cells", "action": "update", "ticket_id": existing.id,
                    "site_id": site_id, "cells_count": len(today_cells), "priority": priority,
                    "sla_hours": sla_hours, "agent": _ag_name, "zone": data.get("zone",""),
                    "timestamp": today_08.isoformat(),
                })
                continue
            else:
                # DIFFERENT cells → close old ticket, create new one below
                existing.status = "closed"
                existing.updated_at = today_08
                print(f"  [CLOSE] {site_id}: cells changed ({len(existing_cells)}→{len(today_cells)}), closing old ticket #{existing.id}")

        # CREATE new ticket — pick an agent by expertise+location+bandwidth+load
        agent = _pick_agent_for_site(data.get("zone"), data.get("city"), data.get("state"), domain="RAN")

        ticket = NetworkIssueTicket(
            site_id=site_id,
            cells_affected=",".join(data["cells"]),
            cell_site_ids=",".join(data["cell_site_ids"]),
            category=data["category"],
            priority=priority, priority_score=score, sla_hours=sla_hours,
            avg_rrc=_f(avg_rrc, 1), max_rrc=_f(max_rrc, 1), revenue_total=_f(revenue, 1),
            avg_drop_rate=data["avg_drop"], avg_cssr=data["avg_cssr"], avg_tput=data["avg_tput"],
            violations=data["violations"], status="open",
            assigned_agent=agent.id if agent else None,
            zone=data["zone"], location=location,
            created_at=today_08, updated_at=today_08,
            deadline_time=today_08 + timedelta(hours=sla_hours),
        )
        db.session.add(ticket)
        db.session.flush()  # get ticket.id for routing log
        created += 1
        _agname = agent.name if agent else "unassigned"
        print(f"  [WORST CREATE] Ticket #{ticket.id}  site={site_id}  cells={len(today_cells)}  priority={priority}  SLA={sla_hours}h  → routed to: {_agname}")
        _TODAYS_ROUTING_LOG.append({
            "type": "worst_cells", "action": "create", "ticket_id": ticket.id,
            "site_id": site_id, "cells_count": len(today_cells), "priority": priority,
            "sla_hours": sla_hours, "agent": _agname, "zone": data.get("zone",""),
            "timestamp": today_08.isoformat(),
        })

    db.session.commit()
    _LAST_JOB_DATE = _date.today()
    print(f"[NETWORK ISSUES] Daily job complete: {created} created, {updated} updated")
    return created + updated


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler: runs at 08:00 IST (02:30 UTC) + startup fallback
# ─────────────────────────────────────────────────────────────────────────────
def schedule_daily_job(app):
    import threading, time

    def _ist_now():
        """Return current datetime in IST (UTC+5:30)."""
        return datetime.utcnow() + timedelta(hours=5, minutes=30)

    # Track whether today's 07:30 pre-scan and 08:00 ticket job have run
    _prescan_done_date = None   # date of last 07:30 pre-scan
    _ticket_done_date = None    # date of last 08:00 ticket job

    def _tickets_already_processed_today():
        """Check if daily job already ran today by looking at created_at OR updated_at date."""
        today_start = datetime.combine(_date.today(), datetime.min.time())
        tomorrow_start = today_start + timedelta(days=1)
        count = NetworkIssueTicket.query.filter(
            db.or_(
                db.and_(NetworkIssueTicket.created_at >= today_start, NetworkIssueTicket.created_at < tomorrow_start),
                db.and_(NetworkIssueTicket.updated_at >= today_start, NetworkIssueTicket.updated_at < tomorrow_start),
            )
        ).count()
        return count > 0

    def _job_loop():
        global _LAST_JOB_DATE
        nonlocal _prescan_done_date, _ticket_done_date
        time.sleep(8)  # wait for app to start
        last_prescan = 0  # epoch seconds of last pre-scan

        with app.app_context():
            # Step 1: Run pre-scan to populate dashboard worst cells
            try:
                run_pre_scan()
            except Exception as e:
                print(f"[NETWORK ISSUES] Startup pre-scan failed: {e}")

            # Step 2: Check if daily job needs to run
            ist = _ist_now()
            if _LAST_JOB_DATE == _date.today():
                print("[NETWORK ISSUES] Daily job already ran today (in-memory flag).")
            elif _tickets_already_processed_today():
                _LAST_JOB_DATE = _date.today()
                _ticket_done_date = _date.today()
                print("[NETWORK ISSUES] Daily job already ran today (DB check — tickets have today 08:00).")
            else:
                # Job hasn't run today — run it now (catch-up for missed 08:00)
                print(f"[NETWORK ISSUES] Running daily job (IST: {ist.strftime('%H:%M:%S')}, timestamp will be 08:00 AM)")
                try:
                    count = run_daily_network_issue_job()
                    _ticket_done_date = _date.today()
                    print(f"[NETWORK ISSUES] Daily job done: {count} tickets processed")
                    # Also run overutilized site detection
                    try:
                        ou_count = run_overutilized_job()
                        print(f"[OVERUTILIZED] Job done: {ou_count} tickets processed")
                    except Exception as oe:
                        print(f"[OVERUTILIZED] Job FAILED: {oe}")
                except Exception as e:
                    print(f"[NETWORK ISSUES] Daily job FAILED: {e}")
                    import traceback; traceback.print_exc()

            if ist.hour >= 7 and ist.minute >= 30 or ist.hour >= 8:
                _prescan_done_date = _date.today()

        while True:
            ist = _ist_now()
            today = _date.today()

            # ── 07:30 IST: Daily pre-scan to refresh dashboard worst cells ──
            if (ist.hour > 7 or (ist.hour == 7 and ist.minute >= 30)) and _prescan_done_date != today:
                _LOG.info("07:30 IST daily pre-scan: refreshing worst cells for dashboard (IST: %s)", ist.strftime("%Y-%m-%d %H:%M:%S"))
                try:
                    with app.app_context():
                        run_pre_scan()
                    _prescan_done_date = today
                except Exception as e:
                    _LOG.error("07:30 pre-scan failed: %s", e)

            # ── 08:00 IST: Daily ticket creation/update (ONLY at 8 AM, never on restart) ──
            if ist.hour == 8 and ist.minute < 30 and _LAST_JOB_DATE != today:
                _LOG.info("08:00 IST daily job: creating/updating network issue tickets (IST: %s)", ist.strftime("%Y-%m-%d %H:%M:%S"))
                try:
                    with app.app_context():
                        run_daily_network_issue_job()
                        try: run_overutilized_job()
                        except Exception as oe: _LOG.error("Overutilized job failed: %s", oe)
                    _ticket_done_date = today
                except Exception as e:
                    _LOG.error("Daily job failed: %s", e)

            time.sleep(30)

    threading.Thread(target=_job_loop, daemon=True).start()
    _LOG.info("Network issue scheduler started (pre-scan at 07:30 IST, tickets at 08:00 IST daily)")

# ─────────────────────────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@network_issues_bp.route("/api/network-issues/worst-cells", methods=["GET"])
@jwt_required()
def get_worst_cells():
    """Return worst cells for the dashboard. Uses cached data or fetches live if cache is empty."""
    user_id = int(get_jwt_identity())
    user = db.session.get(User, user_id)
    if not user or user.role not in ("human_agent", "admin", "manager", "cto"):
        return jsonify({"error": "Unauthorized"}), 403
    # If cache is empty, try a live fetch
    source_data = _LATEST_WORST_CELLS
    if not source_data:
        _LOG.info("Worst cells cache empty, running live query")
        source_data = _get_worst_cells_by_site()
    sites = []
    for site_id, data in source_data.items():
        sites.append({
            "site_id": site_id,
            "cells": data.get("cells", []),
            "cell_site_ids": data.get("cell_site_ids", []),
            "avg_drop": data.get("avg_drop", 0),
            "avg_cssr": data.get("avg_cssr", 0),
            "avg_tput": data.get("avg_tput", 0),
            "category": data.get("category", "Worst"),
            "province": data.get("zone", ""),
            "zone": data.get("zone", ""),
            "city": data.get("city", ""),
        })
    return jsonify({
        "sites": sites,
        "count": len(sites),
        "scan_time": (_LATEST_SCAN_TIME or datetime.combine(_date.today(), datetime.min.time()).replace(hour=8)).isoformat(),
    })


@network_issues_bp.route("/api/network-issues/list", methods=["GET"])
@jwt_required()
def list_network_issues():
    user_id = int(get_jwt_identity())
    user = db.session.get(User, user_id)
    if not user or user.role not in ("human_agent", "admin", "manager", "cto"):
        return jsonify({"error": "Unauthorized"}), 403

    tickets = NetworkIssueTicket.query.order_by(NetworkIssueTicket.created_at.desc()).all()
    now = datetime.utcnow()

    # Pre-fetch site_abs_id + site_name for every distinct site_id in one query
    abs_map = {}
    try:
        site_ids = list({t.site_id for t in tickets if t.site_id})
        if site_ids:
            from models import TelecomSite
            for s in TelecomSite.query.filter(TelecomSite.site_id.in_(site_ids)).all():
                # first row per site_id wins
                if s.site_id not in abs_map:
                    abs_map[s.site_id] = {"abs": s.site_abs_id or "", "name": s.site_name or ""}
    except Exception:
        pass

    result = []
    for t in tickets:
        sla_remaining = sla_pct = 0
        if t.deadline_time:
            rem = (t.deadline_time - now).total_seconds() / 3600
            sla_remaining = round(max(rem, 0), 2)
            sla_pct = round(min((t.sla_hours - max(rem, 0)) / max(t.sla_hours, 1) * 100, 100), 1)
        agent_name = agent_eid = ""
        if t.assigned_agent:
            ag = db.session.get(User, t.assigned_agent)
            if ag: agent_name = ag.name; agent_eid = ag.employee_id or ""

        site_meta = abs_map.get(t.site_id, {})
        result.append({
            "id": t.id, "site_id": t.site_id,
            "site_abs_id": site_meta.get("abs", ""),
            "site_name": site_meta.get("name", ""),
            "cells_affected": t.cells_affected, "cell_site_ids": t.cell_site_ids,
            "cell_count": len(t.cells_affected.split(",")) if t.cells_affected else 0,
            "category": t.category, "priority": t.priority, "priority_score": t.priority_score,
            "sla_hours": t.sla_hours, "sla_remaining": sla_remaining, "sla_pct": sla_pct,
            "sla_breached": sla_remaining <= 0 and t.status not in ("resolved", "closed"),
            "avg_rrc": t.avg_rrc, "max_rrc": t.max_rrc, "revenue_total": t.revenue_total,
            "avg_drop_rate": t.avg_drop_rate, "avg_cssr": t.avg_cssr, "avg_tput": t.avg_tput,
            "violations": t.violations, "status": t.status,
            "assigned_agent": t.assigned_agent, "agent_name": agent_name, "agent_eid": agent_eid,
            "zone": t.zone, "location": t.location,
            "is_mine": t.assigned_agent == user_id,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "updated_at": t.updated_at.isoformat() if t.updated_at else None,
            "deadline_time": t.deadline_time.isoformat() if t.deadline_time else None,
        })
    return jsonify({"tickets": result, "total": len(result)})


@network_issues_bp.route("/api/network-issues/todays-routing", methods=["GET"])
@jwt_required()
def todays_routing():
    """Return ALL tickets (worst cells + overutilized) created or updated TODAY."""
    today = _date.today()
    today_start = datetime.combine(today, datetime.min.time())
    tomorrow_start = today_start + timedelta(days=1)

    routing = []

    # Worst cell tickets created/updated today
    wc_tickets = NetworkIssueTicket.query.filter(
        db.or_(
            db.and_(NetworkIssueTicket.created_at >= today_start, NetworkIssueTicket.created_at < tomorrow_start),
            db.and_(NetworkIssueTicket.updated_at >= today_start, NetworkIssueTicket.updated_at < tomorrow_start),
        )
    ).order_by(NetworkIssueTicket.updated_at.desc()).all()

    for t in wc_tickets:
        agent_name = agent_email = ""
        if t.assigned_agent:
            ag = db.session.get(User, t.assigned_agent)
            if ag: agent_name = ag.name or ""; agent_email = ag.email or ""
        created_today = t.created_at and t.created_at.date() == today
        routing.append({
            "ticket_id": t.id, "site_id": t.site_id,
            "cells_affected": t.cells_affected or "",
            "cell_count": len(t.cells_affected.split(",")) if t.cells_affected else 0,
            "category": "Worst Cell", "priority": t.priority, "status": t.status or "open",
            "avg_drop_rate": t.avg_drop_rate, "avg_cssr": t.avg_cssr, "avg_tput": t.avg_tput,
            "zone": t.zone, "location": t.location,
            "timestamp": t.updated_at.isoformat() if t.updated_at else None,
            "agent_name": agent_name, "agent_email": agent_email,
            "type": "created" if created_today else "updated",
        })

    # Overutilized tickets created/updated today
    try:
        ou_tickets = OverutilizedTicket.query.filter(
            db.or_(
                db.and_(OverutilizedTicket.created_at >= today_start, OverutilizedTicket.created_at < tomorrow_start),
                db.and_(OverutilizedTicket.updated_at >= today_start, OverutilizedTicket.updated_at < tomorrow_start),
            )
        ).order_by(OverutilizedTicket.updated_at.desc()).all()

        for t in ou_tickets:
            agent_name = agent_email = ""
            if t.assigned_agent:
                ag = db.session.get(User, t.assigned_agent)
                if ag: agent_name = ag.name or ""; agent_email = ag.email or ""
            created_today = t.created_at and t.created_at.date() == today
            routing.append({
                "ticket_id": t.id, "site_id": t.site_id,
                "cells_affected": t.cells_affected or "",
                "cell_count": t.site_count or 0,
                "category": "Overutilized", "priority": t.priority, "status": t.status or "open",
                "avg_prb_util": t.avg_prb_util, "avg_dl_tput": t.avg_dl_tput,
                "zone": t.zone, "location": t.location,
                "timestamp": t.updated_at.isoformat() if t.updated_at else None,
                "agent_name": agent_name, "agent_email": agent_email,
                "type": "created" if created_today else "updated",
            })
    except Exception as e:
        _LOG.warning("Overutilized routing fetch failed: %s", e)

    return jsonify({"routing": routing, "total": len(routing), "date": today.isoformat()})


@network_issues_bp.route("/api/network-issues/<int:ticket_id>", methods=["GET"])
@jwt_required()
def get_network_issue(ticket_id):
    t = db.session.get(NetworkIssueTicket, ticket_id)
    if not t: return jsonify({"error": "Not found"}), 404
    agent_name = agent_eid = ""
    if t.assigned_agent:
        ag = db.session.get(User, t.assigned_agent)
        if ag: agent_name = ag.name; agent_eid = ag.employee_id or ""
    now = datetime.utcnow()
    sla_remaining = round(max((t.deadline_time - now).total_seconds() / 3600, 0), 2) if t.deadline_time else 0
    return jsonify({
        "id": t.id, "site_id": t.site_id,
        "cells_affected": t.cells_affected, "cell_site_ids": t.cell_site_ids,
        "cells": t.cells_affected.split(",") if t.cells_affected else [],
        "cell_site_id_list": t.cell_site_ids.split(",") if t.cell_site_ids else [],
        "category": t.category, "priority": t.priority, "priority_score": t.priority_score,
        "sla_hours": t.sla_hours, "sla_remaining": sla_remaining,
        "avg_rrc": t.avg_rrc, "max_rrc": t.max_rrc, "revenue_total": t.revenue_total,
        "avg_drop_rate": t.avg_drop_rate, "avg_cssr": t.avg_cssr, "avg_tput": t.avg_tput,
        "violations": t.violations, "status": t.status,
        "assigned_agent": t.assigned_agent, "agent_name": agent_name, "agent_eid": agent_eid,
        "zone": t.zone, "location": t.location,
        "root_cause": t.root_cause or "", "recommendation": t.recommendation or "",
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "deadline_time": t.deadline_time.isoformat() if t.deadline_time else None,
    })


@network_issues_bp.route("/api/network-issues/<int:ticket_id>/status", methods=["PUT"])
@jwt_required()
def update_network_issue_status(ticket_id):
    t = db.session.get(NetworkIssueTicket, ticket_id)
    if not t: return jsonify({"error": "Not found"}), 404
    new_status = (request.json or {}).get("status", "").strip()
    if new_status not in ("open", "in_progress", "resolved", "closed"):
        return jsonify({"error": "Invalid status"}), 400
    t.status = new_status
    t.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify({"success": True, "status": new_status})


@network_issues_bp.route("/api/network-issues/<int:ticket_id>/pdf-data", methods=["GET"])
@jwt_required()
def network_issue_pdf_data(ticket_id):
    """Return comprehensive data for PDF report: site/cell info, location, KPIs."""
    from models import TelecomSite
    t = db.session.get(NetworkIssueTicket, ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404

    # Site info from telecom_sites
    site_rows = TelecomSite.query.filter_by(site_id=t.site_id).all()
    site_info = None
    cells_info = []
    if site_rows:
        first = site_rows[0]
        site_info = {
            "site_id": first.site_id,
            "site_name": getattr(first, "site_name", None) or first.site_id,
            "site_abs_id": getattr(first, "site_abs_id", None),
            "vendor_name": getattr(first, "vendor_name", None),
            "latitude": first.latitude,
            "longitude": first.longitude,
            "province": getattr(first, "province", "") or first.zone or "",
            "commune": getattr(first, "commune", "") or "",
            "zone": first.zone or "",
            "city": getattr(first, "city", "") or "",
            "state": getattr(first, "state", "") or "",
            "site_status": first.site_status or "on_air",
            "alarms": first.alarms or "",
            "bandwidth_mhz": first.bandwidth_mhz,
            "antenna_gain_dbi": first.antenna_gain_dbi,
            "rf_power_eirp_dbm": first.rf_power_eirp_dbm,
            "antenna_height_agl_m": first.antenna_height_agl_m,
            "e_tilt_degree": first.e_tilt_degree,
            "crs_gain": first.crs_gain,
            "extra_params": getattr(first, "extra_params", None),
        }
        for sr in site_rows:
            if sr.cell_id:
                cells_info.append({
                    "cell_name": sr.cell_id,
                    "cell_id": sr.cell_id,
                    "latitude": sr.latitude,
                    "longitude": sr.longitude,
                    "province": getattr(sr, "province", "") or sr.zone or "",
                    "zone": sr.zone or "",
                    "site_status": sr.site_status or "on_air",
                    "alarms": sr.alarms or "",
                    "bandwidth_mhz": sr.bandwidth_mhz,
                    "antenna_gain_dbi": sr.antenna_gain_dbi,
                    "rf_power_eirp_dbm": sr.rf_power_eirp_dbm,
                    "antenna_height_agl_m": sr.antenna_height_agl_m,
                    "e_tilt_degree": sr.e_tilt_degree,
                    "crs_gain": sr.crs_gain,
                })

    # KPI snapshot per cell from kpi_data (latest date, affected cells only)
    cell_ids = t.cells_affected.split(",") if t.cells_affected else []
    cell_kpis = []
    if cell_ids:
        try:
            placeholders = ",".join([f":c{i}" for i in range(len(cell_ids))])
            params = {f"c{i}": c.strip() for i, c in enumerate(cell_ids)}
            params["sid"] = t.site_id
            rows = _sql(f"""
                SELECT k.cell_id,
                       AVG(CASE WHEN k.kpi_name='E-RAB Call Drop Rate_1' THEN k.value END) AS drop_rate,
                       AVG(CASE WHEN k.kpi_name='LTE Call Setup Success Rate' THEN k.value END) AS cssr,
                       AVG(CASE WHEN k.kpi_name='LTE DL - Usr Ave Throughput' THEN k.value END) AS tput,
                       AVG(CASE WHEN k.kpi_name='Ave RRC Connected Ue' THEN k.value END) AS rrc
                FROM kpi_data k
                WHERE k.site_id = :sid AND k.cell_id IN ({placeholders})
                  AND k.data_level = 'cell' AND k.value IS NOT NULL
                  AND k.date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY k.cell_id
            """, params)
            for r in rows:
                cell_kpis.append({
                    "cell_id": r["cell_id"],
                    "cell_name": r["cell_id"],
                    "drop_rate": round(float(r["drop_rate"] or 0), 2),
                    "cssr": round(float(r["cssr"] or 0), 1),
                    "tput": round(float(r["tput"] or 0), 1),
                    "rrc": round(float(r["rrc"] or 0), 0),
                })
        except Exception as e:
            _LOG.error("pdf-data cell_kpis: %s", e)

    return jsonify({
        "ticket": {
            "id": t.id, "site_id": t.site_id, "category": t.category,
            "priority": t.priority, "priority_score": t.priority_score,
            "status": t.status, "zone": t.zone, "location": t.location,
            "avg_drop_rate": t.avg_drop_rate, "avg_cssr": t.avg_cssr,
            "avg_tput": t.avg_tput, "avg_rrc": t.avg_rrc, "max_rrc": t.max_rrc,
            "revenue_total": t.revenue_total, "violations": t.violations,
            "sla_hours": t.sla_hours,
            "cells_affected": t.cells_affected,
            "cell_site_ids": t.cell_site_ids,
            "root_cause": t.root_cause or "",
            "recommendation": t.recommendation or "",
            "created_at": t.created_at.isoformat() if t.created_at else None,
        },
        "site_info": site_info,
        "cells_info": cells_info,
        "cell_kpis": cell_kpis,
    })


@network_issues_bp.route("/api/network-issues/<int:ticket_id>/trends", methods=["GET"])
@jwt_required()
def network_issue_trends(ticket_id):
    """Get KPI trends for a cell or site within a network issue ticket."""
    t = db.session.get(NetworkIssueTicket, ticket_id)
    if not t: return jsonify({"error": "Not found"}), 404

    target = request.args.get("target", "site")  # cell_id or "site"
    period = request.args.get("period", "day")    # month/week/day/hour

    data_level = "cell" if target != "site" else "site"
    cell_filter = ""
    params = {"sid": t.site_id}
    if target != "site":
        cell_filter = "AND k.cell_id = :cid"
        params["cid"] = target

    kpis = ['E-RAB Call Drop Rate_1', 'LTE Call Setup Success Rate', 'LTE DL - Usr Ave Throughput']
    result = {}
    for kpi in kpis:
        try:
            if period == "month":
                group_key = "TO_CHAR(k.date, 'YYYY-MM')"
            elif period == "week":
                group_key = "TO_CHAR(k.date, 'IYYY-\"W\"IW')"
            elif period == "hour":
                # Last 7 days for hourly (zoomed in)
                group_key = "TO_CHAR(k.date, 'MM/DD')"
                rows = _sql(f"""
                    SELECT {group_key} AS label, AVG(k.value) AS avg, MIN(k.value) AS min, MAX(k.value) AS max
                    FROM kpi_data k WHERE k.site_id=:sid AND k.kpi_name=:kpi
                      AND k.data_level=:dl AND k.value IS NOT NULL {cell_filter}
                      AND k.date >= CURRENT_DATE - INTERVAL '7 days' AND k.date <= CURRENT_DATE
                    GROUP BY {group_key} ORDER BY {group_key}
                """, {**params, "kpi": kpi, "dl": data_level})
                result[kpi] = [{"label": r["label"], "avg": _f(r["avg"], 4), "min": _f(r["min"], 4), "max": _f(r["max"], 4)} for r in rows]
                continue
            else:
                group_key = "k.date::text"

            rows = _sql(f"""
                SELECT {group_key} AS label, AVG(k.value) AS avg, MIN(k.value) AS min, MAX(k.value) AS max
                FROM kpi_data k WHERE k.site_id=:sid AND k.kpi_name=:kpi
                  AND k.data_level=:dl AND k.value IS NOT NULL {cell_filter}
                  AND k.date <= CURRENT_DATE
                GROUP BY {group_key} ORDER BY {group_key}
            """, {**params, "kpi": kpi, "dl": data_level})
            result[kpi] = [{"label": r["label"], "avg": _f(r["avg"], 4), "min": _f(r["min"], 4), "max": _f(r["max"], 4)} for r in rows]
        except Exception as e:
            _LOG.error("Trend query failed for %s/%s: %s", kpi, target, e)
            result[kpi] = []

    return jsonify({"site_id": t.site_id, "target": target, "period": period, "data_level": data_level, "trends": result})


@network_issues_bp.route("/api/network-issues/<int:ticket_id>/rca", methods=["POST"])
@jwt_required()
def network_issue_rca(ticket_id):
    """AI Root Cause Analysis for a specific cell or site."""
    t = db.session.get(NetworkIssueTicket, ticket_id)
    if not t: return jsonify({"error": "Not found"}), 404

    target = (request.json or {}).get("target", "site")
    data_level = "cell" if target != "site" else "site"
    cell_filter = f"AND k.cell_id = '{target}'" if target != "site" else ""

    # ── Gather KPI trend data across 3 time windows ───────────────────────────
    kpi_text = ""
    windows = {}  # parsed: {period: {kpi: {avg, min, max}}}
    for period_name, days in [("7 days", 7), ("30 days", 30), ("60 days", 60)]:
        try:
            rows = _sql(f"""
                SELECT kpi_name, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max
                FROM kpi_data k WHERE site_id=:sid AND data_level=:dl AND value IS NOT NULL
                  AND kpi_name IN ('E-RAB Call Drop Rate_1','LTE Call Setup Success Rate','LTE DL - Usr Ave Throughput','Ave RRC Connected Ue')
                  AND date >= CURRENT_DATE - INTERVAL '{days} days' AND date <= CURRENT_DATE {cell_filter}
                GROUP BY kpi_name
            """, {"sid": t.site_id, "dl": data_level})
            kpi_text += f"\n=== Last {period_name} ({data_level}: {target}) ===\n"
            windows[period_name] = {}
            for r in rows:
                kpi_text += f"  {r['kpi_name']}: avg={_f(r['avg'],2)}, min={_f(r['min'],2)}, max={_f(r['max'],2)}\n"
                windows[period_name][r['kpi_name']] = {
                    "avg": float(r['avg'] or 0), "min": float(r['min'] or 0), "max": float(r['max'] or 0)
                }
        except: pass

    # ── Fetch RF parameters from telecom_sites ────────────────────────────────
    rf = {}
    try:
        cell_rf_filter = f"AND cell_id = '{t.site_id}_{target}'" if target != "site" else ""
        r = _sql(f"""SELECT AVG(bandwidth_mhz) AS bw, AVG(antenna_gain_dbi) AS gain,
                           AVG(rf_power_eirp_dbm) AS eirp, AVG(e_tilt_degree) AS tilt,
                           AVG(crs_gain) AS crs, AVG(antenna_height_agl_m) AS height
                    FROM telecom_sites WHERE site_id=:sid {cell_rf_filter}""", {"sid": t.site_id})
        if r: rf = r[0]
    except: pass
    bw = _f(rf.get("bw"), 1); tilt = _f(rf.get("tilt"), 1); eirp = _f(rf.get("eirp"), 1)
    crs = _f(rf.get("crs"), 2); height = _f(rf.get("height"), 1)
    rf_text = f"Bandwidth: {bw} MHz, E-tilt: {tilt}°, EIRP: {eirp} dBm, CRS Gain: {crs}, Antenna Height: {height} m"

    # ── AI prompt — Senior Telecom RAN Optimization Expert ───────────────────
    # Get site info for the prompt
    site_name_val = t.site_id
    try:
        from models import TelecomSite
        site_obj = TelecomSite.query.filter_by(site_id=t.site_id).first()
        if site_obj:
            site_name_val = site_obj.site_name or t.site_id
    except:
        pass

    # Build all RF params including extra_params
    rf_lines = [rf_text]
    try:
        if site_obj and site_obj.extra_params:
            for k, v in site_obj.extra_params.items():
                rf_lines.append(f"{k}: {v}")
    except:
        pass
    rf_full_text = "\n".join(rf_lines)

    prompt = f"""You are a SENIOR RAN OPTIMISATION ENGINEER on shift at the Ericsson / Nokia 24x7 NOC, with 20+ years of hands-on experience running predictive-analytics RCA on live LTE networks. You have personally led hundreds of worst-cell investigations end-to-end. THINK AND WRITE EXACTLY LIKE A HUMAN RAN EXPERT on the NOC shift handover call: reason from facts only, cite specific numbers from the data blocks below for every claim, never invent values, never speak in generalities. Your job is to replace a senior human engineer doing this RCA — work to that bar.

NOC PREDICTIVE-ANALYSIS METHODOLOGY (apply in this order):
  STEP A — Compare 7d vs 30d vs 60d KPI averages to classify degradation as sudden / gradual / chronic.
  STEP B — Cross-correlate the 3 failing KPIs (drop rate, CSSR, throughput) to identify the dominant chain driver (e.g. low SINR → drop AND retx AND throughput collapse, or PRB saturation → throughput drop AND access blocking).
  STEP C — Reconcile against the RF parameter snapshot: high EIRP + low E-tilt = overshoot/interference; very low CRS gain = poor edge SINR; insufficient bandwidth = scheduler PRB starvation; etc.
  STEP D — Output a single dominant root cause that can be fixed with an RF parameter change.

A cell has been flagged as WORST CELL — all 3 thresholds breached simultaneously on 7-day rolling average.

Site Name: {site_name_val}
Cell ID: {target if target != 'site' else 'All cells at ' + t.site_id}
Site Status: ON AIR
Zone: {t.zone}

THRESHOLD BREACH SUMMARY (7-Day Rolling Averages):
  E-RAB Call Drop Rate: {t.avg_drop_rate}% [Threshold: > 1.5%]
  Call Setup Success Rate: {t.avg_cssr}% [Threshold: < 98.5%]
  DL User Avg Throughput: {t.avg_tput} Mbps [Threshold: < 8 Mbps]

KPI TREND DATA (Daily — compare 7d vs 30d vs 60d averages, min, max):
{kpi_text}

SITE RF PARAMETER DATABASE (Live Values from Network):
{rf_full_text}

IMPORTANT FORMATTING RULES:
- Plain text only. No asterisks, no hash symbols, no dashes as separators, no bullet points, no bold markers.
- Write exactly 4 numbered points (1. 2. 3. 4.)
- Each point should be 3-4 clear sentences. Informative but easy to read.
- Start each point with a short title followed by a colon, then the explanation.

Write these 4 points:

1. KPI Trend Evidence: Compare 7d vs 30d vs 60d averages for Drop Rate, CSSR, and Throughput. Cite the actual numbers. State whether degradation is sudden, gradual, or chronic based on how the values compare across time windows.

2. Root Cause: Identify the single physical root cause driving all 3 KPI failures simultaneously. Use cross-KPI correlation (e.g., low user count + poor throughput = SINR issue not congestion). Cite the specific numbers that prove this diagnosis and explain the cause-effect chain.

3. RF Parameter Impact: Explain how the current RF configuration — Bandwidth {bw} MHz, E-tilt {tilt} deg, EIRP {eirp} dBm, Height {height} m, CRS Gain {crs} — contributes to the root cause. Reference actual values and explain the RF engineering impact of each.

4. Degradation Pattern: Classify as SUDDEN ONSET / GRADUAL DECAY / RECURRING / MIXED with evidence. Note any contributing factors like traffic spikes, PRB trends, or min/max volatility visible in the data."""

    root_cause = ""
    try:
        from app import client as ai_client, DEPLOYMENT_NAME
        if ai_client:
            resp = ai_client.chat.completions.create(
                model=DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2, max_tokens=4000,
            )
            root_cause = _clean_ai(resp.choices[0].message.content)
        else:
            root_cause = "AI client not configured. Please check OpenAI/Gemini configuration."
    except Exception as e:
        _LOG.error("RCA AI call failed for ticket %s: %s", ticket_id, e)
        root_cause = f"RCA generation failed: {str(e)}. Please retry."

    if target == "site":
        t.root_cause = root_cause
        t.updated_at = datetime.utcnow()
        db.session.commit()

    return jsonify({"root_cause": root_cause, "target": target})

@network_issues_bp.route("/api/network-issues/<int:ticket_id>/recommendations", methods=["POST"])
@jwt_required()
def network_issue_recommendations(ticket_id):
    """AI recommendations for a specific cell or site — in terms of RF parameters."""
    t = db.session.get(NetworkIssueTicket, ticket_id)
    if not t: return jsonify({"error": "Not found"}), 404

    target = (request.json or {}).get("target", "site")
    root_cause = (request.json or {}).get("root_cause", t.root_cause or "")

    # ── Dynamically discover EVERY numeric RF column in telecom_sites so the
    # AI gets the complete live-parameter picture from the operator's database,
    # not just a hardcoded subset. Includes: bandwidth_mhz, antenna_gain_dbi,
    # rf_power_eirp_dbm, e_tilt_degree, m_tilt_degree, antenna_height_agl_m,
    # crs_gain, azimuth_degree, pci_cell_id, frequency_band etc. + extra_params.
    from sqlalchemy import inspect as sa_inspect
    RF_SKIP = {"id", "latitude", "longitude", "created_at", "updated_at"}
    rf_live = {}
    try:
        _insp = sa_inspect(db.engine)
        cols = [c["name"] for c in _insp.get_columns("telecom_sites")]
        num_cols = [c for c in cols if c not in RF_SKIP and c not in ("site_id","site_name","cell_id","cell_site_id","site_abs_id","zone","city","state","country","province","commune","site_status","alarms","solution","standard_solution_step","vendor_name","technology","extra_params")]
        if num_cols:
            sel_parts = ", ".join([f"AVG({c}) AS {c}" for c in num_cols])
            q = f"SELECT {sel_parts} FROM telecom_sites WHERE site_id=:sid"
            r = _sql(q, {"sid": t.site_id})
            if r: rf_live = {k: v for k, v in r[0].items() if v is not None}
    except Exception as _e:
        _LOG.warning("RF param scan failed for %s: %s", t.site_id, _e)

    # Merge in extra_params (JSON flex store — holds vendor-specific params)
    try:
        from models import TelecomSite
        site_obj = TelecomSite.query.filter_by(site_id=t.site_id).first()
        if site_obj and site_obj.extra_params:
            for k, v in site_obj.extra_params.items():
                if v is not None and k not in rf_live:
                    rf_live[k] = v
        # Site-level context the AI should know about
        site_vendor = (site_obj.vendor_name or "") if site_obj else ""
        site_tech   = (site_obj.technology or "") if site_obj else ""
    except Exception:
        site_vendor = site_tech = ""

    def _fmt_val(v):
        try:
            fv = float(v)
            if fv == int(fv): return f"{int(fv)}"
            return f"{round(fv, 3)}"
        except (TypeError, ValueError):
            return str(v)

    rf_lines = [f"{k} = {_fmt_val(v)}" for k, v in sorted(rf_live.items())]
    rf_full = "\n".join(rf_lines) if rf_lines else "No RF parameters found in telecom_sites for this site_id."

    prompt = f"""You are a SENIOR RAN OPTIMISATION ENGINEER on shift at the {site_vendor or 'Ericsson / Nokia'} 24x7 NOC, with 20+ years of hands-on experience running predictive-analytics worst-cell investigations on live {site_tech or 'LTE'} networks. The predictive-analytics engine flagged this cell because its 7-day rolling averages breached all three production thresholds simultaneously (E-RAB Call Drop > 1.5%, CSSR < 98.5%, DL User Throughput < 8 Mbps). Your job is to read the live RF parameter snapshot from the operator's telecom_sites database, identify which Managed-Object attributes need to change, and produce a field-deployable change-order an O&M / drive-test team can execute today. THINK EXACTLY AS A HUMAN RAN EXPERT WOULD on the NOC shift handover — reason from facts only, quote actual DB values, never invent numbers, never speak in generalities.

SITE / TARGET: {'cell '+target if target!='site' else 'site '+t.site_id}
VENDOR: {site_vendor or 'Unknown'}     TECHNOLOGY: {site_tech or 'Unknown'}
Current KPIs (7-day rolling): E-RAB Drop Rate = {t.avg_drop_rate}%, CSSR = {t.avg_cssr}%, DL Throughput = {t.avg_tput} Mbps

ROOT CAUSE ANALYSIS FINDINGS (treat as authoritative input — every recommendation must cure something here):
{root_cause if root_cause else 'Compound multi-KPI degradation across all three thresholds.'}

COMPLETE RF PARAMETER DATABASE — every live value currently stored for this site (every column from telecom_sites including extra_params; treat each entry as a Managed-Object attribute on the {site_vendor or 'eNodeB'} — choose your fix FROM THIS LIST, never invent parameter names that are not present):
{rf_full}

NOC PREDICTIVE-ANALYSIS METHODOLOGY YOU FOLLOW (apply in this order):
  STEP 1 — Determine which of the 3 KPI failures is the dominant chain driver (e.g. low SINR causing drop AND retransmissions AND throughput collapse).
  STEP 2 — Map that KPI failure to the RF lever that fixes it. On Ericsson the relevant MOs are EUtranCellFDD / SectorCarrier / AntennaSubunit (digitalTilt, mechTilt, sectorCarrierRef, primaryDlPower, qRxLevMin, qOffsetFreq, cellIndividualOffset, csgPolicy). On Nokia these map to LNCEL / LNCEL_FDD / MOD (elTilt, maxTxPower, cellIndividualOffset, qRxLevMin, qHyst). Look up the equivalent attribute in the RF database above and use that exact name.
  STEP 3 — Quote the current DB value verbatim, propose a typical-NOC-step delta (e.g., +1° E-tilt, -1 dB EIRP, +3 dB CRS gain, -0.5 dB cellIndividualOffset) within 3GPP/vendor-safe bounds for {site_vendor or 'this vendor'} {site_tech or ''}.
  STEP 4 — State the expected KPI recovery in concrete numbers and the 48-72h verification window.

YOUR TASK (think like a senior {site_vendor or 'Ericsson / Nokia'} NOC RAN expert):
1. Scan the FULL RF parameter list above. Note exactly which parameters are stored — common ones include bandwidth_mhz, e_tilt_degree, m_tilt_degree, rf_power_eirp_dbm, antenna_gain_dbi, antenna_height_agl_m, crs_gain, azimuth_degree, pci_cell_id, frequency_band, plus any vendor-specific extra_params (qRxLevMin, cellIndividualOffset, primaryDlPower, etc.).
2. Pick the 2-3 parameters whose adjustment will MOST DIRECTLY address the root cause identified above.
3. For each parameter, state the EXACT previous value (quoted verbatim from the database snapshot) and the EXACT recommended new value, staying within 3GPP / {site_vendor or 'vendor'} {site_tech or ''} safe bounds.
4. Tie each change back to the RCA — show the cause→effect chain explaining HOW this parameter change moves which KPI (drop rate, CSSR, throughput) and by approximately how much within 48-72h.

FORMATTING RULES (strict):
- Plain text only. No asterisks, no hash symbols, no bullet points, no bold markers.
- Write exactly 3 numbered points (1. 2. 3.)
- Each point must start with a title followed by a colon, then 4-6 sentences.
- Each point MUST contain an explicit "Previous value: X → New value: Y" line.

1. [Most critical parameter] Change: Parameter name as stored in DB, previous value, new value, RF engineering rationale tying this change to the specific RCA item it cures, which KPIs it recovers and by how much, risk to neighbors.

2. [Second parameter] Change: Same structure — previous value, new value, RCA linkage, expected KPI recovery, rollback guidance if no improvement within 48 hours.

3. [Third parameter or Validation]: Either a third parameter change with the Previous→New format, OR a post-optimization validation step (drive test / 48-hour KPI window / neighbor cell impact check) that directly validates whether the RCA issue has been resolved."""

    recommendation = ""
    try:
        from app import client as ai_client, DEPLOYMENT_NAME
        if ai_client:
            resp = ai_client.chat.completions.create(
                model=DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2, max_tokens=4000,
            )
            recommendation = _clean_ai(resp.choices[0].message.content)
        else:
            recommendation = "AI client not configured. Please check OpenAI/Gemini configuration."
    except Exception as e:
        _LOG.error("Recommendation AI call failed for ticket %s: %s", ticket_id, e)
        recommendation = f"Recommendation generation failed: {str(e)}. Please retry."

    if target == "site":
        t.recommendation = recommendation
        t.updated_at = datetime.utcnow()
        db.session.commit()

    return jsonify({"recommendation": recommendation, "target": target})


@network_issues_bp.route("/api/network-issues/trigger-job", methods=["POST"])
@jwt_required()
def trigger_network_issue_job():
    """Manual trigger: refreshes worst-cell + overutilized pre-scan AND creates/updates tickets."""
    run_pre_scan()
    count = run_daily_network_issue_job()
    ou_count = run_overutilized_job()
    return jsonify({"success": True, "message": f"Worst cells: {count}, Overutilized: {ou_count} tickets processed.", "tickets_processed": count + ou_count})


@network_issues_bp.route("/api/network-issues/stats", methods=["GET"])
@jwt_required()
def network_issue_stats():
    try:
        total = NetworkIssueTicket.query.count()
        open_c = NetworkIssueTicket.query.filter_by(status="open").count()
        ip = NetworkIssueTicket.query.filter_by(status="in_progress").count()
        res = NetworkIssueTicket.query.filter_by(status="resolved").count()
        crit = NetworkIssueTicket.query.filter_by(priority="Critical").count()
        sev = NetworkIssueTicket.query.filter_by(category="Severe Worst").count()
    except:
        total = open_c = ip = res = crit = sev = 0
    return jsonify({"total": total, "open": open_c, "in_progress": ip, "resolved": res, "critical": crit, "severe_worst": sev})


# ─────────────────────────────────────────────────────────────────────────────
# Parameter Change for Network Issues
# ─────────────────────────────────────────────────────────────────────────────

@network_issues_bp.route("/api/network-issues/<int:ticket_id>/parameter-change", methods=["GET"])
@jwt_required()
def get_network_param_changes(ticket_id):
    """Get latest parameter change + CR for a network issue ticket."""
    change = None
    cr = None
    try:
        rows = _sql("""SELECT * FROM parameter_changes
            WHERE network_issue_id = :tid ORDER BY created_at DESC LIMIT 1""", {"tid": ticket_id})
        if rows:
            change = rows[0]
            # Get linked CR
            cr_rows = _sql("""SELECT * FROM change_requests
                WHERE parameter_change_id = :pcid ORDER BY created_at DESC LIMIT 1""",
                {"pcid": change["id"]})
            if cr_rows:
                cr = cr_rows[0]
    except Exception as e:
        _LOG.error("get_network_param_changes: %s", e)
    return jsonify({"change": change, "cr": cr})


@network_issues_bp.route("/api/network-issues/<int:ticket_id>/parameter-change", methods=["POST"])
@jwt_required()
def create_network_param_change(ticket_id):
    """Create parameter change request with full ITIL workflow — same as customer tickets.
    Creates ParameterChange + ChangeRequest, routes to best manager.
    Approval deadline = 30% of remaining SLA time."""
    user_id = int(get_jwt_identity())
    user = db.session.get(User, user_id)
    if not user or user.role not in ("human_agent", "admin", "manager", "cto"):
        return jsonify({"error": "Unauthorized"}), 403

    t = db.session.get(NetworkIssueTicket, ticket_id)
    if not t:
        return jsonify({"error": "Ticket not found"}), 404

    data = request.json or {}
    proposed = (data.get("proposed_change") or "").strip()
    impact = (data.get("impact_assessment") or "").strip()
    rollback = (data.get("rollback_plan") or "").strip()
    if not proposed:
        return jsonify({"error": "proposed_change required"}), 400

    # Check for existing pending
    existing = _sql("SELECT id FROM parameter_changes WHERE network_issue_id=:nid AND agent_id=:aid AND status='pending' LIMIT 1",
                     {"nid": ticket_id, "aid": user_id})
    if existing:
        return jsonify({"error": "A pending change request already exists"}), 409

    # Approval deadline = 30% of remaining SLA
    now = datetime.utcnow()
    approval_deadline = None
    if t.deadline_time:
        remaining = (t.deadline_time - now).total_seconds()
        approval_window = max(remaining * 0.3, 1800)
        approval_deadline = now + timedelta(seconds=approval_window)

    # Find best manager
    try:
        from app import _find_best_manager, _generate_cr_number
        from models import ParameterChange, ChangeRequest
        manager = _find_best_manager(t.priority.lower() if t.priority else "low")
    except Exception as e:
        _LOG.warning("Manager routing failed: %s", e)
        manager = None

    # Create ParameterChange
    change = ParameterChange(
        ticket_id=None,
        network_issue_id=ticket_id,
        agent_id=user_id,
        proposed_change=proposed,
        status="pending",
    )
    try:
        change.approval_deadline = approval_deadline
    except:
        pass
    db.session.add(change)
    db.session.flush()

    # Create ChangeRequest (ITIL workflow)
    cr_title = f"Network Parameter Change: {t.site_id} — {t.category}"
    cr = ChangeRequest(
        cr_number=_generate_cr_number(),
        ticket_id=None,
        parameter_change_id=change.id,
        raised_by=user_id,
        title=cr_title,
        description=proposed,
        impact_assessment=impact,
        rollback_plan=rollback,
        status="created",
    )
    db.session.add(cr)

    # Update ticket status
    t.status = "in_progress"
    t.updated_at = now

    db.session.commit()

    return jsonify({
        "change": change.to_dict(),
        "cr": cr.to_dict(),
        "assigned_manager": {"id": manager.id, "name": manager.name, "email": manager.email} if manager else None,
        "approval_deadline": approval_deadline.isoformat() if approval_deadline else None,
    }), 201


# ═══════════════════════════════════════════════════════════════════════════════
# OVERUTILIZED SITES — Auto-ticketing for sites with PRB Util > 92% (7-day avg)
# ═══════════════════════════════════════════════════════════════════════════════

class OverutilizedTicket(db.Model):
    """Clubbed ticket for overutilized sites — groups multiple sites by zone/cluster."""
    __tablename__ = "overutilized_tickets"

    id              = db.Column(db.Integer, primary_key=True)
    site_id         = db.Column(db.String(100), nullable=False, index=True)  # primary site or group key
    sites_list      = db.Column(db.Text, default="")          # JSON: list of {site_id, avg_prb, avg_tput, avg_rrc, ...}
    site_count      = db.Column(db.Integer, default=1)
    cells_affected  = db.Column(db.Text, default="")
    cell_site_ids   = db.Column(db.Text, default="")
    category        = db.Column(db.String(30), default="Overutilized")
    priority        = db.Column(db.String(20), default="Medium")
    priority_score  = db.Column(db.Float, default=0)
    sla_hours       = db.Column(db.Float, default=16)
    avg_rrc         = db.Column(db.Float, default=0)
    max_rrc         = db.Column(db.Float, default=0)
    revenue_total   = db.Column(db.Float, default=0)
    avg_prb_util    = db.Column(db.Float, default=0)
    avg_dl_tput     = db.Column(db.Float, default=0)
    avg_ul_prb      = db.Column(db.Float, default=0)
    avg_drop_rate   = db.Column(db.Float, default=0)
    violations      = db.Column(db.Integer, default=0)
    status          = db.Column(db.String(30), default="open")
    assigned_agent  = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    root_cause      = db.Column(db.Text, default="")
    recommendation  = db.Column(db.Text, default="")
    zone            = db.Column(db.String(100), default="")
    location        = db.Column(db.String(200), default="")
    created_at      = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at      = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deadline_time   = db.Column(db.DateTime, nullable=True)
    # Per-site resolution tracking: JSON {"site_id": "resolved"|"open"}
    site_status     = db.Column(db.Text, default="")


_PRB_KPI = "DL PRB Utilization (1BH)"
_PRB_THRESHOLD = 92.0


def _get_overutilized_sites():
    """Find sites where last-7-days (from today) avg DL PRB Utilization > 92%."""
    today = _date.today()
    start_date = today - timedelta(days=7)
    # Resolve PRB KPI name against actual DB values
    _, _, _, prb_kpi_resolved = _resolve_core_kpis()
    global _PRB_KPI
    _PRB_KPI = prb_kpi_resolved
    print(f"[OVERUTILIZED] Scanning from {start_date} to {today}, threshold > {_PRB_THRESHOLD}%, kpi='{_PRB_KPI}'")

    try:
        # Step 1: Find all sites with PRB > threshold.
        # Prefers site-level averaging; for sites that have ONLY cell-level uploads,
        # computes the per-site average across all cells so they're still detected.
        prb_rows = _sql("""
            WITH per_level AS (
                SELECT site_id, data_level,
                       AVG(value) AS avg_prb,
                       MAX(value) AS max_prb
                FROM kpi_data
                WHERE kpi_name = :prb AND value IS NOT NULL
                  AND date >= :start AND date <= :end
                GROUP BY site_id, data_level
            ),
            picked AS (
                SELECT site_id, avg_prb, max_prb FROM per_level WHERE data_level='site'
                UNION ALL
                SELECT p.site_id, p.avg_prb, p.max_prb FROM per_level p
                WHERE p.data_level='cell'
                  AND NOT EXISTS (
                      SELECT 1 FROM per_level s
                      WHERE s.site_id = p.site_id AND s.data_level='site'
                  )
            )
            SELECT site_id, avg_prb, max_prb FROM picked
            WHERE avg_prb > :threshold
            ORDER BY avg_prb DESC
        """, {"prb": _PRB_KPI, "start": start_date, "end": today, "threshold": _PRB_THRESHOLD})
        print(f"[OVERUTILIZED] Found {len(prb_rows)} sites with PRB > {_PRB_THRESHOLD}%")
    except Exception as e:
        print(f"[OVERUTILIZED] PRB query failed: {e}")
        _LOG.error("Overutilized PRB query failed: %s", e)
        return {}

    if not prb_rows:
        return {}

    # Step 2: For each site, get zone + other KPIs.
    # Prefers site-level; if a KPI has no site-level rows for this site, falls back
    # to the averaged cell-level value for that KPI.
    rows = []
    for pr in prb_rows:
        sid = pr["site_id"]
        try:
            extra = _sql("""
                WITH base AS (
                    SELECT k.kpi_name, k.data_level, AVG(k.value) AS v
                    FROM kpi_data k
                    WHERE k.site_id = :sid AND k.value IS NOT NULL
                      AND k.date >= :start AND k.date <= :end
                      AND k.kpi_name IN ('LTE DL - Usr Ave Throughput','E-RAB Call Drop Rate_1','Ave RRC Connected Ue','Max RRC Connected Ue')
                    GROUP BY k.kpi_name, k.data_level
                ),
                picked AS (
                    SELECT kpi_name, v FROM base WHERE data_level='site'
                    UNION ALL
                    SELECT kpi_name, v FROM base b
                    WHERE b.data_level='cell'
                      AND NOT EXISTS (SELECT 1 FROM base s WHERE s.kpi_name=b.kpi_name AND s.data_level='site')
                )
                SELECT
                    (SELECT zone  FROM telecom_sites WHERE site_id=:sid LIMIT 1) AS zone,
                    (SELECT city  FROM telecom_sites WHERE site_id=:sid LIMIT 1) AS city,
                    (SELECT state FROM telecom_sites WHERE site_id=:sid LIMIT 1) AS state,
                    (SELECT v FROM picked WHERE kpi_name='LTE DL - Usr Ave Throughput' LIMIT 1) AS avg_tput,
                    (SELECT v FROM picked WHERE kpi_name='E-RAB Call Drop Rate_1' LIMIT 1)       AS avg_drop,
                    (SELECT v FROM picked WHERE kpi_name='Ave RRC Connected Ue' LIMIT 1)         AS avg_rrc,
                    (SELECT v FROM picked WHERE kpi_name='Max RRC Connected Ue' LIMIT 1)         AS max_rrc
            """, {"sid": sid, "start": start_date, "end": today})
            r = extra[0] if extra else {}
            rows.append({
                "site_id": sid,
                "avg_prb": float(pr["avg_prb"] or 0),
                "max_prb": float(pr["max_prb"] or 0),
                "zone": r.get("zone") or "", "city": r.get("city") or "", "state": r.get("state") or "",
                "avg_tput": float(r.get("avg_tput") or 0),
                "avg_drop": float(r.get("avg_drop") or 0),
                "avg_rrc": float(r.get("avg_rrc") or 0),
                "max_rrc": float(r.get("max_rrc") or 0),
            })
        except Exception:
            rows.append({"site_id": sid, "avg_prb": float(pr["avg_prb"]), "max_prb": float(pr["max_prb"]),
                         "zone": "", "city": "", "state": "", "avg_tput": 0, "avg_drop": 0, "avg_rrc": 0, "max_rrc": 0})

    result = {}
    for r in rows:
        sid = r["site_id"]
        result[sid] = {
            "zone": r.get("zone") or "", "city": r.get("city") or "", "state": r.get("state") or "",
            "avg_prb": _f(r.get("avg_prb"), 2), "max_prb": _f(r.get("max_prb"), 2),
            "avg_tput": _f(r.get("avg_tput"), 2), "avg_drop": _f(r.get("avg_drop"), 2),
            "avg_rrc": _f(r.get("avg_rrc"), 1), "max_rrc": _f(r.get("max_rrc"), 1),
        }
    _LOG.info("Overutilized sites: %d sites with PRB > %.0f%%", len(result), _PRB_THRESHOLD)
    return result


def run_overutilized_job():
    """Create clubbed overutilized tickets — equal distribution of sites across tickets.

    Algorithm:
    1. Find all sites with 7-day avg DL PRB > 92%
    2. Sort by PRB (worst first)
    3. Split into equal groups of ~5 sites each (clubbed tickets)
    4. If today's sites match existing ticket's sites → UPDATE
    5. If sites changed → close old, create new
    6. Route each clubbed ticket to an available agent
    """
    import json as _json
    SITES_PER_TICKET = 5  # each clubbed ticket holds this many sites

    sites_dict = _get_overutilized_sites()
    if not sites_dict:
        print("[OVERUTILIZED] No overutilized sites found.")
        return 0

    # Build enriched site list sorted by PRB (worst first)
    all_sites = []
    for site_id, data in sites_dict.items():
        _, _, revenue, _, _ = _get_site_rrc_and_revenue(site_id)
        all_sites.append({
            "site_id": site_id,
            "avg_prb": data["avg_prb"], "max_prb": data.get("max_prb", 0),
            "avg_tput": data["avg_tput"], "avg_drop": data["avg_drop"],
            "avg_rrc": data["avg_rrc"], "max_rrc": data["max_rrc"],
            "revenue": _f(revenue, 1),
            "zone": data.get("zone", ""), "city": data.get("city", ""), "state": data.get("state", ""),
        })
    all_sites.sort(key=lambda s: -s["avg_prb"])

    # Split into equal groups
    groups = []
    for i in range(0, len(all_sites), SITES_PER_TICKET):
        groups.append(all_sites[i:i+SITES_PER_TICKET])
    print(f"[OVERUTILIZED] {len(all_sites)} sites -> {len(groups)} clubbed tickets ({SITES_PER_TICKET} per ticket)")

    today_08 = datetime.combine(_date.today(), datetime.min.time()).replace(hour=8)

    # Get existing open tickets
    existing_tickets = OverutilizedTicket.query.filter(
        OverutilizedTicket.status.in_(["open", "in_progress"])
    ).order_by(OverutilizedTicket.id).all()

    # (Agent routing is now per-group via _pick_agent_for_site so each clubbed
    # ticket goes to the best-matching agent for its primary site.)

    created = updated = 0
    for gi, group_sites in enumerate(groups):
        n = len(group_sites)
        worst_prb = max(s["avg_prb"] for s in group_sites)
        avg_prb = sum(s["avg_prb"] for s in group_sites) / n
        avg_tput = sum(s["avg_tput"] for s in group_sites) / n
        avg_drop = sum(s["avg_drop"] for s in group_sites) / n
        total_rrc = sum(s["avg_rrc"] for s in group_sites)
        max_rrc = max(s["max_rrc"] for s in group_sites)
        total_rev = sum(s["revenue"] for s in group_sites)
        primary_site = group_sites[0]["site_id"]
        zone = group_sites[0].get("zone") or f"Group-{gi+1}"
        location = group_sites[0].get("city") or zone

        score = 5
        if total_rev >= 100: score += 2
        elif total_rev >= 50: score += 1
        if total_rrc >= 1000: score += 2
        elif total_rrc >= 300: score += 1
        if worst_prb >= 97: score += 2
        elif worst_prb >= 95: score += 1
        if n >= 5: score += 1

        # SLA spec: Critical=2h, High=4h, Medium=8h, Low=16h
        if score >= 10: priority, sla = "Critical", 2
        elif score >= 7: priority, sla = "High", 4
        elif score >= 5: priority, sla = "Medium", 8
        else: priority, sla = "Low", 16

        sites_json = _json.dumps(group_sites)
        today_site_ids = set(s["site_id"] for s in group_sites)
        site_status_json = _json.dumps({s["site_id"]: "open" for s in group_sites})

        # Try to match with existing ticket (same sites)
        matched = None
        for ex in existing_tickets:
            try:
                ex_sites = set(s["site_id"] for s in _json.loads(ex.sites_list or "[]"))
            except: ex_sites = set()
            if ex_sites == today_site_ids:
                matched = ex; break

        if matched:
            # Agent stays the same (load balance preserved); only metrics refresh.
            matched.sites_list = sites_json; matched.site_count = n
            matched.avg_prb_util = _f(avg_prb, 2); matched.avg_dl_tput = _f(avg_tput, 2)
            matched.avg_drop_rate = _f(avg_drop, 2); matched.avg_rrc = _f(total_rrc, 1)
            matched.max_rrc = _f(max_rrc, 1); matched.revenue_total = _f(total_rev, 1)
            matched.priority = priority; matched.priority_score = score; matched.sla_hours = sla
            matched.zone = zone; matched.location = location; matched.updated_at = today_08
            matched.violations = n
            existing_tickets.remove(matched)
            updated += 1
            # Resolve agent name for logging
            agent_name = "unassigned"
            try:
                if matched.assigned_agent:
                    _ag = db.session.get(User, matched.assigned_agent)
                    if _ag: agent_name = _ag.name or _ag.email
            except Exception: pass
            site_list_str = ",".join(s["site_id"] for s in group_sites)
            print(f"  [OVERUTIL UPDATE] Ticket #{matched.id}  sites=[{site_list_str}]  PRB={avg_prb:.1f}%  priority={priority}  SLA={sla}h  agent={agent_name}")
            _TODAYS_ROUTING_LOG.append({
                "type": "overutilized", "action": "update", "ticket_id": matched.id,
                "sites": [s["site_id"] for s in group_sites], "priority": priority,
                "sla_hours": sla, "agent": agent_name, "zone": zone, "timestamp": today_08.isoformat(),
            })
        else:
            agent = _pick_agent_for_site(zone, location, group_sites[0].get("state",""), domain="RAN")
            t = OverutilizedTicket(
                site_id=primary_site, sites_list=sites_json, site_count=n,
                cells_affected=",".join(s["site_id"] for s in group_sites),
                category="Overutilized", priority=priority, priority_score=score, sla_hours=sla,
                avg_rrc=_f(total_rrc, 1), max_rrc=_f(max_rrc, 1), revenue_total=_f(total_rev, 1),
                avg_prb_util=_f(avg_prb, 2), avg_dl_tput=_f(avg_tput, 2),
                avg_drop_rate=_f(avg_drop, 2), violations=n,
                status="open", assigned_agent=agent.id if agent else None,
                zone=zone, location=location, site_status=site_status_json,
                created_at=today_08, updated_at=today_08,
                deadline_time=today_08 + timedelta(hours=sla),
            )
            db.session.add(t)
            db.session.flush()  # get t.id for logging
            created += 1
            agent_name = agent.name if agent else "unassigned"
            site_list_str = ",".join(s["site_id"] for s in group_sites)
            print(f"  [OVERUTIL CREATE] Ticket #{t.id}  sites=[{site_list_str}]  PRB={avg_prb:.1f}%  priority={priority}  SLA={sla}h  → routed to: {agent_name}")
            _TODAYS_ROUTING_LOG.append({
                "type": "overutilized", "action": "create", "ticket_id": t.id,
                "sites": [s["site_id"] for s in group_sites], "priority": priority,
                "sla_hours": sla, "agent": agent_name, "zone": zone, "timestamp": today_08.isoformat(),
            })

    # Close old tickets that don't match any current group
    for old in existing_tickets:
        old.status = "closed"; old.updated_at = today_08

    db.session.commit()
    print(f"[OVERUTILIZED] Job complete: {created} created, {updated} updated, {len(existing_tickets)} closed")
    return created + updated


# ── Overutilized API Endpoints ──────────────────────────────────────────────

@network_issues_bp.route("/api/network-issues/overutilized/list", methods=["GET"])
@jwt_required()
def list_overutilized():
    user_id = int(get_jwt_identity())
    user = db.session.get(User, user_id)
    if not user or user.role not in ("human_agent", "admin", "manager", "cto"):
        return jsonify({"error": "Unauthorized"}), 403
    tickets = OverutilizedTicket.query.order_by(OverutilizedTicket.created_at.desc()).all()
    now = datetime.utcnow()

    # Pre-fetch site_abs_id / site_name for every site referenced by these tickets
    abs_map = {}
    try:
        import json as _json_preload
        all_site_ids = set()
        for t in tickets:
            if t.site_id: all_site_ids.add(t.site_id)
            try:
                for s in _json_preload.loads(t.sites_list or "[]"):
                    if s.get("site_id"): all_site_ids.add(s["site_id"])
            except Exception:
                pass
        if all_site_ids:
            from models import TelecomSite
            for s in TelecomSite.query.filter(TelecomSite.site_id.in_(all_site_ids)).all():
                if s.site_id not in abs_map:
                    abs_map[s.site_id] = {"abs": s.site_abs_id or "", "name": s.site_name or ""}
    except Exception:
        pass

    result = []
    for t in tickets:
        sla_remaining = round(max((t.deadline_time - now).total_seconds() / 3600, 0), 2) if t.deadline_time else 0
        sla_pct = round(min((t.sla_hours - max(sla_remaining, 0)) / max(t.sla_hours, 1) * 100, 100), 1) if t.sla_hours else 0
        agent_name = ""
        if t.assigned_agent:
            ag = db.session.get(User, t.assigned_agent)
            if ag: agent_name = ag.name
        import json as _json
        sites_data = []
        try: sites_data = _json.loads(t.sites_list) if t.sites_list else []
        except: pass
        # Enrich each site in the sites list with abs id
        for s in sites_data:
            meta = abs_map.get(s.get("site_id"), {})
            s["site_abs_id"] = meta.get("abs", "")
            s["site_name"] = meta.get("name", "")
        site_statuses = {}
        try: site_statuses = _json.loads(t.site_status) if t.site_status else {}
        except: pass
        primary_meta = abs_map.get(t.site_id, {})
        result.append({
            "id": t.id, "ticket_ref": f"OU-{t.id:04d}",
            "site_id": t.site_id,
            "site_abs_id": primary_meta.get("abs", ""),
            "site_name": primary_meta.get("name", ""),
            "category": t.category,
            "priority": t.priority, "priority_score": t.priority_score,
            "sla_hours": t.sla_hours, "sla_remaining": sla_remaining, "sla_pct": sla_pct,
            "sla_breached": sla_remaining <= 0 and t.status not in ("resolved", "closed"),
            "avg_rrc": t.avg_rrc, "max_rrc": t.max_rrc, "revenue_total": t.revenue_total,
            "avg_prb_util": t.avg_prb_util, "avg_dl_tput": t.avg_dl_tput,
            "avg_drop_rate": t.avg_drop_rate,
            "site_count": t.site_count or len(sites_data) or 1,
            "sites": sites_data,
            "site_statuses": site_statuses,
            "status": t.status, "assigned_agent": t.assigned_agent, "agent_name": agent_name,
            "zone": t.zone, "location": t.location,
            "is_mine": t.assigned_agent == user_id,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "updated_at": t.updated_at.isoformat() if t.updated_at else None,
            "deadline_time": t.deadline_time.isoformat() if t.deadline_time else None,
        })
    return jsonify({"tickets": result, "total": len(result)})


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>", methods=["GET"])
@jwt_required()
def get_overutilized(tid):
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404
    agent_name = ""
    if t.assigned_agent:
        ag = db.session.get(User, t.assigned_agent)
        if ag: agent_name = ag.name
    return jsonify({
        "id": t.id, "site_id": t.site_id, "category": t.category,
        "priority": t.priority, "priority_score": t.priority_score,
        "sla_hours": t.sla_hours, "avg_rrc": t.avg_rrc, "max_rrc": t.max_rrc,
        "revenue_total": t.revenue_total, "avg_prb_util": t.avg_prb_util,
        "avg_dl_tput": t.avg_dl_tput, "avg_drop_rate": t.avg_drop_rate,
        "status": t.status, "agent_name": agent_name, "zone": t.zone, "location": t.location,
        "root_cause": t.root_cause or "", "recommendation": t.recommendation or "",
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "deadline_time": t.deadline_time.isoformat() if t.deadline_time else None,
    })


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/status", methods=["PUT"])
@jwt_required()
def update_overutilized_status(tid):
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404
    new_status = (request.json or {}).get("status", "").strip()
    if new_status not in ("open", "in_progress", "resolved", "closed"):
        return jsonify({"error": "Invalid status"}), 400
    t.status = new_status; t.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify({"success": True, "status": new_status})


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/resolve-site", methods=["POST"])
@jwt_required()
def resolve_overutilized_site(tid):
    """Resolve a single site within a clubbed overutilized ticket.
    Once all sites are resolved, the ticket auto-resolves."""
    import json as _json
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404
    site_id = (request.json or {}).get("site_id", "").strip()
    if not site_id: return jsonify({"error": "site_id required"}), 400
    try:
        statuses = _json.loads(t.site_status) if t.site_status else {}
    except: statuses = {}
    statuses[site_id] = "resolved"
    t.site_status = _json.dumps(statuses)
    t.updated_at = datetime.utcnow()
    # Auto-resolve ticket if all sites are resolved
    all_resolved = all(v == "resolved" for v in statuses.values()) if statuses else False
    if all_resolved:
        t.status = "resolved"
    else:
        t.status = "in_progress"
    db.session.commit()
    return jsonify({"success": True, "site_id": site_id, "ticket_status": t.status, "all_resolved": all_resolved})


def _fetch_trends_for_site(site_id, kpis):
    """Fetch daily KPI trends for a single site. Returns {kpi_display_name: [{label, avg, min, max}, ...]}."""
    out = {}
    for kpi in kpis:
        try:
            rows = _sql("""
                SELECT k.date::text AS label, AVG(k.value) AS avg, MIN(k.value) AS min, MAX(k.value) AS max
                FROM kpi_data_merged k WHERE k.site_id=:sid AND k.kpi_name ILIKE :kpi
                  AND k.value IS NOT NULL AND k.date <= CURRENT_DATE
                GROUP BY k.date ORDER BY k.date
            """, {"sid": site_id, "kpi": kpi + '%'})
            display_name = kpi if kpi.endswith(')') else kpi + ')'
            out[display_name] = [{"label": r["label"], "avg": _f(r["avg"], 4), "min": _f(r["min"], 4), "max": _f(r["max"], 4)} for r in rows]
        except Exception:
            out[kpi] = []
    return out


def _overutilized_neighbors(target_site_id, n=3):
    """Return n nearest telecom sites to target_site_id with 7-day KPIs and RF params.

    Used by overutilized ticket endpoints for load-balancing analysis.
    """
    from models import TelecomSite
    site_obj = TelecomSite.query.filter_by(site_id=target_site_id).first()
    if not site_obj or site_obj.latitude is None or site_obj.longitude is None:
        return []
    try:
        from app import find_nearest_sites
        raw = find_nearest_sites(site_obj.latitude, site_obj.longitude, n=n + 1)
    except Exception:
        return []
    neighbors = []
    for nb in raw:
        if nb.get("site_id") == target_site_id:
            continue
        nb_prb = nb_rrc = nb_tput = nb_drop = 0
        try:
            pr = _sql("""SELECT AVG(CASE WHEN kpi_name=:prb THEN value END) AS prb,
                                AVG(CASE WHEN kpi_name='Ave RRC Connected Ue' THEN value END) AS rrc,
                                AVG(CASE WHEN kpi_name='LTE DL - Usr Ave Throughput' THEN value END) AS tput,
                                AVG(CASE WHEN kpi_name='E-RAB Call Drop Rate_1' THEN value END) AS drop
                         FROM kpi_data_merged WHERE site_id=:sid AND value IS NOT NULL
                           AND date >= CURRENT_DATE - INTERVAL '7 days'""",
                      {"sid": nb["site_id"], "prb": _PRB_KPI})
            if pr:
                nb_prb = _f(pr[0].get("prb"), 1)
                nb_rrc = _f(pr[0].get("rrc"), 0)
                nb_tput = _f(pr[0].get("tput"), 1)
                nb_drop = _f(pr[0].get("drop"), 2)
        except Exception:
            pass
        spare = round(max(92 - nb_prb, 0), 1)
        neighbors.append({
            "site_id": nb.get("site_id"),
            "site_name": nb.get("site_name") or nb.get("site_id"),
            "latitude": nb.get("latitude"),
            "longitude": nb.get("longitude"),
            "distance_km": nb.get("distance_km", 0),
            "zone": nb.get("zone", ""),
            "city": nb.get("city", ""),
            "site_status": nb.get("site_status", "on_air"),
            "avg_prb": nb_prb,
            "avg_rrc": nb_rrc,
            "avg_tput": nb_tput,
            "avg_drop": nb_drop,
            "spare_capacity_pct": spare,
            "bandwidth_mhz": nb.get("bandwidth_mhz"),
            "antenna_gain_dbi": nb.get("antenna_gain_dbi"),
            "rf_power_eirp_dbm": nb.get("rf_power_eirp_dbm"),
            "antenna_height_agl_m": nb.get("antenna_height_agl_m"),
            "e_tilt_degree": nb.get("e_tilt_degree"),
            "crs_gain": nb.get("crs_gain"),
        })
        if len(neighbors) >= n:
            break
    return neighbors


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/nearest-sites", methods=["GET"])
@jwt_required()
def overutilized_nearest_sites(tid):
    """Return 3 nearest telecom sites to the target site with KPIs for load-balancing analysis."""
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404
    target_site = request.args.get("site_id", t.site_id).strip()
    from models import TelecomSite
    site_obj = TelecomSite.query.filter_by(site_id=target_site).first()
    target_info = None
    if site_obj:
        target_info = {
            "site_id": site_obj.site_id,
            "site_name": site_obj.site_name or site_obj.site_id,
            "latitude": site_obj.latitude,
            "longitude": site_obj.longitude,
            "zone": site_obj.zone or "",
            "city": site_obj.city or "",
            "avg_prb": t.avg_prb_util,
            "avg_rrc": t.avg_rrc,
            "avg_tput": t.avg_dl_tput,
        }
    neighbors = _overutilized_neighbors(target_site, n=3)
    return jsonify({"target": target_info, "neighbors": neighbors})


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/trends", methods=["GET"])
@jwt_required()
def overutilized_trends(tid):
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404
    # Allow querying specific site within the clubbed ticket
    target_site = request.args.get("site_id", t.site_id).strip()
    # ?target=<site_id> alias supported for compatibility with modal UI
    target_alias = request.args.get("target", "").strip()
    if target_alias and target_alias != "site":
        target_site = target_alias
    include_neighbors = request.args.get("include_neighbors", "1") != "0"

    kpis = ['DL PRB Utilization (1BH)', 'UL PRB Utilization (1BH', 'LTE DL - Usr Ave Throughput',
            'Ave RRC Connected Ue', 'E-RAB Call Drop Rate_1', 'LTE Call Setup Success Rate']

    result = _fetch_trends_for_site(target_site, kpis)

    neighbor_trends = {}
    neighbors_meta = []
    if include_neighbors:
        neighbors_meta = _overutilized_neighbors(target_site, n=3)
        for nb in neighbors_meta:
            neighbor_trends[nb["site_id"]] = _fetch_trends_for_site(nb["site_id"], kpis)

    return jsonify({
        "site_id": target_site,
        "trends": result,
        "neighbors": neighbors_meta,
        "neighbor_trends": neighbor_trends,
    })


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/rca", methods=["POST"])
@jwt_required()
def overutilized_rca(tid):
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404

    # Resolve which site within the clubbed ticket the agent is analysing.
    # Frontend may send target='site' (literal — meaning "the ticket's primary
    # site") or target=<actual_site_id> when iterating individual sites.
    body = request.json or {}
    raw_target = (body.get("target") or "").strip()
    explicit_site = (body.get("site_id") or "").strip()
    if explicit_site:
        target_site_id = explicit_site
    elif raw_target and raw_target.lower() not in ("site", "primary"):
        target_site_id = raw_target
    else:
        target_site_id = t.site_id

    # Resolve site lat/lng + city/zone from telecom_sites for context (and for
    # the new-site placement recommendation when no offload is feasible).
    target_lat = target_lng = None
    target_zone = t.zone or ""
    target_city = t.location or ""
    try:
        from models import TelecomSite
        _site_obj = TelecomSite.query.filter_by(site_id=target_site_id).first()
        if _site_obj:
            target_lat = _site_obj.latitude
            target_lng = _site_obj.longitude
            target_zone = _site_obj.zone or target_zone
            target_city = _site_obj.city or target_city
    except Exception:
        pass

    # Gather KPI data — prefer site-level; use cell-level (averaged) for cell-only sites.
    kpi_text = ""
    for period_name, days in [("7 days", 7), ("30 days", 30), ("60 days", 60)]:
        try:
            rows = _sql(f"""
                WITH s AS (
                    SELECT kpi_name, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max
                    FROM kpi_data
                    WHERE site_id=:sid AND data_level='site' AND value IS NOT NULL
                      AND kpi_name IN ('DL PRB Utilization (1BH)','UL PRB Utilization (1BH)','LTE DL - Usr Ave Throughput','Ave RRC Connected Ue','Max RRC Connected Ue','E-RAB Call Drop Rate_1','LTE Call Setup Success Rate')
                      AND date >= CURRENT_DATE - INTERVAL '{days} days' AND date <= CURRENT_DATE
                    GROUP BY kpi_name
                ),
                c AS (
                    SELECT kpi_name, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max
                    FROM kpi_data
                    WHERE site_id=:sid AND data_level='cell' AND value IS NOT NULL
                      AND kpi_name IN ('DL PRB Utilization (1BH)','UL PRB Utilization (1BH)','LTE DL - Usr Ave Throughput','Ave RRC Connected Ue','Max RRC Connected Ue','E-RAB Call Drop Rate_1','LTE Call Setup Success Rate')
                      AND date >= CURRENT_DATE - INTERVAL '{days} days' AND date <= CURRENT_DATE
                    GROUP BY kpi_name
                )
                SELECT kpi_name, avg, min, max FROM s
                UNION ALL
                SELECT c.kpi_name, c.avg, c.min, c.max FROM c
                WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.kpi_name = c.kpi_name)
            """, {"sid": target_site_id})
            kpi_text += f"\n=== Last {period_name} ===\n"
            for r in rows:
                kpi_text += f"  {r['kpi_name']}: avg={_f(r['avg'],2)}, min={_f(r['min'],2)}, max={_f(r['max'],2)}\n"
        except: pass

    # RF parameters — full dynamic scan of every numeric column in telecom_sites
    from sqlalchemy import inspect as sa_inspect
    RF_SKIP = {"id", "latitude", "longitude", "created_at", "updated_at"}
    TEXT_SKIP = {"site_id","site_name","cell_id","cell_site_id","site_abs_id","zone",
                 "city","state","country","province","commune","site_status","alarms",
                 "solution","standard_solution_step","vendor_name","technology","extra_params"}
    rf_live = {}
    try:
        _insp = sa_inspect(db.engine)
        cols = [c["name"] for c in _insp.get_columns("telecom_sites")]
        num_cols = [c for c in cols if c not in RF_SKIP and c not in TEXT_SKIP]
        if num_cols:
            sel_parts = ", ".join([f"AVG({c}) AS {c}" for c in num_cols])
            rrow = _sql(f"SELECT {sel_parts} FROM telecom_sites WHERE site_id=:sid", {"sid": target_site_id})
            if rrow: rf_live = {k: v for k, v in rrow[0].items() if v is not None}
        try:
            from models import TelecomSite as _TS
            _so = _TS.query.filter_by(site_id=target_site_id).first()
            if _so and _so.extra_params:
                for k, v in _so.extra_params.items():
                    if v is not None and k not in rf_live: rf_live[k] = v
        except Exception: pass
    except Exception: pass

    def _fmt_val_rca(v):
        try:
            fv = float(v)
            return f"{int(fv)}" if fv == int(fv) else f"{round(fv, 3)}"
        except (TypeError, ValueError):
            return str(v)

    rf_lines_full = [f"  {k} = {_fmt_val_rca(v)}" for k, v in sorted(rf_live.items())]
    rf_full_text = "\n".join(rf_lines_full) if rf_lines_full else "  (no RF parameters stored)"
    bw = _fmt_val_rca(rf_live.get("bandwidth_mhz")) if rf_live.get("bandwidth_mhz") is not None else "?"
    tilt = _fmt_val_rca(rf_live.get("e_tilt_degree")) if rf_live.get("e_tilt_degree") is not None else "?"
    eirp = _fmt_val_rca(rf_live.get("rf_power_eirp_dbm")) if rf_live.get("rf_power_eirp_dbm") is not None else "?"
    crs = _fmt_val_rca(rf_live.get("crs_gain")) if rf_live.get("crs_gain") is not None else "?"
    height = _fmt_val_rca(rf_live.get("antenna_height_agl_m")) if rf_live.get("antenna_height_agl_m") is not None else "?"

    # Revenue and RRC for the *selected* site (not the clubbed ticket primary)
    avg_rrc, max_rrc, revenue, _, _ = _get_site_rrc_and_revenue(target_site_id)
    # Pull selected-site PRB/Tput/Drop direct from KPI data so the AI sees that
    # site's actual numbers (the clubbed ticket fields t.avg_prb_util etc are
    # AVERAGES across all 5 sites, not the selected site).
    site_prb = site_tput = site_drop = None
    try:
        rr = _sql("""
            SELECT AVG(CASE WHEN kpi_name=:prb THEN value END) AS prb,
                   AVG(CASE WHEN kpi_name='LTE DL - Usr Ave Throughput' THEN value END) AS tput,
                   AVG(CASE WHEN kpi_name='E-RAB Call Drop Rate_1' THEN value END) AS drop
            FROM kpi_data_merged WHERE site_id=:sid AND value IS NOT NULL
              AND date >= CURRENT_DATE - INTERVAL '7 days'
        """, {"sid": target_site_id, "prb": _PRB_KPI})
        if rr:
            site_prb = _f(rr[0].get("prb"), 1)
            site_tput = _f(rr[0].get("tput"), 1)
            site_drop = _f(rr[0].get("drop"), 2)
    except Exception:
        pass
    if site_prb is None: site_prb = t.avg_prb_util
    if site_tput is None: site_tput = t.avg_dl_tput
    if site_drop is None: site_drop = t.avg_drop_rate

    # Fetch nearest 3 sites with PRB, RRC, and RF params for load balancing analysis
    neighbors = _overutilized_neighbors(target_site_id, n=3)
    neighbor_text = ""
    if neighbors:
        neighbor_text = "\nNEAREST 3 NEIGHBOUR SITES (live DB values, last 7-day averages — use for load-balancing feasibility):\n"
        for nb in neighbors:
            neighbor_text += (f"  {nb['site_id']} ({nb.get('site_name','')}): Distance={nb.get('distance_km',0)}km, "
                              f"Status={nb.get('site_status','on_air')}, "
                              f"PRB={nb.get('avg_prb',0)}% (spare={nb.get('spare_capacity_pct',0)}%), "
                              f"RRC={nb.get('avg_rrc',0)} users, Tput={nb.get('avg_tput',0)}Mbps, "
                              f"Drop={nb.get('avg_drop',0)}%, BW={nb.get('bandwidth_mhz','?')}MHz, "
                              f"E-tilt={nb.get('e_tilt_degree','?')}deg, EIRP={nb.get('rf_power_eirp_dbm','?')}dBm, "
                              f"CRS={nb.get('crs_gain','?')}, Height={nb.get('antenna_height_agl_m','?')}m, "
                              f"lat={nb.get('latitude','?')}, lng={nb.get('longitude','?')}\n")
    else:
        neighbor_text = ("\nNEAREST 3 NEIGHBOUR SITES: lookup returned no results — either lat/lng "
                         f"is missing for {target_site_id} or no other sites are stored. State this "
                         "explicitly and recommend a new-site build at the overutilized site's coordinates.\n")

    prompt = f"""You are a SENIOR RAN OPTIMISATION EXECUTIVE running the Ericsson / Nokia 24x7 NOC predictive-analytics shift. You have 20+ years of live network experience: capacity audits, sector-split rollouts, dense-urban re-homing, green-field site planning. You will reason and write EXACTLY like a human RAN expert presenting on a shift-handover call — terse, fact-anchored, decision-led. NEVER invent values. NEVER speak in generalities. EVERY claim must cite a specific number from the data blocks below.

CRITICAL CONSTRAINT — LOAD-BALANCING SCOPE:
  The 3 nearest neighbour sites listed below ARE the only neighbours for this overutilized site. Treat them as the complete offload candidate pool. Do NOT mention or assume any other neighbour. All load-balancing decisions, RF re-aim suggestions, and capacity verdicts MUST be derived using ONLY these 3 sites and their RF parameters listed below.

NOC PREDICTIVE-ANALYSIS METHODOLOGY (apply in order):
  STEP A — Confirm capacity exhaustion: compare {target_site_id}'s 7d / 30d / 60d PRB and throughput from the KPI trend block.
  STEP B — Cross-correlate PRB with RRC users, drop rate and throughput to confirm the breach is congestion-driven, not hardware or coverage.
  STEP C — Read this site's RF parameter snapshot for contributing factors (low E-tilt overshoot, high EIRP concentrating users, low CRS, narrow bandwidth).
  STEP D — Walk through EACH of the 3 listed neighbours by site_id, comparing their PRB, spare capacity, RRC, throughput AND RF parameters (E-tilt, EIRP, BW, CRS) against {target_site_id}.
  STEP E — Pick a SINGLE dominant root cause that maps to a concrete RF parameter on {target_site_id} OR to a "no neighbour can absorb" verdict that mandates a new site / carrier-add.

OVERUTILIZED SITE UNDER INVESTIGATION
  Site ID: {target_site_id}
  Coordinates: lat={target_lat if target_lat is not None else '?'}, lng={target_lng if target_lng is not None else '?'}
  Zone: {target_zone} | City: {target_city}
  7-Day Avg DL PRB Utilization: {site_prb}%   [Threshold breached: > 92%]
  7-Day Avg DL User Throughput: {site_tput} Mbps
  Avg RRC Connected Users: {avg_rrc:.0f}   Peak RRC: {max_rrc:.0f}
  Monthly Revenue Impact: ${revenue:.1f}
  E-RAB Drop Rate: {site_drop}%

KPI TREND DATA (7d / 30d / 60d windows for {target_site_id} — compare and cite actual values):
{kpi_text}

COMPLETE RF PARAMETER SNAPSHOT for {target_site_id} (every live numeric column stored in telecom_sites):
{rf_full_text}
{neighbor_text}

CRITICAL FORMATTING RULES:
- Plain text only. No markdown, no asterisks, no bold, no hash symbols.
- Write exactly 4 numbered points on separate lines, BLANK LINE between them.
- Format: "N. Title: evidence-backed explanation."
- Every point MUST cite at least one specific numeric value AND, where the point concerns offload, must reference a neighbour BY site_id from the 3 listed above.

1. Capacity Exhaustion Evidence: Quote {target_site_id}'s {site_prb}% PRB and compare to its 30-day and 60-day averages from the KPI trend block. Bandwidth = {bw} MHz yielding ~{int(float(bw)*5) if bw not in ('?','None','') else 'N/A'} PRBs/TTI at 15 kHz SCS. Quantify how {avg_rrc:.0f} avg / {max_rrc:.0f} peak users are exhausting that PRB pool. Classify the breach as sudden, gradual, or chronic from the 7d-vs-30d-vs-60d delta.

2. Throughput, Drop and Revenue Impact: Explain why {site_prb}% PRB drives the {site_tput} Mbps per-user throughput — cite scheduler PRB starvation and Shannon-limit behaviour when buffers saturate. Tie ${revenue:.1f}/month revenue at risk and the {site_drop}% E-RAB drop rate to user-plane congestion / access-class blocking. Cite the actual KPI deltas.

3. Per-Neighbour Offload Feasibility (USE ONLY THE 3 LISTED NEIGHBOURS): Walk through EACH of the 3 neighbour sites by site_id. For each, quote its distance, PRB %, spare capacity %, RRC, throughput AND its RF parameters (E-tilt, EIRP, BW). State a yes/no verdict on whether it can absorb cell-edge traffic from {target_site_id}. If ALL 3 are >80% PRB or unhealthy, conclude offload is INFEASIBLE — a new site or carrier-add is mandatory. Otherwise name the SINGLE BEST candidate by site_id and estimate the % of {target_site_id}'s offered load it can absorb without exceeding 80% PRB on the receiving neighbour.

4. Root Cause Verdict tied to {target_site_id}'s RF Parameters: Pick the ONE dominant root cause from {{(a) traffic growth exceeding carrier capacity, (b) insufficient bandwidth, (c) coverage overshoot via high EIRP / low E-tilt directing too many users to this site, (d) outage on one of the 3 listed neighbours concentrating users here, (e) scheduler misconfiguration}}. Quote the specific RF parameter value from the snapshot above (E-tilt {tilt}°, EIRP {eirp} dBm, CRS {crs}, Height {height} m, plus any other parameter visible) that supports your diagnosis."""

    root_cause = ""
    try:
        from app import client as ai_client, DEPLOYMENT_NAME
        if ai_client:
            resp = ai_client.chat.completions.create(model=DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}], temperature=0.2, max_tokens=8000)
            root_cause = _clean_ai(resp.choices[0].message.content)
        else:
            root_cause = "AI client not configured."
    except Exception as e:
        root_cause = f"RCA failed: {str(e)}"

    # Only persist on the ticket record when the analysis is for the ticket's
    # primary site — otherwise the clubbed ticket would overwrite its RCA each
    # time an agent inspects a different site within the cluster.
    if target_site_id == t.site_id:
        t.root_cause = root_cause
        t.updated_at = datetime.utcnow()
        db.session.commit()
    return jsonify({"root_cause": root_cause, "site_id": target_site_id})


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/recommendations", methods=["POST"])
@jwt_required()
def overutilized_recommendations(tid):
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404
    body = request.json or {}
    root_cause = body.get("root_cause", t.root_cause or "")

    # Resolve which site within the clubbed ticket — same logic as RCA endpoint.
    raw_target = (body.get("target") or "").strip()
    explicit_site = (body.get("site_id") or "").strip()
    if explicit_site:
        target_site = explicit_site
    elif raw_target and raw_target.lower() not in ("site", "primary"):
        target_site = raw_target
    else:
        target_site = t.site_id

    # Dynamically discover EVERY numeric RF column stored for this site in telecom_sites
    # so the AI can choose the most appropriate parameter rather than picking from a
    # hard-coded subset. Merges in extra_params JSON for vendor-specific fields.
    from sqlalchemy import inspect as sa_inspect
    RF_SKIP = {"id", "latitude", "longitude", "created_at", "updated_at"}
    TEXT_SKIP = {"site_id","site_name","cell_id","cell_site_id","site_abs_id","zone",
                 "city","state","country","province","commune","site_status","alarms",
                 "solution","standard_solution_step","vendor_name","technology","extra_params"}
    rf_live = {}
    try:
        _insp = sa_inspect(db.engine)
        cols = [c["name"] for c in _insp.get_columns("telecom_sites")]
        num_cols = [c for c in cols if c not in RF_SKIP and c not in TEXT_SKIP]
        if num_cols:
            sel_parts = ", ".join([f"AVG({c}) AS {c}" for c in num_cols])
            r = _sql(f"SELECT {sel_parts} FROM telecom_sites WHERE site_id=:sid", {"sid": target_site})
            if r: rf_live = {k: v for k, v in r[0].items() if v is not None}
    except Exception as _e:
        _LOG.warning("RF param scan failed for %s: %s", target_site, _e)

    site_vendor = site_tech = ""
    target_lat = target_lng = None
    target_zone = t.zone or ""
    target_city = t.location or ""
    try:
        from models import TelecomSite
        site_obj = TelecomSite.query.filter_by(site_id=target_site).first()
        if site_obj:
            if site_obj.extra_params:
                for k, v in site_obj.extra_params.items():
                    if v is not None and k not in rf_live:
                        rf_live[k] = v
            site_vendor = site_obj.vendor_name or ""
            site_tech = site_obj.technology or ""
            target_lat = site_obj.latitude
            target_lng = site_obj.longitude
            target_zone = site_obj.zone or target_zone
            target_city = site_obj.city or target_city
    except Exception:
        pass

    def _fmt_val(v):
        try:
            fv = float(v)
            if fv == int(fv): return f"{int(fv)}"
            return f"{round(fv, 3)}"
        except (TypeError, ValueError):
            return str(v)

    rf_lines = [f"  {k} = {_fmt_val(v)}" for k, v in sorted(rf_live.items())]
    rf_full = "\n".join(rf_lines) if rf_lines else "  (no RF parameters stored for this site)"

    # Keep the "headline" RF values for the narrative instructions
    bw = _fmt_val(rf_live.get("bandwidth_mhz")) if rf_live.get("bandwidth_mhz") is not None else "?"
    tilt = _fmt_val(rf_live.get("e_tilt_degree")) if rf_live.get("e_tilt_degree") is not None else "?"
    eirp = _fmt_val(rf_live.get("rf_power_eirp_dbm")) if rf_live.get("rf_power_eirp_dbm") is not None else "?"

    avg_rrc, max_rrc, revenue, _, _ = _get_site_rrc_and_revenue(target_site)

    # Pull selected-site PRB/Tput/Drop direct from KPI data so the AI sees that
    # site's actual numbers (not the clubbed-ticket averages).
    site_prb = site_tput = site_drop = None
    try:
        rr = _sql("""
            SELECT AVG(CASE WHEN kpi_name=:prb THEN value END) AS prb,
                   AVG(CASE WHEN kpi_name='LTE DL - Usr Ave Throughput' THEN value END) AS tput,
                   AVG(CASE WHEN kpi_name='E-RAB Call Drop Rate_1' THEN value END) AS drop
            FROM kpi_data_merged WHERE site_id=:sid AND value IS NOT NULL
              AND date >= CURRENT_DATE - INTERVAL '7 days'
        """, {"sid": target_site, "prb": _PRB_KPI})
        if rr:
            site_prb = _f(rr[0].get("prb"), 1)
            site_tput = _f(rr[0].get("tput"), 1)
            site_drop = _f(rr[0].get("drop"), 2)
    except Exception:
        pass
    if site_prb is None: site_prb = t.avg_prb_util
    if site_tput is None: site_tput = t.avg_dl_tput
    if site_drop is None: site_drop = t.avg_drop_rate

    # Fetch nearest 3 sites for load-balancing analysis (shared helper)
    neighbors = _overutilized_neighbors(target_site, n=3)
    neighbor_text = ""
    if neighbors:
        neighbor_text = "\nNEAREST 3 NEIGHBOUR SITES (LIVE DB, last 7 days — use for load-balancing verdict):\n"
        for nb in neighbors:
            neighbor_text += (f"  {nb['site_id']} ({nb.get('site_name','')}): Distance={nb.get('distance_km',0)}km, "
                              f"Status={nb.get('site_status','on_air')}, "
                              f"PRB={nb.get('avg_prb',0)}% (spare={nb.get('spare_capacity_pct',0)}%), "
                              f"RRC={nb.get('avg_rrc',0)} users, Tput={nb.get('avg_tput',0)}Mbps, "
                              f"Drop={nb.get('avg_drop',0)}%, "
                              f"BW={nb.get('bandwidth_mhz','?')}MHz, E-tilt={nb.get('e_tilt_degree','?')}deg, "
                              f"EIRP={nb.get('rf_power_eirp_dbm','?')}dBm, CRS={nb.get('crs_gain','?')}, "
                              f"lat={nb.get('latitude','?')}, lng={nb.get('longitude','?')}\n")
    else:
        neighbor_text = ("\nNEAREST 3 NEIGHBOUR SITES: lookup returned NO results — either lat/lng "
                         f"is missing for {target_site} or no other sites are stored. Treat offload as "
                         "INFEASIBLE and recommend a new-site build at the lat/lng of the overutilized site.\n")

    prompt = f"""You are a SENIOR RAN OPTIMISATION EXECUTIVE running the {site_vendor or 'Ericsson / Nokia'} 24x7 NOC predictive-analytics shift, with 20+ years of live {site_tech or 'LTE'} network experience. The predictive-analytics engine has flagged {target_site} because its 7-day PRB utilisation breached the 92% trigger. Your output is the change-order an O&M team executes TODAY. THINK and WRITE exactly like a human RAN executive on the shift handover call: terse, fact-anchored, decision-led. NEVER invent values. NEVER speak in generalities. EVERY sentence must cite a concrete number or a parameter name from the data blocks below.

CRITICAL CONSTRAINT — LOAD-BALANCING SCOPE:
  The 3 nearest neighbour sites listed below ARE the entire offload candidate pool for {target_site}. Treat them as the ONLY neighbours that exist. Do NOT assume any other neighbour. All load-balancing decisions, RF re-aim suggestions, capacity expansion, and new-site recommendations MUST be derived using ONLY:
    (a) {target_site}'s own RF parameters listed in the snapshot below, AND
    (b) the 3 listed neighbour sites and their RF parameters / KPIs.

NOC PREDICTIVE-ANALYSIS PLAYBOOK (apply in this exact order):
  STEP 1 — Per-neighbour offload feasibility: walk through EACH of the 3 listed neighbours by site_id. For each, weigh its PRB headroom (must stay below 80% after offload), RRC headroom, RF posture (E-tilt, EIRP, BW), and distance from {target_site}. Decide if it can absorb cell-edge traffic.
  STEP 2 — Translate the chosen neighbour into precise RF-shaping change-orders on {target_site}: a digital E-tilt step + EIRP step + a HO bias parameter ({{Ericsson}}: cellIndividualOffset / qOffsetCell / qRxLevMin; {{Nokia}}: cellIndividualOffset / qHyst / qRxLevMin). Quote the exact previous value from the DB snapshot and the safe new value.
  STEP 3 — If load-balancing alone cannot bring {target_site} PRB below 70%, recommend the next RF parameter from the snapshot whose change cures the residual cause.
  STEP 4 — Capacity-expansion decision: carrier-add / BW upgrade vs new site. Trigger NEW SITE only if ALL 3 listed neighbours exceed 80% PRB.
  STEP 5 — Verification KPIs and rollback rule.

OVERUTILIZED SITE: {target_site}
  Coordinates: lat={target_lat if target_lat is not None else '?'}, lng={target_lng if target_lng is not None else '?'}
  Zone: {target_zone} | City: {target_city}
  7-day PRB: {site_prb}%   DL Tput: {site_tput} Mbps   Users: {avg_rrc:.0f} avg / {max_rrc:.0f} peak
  Revenue: ${revenue:.1f}/month   E-RAB Drop: {site_drop}%

COMPLETE RF PARAMETER DATABASE for {target_site} (every live numeric value stored in telecom_sites — pick fixes FROM THIS LIST only, treat each as a Managed-Object attribute on {site_vendor or 'the eNodeB'}):
{rf_full}
{neighbor_text}
ROOT CAUSE ANALYSIS FINDINGS (treat as authoritative input — every recommendation must cure something here):
{root_cause if root_cause else 'Capacity exhaustion — PRB consistently above 92%.'}

YOUR TASK (act exactly like a senior {site_vendor or 'Ericsson / Nokia'} NOC RAN executive):
1. Compare each of the 3 listed neighbour sites side-by-side against {target_site}. Quote EACH neighbour by site_id with PRB %, spare %, RRC, throughput, distance, AND its RF parameters (E-tilt, EIRP, BW, CRS). Calculate roughly what percentage of {target_site}'s offered load can be migrated to the BEST candidate without pushing that neighbour above 80% PRB.
2. Translate the load-balancing decision into the EXACT MO-level parameter changes on {target_site}. Use parameter names verbatim from the RF database above — quote "Previous value = X → New value = Y" — and explain the cell-edge user behaviour change in plain RF terms (footprint shrink, RSRP edge shift, A3/A5 handover trigger nudged toward the receiving neighbour).
3. If load-balancing alone cannot bring {target_site} below 70% PRB, escalate with a SECOND RF parameter from the snapshot, quoting Previous → New.
4. Capacity expansion decision: ONLY recommend a new site if ALL 3 listed neighbours are >80% PRB or unhealthy. If a new site is justified, give approximate lat/lng (~1.0 km offset from {target_site} at lat={target_lat if target_lat is not None else '?'}, lng={target_lng if target_lng is not None else '?'} toward the highest-density listed neighbour) and justify with neighbour data.
5. KPI verification thresholds + rollback rule a real NOC would set.

FORMATTING RULES (strict):
- Plain text only. No markdown, no asterisks, no hash symbols.
- Write exactly 5 numbered points, BLANK LINE between them, 3-4 sentences each.
- Every point MUST cite at least one specific numeric value from the data blocks.
- Whenever you mention a neighbour, quote its site_id from the 3 listed.

1. Load-Balancing Verdict (USE ONLY THE 3 LISTED NEIGHBOURS): Walk through all 3 neighbour sites one-by-one. For EACH quote site_id, distance_km, current PRB %, spare capacity %, RRC count, throughput, AND its E-tilt / EIRP / BW. Give an explicit YES/NO verdict on whether it can absorb traffic from {target_site}. Identify the SINGLE BEST candidate (lowest PRB, highest spare, healthy). State approximately what percentage of {target_site}'s {avg_rrc:.0f} active users can be steered to that neighbour without pushing it above the 80% PRB safe-threshold.

2. RF Re-aim Change-Order toward the chosen neighbour ({site_vendor or 'eNodeB'} MO parameters): Quote the chosen neighbour by site_id from point 1. Issue the precise change on {target_site}: "e_tilt_degree: Previous value = {tilt}° → New value = N°" and "rf_power_eirp_dbm: Previous value = {eirp} dBm → New value = M dBm". Pick N and M so {target_site}'s footprint shrinks toward the chosen neighbour's azimuth (typical NOC step: +1° tilt, -1 dB EIRP). State expected PRB drop on {target_site} ({site_prb}% → target) and projected PRB rise on the chosen neighbour (must remain below 80%). Add ONE handover-bias parameter (e.g., cellIndividualOffset or qOffsetCell) from the RF list with Previous → New that nudges A3/A5 events toward the receiving neighbour.

3. Most Appropriate Single-Parameter Change for the Residual Root Cause: From the RF parameter list above, pick the SINGLE parameter different from point 2's (e.g., crs_gain, antenna_height_agl_m, antenna_gain_dbi, m_tilt_degree, frequency_band, or any vendor-specific extra_param) whose change best cures the RCA-identified root cause. Quote "Parameter <exact_name>: Previous value = X → New value = Y" with X verbatim from the snapshot. State the RF-engineering rationale in one sentence and the expected KPI recovery (which KPI moves, by how much, within what timeframe).

4. Capacity Expansion Decision — Carrier-Add / Bandwidth Upgrade vs New Site (use ONLY the 3 listed neighbours): Quote current bandwidth_mhz = {bw} MHz and {avg_rrc:.0f} avg users at {site_prb}% PRB. If point 1's verdict is FEASIBLE, defer expansion. If INFEASIBLE (every one of the 3 listed neighbours is >80% PRB OR all unhealthy), recommend EITHER (a) carrier-add or bandwidth expansion to 15 or 20 MHz with the PRB headroom gain quoted, OR (b) a NEW SITE — give approximate lat/lng offset ~1.0 km from {target_site} at lat={target_lat if target_lat is not None else '?'}, lng={target_lng if target_lng is not None else '?'} toward the highest-density listed neighbour, justified by that neighbour's traffic data.

5. KPI Verification Window and Rollback Trigger: Sequence the changes fastest-safe-first: RF re-aim → second RF parameter → carrier-add → new site. State validation KPIs ({target_site} PRB < 70%, DL user throughput > 15 Mbps, no regression on drop rate currently {site_drop}%, no PRB rise above 80% on the chosen neighbour from the 3 listed). State the 48-72h post-change monitoring window typical of {site_vendor or 'Ericsson / Nokia'} NOC and the hard rollback trigger ("revert if the chosen neighbour's PRB rises above 85% or drop rate exceeds {round(float(site_drop)+0.5,2) if site_drop is not None else '?'}% on {target_site} or any of the 3 listed neighbours within 48 hours")."""

    recommendation = ""
    try:
        from app import client as ai_client, DEPLOYMENT_NAME
        if ai_client:
            resp = ai_client.chat.completions.create(model=DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}], temperature=0.2, max_tokens=8000)
            recommendation = _clean_ai(resp.choices[0].message.content)
        else:
            recommendation = "AI client not configured."
    except Exception as e:
        recommendation = f"Recommendation failed: {str(e)}"

    # Persist on the ticket only when the analysis is for the primary site,
    # so per-site analysis on clubbed tickets doesn't overwrite siblings' data.
    if target_site == t.site_id:
        t.recommendation = recommendation
        t.updated_at = datetime.utcnow()
        db.session.commit()
    return jsonify({"recommendation": recommendation, "site_id": target_site})


@network_issues_bp.route("/api/network-issues/overutilized/stats", methods=["GET"])
@jwt_required()
def overutilized_stats():
    try:
        total = OverutilizedTicket.query.count()
        open_c = OverutilizedTicket.query.filter_by(status="open").count()
        ip = OverutilizedTicket.query.filter_by(status="in_progress").count()
        crit = OverutilizedTicket.query.filter_by(priority="Critical").count()
    except:
        total = open_c = ip = crit = 0
    return jsonify({"total": total, "open": open_c, "in_progress": ip, "critical": crit})


@network_issues_bp.route("/api/network-issues/overutilized/trigger", methods=["POST"])
@jwt_required()
def trigger_overutilized():
    count = run_overutilized_job()
    return jsonify({"success": True, "tickets_processed": count})


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/pdf-data", methods=["GET"])
@jwt_required()
def overutilized_pdf_data(tid):
    """Return data for PDF report of overutilized site ticket."""
    from models import TelecomSite
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Not found"}), 404
    site_info = None
    try:
        s = TelecomSite.query.filter_by(site_id=t.site_id).first()
        if s:
            site_info = {
                "site_id": s.site_id, "site_name": s.site_name or s.site_id,
                "latitude": s.latitude, "longitude": s.longitude,
                "province": getattr(s, "province", "") or s.zone or "",
                "commune": getattr(s, "commune", "") or "",
                "zone": s.zone or "", "city": getattr(s, "city", "") or "",
                "state": getattr(s, "state", "") or "",
                "site_status": s.site_status or "on_air",
                "bandwidth_mhz": s.bandwidth_mhz, "antenna_gain_dbi": s.antenna_gain_dbi,
                "rf_power_eirp_dbm": s.rf_power_eirp_dbm, "antenna_height_agl_m": s.antenna_height_agl_m,
                "e_tilt_degree": s.e_tilt_degree, "crs_gain": s.crs_gain,
            }
    except Exception: pass
    agent_name = ""
    if t.assigned_agent:
        ag = db.session.get(User, t.assigned_agent)
        if ag: agent_name = ag.name
    return jsonify({
        "ticket": {
            "id": t.id, "site_id": t.site_id, "category": t.category,
            "priority": t.priority, "priority_score": t.priority_score,
            "status": t.status, "zone": t.zone, "location": t.location,
            "avg_prb_util": t.avg_prb_util, "avg_dl_tput": t.avg_dl_tput,
            "avg_drop_rate": t.avg_drop_rate, "avg_rrc": t.avg_rrc, "max_rrc": t.max_rrc,
            "revenue_total": t.revenue_total, "sla_hours": t.sla_hours,
            "root_cause": t.root_cause or "", "recommendation": t.recommendation or "",
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "agent_name": agent_name,
        },
        "site_info": site_info,
    })


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/parameter-change", methods=["GET"])
@jwt_required()
def get_overutilized_param_change(tid):
    """Get parameter change for overutilized ticket."""
    change = cr = None
    try:
        rows = _sql("SELECT * FROM parameter_changes WHERE network_issue_id = :tid ORDER BY created_at DESC LIMIT 1", {"tid": tid + 100000})
        if rows:
            change = rows[0]
            cr_rows = _sql("SELECT * FROM change_requests WHERE parameter_change_id = :pcid ORDER BY created_at DESC LIMIT 1", {"pcid": change["id"]})
            if cr_rows: cr = cr_rows[0]
    except Exception: pass
    return jsonify({"change": change, "cr": cr})


@network_issues_bp.route("/api/network-issues/overutilized/<int:tid>/parameter-change", methods=["POST"])
@jwt_required()
def create_overutilized_param_change(tid):
    """Create parameter change for overutilized ticket — same ITIL workflow."""
    user_id = int(get_jwt_identity())
    user = db.session.get(User, user_id)
    if not user or user.role not in ("human_agent", "admin", "manager", "cto"):
        return jsonify({"error": "Unauthorized"}), 403
    t = db.session.get(OverutilizedTicket, tid)
    if not t: return jsonify({"error": "Ticket not found"}), 404
    data = request.json or {}
    proposed = (data.get("proposed_change") or "").strip()
    if not proposed: return jsonify({"error": "proposed_change required"}), 400
    impact = (data.get("impact_assessment") or "").strip()
    rollback = (data.get("rollback_plan") or "").strip()
    now = datetime.utcnow()
    approval_deadline = None
    if t.deadline_time:
        remaining = (t.deadline_time - now).total_seconds()
        approval_deadline = now + timedelta(seconds=max(remaining * 0.3, 1800))
    try:
        from app import _find_best_manager, _generate_cr_number
        from models import ParameterChange, ChangeRequest
        manager = _find_best_manager(t.priority.lower() if t.priority else "low")
    except Exception:
        manager = None
    change = ParameterChange(
        ticket_id=None, network_issue_id=tid + 100000,
        agent_id=user_id, proposed_change=proposed, status="pending",
    )
    try: change.approval_deadline = approval_deadline
    except: pass
    db.session.add(change); db.session.flush()
    cr = ChangeRequest(
        cr_number=_generate_cr_number(), ticket_id=None,
        parameter_change_id=change.id, raised_by=user_id,
        title=f"Capacity Change: {t.site_id} — Overutilized (PRB {t.avg_prb_util}%)",
        description=proposed, impact_assessment=impact, rollback_plan=rollback,
        status="created",
    )
    db.session.add(cr)
    t.status = "in_progress"; t.updated_at = now
    db.session.commit()
    return jsonify({
        "change": change.to_dict(), "cr": cr.to_dict(),
        "assigned_manager": {"id": manager.id, "name": manager.name} if manager else None,
    }), 201
