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

network_issues_bp = Blueprint("network_issues", __name__)

# Track if today's job has run (reset daily)
_LAST_JOB_DATE = None

# Pre-scan cache: stores latest worst-cell scan results for dashboard display
_LATEST_WORST_CELLS = {}
_LATEST_SCAN_TIME = None


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


# ─────────────────────────────────────────────────────────────────────────────
# Priority calculation
# ─────────────────────────────────────────────────────────────────────────────
def _calc_priority(category, revenue_total, avg_rrc_60d, max_rrc_60d):
    severity = 3 if category == "Severe Worst" else 1
    rev = float(revenue_total or 0)
    rev_score = 3 if rev >= 60 else (2 if rev >= 45 else 1)
    arrc = float(avg_rrc_60d or 0)
    avg_rrc_score = 3 if arrc >= 1000 else (2 if arrc >= 300 else 1)
    mrrc = float(max_rrc_60d or 0)
    max_rrc_score = 3 if mrrc >= 1500 else (2 if mrrc >= 500 else 1)
    score = severity + rev_score + avg_rrc_score + max_rrc_score
    if score >= 10: return score, "Critical", 2
    elif score >= 7: return score, "High", 4
    elif score >= 4: return score, "Medium", 8
    else: return score, "Low", 16


# ─────────────────────────────────────────────────────────────────────────────
# Worst cells detection (ALL 3 thresholds must fail)
# ─────────────────────────────────────────────────────────────────────────────
def _get_worst_cells_by_site():
    """Get worst cells grouped by site. Only cells failing ALL 3 thresholds (3/3).
    Limited to top 10 worst cells ranked by severity, then grouped by site.
    Only sites matching the dashboard worst cells list are included."""
    try:
        # Get cells meeting ALL 3 conditions, ranked by impact
        rows = _sql("""
            SELECT k.site_id, k.cell_id, k.cell_site_id, ts.zone, ts.city, ts.state,
                   AVG(CASE WHEN k.kpi_name='E-RAB Call Drop Rate_1' THEN k.value END) AS avg_drop,
                   AVG(CASE WHEN k.kpi_name='LTE Call Setup Success Rate' THEN k.value END) AS avg_cssr,
                   AVG(CASE WHEN k.kpi_name='LTE DL - Usr Ave Throughput' THEN k.value END) AS avg_tput
            FROM kpi_data k
            JOIN telecom_sites ts ON k.site_id = ts.site_id
            WHERE k.value IS NOT NULL AND k.data_level = 'cell'
              AND k.kpi_name IN ('E-RAB Call Drop Rate_1','LTE Call Setup Success Rate','LTE DL - Usr Ave Throughput')
              AND k.date >= CURRENT_DATE - INTERVAL '7 days' AND k.date <= CURRENT_DATE
            GROUP BY k.site_id, k.cell_id, k.cell_site_id, ts.zone, ts.city, ts.state
            HAVING (AVG(CASE WHEN k.kpi_name='E-RAB Call Drop Rate_1' THEN k.value END) > 1.5
               AND AVG(CASE WHEN k.kpi_name='LTE Call Setup Success Rate' THEN k.value END) < 98.5
               AND AVG(CASE WHEN k.kpi_name='LTE DL - Usr Ave Throughput' THEN k.value END) < 8)
            ORDER BY AVG(CASE WHEN k.kpi_name='E-RAB Call Drop Rate_1' THEN k.value END) DESC
            LIMIT 10
        """)
    except Exception as e:
        _LOG.error("Failed to get worst cells: %s", e)
        return {}

    sites = {}
    for r in rows:
        sid = r["site_id"]
        drop = float(r.get("avg_drop") or 0)
        cssr = float(r.get("avg_cssr") or 100)
        tput = float(r.get("avg_tput") or 999)
        violations = sum([1 if drop > 1.5 else 0, 1 if cssr < 98.5 else 0, 1 if tput < 8 else 0])
        if sid not in sites:
            sites[sid] = {"cells": [], "cell_site_ids": [], "zone": r.get("zone") or "",
                          "city": r.get("city") or "", "state": r.get("state") or "",
                          "drops": [], "cssrs": [], "tputs": [], "max_violations": 0}
        sites[sid]["cells"].append(r["cell_id"] or "")
        sites[sid]["cell_site_ids"].append(r.get("cell_site_id") or r["cell_id"] or "")
        sites[sid]["drops"].append(drop)
        sites[sid]["cssrs"].append(cssr)
        sites[sid]["tputs"].append(tput)
        sites[sid]["max_violations"] = max(sites[sid]["max_violations"], violations)

    result = {}
    for sid, d in sites.items():
        n = len(d["cells"])
        result[sid] = {
            "cells": d["cells"], "cell_site_ids": d["cell_site_ids"],
            "zone": d["zone"], "city": d["city"], "state": d["state"],
            "avg_drop": _f(sum(d["drops"])/n, 2), "avg_cssr": _f(sum(d["cssrs"])/n, 2),
            "avg_tput": _f(sum(d["tputs"])/n, 2), "violations": d["max_violations"],
            "category": "Severe Worst" if d["max_violations"] >= 2 else "Worst",
        }
    return result


def _get_site_rrc_and_revenue(site_id):
    avg_rrc = max_rrc = revenue = 0
    try:
        r = _sql("""SELECT AVG(CASE WHEN kpi_name='Ave RRC Connected Ue' THEN value END) AS avg_rrc,
                           MAX(CASE WHEN kpi_name='Max RRC Connected Ue' THEN value END) AS max_rrc
                    FROM kpi_data WHERE site_id=:sid AND data_level='site' AND value IS NOT NULL
                      AND kpi_name IN ('Ave RRC Connected Ue','Max RRC Connected Ue')
                      AND date >= CURRENT_DATE - INTERVAL '60 days' AND date <= CURRENT_DATE""", {"sid": site_id})
        if r: avg_rrc = float(r[0].get("avg_rrc") or 0); max_rrc = float(r[0].get("max_rrc") or 0)
    except: pass
    try:
        r = _sql("""SELECT SUM(num_value) AS rev FROM flexible_kpi_uploads
                    WHERE site_id=:sid AND kpi_type='revenue' AND column_name ILIKE '%revenue%' AND num_value IS NOT NULL""", {"sid": site_id})
        if r and r[0].get("rev"): revenue = float(r[0]["rev"])
    except: pass
    return avg_rrc, max_rrc, revenue


# ─────────────────────────────────────────────────────────────────────────────
# Pre-scan: keeps worst cells fresh for the dashboard (runs every 30 min)
# ─────────────────────────────────────────────────────────────────────────────
def run_pre_scan():
    global _LATEST_WORST_CELLS, _LATEST_SCAN_TIME
    _LOG.info("Running pre-scan for worst cells...")
    try:
        _LATEST_WORST_CELLS = _get_worst_cells_by_site()
        _LATEST_SCAN_TIME = datetime.utcnow()
        _LOG.info("Pre-scan complete: %d sites with worst cells", len(_LATEST_WORST_CELLS))
    except Exception as e:
        _LOG.error("Pre-scan failed: %s", e)


def _get_yesterday_cells():
    """Get yesterday's ticket cells per site.
    Returns { site_id: { 'ticket': NetworkIssueTicket, 'cells': set(cell_ids) } }"""
    yesterday = _date.today() - timedelta(days=1)
    tickets = NetworkIssueTicket.query.filter(
        db.func.date(NetworkIssueTicket.created_at) == yesterday,
    ).all()
    result = {}
    for t in tickets:
        cells = set(t.cells_affected.split(",")) if t.cells_affected else set()
        result[t.site_id] = {"ticket": t, "cells": cells}
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Daily job
# ─────────────────────────────────────────────────────────────────────────────
def run_daily_network_issue_job():
    global _LAST_JOB_DATE
    _LOG.info("Running daily network issue ticket creation...")

    worst_sites = _get_worst_cells_by_site()
    if not worst_sites:
        _LOG.info("No worst cells found.")
        _LAST_JOB_DATE = _date.today()
        return 0

    yesterday_data = _get_yesterday_cells()

    created = updated = 0
    for site_id, data in worst_sites.items():
        avg_rrc, max_rrc, revenue = _get_site_rrc_and_revenue(site_id)
        score, priority, sla_hours = _calc_priority(data["category"], revenue, avg_rrc, max_rrc)

        from datetime import date as _dt
        now_08 = datetime.combine(_dt.today(), datetime.min.time()).replace(hour=8, minute=0, second=0)
        now_real = datetime.now()
        location = f"{data['city']}, {data['state']}" if data['city'] else data['zone']
        today_cells = set(data["cells"])

        # Compare with yesterday's cells for this site
        yesterday_info = yesterday_data.get(site_id)
        same_as_yesterday = yesterday_info and yesterday_info["cells"] == today_cells

        if same_as_yesterday:
            # Same cells as yesterday -- find existing open ticket and update it
            existing = NetworkIssueTicket.query.filter(
                NetworkIssueTicket.site_id == site_id,
                NetworkIssueTicket.status.in_(["open", "in_progress"]),
            ).first()
            if existing:
                existing.cells_affected = ",".join(data["cells"])
                existing.cell_site_ids = ",".join(data["cell_site_ids"])
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
                existing.updated_at = now_real
                updated += 1
                continue
            # If no open ticket found, fall through to create a new one

        # Different cells from yesterday OR no previous ticket -- create new ticket
        # Assign agent based on expertise/capacity (no online requirement)
        agent = None
        try:
            from app import _find_best_expert, _open_ticket_count
            network_agents = User.query.filter(
                User.role == "human_agent",
                User.expertise.in_(["NETWORK_RF", "NETWORK_OPTIMIZATION", "LTE", "5G"])
            ).all()
            if network_agents:
                same_zone = [a for a in network_agents if (a.location or "").lower() in (data["zone"] or "").lower() or (data["city"] or "").lower() in (a.location or "").lower()]
                pool = same_zone if same_zone else network_agents
                under_cap = [a for a in pool if _open_ticket_count(a.id) < (a.bandwidth_capacity or 10)]
                agent = min(under_cap or pool, key=lambda a: _open_ticket_count(a.id))
            else:
                agent = _find_best_expert("mobile", data["zone"], priority.lower())
        except Exception as e:
            _LOG.warning("Agent routing failed: %s", e)

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
            created_at=now_08, updated_at=now_real,
            deadline_time=now_08 + timedelta(hours=sla_hours),
        )
        db.session.add(ticket)
        created += 1

    db.session.commit()
    _LAST_JOB_DATE = _date.today()
    _LOG.info("Network issue job: %d created, %d updated", created, updated)
    return created + updated


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler: runs at 08:00 IST (02:30 UTC) + startup fallback
# ─────────────────────────────────────────────────────────────────────────────
def schedule_daily_job(app):
    import threading, time

    def _ist_now():
        """Return current datetime in IST (UTC+5:30)."""
        return datetime.utcnow() + timedelta(hours=5, minutes=30)

    def _job_loop():
        global _LAST_JOB_DATE
        time.sleep(8)  # wait for app to start
        last_prescan = 0  # epoch seconds of last pre-scan

        # Startup: run pre-scan immediately
        with app.app_context():
            try:
                run_pre_scan()
                last_prescan = time.time()
            except Exception as e:
                _LOG.error("Startup pre-scan failed: %s", e)

            # If today's job hasn't run yet AND it's past 08:00 IST, run now
            ist = _ist_now()
            if _LAST_JOB_DATE != _date.today() and ist.hour >= 8:
                _LOG.info("Startup: running network issue job for today (missed 08:00 IST)")
                try:
                    run_daily_network_issue_job()
                except Exception as e:
                    _LOG.error("Startup job failed: %s", e)

        while True:
            ist = _ist_now()

            # Pre-scan every 30 minutes to keep dashboard worst cells fresh
            if time.time() - last_prescan >= 1800:
                try:
                    with app.app_context():
                        run_pre_scan()
                    last_prescan = time.time()
                except Exception as e:
                    _LOG.error("Pre-scan failed: %s", e)

            # Run daily ticket job at or after 08:00 IST
            if ist.hour >= 8 and _LAST_JOB_DATE != _date.today():
                _LOG.info("Scheduled: running network issue job (IST: %s)", ist.strftime("%Y-%m-%d %H:%M:%S"))
                try:
                    with app.app_context():
                        run_daily_network_issue_job()
                except Exception as e:
                    _LOG.error("Daily job failed: %s", e)
            time.sleep(30)

    threading.Thread(target=_job_loop, daemon=True).start()
    _LOG.info("Network issue scheduler started (08:00 IST daily + pre-scan every 30 min)")

# ─────────────────────────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@network_issues_bp.route("/api/network-issues/worst-cells", methods=["GET"])
@jwt_required()
def get_worst_cells():
    """Return pre-scanned worst cells for the dashboard (updated every 30 min)."""
    user_id = int(get_jwt_identity())
    user = db.session.get(User, user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403
    sites = []
    for site_id, data in _LATEST_WORST_CELLS.items():
        sites.append({
            "site_id": site_id,
            "cells": data.get("cells", []),
            "cell_site_ids": data.get("cell_site_ids", []),
            "avg_drop": data.get("avg_drop", 0),
            "avg_cssr": data.get("avg_cssr", 0),
            "avg_tput": data.get("avg_tput", 0),
            "category": data.get("category", "Worst"),
            "zone": data.get("zone", ""),
            "city": data.get("city", ""),
        })
    return jsonify({
        "sites": sites,
        "count": len(sites),
        "scan_time": _LATEST_SCAN_TIME.isoformat() if _LATEST_SCAN_TIME else None,
    })


@network_issues_bp.route("/api/network-issues/list", methods=["GET"])
@jwt_required()
def list_network_issues():
    user_id = int(get_jwt_identity())
    user = db.session.get(User, user_id)
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    tickets = NetworkIssueTicket.query.order_by(NetworkIssueTicket.created_at.desc()).all()
    now = datetime.utcnow()
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

        result.append({
            "id": t.id, "site_id": t.site_id,
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
            "latitude": first.latitude,
            "longitude": first.longitude,
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
        }
        for sr in site_rows:
            if sr.cell_id:
                cells_info.append({
                    "cell_id": sr.cell_id,
                    "latitude": sr.latitude,
                    "longitude": sr.longitude,
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

    # ── AI prompt ─────────────────────────────────────────────────────────────
    rca_system = f"""You are a principal RF optimization engineer. Analyze worst-cell KPI trends and identify physical root causes.

The cell/site is already flagged as worst offender. DO NOT restate threshold violations. Explain WHY the KPIs degraded.

Current RF Parameters: {rf_text}

ANALYSIS METHOD:
- Compare 7d vs 30d vs 60d averages to find WHEN degradation started
- Check min/max spread: wide spread = intermittent fault, narrow = persistent
- Correlate KPIs: Drop+CSSR down = RF interference; Drop+Tput down = DL SINR issue; all down = hardware/antenna fault
- Link findings to RF parameters (e-tilt, EIRP, antenna height, CRS gain) where relevant

OUTPUT: Exactly 4 numbered points. Each point:
1. **Bold Title**: Trend evidence (7d vs 60d numbers, min/max) → physical root cause → which RF parameter may be contributing.

NO square brackets. NO headers/summaries. NO threshold restatements. ONLY the 4 numbered points."""

    prompt = f"""Root cause analysis for {'cell '+target if target!='site' else 'site '+t.site_id} (Zone: {t.zone}).

{kpi_text}

Write exactly 4 points with **bold titles**. Each must compare 7d vs 60d trend, identify the physical cause, and reference RF parameters where relevant."""

    root_cause = ""
    try:
        from app import client as ai_client, DEPLOYMENT_NAME
        if ai_client:
            resp = ai_client.chat.completions.create(
                model=DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": rca_system},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2, max_tokens=1200,
            )
            raw = (resp.choices[0].message.content or "").strip()
            import re
            lines = raw.split('\n')
            clean = [l for l in lines if l.strip() and not re.match(
                r'^(I |Let me|Though |However |Note:|Wait|Hmm|Ok |Sure|Here)', l.strip(), re.IGNORECASE)]
            root_cause = '\n'.join(clean) if clean else raw
            # Validate: must have at least 3 numbered points, otherwise use fallback
            numbered = [l for l in clean if re.match(r'^\d+[\.\)]', l.strip())]
            if len(numbered) < 3:
                root_cause = ""  # force fallback
    except Exception as e:
        _LOG.error("RCA AI call failed for ticket %s: %s", ticket_id, e)

    # ── Rule-based fallback (always produces 4 expert points) ─────────────────
    if not root_cause:
        drop = t.avg_drop_rate or 0
        cssr = t.avg_cssr or 0
        tput = t.avg_tput or 0
        rrc = t.avg_rrc or 0
        max_rrc = t.max_rrc or 0

        d7 = windows.get("7 days", {})
        d60 = windows.get("60 days", {})
        DK = "E-RAB Call Drop Rate_1"
        CK = "LTE Call Setup Success Rate"
        TK = "LTE DL - Usr Ave Throughput"
        RK = "Ave RRC Connected Ue"

        d7_drop = d7.get(DK, {}).get("avg", drop);    d60_drop = d60.get(DK, {}).get("avg", drop)
        d7_drop_max = d7.get(DK, {}).get("max", drop); d7_drop_min = d7.get(DK, {}).get("min", 0)
        d7_cssr = d7.get(CK, {}).get("avg", cssr);    d60_cssr = d60.get(CK, {}).get("avg", cssr)
        d7_cssr_min = d7.get(CK, {}).get("min", cssr); d7_cssr_max = d7.get(CK, {}).get("max", cssr)
        d7_tput = d7.get(TK, {}).get("avg", tput);    d60_tput = d60.get(TK, {}).get("avg", tput)
        d7_tput_min = d7.get(TK, {}).get("min", tput); d7_tput_max = d7.get(TK, {}).get("max", tput)
        d7_rrc = d7.get(RK, {}).get("avg", rrc);      d7_rrc_max = d7.get(RK, {}).get("max", max_rrc)

        drop_spread = d7_drop_max - d7_drop_min
        cssr_spread = d7_cssr_max - d7_cssr_min
        tput_spread = d7_tput_max - d7_tput_min
        drop_spiked = d60_drop > 0 and d7_drop > d60_drop * 1.3
        cssr_degraded = d7_cssr < d60_cssr - 2

        pts = []

        # Point 1: Drop Rate trend analysis
        if drop_spiked:
            pts.append(f"1. **Recent Interference or Hardware Onset**: E-RAB drop rate increased from {d60_drop:.2f}% (60-day avg) to {d7_drop:.2f}% (7-day avg) — a {((d7_drop-d60_drop)/max(d60_drop,0.01)*100):.0f}% spike. The min/max spread of {d7_drop_min:.2f}%–{d7_drop_max:.2f}% ({'wide — suggesting intermittent fault events like VSWR alarms or TMA failures' if drop_spread > 3 else 'narrow — indicating persistent interference'}). With current E-tilt at {tilt}° and EIRP at {eirp} dBm, the cell may be overshooting into neighbor coverage, picking up co-channel interference on the return path.")
        else:
            pts.append(f"1. **Chronic Overshooting or Pilot Pollution**: Drop rate has been persistently elevated at {d7_drop:.2f}% (7-day) vs {d60_drop:.2f}% (60-day), with peaks reaching {d7_drop_max:.2f}%. The stability across time windows rules out a recent trigger — this is an entrenched RF footprint problem. Current E-tilt of {tilt}° with antenna height {height}m is likely causing the cell to overshoot its intended coverage boundary, creating a pilot pollution zone where 3+ cells overlap with similar RSRP, resulting in frequent handover ping-pong and bearer drops.")

        # Point 2: CSSR trend analysis
        if cssr_degraded:
            pts.append(f"2. **Accelerating Access Failure**: CSSR declined sharply from {d60_cssr:.2f}% (60-day) to {d7_cssr:.2f}% (7-day) — a {d60_cssr - d7_cssr:.1f} percentage point drop in the recent window. The 7-day minimum of {d7_cssr_min:.2f}% reveals periods of near-total RACH failure. This pattern is consistent with uplink interference in the PRACH band or a degrading TMA/LNA reducing uplink sensitivity. With current EIRP at {eirp} dBm, the DL/UL link budget may be asymmetric — strong downlink signal attracting users who then fail on the weaker uplink path.")
        elif d7_rrc > 300:
            pts.append(f"2. **Capacity-Induced RACH Congestion**: CSSR at {d7_cssr:.2f}% with {d7_rrc:.0f} avg connected users (peak {d7_rrc_max:.0f}) indicates PRACH/PUCCH resource exhaustion. The cell is handling more traffic than its configured capacity allows — users compete for limited random access opportunities. Current bandwidth of {bw} MHz may be insufficient for this traffic density, and the E-tilt of {tilt}° is likely pulling in users from beyond the cell's intended coverage radius.")
        else:
            pts.append(f"2. **Uplink Coverage Gap**: CSSR at {d7_cssr:.2f}% with only {d7_rrc:.0f} avg users rules out congestion. The 7-day spread of {d7_cssr_min:.2f}%–{d7_cssr_max:.2f}% ({'shows severe intermittent failures — possible external interference source active during specific hours' if cssr_spread > 20 else 'indicates persistent uplink weakness'}). With antenna height at {height}m and EIRP at {eirp} dBm, the DL coverage extends beyond the UL range, causing cell-edge users to camp on this cell but fail RACH due to insufficient uplink power budget.")

        # Point 3: Throughput & SINR analysis
        if d7_tput < d60_tput * 0.8:
            pts.append(f"3. **DL SINR Degradation**: Throughput dropped from {d60_tput:.2f} Mbps (60-day) to {d7_tput:.2f} Mbps (7-day) — a {((d60_tput - d7_tput)/max(d60_tput,0.01)*100):.0f}% decline. Min throughput of {d7_tput_min:.2f} Mbps confirms periods of severe quality degradation. This is not a capacity issue (RRC: {d7_rrc:.0f} users) — it is a SINR problem. With CRS Gain at {crs} and E-tilt at {tilt}°, the CRS reference signals may be experiencing interference from co-channel neighbors, degrading channel estimation accuracy and forcing lower MCS (modulation and coding scheme) selection.")
        elif d7_rrc > 200 and d7_tput < 6:
            pts.append(f"3. **PRB Congestion Limiting Per-User Throughput**: Throughput at {d7_tput:.2f} Mbps (min {d7_tput_min:.2f} Mbps) with {d7_rrc:.0f} avg users indicates high PRB utilization. The available bandwidth of {bw} MHz provides limited scheduling resources — at this traffic density, each user receives fewer PRBs per TTI, directly reducing achievable throughput. The cell's coverage area (controlled by E-tilt {tilt}° and height {height}m) may be too large, pulling in users that should be served by neighboring cells.")
        else:
            pts.append(f"3. **Persistent SINR and Scheduling Inefficiency**: Throughput has been chronically low at {d7_tput:.2f} Mbps (7-day) vs {d60_tput:.2f} Mbps (60-day), with fluctuation between {d7_tput_min:.2f}–{d7_tput_max:.2f} Mbps. With CRS Gain at {crs}, the reference signal quality may be marginal — low CRS SINR forces conservative MCS selection (QPSK instead of 64QAM), capping DL throughput regardless of traffic load. The E-tilt of {tilt}° may need adjustment to optimize the main beam coverage and improve SINR at cell center.")

        # Point 4: Cross-KPI correlation & RF parameter linkage
        all_worsening = d7_drop > d60_drop and d7_cssr < d60_cssr and d7_tput < d60_tput
        if all_worsening:
            pts.append(f"4. **Simultaneous Multi-KPI Collapse — Antenna System Fault Suspected**: All three KPIs worsened in the last 7 days vs 60-day baseline (Drop: {d60_drop:.2f}%→{d7_drop:.2f}%, CSSR: {d60_cssr:.2f}%→{d7_cssr:.2f}%, Tput: {d60_tput:.2f}→{d7_tput:.2f} Mbps). Simultaneous degradation across access, retention, and throughput strongly suggests a single physical root cause — most likely an antenna/feeder system fault (high VSWR, water ingress in connectors, or TMA failure). With current antenna height at {height}m, a physical site inspection should verify feeder cable integrity, connector tightness, and VSWR readings at the antenna port.")
        else:
            pts.append(f"4. **RF Parameter Mismatch for Site Environment**: The combination of elevated drops ({d7_drop:.2f}%), degraded CSSR ({d7_cssr:.2f}%), and low throughput ({d7_tput:.2f} Mbps) with current parameters (E-tilt: {tilt}°, EIRP: {eirp} dBm, CRS Gain: {crs}, Height: {height}m) suggests the RF configuration does not match the site's propagation environment. Priority actions: verify E-tilt against coverage planning tool predictions, check if recent construction or vegetation growth has altered the propagation path, and review neighbor cell parameter changes that may have shifted interference patterns in this cluster.")

        root_cause = '\n'.join(pts)

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

    # ── Get RF parameters (per-cell if cell target, otherwise site avg) ────────
    rf = {}
    try:
        cell_rf_filter = f"AND cell_id = '{t.site_id}_{target}'" if target != "site" else ""
        r = _sql(f"""SELECT AVG(bandwidth_mhz) AS bw, AVG(antenna_gain_dbi) AS gain,
                           AVG(rf_power_eirp_dbm) AS eirp, AVG(e_tilt_degree) AS tilt,
                           AVG(crs_gain) AS crs, AVG(antenna_height_agl_m) AS height
                    FROM telecom_sites WHERE site_id=:sid {cell_rf_filter}""", {"sid": t.site_id})
        if r: rf = r[0]
    except: pass

    def _safe_float(v):
        try: return float(v) if v is not None else None
        except (ValueError, TypeError): return None

    bw_val = _safe_float(rf.get("bw"));     bw = _f(bw_val, 1) if bw_val else "N/A"
    tilt_val = _safe_float(rf.get("tilt")); tilt = _f(tilt_val, 1) if tilt_val else "N/A"
    eirp_val = _safe_float(rf.get("eirp")); eirp = _f(eirp_val, 1) if eirp_val else "N/A"
    crs_val = _safe_float(rf.get("crs"));   crs = _f(crs_val, 2) if crs_val else "N/A"
    height_val = _safe_float(rf.get("height")); height = _f(height_val, 1) if height_val else "N/A"

    # ── AI prompt ─────────────────────────────────────────────────────────────
    system_msg = f"""You are a principal RF optimization engineer. Based on root cause analysis, recommend specific RF parameter changes.

Current RF Parameters: Bandwidth={bw} MHz, E-tilt={tilt}°, EIRP={eirp} dBm, CRS Gain={crs}, Antenna Height={height}m

OUTPUT: Exactly 4 numbered points. Each point:
1. **Parameter Name Adjustment**: Current value is X. Change to Y because [link to root cause]. Expected improvement: [KPI] by ~[X%].

RULES:
- Use **bold titles** (NOT square brackets)
- Each point changes ONE parameter: E-tilt, EIRP, CRS Gain, or Antenna Height
- Each must explain HOW it fixes the root cause
- Include current value → new value → expected KPI improvement
- NO headers, summaries, disclaimers. ONLY the 4 points."""

    prompt = f"""RF parameter recommendations for {'cell '+target if target!='site' else 'site '+t.site_id}.

Root Cause:
{root_cause if root_cause else 'Multi-KPI degradation — high drop rate, low CSSR, poor throughput.'}

Write exactly 4 parameter change recommendations with **bold titles**, specific values, and expected KPI improvement."""

    recommendation = ""
    try:
        from app import client as ai_client, DEPLOYMENT_NAME
        if ai_client:
            resp = ai_client.chat.completions.create(
                model=DEPLOYMENT_NAME,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2, max_tokens=1000,
            )
            raw = (resp.choices[0].message.content or "").strip()
            import re
            lines = raw.split('\n')
            clean = [l for l in lines if l.strip() and not re.match(
                r'^(I |Let me|Though |However |Note:|Wait|Hmm|Ok |Sure|Here)', l.strip(), re.IGNORECASE)]
            recommendation = '\n'.join(clean) if clean else raw
            # Validate: must have at least 3 numbered points
            numbered = [l for l in clean if re.match(r'^\d+[\.\)]', l.strip())]
            if len(numbered) < 3:
                recommendation = ""  # force fallback
    except Exception as e:
        _LOG.error("Recommendation AI call failed for ticket %s: %s", ticket_id, e)

    # ── Rule-based fallback (always produces 4 expert points) ─────────────────
    if not recommendation:
        drop = t.avg_drop_rate or 0
        cssr = t.avg_cssr or 0
        tput = t.avg_tput or 0
        rrc = t.avg_rrc or 0
        pts = []

        # 1. E-tilt adjustment (addresses overshooting, drop rate, interference)
        if tilt_val is not None:
            if drop > 2:
                new_tilt = min(tilt_val + 2, 15)
                pts.append(f"1. **E-tilt Downtilt Increase**: Current E-tilt is {tilt}°. Increase to {_f(new_tilt,1)}° to pull back the cell's coverage footprint and reduce overshooting into neighbor cells. This directly addresses the high drop rate ({drop:.2f}%) by reducing pilot pollution and handover ping-pong at cell edge. Expected improvement: E-RAB Drop Rate reduction by 25-35%, with secondary improvement in neighbor cell throughput.")
            else:
                new_tilt = max(tilt_val - 1, 0)
                pts.append(f"1. **E-tilt Optimization**: Current E-tilt is {tilt}°. Reduce to {_f(new_tilt,1)}° to extend coverage footprint and improve signal strength at cell edge, where most RACH failures occur. This addresses the low CSSR ({cssr:.2f}%) by ensuring UE at the cell boundary receive sufficient DL RSRP to attempt connection. Expected improvement: CSSR increase by 2-4%.")
        else:
            pts.append(f"1. **E-tilt Configuration Required**: E-tilt data is not available in the database for this {'cell' if target != 'site' else 'site'}. Immediate action: perform site audit to record current electrical tilt, then optimize using coverage planning tool targeting 3dB beamwidth at cell edge. Correct E-tilt directly reduces overshooting (drop rate) and improves SINR (throughput).")

        # 2. EIRP adjustment (addresses CSSR, UL/DL balance, coverage)
        if eirp_val is not None:
            if cssr < 95:
                new_eirp = eirp_val + 3
                pts.append(f"2. **EIRP Power Increase**: Current EIRP is {eirp} dBm. Increase to {_f(new_eirp,1)} dBm to strengthen DL coverage and improve the DL/UL link budget balance. With CSSR at {cssr:.2f}%, users at cell edge are failing RACH because the DL signal is too weak for reliable synchronization. Higher EIRP extends the reliable coverage radius. Expected improvement: CSSR increase by 3-5%, with drop rate reduction of 10-15% from better handover reliability.")
            else:
                new_eirp = max(eirp_val - 2, 20)
                pts.append(f"2. **EIRP Power Reduction**: Current EIRP is {eirp} dBm. Reduce to {_f(new_eirp,1)} dBm to contract the DL coverage footprint and match it to the UL range. This reduces the DL/UL asymmetry that causes users to camp on this cell but fail uplink transmissions. Combined with E-tilt adjustment, this will reduce interference to neighbors. Expected improvement: Drop Rate reduction by 15-20%, neighbor cell throughput improvement.")
        else:
            pts.append(f"2. **EIRP Audit Required**: EIRP data is not available in the database. Measure current Tx power at antenna port and verify against licensed EIRP. For the observed CSSR of {cssr:.2f}%, a power increase of 2-3 dB would extend reliable coverage radius by ~15%, directly improving RACH success rates.")

        # 3. CRS Gain adjustment (addresses throughput, SINR, MCS)
        if crs_val is not None:
            if tput < 6:
                new_crs = round(crs_val + 1, 1)
                pts.append(f"3. **CRS Gain Enhancement**: Current CRS Gain is {crs}. Increase to {_f(new_crs,1)} to boost reference signal power relative to data channels. With throughput at {tput:.2f} Mbps, low CRS SINR is likely forcing conservative MCS selection (QPSK instead of 16/64QAM), severely capping achievable data rates. Higher CRS Gain improves channel estimation accuracy at the UE. Expected improvement: DL throughput increase by 15-25% (higher MCS selection), with secondary improvement in BLER and reduced HARQ retransmissions.")
            else:
                new_crs = max(round(crs_val - 0.5, 1), 0)
                pts.append(f"3. **CRS Gain Rebalance**: Current CRS Gain is {crs}. Reduce to {_f(new_crs,1)} to allocate more power to PDSCH data channels. Since throughput is at {tput:.2f} Mbps with moderate SINR, shifting power from reference signals to data channels will directly improve per-user throughput. Expected improvement: DL throughput increase by 8-12%.")
        else:
            pts.append(f"3. **CRS Gain Configuration Required**: CRS Gain data is not available. With throughput at {tput:.2f} Mbps, verify CRS power boosting configuration. A typical optimization is to set CRS Gain 3dB above PDSCH RE power to ensure reliable channel estimation while maintaining data channel capacity.")

        # 4. Antenna Height review (addresses coverage footprint, interference pattern)
        if height_val is not None:
            if drop > 2 and tilt_val is not None and tilt_val < 4:
                pts.append(f"4. **Antenna Height & Mechanical Tilt Review**: Current antenna height is {height}m with E-tilt {tilt}°. At this height with low tilt, the cell overshoots significantly — the main beam travels too far, causing interference and pilot pollution. Options: (a) increase E-tilt to {_f(min(tilt_val+3, 12),1)}° to compensate for height, or (b) if site structure allows, lower antenna to {_f(max(height_val-3, 15),1)}m to naturally reduce coverage radius. Expected improvement: combined Drop Rate reduction of 30-40% and throughput increase of 20% from reduced inter-cell interference.")
            else:
                pts.append(f"4. **Antenna Height Verification**: Current antenna height is {height}m. Verify against original site design — if height has changed (antenna repositioned during maintenance) or if new obstructions (buildings, trees) now block the main beam, the effective coverage pattern will differ from planned. Physical site inspection recommended to check antenna orientation (azimuth and mechanical tilt), feeder connections, and VSWR. Expected improvement: correcting any physical misalignment can improve all three KPIs by 10-20%.")
        else:
            pts.append(f"4. **Physical Site Audit Required**: Antenna height data is not available. Schedule a physical site visit to document: antenna height AGL, mechanical tilt, azimuth orientation, feeder cable condition, and VSWR readings. Any deviation from planned parameters (common after maintenance or equipment swap) directly impacts all three KPIs. This data is essential for accurate RF planning tool simulations.")

        recommendation = '\n'.join(pts)

    if target == "site":
        t.recommendation = recommendation
        t.updated_at = datetime.utcnow()
        db.session.commit()

    return jsonify({"recommendation": recommendation, "target": target})

@network_issues_bp.route("/api/network-issues/trigger-job", methods=["POST"])
@jwt_required()
def trigger_network_issue_job():
    count = run_daily_network_issue_job()
    return jsonify({"success": True, "tickets_processed": count})


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
    if not user or user.role != "human_agent":
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
