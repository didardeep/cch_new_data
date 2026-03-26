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
# Worst cells detection
# A cell is "worst" if ALL 3 conditions fail on 7-day average:
#   1. E-RAB Call Drop Rate_1 > 1.5%
#   2. LTE Call Setup Success Rate < 98.5%
#   3. LTE DL - Usr Ave Throughput < 8 Mbps
# ─────────────────────────────────────────────────────────────────────────────
_DROP_KPI = "E-RAB Call Drop Rate_1"
_CSSR_KPI = "LTE Call Setup Success Rate"
_TPUT_KPI = "LTE DL - Usr Ave Throughput"


def _get_worst_cells_by_site():
    """Find worst cells: every cell_site_id where ALL 3 KPI thresholds fail
    on last 7 days average from CURRENT DATE. Groups results by site_id."""

    # Last 7 days counted from today (CURRENT_DATE)
    today = _date.today()
    start_date = today - timedelta(days=7)
    print(f"[WORST CELLS] Scanning cells from {start_date} to {today} (last 7 days from today)")

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
        """, {"drop": _DROP_KPI, "cssr": _CSSR_KPI, "tput": _TPUT_KPI,
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
    """Get RRC and revenue data for priority calculation."""
    avg_rrc = max_rrc = revenue = 0
    try:
        r = _sql(f"""SELECT AVG(CASE WHEN kpi_name='Ave RRC Connected Ue' THEN value END) AS avg_rrc,
                           MAX(CASE WHEN kpi_name='Max RRC Connected Ue' THEN value END) AS max_rrc
                    FROM kpi_data WHERE site_id=:sid AND data_level='site' AND value IS NOT NULL
                      AND kpi_name IN ('Ave RRC Connected Ue','Max RRC Connected Ue')
                      AND date >= CURRENT_DATE - INTERVAL '60 days'""", {"sid": site_id})
        if r: avg_rrc = float(r[0].get("avg_rrc") or 0); max_rrc = float(r[0].get("max_rrc") or 0)
    except: pass
    try:
        r = _sql("""SELECT SUM(num_value) AS rev FROM flexible_kpi_uploads
                    WHERE site_id=:sid AND kpi_type='revenue' AND column_name ILIKE '%revenue%' AND num_value IS NOT NULL""", {"sid": site_id})
        if r and r[0].get("rev"): revenue = float(r[0]["rev"])
    except: pass
    return avg_rrc, max_rrc, revenue


# ─────────────────────────────────────────────────────────────────────────────
# Pre-scan: keeps worst cells fresh for the dashboard
# ─────────────────────────────────────────────────────────────────────────────
def run_pre_scan():
    global _LATEST_WORST_CELLS, _LATEST_SCAN_TIME
    print("[NETWORK ISSUES] Running pre-scan for worst cells...")
    try:
        _LATEST_WORST_CELLS = _get_worst_cells_by_site()
        # Always show 08:00 AM today as the scan time (daily schedule time)
        _LATEST_SCAN_TIME = datetime.combine(_date.today(), datetime.min.time()).replace(hour=8)
        print(f"[NETWORK ISSUES] Pre-scan complete: {len(_LATEST_WORST_CELLS)} sites with worst cells")
    except Exception as e:
        print(f"[NETWORK ISSUES] Pre-scan failed: {e}")
        _LOG.error("Pre-scan failed: %s", e)


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
    global _LAST_JOB_DATE

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
        avg_rrc, max_rrc, revenue = _get_site_rrc_and_revenue(site_id)
        score, priority, sla_hours = _calc_priority(data["category"], revenue, avg_rrc, max_rrc)
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
                print(f"  [UPDATE] {site_id}: {len(today_cells)} cells (same as before), priority={priority}")
                continue
            else:
                # DIFFERENT cells → close old ticket, create new one below
                existing.status = "closed"
                existing.updated_at = today_08
                print(f"  [CLOSE] {site_id}: cells changed ({len(existing_cells)}→{len(today_cells)}), closing old ticket #{existing.id}")

        # CREATE new ticket — either no existing ticket or cells changed
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
            _LOG.warning("Agent routing failed for %s: %s", site_id, e)

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
        created += 1
        print(f"  [CREATE] {site_id}: {len(today_cells)} cells, priority={priority}, agent={agent.name if agent else 'none'}")

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
        """Check if daily job already ran today by looking at updated_at = today 08:00."""
        today_08 = datetime.combine(_date.today(), datetime.min.time()).replace(hour=8)
        count = NetworkIssueTicket.query.filter(
            NetworkIssueTicket.updated_at == today_08
        ).count()
        return count > 0

    def _job_loop():
        global _LAST_JOB_DATE
        nonlocal _prescan_done_date, _ticket_done_date
        time.sleep(8)  # wait for app to start

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
    if not user or user.role != "human_agent":
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


@network_issues_bp.route("/api/network-issues/todays-routing", methods=["GET"])
@jwt_required()
def todays_routing():
    """Return today's ticket routing: created + updated tickets today."""
    from datetime import date as _date_cls
    today = _date_cls.today()
    # Get tickets created OR updated today
    tickets = NetworkIssueTicket.query.filter(
        db.or_(
            db.func.date(NetworkIssueTicket.created_at) == today,
            db.func.date(NetworkIssueTicket.updated_at) == today,
        )
    ).order_by(NetworkIssueTicket.id.asc()).all()

    routing = []
    for t in tickets:
        agent_name = agent_email = ""
        if t.assigned_agent:
            ag = db.session.get(User, t.assigned_agent)
            if ag:
                agent_name = ag.name or ""
                agent_email = ag.email or ""
        created_today = t.created_at and t.created_at.date() == today
        routing.append({
            "ticket_id": t.id,
            "site_id": t.site_id,
            "priority": t.priority,
            "status": t.status or "open",
            "date": t.created_at.strftime("%Y-%m-%d") if t.created_at else today.isoformat(),
            "agent_name": agent_name,
            "agent_email": agent_email,
            "type": "created" if created_today else "updated",
        })
    return jsonify({"routing": routing, "date": today.isoformat()})


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

    # Gather KPI data
    kpi_text = ""
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
            for r in rows:
                kpi_text += f"  {r['kpi_name']}: avg={_f(r['avg'],2)}, min={_f(r['min'],2)}, max={_f(r['max'],2)}\n"
        except: pass

    rca_system = """You are a senior telecom network engineer performing root cause analysis.

STRICT RULES:
- Output ONLY numbered points (1. 2. 3. etc.) — nothing else
- Each point identifies ONE specific root cause with actual KPI values from the data
- Use this format: **[Root Cause Title]**: Explanation with specific KPI numbers
- Do NOT include any thinking, reasoning, disclaimers, or meta-commentary
- Do NOT say "I must", "I should", "though", "however", "let me", or explain your thought process
- Do NOT add summaries, conclusions, headers, or introductory text
- Each point must be about a DIFFERENT aspect of the degradation"""

    prompt = f"""Root cause analysis for {'cell '+target if target!='site' else 'site '+t.site_id}.

Site: {t.site_id} | Zone: {t.zone} | Category: {t.category}
KPI Violations: Drop={t.avg_drop_rate}% (>1.5%), CSSR={t.avg_cssr}% (<98.5%), Tput={t.avg_tput}Mbps (<8)

{kpi_text}

Write exactly 3-4 numbered root cause points. Each must cite specific KPI values from the data above."""

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
                temperature=0.15, max_tokens=1000,
            )
            raw = (resp.choices[0].message.content or "").strip()
            # Strip leaked reasoning lines (start with "I ", "Let me", "Though", etc.)
            import re
            lines = raw.split('\n')
            clean = []
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                # Skip obvious chain-of-thought leaks
                if re.match(r'^(I |Let me|Though |However |Note:|Wait|Hmm|Ok |Sure)', stripped, re.IGNORECASE):
                    continue
                clean.append(line)
            root_cause = '\n'.join(clean) if clean else raw
    except Exception as e:
        _LOG.error("RCA AI call failed for ticket %s: %s", ticket_id, e)
        # Build a meaningful 3-4 point fallback using actual data
        drop = t.avg_drop_rate or 0
        cssr = t.avg_cssr or 0
        tput = t.avg_tput or 0
        rrc = t.avg_rrc or 0
        pts = []
        pts.append(f"1. **Elevated E-RAB Drop Rate**: The {'cell ' + target if target != 'site' else 'site ' + t.site_id} is experiencing a call drop rate of {drop:.2f}%, significantly exceeding the 1.5% threshold. This indicates potential RF interference, overshooting, or hardware issues on the radio unit causing bearer releases during active sessions.")
        if cssr < 98.5:
            pts.append(f"2. **Low Call Setup Success Rate**: CSSR has degraded to {cssr:.2f}%, well below the 98.5% benchmark. This suggests RRC connection failures or RACH congestion, possibly due to uplink interference or insufficient PRACH resources preventing new users from establishing connections.")
        if tput < 8:
            pts.append(f"3. **Throughput Degradation**: Average DL user throughput is only {tput:.2f} Mbps against an 8 Mbps target. This is likely caused by high PRB utilization from concentrated traffic load, poor SINR conditions, or suboptimal scheduling parameters reducing spectral efficiency.")
        if rrc > 0:
            pts.append(f"4. **Traffic Load Correlation**: Average RRC connected users of {rrc:.0f} combined with the above KPI violations suggests the cell is operating near or beyond its capacity envelope, causing resource contention that amplifies both drop rate and throughput degradation.")
        if len(pts) < 3:
            pts.append(f"{len(pts)+1}. **Multi-KPI Correlation**: The simultaneous degradation across drop rate ({drop:.2f}%), CSSR ({cssr:.2f}%), and throughput ({tput:.2f} Mbps) points to a systemic issue rather than isolated faults — likely RF coverage or capacity related requiring parameter optimization.")
        root_cause = '\n'.join(pts[:4])

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

    # Get RF parameters
    rf = {}
    try:
        r = _sql("""SELECT AVG(bandwidth_mhz) AS bw, AVG(antenna_gain_dbi) AS gain,
                           AVG(rf_power_eirp_dbm) AS eirp, AVG(e_tilt_degree) AS tilt,
                           AVG(crs_gain) AS crs, AVG(antenna_height_agl_m) AS height
                    FROM telecom_sites WHERE site_id=:sid""", {"sid": t.site_id})
        if r: rf = r[0]
    except: pass

    bw = _f(rf.get("bw"), 1); tilt = _f(rf.get("tilt"), 1); eirp = _f(rf.get("eirp"), 1)
    crs = _f(rf.get("crs"), 1); height = _f(rf.get("height"), 1)

    system_msg = """You are a senior RF optimization engineer. You give ONLY concrete RF parameter change recommendations.

STRICT RULES:
- Output ONLY numbered points (1. 2. 3. etc.) — nothing else
- Each point changes exactly ONE RF parameter from the Current Parameters list
- Use this exact format: **[Parameter Name] Adjustment**: The current [parameter] is [value]. Change to [new value] to [reason]. This will improve [KPI] by approximately [X%].
- Do NOT include any thinking, reasoning, hedging, disclaimers, or meta-commentary
- Do NOT say "I must", "I should", "though", "however", "let me", or explain your thought process
- Do NOT add summaries, conclusions, or headers
- Choose parameters ONLY from: Bandwidth, E-tilt, EIRP, CRS Gain, Antenna Height
- If a parameter value is N/A, skip it and pick another parameter"""

    prompt = f"""Provide exactly 3-4 RF parameter change recommendations for {'cell '+target if target!='site' else 'site '+t.site_id}.

Root Cause: {root_cause if root_cause else 'KPI degradation detected — high call drop rate and low throughput.'}

Current Parameters:
- Bandwidth: {bw} MHz
- E-tilt: {tilt}°
- EIRP: {eirp} dBm
- CRS Gain: {crs}
- Antenna Height: {height} m

Write ONLY the numbered recommendations. No other text."""

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
                temperature=0.15, max_tokens=800,
            )
            raw = (resp.choices[0].message.content or "").strip()
            # Strip leaked reasoning lines but keep all substantive content
            import re
            lines = raw.split('\n')
            clean = []
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                if re.match(r'^(I |Let me|Though |However |Note:|Wait|Hmm|Ok |Sure)', stripped, re.IGNORECASE):
                    continue
                clean.append(line)
            recommendation = '\n'.join(clean) if clean else raw
    except Exception as e:
        _LOG.error("Recommendation AI call failed for ticket %s: %s", ticket_id, e)
        # Fallback: build concrete 4-point parameter recommendations from actual values
        pts = []
        try:
            bw_val = float(bw) if bw and bw != 'N/A' else None
        except (ValueError, TypeError):
            bw_val = None
        try:
            tilt_val = float(tilt) if tilt and tilt != 'N/A' else None
        except (ValueError, TypeError):
            tilt_val = None
        try:
            eirp_val = float(eirp) if eirp and eirp != 'N/A' else None
        except (ValueError, TypeError):
            eirp_val = None
        try:
            crs_val = float(crs) if crs and crs != 'N/A' else None
        except (ValueError, TypeError):
            crs_val = None
        try:
            height_val = float(height) if height and height != 'N/A' else None
        except (ValueError, TypeError):
            height_val = None

        if bw_val is not None:
            new_bw = bw_val + 5 if bw_val < 20 else bw_val + 10
            pts.append(f"1. **Bandwidth Expansion**: The current bandwidth is {bw} MHz. Increase to {_f(new_bw,0)} MHz to reduce PRB congestion and improve DL throughput. This will improve DL User Throughput by approximately 15-25%.")
        if tilt_val is not None:
            new_tilt = max(tilt_val - 1, 0)
            pts.append(f"{len(pts)+1}. **E-tilt Optimization**: The current E-tilt is {tilt}°. Adjust to {_f(new_tilt,1)}° to reduce overshooting and minimize inter-cell interference. This will improve E-RAB Drop Rate by approximately 20-30%.")
        if eirp_val is not None:
            new_eirp = eirp_val + 2
            pts.append(f"{len(pts)+1}. **EIRP Power Increase**: The current EIRP is {eirp} dBm. Increase to {_f(new_eirp,1)} dBm to strengthen signal coverage in weak spots. This will improve CSSR by approximately 1-2% and reduce edge-user drops.")
        if crs_val is not None:
            new_crs = round(crs_val + 1)
            pts.append(f"{len(pts)+1}. **CRS Gain Boost**: The current CRS Gain is {crs}. Increase to {new_crs} to improve reference signal quality and channel estimation accuracy. This will reduce DL retransmissions and improve throughput by 10-15%.")
        if height_val is not None and len(pts) < 4:
            pts.append(f"{len(pts)+1}. **Antenna Height Review**: Current antenna height is {height} m. Consider adjusting by +/-2m combined with mechanical tilt to optimize the coverage footprint and reduce pilot pollution in overlapping zones.")
        recommendation = '\n'.join(pts[:4]) if pts else f"1. **Parameter Review Required**: Site {t.site_id} requires RF parameter audit. Current KPIs (Drop={t.avg_drop_rate}%, CSSR={t.avg_cssr}%) indicate systemic degradation."

    if target == "site":
        t.recommendation = recommendation
        t.updated_at = datetime.utcnow()
        db.session.commit()

    return jsonify({"recommendation": recommendation, "target": target})


@network_issues_bp.route("/api/network-issues/trigger-job", methods=["POST"])
@jwt_required()
def trigger_network_issue_job():
    """Manual trigger: refreshes worst-cell pre-scan AND creates/updates tickets."""
    run_pre_scan()
    count = run_daily_network_issue_job()
    return jsonify({"success": True, "message": f"Worst cells refreshed. {count} ticket(s) created/updated.", "tickets_processed": count})


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
