"""
One-time seed: create an AI ticket assigned to Gunjan Kaur (gunjankaur@kpmg.com)
with realistic kpi_data + telecom_sites so AI diagnosis works.

Called once at startup — skips if ticket already exists.
Does NOT touch any existing logic or tables beyond inserting rows.
"""

from datetime import datetime, timedelta, date as _date
import random, math

GUNJAN_EMAIL = "gunjankaur@kpmg.com"

# ── Site details ─────────────────────────────────────────────────────────────
SITE_ID     = "GUR_LTE_0500"
SITE_NAME   = "GUR_LTE_0500"
ZONE        = "Gurgaon_South"
CITY        = "Gurgaon"
STATE       = "Haryana"
LATITUDE    = 28.4230
LONGITUDE   = 77.0430

# 4 cells in this site (typical LTE tri-sector + 1 capacity layer)
CELLS = [
    {"cell_id": "cell_A1", "cell_site_id": "GUR_LTE_0500_A1"},
    {"cell_id": "cell_A2", "cell_site_id": "GUR_LTE_0500_A2"},
    {"cell_id": "cell_B1", "cell_site_id": "GUR_LTE_0500_B1"},
    {"cell_id": "cell_B2", "cell_site_id": "GUR_LTE_0500_B2"},
]

# RF parameters for RCA / recommendations
RF_PARAMS = {
    "bandwidth_mhz": 10,
    "antenna_gain_dbi": 18.0,
    "rf_power_eirp_dbm": 46.0,
    "antenna_height_agl_m": 30.0,
    "e_tilt_degree": 4.0,
    "crs_gain": 0.0,
}

# KPI baselines — degraded values so the site qualifies as "worst cell"
#   Drop Rate > 1.5 %, CSSR < 98.5 %, Throughput < 8 Mbps
KPI_PROFILES = {
    "E-RAB Call Drop Rate_1":       {"mean": 2.8,  "std": 0.6,  "clamp": (0.5, 6.0)},
    "LTE Call Setup Success Rate":  {"mean": 96.5, "std": 1.0,  "clamp": (90.0, 99.5)},
    "LTE DL - Usr Ave Throughput":  {"mean": 5.5,  "std": 1.2,  "clamp": (1.0, 12.0)},
    "Ave RRC Connected Ue":         {"mean": 350,  "std": 80,   "clamp": (50, 1200)},
    "Max RRC Connected Ue":         {"mean": 600,  "std": 120,  "clamp": (100, 2000)},
}

DAYS_BACK = 14  # 14 days of history for trends


def _rand(profile):
    v = random.gauss(profile["mean"], profile["std"])
    lo, hi = profile["clamp"]
    return round(max(lo, min(hi, v)), 4)


def seed_gunjan_ticket(app):
    """Insert ticket + supporting data.  Idempotent — skips if already done."""
    from models import db, User, KpiData, TelecomSite
    from network_issues import NetworkIssueTicket

    with app.app_context():
        # ── 1. Find Gunjan Kaur ──────────────────────────────────────────
        gunjan = User.query.filter_by(email=GUNJAN_EMAIL).first()
        if not gunjan:
            print(f"[SEED] User {GUNJAN_EMAIL} not found — skipping seed.")
            return

        # ── 2. Already seeded? ───────────────────────────────────────────
        existing = NetworkIssueTicket.query.filter_by(
            site_id=SITE_ID, assigned_agent=gunjan.id
        ).filter(NetworkIssueTicket.status.in_(["open", "in_progress"])).first()
        if existing:
            print(f"[SEED] Ticket already exists (#{existing.id}) — skipping.")
            return

        print(f"[SEED] Seeding AI ticket for {gunjan.name} ({GUNJAN_EMAIL}) ...")

        # ── 3. Seed telecom_sites (one per cell) ────────────────────────
        for c in CELLS:
            exists = TelecomSite.query.filter_by(
                site_id=SITE_ID, cell_id=c["cell_id"]
            ).first()
            if not exists:
                db.session.add(TelecomSite(
                    site_id=SITE_ID, site_name=SITE_NAME,
                    cell_id=c["cell_id"],
                    latitude=LATITUDE, longitude=LONGITUDE,
                    zone=ZONE, city=CITY, state=STATE,
                    technology="LTE", site_status="on_air",
                    **RF_PARAMS,
                ))
        db.session.flush()
        print(f"[SEED]   telecom_sites: {len(CELLS)} cell entries")

        # ── 4. Seed kpi_data (cell-level + site-level, 14 days) ─────────
        today = _date.today()
        kpi_count = 0
        for day_offset in range(DAYS_BACK):
            d = today - timedelta(days=day_offset)
            for kpi_name, profile in KPI_PROFILES.items():
                # Site-level aggregate
                site_val = _rand(profile)
                db.session.add(KpiData(
                    site_id=SITE_ID, kpi_name=kpi_name,
                    date=d, hour=0, value=site_val,
                    data_level="site", cell_id=None, cell_site_id=None,
                ))
                kpi_count += 1
                # Cell-level per cell
                for c in CELLS:
                    cell_val = _rand(profile)
                    db.session.add(KpiData(
                        site_id=SITE_ID, kpi_name=kpi_name,
                        date=d, hour=0, value=cell_val,
                        data_level="cell",
                        cell_id=c["cell_id"],
                        cell_site_id=c["cell_site_id"],
                    ))
                    kpi_count += 1
        db.session.flush()
        print(f"[SEED]   kpi_data: {kpi_count} rows ({DAYS_BACK} days x {len(KPI_PROFILES)} KPIs x {1 + len(CELLS)} levels)")

        # ── 5. Compute averages for the ticket ──────────────────────────
        avg_drop = round(sum(_rand(KPI_PROFILES["E-RAB Call Drop Rate_1"]) for _ in range(7)) / 7, 2)
        avg_cssr = round(sum(_rand(KPI_PROFILES["LTE Call Setup Success Rate"]) for _ in range(7)) / 7, 2)
        avg_tput = round(sum(_rand(KPI_PROFILES["LTE DL - Usr Ave Throughput"]) for _ in range(7)) / 7, 2)
        avg_rrc  = round(sum(_rand(KPI_PROFILES["Ave RRC Connected Ue"]) for _ in range(7)) / 7, 1)
        max_rrc  = round(max(_rand(KPI_PROFILES["Max RRC Connected Ue"]) for _ in range(7)), 1)

        # Priority (same logic as network_issues._calc_priority)
        severity = 3  # Severe Worst
        rev = 52.0    # moderate revenue
        rev_score = 2 if rev >= 45 else 1
        avg_rrc_score = 2 if avg_rrc >= 300 else 1
        max_rrc_score = 2 if max_rrc >= 500 else 1
        score = severity + rev_score + avg_rrc_score + max_rrc_score
        if score >= 10:
            priority, sla = "Critical", 2
        elif score >= 7:
            priority, sla = "High", 4
        elif score >= 4:
            priority, sla = "Medium", 8
        else:
            priority, sla = "Low", 16

        now_08 = datetime.combine(today, datetime.min.time()).replace(hour=8)

        # ── 6. Create the ticket ─────────────────────────────────────────
        ticket = NetworkIssueTicket(
            site_id=SITE_ID,
            cells_affected=",".join(c["cell_id"] for c in CELLS),
            cell_site_ids=",".join(c["cell_site_id"] for c in CELLS),
            category="Severe Worst",
            priority=priority,
            priority_score=score,
            sla_hours=sla,
            avg_rrc=avg_rrc,
            max_rrc=max_rrc,
            revenue_total=rev,
            avg_drop_rate=avg_drop,
            avg_cssr=avg_cssr,
            avg_tput=avg_tput,
            violations=3,
            status="open",
            assigned_agent=gunjan.id,
            zone=ZONE,
            location=f"{CITY}, {STATE}",
            created_at=now_08,
            updated_at=now_08,
            deadline_time=now_08 + timedelta(hours=sla),
        )
        db.session.add(ticket)
        db.session.commit()

        print(f"[SEED]   Ticket #{ticket.id}: {SITE_ID}, {len(CELLS)} cells, "
              f"priority={priority} (score={score}), SLA={sla}h, "
              f"assigned to {gunjan.name} (id={gunjan.id})")
        print(f"[SEED] Done!")
