"""
Seed script — populates telecom_sites + kpi_data (site & cell level) with
realistic dummy data matching the production schema.

Usage:
    python seed_data.py

Set DATABASE_URL env var or it reads from .env automatically.
Inserts ~25M rows in batches using raw SQL for speed.
"""

import os
import sys
import time
import random
import math
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SQLALCHEMY_DATABASE_URI")
if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL in .env")
    sys.exit(1)

import psycopg2
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
NUM_SITES     = 1500
CELLS_PER_SITE = 6
ZONES         = [("CBD", 500), ("Urban", 600), ("Edge", 400)]  # zone, count
LAT_MIN, LAT_MAX = 28.3699, 28.5470
LON_MIN, LON_MAX = 76.9272, 77.1282

# Date ranges
SITE_DATE_START = date(2026, 1, 20)
SITE_DATE_END   = date(2026, 4, 23)   # 94 days
CELL_DATE_START = date(2026, 1, 20)
CELL_DATE_END   = date(2026, 4, 22)   # 93 days

BATCH_SIZE = 5000  # rows per INSERT

# 27 KPIs: (name, mean, stddev, min_val, max_val)
KPIS = [
    ("Availability",                   99.83,   3.72,    0.0,   100.0),
    ("Ave RRC Connected Ue",         1424.20, 3617.85,   0.0, 65792.58),
    ("Average Act UE DL Per Cell",    109.25,  361.90,   0.0,  7270.13),
    ("Average Act UE UL Per Cell",     82.47,  209.17,   0.0,  6666.23),
    ("Average Latency Downlink",       23.25,   43.01,   0.0,   393.0),
    ("Average NI of Carrier-",       -108.32,    8.82, -120.0,    0.0),
    ("CSFB Access Success Rate",       98.61,   11.11,   0.0,   106.25),
    ("DL Data Total Volume",           63.62,   72.37,   0.0,   463.51),
    ("DL PRB Utilization (1BH)",       50.47,   26.90,   0.0,    99.54),
    ("E-RAB Call Drop Rate_1",          0.29,    0.84,   0.0,   100.0),
    ("Inter-eNBS1HO Success Rate",     59.21,   44.85,   0.0,   121.05),
    ("Inter-eNBX2HO Success Rate",     90.85,   14.76,   0.0,   100.0),
    ("Intra-eNB HO Success Rate",      97.53,    8.67,   0.0,   100.0),
    ("LTE Call Setup Success Rate",    99.17,    6.65,   0.0,   100.0),
    ("LTE DL - Cell Ave Throughput",   22.58,    4.79,   0.0,    52.49),
    ("LTE DL - Usr Ave Throughput",    14.21,   12.42,   0.0,   301.39),
    ("LTE E-RAB Setup Success Rate",   99.33,    6.64,   0.0,   100.0),
    ("LTE Intra-Freq HO Success Rate", 93.65,   10.85,   0.0,   100.0),
    ("LTE RRC Setup Success Rate",     99.27,    5.54,   0.0,   100.0),
    ("LTE UL - Cell Ave Throughput",    1.94,    0.87,   0.0,    33.33),
    ("LTE UL - User Ave Throughput",    0.78,    0.81,   0.0,    42.15),
    ("Max RRC Connected Ue",          210.11,  342.99,   0.0,  7808.0),
    ("UL Data Total Volume",           37.57,   39.15,   0.0,   398.41),
    ("UL PRB Utilization (1BH",        17.67,   14.83,   0.0,    88.68),
    ("VoLTE Traffic DL",                0.052,   0.035,  0.0,     0.52),
    ("VoLTE Traffic Erlang",            2.57,    2.76,   0.0,    27.69),
    ("VoLTE Traffic UL",                0.039,   0.028,  0.0,     0.22),
]


def gen_value(mean, std, lo, hi):
    """Generate a realistic random value using truncated normal distribution."""
    v = random.gauss(mean, std)
    return round(max(lo, min(hi, v)), 4)


def date_range(start, end):
    """Yield dates from start to end inclusive."""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    t0 = time.time()

    # ── 1. Create tables if not exist ──────────────────────────────────────────
    print("Ensuring tables exist...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS telecom_sites (
            id SERIAL PRIMARY KEY,
            site_id VARCHAR(50) NOT NULL,
            site_name VARCHAR(100),
            cell_id VARCHAR(100),
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            zone VARCHAR(100) DEFAULT '',
            city VARCHAR(100),
            state VARCHAR(100),
            site_status VARCHAR(20) DEFAULT 'on_air',
            alarms TEXT DEFAULT '',
            solution TEXT DEFAULT '',
            standard_solution_step TEXT DEFAULT '',
            bandwidth_mhz DOUBLE PRECISION,
            antenna_gain_dbi DOUBLE PRECISION,
            rf_power_eirp_dbm DOUBLE PRECISION,
            antenna_height_agl_m DOUBLE PRECISION,
            e_tilt_degree DOUBLE PRECISION,
            crs_gain DOUBLE PRECISION
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_data (
            id SERIAL PRIMARY KEY,
            site_id VARCHAR(100) NOT NULL,
            kpi_name VARCHAR(200) NOT NULL,
            value DOUBLE PRECISION,
            date DATE,
            hour INTEGER DEFAULT 0,
            data_level VARCHAR(10) DEFAULT 'site',
            cell_id VARCHAR(100),
            cell_site_id VARCHAR(100)
        );
    """)
    conn.commit()

    # ── 2. Clear existing data ─────────────────────────────────────────────────
    print("Clearing existing cell-level KPI data...")
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE data_level='cell'")
    old_count = cur.fetchone()[0]
    cur.execute("DELETE FROM kpi_data WHERE data_level='cell'")
    conn.commit()
    print(f"  Cleared {old_count:,} existing cell-level rows.")

    print("Clearing existing telecom_sites...")
    cur.execute("SELECT COUNT(*) FROM telecom_sites")
    old_ts = cur.fetchone()[0]
    cur.execute("DELETE FROM telecom_sites")
    conn.commit()
    print(f"  Cleared {old_ts:,} existing telecom_sites rows.")

    # Note: site-level KPI data is preserved. To clear it too, uncomment:
    # cur.execute("DELETE FROM kpi_data WHERE data_level='site'")
    # conn.commit()

    # ── 3. Seed telecom_sites ──────────────────────────────────────────────────
    print(f"Seeding {NUM_SITES} sites x {CELLS_PER_SITE} cells = {NUM_SITES * CELLS_PER_SITE} telecom_sites rows...")

    site_ids = []
    zone_list = []
    for zone_name, count in ZONES:
        zone_list.extend([zone_name] * count)

    sites_data = []
    for i in range(1, NUM_SITES + 1):
        sid = f"GUR_LTE_{i:04d}"
        site_ids.append(sid)
        zone = zone_list[i - 1]
        lat = round(random.uniform(LAT_MIN, LAT_MAX), 6)
        lon = round(random.uniform(LON_MIN, LON_MAX), 6)

        for c in range(1, CELLS_PER_SITE + 1):
            cell_site_id = f"{sid}_{c}"
            sites_data.append((
                sid, sid, cell_site_id, lat, lon, zone,
                None, None, "on_air", "", "", "",
                None, None, None, None, None, None
            ))

    execute_values(cur, """
        INSERT INTO telecom_sites
            (site_id, site_name, cell_id, latitude, longitude, zone,
             city, state, site_status, alarms, solution, standard_solution_step,
             bandwidth_mhz, antenna_gain_dbi, rf_power_eirp_dbm,
             antenna_height_agl_m, e_tilt_degree, crs_gain)
        VALUES %s
    """, sites_data, page_size=2000)
    conn.commit()
    print(f"  Inserted {len(sites_data)} telecom_sites rows.")

    # ── 4. Create indexes on kpi_data for fast queries ──────────────────────
    print("Creating indexes (will speed up queries later)...")
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_kpi_site_id ON kpi_data (site_id)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_name ON kpi_data (kpi_name)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_level ON kpi_data (data_level)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_date ON kpi_data (date)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_cell_site ON kpi_data (cell_site_id)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_site_name_level ON kpi_data (site_id, kpi_name, data_level)",
    ]:
        try:
            cur.execute(idx_sql)
        except Exception:
            conn.rollback()
    conn.commit()

    # ── 5. Seed site-level KPI data ────────────────────────────────────────────
    site_dates = list(date_range(SITE_DATE_START, SITE_DATE_END))
    total_site_rows = NUM_SITES * len(KPIS) * len(site_dates)
    print(f"Seeding site-level KPI data: {NUM_SITES} sites x {len(KPIS)} KPIs x {len(site_dates)} dates = {total_site_rows:,} rows...")

    batch = []
    inserted = 0
    for si, sid in enumerate(site_ids):
        for kpi_name, mean, std, lo, hi in KPIS:
            # Give each site a slightly different baseline
            site_offset = random.gauss(0, std * 0.1)
            for d in site_dates:
                v = gen_value(mean + site_offset, std, lo, hi)
                batch.append((sid, kpi_name, v, d, 0, "site", None, None))

                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
                        VALUES %s
                    """, batch, page_size=BATCH_SIZE)
                    conn.commit()
                    inserted += len(batch)
                    batch = []
                    if inserted % 100000 == 0:
                        elapsed = time.time() - t0
                        pct = inserted / total_site_rows * 100
                        print(f"  Site-level: {inserted:>10,} / {total_site_rows:,} ({pct:.1f}%) — {elapsed:.0f}s")

    if batch:
        execute_values(cur, """
            INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
            VALUES %s
        """, batch, page_size=BATCH_SIZE)
        conn.commit()
        inserted += len(batch)
        batch = []

    print(f"  Done: {inserted:,} site-level rows inserted.")

    # ── 6. Seed cell-level KPI data ────────────────────────────────────────────
    cell_dates = list(date_range(CELL_DATE_START, CELL_DATE_END))
    total_cell_rows = NUM_SITES * CELLS_PER_SITE * len(KPIS) * len(cell_dates)
    print(f"Seeding cell-level KPI data: {NUM_SITES} sites x {CELLS_PER_SITE} cells x {len(KPIS)} KPIs x {len(cell_dates)} dates = {total_cell_rows:,} rows...")

    inserted = 0
    for si, sid in enumerate(site_ids):
        for cell_num in range(1, CELLS_PER_SITE + 1):
            cell_id = str(cell_num)
            cell_site_id = f"{sid}_{cell_num}"

            for kpi_name, mean, std, lo, hi in KPIS:
                # Per-cell baseline variation
                cell_offset = random.gauss(0, std * 0.15)
                for d in cell_dates:
                    v = gen_value(mean + cell_offset, std, lo, hi)
                    batch.append((sid, kpi_name, v, d, 0, "cell", cell_id, cell_site_id))

                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
                            VALUES %s
                        """, batch, page_size=BATCH_SIZE)
                        conn.commit()
                        inserted += len(batch)
                        batch = []
                        if inserted % 500000 == 0:
                            elapsed = time.time() - t0
                            pct = inserted / total_cell_rows * 100
                            print(f"  Cell-level: {inserted:>12,} / {total_cell_rows:,} ({pct:.1f}%) — {elapsed:.0f}s")

        # Progress per site batch (every 100 sites)
        if (si + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"  Sites processed: {si + 1}/{NUM_SITES} — {elapsed:.0f}s")

    if batch:
        execute_values(cur, """
            INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
            VALUES %s
        """, batch, page_size=BATCH_SIZE)
        conn.commit()
        inserted += len(batch)

    print(f"  Done: {inserted:,} cell-level rows inserted.")

    # ── 7. Final summary ──────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM telecom_sites")
    ts_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE data_level='site'")
    site_kpi_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE data_level='cell'")
    cell_kpi_count = cur.fetchone()[0]

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"SEED COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  telecom_sites:     {ts_count:>12,}")
    print(f"  kpi_data (site):   {site_kpi_count:>12,}")
    print(f"  kpi_data (cell):   {cell_kpi_count:>12,}")
    print(f"  kpi_data (total):  {site_kpi_count + cell_kpi_count:>12,}")
    print(f"{'='*60}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
"""
Seed script — populates telecom_sites + kpi_data (site & cell level) with
realistic dummy data matching the production schema.

Usage:
    python seed_data.py

Set DATABASE_URL env var or it reads from .env automatically.
Inserts ~25M rows in batches using raw SQL for speed.
"""

import os
import sys
import time
import random
import math
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SQLALCHEMY_DATABASE_URI")
if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL in .env")
    sys.exit(1)

import psycopg2
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
NUM_SITES     = 1500
CELLS_PER_SITE = 6
ZONES         = [("CBD", 500), ("Urban", 600), ("Edge", 400)]  # zone, count
LAT_MIN, LAT_MAX = 28.3699, 28.5470
LON_MIN, LON_MAX = 76.9272, 77.1282

# Date ranges
SITE_DATE_START = date(2026, 1, 20)
SITE_DATE_END   = date(2026, 4, 23)   # 94 days
CELL_DATE_START = date(2026, 1, 20)
CELL_DATE_END   = date(2026, 4, 22)   # 93 days

BATCH_SIZE = 5000  # rows per INSERT

# 27 KPIs: (name, mean, stddev, min_val, max_val)
KPIS = [
    ("Availability",                   99.83,   3.72,    0.0,   100.0),
    ("Ave RRC Connected Ue",         1424.20, 3617.85,   0.0, 65792.58),
    ("Average Act UE DL Per Cell",    109.25,  361.90,   0.0,  7270.13),
    ("Average Act UE UL Per Cell",     82.47,  209.17,   0.0,  6666.23),
    ("Average Latency Downlink",       23.25,   43.01,   0.0,   393.0),
    ("Average NI of Carrier-",       -108.32,    8.82, -120.0,    0.0),
    ("CSFB Access Success Rate",       98.61,   11.11,   0.0,   106.25),
    ("DL Data Total Volume",           63.62,   72.37,   0.0,   463.51),
    ("DL PRB Utilization (1BH)",       50.47,   26.90,   0.0,    99.54),
    ("E-RAB Call Drop Rate_1",          0.29,    0.84,   0.0,   100.0),
    ("Inter-eNBS1HO Success Rate",     59.21,   44.85,   0.0,   121.05),
    ("Inter-eNBX2HO Success Rate",     90.85,   14.76,   0.0,   100.0),
    ("Intra-eNB HO Success Rate",      97.53,    8.67,   0.0,   100.0),
    ("LTE Call Setup Success Rate",    99.17,    6.65,   0.0,   100.0),
    ("LTE DL - Cell Ave Throughput",   22.58,    4.79,   0.0,    52.49),
    ("LTE DL - Usr Ave Throughput",    14.21,   12.42,   0.0,   301.39),
    ("LTE E-RAB Setup Success Rate",   99.33,    6.64,   0.0,   100.0),
    ("LTE Intra-Freq HO Success Rate", 93.65,   10.85,   0.0,   100.0),
    ("LTE RRC Setup Success Rate",     99.27,    5.54,   0.0,   100.0),
    ("LTE UL - Cell Ave Throughput",    1.94,    0.87,   0.0,    33.33),
    ("LTE UL - User Ave Throughput",    0.78,    0.81,   0.0,    42.15),
    ("Max RRC Connected Ue",          210.11,  342.99,   0.0,  7808.0),
    ("UL Data Total Volume",           37.57,   39.15,   0.0,   398.41),
    ("UL PRB Utilization (1BH",        17.67,   14.83,   0.0,    88.68),
    ("VoLTE Traffic DL",                0.052,   0.035,  0.0,     0.52),
    ("VoLTE Traffic Erlang",            2.57,    2.76,   0.0,    27.69),
    ("VoLTE Traffic UL",                0.039,   0.028,  0.0,     0.22),
]


def gen_value(mean, std, lo, hi):
    """Generate a realistic random value using truncated normal distribution."""
    v = random.gauss(mean, std)
    return round(max(lo, min(hi, v)), 4)


def date_range(start, end):
    """Yield dates from start to end inclusive."""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    t0 = time.time()

    # ── 1. Create tables if not exist ──────────────────────────────────────────
    print("Ensuring tables exist...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS telecom_sites (
            id SERIAL PRIMARY KEY,
            site_id VARCHAR(50) NOT NULL,
            site_name VARCHAR(100),
            cell_id VARCHAR(100),
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            zone VARCHAR(100) DEFAULT '',
            city VARCHAR(100),
            state VARCHAR(100),
            site_status VARCHAR(20) DEFAULT 'on_air',
            alarms TEXT DEFAULT '',
            solution TEXT DEFAULT '',
            standard_solution_step TEXT DEFAULT '',
            bandwidth_mhz DOUBLE PRECISION,
            antenna_gain_dbi DOUBLE PRECISION,
            rf_power_eirp_dbm DOUBLE PRECISION,
            antenna_height_agl_m DOUBLE PRECISION,
            e_tilt_degree DOUBLE PRECISION,
            crs_gain DOUBLE PRECISION
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_data (
            id SERIAL PRIMARY KEY,
            site_id VARCHAR(100) NOT NULL,
            kpi_name VARCHAR(200) NOT NULL,
            value DOUBLE PRECISION,
            date DATE,
            hour INTEGER DEFAULT 0,
            data_level VARCHAR(10) DEFAULT 'site',
            cell_id VARCHAR(100),
            cell_site_id VARCHAR(100)
        );
    """)
    conn.commit()

    # ── 2. Clear existing data ─────────────────────────────────────────────────
    print("Clearing existing cell-level KPI data...")
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE data_level='cell'")
    old_count = cur.fetchone()[0]
    cur.execute("DELETE FROM kpi_data WHERE data_level='cell'")
    conn.commit()
    print(f"  Cleared {old_count:,} existing cell-level rows.")

    print("Clearing existing telecom_sites...")
    cur.execute("SELECT COUNT(*) FROM telecom_sites")
    old_ts = cur.fetchone()[0]
    cur.execute("DELETE FROM telecom_sites")
    conn.commit()
    print(f"  Cleared {old_ts:,} existing telecom_sites rows.")

    # Note: site-level KPI data is preserved. To clear it too, uncomment:
    # cur.execute("DELETE FROM kpi_data WHERE data_level='site'")
    # conn.commit()

    # ── 3. Seed telecom_sites ──────────────────────────────────────────────────
    print(f"Seeding {NUM_SITES} sites x {CELLS_PER_SITE} cells = {NUM_SITES * CELLS_PER_SITE} telecom_sites rows...")

    site_ids = []
    zone_list = []
    for zone_name, count in ZONES:
        zone_list.extend([zone_name] * count)

    sites_data = []
    for i in range(1, NUM_SITES + 1):
        sid = f"GUR_LTE_{i:04d}"
        site_ids.append(sid)
        zone = zone_list[i - 1]
        lat = round(random.uniform(LAT_MIN, LAT_MAX), 6)
        lon = round(random.uniform(LON_MIN, LON_MAX), 6)

        for c in range(1, CELLS_PER_SITE + 1):
            cell_site_id = f"{sid}_{c}"
            sites_data.append((
                sid, sid, cell_site_id, lat, lon, zone,
                None, None, "on_air", "", "", "",
                None, None, None, None, None, None
            ))

    execute_values(cur, """
        INSERT INTO telecom_sites
            (site_id, site_name, cell_id, latitude, longitude, zone,
             city, state, site_status, alarms, solution, standard_solution_step,
             bandwidth_mhz, antenna_gain_dbi, rf_power_eirp_dbm,
             antenna_height_agl_m, e_tilt_degree, crs_gain)
        VALUES %s
    """, sites_data, page_size=2000)
    conn.commit()
    print(f"  Inserted {len(sites_data)} telecom_sites rows.")

    # ── 4. Create indexes on kpi_data for fast queries ──────────────────────
    print("Creating indexes (will speed up queries later)...")
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_kpi_site_id ON kpi_data (site_id)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_name ON kpi_data (kpi_name)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_level ON kpi_data (data_level)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_date ON kpi_data (date)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_cell_site ON kpi_data (cell_site_id)",
        "CREATE INDEX IF NOT EXISTS idx_kpi_site_name_level ON kpi_data (site_id, kpi_name, data_level)",
    ]:
        try:
            cur.execute(idx_sql)
        except Exception:
            conn.rollback()
    conn.commit()

    # ── 5. Seed site-level KPI data ────────────────────────────────────────────
    site_dates = list(date_range(SITE_DATE_START, SITE_DATE_END))
    total_site_rows = NUM_SITES * len(KPIS) * len(site_dates)
    print(f"Seeding site-level KPI data: {NUM_SITES} sites x {len(KPIS)} KPIs x {len(site_dates)} dates = {total_site_rows:,} rows...")

    batch = []
    inserted = 0
    for si, sid in enumerate(site_ids):
        for kpi_name, mean, std, lo, hi in KPIS:
            # Give each site a slightly different baseline
            site_offset = random.gauss(0, std * 0.1)
            for d in site_dates:
                v = gen_value(mean + site_offset, std, lo, hi)
                batch.append((sid, kpi_name, v, d, 0, "site", None, None))

                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
                        VALUES %s
                    """, batch, page_size=BATCH_SIZE)
                    conn.commit()
                    inserted += len(batch)
                    batch = []
                    if inserted % 100000 == 0:
                        elapsed = time.time() - t0
                        pct = inserted / total_site_rows * 100
                        print(f"  Site-level: {inserted:>10,} / {total_site_rows:,} ({pct:.1f}%) — {elapsed:.0f}s")

    if batch:
        execute_values(cur, """
            INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
            VALUES %s
        """, batch, page_size=BATCH_SIZE)
        conn.commit()
        inserted += len(batch)
        batch = []

    print(f"  Done: {inserted:,} site-level rows inserted.")

    # ── 6. Seed cell-level KPI data ────────────────────────────────────────────
    cell_dates = list(date_range(CELL_DATE_START, CELL_DATE_END))
    total_cell_rows = NUM_SITES * CELLS_PER_SITE * len(KPIS) * len(cell_dates)
    print(f"Seeding cell-level KPI data: {NUM_SITES} sites x {CELLS_PER_SITE} cells x {len(KPIS)} KPIs x {len(cell_dates)} dates = {total_cell_rows:,} rows...")

    inserted = 0
    for si, sid in enumerate(site_ids):
        for cell_num in range(1, CELLS_PER_SITE + 1):
            cell_id = str(cell_num)
            cell_site_id = f"{sid}_{cell_num}"

            for kpi_name, mean, std, lo, hi in KPIS:
                # Per-cell baseline variation
                cell_offset = random.gauss(0, std * 0.15)
                for d in cell_dates:
                    v = gen_value(mean + cell_offset, std, lo, hi)
                    batch.append((sid, kpi_name, v, d, 0, "cell", cell_id, cell_site_id))

                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
                            VALUES %s
                        """, batch, page_size=BATCH_SIZE)
                        conn.commit()
                        inserted += len(batch)
                        batch = []
                        if inserted % 500000 == 0:
                            elapsed = time.time() - t0
                            pct = inserted / total_cell_rows * 100
                            print(f"  Cell-level: {inserted:>12,} / {total_cell_rows:,} ({pct:.1f}%) — {elapsed:.0f}s")

        # Progress per site batch (every 100 sites)
        if (si + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"  Sites processed: {si + 1}/{NUM_SITES} — {elapsed:.0f}s")

    if batch:
        execute_values(cur, """
            INSERT INTO kpi_data (site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
            VALUES %s
        """, batch, page_size=BATCH_SIZE)
        conn.commit()
        inserted += len(batch)

    print(f"  Done: {inserted:,} cell-level rows inserted.")

    # ── 7. Final summary ──────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM telecom_sites")
    ts_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE data_level='site'")
    site_kpi_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE data_level='cell'")
    cell_kpi_count = cur.fetchone()[0]

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"SEED COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  telecom_sites:     {ts_count:>12,}")
    print(f"  kpi_data (site):   {site_kpi_count:>12,}")
    print(f"  kpi_data (cell):   {cell_kpi_count:>12,}")
    print(f"  kpi_data (total):  {site_kpi_count + cell_kpi_count:>12,}")
    print(f"{'='*60}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
