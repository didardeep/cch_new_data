"""
Standalone diagnostic for the network overview dashboard.

Runs the same SQL the /api/network/overview-stats endpoint runs and prints
the raw row counts + computed averages, so you can see whether the dashboard
"no data" symptom is caused by:

    (a) kpi_data being empty                    → seed/upload needed
    (b) date window missing the data            → max_date logic problem
    (c) KPI name patterns not matching uploads  → ILIKE fix needed
    (d) something else                           → SQL error printed

Usage (from backend/):
    python diagnose_overview.py
"""

import os
import sys
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = (os.environ.get("DATABASE_URL")
                or "postgresql://postgres:root@localhost:5432/telecom_cch")

import psycopg2
from psycopg2.extras import RealDictCursor


def main():
    print(f"DB: {DATABASE_URL}")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1. Row counts
    print("\n=== TABLE COUNTS ===")
    for tbl in ("kpi_data", "telecom_sites"):
        try:
            cur.execute(f"SELECT COUNT(*) AS n FROM {tbl}")
            print(f"  {tbl:20s} = {cur.fetchone()['n']:,}")
        except Exception as e:
            print(f"  {tbl:20s} ERROR: {e}")
            conn.rollback()

    # 2. kpi_data shape
    print("\n=== kpi_data SHAPE ===")
    try:
        cur.execute("""
            SELECT
                COUNT(DISTINCT site_id)    AS sites,
                COUNT(DISTINCT kpi_name)   AS kpis,
                COUNT(DISTINCT data_level) AS lvls,
                MIN(date)::text             AS min_date,
                MAX(date)::text             AS max_date
            FROM kpi_data
        """)
        r = cur.fetchone()
        print(f"  distinct sites:      {r['sites']:,}")
        print(f"  distinct kpi_names:  {r['kpis']:,}")
        print(f"  distinct data_level: {r['lvls']}")
        print(f"  date range:          {r['min_date']} → {r['max_date']}")
    except Exception as e:
        print(f"  ERROR: {e}"); conn.rollback()

    # 3. data_level values
    print("\n=== data_level VALUES ===")
    try:
        cur.execute("SELECT data_level, COUNT(*) AS n FROM kpi_data GROUP BY data_level ORDER BY n DESC")
        for row in cur.fetchall():
            print(f"  {repr(row['data_level']):20s} → {row['n']:,}")
    except Exception as e:
        print(f"  ERROR: {e}"); conn.rollback()

    # 4. Sample kpi_names (first 30)
    print("\n=== SAMPLE kpi_names ===")
    try:
        cur.execute("SELECT DISTINCT kpi_name FROM kpi_data ORDER BY kpi_name LIMIT 30")
        for row in cur.fetchall():
            print(f"  {row['kpi_name']}")
    except Exception as e:
        print(f"  ERROR: {e}"); conn.rollback()

    # 5. Run the overview aggregation query
    print("\n=== OVERVIEW AGGREGATION (exactly what backend runs) ===")
    try:
        cur.execute("SELECT MAX(date) AS md FROM kpi_data")
        max_date = cur.fetchone()["md"]
        if not max_date:
            print("  kpi_data is empty — that's why the dashboard is blank.")
            return
        start = max_date - timedelta(days=30)
        end = max_date
        print(f"  window: {start} → {end}")

        cur.execute(f"""
            SELECT k.site_id,
                   AVG(CASE WHEN k.kpi_name ILIKE '%dl%prb%util%'
                            THEN k.value END) AS dl_prb,
                   AVG(CASE WHEN k.kpi_name ILIKE '%ul%prb%util%'
                            THEN k.value END) AS ul_prb,
                   AVG(CASE WHEN k.kpi_name ILIKE '%dl%usr%ave%throughput%'
                              OR k.kpi_name ILIKE '%dl%user%ave%throughput%'
                            THEN k.value END) AS usr_tput,
                   AVG(CASE WHEN k.kpi_name ILIKE '%dl%cell%ave%throughput%'
                              OR k.kpi_name ILIKE '%dl%cell%tput%'
                            THEN k.value END) AS cell_tput,
                   AVG(CASE WHEN k.kpi_name ILIKE '%e-rab%call%drop%'
                              OR k.kpi_name ILIKE '%call%drop%rate%'
                            THEN k.value END) AS drop_rate,
                   AVG(CASE WHEN k.kpi_name ILIKE '%rrc%connected%'
                              OR k.kpi_name ILIKE '%ave%rrc%'
                            THEN k.value END) AS rrc,
                   AVG(CASE WHEN k.kpi_name ILIKE '%availability%'
                            THEN k.value END) AS avail,
                   AVG(CASE WHEN k.kpi_name ILIKE '%dl%data%total%volume%'
                              OR k.kpi_name ILIKE '%dl%volume%'
                            THEN k.value END) AS dl_vol,
                   AVG(CASE WHEN k.kpi_name ILIKE '%call%setup%success%'
                              OR k.kpi_name ILIKE '%cssr%'
                            THEN k.value END) AS cssr
            FROM kpi_data k
            WHERE k.value IS NOT NULL
              AND (k.data_level = 'site' OR k.data_level IS NULL OR k.data_level = '')
              AND k.date >= %s AND k.date <= %s
            GROUP BY k.site_id
        """, (start, end))
        rows = cur.fetchall()
        print(f"  rows (sites with data): {len(rows)}")

        if not rows:
            print("\n  [!] AGGREGATION RETURNED ZERO ROWS")
            print("      → check: data_level values, date window, KPI name patterns")
            # Try without data_level filter
            cur.execute("""
                SELECT COUNT(*) AS n FROM kpi_data
                WHERE value IS NOT NULL AND date >= %s AND date <= %s
            """, (start, end))
            print(f"      kpi_data rows in window (no data_level filter): {cur.fetchone()['n']:,}")
            return

        # Network-wide means (same math the endpoint does)
        def avg_col(col):
            xs = [float(r[col]) for r in rows if r[col] is not None]
            return sum(xs) / len(xs) if xs else None

        print("\n  Network-wide averages:")
        for col in ("dl_prb", "ul_prb", "usr_tput", "cell_tput",
                    "drop_rate", "rrc", "avail", "dl_vol", "cssr"):
            v = avg_col(col)
            print(f"    {col:12s} = {v if v is None else round(v, 3)}")

        # Sample per-site rows
        print("\n  Sample per-site (first 3):")
        for r in rows[:3]:
            print(f"    {r['site_id']}: "
                  f"dl_prb={r['dl_prb']}, drop={r['drop_rate']}, "
                  f"tput={r['usr_tput']}, cssr={r['cssr']}")
    except Exception as e:
        print(f"  ERROR: {e}"); conn.rollback()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
