"""
migrate_fix_kpi_data.py
-----------------------
One-time migration to fix data quality issues in kpi_data without a full reseed.

Fixes applied:
  1. KPI name: 'UL PRB Utilization (1BH'  →  'UL PRB Utilization (1BH)'
  2. Remove future-dated rows (date > CURRENT_DATE) — these are invisible to all
     chart queries but waste space and cause misleading row counts.
  3. Create composite index idx_kpi_full_lookup if it doesn't already exist.

Run once:
    cd backend && python migrate_fix_kpi_data.py
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/telecom_cch")


def run():
    print(f"Connecting to: {DB_URL}")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # ── 1. Fix UL PRB KPI name (missing closing parenthesis) ──────────────────
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE kpi_name = 'UL PRB Utilization (1BH'")
    bad_count = cur.fetchone()[0]
    if bad_count > 0:
        print(f"[1] Fixing {bad_count:,} rows: 'UL PRB Utilization (1BH' → 'UL PRB Utilization (1BH)'")
        cur.execute(
            "UPDATE kpi_data SET kpi_name = 'UL PRB Utilization (1BH)' "
            "WHERE kpi_name = 'UL PRB Utilization (1BH)'"
        )
        # Correct the typo above — actual broken name has no closing paren
        cur.execute(
            "UPDATE kpi_data SET kpi_name = 'UL PRB Utilization (1BH)' "
            "WHERE kpi_name = 'UL PRB Utilization (1BH'"
        )
        print(f"    ✅ Updated {cur.rowcount:,} rows")
    else:
        print("[1] UL PRB name already correct — skipping")

    # ── 2. Remove future-dated rows ────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM kpi_data WHERE date > CURRENT_DATE")
    future_count = cur.fetchone()[0]
    if future_count > 0:
        print(f"[2] Deleting {future_count:,} future-dated rows (date > CURRENT_DATE)")
        cur.execute("DELETE FROM kpi_data WHERE date > CURRENT_DATE")
        print(f"    ✅ Deleted {cur.rowcount:,} rows")
    else:
        print("[2] No future-dated rows — skipping")

    conn.commit()
    print("Committed.")

    # ── 3. Create composite index (outside transaction — DDL) ─────────────────
    conn.autocommit = True
    print("[3] Creating idx_kpi_full_lookup (site_id, kpi_name, data_level, date) ...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_kpi_full_lookup
        ON kpi_data (site_id, kpi_name, data_level, date)
    """)
    print("    ✅ Index created (or already existed)")

    cur.close()
    conn.close()
    print("\nAll fixes applied successfully.")


if __name__ == "__main__":
    run()
