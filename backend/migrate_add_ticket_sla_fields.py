"""
Migration: Add SLA tracking fields to tickets table.

New columns:
  tickets.first_response_at  TIMESTAMPTZ NULL
  tickets.reopened_count     INTEGER     NOT NULL DEFAULT 0
  tickets.last_reopened_at   TIMESTAMPTZ NULL

Run once against an existing database:
    python migrate_add_ticket_sla_fields.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/telecom_complaints",
)

ADD_COLUMNS = [
    ("tickets", "first_response_at", "TIMESTAMPTZ DEFAULT NULL"),
    ("tickets", "reopened_count",    "INTEGER NOT NULL DEFAULT 0"),
    ("tickets", "last_reopened_at",  "TIMESTAMPTZ DEFAULT NULL"),
]


def column_exists(cur, table, column):
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone() is not None


conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
cur = conn.cursor()

try:
    for table, column, definition in ADD_COLUMNS:
        if column_exists(cur, table, column):
            print(f"  [SKIP] {table}.{column} already exists.")
        else:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            print(f"  [OK]   Added {table}.{column}")
    conn.commit()
    print("\n[DONE] Migration complete.")
except Exception as e:
    conn.rollback()
    print(f"\n[FAIL] Migration failed: {e}")
    raise
finally:
    cur.close()
    conn.close()
