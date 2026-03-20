"""
Migration: Add escalation tracking fields to tickets table.

New columns:
  tickets.escalated_by    INTEGER  NULL  FK → users.id
  tickets.escalated_at    TIMESTAMPTZ NULL
  tickets.escalation_note TEXT     DEFAULT ''

Run once against an existing database:
    python migrate_add_escalation_fields.py
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
    ("tickets", "escalated_by",    "INTEGER DEFAULT NULL REFERENCES users(id)"),
    ("tickets", "escalated_at",    "TIMESTAMPTZ DEFAULT NULL"),
    ("tickets", "escalation_note", "TEXT DEFAULT ''"),
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
