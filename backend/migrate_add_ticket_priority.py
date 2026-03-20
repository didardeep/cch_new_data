"""
Migration: Add priority column to tickets table.

New column:
  tickets.priority   VARCHAR(20)  DEFAULT 'medium'  -- critical/high/medium/low
  (final priority = max(severity, user_type_floor))

Run once against an existing database:
    python migrate_add_ticket_priority.py
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
    ("tickets", "priority", "VARCHAR(20) NOT NULL DEFAULT 'medium'"),
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
