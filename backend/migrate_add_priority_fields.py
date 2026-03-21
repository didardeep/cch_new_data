"""
Migration: Add user_type to users, severity to tickets.

New columns:
  users.user_type    VARCHAR(20)  DEFAULT 'bronze'  -- platinum/gold/silver/bronze
  tickets.severity   VARCHAR(20)  DEFAULT 'medium'  -- critical/high/medium/low

Run once against an existing database:
    python migrate_add_priority_fields.py
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
    ("users",   "user_type", "VARCHAR(20) NOT NULL DEFAULT 'bronze'"),
    ("tickets", "severity",  "VARCHAR(20) NOT NULL DEFAULT 'medium'"),
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
