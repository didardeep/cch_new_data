"""
Migration: Add expert fields to users and domain field to tickets.

Runs directly via psycopg2 — does NOT import app.py — so it is safe
to execute against an existing database before the app starts.

New columns:
  users.domain             VARCHAR(50)   NULL
  users.location           VARCHAR(100)  NULL
  users.bandwidth_capacity INTEGER       NOT NULL DEFAULT 10
  tickets.domain           VARCHAR(50)   NULL

Run once:
    python migrate_add_expert_fields.py
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
    ("users",   "domain",             "VARCHAR(50)  DEFAULT NULL"),
    ("users",   "location",           "VARCHAR(100) DEFAULT NULL"),
    ("users",   "bandwidth_capacity", "INTEGER      NOT NULL DEFAULT 10"),
    ("tickets", "domain",             "VARCHAR(50)  DEFAULT NULL"),
]


def column_exists(cur, table, column):
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
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
    print("\n[DONE] Migration complete: expert fields added.")
    print("       Run init_db.py to seed the dummy expert dataset.")
except Exception as e:
    conn.rollback()
    print(f"\n[FAIL] Migration failed: {e}")
    raise
finally:
    cur.close()
    conn.close()
