"""
Master migration: runs ALL pending schema changes idempotently.

Run once against an existing database:
    python migrate_run_all.py

Safe to re-run — every step checks existence before altering.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/telecom_complaints",
)

# ── Column additions ──────────────────────────────────────────────────────────
# (table, column, definition)
ADD_COLUMNS = [
    # Expert profile fields
    ("users",   "domain",              "VARCHAR(50)  DEFAULT NULL"),
    ("users",   "location",            "VARCHAR(100) DEFAULT NULL"),
    ("users",   "bandwidth_capacity",  "INTEGER      NOT NULL DEFAULT 10"),
    # Customer tier
    ("users",   "user_type",           "VARCHAR(20)  NOT NULL DEFAULT 'bronze'"),
    # Ticket enrichment
    ("tickets", "domain",              "VARCHAR(50)  DEFAULT NULL"),
    ("tickets", "severity",            "VARCHAR(20)  NOT NULL DEFAULT 'medium'"),
    ("tickets", "priority",            "VARCHAR(20)  NOT NULL DEFAULT 'medium'"),
    # Escalation tracking
    ("tickets", "escalated_by",        "INTEGER DEFAULT NULL REFERENCES users(id)"),
    ("tickets", "escalated_at",        "TIMESTAMPTZ DEFAULT NULL"),
    ("tickets", "escalation_note",     "TEXT DEFAULT ''"),
]

# ── Table creations ───────────────────────────────────────────────────────────
CREATE_TABLES = [
    (
        "parameter_changes",
        """
        CREATE TABLE parameter_changes (
            id              SERIAL PRIMARY KEY,
            ticket_id       INTEGER NOT NULL REFERENCES tickets(id),
            agent_id        INTEGER NOT NULL REFERENCES users(id),
            proposed_change TEXT    NOT NULL,
            status          VARCHAR(20) DEFAULT 'pending',
            manager_note    TEXT    DEFAULT '',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            reviewed_at     TIMESTAMPTZ DEFAULT NULL,
            reviewed_by     INTEGER DEFAULT NULL REFERENCES users(id)
        )
        """,
    ),
]


def column_exists(cur, table, column):
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone() is not None


def table_exists(cur, table):
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_name = %s",
        (table,),
    )
    return cur.fetchone() is not None


conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
cur = conn.cursor()

try:
    # -- Column migrations
    for table, column, definition in ADD_COLUMNS:
        if column_exists(cur, table, column):
            print(f"  [SKIP] {table}.{column} already exists.")
        else:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            print(f"  [OK]   Added {table}.{column}")

    # -- Table migrations
    for tname, ddl in CREATE_TABLES:
        if table_exists(cur, tname):
            print(f"  [SKIP] table {tname} already exists.")
        else:
            cur.execute(ddl)
            print(f"  [OK]   Created table {tname}")

    conn.commit()
    print("\n[DONE] All migrations complete.")
except Exception as e:
    conn.rollback()
    print(f"\n[FAIL] Migration failed: {e}")
    raise
finally:
    cur.close()
    conn.close()
