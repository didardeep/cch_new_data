"""
Migration: Add human agent status and SLA tracking fields
Run this ONCE to add new columns to existing database tables.

Usage:
    python migrate_add_agent_sla.py
"""

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/telecom_complaints")

# Extract connection params from DATABASE_URL
# Format: postgresql://user:password@host:port/dbname
url = DATABASE_URL.replace("postgresql://", "")
user_pass, rest = url.split("@", 1)
host_port, dbname = rest.split("/", 1)
user, password = user_pass.split(":", 1) if ":" in user_pass else (user_pass, "")
host, port = host_port.split(":", 1) if ":" in host_port else (host_port, "5432")

conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=int(port),
)
conn.autocommit = True
cur = conn.cursor()

migrations = [
    # User: agent online/offline status
    ("users", "is_online", "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_online BOOLEAN NOT NULL DEFAULT FALSE"),

    # Ticket: SLA tracking fields
    ("tickets", "sla_hours", "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS sla_hours FLOAT"),
    ("tickets", "sla_deadline", "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS sla_deadline TIMESTAMP"),
    ("tickets", "sla_breached", "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS sla_breached BOOLEAN NOT NULL DEFAULT FALSE"),
    ("tickets", "alert_625_sent", "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS alert_625_sent BOOLEAN NOT NULL DEFAULT FALSE"),
    ("tickets", "alert_750_sent", "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS alert_750_sent BOOLEAN NOT NULL DEFAULT FALSE"),
    ("tickets", "alert_875_sent", "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS alert_875_sent BOOLEAN NOT NULL DEFAULT FALSE"),
    ("tickets", "breach_alert_sent", "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS breach_alert_sent BOOLEAN NOT NULL DEFAULT FALSE"),
]

print("Running migrations...")
for table, column, sql in migrations:
    try:
        cur.execute(sql)
        print(f"  [OK] Added column '{column}' to '{table}'")
    except Exception as e:
        print(f"  [SKIP] '{column}' on '{table}': {e}")

cur.close()
conn.close()
print("\nMigration complete!")
