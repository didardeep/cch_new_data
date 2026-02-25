"""
Migration: Create sla_alerts table for dashboard SLA notifications
Run this ONCE to create the new table.

Usage:
    python migrate_add_sla_alerts.py
"""

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/telecom_complaints")

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

print("Creating sla_alerts table...")

cur.execute("""
    CREATE TABLE IF NOT EXISTS sla_alerts (
        id SERIAL PRIMARY KEY,
        ticket_id INTEGER NOT NULL REFERENCES tickets(id),
        alert_level VARCHAR(10) NOT NULL,
        recipient_role VARCHAR(20) NOT NULL,
        message VARCHAR(300) NOT NULL,
        is_read BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT NOW()
    );
""")
print("  [OK] sla_alerts table created")

# Index for fast dashboard queries
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_sla_alerts_role_read
    ON sla_alerts (recipient_role, is_read, created_at DESC);
""")
print("  [OK] index on (recipient_role, is_read, created_at) created")

cur.close()
conn.close()
print("\nMigration complete!")
