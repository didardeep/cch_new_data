"""
Standalone migration — adds all missing columns directly via psycopg2.
Uses IF NOT EXISTS so it's completely safe to re-run at any time.

Run:  python migrate_add_user_type.py
"""
import psycopg2

DATABASE_URL = "postgresql://postgres:Didardeep.12@localhost:5432/postgres"

MIGRATIONS = [
    # users table
    ("users",   "user_type",          "VARCHAR(20) DEFAULT 'bronze'"),
    ("users",   "domain",             "VARCHAR(50)"),
    ("users",   "location",           "VARCHAR(100)"),
    ("users",   "bandwidth_capacity", "INTEGER DEFAULT 8"),
    # tickets table
    ("tickets", "subcategory",        "VARCHAR(200) DEFAULT ''"),
    ("tickets", "domain",             "VARCHAR(50)"),
    ("tickets", "severity",           "VARCHAR(20) DEFAULT 'medium'"),
    ("tickets", "priority",           "VARCHAR(20) DEFAULT 'medium'"),
    ("tickets", "resolved_at",        "TIMESTAMP"),
    ("tickets", "sla_hours",          "FLOAT"),
    ("tickets", "sla_deadline",       "TIMESTAMP"),
    ("tickets", "sla_breached",       "BOOLEAN DEFAULT FALSE"),
    ("tickets", "alert_625_sent",     "BOOLEAN DEFAULT FALSE"),
    ("tickets", "alert_750_sent",     "BOOLEAN DEFAULT FALSE"),
    ("tickets", "alert_875_sent",     "BOOLEAN DEFAULT FALSE"),
    ("tickets", "breach_alert_sent",  "BOOLEAN DEFAULT FALSE"),
    ("tickets", "escalated_by",       "INTEGER"),
    ("tickets", "escalated_at",       "TIMESTAMP"),
    ("tickets", "escalation_note",    "TEXT DEFAULT ''"),
]

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

for table, col_name, col_def in MIGRATIONS:
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
        print(f"  ✅ {table}.{col_name} — OK")
    except Exception as e:
        print(f"  ❌ {table}.{col_name} — ERROR: {e}")

cur.close()
conn.close()
print("\nDone. You can now run: python app.py")
