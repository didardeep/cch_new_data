"""
Standalone migration — adds ALL missing columns from models.py.
Uses IF NOT EXISTS so it's completely safe to re-run at any time.

Run:  python migrate_add_user_type.py
"""
import psycopg2

DATABASE_URL = "postgresql://postgres:Didardeep.12@localhost:5432/postgres"

MIGRATIONS = [
    # ── users ──────────────────────────────────────────────────────
    ("users", "user_type",          "VARCHAR(20) DEFAULT 'bronze'"),
    ("users", "is_online",          "BOOLEAN DEFAULT FALSE"),
    ("users", "domain",             "VARCHAR(50)"),
    ("users", "location",           "VARCHAR(100)"),
    ("users", "bandwidth_capacity", "INTEGER DEFAULT 10"),

    # ── chat_sessions ─────────────────────────────────────────────
    ("chat_sessions", "sector_name",           "VARCHAR(200) DEFAULT ''"),
    ("chat_sessions", "subprocess_name",       "VARCHAR(200) DEFAULT ''"),
    ("chat_sessions", "query_text",            "TEXT DEFAULT ''"),
    ("chat_sessions", "resolution",            "TEXT DEFAULT ''"),
    ("chat_sessions", "language",              "VARCHAR(50) DEFAULT 'English'"),
    ("chat_sessions", "summary",               "TEXT DEFAULT ''"),
    ("chat_sessions", "resolved_at",           "TIMESTAMP"),
    ("chat_sessions", "latitude",              "FLOAT"),
    ("chat_sessions", "longitude",             "FLOAT"),
    ("chat_sessions", "location_description",  "TEXT"),
    ("chat_sessions", "customer_present",      "BOOLEAN DEFAULT FALSE"),
    ("chat_sessions", "diagnosis_ran",         "BOOLEAN DEFAULT FALSE"),
    ("chat_sessions", "current_step",          "VARCHAR(50) DEFAULT 'greeting'"),
    ("chat_sessions", "last_message_at",       "TIMESTAMP"),

    # ── chat_messages ─────────────────────────────────────────────
    ("chat_messages", "content_json",  "JSON"),
    ("chat_messages", "delivered_at",  "TIMESTAMP"),
    ("chat_messages", "seen_at",       "TIMESTAMP"),

    # ── tickets ───────────────────────────────────────────────────
    ("tickets", "subcategory",        "VARCHAR(200) DEFAULT ''"),
    ("tickets", "domain",             "VARCHAR(50)"),
    ("tickets", "severity",           "VARCHAR(20) DEFAULT 'medium'"),
    ("tickets", "priority",           "VARCHAR(20) DEFAULT 'medium'"),
    ("tickets", "resolved_at",        "TIMESTAMP"),
    ("tickets", "first_response_at",  "TIMESTAMP"),
    ("tickets", "reopened_count",     "INTEGER DEFAULT 0"),
    ("tickets", "last_reopened_at",   "TIMESTAMP"),
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

    # ── sla_alerts (create table if missing, then columns) ────────
    ("sla_alerts", "ticket_id",       "INTEGER"),
    ("sla_alerts", "alert_level",     "VARCHAR(10)"),
    ("sla_alerts", "recipient_role",  "VARCHAR(20)"),
    ("sla_alerts", "message",         "VARCHAR(300)"),
    ("sla_alerts", "is_read",         "BOOLEAN DEFAULT FALSE"),
    ("sla_alerts", "created_at",      "TIMESTAMP"),

    # ── parameter_changes ─────────────────────────────────────────
    ("parameter_changes", "ticket_id",        "INTEGER"),
    ("parameter_changes", "agent_id",         "INTEGER"),
    ("parameter_changes", "proposed_change",  "TEXT"),
    ("parameter_changes", "status",           "VARCHAR(20) DEFAULT 'pending'"),
    ("parameter_changes", "manager_note",     "TEXT DEFAULT ''"),
    ("parameter_changes", "created_at",       "TIMESTAMP"),
    ("parameter_changes", "reviewed_at",      "TIMESTAMP"),
    ("parameter_changes", "reviewed_by",      "INTEGER"),
]

# Tables that might not exist yet
CREATE_TABLES = [
    """CREATE TABLE IF NOT EXISTS sla_alerts (
        id SERIAL PRIMARY KEY,
        ticket_id INTEGER,
        alert_level VARCHAR(10),
        recipient_role VARCHAR(20),
        message VARCHAR(300),
        is_read BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS parameter_changes (
        id SERIAL PRIMARY KEY,
        ticket_id INTEGER,
        agent_id INTEGER,
        proposed_change TEXT,
        status VARCHAR(20) DEFAULT 'pending',
        manager_note TEXT DEFAULT '',
        created_at TIMESTAMP,
        reviewed_at TIMESTAMP,
        reviewed_by INTEGER
    )""",
]

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

# Create any tables that might not exist
for sql in CREATE_TABLES:
    try:
        cur.execute(sql)
    except Exception as e:
        print(f"  ⚠️ Table create: {e}")

# Add all missing columns
for table, col_name, col_def in MIGRATIONS:
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
        print(f"  ✅ {table}.{col_name} — OK")
    except Exception as e:
        print(f"  ❌ {table}.{col_name} — {e}")

cur.close()
conn.close()
print("\nDone. You can now run: python app.py")
