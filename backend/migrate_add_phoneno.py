"""
Migration script to add phone_number column to users table
"""

import psycopg2

# Your database connection
DATABASE_URL = "postgresql://postgres:ROOT@localhost:5432/telecom_cch"

try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Add phone_number column
    try:
        cur.execute("ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);")
        print("✅ Added phone_number column to users table")
    except Exception as e:
        print(f"phone_number column: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Migration complete!")
    
except Exception as e:
    print(f"❌ Migration failed: {e}")