"""Migration: add diagnosis_ran column to chat_sessions table."""
from app import app, db
from sqlalchemy import text

with app.app_context():
    with db.engine.connect() as conn:
        try:
            conn.execute(text(
                "ALTER TABLE chat_sessions ADD COLUMN diagnosis_ran BOOLEAN NOT NULL DEFAULT FALSE"
            ))
            conn.commit()
            print("✓ Added diagnosis_ran column to chat_sessions")
        except Exception as e:
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                print("✓ Column diagnosis_ran already exists — skipping")
            else:
                raise
