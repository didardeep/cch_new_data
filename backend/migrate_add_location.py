from app import app, db
from sqlalchemy import text

with app.app_context():
    with db.engine.connect() as conn:
        # Add Latitude
        try:
            conn.execute(text("ALTER TABLE chat_sessions ADD COLUMN latitude FLOAT;"))
            conn.commit()
            print("✅ Added latitude column")
        except Exception as e:
            print(f"⚠️ latitude column check: {e}")

        # Add Longitude
        try:
            conn.execute(text("ALTER TABLE chat_sessions ADD COLUMN longitude FLOAT;"))
            conn.commit()
            print("✅ Added longitude column")
        except Exception as e:
            print(f"⚠️ longitude column check: {e}")

    print("\n✅ Migration complete! Database now has latitude & longitude columns.")