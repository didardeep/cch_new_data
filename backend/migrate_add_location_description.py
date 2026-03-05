from app import app, db
from sqlalchemy import text

with app.app_context():
    with db.engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE chat_sessions ADD COLUMN location_description TEXT;"))
            conn.commit()
            print("Added location_description column")
        except Exception as e:
            print(f"location_description column check: {e}")

    print("\nMigration complete! Database now has location_description column.")
