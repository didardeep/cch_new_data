from app import app, db
from sqlalchemy import text

with app.app_context():
    with db.engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE telecom_sites ADD COLUMN site_status VARCHAR(20) DEFAULT 'on_air';"))
            conn.commit()
            print("Added site_status column")
        except Exception as e:
            print(f"site_status column check: {e}")

        try:
            conn.execute(text("ALTER TABLE telecom_sites ADD COLUMN alarms TEXT DEFAULT '';"))
            conn.commit()
            print("Added alarms column")
        except Exception as e:
            print(f"alarms column check: {e}")

        try:
            conn.execute(text("ALTER TABLE telecom_sites ADD COLUMN solution TEXT DEFAULT '';"))
            conn.commit()
            print("Added solution column")
        except Exception as e:
            print(f"solution column check: {e}")

    print("\nMigration complete! telecom_sites now has site_status, alarms, and solution columns.")
