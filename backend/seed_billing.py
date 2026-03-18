from datetime import date
from app import app, db
from models import User, BillingAccount

DEFAULT_PLAN = {
    "plan_name": "100 Mbps Fiber",
    "plan_speed_mbps": 100,
    "account_active": True,
    "bill_paid": True,
    "outstanding_amount": 0.0,
    "fup_hit": False,
    "fup_speed_mbps": None,
    "plan_expiry": date(2026, 6, 15),
    "data_used_gb": 210,
    "data_limit_gb": 500,
}

if __name__ == "__main__":
    with app.app_context():
        customers = User.query.filter_by(role="customer").all()
        if not customers:
            print("No customers found; nothing to seed.")
        for u in customers:
            if BillingAccount.query.filter_by(user_id=u.id).first():
                print(f"Billing already exists for {u.email}, skipping")
                continue
            acct = BillingAccount(user_id=u.id, **DEFAULT_PLAN)
            db.session.add(acct)
            print(f"Seeded billing for {u.email}: {DEFAULT_PLAN['plan_name']}")
        db.session.commit()
        print("Billing seeding complete.")
