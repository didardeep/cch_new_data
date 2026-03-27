from datetime import date
from app import app, db
from models import User, BillingAccount

# ─── Customer definitions ────────────────────────────────────────────────────
# Add/edit customers here. The script is idempotent — safe to re-run on any machine.

CUSTOMERS = [
    {
        "name": "Aarav Sharma",
        "email": "aarav.sharma21@gmail.com",
        "phone_number": "9876543210",
        "password": "Admin@123",
    },
    {
        "name": "Vihaan Gupta",
        "email": "vihaan.gupta34@yahoo.com",
        "phone_number": "9123456781",
        "password": "Admin@123",
    },
]

# ─── Billing plan per customer email ─────────────────────────────────────────
# Scenarios: tweak per customer to test different UI states.

BILLING = {
    "aarav.sharma21@gmail.com": {
        "plan_name": "100 Mbps Fiber",
        "plan_speed_mbps": 100,
        "account_active": True,
        "bill_paid": False,
        "outstanding_amount": 0.0,
        "fup_hit": False,
        "fup_speed_mbps": None,
        "plan_expiry": date(2026, 6, 15),
        "data_used_gb": 210,
        "data_limit_gb": 500,
    },
    "vihaan.gupta34@yahoo.com": {
        "plan_name": "50 Mbps Basic",
        "plan_speed_mbps": 100,
        "account_active": True,
        "bill_paid": False,
        "outstanding_amount": 1500.0,
        "fup_hit": False,
        "fup_speed_mbps": None,
        "plan_expiry": date(2026, 4, 30),
        "data_used_gb": 80,
        "data_limit_gb": 200,
    },
}

# ─── Default fallback billing for any other customer without a specific entry ─
DEFAULT_PLAN = {
    "plan_name": "100 Mbps Fiber",
    "plan_speed_mbps": 150,
    "account_active": True,
    "bill_paid": True,
    "outstanding_amount": 0.0,
    "fup_hit": False,
    "fup_speed_mbps": None,
    "plan_expiry": date(2026, 6, 15),
    "data_used_gb": 210,
    "data_limit_gb": 500,
}

# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    with app.app_context():

        # 1. Create / verify customers
        print("\n--- Seeding Customers ---")
        for c in CUSTOMERS:
            existing = User.query.filter_by(email=c["email"]).first()
            if existing:
                if existing.role != "customer":
                    old_role = existing.role
                    existing.role = "customer"
                    print(f"  🔄 Fixed role → customer for {c['email']} (was: {old_role})")
                else:
                    print(f"  ⚠️  Already exists as customer: {c['email']}, skipping")
            else:
                user = User(
                    name=c["name"],
                    email=c["email"],
                    phone_number=c.get("phone_number"),
                    role="customer",
                )
                user.set_password(c["password"])
                db.session.add(user)
                print(f"  ✅ Created customer: {c['name']} <{c['email']}>")
        db.session.commit()

        # 2. Seed / update billing for all customers
        print("\n--- Seeding Billing Accounts ---")
        all_customers = User.query.filter_by(role="customer").all()
        if not all_customers:
            print("  No customers found; nothing to seed.")

        for u in all_customers:
            plan = BILLING.get(u.email, DEFAULT_PLAN)
            acct = BillingAccount.query.filter_by(user_id=u.id).first()
            if acct:
                # Update to ensure data matches this script on every run
                for k, v in plan.items():
                    setattr(acct, k, v)
                print(f"  🔄 Updated billing for {u.email}: {plan['plan_name']}")
            else:
                acct = BillingAccount(user_id=u.id, **plan)
                db.session.add(acct)
                print(f"  ✅ Created billing for {u.email}: {plan['plan_name']}")

        db.session.commit()
        print("\n✅ Seeding complete.")
