from app import app, db
from models import User, SystemSetting, BillingAccount

with app.app_context():
    # Create all tables
    db.create_all()
    print("✅ All tables created!")
    print("   - users")
    print("   - chat_sessions")
    print("   - chat_messages")
    print("   - tickets")
    print("   - system_settings")
    print("   - feedbacks")
    print("   - billing_accounts")

    # Create Admin user
    if not User.query.filter_by(email="admin@telecom.com").first() and \
       not User.query.filter_by(employee_id="ADM00001").first():
        admin = User(
            name="Admin",
            email="admin@telecom.com",
            role="admin",
            employee_id="ADM00001"
        )
        admin.set_password("admin123")
        db.session.add(admin)
        print("✅ Admin user created!")
    else:
        print("⚠️ Admin already exists, skipping!")

    # Create Manager user
    if not User.query.filter_by(email="manager@telecom.com").first() and \
       not User.query.filter_by(employee_id="MGR00001").first():
        manager = User(
            name="Manager",
            email="manager@telecom.com",
            role="manager",
            employee_id="MGR00001"
        )
        manager.set_password("manager123")
        db.session.add(manager)
        print("✅ Manager user created!")
    else:
        print("⚠️ Manager already exists, skipping!")

    # Create CTO user
    if not User.query.filter_by(email="cto@telecom.com").first() and \
       not User.query.filter_by(employee_id="CTO00001").first():
        cto = User(
            name="CTO",
            email="cto@telecom.com",
            role="cto",
            employee_id="CTO00001"
        )
        cto.set_password("cto123")
        db.session.add(cto)
        print("✅ CTO user created!")
    else:
        print("⚠️ CTO already exists, skipping!")

    # ── Domain Experts (human_agents with domain/expertise/location/capacity) ──
    # Seeded to cover every (domain × expertise × major Cambodian province) so
    # the load-balancer has genuine headroom regardless of where a ticket lands.
    PROVINCES = [
        "Phnom Penh", "Siem Reap", "Battambang", "Kampong Cham",
        "Sihanoukville", "Kampong Speu", "Kandal", "Takeo",
    ]
    DOMAIN_EXPERTISE = [
        # (domain_label, expertise_tag)
        ("mobile",     "NETWORK_RF"),
        ("mobile",     "LTE"),
        ("mobile",     "5G"),
        ("broadband",  "TRANSPORT"),
        ("broadband",  "BACKHAUL"),
        ("dth",        "TRANSMISSION"),
        ("landline",   "CORE"),
        ("enterprise", "IP"),
        ("enterprise", "MPLS"),
    ]
    EXPERTS = []
    _eid = 1
    for prov in PROVINCES:
        for dom, exp in DOMAIN_EXPERTISE:
            slug = f"{dom}.{exp.lower()}.{prov.split()[0].lower()}{_eid}"
            EXPERTS.append({
                "name":  f"{dom.title()} {exp} Expert — {prov} #{_eid}",
                "email": f"{slug}@example.com",
                "employee_id": f"HA{_eid:05d}",
                "domain":   dom,
                "expertise": exp,
                "location": prov,
                "bandwidth_capacity": 8,
            })
            _eid += 1

    for e in EXPERTS:
        existing = User.query.filter_by(employee_id=e["employee_id"]).first()
        if existing:
            existing.domain = e["domain"]
            existing.expertise = e.get("expertise") or existing.expertise
            existing.location = e["location"]
            existing.bandwidth_capacity = e["bandwidth_capacity"]
            print(f"  [UPDATE] {e['employee_id']} {e['name']} -> domain={e['domain']}, exp={e.get('expertise','')}, loc={e['location']}")
        elif not User.query.filter_by(email=e["email"]).first():
            expert = User(
                name=e["name"],
                email=e["email"],
                role="human_agent",
                employee_id=e["employee_id"],
                domain=e["domain"],
                expertise=e.get("expertise"),
                location=e["location"],
                bandwidth_capacity=e["bandwidth_capacity"],
            )
            expert.set_password("agent123")
            db.session.add(expert)
            print(f"  [CREATE] {e['employee_id']} {e['name']} (domain={e['domain']}, loc={e['location']})")
        else:
            print(f"  [SKIP]   {e['email']} already exists")

    print("Domain experts seeded!")

    # ── Test Customers (one per tier) ─────────────────────────────────────────
    CUSTOMERS = [
        {"name": "Madhav Sharma",   "email": "madhavsharma3@kpmg.com",    "password": "123456",    "user_type": "bronze"},
        {"name": "Dhanoa",          "email": "dhanoatwk@gmail.com",       "password": "Admin@123", "user_type": "silver"},
        {"name": "Priyanshi",       "email": "spriyanshi542@gmail.com",   "password": "Admin@123", "user_type": "gold"},
        {"name": "Manav Verma",     "email": "manav.verma343@gmail.com",  "password": "Admin@123", "user_type": "platinum"},
    ]

    for c in CUSTOMERS:
        existing = User.query.filter_by(email=c["email"]).first()
        if existing:
            existing.user_type = c["user_type"]
            existing.set_password(c["password"])
            print(f"  [UPDATE] {c['email']} -> tier={c['user_type']}")
        else:
            cust = User(
                name=c["name"],
                email=c["email"],
                role="customer",
                user_type=c["user_type"],
            )
            cust.set_password(c["password"])
            db.session.add(cust)
            print(f"  [CREATE] {c['email']} (tier={c['user_type']})")

    print("Test customers seeded!")

    # Add default system settings
    settings = [
        {"key": "bot_name", "value": "TeleBot", "category": "general", "description": "Chatbot display name"},
        {"key": "default_language", "value": "English", "category": "general", "description": "Default language"},
        {"key": "max_escalation_time", "value": "24", "category": "escalation", "description": "Hours before escalation"},
    ]
    for s in settings:
        if not SystemSetting.query.filter_by(key=s["key"]).first():
            db.session.add(SystemSetting(**s))

    db.session.commit()
    print("✅ Default settings added!")

    print("\n=============================")
    print("✅ DATABASE SETUP COMPLETE!")
    print("=============================")
    print("\n--- All Users in Database ---")
    users = User.query.all()
    for u in users:
        print(f"  {u.role.upper()} → {u.email} (employee_id: {u.employee_id})")
