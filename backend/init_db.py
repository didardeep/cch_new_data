from app import app, db
from models import User, SystemSetting

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

    # ── Domain Experts (human_agents with domain/location/capacity) ──────────
    EXPERTS = [
        # Mobile experts
        {"name": "Mobile Expert 1",     "email": "mobile.expert1@example.com",     "employee_id": "HA00001", "domain": "mobile",     "location": "Delhi",  "bandwidth_capacity": 8},
        {"name": "Mobile Expert 2",     "email": "mobile.expert2@example.com",     "employee_id": "HA00002", "domain": "mobile",     "location": "Mumbai", "bandwidth_capacity": 8},
        # Broadband experts
        {"name": "Broadband Expert 1",  "email": "broadband.expert1@example.com",  "employee_id": "HA00003", "domain": "broadband",  "location": "Delhi",  "bandwidth_capacity": 8},
        {"name": "Broadband Expert 2",  "email": "broadband.expert2@example.com",  "employee_id": "HA00004", "domain": "broadband",  "location": "Mumbai", "bandwidth_capacity": 8},
        # DTH experts
        {"name": "DTH Expert 1",        "email": "dth.expert1@example.com",        "employee_id": "HA00005", "domain": "dth",        "location": "Delhi",  "bandwidth_capacity": 8},
        {"name": "DTH Expert 2",        "email": "dth.expert2@example.com",        "employee_id": "HA00006", "domain": "dth",        "location": "Mumbai", "bandwidth_capacity": 8},
        # Landline experts
        {"name": "Landline Expert 1",   "email": "landline.expert1@example.com",   "employee_id": "HA00007", "domain": "landline",   "location": "Delhi",  "bandwidth_capacity": 8},
        {"name": "Landline Expert 2",   "email": "landline.expert2@example.com",   "employee_id": "HA00008", "domain": "landline",   "location": "Mumbai", "bandwidth_capacity": 8},
        # Enterprise experts
        {"name": "Enterprise Expert 1", "email": "enterprise.expert1@example.com", "employee_id": "HA00009", "domain": "enterprise", "location": "Delhi",  "bandwidth_capacity": 6},
        {"name": "Enterprise Expert 2", "email": "enterprise.expert2@example.com", "employee_id": "HA00010", "domain": "enterprise", "location": "Mumbai", "bandwidth_capacity": 6},
    ]

    for e in EXPERTS:
        existing = User.query.filter_by(employee_id=e["employee_id"]).first()
        if existing:
            # Update domain/location/capacity in case they were missing
            existing.domain = e["domain"]
            existing.location = e["location"]
            existing.bandwidth_capacity = e["bandwidth_capacity"]
            print(f"  [UPDATE] {e['employee_id']} {e['name']} -> domain={e['domain']}, loc={e['location']}")
        elif not User.query.filter_by(email=e["email"]).first():
            expert = User(
                name=e["name"],
                email=e["email"],
                role="human_agent",
                employee_id=e["employee_id"],
                domain=e["domain"],
                location=e["location"],
                bandwidth_capacity=e["bandwidth_capacity"],
            )
            expert.set_password("agent123")
            db.session.add(expert)
            print(f"  [CREATE] {e['employee_id']} {e['name']} (domain={e['domain']}, loc={e['location']})")
        else:
            print(f"  [SKIP]   {e['email']} already exists")

    print("Domain experts seeded!")

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