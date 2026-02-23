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

    # Create Human Agent user
    if not User.query.filter_by(email="agent@telecom.com").first() and \
       not User.query.filter_by(employee_id="HA00001").first():
        agent = User(
            name="Human Agent",
            email="agent@telecom.com",
            role="human_agent",
            employee_id="HA00001"
        )
        agent.set_password("agent123")
        db.session.add(agent)
        print("✅ Agent user created!")
    else:
        print("⚠️ Agent already exists, skipping!")

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