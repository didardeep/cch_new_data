"""
Database Models for Telecom Complaint Handling System
"""

from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt

db = SQLAlchemy()
bcrypt = Bcrypt()


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    phone_number = db.Column(db.String(20), nullable=True)  # ← WhatsApp phone number
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="customer")
    employee_id = db.Column(db.String(20), unique=True, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    # Human agent online/offline status
    is_online = db.Column(db.Boolean, default=False, nullable=False)

    chat_sessions = db.relationship("ChatSession", backref="user", lazy=True)
    tickets = db.relationship("Ticket", backref="user", lazy=True, foreign_keys="Ticket.user_id")
    feedbacks = db.relationship("Feedback", backref="user", lazy=True)

    def set_password(self, password):
        self.password_hash = bcrypt.generate_password_hash(password).decode("utf-8")

    def check_password(self, password):
        return bcrypt.check_password_hash(self.password_hash, password)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
            "phone_number": self.phone_number,
            "role": self.role,
            "employee_id": self.employee_id,
            "is_online": self.is_online,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ChatSession(db.Model):
    __tablename__ = "chat_sessions"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    sector_name = db.Column(db.String(200), default="")
    subprocess_name = db.Column(db.String(200), default="")
    query_text = db.Column(db.Text, default="")
    resolution = db.Column(db.Text, default="")
    status = db.Column(db.String(30), default="active")
    language = db.Column(db.String(50), default="English")
    summary = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    resolved_at = db.Column(db.DateTime, nullable=True)
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    location_description = db.Column(db.Text, nullable=True)

    messages = db.relationship("ChatMessage", backref="session", lazy=True, order_by="ChatMessage.created_at")
    ticket = db.relationship("Ticket", backref="chat_session", uselist=False, lazy=True)

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "user_name": self.user.name if self.user else "",
            "user_email": self.user.email if self.user else "",
            "user_phone": self.user.phone_number if self.user else "",
            "sector_name": self.sector_name,
            "subprocess_name": self.subprocess_name,
            "query_text": self.query_text,
            "resolution": self.resolution,
            "status": self.status,
            "language": self.language,
            "summary": self.summary,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "location_description": self.location_description,
        }


class ChatMessage(db.Model):
    __tablename__ = "chat_messages"

    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.Integer, db.ForeignKey("chat_sessions.id"), nullable=False)
    sender = db.Column(db.String(20), nullable=False)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    delivered_at = db.Column(db.DateTime, nullable=True)
    seen_at = db.Column(db.DateTime, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "session_id": self.session_id,
            "sender": self.sender,
            "content": self.content,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "seen_at": self.seen_at.isoformat() if self.seen_at else None,
        }


class Ticket(db.Model):
    __tablename__ = "tickets"

    id = db.Column(db.Integer, primary_key=True)
    chat_session_id = db.Column(db.Integer, db.ForeignKey("chat_sessions.id"), nullable=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    reference_number = db.Column(db.String(50), unique=True, nullable=False)
    category = db.Column(db.String(200), default="General")
    subcategory = db.Column(db.String(200), default="")
    description = db.Column(db.Text, default="")
    status = db.Column(db.String(30), default="pending")
    priority = db.Column(db.String(20), default="medium")
    assigned_to = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    resolution_notes = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    resolved_at = db.Column(db.DateTime, nullable=True)
    # SLA fields
    sla_hours = db.Column(db.Float, nullable=True)        # SLA time assigned at ticket creation (hours)
    sla_deadline = db.Column(db.DateTime, nullable=True)  # Absolute deadline = created_at + sla_hours
    sla_breached = db.Column(db.Boolean, default=False)   # True if SLA breach occurred
    # Alert tracking flags
    alert_625_sent = db.Column(db.Boolean, default=False)  # Alert at 62.5% of SLA time elapsed
    alert_750_sent = db.Column(db.Boolean, default=False)  # Alert at 75% of SLA time elapsed
    alert_875_sent = db.Column(db.Boolean, default=False)  # Alert at 87.5% of SLA time elapsed
    breach_alert_sent = db.Column(db.Boolean, default=False)  # Breach alert sent to CTO

    assignee = db.relationship("User", foreign_keys=[assigned_to], backref="assigned_tickets")

    def to_dict(self):
        return {
            "id": self.id,
            "chat_session_id": self.chat_session_id,
            "user_id": self.user_id,
            "user_name": self.user.name if self.user else "",
            "user_email": self.user.email if self.user else "",
            "user_phone": self.user.phone_number if self.user else "",
            "reference_number": self.reference_number,
            "category": self.category,
            "subcategory": self.subcategory,
            "description": self.description,
            "status": self.status,
            "priority": self.priority,
            "assigned_to": self.assigned_to,
            "assignee_name": self.assignee.name if self.assignee else "Unassigned",
            "assignee_phone": self.assignee.phone_number if self.assignee else None,
            "resolution_notes": self.resolution_notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "sla_hours": self.sla_hours,
            "sla_deadline": self.sla_deadline.isoformat() if self.sla_deadline else None,
            "sla_breached": self.sla_breached,
        }


class SystemSetting(db.Model):
    __tablename__ = "system_settings"

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.String(500), nullable=False)
    category = db.Column(db.String(50), default="general")
    description = db.Column(db.Text, default="")
    updated_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "id": self.id,
            "key": self.key,
            "value": self.value,
            "category": self.category,
            "description": self.description,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class Feedback(db.Model):
    __tablename__ = "feedbacks"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    chat_session_id = db.Column(db.Integer, db.ForeignKey("chat_sessions.id"), nullable=True)
    rating = db.Column(db.Integer, default=0)
    comment = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    chat_session = db.relationship("ChatSession", backref="feedbacks")

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "user_name": self.user.name if self.user else "",
            "chat_session_id": self.chat_session_id,
            "rating": self.rating,
            "comment": self.comment,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class SlaAlert(db.Model):
    __tablename__ = "sla_alerts"

    id = db.Column(db.Integer, primary_key=True)
    ticket_id = db.Column(db.Integer, db.ForeignKey("tickets.id"), nullable=False)
    alert_level = db.Column(db.String(10), nullable=False)       # '625', '750', '875', 'breach'
    recipient_role = db.Column(db.String(20), nullable=False)    # 'manager' or 'cto'
    message = db.Column(db.String(300), nullable=False)
    is_read = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    ticket = db.relationship("Ticket", backref="sla_alerts")

    def to_dict(self):
        t = self.ticket
        return {
            "id": self.id,
            "ticket_id": self.ticket_id,
            "alert_level": self.alert_level,
            "recipient_role": self.recipient_role,
            "message": self.message,
            "is_read": self.is_read,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "reference_number": t.reference_number if t else "",
            "category": t.category if t else "",
            "subcategory": t.subcategory if t else "",
            "priority": t.priority if t else "",
            "status": t.status if t else "",
            "description": (t.description[:150] if t and t.description else ""),
            "assignee_name": t.assignee.name if t and t.assignee else "Unassigned",
            "sla_hours": t.sla_hours if t else None,
            "sla_deadline": t.sla_deadline.isoformat() if t and t.sla_deadline else None,
        }


class TelecomSite(db.Model):
    __tablename__ = "telecom_sites"

    id = db.Column(db.Integer, primary_key=True)
    site_id = db.Column(db.String(50), unique=True, nullable=False)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    zone = db.Column(db.String(100), default="")
    site_status = db.Column(db.String(20), default="on_air")   # 'on_air' or 'off_air'
    alarms = db.Column(db.Text, default="")
    solution = db.Column(db.Text, default="")

    def to_dict(self):
        return {
            "id": self.id,
            "site_id": self.site_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "zone": self.zone,
            "site_status": self.site_status or "on_air",
            "alarms": self.alarms or "",
            "solution": self.solution or "",
        }


class ParameterChange(db.Model):
    __tablename__ = "parameter_changes"

    id = db.Column(db.Integer, primary_key=True)
    ticket_id = db.Column(db.Integer, db.ForeignKey("tickets.id"), nullable=False)
    agent_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    proposed_change = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(20), default="pending")   # 'pending', 'approved', 'disapproved'
    manager_note = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    reviewed_at = db.Column(db.DateTime, nullable=True)
    reviewed_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)

    ticket = db.relationship("Ticket", backref="parameter_changes", foreign_keys=[ticket_id])
    agent = db.relationship("User", foreign_keys=[agent_id], backref="submitted_changes")
    reviewer = db.relationship("User", foreign_keys=[reviewed_by], backref="reviewed_changes")

    def to_dict(self):
        return {
            "id": self.id,
            "ticket_id": self.ticket_id,
            "agent_id": self.agent_id,
            "agent_name": self.agent.name if self.agent else "",
            "proposed_change": self.proposed_change,
            "status": self.status,
            "manager_note": self.manager_note,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "reviewed_at": self.reviewed_at.isoformat() if self.reviewed_at else None,
            "reviewed_by": self.reviewed_by,
            "reviewer_name": self.reviewer.name if self.reviewer else None,
            "ticket": self.ticket.to_dict() if self.ticket else None,
        }


class KpiData(db.Model):
    __tablename__ = "kpi_data"

    id = db.Column(db.Integer, primary_key=True)
    site_id = db.Column(db.String(50), nullable=False, index=True)
    kpi_name = db.Column(db.String(100), nullable=False, index=True)
    date = db.Column(db.Date, nullable=False)
    hour = db.Column(db.Integer, nullable=False, default=0)
    value = db.Column(db.Float, nullable=True)
    data_level = db.Column(db.String(10), nullable=False, default="site")  # 'site' or 'cell'
    cell_id = db.Column(db.String(100), nullable=True)
    cell_site_id = db.Column(db.String(100), nullable=True)

    __table_args__ = (
        db.Index("idx_kpi_site_name_date", "site_id", "kpi_name", "date"),
        db.Index("idx_kpi_data_level", "data_level", "kpi_name"),
    )
