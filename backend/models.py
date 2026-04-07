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
    # Customer tier — drives ticket priority floor (platinum > gold > silver > bronze)
    user_type = db.Column(db.String(20), nullable=True, default="bronze")  # platinum/gold/silver/bronze
    # Human agent online/offline status
    is_online = db.Column(db.Boolean, default=False, nullable=False)
    # Expert fields (applicable when role == "human_agent")
    domain = db.Column(db.String(50), nullable=True)           # e.g. "mobile", "broadband", "dth", "landline", "enterprise", "fiber"
    location = db.Column(db.String(100), nullable=True)        # City name, e.g. "Gurugram", "Mumbai"
    expertise = db.Column(db.String(100), nullable=True)       # e.g. "NETWORK_RF", "NETWORK_OPTIMIZATION", "LTE", "5G"
    specialization = db.Column(db.String(200), nullable=True)  # Additional specialization details
    bandwidth_capacity = db.Column(db.Integer, default=10, nullable=False)  # Max concurrent open tickets

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
            "user_type": self.user_type or "bronze",
            "domain": self.domain,
            "location": self.location,
            "expertise": getattr(self, 'expertise', None),
            "specialization": getattr(self, 'specialization', None),
            "bandwidth_capacity": self.bandwidth_capacity,
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
    state_province = db.Column(db.String(200), nullable=True)   # State/Province/Region
    country = db.Column(db.String(100), nullable=True)          # Country name
    customer_present = db.Column(db.Boolean, default=False)
    diagnosis_ran = db.Column(db.Boolean, default=False, nullable=False)
    current_step = db.Column(db.String(50), default="greeting")
    last_message_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

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
            "state_province": self.state_province,
            "country": self.country,
            "customer_present": self.customer_present,
            "diagnosis_ran": bool(self.diagnosis_ran),
            "current_step": self.current_step or "greeting",
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "assignee_name": (self.ticket.assignee.name if self.ticket and self.ticket.assignee else None),
            "assignee_domain": (self.ticket.assignee.domain if self.ticket and self.ticket.assignee else None),
        }


class ChatMessage(db.Model):
    __tablename__ = "chat_messages"

    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.Integer, db.ForeignKey("chat_sessions.id"), nullable=False)
    sender = db.Column(db.String(20), nullable=False)
    content = db.Column(db.Text, nullable=False)
    content_json = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    delivered_at = db.Column(db.DateTime, nullable=True)
    seen_at = db.Column(db.DateTime, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "session_id": self.session_id,
            "sender": self.sender,
            "content": self.content,
            "payload": self.content_json,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "seen_at": self.seen_at.isoformat() if self.seen_at else None,
        }


class NetworkAiSession(db.Model):
    __tablename__ = "network_ai_sessions"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    title = db.Column(db.String(200), default="New Chat")
    status = db.Column(db.String(20), default="active")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    last_message_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    # ── CHANGE: session_context stores active sites/KPIs/days/chart for follow-ups ──
    session_context = db.Column(db.JSON, default=dict)
    # ── CHANGE: conversation_summary stores rolling plain-text summary ──
    conversation_summary = db.Column(db.Text, nullable=True)

    messages = db.relationship("NetworkAiMessage", backref="session", lazy=True,
                               order_by="NetworkAiMessage.created_at")

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "title": self.title,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "message_count": len(self.messages) if self.messages else 0,
        }


class NetworkAiMessage(db.Model):
    __tablename__ = "network_ai_messages"

    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.Integer, db.ForeignKey("network_ai_sessions.id"), nullable=False)
    role = db.Column(db.String(10), nullable=False)
    content = db.Column(db.Text, nullable=False)
    content_json = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "id": self.id,
            "session_id": self.session_id,
            "role": self.role,
            "content": self.content,
            "payload": self.content_json,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class Ticket(db.Model):
    __tablename__ = "tickets"

    id = db.Column(db.Integer, primary_key=True)
    chat_session_id = db.Column(db.Integer, db.ForeignKey("chat_sessions.id"), nullable=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    reference_number = db.Column(db.String(50), unique=True, nullable=False)
    category = db.Column(db.String(200), default="General")
    subcategory = db.Column(db.String(200), default="")
    domain = db.Column(db.String(50), nullable=True)           # Expert domain this ticket belongs to
    description = db.Column(db.Text, default="")
    status = db.Column(db.String(30), default="pending")
    severity = db.Column(db.String(20), default="medium")   # issue urgency: critical/high/medium/low
    priority = db.Column(db.String(20), default="medium")   # final priority = max(severity, user_type_priority)
    assigned_to = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    resolution_notes = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    resolved_at = db.Column(db.DateTime, nullable=True)
    # Response/reopen tracking
    first_response_at = db.Column(db.DateTime, nullable=True)
    reopened_count = db.Column(db.Integer, default=0)
    last_reopened_at = db.Column(db.DateTime, nullable=True)
    # SLA fields
    sla_hours = db.Column(db.Float, nullable=True)        # SLA time assigned at ticket creation (hours)
    sla_deadline = db.Column(db.DateTime, nullable=True)  # Absolute deadline = created_at + sla_hours
    sla_breached = db.Column(db.Boolean, default=False)   # True if SLA breach occurred
    # Alert tracking flags
    alert_625_sent = db.Column(db.Boolean, default=False)  # Alert at 62.5% of SLA time elapsed
    alert_750_sent = db.Column(db.Boolean, default=False)  # Alert at 75% of SLA time elapsed
    alert_875_sent = db.Column(db.Boolean, default=False)  # Alert at 87.5% of SLA time elapsed
    breach_alert_sent = db.Column(db.Boolean, default=False)  # Breach alert sent to CTO
    # Escalation tracking (expert → manager via parameter-change button)
    escalated_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)   # expert who escalated
    escalated_at = db.Column(db.DateTime, nullable=True)                              # when escalated
    escalation_note = db.Column(db.Text, default="")                                  # expert's note at escalation

    assignee = db.relationship("User", foreign_keys=[assigned_to], backref="assigned_tickets")
    escalator = db.relationship("User", foreign_keys=[escalated_by], backref="escalated_tickets")

    @staticmethod
    def _iso_z(dt):
        if not dt:
            return None
        s = dt.replace(tzinfo=None).isoformat() + "Z" if dt.tzinfo else dt.isoformat() + "Z"
        return s

    def to_dict(self):
        iz = self._iso_z
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
            "domain": self.domain,
            "description": self.description,
            "status": self.status,
            "severity": self.severity,
            "priority": self.priority,
            "user_type": (self.user.user_type or "bronze") if self.user else "bronze",
            "assigned_to": self.assigned_to,
            "assignee_name": self.assignee.name if self.assignee else "Unassigned",
            "assignee_phone": self.assignee.phone_number if self.assignee else None,
            "assignee_domain": self.assignee.domain if self.assignee else None,
            "assignee_location": self.assignee.location if self.assignee else None,
            "resolution_notes": self.resolution_notes,
            "created_at": iz(self.created_at),
            "resolved_at": iz(self.resolved_at),
            "sla_hours": self.sla_hours,
            "sla_deadline": iz(self.sla_deadline),
            "sla_breached": self.sla_breached,
            "escalated_by": self.escalated_by,
            "escalated_by_name": self.escalator.name if self.escalator else None,
            "escalated_at": iz(self.escalated_at),
            "escalation_note": self.escalation_note or "",
            "first_response_at": iz(self.first_response_at),
            "reopened_count": self.reopened_count,
            "last_reopened_at": iz(self.last_reopened_at),
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


class BillingAccount(db.Model):
    __tablename__ = "billing_accounts"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, unique=True)
    plan_name = db.Column(db.String(200), nullable=False)
    plan_speed_mbps = db.Column(db.Integer, nullable=False)
    account_active = db.Column(db.Boolean, default=True, nullable=False)
    bill_paid = db.Column(db.Boolean, default=True, nullable=False)
    outstanding_amount = db.Column(db.Float, default=0.0, nullable=False)
    fup_hit = db.Column(db.Boolean, default=False, nullable=False)
    fup_speed_mbps = db.Column(db.Integer, nullable=True)
    plan_expiry = db.Column(db.Date, nullable=True)
    data_used_gb = db.Column(db.Float, nullable=True)
    data_limit_gb = db.Column(db.Float, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    user = db.relationship("User", backref=db.backref("billing_account", uselist=False))

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "customer_email": self.user.email if self.user else None,
            "customer_name": self.user.name if self.user else None,
            "plan_name": self.plan_name,
            "plan_speed_mbps": self.plan_speed_mbps,
            "account_active": self.account_active,
            "bill_paid": self.bill_paid,
            "outstanding_amount": self.outstanding_amount,
            "fup_hit": self.fup_hit,
            "fup_speed_mbps": self.fup_speed_mbps,
            "plan_expiry": self.plan_expiry.isoformat() if self.plan_expiry else None,
            "data_used_gb": self.data_used_gb,
            "data_limit_gb": self.data_limit_gb,
            "created_at": self.created_at.isoformat() if self.created_at else None,
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
    site_id = db.Column(db.String(50), nullable=False, index=True)
    site_name = db.Column(db.String(100), nullable=True)
    cell_id = db.Column(db.String(100), nullable=True)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    zone = db.Column(db.String(100), default="")
    city = db.Column(db.String(100), nullable=True)
    state = db.Column(db.String(100), nullable=True)
    country = db.Column(db.String(100), default="India")
    technology = db.Column(db.String(50), nullable=True)
    site_status = db.Column(db.String(20), default="on_air")   # 'on_air' or 'off_air'
    alarms = db.Column(db.Text, default="")
    solution = db.Column(db.Text, default="")
    standard_solution_step = db.Column(db.Text, default="")
    bandwidth_mhz = db.Column(db.Float, nullable=True)
    antenna_gain_dbi = db.Column(db.Float, nullable=True)
    rf_power_eirp_dbm = db.Column(db.Float, nullable=True)
    antenna_height_agl_m = db.Column(db.Float, nullable=True)
    e_tilt_degree = db.Column(db.Float, nullable=True)
    crs_gain = db.Column(db.Float, nullable=True)

    __table_args__ = (
        db.UniqueConstraint("site_id", "cell_id", name="uq_telecom_sites_site_cell"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "site_id": self.site_id,
            "site_name": self.site_name or self.site_id,
            "cell_id": self.cell_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "zone": self.zone,
            "city": self.city,
            "state": self.state,
            "country": self.country or "India",
            "technology": self.technology or "",
            "site_status": self.site_status or "on_air",
            "alarms": self.alarms or "",
            "solution": self.solution or "",
            "standard_solution_step": self.standard_solution_step or "",
            "bandwidth_mhz": self.bandwidth_mhz,
            "antenna_gain_dbi": self.antenna_gain_dbi,
            "rf_power_eirp_dbm": self.rf_power_eirp_dbm,
            "antenna_height_agl_m": self.antenna_height_agl_m,
            "e_tilt_degree": self.e_tilt_degree,
            "crs_gain": self.crs_gain,
        }


class ParameterChange(db.Model):
    __tablename__ = "parameter_changes"

    id = db.Column(db.Integer, primary_key=True)
    ticket_id = db.Column(db.Integer, db.ForeignKey("tickets.id"), nullable=True)
    network_issue_id = db.Column(db.Integer, nullable=True)  # links to network_issue_tickets
    agent_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    approval_deadline = db.Column(db.DateTime, nullable=True)
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
            "network_issue_id": self.network_issue_id,
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


class ChangeRequest(db.Model):
    """
    ITIL-aligned Change Request lifecycle:
    created → classified → validated → approved → [pending_cto] → implementing → implemented → closed
    Rejection at validation resets to 'invalid' (max 2 times → auto_rejected).
    Rejection at approval → 'rejected'. Failed implementation → 'failed' → 'rolled_back'.
    For urgent/emergency: after manager approval → pending_cto → cto_approved/cto_rejected.
    """
    __tablename__ = "change_requests"

    id          = db.Column(db.Integer, primary_key=True)
    cr_number   = db.Column(db.String(30), unique=True, nullable=False)          # PCR-XXXX
    ticket_id   = db.Column(db.Integer, db.ForeignKey("tickets.id"), nullable=True)
    network_issue_id = db.Column(db.Integer, nullable=True)  # links to network_issue_tickets
    parameter_change_id = db.Column(db.Integer, db.ForeignKey("parameter_changes.id"), nullable=True)
    raised_by   = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)

    # Core content
    title              = db.Column(db.String(200), nullable=False)
    description        = db.Column(db.Text,        nullable=False)
    impact_assessment  = db.Column(db.Text, default="")
    rollback_plan      = db.Column(db.Text, default="")
    justification      = db.Column(db.Text, default="")

    # ── Complaint classification ────────────────────────────────────────────
    category           = db.Column(db.String(200), default="")
    subcategory        = db.Column(db.String(200), default="")
    telecom_domain_primary   = db.Column(db.String(50), default="")      # RAN/Core/Transport/Transmission/IMS
    telecom_domain_secondary = db.Column(db.String(200), default="")     # comma-separated

    # ── Location / Customer ─────────────────────────────────────────────────
    zone               = db.Column(db.String(100), default="")
    location           = db.Column(db.String(200), default="")
    nearest_site_id    = db.Column(db.String(50), default="")
    customer_type      = db.Column(db.String(20), default="")           # platinum/gold/silver/bronze

    # ── RF Parameters (current + proposed) ──────────────────────────────────
    rf_bandwidth_current     = db.Column(db.Float, nullable=True)
    rf_bandwidth_proposed    = db.Column(db.Float, nullable=True)
    rf_antenna_gain_current  = db.Column(db.Float, nullable=True)
    rf_antenna_gain_proposed = db.Column(db.Float, nullable=True)
    rf_eirp_current          = db.Column(db.Float, nullable=True)
    rf_eirp_proposed         = db.Column(db.Float, nullable=True)
    rf_antenna_height_current  = db.Column(db.Float, nullable=True)
    rf_antenna_height_proposed = db.Column(db.Float, nullable=True)
    rf_etilt_current         = db.Column(db.Float, nullable=True)
    rf_etilt_proposed        = db.Column(db.Float, nullable=True)
    rf_crs_gain_current      = db.Column(db.Float, nullable=True)
    rf_crs_gain_proposed     = db.Column(db.Float, nullable=True)

    # ── PDF upload ──────────────────────────────────────────────────────────
    pdf_filename       = db.Column(db.String(300), default="")
    pdf_path           = db.Column(db.String(500), default="")

    # Status: created|classified|invalid|auto_rejected|validated|approved|rejected|
    #         pending_cto|cto_approved|cto_rejected|implementing|implemented|failed|rolled_back|closed
    status      = db.Column(db.String(30), default="created")
    change_type = db.Column(db.String(20), nullable=True)   # standard|normal|urgent|emergency

    # ── CR SLA ──────────────────────────────────────────────────────────────
    cr_sla_hours       = db.Column(db.Float, nullable=True)
    cr_sla_deadline    = db.Column(db.DateTime, nullable=True)
    cr_sla_breached    = db.Column(db.Boolean, default=False)
    cr_breach_alert_sent = db.Column(db.Boolean, default=False)
    cr_alert_75_sent   = db.Column(db.Boolean, default=False)
    cr_alert_90_sent   = db.Column(db.Boolean, default=False)

    # ── Manager assignment ──────────────────────────────────────────────────
    assigned_manager_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)

    # ── Validation ──────────────────────────────────────────────────────────
    rejection_count    = db.Column(db.Integer, default=0)
    validation_remark  = db.Column(db.Text, default="")
    validated_by       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    validated_at       = db.Column(db.DateTime, nullable=True)

    # ── Classification (by agent at creation time) ──────────────────────────
    classification_note = db.Column(db.Text, default="")
    classified_by       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    classified_at       = db.Column(db.DateTime, nullable=True)

    # ── Approval ─────────────────────────────────────────────────────────────
    approval_remark = db.Column(db.Text, default="")
    approved_by     = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    approved_at     = db.Column(db.DateTime, nullable=True)
    manager_proposed_changes = db.Column(db.Text, default="")   # JSON: manager's proposed RF modifications

    # ── CTO Approval (urgent/emergency only) ─────────────────────────────────
    cto_approval_required = db.Column(db.Boolean, default=False)
    cto_approved_by       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    cto_approved_at       = db.Column(db.DateTime, nullable=True)
    cto_status            = db.Column(db.String(20), default="")   # pending_cto|cto_approved|cto_rejected
    cto_remark            = db.Column(db.Text, default="")

    # ── Implementation ───────────────────────────────────────────────────────
    implementation_notes = db.Column(db.Text, default="")
    implemented_by       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    implemented_at       = db.Column(db.DateTime, nullable=True)

    # ── Rollback ─────────────────────────────────────────────────────────────
    rollback_notes = db.Column(db.Text, default="")
    rollback_at    = db.Column(db.DateTime, nullable=True)

    # ── Closure ──────────────────────────────────────────────────────────────
    closure_notes = db.Column(db.Text, default="")
    closed_at     = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    ticket            = db.relationship("Ticket",          foreign_keys=[ticket_id],   backref="change_requests")
    parameter_change  = db.relationship("ParameterChange", foreign_keys=[parameter_change_id], backref="change_request", uselist=False)
    raiser            = db.relationship("User", foreign_keys=[raised_by],     backref="raised_crs")
    assigned_manager  = db.relationship("User", foreign_keys=[assigned_manager_id], backref="assigned_crs")
    validator         = db.relationship("User", foreign_keys=[validated_by],  backref="validated_crs")
    classifier        = db.relationship("User", foreign_keys=[classified_by], backref="classified_crs")
    approver          = db.relationship("User", foreign_keys=[approved_by],   backref="approved_crs")
    cto_approver      = db.relationship("User", foreign_keys=[cto_approved_by], backref="cto_approved_crs")
    implementer       = db.relationship("User", foreign_keys=[implemented_by],backref="implemented_crs")
    audit_entries     = db.relationship("CRAuditTrail", backref="change_request", order_by="CRAuditTrail.created_at")

    def to_dict(self):
        t = self.ticket
        return {
            "id":                   self.id,
            "cr_number":            self.cr_number,
            "ticket_id":            self.ticket_id,
            "network_issue_id":     self.network_issue_id,
            "ticket_ref":           t.reference_number if t else "",
            "ticket_priority":      t.priority         if t else "medium",
            "ticket_domain":        t.domain           if t else "",
            "ticket_category":      t.category         if t else "",
            "ticket_subcategory":   t.subcategory      if t else "",
            "ticket_user_name":     t.user.name        if t and t.user else "",
            "parameter_change_id":  self.parameter_change_id,
            "raised_by":            self.raised_by,
            "raised_by_name":       self.raiser.name   if self.raiser else "",
            "title":                self.title,
            "description":          self.description or "",
            "justification":        self.justification or "",
            "impact_assessment":    self.impact_assessment  or "",
            "rollback_plan":        self.rollback_plan       or "",
            "category":             self.category or "",
            "subcategory":          self.subcategory or "",
            "telecom_domain_primary":   self.telecom_domain_primary or "",
            "telecom_domain_secondary": self.telecom_domain_secondary or "",
            "zone":                 self.zone or "",
            "location":             self.location or "",
            "nearest_site_id":      self.nearest_site_id or "",
            "customer_type":        self.customer_type or "",
            "rf_params": {
                "bandwidth":     {"current": round(self.rf_bandwidth_current, 1) if self.rf_bandwidth_current else None,     "proposed": round(self.rf_bandwidth_proposed, 1) if self.rf_bandwidth_proposed else None},
                "antenna_gain":  {"current": round(self.rf_antenna_gain_current, 1) if self.rf_antenna_gain_current else None,  "proposed": round(self.rf_antenna_gain_proposed, 1) if self.rf_antenna_gain_proposed else None},
                "eirp":          {"current": round(self.rf_eirp_current, 1) if self.rf_eirp_current else None,          "proposed": round(self.rf_eirp_proposed, 1) if self.rf_eirp_proposed else None},
                "antenna_height":{"current": round(self.rf_antenna_height_current, 1) if self.rf_antenna_height_current else None,"proposed": round(self.rf_antenna_height_proposed, 1) if self.rf_antenna_height_proposed else None},
                "etilt":         {"current": round(self.rf_etilt_current, 1) if self.rf_etilt_current else None,         "proposed": round(self.rf_etilt_proposed, 1) if self.rf_etilt_proposed else None},
                "crs_gain":      {"current": round(self.rf_crs_gain_current, 1) if self.rf_crs_gain_current else None,      "proposed": round(self.rf_crs_gain_proposed, 1) if self.rf_crs_gain_proposed else None},
            },
            "pdf_filename":         self.pdf_filename or "",
            "status":               self.status,
            "change_type":          self.change_type,
            "cr_sla_hours":         self.cr_sla_hours,
            "cr_sla_deadline":      self.cr_sla_deadline.isoformat() if self.cr_sla_deadline else None,
            "cr_sla_breached":      self.cr_sla_breached or False,
            "assigned_manager_id":  self.assigned_manager_id,
            "assigned_manager_name": self.assigned_manager.name if self.assigned_manager else "",
            "assigned_manager_email": self.assigned_manager.email if self.assigned_manager else "",
            "rejection_count":      self.rejection_count,
            "validation_remark":    self.validation_remark   or "",
            "validated_by_name":    self.validator.name  if self.validator  else None,
            "validated_at":         self.validated_at.isoformat()  if self.validated_at  else None,
            "classification_note":  self.classification_note or "",
            "classified_by_name":   self.classifier.name if self.classifier else None,
            "classified_at":        self.classified_at.isoformat() if self.classified_at else None,
            "approval_remark":      self.approval_remark     or "",
            "approved_by_name":     self.approver.name   if self.approver   else None,
            "approved_at":          self.approved_at.isoformat()   if self.approved_at   else None,
            "manager_proposed_changes": self.manager_proposed_changes or "",
            "cto_approval_required": self.cto_approval_required,
            "cto_status":           self.cto_status or "",
            "cto_remark":           self.cto_remark or "",
            "cto_approved_by_name": self.cto_approver.name if self.cto_approver else None,
            "cto_approved_at":      self.cto_approved_at.isoformat() if self.cto_approved_at else None,
            "implementation_notes": self.implementation_notes or "",
            "implemented_by_name":  self.implementer.name if self.implementer else None,
            "implemented_at":       self.implemented_at.isoformat() if self.implemented_at else None,
            "rollback_notes":       self.rollback_notes      or "",
            "rollback_at":          self.rollback_at.isoformat()    if self.rollback_at    else None,
            "closure_notes":        self.closure_notes       or "",
            "closed_at":            self.closed_at.isoformat()      if self.closed_at      else None,
            "created_at":           self.created_at.isoformat()     if self.created_at     else None,
            "updated_at":           self.updated_at.isoformat()     if self.updated_at     else None,
        }


class CRAuditTrail(db.Model):
    """Tracks every status change on a ChangeRequest with timestamps and actor."""
    __tablename__ = "cr_audit_trail"

    id          = db.Column(db.Integer, primary_key=True)
    cr_id       = db.Column(db.Integer, db.ForeignKey("change_requests.id"), nullable=False, index=True)
    action      = db.Column(db.String(50), nullable=False)     # created, classified, validated, approved, etc.
    performed_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    old_status  = db.Column(db.String(30), default="")
    new_status  = db.Column(db.String(30), default="")
    notes       = db.Column(db.Text, default="")
    created_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    performer   = db.relationship("User", foreign_keys=[performed_by])

    def to_dict(self):
        return {
            "id": self.id,
            "cr_id": self.cr_id,
            "action": self.action,
            "performed_by": self.performed_by,
            "performed_by_name": self.performer.name if self.performer else "",
            "old_status": self.old_status,
            "new_status": self.new_status,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class CrSlaAlert(db.Model):
    """SLA alerts for Change Requests — sent to CTO/manager when CR SLA approaches or breaches."""
    __tablename__ = "cr_sla_alerts"

    id             = db.Column(db.Integer, primary_key=True)
    cr_id          = db.Column(db.Integer, db.ForeignKey("change_requests.id"), nullable=False)
    alert_level    = db.Column(db.String(10), nullable=False)     # '75', '90', 'breach'
    recipient_role = db.Column(db.String(20), nullable=False)     # 'manager' or 'cto'
    message        = db.Column(db.String(500), nullable=False)
    is_read        = db.Column(db.Boolean, default=False)
    created_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    cr = db.relationship("ChangeRequest", backref="sla_alerts")

    def to_dict(self):
        c = self.cr
        return {
            "id": self.id,
            "cr_id": self.cr_id,
            "cr_number": c.cr_number if c else "",
            "alert_level": self.alert_level,
            "recipient_role": self.recipient_role,
            "message": self.message,
            "is_read": self.is_read,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "change_type": c.change_type if c else "",
            "status": c.status if c else "",
            "title": c.title if c else "",
            "category": c.category if c else "",
            "subcategory": c.subcategory if c else "",
            "zone": c.zone if c else "",
            "cr_sla_hours": c.cr_sla_hours if c else None,
            "cr_sla_deadline": c.cr_sla_deadline.isoformat() if c and c.cr_sla_deadline else None,
            "raised_by_name": c.raiser.name if c and c.raiser else "",
            "assigned_manager_name": c.assigned_manager.name if c and c.assigned_manager else "",
            "ticket_ref": c.ticket.reference_number if c and c.ticket else "",
            "source": "customer" if (c and c.ticket_id and not c.network_issue_id) else "ai",
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


class FlexibleKpiUpload(db.Model):
    """
    Schema-flexible EAV storage for Core KPI and Revenue KPI files uploaded by admin.
    Only site_id is mandatory. Every other column the admin uploads is stored as a
    key→value pair so the DB schema never needs migration when columns change.

    kpi_type   : 'core' | 'revenue'
    column_name: normalised column name (e.g. 'auth_success_rate', 'revenue_jan')
    column_type: auto-detected  'numeric' | 'text' | 'date'
    num_value  : populated when column_type == 'numeric'
    str_value  : populated when column_type == 'text' | 'date'
    upload_batch: UUID string grouping all rows from one upload
    """
    __tablename__ = "flexible_kpi_uploads"

    id           = db.Column(db.Integer, primary_key=True)
    kpi_type     = db.Column(db.String(20), nullable=False, index=True)
    upload_batch = db.Column(db.String(40), nullable=False, index=True)
    site_id      = db.Column(db.String(100), nullable=False, index=True)
    column_name  = db.Column(db.String(120), nullable=False, index=True)
    column_type  = db.Column(db.String(10), nullable=False, default='numeric')
    num_value    = db.Column(db.Float, nullable=True)
    str_value    = db.Column(db.String(500), nullable=True)
    row_date     = db.Column(db.Date, nullable=True)
    kpi_name     = db.Column(db.String(120), nullable=True, index=True)
    uploaded_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.Index("idx_flex_type_site_col", "kpi_type", "site_id", "column_name"),
    )

    def to_dict(self):
        return {
            "id":           self.id,
            "kpi_type":     self.kpi_type,
            "upload_batch": self.upload_batch,
            "site_id":      self.site_id,
            "column_name":  self.column_name,
            "column_type":  self.column_type,
            "value":        self.num_value if self.column_type == 'numeric' else self.str_value,
            "row_date":     self.row_date.isoformat() if self.row_date else None,
        }


class FlexibleKpiMeta(db.Model):
    """
    Stores schema metadata for each upload batch so the dashboard knows
    what columns are available, their labels, units and types.
    """
    __tablename__ = "flexible_kpi_meta"

    id           = db.Column(db.Integer, primary_key=True)
    kpi_type     = db.Column(db.String(20), nullable=False, index=True)
    upload_batch = db.Column(db.String(40), nullable=False)
    column_name  = db.Column(db.String(120), nullable=False)
    column_label = db.Column(db.String(200), nullable=True)
    column_type  = db.Column(db.String(10), nullable=False, default='numeric')
    unit         = db.Column(db.String(30), nullable=True)
    is_active    = db.Column(db.Boolean, default=True)
    uploaded_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("kpi_type", "upload_batch", "column_name",
                            name="uq_flex_meta_type_batch_col"),
    )