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
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "sla_hours": self.sla_hours,
            "sla_deadline": self.sla_deadline.isoformat() if self.sla_deadline else None,
            "sla_breached": self.sla_breached,
            "escalated_by": self.escalated_by,
            "escalated_by_name": self.escalator.name if self.escalator else None,
            "escalated_at": self.escalated_at.isoformat() if self.escalated_at else None,
            "escalation_note": self.escalation_note or "",
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
    site_id = db.Column(db.String(50), nullable=False, index=True)
    site_name = db.Column(db.String(100), nullable=True)
    cell_id = db.Column(db.String(100), nullable=True)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    zone = db.Column(db.String(100), default="")
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


class ChangeRequest(db.Model):
    """
    ITIL-aligned Change Request lifecycle:
    created → validated → classified → approved → implementing → implemented → closed
    Rejection at validation resets to 'invalid' (max 2 times → auto_rejected).
    Rejection at approval → 'rejected'. Failed implementation → 'failed' → 'rolled_back'.
    """
    __tablename__ = "change_requests"

    id          = db.Column(db.Integer, primary_key=True)
    cr_number   = db.Column(db.String(30), unique=True, nullable=False)          # CR-20260320-A3F7
    ticket_id   = db.Column(db.Integer, db.ForeignKey("tickets.id"), nullable=False)
    parameter_change_id = db.Column(db.Integer, db.ForeignKey("parameter_changes.id"), nullable=True)
    raised_by   = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)

    # Core content
    title              = db.Column(db.String(200), nullable=False)
    description        = db.Column(db.Text,        nullable=False)
    impact_assessment  = db.Column(db.Text, default="")
    rollback_plan      = db.Column(db.Text, default="")

    # Status: created|invalid|auto_rejected|validated|classified|approved|rejected|
    #         implementing|implemented|failed|rolled_back|closed
    status      = db.Column(db.String(30), default="created")
    change_type = db.Column(db.String(20), nullable=True)   # standard|normal|emergency

    # ── Validation ──────────────────────────────────────────────────────────
    rejection_count    = db.Column(db.Integer, default=0)
    validation_remark  = db.Column(db.Text, default="")
    validated_by       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    validated_at       = db.Column(db.DateTime, nullable=True)

    # ── Classification ───────────────────────────────────────────────────────
    classification_note = db.Column(db.Text, default="")
    classified_by       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    classified_at       = db.Column(db.DateTime, nullable=True)

    # ── Approval ─────────────────────────────────────────────────────────────
    approval_remark = db.Column(db.Text, default="")
    approved_by     = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    approved_at     = db.Column(db.DateTime, nullable=True)

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
    validator         = db.relationship("User", foreign_keys=[validated_by],  backref="validated_crs")
    classifier        = db.relationship("User", foreign_keys=[classified_by], backref="classified_crs")
    approver          = db.relationship("User", foreign_keys=[approved_by],   backref="approved_crs")
    implementer       = db.relationship("User", foreign_keys=[implemented_by],backref="implemented_crs")

    def to_dict(self):
        t = self.ticket
        return {
            "id":                   self.id,
            "cr_number":            self.cr_number,
            "ticket_id":            self.ticket_id,
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
            "description":          self.description,
            "impact_assessment":    self.impact_assessment  or "",
            "rollback_plan":        self.rollback_plan       or "",
            "status":               self.status,
            "change_type":          self.change_type,
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
