"""
Core Network Predictive Ticketing — MME / SGW / PGW / HSS / PCRF.

Flow:
- Background job runs every hour at :55 (5 mins before the hour completes).
  It computes the hourly average from four 15-min readings (xx:00, xx:15, xx:30, xx:45)
  for each (component_type, component_id, kpi_name) tuple.
- For each hourly average:
    * normal      → no action
    * degradation → CoreAlert (only assigned agent sees it; agent acknowledges to clear)
    * outage      → CoreTicket (auto-routed to agent with SLA; visible in hourly routing)
- Catch-up: when the server starts, the job back-fills any hours since the last run
  whose average has not yet been computed (uses CoreJobLog as a marker).
- Tickets carry: trend analysis, RCA (OpenAI), parameter recommendations (OpenAI over
  uploaded CoreParameter rows), change-parameter request, mark-resolved.
"""
from __future__ import annotations

import logging
import json
from datetime import datetime, timedelta, date as _date, timezone
from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text as sa_text, inspect as sa_inspect
from sqlalchemy.orm import joinedload

from models import db, User, CoreComponentKpi

_LOG = logging.getLogger("core_tickets")
_LOG.setLevel(logging.INFO)
if not _LOG.handlers:
    _LOG.addHandler(logging.StreamHandler())

core_tickets_bp = Blueprint("core_tickets", __name__)

# OpenAI client/deployment, set later from app.py
_LLM_CLIENT = None
_LLM_DEPLOYMENT = None


def init_llm(client, deployment):
    """Wire the shared Azure OpenAI client into this module."""
    global _LLM_CLIENT, _LLM_DEPLOYMENT
    _LLM_CLIENT = client
    _LLM_DEPLOYMENT = deployment


# ─────────────────────────────────────────────────────────────────────────────
# Models — defined locally to keep models.py untouched. Tables auto-create at
# app startup via `*_table.create(bind=db.engine, checkfirst=True)`.
# ─────────────────────────────────────────────────────────────────────────────
class CoreAlert(db.Model):
    __tablename__ = "core_alerts"

    id              = db.Column(db.Integer, primary_key=True)
    component_type  = db.Column(db.String(20), nullable=False, index=True)
    component_id    = db.Column(db.String(50), nullable=False, index=True)
    kpi_name        = db.Column(db.String(120), nullable=False)
    hour_date       = db.Column(db.Date, nullable=False)
    hour_value      = db.Column(db.Integer, nullable=False)        # 0–23
    avg_value       = db.Column(db.Float, nullable=True)
    threshold_low   = db.Column(db.Float, nullable=True)
    threshold_high  = db.Column(db.Float, nullable=True)
    severity        = db.Column(db.String(20), default="degradation")
    forecast_minutes = db.Column(db.Integer, default=60)           # forecast time-to-outage
    forecast_message = db.Column(db.String(500), default="")
    assigned_agent  = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True, index=True)
    acknowledged    = db.Column(db.Boolean, default=False, index=True)
    acknowledged_at = db.Column(db.DateTime, nullable=True)
    created_at      = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.Index("idx_core_alert_comp_kpi_hour",
                 "component_type", "component_id", "kpi_name", "hour_date", "hour_value"),
    )


class CoreTicket(db.Model):
    __tablename__ = "core_tickets"

    id              = db.Column(db.Integer, primary_key=True)
    reference_number = db.Column(db.String(40), unique=True, nullable=False)
    component_type  = db.Column(db.String(20), nullable=False, index=True)
    component_id    = db.Column(db.String(50), nullable=False, index=True)
    kpi_name        = db.Column(db.String(120), nullable=False)
    hour_date       = db.Column(db.Date, nullable=False)
    hour_value      = db.Column(db.Integer, nullable=False)
    avg_value       = db.Column(db.Float, nullable=True)
    threshold_value = db.Column(db.Float, nullable=True)           # outage edge crossed
    priority        = db.Column(db.String(20), default="High")
    sla_hours       = db.Column(db.Float, default=4.0)
    sla_deadline    = db.Column(db.DateTime, nullable=True)
    status          = db.Column(db.String(30), default="open")     # open|in_progress|resolved|closed
    assigned_agent  = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True, index=True)
    root_cause      = db.Column(db.Text, default="")
    recommendation  = db.Column(db.Text, default="")
    resolution_notes = db.Column(db.Text, default="")
    timestamp_at    = db.Column(db.DateTime, nullable=False)       # the hour-end this ticket pertains to
    created_at      = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at      = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    resolved_at     = db.Column(db.DateTime, nullable=True)

    __table_args__ = (
        db.Index("idx_core_ticket_comp_kpi_hour",
                 "component_type", "component_id", "kpi_name", "hour_date", "hour_value"),
    )


class CoreParameter(db.Model):
    """Parameters uploaded by admin via the Core Parameter Upload sheet.

    The Excel sheet name = component_type (MME/SGW/PGW/HSS/PCRF).
    Columns expected (case/space tolerant):
        kpi_name, parameter_group, parameter_name, unit, <component_id columns>
    Each <component_id column> (e.g. MME1, MME2) becomes one row per (parameter,
    component_id) holding its current value.
    """
    __tablename__ = "core_parameters"

    id              = db.Column(db.Integer, primary_key=True)
    component_type  = db.Column(db.String(20), nullable=False, index=True)
    component_id    = db.Column(db.String(50), nullable=False, index=True)  # MME1, MME2, …
    kpi_name        = db.Column(db.String(120), nullable=True, index=True)
    parameter_group = db.Column(db.String(120), nullable=True)
    parameter_name  = db.Column(db.String(200), nullable=False)
    current_value   = db.Column(db.String(120), nullable=True)
    unit            = db.Column(db.String(40), nullable=True)
    upload_batch    = db.Column(db.String(40), nullable=True)
    uploaded_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.Index("idx_core_param_lookup", "component_type", "component_id", "kpi_name"),
    )


class CoreParameterChange(db.Model):
    """Change requests raised from a Core Ticket — replaces RF parameters with
    core parameters specific to MME/SGW/PGW/HSS/PCRF.

    Workflow mirrors the worst-cell CR flow:
      - Standard / Normal → status = 'pending' awaiting manager
                          → 'approved' / 'disapproved'
      - Urgent / Emergency (Critical / High priority on the source ticket)
                          → 'pending' awaiting manager
                          → 'pending_cto' once manager approves
                          → 'cto_approved' / 'cto_rejected'
    """
    __tablename__ = "core_parameter_changes"

    id              = db.Column(db.Integer, primary_key=True)
    core_ticket_id  = db.Column(db.Integer, db.ForeignKey("core_tickets.id"), nullable=False, index=True)
    component_type  = db.Column(db.String(20), nullable=False)
    component_id    = db.Column(db.String(50), nullable=False)
    parameter_group = db.Column(db.String(120), nullable=True)
    parameter_name  = db.Column(db.String(200), nullable=False)
    current_value   = db.Column(db.String(120), nullable=True)
    proposed_value  = db.Column(db.String(120), nullable=False)
    unit            = db.Column(db.String(40), nullable=True)
    reason          = db.Column(db.Text, default="")
    agent_id        = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    # Workflow & routing
    change_type     = db.Column(db.String(20), default="standard")  # standard|normal|urgent|emergency
    cto_required    = db.Column(db.Boolean, default=False)
    assigned_manager_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    assigned_cto_id     = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    status          = db.Column(db.String(30), default="pending")  # pending|approved|disapproved|pending_cto|cto_approved|cto_rejected
    manager_note    = db.Column(db.Text, default="")
    cto_note        = db.Column(db.Text, default="")
    reviewed_by     = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    reviewed_at     = db.Column(db.DateTime, nullable=True)
    created_at      = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class CoreKpiThreshold(db.Model):
    """Operator-uploaded thresholds per (component_type, kpi_name).

    Overrides the in-memory `KPI_RANGES` defaults at runtime — every read
    of the threshold catalogue (classification, scaling, trend bands, RCA
    prompt) consults this table first via `_effective_kpi_cfg`.

    Excel sheet columns that are accepted (case- and whitespace-insensitive):
        component_type | component | type        — required (or sheet name)
        kpi_name | kpi                            — required
        direction                                  — optional: "higher_is_better"
                                                     or "lower_is_better".
                                                     Auto-detected from the
                                                     numeric ordering of normal
                                                     vs critical when omitted.
        normal_min | normal_low | normal           — start of the normal band
        normal_max | normal_high                   — end of the normal band
        degrade_min | degrade_low | degradation_low
        degrade_max | degrade_high | degradation_high
        critical_min | critical_low                — start of critical band
        critical_max | critical_high               — end of critical band
        unit                                       — e.g. "%", "ms"
        color                                      — optional hex, e.g. "#00338D"
    """
    __tablename__ = "core_kpi_thresholds"

    id              = db.Column(db.Integer, primary_key=True)
    component_type  = db.Column(db.String(20), nullable=False, index=True)
    kpi_name        = db.Column(db.String(160), nullable=False, index=True)
    direction       = db.Column(db.String(30), nullable=False)  # higher_is_better | lower_is_better
    normal_low      = db.Column(db.Float, nullable=True)
    normal_high     = db.Column(db.Float, nullable=True)
    degrade_low     = db.Column(db.Float, nullable=True)
    degrade_high    = db.Column(db.Float, nullable=True)
    critical_low    = db.Column(db.Float, nullable=True)
    critical_high   = db.Column(db.Float, nullable=True)
    unit            = db.Column(db.String(20), default='')
    color           = db.Column(db.String(20), default='#64748b')
    upload_batch    = db.Column(db.String(40), nullable=True)
    uploaded_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("component_type", "kpi_name", name="uq_core_thr_comp_kpi"),
    )


class CoreJobLog(db.Model):
    """Per-hour idempotency marker so the catch-up logic doesn't double-run."""
    __tablename__ = "core_job_log"

    id          = db.Column(db.Integer, primary_key=True)
    hour_date   = db.Column(db.Date, nullable=False)
    hour_value  = db.Column(db.Integer, nullable=False)
    started_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = db.Column(db.DateTime, nullable=True)
    alerts_created  = db.Column(db.Integer, default=0)
    tickets_created = db.Column(db.Integer, default=0)

    __table_args__ = (
        db.UniqueConstraint("hour_date", "hour_value", name="uq_core_job_log_hour"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# KPI threshold catalogue — defines normal / degradation / critical bands per
# (component_type, kpi_name). A "degradation" hourly avg → alert. A "critical"
# hourly avg → ticket.
#
# `direction`:
#   higher_is_better → outage when avg < critical_low; degradation when
#                      degrade_low ≤ avg < normal_low.
#   lower_is_better  → outage when avg > critical_high; degradation when
#                      normal_high < avg ≤ degrade_high.
# ─────────────────────────────────────────────────────────────────────────────
# Threshold catalogue — values transcribed directly from the operator's
# uploaded KPI threshold sheets (MME / SGW / PGW / HSS / PCRF). For every
# KPI the three bands are encoded by:
#   higher_is_better  →  Normal  : avg ≥ normal_low
#                       Degrade : critical_low ≤ avg < normal_low
#                       Outage  : avg < critical_low
#   lower_is_better   →  Normal  : avg ≤ normal_high
#                       Degrade : normal_high < avg ≤ critical_high
#                       Outage  : avg > critical_high
KPI_RANGES = {
    # ── MME ─────────────────────────────────────────────────────────────────
    "MME": {
        "Attach Success Rate":          {"direction": "higher_is_better", "normal_low": 99.80, "critical_low": 99.50, "unit": "%",     "color": "#00338D"},
        "Service Request Success Rate": {"direction": "higher_is_better", "normal_low": 99.80, "critical_low": 99.50, "unit": "%",     "color": "#0091DA"},
        "Paging Success Rate":          {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",     "color": "#1E40AF"},
        "CSFB Success Rate":            {"direction": "higher_is_better", "normal_low": 99.00, "critical_low": 98.00, "unit": "%",     "color": "#2563EB"},
        "SRVCC Success Rate":           {"direction": "higher_is_better", "normal_low": 99.00, "critical_low": 98.00, "unit": "%",     "color": "#3B82F6"},
        "HOSR":                         {"direction": "higher_is_better", "normal_low": 99.50, "critical_low": 99.00, "unit": "%",     "color": "#06B6D4"},
        "Context Setup Failure Rate":   {"direction": "lower_is_better",  "normal_high": 0.30, "critical_high": 1.00, "unit": "%",     "color": "#DC2626"},
        "CPU Utilization":              {"direction": "lower_is_better",  "normal_high": 50,    "critical_high": 80,   "unit": "%",     "color": "#7C3AED"},
        "Memory Utilization":           {"direction": "lower_is_better",  "normal_high": 65,    "critical_high": 80,   "unit": "%",     "color": "#B8094A"},
        "Thread Process Queue Depth":   {"direction": "lower_is_better",  "normal_high": 20,    "critical_high": 50,   "unit": "count", "color": "#0EA5E9"},
        "Bearer Count":                 {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "% cap", "color": "#16A34A"},
        "TCP Session Count":            {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "% cap", "color": "#0F766E"},
        "SCTP Association Status":      {"direction": "higher_is_better", "normal_low": 99.80, "critical_low": 95,    "unit": "% UP",  "color": "#6D2077"},
    },

    # ── HSS ─────────────────────────────────────────────────────────────────
    "HSS": {
        "Authentication Success Rate":      {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.70, "unit": "%",  "color": "#DC2626"},
        "Location Update Success Rate":     {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.70, "unit": "%",  "color": "#EA580C"},
        "Profile Retrieval Success Rate":   {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#D97706"},
        "Authentication Failure Rate":      {"direction": "lower_is_better",  "normal_high": 0.05,  "critical_high": 0.30, "unit": "%",  "color": "#991B1B"},
        "S6a Transaction Success Rate":     {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#7E22CE"},
        "S6a Response Latency":             {"direction": "lower_is_better",  "normal_high": 20,    "critical_high": 50,   "unit": "ms", "color": "#A21CAF"},
        "S6a Timeout Rate":                 {"direction": "lower_is_better",  "normal_high": 0.01,  "critical_high": 0.05, "unit": "%",  "color": "#BE185D"},
        "Cx Success Rate":                  {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#0EA5E9"},
        "Cx Response Latency":              {"direction": "lower_is_better",  "normal_high": 20,    "critical_high": 40,   "unit": "ms", "color": "#0284C7"},
        "DB Query Success Rate":            {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#0F766E"},
        "DB Replication Lag":               {"direction": "lower_is_better",  "normal_high": 50,    "critical_high": 200,  "unit": "ms", "color": "#14532D"},
        "DB Lock Contention Rate":          {"direction": "lower_is_better",  "normal_high": 0.05,  "critical_high": 0.50, "unit": "%",  "color": "#15803D"},
        "CPU Utilization":                  {"direction": "lower_is_better",  "normal_high": 45,    "critical_high": 75,   "unit": "%",  "color": "#16A34A"},
        "Memory Utilization":               {"direction": "lower_is_better",  "normal_high": 65,    "critical_high": 85,   "unit": "%",  "color": "#65A30D"},
        "Diameter TPS vs Capacity":         {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "%",  "color": "#CA8A04"},
        "Thread Worker Queue Depth":        {"direction": "lower_is_better",  "normal_high": 30,    "critical_high": 60,   "unit": "%",  "color": "#92400E"},
        "HSS Availability":                 {"direction": "higher_is_better", "normal_low": 100,    "critical_low": 99.99, "unit": "%",  "color": "#CA8A04"},
        "Geo Redundancy Sync Status":       {"direction": "higher_is_better", "normal_low": 100,    "critical_low": 99.99, "unit": "%",  "color": "#52525B"},
        "Failover Time":                    {"direction": "lower_is_better",  "normal_high": 2,     "critical_high": 2,    "unit": "s",  "color": "#525252"},
    },

    # ── PGW ─────────────────────────────────────────────────────────────────
    "PGW": {
        "Default Bearer Setup Success Rate":  {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#7C3AED"},
        "Dedicated Bearer Success Rate":      {"direction": "higher_is_better", "normal_low": 99.70, "critical_low": 99.00, "unit": "%",  "color": "#A855F7"},
        "Session Setup Success Rate":         {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#6D28D9"},
        "PCRF PCF Interaction Success Rate":  {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#0284C7"},
        "Packet Loss DL UL":                  {"direction": "lower_is_better",  "normal_high": 0.03,  "critical_high": 0.10, "unit": "%",  "color": "#DC2626"},
        "User Plane Latency":                 {"direction": "lower_is_better",  "normal_high": 1,     "critical_high": 5,    "unit": "ms", "color": "#0EA5E9"},
        "Throughput Per Subscriber":          {"direction": "higher_is_better", "normal_low": 90,     "critical_low": 70,    "unit": "%",  "color": "#10B981"},
        "Jitter":                             {"direction": "lower_is_better",  "normal_high": 5,     "critical_high": 15,   "unit": "ms", "color": "#06B6D4"},
        "Charging Record Success Rate":       {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#0891B2"},
        "Online Charging Latency":            {"direction": "lower_is_better",  "normal_high": 50,    "critical_high": 100,  "unit": "ms", "color": "#0F766E"},
        "Policy Enforcement Accuracy":        {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.70, "unit": "%",  "color": "#16A34A"},
        "CPU Utilization":                    {"direction": "lower_is_better",  "normal_high": 50,    "critical_high": 80,   "unit": "%",  "color": "#4338CA"},
        "Memory Utilization":                 {"direction": "lower_is_better",  "normal_high": 65,    "critical_high": 85,   "unit": "%",  "color": "#C026D3"},
        "Active Sessions vs Capacity":        {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "%",  "color": "#7C3AED"},
        "NAT Table Utilization":              {"direction": "lower_is_better",  "normal_high": 65,    "critical_high": 80,   "unit": "%",  "color": "#9333EA"},
    },

    # ── SGW ─────────────────────────────────────────────────────────────────
    "SGW": {
        "Create Session Success Rate":     {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#059669"},
        "Modify Bearer Success Rate":      {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#10B981"},
        "Delete Session Success Rate":     {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#34D399"},
        "Handover Data Path Success Rate": {"direction": "higher_is_better", "normal_low": 99.70, "critical_low": 99.30, "unit": "%",  "color": "#0F766E"},
        "SGW Relocation Success Rate":     {"direction": "higher_is_better", "normal_low": 99.50, "critical_low": 99.00, "unit": "%",  "color": "#0E7490"},
        "Packet Loss":                     {"direction": "lower_is_better",  "normal_high": 0.05,  "critical_high": 0.10, "unit": "%",  "color": "#DC2626"},
        "User Plane Latency":              {"direction": "lower_is_better",  "normal_high": 1,     "critical_high": 3,    "unit": "ms", "color": "#0891B2"},
        "Throughput Utilization":          {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "%",  "color": "#65A30D"},
        "CPU Utilization":                 {"direction": "lower_is_better",  "normal_high": 50,    "critical_high": 80,   "unit": "%",  "color": "#0284C7"},
        "Memory Utilization":              {"direction": "lower_is_better",  "normal_high": 65,    "critical_high": 85,   "unit": "%",  "color": "#7C3AED"},
        "Bearer Count vs Capacity":        {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "%",  "color": "#16A34A"},
        "GTP-U Tunnel Availability":       {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.50, "unit": "%",  "color": "#15803D"},
    },

    # ── PCRF ────────────────────────────────────────────────────────────────
    "PCRF": {
        "Policy Decision Success Rate":      {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#EA580C"},
        "Session Establishment Success Rate":{"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#F97316"},
        "Charging Rule Install Success Rate":{"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#FB923C"},
        "Charging Rule Update Success Rate": {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#FDBA74"},
        "Policy Enforcement Accuracy":       {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.80, "unit": "%",  "color": "#E11D48"},
        "Gx Success Rate":                   {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#BE185D"},
        "Gx Response Latency":               {"direction": "lower_is_better",  "normal_high": 30,    "critical_high": 80,   "unit": "ms", "color": "#92400E"},
        "Gx Timeout Rate":                   {"direction": "lower_is_better",  "normal_high": 0.01,  "critical_high": 0.05, "unit": "%",  "color": "#DC2626"},
        "OCS Sy Gy Success Rate":            {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#9F1239"},
        "Credit Control Failure Rate":       {"direction": "lower_is_better",  "normal_high": 0.01,  "critical_high": 0.05, "unit": "%",  "color": "#991B1B"},
        "Active Policy Sessions":            {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "%",  "color": "#A16207"},
        "Policy Session Retention Rate":     {"direction": "higher_is_better", "normal_low": 99.99, "critical_low": 99.90, "unit": "%",  "color": "#CA8A04"},
        "Dedicated Bearer Trigger Accuracy": {"direction": "higher_is_better", "normal_low": 99.90, "critical_low": 99.50, "unit": "%",  "color": "#A21CAF"},
        "CPU Utilization":                   {"direction": "lower_is_better",  "normal_high": 45,    "critical_high": 75,   "unit": "%",  "color": "#7C3AED"},
        "Memory Utilization":                {"direction": "lower_is_better",  "normal_high": 65,    "critical_high": 85,   "unit": "%",  "color": "#0EA5E9"},
        "Diameter TPS Usage":                {"direction": "lower_is_better",  "normal_high": 70,    "critical_high": 85,   "unit": "%",  "color": "#0F766E"},
        "PCRF Availability":                 {"direction": "higher_is_better", "normal_low": 100,    "critical_low": 99.99, "unit": "%",  "color": "#FB7185"},
    },
}

COMPONENT_TYPES = list(KPI_RANGES.keys())   # ["MME", "HSS", "PGW", "SGW", "PCRF"]

# ─────────────────────────────────────────────────────────────────────────────
# Monitored KPIs — the ONLY KPIs the hourly job will create alerts/tickets
# for. The other KPIs in KPI_RANGES are still loaded so they can be drawn in
# the trend chart as related/sibling KPIs and used by the RCA prompt for
# cross-correlation context.
# ─────────────────────────────────────────────────────────────────────────────
_MONITORED_KPIS = {
    "MME": [
        "CPU Utilization",
        "Memory Utilization",
        "Thread Process Queue Depth",
        "SCTP Association Status",
    ],
    "HSS": [
        "Authentication Success Rate",
        "S6a Transaction Success Rate",
        "S6a Response Latency",
        "HSS Availability",
    ],
    "PGW": [
        "Session Setup Success Rate",
        "Active Sessions vs Capacity",
        "CPU Utilization",
        "Memory Utilization",
    ],
    "SGW": [
        "Create Session Success Rate",
        "Bearer Count vs Capacity",
        "CPU Utilization",
        "Memory Utilization",
    ],
    "PCRF": [
        "Policy Decision Success Rate",
        "Gx Success Rate",
        "Gx Response Latency",
        "PCRF Availability",
    ],
}


def _is_monitored(component_type: str, kpi_name: str) -> bool:
    """Whether the hourly job should create alerts/tickets for this KPI."""
    if not component_type or not kpi_name:
        return False
    monitored = _MONITORED_KPIS.get(component_type.upper(), [])
    if kpi_name in monitored:
        return True
    canonical = _resolve_kpi_name(component_type, kpi_name)
    return canonical in monitored

# ─────────────────────────────────────────────────────────────────────────────
# Related-KPI graph — for each degraded KPI, which sibling KPIs to display in
# the trend panel and use for cross-correlation in the RCA prompt. Comes
# straight from the operator's specification.
# ─────────────────────────────────────────────────────────────────────────────
_RELATED_KPIS = {
    "MME": {
        "CPU Utilization":            ["Attach Success Rate", "Service Request Success Rate", "Paging Success Rate", "TAU Success Rate", "Active Subscriber Count"],
        "Memory Utilization":         ["CPU Utilization", "Attach Success Rate", "Service Request Success Rate", "Paging Success Rate", "Active Subscriber Count"],
        "Thread Process Queue Depth": ["CPU Utilization", "Memory Utilization", "Attach Success Rate", "Service Request Success Rate", "NAS Signaling Success Rate"],
        "SCTP Association Status":    ["Attach Success Rate", "Service Request Success Rate", "Paging Success Rate", "NAS Signaling Success Rate", "TCP Session Count"],
    },
    "HSS": {
        "Authentication Success Rate":  ["S6a Transaction Success Rate", "Authentication Failure Rate", "HSS Availability", "S6a Response Latency"],
        "S6a Transaction Success Rate": ["Authentication Success Rate", "S6a Response Latency", "S6a Timeout Rate"],
        "S6a Response Latency":         ["S6a Timeout Rate", "DB Replication Lag", "CPU Utilization"],
        "HSS Availability":             ["Authentication Success Rate", "S6a Transaction Success Rate", "S6a Response Latency", "S6a Timeout Rate"],
    },
    "PGW": {
        "Session Setup Success Rate":  ["Default Bearer Setup Success Rate", "Dedicated Bearer Success Rate", "CPU Utilization", "NAT Table Utilization"],
        "Active Sessions vs Capacity": ["Default Bearer Setup Success Rate", "Dedicated Bearer Success Rate", "CPU Utilization"],
        "CPU Utilization":             ["Session Setup Success Rate", "User Plane Latency", "Online Charging Latency"],
        "Memory Utilization":          ["CPU Utilization", "Session Setup Success Rate"],
    },
    "SGW": {
        "Create Session Success Rate": ["Default Bearer Success Rate", "PGW Session Setup Success Rate", "CPU Utilization"],
        "Bearer Count vs Capacity":    ["CPU Utilization", "Memory Utilization", "Session Setup Success Rate"],
        "CPU Utilization":             ["Session Setup Success Rate", "User Plane Latency", "Online Charging Latency"],
        "Memory Utilization":          ["CPU Utilization", "Session Setup Success Rate"],
    },
    "PCRF": {
        "Policy Decision Success Rate": ["Gx Success Rate", "Policy Enforcement Accuracy", "Session Establishment Success Rate"],
        "Gx Success Rate":              ["Gx Response Latency", "Gx Timeout Rate", "Session Establishment Success Rate"],
        "Gx Response Latency":          ["Gx Timeout Rate", "CPU Utilization", "Diameter TPS Usage"],
        "PCRF Availability":            ["Policy Decision Success Rate", "Gx Success Rate", "Gx Response Latency"],
    },
}


def _related_kpis(component_type: str, kpi_name: str) -> list[str]:
    """Return the operator-defined list of related KPIs for this (component, kpi)."""
    return list(_RELATED_KPIS.get(component_type, {}).get(kpi_name, []))


# ─────────────────────────────────────────────────────────────────────────────
# KPI Expert Knowledge Base
#
# Per-KPI engineering context that gets injected into the RCA prompt so the
# LLM produces technically-accurate analysis instead of generic prose. Each
# entry covers:
#   protocol     – the wire protocol(s) involved (S1AP, SCTP, S6a/Diameter,
#                  Gx, GTP-U, etc.)
#   role         – control plane / user plane / signalling / charging / DB
#   measures     – what the KPI literally counts
#   failure_modes– concrete sub-failures that drive this KPI down
#   signals      – sibling KPIs / counters that move in lock-step
#   impact       – downstream subscriber-facing effects
#   parameters   – configuration knobs that typically govern this KPI
# ─────────────────────────────────────────────────────────────────────────────
_KPI_EXPERT = {
    # ─────────────── MME ───────────────
    ("MME", "CPU Utilization"): {
        "protocol":      "Internal MME control-plane processing for S1AP/NAS/S11",
        "role":          "Control-plane compute capacity",
        "measures":      "Aggregate CPU usage across MME worker threads handling NAS, S1AP, paging and S6a interactions",
        "failure_modes": ["Attach storms or paging floods saturating the dispatcher",
                          "Stuck NAS retransmission loops",
                          "Inefficient TAU procedures during mass mobility events",
                          "Garbage-collection pauses on JVM-based stacks"],
        "signals":       ["Thread Process Queue Depth", "Attach Success Rate falling", "Service Request Success Rate falling"],
        "impact":        "Slow attach/TAU, dropped paging, failed VoLTE call setup, increased NAS retransmit rate",
        "parameters":    ["mme_thread_pool_size", "max_concurrent_attaches", "s1ap_dispatcher_queue_limit", "nas_t3450_timer"],
    },
    ("MME", "Memory Utilization"): {
        "protocol":      "MME UE-context, EMM/ECM session state, transaction maps",
        "role":          "Subscriber state retention",
        "measures":      "RAM footprint of UE contexts, transaction tables, internal queues",
        "failure_modes": ["UE-context leak from unfinished detach",
                          "Buffer accumulation when downstream peer (HSS/SGW) is slow",
                          "Long-running NAS transactions not garbage-collected"],
        "signals":       ["CPU Utilization rising", "Active subscriber count climbing", "S6a Response Latency"],
        "impact":        "Attach rejections with cause #22 (congestion), TAU failures, MME reboots when OOM",
        "parameters":    ["ue_context_retention_timer", "max_ue_contexts", "transaction_table_size"],
    },
    ("MME", "Thread Process Queue Depth"): {
        "protocol":      "S1AP/NAS dispatch queue inside MME",
        "role":          "Backpressure indicator on the control-plane pipeline",
        "measures":      "Number of queued S1AP/NAS messages awaiting a worker thread",
        "failure_modes": ["Worker thread starvation due to high CPU",
                          "Dependency stall (HSS S6a slow → MME blocks)",
                          "Mis-tuned thread-pool size vs offered load"],
        "signals":       ["CPU Utilization", "S6a Response Latency on HSS", "Attach Success Rate"],
        "impact":        "Service Request and TAU timeouts, NAS T3450/T3460 expirations, attach storms cascade",
        "parameters":    ["mme_thread_pool_size", "queue_high_watermark", "queue_drop_policy"],
    },
    ("MME", "SCTP Association Status"): {
        "protocol":      "SCTP transport on S1-MME (MME ↔ eNodeB) and S6a (MME ↔ HSS)",
        "role":          "Underlying reliable transport for S1AP and Diameter",
        "measures":      "Percent of expected SCTP associations currently UP and stable",
        "failure_modes": ["IP path flap between MME and eNB / HSS",
                          "Heartbeat (SCTP HB) loss exceeding rto_max",
                          "MTU/PMTU misconfiguration causing fragmentation",
                          "Firewall / IPsec tunnel renegotiation"],
        "signals":       ["Attach Success Rate dropping", "S1AP error counters", "Paging Success Rate"],
        "impact":        "eNodeBs disappear from MME, mass detach, no paging delivery, 4G outage in affected area",
        "parameters":    ["sctp_hb_interval", "sctp_path_max_retrans", "sctp_assoc_max_retrans", "sctp_rto_min/rto_max", "ip_pmtu_discovery"],
    },

    # ─────────────── HSS ───────────────
    ("HSS", "Authentication Success Rate"): {
        "protocol":      "Diameter S6a (MME ↔ HSS) — Authentication-Information-Request/Answer",
        "role":          "Subscriber authentication via Milenage / EPS-AKA vectors",
        "measures":      "Successful AIR/AIA exchanges over total attempts",
        "failure_modes": ["HSS DB lookup failures (subscriber not provisioned, USIM mismatch)",
                          "Crypto vector generation slow under load",
                          "Diameter peer congestion or timeouts",
                          "K/OPc/Algorithm mis-provisioning"],
        "signals":       ["S6a Response Latency", "Authentication Failure Rate", "DB Query Success Rate"],
        "impact":        "Attach reject (#11 PLMN not allowed / #15 no suitable cells) at scale, subscribers can't register, voice/data outage",
        "parameters":    ["hss_db_pool_size", "milenage_worker_threads", "diameter_tx_timer", "vector_cache_size"],
    },
    ("HSS", "S6a Transaction Success Rate"): {
        "protocol":      "Diameter S6a (Update-Location, Authentication-Information, Cancel-Location, Insert-Subscriber-Data)",
        "role":          "Subscription / mobility transaction layer between MME ↔ HSS",
        "measures":      "Successful S6a answers / total requests",
        "failure_modes": ["Diameter peer overload / DWR timeouts",
                          "HSS application-layer timeouts on slow DB queries",
                          "Routing-Realm misconfiguration",
                          "TLS/IPSec re-handshake on transport"],
        "signals":       ["S6a Response Latency", "S6a Timeout Rate", "Authentication Success Rate"],
        "impact":        "Attach/TAU/Service-Request failures, ULR failures cascade as Insert-Subscriber-Data rejects, no IMS registration",
        "parameters":    ["s6a_tx_timer", "diameter_overload_throttle", "peer_failure_detection_interval"],
    },
    ("HSS", "S6a Response Latency"): {
        "protocol":      "Diameter S6a request → answer round-trip",
        "role":          "Latency budget for subscription/auth procedures",
        "measures":      "Mean (or p95) request-to-answer time in ms",
        "failure_modes": ["DB replication lag stalling reads",
                          "Connection-pool exhaustion to subscriber DB",
                          "Lock contention on hot subscriber rows",
                          "Network latency between MME and HSS regions"],
        "signals":       ["DB Replication Lag", "DB Query Success Rate", "CPU Utilization"],
        "impact":        "MME T-timer expirations, Attach failures, subscribers see delayed service, perceived poor coverage",
        "parameters":    ["db_pool_size", "db_query_timeout", "diameter_tx_timer", "replication_lag_threshold"],
    },
    ("HSS", "HSS Availability"): {
        "protocol":      "Aggregate health of HSS (S6a, Cx, Sh, DB tier, geo-redundancy)",
        "role":          "End-to-end HSS service availability",
        "measures":      "Percent of time the HSS is answering all interfaces normally",
        "failure_modes": ["Geo-redundancy failover during a partial outage",
                          "DB primary unavailable with slow secondary promotion",
                          "Diameter peers all flapping",
                          "Underlying VM/container crashes"],
        "signals":       ["DB Replication Lag", "Geo Redundancy Sync Status", "All S6a/Cx KPIs"],
        "impact":        "Network-wide auth outage; subscribers cannot attach / re-attach; emergency callout territory",
        "parameters":    ["geo_failover_threshold", "db_election_timeout", "health_check_interval"],
    },

    # ─────────────── PGW ───────────────
    ("PGW", "Session Setup Success Rate"): {
        "protocol":      "GTP-C v2 over S5/S8 (PGW ↔ SGW) — Create-Session-Request/Response",
        "role":          "PDN connection establishment (data session activation)",
        "measures":      "Successful Create-Session responses / total requests",
        "failure_modes": ["IP-pool exhaustion on the PDN",
                          "PCRF Gx interaction failure / timeout",
                          "Diameter Rx/Sd issues",
                          "OCS (Sy/Gy) credit-control reject",
                          "PGW capacity at the session ceiling"],
        "signals":       ["PCRF PCF Interaction Success Rate", "Active Sessions vs Capacity", "CPU Utilization"],
        "impact":        "Subscriber attach succeeds but data session fails — no internet, ESM cause #26 / #27 / #29",
        "parameters":    ["pdp_pool_size", "max_active_sessions", "gx_tx_timer", "session_setup_retry_count"],
    },
    ("PGW", "Active Sessions vs Capacity"): {
        "protocol":      "Capacity counter against the configured session ceiling",
        "role":          "Capacity headroom for new PDN connections",
        "measures":      "Currently active PDP/EPS sessions as percent of provisioned ceiling",
        "failure_modes": ["Hitting the licensed session limit",
                          "Stale session leak (no idle-timeout enforcement)",
                          "Unbalanced load across PGW instances"],
        "signals":       ["Session Setup Success Rate", "CPU Utilization", "Memory Utilization"],
        "impact":        "New PDN connections rejected with cause #26 (insufficient resources); existing sessions OK",
        "parameters":    ["max_active_sessions", "session_idle_timeout", "session_balancer_weight"],
    },
    ("PGW", "CPU Utilization"): {
        "protocol":      "PGW data-plane (GTP-U) and control-plane (GTP-C) processing",
        "role":          "Compute capacity for packet forwarding + session management",
        "measures":      "Aggregate CPU across PGW worker cores",
        "failure_modes": ["Heavy DL/UL traffic on user plane",
                          "DPI / inline charging adding per-packet cost",
                          "Excessive Gx/Gy signalling per session"],
        "signals":       ["Session Setup Success Rate", "User Plane Latency", "Online Charging Latency"],
        "impact":        "Packet forwarding latency rises, OCS responses slow, throughput per subscriber drops",
        "parameters":    ["pgw_worker_threads", "gtpu_fastpath_enabled", "dpi_offload"],
    },
    ("PGW", "Memory Utilization"): {
        "protocol":      "PGW session state, bearer state, IP pools, DPI session tables",
        "role":          "RAM footprint for active sessions and per-subscriber policy state",
        "measures":      "Used memory as percent of total",
        "failure_modes": ["Session state leak when GTP delete signalling is lost",
                          "DPI signature DB reload",
                          "Buffer accumulation under congestion"],
        "signals":       ["CPU Utilization", "Session Setup Success Rate", "Active Sessions vs Capacity"],
        "impact":        "Session creation rejections, OOM-kills of worker processes, partial PGW outage",
        "parameters":    ["session_idle_timeout", "max_buffered_packets_per_bearer", "dpi_table_size"],
    },

    # ─────────────── SGW ───────────────
    ("SGW", "Create Session Success Rate"): {
        "protocol":      "GTP-C v2 on S11 (MME ↔ SGW) and S5/S8 (SGW ↔ PGW)",
        "role":          "Bearer establishment for new EPS connections",
        "measures":      "Successful Create-Session responses on S11/S5",
        "failure_modes": ["S5 path to PGW failing",
                          "SGW capacity ceiling on bearers",
                          "GTP-C timer expiry under load",
                          "Diameter dependencies (where applicable)"],
        "signals":       ["PGW Session Setup Success Rate", "Bearer Count vs Capacity", "CPU Utilization"],
        "impact":        "Default bearer activation fails → user has signal but no data, ESM cause #26/#29",
        "parameters":    ["max_bearers", "gtp_c_tx_timer", "s5_retry_count"],
    },
    ("SGW", "Bearer Count vs Capacity"): {
        "protocol":      "Bearer table capacity counter",
        "role":          "Capacity headroom for active EPS bearers",
        "measures":      "Active default + dedicated bearers vs licensed/configured ceiling",
        "failure_modes": ["License/cap reached during peak hour",
                          "Idle-bearer timer not firing",
                          "Dedicated bearers piling up from VoLTE / video"],
        "signals":       ["CPU Utilization", "Memory Utilization", "Create Session Success Rate"],
        "impact":        "New bearer creates fail; existing bearers continue; new attaches blocked",
        "parameters":    ["max_bearers", "bearer_idle_timer", "dedicated_bearer_quota"],
    },
    ("SGW", "CPU Utilization"): {
        "protocol":      "SGW GTP-U forwarding plane and GTP-C control plane",
        "role":          "Compute capacity for tunnelled packet forwarding",
        "measures":      "Aggregate CPU usage on SGW workers",
        "failure_modes": ["Mass handover storms generating GTP-U path switching",
                          "Unbalanced load across SGW instances",
                          "Buffering during PGW slowness"],
        "signals":       ["User Plane Latency", "Online Charging Latency", "Session Setup Success Rate"],
        "impact":        "Higher latency on user plane, bearer setup slowdowns",
        "parameters":    ["sgw_worker_threads", "gtpu_fastpath", "load_balancer_weights"],
    },
    ("SGW", "Memory Utilization"): {
        "protocol":      "SGW bearer/session state, GTP-U buffers",
        "role":          "RAM footprint of bearer state and pending packets",
        "measures":      "Used memory as percent of total",
        "failure_modes": ["Leaked bearer state when delete signalling lost",
                          "Backed-up buffers when downstream PGW is slow"],
        "signals":       ["CPU Utilization", "Session Setup Success Rate"],
        "impact":        "OOM kills, partial SGW outage, dropped active sessions",
        "parameters":    ["bearer_idle_timer", "buffer_high_watermark"],
    },

    # ─────────────── PCRF ───────────────
    ("PCRF", "Policy Decision Success Rate"): {
        "protocol":      "Diameter Gx (PCRF ↔ PGW), Sd (PCRF ↔ TDF), Rx (PCRF ↔ AF/IMS)",
        "role":          "Policy-decision engine for QoS, charging and gating",
        "measures":      "Successful policy-decision responses / total requests",
        "failure_modes": ["UDR/SPR back-end slowness",
                          "Rule database hot-reload contention",
                          "Diameter peer flapping",
                          "Mis-classified subscriber profile"],
        "signals":       ["Gx Success Rate", "Session Establishment Success Rate", "Policy Enforcement Accuracy"],
        "impact":        "PGW falls back to default rules, dedicated bearer creation fails, VoLTE QoS broken",
        "parameters":    ["udr_pool_size", "rule_cache_ttl", "policy_eval_threads"],
    },
    ("PCRF", "Gx Success Rate"): {
        "protocol":      "Diameter Gx (CCR/CCA Initial / Update / Terminate)",
        "role":          "PCEF↔PCRF policy and charging signalling",
        "measures":      "Successful CCA responses / total CCR sent",
        "failure_modes": ["Diameter peer overload at PCRF",
                          "PGW Diameter client misconfiguration",
                          "CCR-T never being sent → session leak",
                          "Realm/identity mis-routing"],
        "signals":       ["Gx Response Latency", "Gx Timeout Rate", "Session Establishment Success Rate"],
        "impact":        "Default-bearer setup fails on PGW, DPI rules absent, charging gaps",
        "parameters":    ["gx_tx_timer", "gx_overload_threshold", "diameter_peer_window"],
    },
    ("PCRF", "Gx Response Latency"): {
        "protocol":      "Diameter Gx CCR-CCA round-trip",
        "role":          "Latency budget for policy decisions during session setup/update",
        "measures":      "Mean (or p95) Gx round-trip in ms",
        "failure_modes": ["Slow UDR/SPR queries",
                          "GC pauses on the Diameter stack",
                          "Network latency between PGW and PCRF",
                          "Rule-evaluation cost on complex policies"],
        "signals":       ["Gx Timeout Rate", "CPU Utilization", "Diameter TPS Usage"],
        "impact":        "Session setup slows, dedicated bearers fail, VoLTE call setup latency rises",
        "parameters":    ["gx_tx_timer", "udr_query_timeout", "rule_cache_ttl", "diameter_thread_pool"],
    },
    ("PCRF", "PCRF Availability"): {
        "protocol":      "Aggregate health (Gx, Rx, Sd, Sy, UDR access)",
        "role":          "End-to-end PCRF service availability",
        "measures":      "Percent of time PCRF answers all interfaces normally",
        "failure_modes": ["Geo-redundancy failover under partial fault",
                          "UDR/SPR back-end completely unavailable",
                          "All Diameter peers timing out",
                          "VM/container restarts"],
        "signals":       ["All PCRF KPIs", "Geo redundancy sync"],
        "impact":        "Network-wide policy outage; default rules only; VoLTE/ViLTE QoS lost; revenue-sensitive",
        "parameters":    ["geo_failover_threshold", "health_check_interval", "udr_election_timeout"],
    },
}


def _kpi_expert(component_type: str, kpi_name: str) -> dict | None:
    canonical = _resolve_kpi_name(component_type, kpi_name) or kpi_name
    return _KPI_EXPERT.get((component_type, canonical))


# ─────────────────────────────────────────────────────────────────────────────
# Effective threshold lookup — DB overrides win over the in-memory defaults.
# Cached for 5 seconds so the per-row classification doesn't hit the DB on
# every read; operator uploads can call _bust_threshold_cache() to clear it.
# ─────────────────────────────────────────────────────────────────────────────
_THR_CACHE: dict = {"data": None, "fetched_at": 0.0}
_THR_TTL_SEC = 5.0

def _bust_threshold_cache():
    _THR_CACHE["data"] = None
    _THR_CACHE["fetched_at"] = 0.0

def _load_db_thresholds() -> dict:
    """Snapshot the core_kpi_thresholds table into a dict
    {(component_type, kpi_name_canonical): cfg_dict}.
    Re-fetches at most once every _THR_TTL_SEC seconds."""
    import time as _t
    if _THR_CACHE["data"] is not None and (_t.time() - _THR_CACHE["fetched_at"]) < _THR_TTL_SEC:
        return _THR_CACHE["data"]
    out = {}
    try:
        rows = CoreKpiThreshold.query.all()
        for r in rows:
            ct_ = (r.component_type or "").upper().strip()
            canonical = _resolve_kpi_name(ct_, r.kpi_name) or r.kpi_name
            out[(ct_, canonical)] = {
                "direction": r.direction or "higher_is_better",
                "normal_low": r.normal_low,
                "normal_high": r.normal_high,
                "degrade_low": r.degrade_low,
                "degrade_high": r.degrade_high,
                "critical_low": r.critical_low,
                "critical_high": r.critical_high,
                "unit": (r.unit or "").strip(),
                "color": (r.color or "#64748b").strip(),
                "_source": "db",
            }
    except Exception as e:
        _LOG.warning("Threshold DB load failed (using defaults): %s", e)
    _THR_CACHE["data"] = out
    _THR_CACHE["fetched_at"] = _t.time()
    return out


def _effective_kpi_cfg(component_type: str, kpi_name: str) -> dict | None:
    """Return the effective threshold config for (component_type, kpi_name).
    DB-stored overrides win; falls back to KPI_RANGES defaults; finally None."""
    if not component_type or not kpi_name:
        return None
    ct_ = component_type.upper().strip()
    db_overrides = _load_db_thresholds()
    canonical = _resolve_kpi_name(ct_, kpi_name) or kpi_name
    if (ct_, canonical) in db_overrides:
        return db_overrides[(ct_, canonical)]
    if (ct_, kpi_name) in db_overrides:
        return db_overrides[(ct_, kpi_name)]
    return KPI_RANGES.get(ct_, {}).get(canonical) or KPI_RANGES.get(ct_, {}).get(kpi_name)

# Strip color per (component_type, kpi_name) — used by frontend
def get_kpi_color(component_type: str, kpi_name: str) -> str:
    return KPI_RANGES.get(component_type, {}).get(kpi_name, {}).get("color", "#64748b")


def _to_float(v):
    """Best-effort numeric coercion. Accepts int, float, Decimal, numeric str.
    Strips '%', commas, whitespace; returns None when conversion fails."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        s = str(v).strip().replace(",", "").rstrip("%").strip()
        return float(s) if s else None
    except (TypeError, ValueError):
        return None


def _scale_value(component_type: str, kpi_name: str, raw):
    """Auto-scale a stored KPI value to match the threshold catalogue.

    Many operators upload percentage-success-rate data in fractional form
    (e.g. 0.999 instead of 99.9). The threshold catalogue is in 0–100 percent,
    so a raw 0.999 looks like an outage when it really is 99.9% (normal).

    The decision is driven purely by the thresholds (NOT the stored unit
    string, which is unreliable across uploads):
      - If the catalogue's largest threshold is on a 0-100 scale (≥ 5), AND
      - The raw value is plausibly on a 0-1 scale (|val| < 1.5),
      → multiply by 100 so both sides line up at 0-100.
    Tiny ratio KPIs (e.g. Failure Rate with thresholds 0.05–0.30) keep their
    raw 0-1 representation because the threshold itself is < 5.
    """
    val = _to_float(raw)
    if val is None:
        return None
    cfg = _effective_kpi_cfg(component_type, kpi_name) or {}
    if not cfg:
        return val
    threshold = max(
        _to_float(cfg.get("normal_low")) or 0,
        _to_float(cfg.get("normal_high")) or 0,
        _to_float(cfg.get("critical_low")) or 0,
        _to_float(cfg.get("critical_high")) or 0,
    )
    if threshold >= 5 and abs(val) < 1.5:
        return val * 100.0
    return val


# ─────────────────────────────────────────────────────────────────────────────
# KPI-name resolution — tolerates spelling/spacing/punctuation drift in uploads
# ─────────────────────────────────────────────────────────────────────────────
import re as _re

def _norm_kpi(s: str) -> str:
    """Lowercase + strip everything that isn't a letter or digit so
    'CPU Utilization', 'cpu_utilization', 'CPU-Utilization (%)' all collapse
    to the same canonical key."""
    if not s:
        return ""
    return _re.sub(r"[^a-z0-9]", "", s.lower())


# Per-component synonym table — maps any uploaded KPI name (after normalisation)
# back to the canonical name we use in KPI_RANGES.
_KPI_SYNONYMS = {
    "MME": {
        "attachsuccessrate":         "Attach Success Rate",
        "attachsr":                  "Attach Success Rate",
        "servicerequestsuccessrate": "Service Request Success Rate",
        "servicerequestsr":          "Service Request Success Rate",
        "pagingsuccessrate":         "Paging Success Rate",
        "pagingsr":                  "Paging Success Rate",
        "csfbsuccessrate":           "CSFB Success Rate",
        "srvccsuccessrate":          "SRVCC Success Rate",
        "hosr":                      "HOSR",
        "handoversuccessrate":       "HOSR",
        "contextsetupfailurerate":   "Context Setup Failure Rate",
        "contextfailurerate":        "Context Setup Failure Rate",
        "cpuutilization":            "CPU Utilization",
        "cpuutil":                   "CPU Utilization",
        "memoryutilization":         "Memory Utilization",
        "memutil":                   "Memory Utilization",
        "memoryutil":                "Memory Utilization",
        "threadprocessqueuedepth":   "Thread Process Queue Depth",
        "threadqueuedepth":          "Thread Process Queue Depth",
        "queuedepth":                "Thread Process Queue Depth",
        "bearercount":               "Bearer Count",
        "tcpsessioncount":           "TCP Session Count",
        "sctpassociationstatus":     "SCTP Association Status",
        "sctpassoc":                 "SCTP Association Status",
        "sctpstatus":                "SCTP Association Status",
        "tausuccessrate":            "Service Request Success Rate",
        "nassignalingsuccessrate":   "Service Request Success Rate",
        "activesubscribercount":     "TCP Session Count",
    },
    "HSS": {
        "authenticationsuccessrate": "Authentication Success Rate",
        "authsuccessrate":           "Authentication Success Rate",
        "authsr":                    "Authentication Success Rate",
        "locationupdatesuccessrate": "Location Update Success Rate",
        "profileretrievalsuccessrate": "Profile Retrieval Success Rate",
        "authenticationfailurerate": "Authentication Failure Rate",
        "authfailurerate":           "Authentication Failure Rate",
        "s6atransactionsuccessrate": "S6a Transaction Success Rate",
        "s6asuccessrate":            "S6a Transaction Success Rate",
        "s6aresponselatency":        "S6a Response Latency",
        "s6alatency":                "S6a Response Latency",
        "s6aresponselatencyms":      "S6a Response Latency",
        "s6atimeoutrate":            "S6a Timeout Rate",
        "cxsuccessrate":             "Cx Success Rate",
        "cxresponselatency":         "Cx Response Latency",
        "cxresponselatencyms":       "Cx Response Latency",
        "dbquerysuccessrate":        "DB Query Success Rate",
        "dbreplicationlag":          "DB Replication Lag",
        "dbreplicationlagms":        "DB Replication Lag",
        "dblockcontentionrate":      "DB Lock Contention Rate",
        "cpuutilization":            "CPU Utilization",
        "memoryutilization":         "Memory Utilization",
        "diametertpsvscapacity":     "Diameter TPS vs Capacity",
        "diametertps":               "Diameter TPS vs Capacity",
        "threadworkerqueuedepth":    "Thread Worker Queue Depth",
        "hssavailability":           "HSS Availability",
        "availability":              "HSS Availability",
        "georedundancysyncstatus":   "Geo Redundancy Sync Status",
        "failovertime":              "Failover Time",
        "failovertimes":             "Failover Time",
    },
    "PGW": {
        "defaultbearersetupsuccessrate": "Default Bearer Setup Success Rate",
        "dedicatedbearersuccessrate":    "Dedicated Bearer Success Rate",
        "sessionsetupsuccessrate":       "Session Setup Success Rate",
        "setupsuccessrate":              "Session Setup Success Rate",
        "pcrfpcfinteractionsuccessrate": "PCRF PCF Interaction Success Rate",
        "packetlossdlul":                "Packet Loss DL UL",
        "packetloss":                    "Packet Loss DL UL",
        "userplanelatency":              "User Plane Latency",
        "userplanelatencyms":            "User Plane Latency",
        "throughputpersubscriber":       "Throughput Per Subscriber",
        "jitter":                        "Jitter",
        "jitterms":                      "Jitter",
        "chargingrecordsuccessrate":     "Charging Record Success Rate",
        "onlinecharginglatency":         "Online Charging Latency",
        "onlinecharginglatencyms":       "Online Charging Latency",
        "policyenforcementaccuracy":     "Policy Enforcement Accuracy",
        "cpuutilization":                "CPU Utilization",
        "memoryutilization":             "Memory Utilization",
        "activesessionsvscapacity":      "Active Sessions vs Capacity",
        "activesessionscapacity":        "Active Sessions vs Capacity",
        "sessionscapacity":              "Active Sessions vs Capacity",
        "nattableutilization":           "NAT Table Utilization",
    },
    "SGW": {
        "createsessionsuccessrate":      "Create Session Success Rate",
        "sessionsuccessrate":            "Create Session Success Rate",
        "modifybearersuccessrate":       "Modify Bearer Success Rate",
        "deletesessionsuccessrate":      "Delete Session Success Rate",
        "handoverdatapathsuccessrate":   "Handover Data Path Success Rate",
        "sgwrelocationsuccessrate":      "SGW Relocation Success Rate",
        "packetloss":                    "Packet Loss",
        "userplanelatency":              "User Plane Latency",
        "userplanelatencyms":            "User Plane Latency",
        "throughpututilization":         "Throughput Utilization",
        "cpuutilization":                "CPU Utilization",
        "memoryutilization":             "Memory Utilization",
        "bearercountvscapacity":         "Bearer Count vs Capacity",
        "bearercapacity":                "Bearer Count vs Capacity",
        "gtputunnelavailability":        "GTP-U Tunnel Availability",
        "gtputunnel":                    "GTP-U Tunnel Availability",
    },
    "PCRF": {
        "policydecisionsuccessrate":      "Policy Decision Success Rate",
        "policysuccessrate":              "Policy Decision Success Rate",
        "sessionestablishmentsuccessrate":"Session Establishment Success Rate",
        "chargingruleinstallsuccessrate": "Charging Rule Install Success Rate",
        "chargingruleupdatesuccessrate":  "Charging Rule Update Success Rate",
        "policyenforcementaccuracy":      "Policy Enforcement Accuracy",
        "gxsuccessrate":                  "Gx Success Rate",
        "gxresponselatency":              "Gx Response Latency",
        "gxlatency":                      "Gx Response Latency",
        "gxresponselatencyms":            "Gx Response Latency",
        "gxtimeoutrate":                  "Gx Timeout Rate",
        "ocssygysuccessrate":             "OCS Sy Gy Success Rate",
        "creditcontrolfailurerate":       "Credit Control Failure Rate",
        "activepolicysessions":           "Active Policy Sessions",
        "policysessionretentionrate":     "Policy Session Retention Rate",
        "dedicatedbearertriggeraccuracy": "Dedicated Bearer Trigger Accuracy",
        "cpuutilization":                 "CPU Utilization",
        "memoryutilization":              "Memory Utilization",
        "diametertpsusage":               "Diameter TPS Usage",
        "diametertps":                    "Diameter TPS Usage",
        "pcrfavailability":               "PCRF Availability",
        "availability":                   "PCRF Availability",
    },
}

# Build a reverse normalisation index of canonical names too — so an uploaded
# value that already matches the canonical name (just whitespace difference)
# resolves cleanly.
for _ct, _kpis in KPI_RANGES.items():
    for _canonical in _kpis.keys():
        _KPI_SYNONYMS.setdefault(_ct, {})[_norm_kpi(_canonical)] = _canonical


def _resolve_kpi_name(component_type: str, raw_kpi_name: str) -> str | None:
    """Return the canonical KPI name as used in KPI_RANGES, or None if there
    is no plausible match. Order: exact → normalised → keyword scoring."""
    if not raw_kpi_name:
        return None
    kpis = KPI_RANGES.get(component_type, {})
    if not kpis:
        return None
    if raw_kpi_name in kpis:
        return raw_kpi_name
    norm = _norm_kpi(raw_kpi_name)
    syn = _KPI_SYNONYMS.get(component_type, {})
    if norm in syn:
        return syn[norm]
    # Last resort: pick the canonical with the highest token overlap
    raw_tokens = {t for t in _re.split(r"[^a-z0-9]+", raw_kpi_name.lower()) if t}
    best, best_score = None, 0
    for canonical in kpis.keys():
        c_tokens = {t for t in _re.split(r"[^a-z0-9]+", canonical.lower()) if t}
        if not c_tokens:
            continue
        score = len(raw_tokens & c_tokens) / max(len(c_tokens), 1)
        if score > best_score and score >= 0.6:
            best, best_score = canonical, score
    return best


def _classify(component_type: str, kpi_name: str, avg):
    """Return ('normal'|'degradation'|'outage', range_dict_or_None)."""
    cfg = _effective_kpi_cfg(component_type, kpi_name)
    avgf = _to_float(avg)
    if not cfg or avgf is None:
        return "normal", cfg
    direction = cfg["direction"]
    if direction == "higher_is_better":
        crit = _to_float(cfg.get("critical_low"))
        norm = _to_float(cfg.get("normal_low"))
        if crit is not None and avgf < crit:
            return "outage", cfg
        if norm is not None and avgf < norm:
            return "degradation", cfg
        return "normal", cfg
    else:  # lower_is_better
        crit = _to_float(cfg.get("critical_high"))
        norm = _to_float(cfg.get("normal_high"))
        if crit is not None and avgf > crit:
            return "outage", cfg
        if norm is not None and avgf > norm:
            return "degradation", cfg
        return "normal", cfg


def _forecast_minutes(component_type: str, kpi_name: str, avg) -> int:
    """Rough linear-extrapolation forecast in minutes until the value is
    expected to cross the critical threshold. Falls back to 60 min."""
    cfg = _effective_kpi_cfg(component_type, kpi_name)
    avgf = _to_float(avg)
    if not cfg or avgf is None:
        return 60
    if cfg["direction"] == "higher_is_better":
        normal = _to_float(cfg.get("normal_low")) or 0.0
        crit = _to_float(cfg.get("critical_low")) or 0.0
        gap = max(normal - crit, 0.001)
        progress = max(min((normal - avgf) / gap, 1.0), 0.0)
    else:
        normal = _to_float(cfg.get("normal_high")) or 0.0
        crit = _to_float(cfg.get("critical_high")) or 0.0
        gap = max(crit - normal, 0.001)
        progress = max(min((avgf - normal) / gap, 1.0), 0.0)
    # 0% progressed = 4 hrs, 100% = 30 min
    minutes = int(240 - 210 * progress)
    return max(30, minutes)


# ─────────────────────────────────────────────────────────────────────────────
# Agent routing — pick a Core-domain agent
# ─────────────────────────────────────────────────────────────────────────────
_PRIORITY_SLA = {  # priority → SLA hours
    "Critical": 2.0,
    "High":     4.0,
    "Medium":   8.0,
    "Low":     16.0,
}


def _open_core_ticket_count(agent_id: int) -> int:
    """Open + in-progress CoreTickets for this agent. The hot-path counter
    that drives load-balanced routing — we count CoreTickets specifically
    so the customer-complaint load doesn't distort the math (and vice versa)."""
    try:
        return int(CoreTicket.query.filter(
            CoreTicket.assigned_agent == agent_id,
            CoreTicket.status.in_(("open", "in_progress")),
        ).count() or 0)
    except Exception:
        return 0


# Per-process round-robin cursor so that within a single job run, multiple
# tickets created back-to-back cycle through equally qualified agents instead
# of all landing on whoever scored 0.001 higher.
_RR_CURSOR: dict[str, int] = {}


def _pick_core_agent(component_type: str):
    """Pick a human_agent matched to the exact Core sub-component
    (MME / SGW / PGW / HSS / PCRF) with bandwidth-aware load balancing.

      Tier-1  domain == 'core' AND expertise == component_type
      Tier-2  domain == 'core' AND expertise is any other core sub
      Tier-3  domain == 'core' (no expertise set)
      Tier-4  expertise contains a generic CORE/EPC/5GC/IMS token
      Tier-5  any human_agent                                  (last-resort)

    Within the chosen tier the load is balanced two ways:
      1. We count this agent's *open CoreTickets* and prefer the least-loaded
         agent (bandwidth headroom). The customer-ticket counter is only used
         as a tie-breaker so the agent isn't oversubscribed across queues.
      2. When several agents are tied on load, a per-component round-robin
         cursor rotates among them so the next ticket lands on a different
         agent — preventing any single agent from snowballing.
    """
    try:
        from app import _open_ticket_count as _open_customer_ticket_count
    except Exception:
        def _open_customer_ticket_count(_): return 0

    comp = (component_type or "").upper()
    core_subs = {"MME", "SGW", "PGW", "HSS", "PCRF"}
    legacy_core_tokens = {"CORE", "EPC", "5GC", "IMS"}

    try:
        agents = User.query.filter(User.role == "human_agent").all()
    except Exception:
        return None
    if not agents:
        return None

    def _exp(a):
        return (a.expertise or "").strip().upper()

    def tier(a):
        is_core_domain = (a.domain or "").lower() == "core"
        exp = _exp(a)
        if is_core_domain and exp == comp:
            return 1
        if is_core_domain and exp in core_subs:
            return 2
        if is_core_domain:
            return 3
        if any(tok in exp for tok in (legacy_core_tokens | {comp})):
            return 4
        return 5

    def _cap(a):
        try:
            return int(a.bandwidth_capacity) if a.bandwidth_capacity is not None else 10
        except (TypeError, ValueError):
            return 10

    def _load(a):
        # Core tickets count toward bandwidth first, customer tickets second
        try:
            core_open = _open_core_ticket_count(a.id)
        except Exception:
            core_open = 0
        try:
            cust_open = int(_open_customer_ticket_count(a.id) or 0)
        except (TypeError, ValueError):
            cust_open = 0
        return core_open, cust_open

    # Sort agents primarily by tier (lower=better), then by core load (lower=
    # better), then by total load. Online flag breaks ties in favour of online.
    def sort_key(a):
        t = tier(a)
        core_open, cust_open = _load(a)
        cap = _cap(a)
        # Penalise overload heavily so saturated agents drop below the line
        overflow = max(0, core_open - cap)
        return (
            t,                          # smaller = better tier
            overflow,                   # overloaded → push down
            core_open,                  # least core-loaded first
            cust_open,                  # then least customer-loaded
            0 if a.is_online else 1,    # online preferred
        )

    ranked = sorted(agents, key=sort_key)
    if not ranked:
        return None

    best_tier = tier(ranked[0])
    same_tier = [a for a in ranked if tier(a) == best_tier]

    # Build a "least loaded" set. Among same-tier agents, take everyone whose
    # core_open == min core_open in this tier — these are tied on load. Then
    # round-robin pick among them.
    loads = [_open_core_ticket_count(a.id) for a in same_tier]
    min_load = min(loads)
    least_loaded = [a for a, ld in zip(same_tier, loads) if ld == min_load]

    # Drop overloaded agents (load >= cap). If everyone is overloaded, keep the
    # least-loaded set so we still assign someone (better than dropping the ticket).
    not_overloaded = [a for a in least_loaded if _open_core_ticket_count(a.id) < _cap(a)]
    pool = not_overloaded or least_loaded

    # Round-robin within pool, keyed by component_type so MME and SGW each have
    # their own rotation cursor.
    pool.sort(key=lambda a: a.id)
    rr_key = f"{best_tier}:{comp}"
    cursor = _RR_CURSOR.get(rr_key, 0)
    chosen = pool[cursor % len(pool)]
    _RR_CURSOR[rr_key] = (cursor + 1) % max(len(pool), 1)

    _LOG.info(
        "[CORE ROUTE] %s ticket → agent %s (tier=%d, load=%d/%d, online=%s, pool_size=%d)",
        comp, chosen.name or chosen.email, best_tier, min_load, _cap(chosen),
        chosen.is_online, len(pool),
    )
    return chosen


# ─────────────────────────────────────────────────────────────────────────────
# Hourly aggregation
# ─────────────────────────────────────────────────────────────────────────────
def _compute_hour_averages(target_date: _date, hour: int):
    """Group CoreComponentKpi 15-min rows for (date,hour) → avg per
    (component_type, component_id, kpi_name)."""
    rows = (
        db.session.query(
            CoreComponentKpi.component_type,
            CoreComponentKpi.component_id,
            CoreComponentKpi.kpi_name,
            db.func.avg(CoreComponentKpi.value).label("avg_val"),
            db.func.count(CoreComponentKpi.id).label("samples"),
        )
        .filter(
            CoreComponentKpi.date == target_date,
            CoreComponentKpi.hour == hour,
            CoreComponentKpi.value.isnot(None),
        )
        .group_by(
            CoreComponentKpi.component_type,
            CoreComponentKpi.component_id,
            CoreComponentKpi.kpi_name,
        )
        .all()
    )
    return rows


def _process_hour(target_date: _date, hour: int) -> tuple[int, int]:
    """Score every aggregated KPI for one hour. Create alerts/tickets as needed.
    Returns (alerts_created, tickets_created)."""
    alerts_created = 0
    tickets_created = 0

    # Idempotency: skip if already processed
    log = CoreJobLog.query.filter_by(hour_date=target_date, hour_value=hour).first()
    if log and log.finished_at:
        return 0, 0

    if not log:
        log = CoreJobLog(hour_date=target_date, hour_value=hour)
        db.session.add(log)
        db.session.flush()

    # Hour-end timestamp (the boundary the average represents)
    hour_end = datetime(target_date.year, target_date.month, target_date.day, hour, 0, 0) + timedelta(hours=1)

    rows = _compute_hour_averages(target_date, hour)
    # Skip any rows whose component_id is missing or "None"/"NaN" — these
    # are upload artifacts and don't represent real instances.
    def _good_id(s):
        s = (s or "").strip()
        return bool(s) and s.lower() not in ("none", "nan", "null")
    rows = [r for r in rows if _good_id(r.component_id)]

    # ── Verbose per-KPI scan log ──────────────────────────────────────────
    # Print one line per (component_type, component_id, monitored_kpi) so the
    # operator can confirm all 4 KPIs per component were inspected for each
    # instance during this hour. Even rows with no data are reported.
    print(f"\n[CORE SCAN] hour {target_date} {hour:02d}:00–{(hour+1):02d}:00  "
          f"({len(rows)} (component_id, kpi) groups with data)")
    by_inst: dict[tuple, dict] = {}
    for r in rows:
        ct_ = (r.component_type or "").upper()
        cid = r.component_id
        kpi_canonical = _resolve_kpi_name(ct_, r.kpi_name) or r.kpi_name
        by_inst[(ct_, cid, kpi_canonical)] = r
    # Group by (component_type, component_id) for tidy output
    inst_keys = sorted({(ct_, cid) for (ct_, cid, _k) in by_inst.keys()})
    for ct_, cid in inst_keys:
        monitored_kpis = _MONITORED_KPIS.get(ct_, [])
        if not monitored_kpis:
            continue
        print(f"  ─ {ct_}/{cid}")
        for mk in monitored_kpis:
            r = by_inst.get((ct_, cid, mk))
            if r is None:
                # Try fuzzy match on raw key-set
                for (kct, kcid, kname), rv in by_inst.items():
                    if kct == ct_ and kcid == cid and _resolve_kpi_name(ct_, kname) == mk:
                        r = rv
                        break
            if r is None:
                print(f"      · {mk:32s} no-data")
                continue
            try:
                raw_avg = float(r.avg_val) if r.avg_val is not None else None
            except (TypeError, ValueError):
                raw_avg = None
            scaled = _scale_value(ct_, mk, raw_avg) if raw_avg is not None else None
            cat, _cfg = _classify(ct_, mk, scaled) if scaled is not None else ("normal", None)
            cfg = _effective_kpi_cfg(ct_, mk) or {}
            unit = cfg.get("unit", "")
            print(f"      · {mk:32s} avg={(scaled if scaled is not None else raw_avg):>9.4f}{unit:<5s}  band={cat}")

    for r in rows:
        try:
            ctype = r.component_type
            cid = r.component_id
            raw_kpi = r.kpi_name
            # Resolve uploaded KPI name to its canonical form (handles drift
            # like "CPU_Utilization" vs "CPU Utilization").
            kpi = _resolve_kpi_name(ctype, raw_kpi) or raw_kpi
            try:
                avg_raw = float(r.avg_val) if r.avg_val is not None else None
            except (TypeError, ValueError):
                _LOG.warning("[CORE JOB] %s/%s/%s @ %s:%s — non-numeric avg_val %r, skipping",
                             ctype, cid, kpi, target_date, hour, r.avg_val)
                continue
            if avg_raw is None:
                continue
            # Auto-scale fractional percentages so 0.999 → 99.9 etc.
            avg = _scale_value(ctype, kpi, avg_raw)
            if avg is None:
                continue
            # Skip ticket/alert creation for KPIs the operator has not asked
            # us to monitor. Their data is still queryable for trend display
            # and RCA context, but no alerts/tickets fire.
            if not _is_monitored(ctype, kpi):
                continue
            category, cfg = _classify(ctype, kpi, avg)
            if category == "normal" or not cfg:
                continue

            # Skip duplicates for this exact hour
            existing_alert = CoreAlert.query.filter_by(
                component_type=ctype, component_id=cid, kpi_name=kpi,
                hour_date=target_date, hour_value=hour
            ).first()
            existing_ticket = CoreTicket.query.filter_by(
                component_type=ctype, component_id=cid, kpi_name=kpi,
                hour_date=target_date, hour_value=hour
            ).first()

            agent = _pick_core_agent(ctype)
            agent_id = agent.id if agent else None

            if category == "degradation" and not existing_alert and not existing_ticket:
                mins = _forecast_minutes(ctype, kpi, avg)
                msg = (f"{kpi} on {cid} averaged {round(avg, 3)}{cfg.get('unit','')}, "
                       f"in degradation band — projected to cross critical in ~{mins} min if uncorrected.")
                alert = CoreAlert(
                    component_type=ctype, component_id=cid, kpi_name=kpi,
                    hour_date=target_date, hour_value=hour, avg_value=avg,
                    threshold_low=_to_float(cfg.get("normal_low") or cfg.get("normal_high")),
                    threshold_high=_to_float(cfg.get("critical_low") or cfg.get("critical_high")),
                    severity="degradation",
                    forecast_minutes=mins,
                    forecast_message=msg,
                    assigned_agent=agent_id,
                )
                db.session.add(alert)
                alerts_created += 1
                ag_name = agent.name if agent else "Unassigned"
                ag_email = agent.email if agent else "—"
                print(f"  [HOUR {target_date} {hour:02d}:00] ALERT CREATED  "
                      f"component={ctype}/{cid}  kpi='{kpi}'  "
                      f"avg={avg:.3f}{cfg.get('unit','')}  forecast={mins}min  "
                      f"-> agent={ag_name} ({ag_email})")
                _LOG.info("[CORE ALERT] %s/%s/%s avg=%.3f — agent=%s", ctype, cid, kpi, avg, ag_name)

            elif category == "outage":
                # Decide priority by how far past critical we are. Cast both
                # sides to float so we never trip on Decimal/str types.
                crit_low = _to_float(cfg.get("critical_low"))
                crit_high = _to_float(cfg.get("critical_high"))
                if cfg["direction"] == "higher_is_better":
                    gap = (crit_low if crit_low is not None else 0.0) - avg
                else:
                    gap = avg - (crit_high if crit_high is not None else 0.0)
                if gap > 20:
                    priority = "Critical"
                elif gap > 5:
                    priority = "High"
                else:
                    priority = "Medium"
                sla_hours = float(_PRIORITY_SLA[priority])

                real_now = datetime.now(timezone.utc).replace(tzinfo=None)

                # ── DEDUP: an open/in-progress ticket for the same
                # (component_type, component_id, kpi_name) created or updated
                # within the last 24 hours is REUSED rather than re-created.
                # This way recurring outages on the same node within a day
                # roll up onto a single ticket with the latest hour/avg.
                window_start = real_now - timedelta(hours=24)
                open_existing = (CoreTicket.query
                    .filter(
                        CoreTicket.component_type == ctype,
                        CoreTicket.component_id == cid,
                        CoreTicket.kpi_name == kpi,
                        CoreTicket.status.in_(("open", "in_progress")),
                        CoreTicket.updated_at >= window_start,
                    )
                    .order_by(CoreTicket.updated_at.desc())
                    .first())

                ag_name = agent.name if agent else "Unassigned"
                ag_email = agent.email if agent else "—"
                u = cfg.get("unit", "")
                if open_existing:
                    # Update the existing ticket — refresh hour, avg and bump
                    # SLA only if the new hour is more severe.
                    prev_hour_str = f"{open_existing.hour_date} {open_existing.hour_value:02d}:00"
                    new_hour_str = f"{target_date} {hour:02d}:00"
                    open_existing.hour_date = target_date
                    open_existing.hour_value = hour
                    open_existing.avg_value = avg
                    open_existing.threshold_value = _to_float(cfg.get("critical_low") or cfg.get("critical_high"))
                    open_existing.timestamp_at = hour_end
                    open_existing.updated_at = real_now
                    rank = {"Critical": 4, "High": 3, "Medium": 2, "Low": 1}
                    if rank[priority] > rank.get(open_existing.priority, 1):
                        open_existing.priority = priority
                        open_existing.sla_hours = sla_hours
                        open_existing.sla_deadline = real_now + timedelta(hours=sla_hours)
                    print(f"  [HOUR {target_date} {hour:02d}:00] TICKET UPDATED  "
                          f"ref={open_existing.reference_number}  "
                          f"component={ctype}/{cid}  kpi='{kpi}'  "
                          f"avg={avg:.3f}{u}  priority={open_existing.priority}  "
                          f"prev_hour={prev_hour_str}  -> agent={ag_name} ({ag_email})")
                    _LOG.info("[CORE TICKET UPDATED] %s — %s/%s/%s prev_hour=%s new_hour=%s avg=%.3f agent=%s",
                              open_existing.reference_number, ctype, cid, kpi,
                              prev_hour_str, new_hour_str, avg, ag_name)
                else:
                    ref = f"CORE-{ctype}-{target_date.strftime('%Y%m%d')}-{hour:02d}-{cid}-{abs(hash(kpi)) % 10000:04d}"
                    ticket = CoreTicket(
                        reference_number=ref,
                        component_type=ctype, component_id=cid, kpi_name=kpi,
                        hour_date=target_date, hour_value=hour, avg_value=avg,
                        threshold_value=_to_float(cfg.get("critical_low") or cfg.get("critical_high")),
                        priority=priority, sla_hours=sla_hours,
                        sla_deadline=real_now + timedelta(hours=sla_hours),
                        status="open",
                        assigned_agent=agent_id,
                        timestamp_at=hour_end,
                        created_at=real_now,
                        updated_at=real_now,
                    )
                    db.session.add(ticket)
                    tickets_created += 1
                    print(f"  [HOUR {target_date} {hour:02d}:00] TICKET CREATED  "
                          f"ref={ref}  component={ctype}/{cid}  kpi='{kpi}'  "
                          f"avg={avg:.3f}{u}  priority={priority}  SLA={sla_hours}h  "
                          f"-> agent={ag_name} ({ag_email})")
                    _LOG.info("[CORE TICKET CREATED] %s/%s/%s avg=%.3f priority=%s — agent=%s",
                              ctype, cid, kpi, avg, priority, ag_name)

                # Promote any existing open alert for this hour to acknowledged so it
                # doesn't double-display alongside the ticket.
                if existing_alert and not existing_alert.acknowledged:
                    existing_alert.acknowledged = True
                    existing_alert.acknowledged_at = datetime.now(timezone.utc)
        except Exception as row_err:
            _LOG.warning("[CORE JOB] %s/%s/%s @ %s:%s — row failed (%s): %s",
                         getattr(r, "component_type", "?"), getattr(r, "component_id", "?"),
                         getattr(r, "kpi_name", "?"), target_date, hour, type(row_err).__name__, row_err)
            # If a SQL error left the session in a bad state, recover; otherwise
            # keep partial progress (alerts/tickets added by previous rows).
            try:
                db.session.flush()
            except Exception:
                db.session.rollback()
                log = CoreJobLog.query.filter_by(hour_date=target_date, hour_value=hour).first()
                if not log:
                    log = CoreJobLog(hour_date=target_date, hour_value=hour)
                    db.session.add(log)
                    db.session.flush()
            continue

    log.alerts_created = alerts_created
    log.tickets_created = tickets_created
    log.finished_at = datetime.now(timezone.utc)
    db.session.commit()
    # Hour-end summary
    print(f"  [HOUR {target_date} {hour:02d}:00] SUMMARY  "
          f"new_alerts={alerts_created}  new_tickets={tickets_created}")
    return alerts_created, tickets_created


CATCHUP_DEFAULT_DAYS = 30  # how far back to scan when there's no log yet


def _data_date_range():
    """Return (min_date, max_date) of timestamps available in CoreComponentKpi.
    Used so the catch-up doesn't blindly scan an arbitrary 24h window when the
    uploaded data lives on completely different days."""
    try:
        row = (db.session.query(
                    db.func.min(CoreComponentKpi.date),
                    db.func.max(CoreComponentKpi.date),
                    db.func.count(CoreComponentKpi.id),
                ).first())
        if row and row[0] and row[1]:
            return row[0], row[1], int(row[2] or 0)
    except Exception:
        pass
    return None, None, 0


def _hours_to_process(now: datetime, last_processed: tuple[_date, int] | None,
                      data_min: _date | None = None, data_max: _date | None = None):
    """Build a list of (date, hour) tuples to process.

    Hard rule: **never process an hour whose end is later than wall-clock now.**
    The job is meant to operate on hours that have *already happened* in real
    time. Future-dated demo data sitting in the DB is ignored until its date
    actually arrives.

    Upper bound:
      - `wall_clock_last_completed_hour` = current_hour - 1, or current_hour
        when the wall clock has reached MM:55 of that hour (so the four 15-min
        intervals are all in).
      - Capped further by `data_max` if no data exists past today (i.e. we
        won't iterate hours where there's nothing to read anyway).

    Lower bound:
      - If `last_processed` is known, cursor starts at `last_processed + 1h`.
      - Otherwise starts at `data_min 00:00`, but never earlier than
        `last_complete_dt - CATCHUP_DEFAULT_DAYS` so a fresh DB doesn't
        catch-up months of history.
    """
    cur_hour = now.hour
    cur_date = now.date()

    # Wall-clock last-completed hour (the only legitimate ceiling).
    include_current = now.minute >= 55
    last_complete_dt = datetime(cur_date.year, cur_date.month, cur_date.day, cur_hour, 0, 0)
    if not include_current:
        last_complete_dt -= timedelta(hours=1)

    # If data ends before now, there's nothing to read past data_max.
    if data_max:
        data_end = datetime(data_max.year, data_max.month, data_max.day, 23, 0, 0)
        if data_end < last_complete_dt:
            last_complete_dt = data_end

    if last_processed:
        start_dt = datetime(last_processed[0].year, last_processed[0].month, last_processed[0].day,
                            last_processed[1], 0, 0) + timedelta(hours=1)
    elif data_min:
        start_dt = datetime(data_min.year, data_min.month, data_min.day, 0, 0, 0)
        floor_dt = last_complete_dt - timedelta(days=CATCHUP_DEFAULT_DAYS)
        if start_dt < floor_dt:
            start_dt = floor_dt
    else:
        start_dt = last_complete_dt - timedelta(days=CATCHUP_DEFAULT_DAYS)

    out = []
    cursor = start_dt
    while cursor <= last_complete_dt:
        out.append((cursor.date(), cursor.hour))
        cursor += timedelta(hours=1)
    return out


def delete_future_tickets() -> dict:
    """Wipe tickets / alerts / job-log entries that live in the future
    (their hour-end is past wall-clock now). The job must only react to
    hours that have actually happened in real time.
    """
    now = datetime.now()
    print("=" * 70)
    print(f"[CORE FUTURE CLEAN] Removing tickets/alerts/log entries beyond {now}")
    print("=" * 70)
    # Tickets: timestamp_at strictly after now → future
    fut_tickets = (CoreTicket.query
                   .filter(CoreTicket.timestamp_at > now)
                   .all())
    fut_ticket_ids = [t.id for t in fut_tickets]
    for t in fut_tickets:
        print(f"  [DELETE TICKET] {t.reference_number}  "
              f"{t.component_type}/{t.component_id}/{t.kpi_name}  "
              f"timestamp={t.timestamp_at}")
    if fut_ticket_ids:
        try:
            CoreParameterChange.query.filter(
                CoreParameterChange.core_ticket_id.in_(fut_ticket_ids)
            ).delete(synchronize_session=False)
        except Exception:
            db.session.rollback()
        CoreTicket.query.filter(CoreTicket.id.in_(fut_ticket_ids)).delete(synchronize_session=False)
    # Alerts whose hour-end is in the future
    today = now.date()
    cur_hour = now.hour
    fut_alerts = (CoreAlert.query
                  .filter(db.or_(
                      CoreAlert.hour_date > today,
                      db.and_(CoreAlert.hour_date == today, CoreAlert.hour_value >= cur_hour),
                  ))
                  .all())
    fut_alert_ids = [a.id for a in fut_alerts]
    for a in fut_alerts:
        print(f"  [DELETE ALERT ] id={a.id}  "
              f"{a.component_type}/{a.component_id}/{a.kpi_name}  "
              f"{a.hour_date} {a.hour_value:02d}:00")
    if fut_alert_ids:
        CoreAlert.query.filter(CoreAlert.id.in_(fut_alert_ids)).delete(synchronize_session=False)
    # Job log entries beyond now
    fut_logs = (CoreJobLog.query
                .filter(db.or_(
                    CoreJobLog.hour_date > today,
                    db.and_(CoreJobLog.hour_date == today, CoreJobLog.hour_value >= cur_hour),
                ))
                .all())
    fut_log_ids = [l.id for l in fut_logs]
    if fut_log_ids:
        CoreJobLog.query.filter(CoreJobLog.id.in_(fut_log_ids)).delete(synchronize_session=False)
    db.session.commit()
    print(f"[CORE FUTURE CLEAN] Deleted {len(fut_ticket_ids)} tickets, "
          f"{len(fut_alert_ids)} alerts, {len(fut_log_ids)} job-log entries")
    return {
        "tickets_deleted": len(fut_ticket_ids),
        "alerts_deleted": len(fut_alert_ids),
        "log_entries_deleted": len(fut_log_ids),
    }


def run_todays_job(force: bool = True) -> dict:
    """Process the COMPLETED hours of TODAY (wall-clock).
    A "completed" hour is any hour whose end is at or before the current
    wall-clock minute. The current hour is included only after MM:55 of
    that hour.
    """
    now = datetime.now()
    today = now.date()
    print("=" * 70)
    print(f"[CORE TODAY] Running today's job for {today} (now={now.strftime('%H:%M:%S')}, force={force})")
    print("=" * 70)
    a, t = run_core_hourly_job(from_date=today, to_date=today, force=force)
    print(f"[CORE TODAY] Done — {a} alerts, {t} tickets created/updated for {today}")
    return {"date": today.isoformat(), "alerts_created": a, "tickets_created": t, "force": force}


def rebuild_tickets_from_data() -> dict:
    """End-to-end rebuild: wipe core_job_log within the data window, then
    force-replay every hour against the CURRENT thresholds and monitored set.
    Used after threshold uploads / reconciliation when the operator wants
    tickets recreated to match the new bands.
    """
    data_min, data_max, data_count = _data_date_range()
    print("=" * 70)
    print(f"[CORE REBUILD] Wiping job log + replaying {data_count} rows "
          f"({data_min} → {data_max}) under current thresholds")
    print("=" * 70)
    if not data_min or not data_max:
        print("[CORE REBUILD] No data — nothing to replay.")
        return {"alerts_created": 0, "tickets_created": 0, "skipped": "no_data"}

    # Wipe job log so every hour is re-processed
    try:
        deleted = (CoreJobLog.query
                   .filter(CoreJobLog.hour_date >= data_min,
                           CoreJobLog.hour_date <= data_max)
                   .delete(synchronize_session=False))
        db.session.commit()
        print(f"[CORE REBUILD] Cleared {deleted} core_job_log entries")
    except Exception as e:
        db.session.rollback()
        _LOG.warning("[CORE REBUILD] log wipe failed: %s", e)

    a, t = run_core_hourly_job(from_date=data_min, to_date=data_max, force=True)
    print(f"[CORE REBUILD] Done — {a} alerts, {t} tickets created")
    return {
        "alerts_created": a, "tickets_created": t,
        "from_date": data_min.isoformat() if data_min else None,
        "to_date": data_max.isoformat() if data_max else None,
    }


def reconcile_open_tickets(dry_run: bool = False) -> dict:
    """Re-evaluate every open/in_progress CoreTicket against the CURRENT
    threshold catalogue (DB overrides + in-memory defaults) and the current
    `_MONITORED_KPIS` gate. A ticket is **deleted** when:

      - Its KPI is no longer in `_MONITORED_KPIS` for that component, OR
      - Re-classifying its stored avg_value yields anything other than "outage".

    Open alerts that no longer classify as "degradation" or "outage" are
    likewise deleted. All decisions are logged to the terminal.
    """
    print("=" * 70)
    print("[CORE RECONCILE] Re-evaluating open tickets/alerts vs current thresholds")
    print("=" * 70)
    deleted_tickets, kept_tickets = 0, 0
    deleted_alerts, kept_alerts = 0, 0
    decisions = []

    open_tickets = (CoreTicket.query
                    .filter(CoreTicket.status.in_(("open", "in_progress")))
                    .all())
    for t in open_tickets:
        ctype = (t.component_type or "").upper()
        kpi = t.kpi_name
        cid = (t.component_id or "").strip()
        # Drop bogus rows whose component_id is missing/None/NaN — these came
        # from earlier uploads that didn't carry a valid instance id.
        bogus_id = (not cid) or cid.lower() in ("none", "nan", "null")
        scaled = _scale_value(ctype, kpi, t.avg_value)
        category, cfg = _classify(ctype, kpi, scaled if scaled is not None else t.avg_value)
        is_monitored = _is_monitored(ctype, kpi)
        keep = (not bogus_id) and is_monitored and category == "outage"
        # Pretty avg display: show scaled value with appropriate precision
        if scaled is None:
            avg_disp = "—"
        elif abs(scaled) >= 100:
            avg_disp = f"{scaled:.4f}"
        elif abs(scaled) >= 1:
            avg_disp = f"{scaled:.4f}"
        else:
            avg_disp = f"{scaled:.6f}"
        unit = (cfg.get("unit", "") if cfg else "")
        bogus_tag = "  bogus_id=True" if bogus_id else ""
        line = (f"  [{'KEEP  ' if keep else 'DELETE'}] {t.reference_number}  "
                f"{ctype}/{t.component_id}/{kpi}  avg={avg_disp}{unit}  "
                f"band={category}  monitored={is_monitored}{bogus_tag}")
        print(line)
        decisions.append({
            "reference_number": t.reference_number,
            "component_type": ctype, "component_id": t.component_id,
            "kpi_name": kpi, "avg": scaled, "band": category,
            "monitored": is_monitored, "kept": keep,
        })
        if keep:
            kept_tickets += 1
        else:
            deleted_tickets += 1
            if not dry_run:
                # Also remove any related parameter-change requests so they
                # don't dangle.
                try:
                    CoreParameterChange.query.filter_by(core_ticket_id=t.id).delete()
                except Exception:
                    db.session.rollback()
                db.session.delete(t)

    open_alerts = (CoreAlert.query
                   .filter(CoreAlert.acknowledged == False)  # noqa: E712
                   .all())
    for a in open_alerts:
        ctype = (a.component_type or "").upper()
        kpi = a.kpi_name
        cid = (a.component_id or "").strip()
        bogus_id = (not cid) or cid.lower() in ("none", "nan", "null")
        scaled = _scale_value(ctype, kpi, a.avg_value)
        category, cfg = _classify(ctype, kpi, scaled if scaled is not None else a.avg_value)
        is_monitored = _is_monitored(ctype, kpi)
        keep = (not bogus_id) and is_monitored and category in ("degradation", "outage")
        if keep:
            kept_alerts += 1
        else:
            deleted_alerts += 1
            if not dry_run:
                db.session.delete(a)

    if not dry_run:
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            _LOG.error("[CORE RECONCILE] commit failed: %s", e)
            return {"error": str(e)}

    print(f"[CORE RECONCILE] Tickets: kept={kept_tickets}  deleted={deleted_tickets}")
    print(f"[CORE RECONCILE] Alerts:  kept={kept_alerts}  deleted={deleted_alerts}")
    print("=" * 70)
    _LOG.info("[CORE RECONCILE] tickets kept=%d deleted=%d; alerts kept=%d deleted=%d",
              kept_tickets, deleted_tickets, kept_alerts, deleted_alerts)
    return {
        "tickets_kept": kept_tickets, "tickets_deleted": deleted_tickets,
        "alerts_kept": kept_alerts, "alerts_deleted": deleted_alerts,
        "dry_run": dry_run, "decisions": decisions,
    }


def run_core_hourly_job(now: datetime | None = None,
                        from_date: _date | None = None,
                        to_date: _date | None = None,
                        force: bool = False) -> tuple[int, int]:
    """Catch-up runner.

    Behaviour:
      - **Default (no params):** processes from `last log entry` to whichever
        comes first — the most recent wall-clock completed hour, or `data_max`
        when data extends into the future. This means future-dated test data
        IS processed (so the demo flow works).
      - **`from_date` / `to_date`:** explicit backfill. Hours within that
        range are processed regardless of wall-clock — admin knows what they
        want. Already-finished hours are skipped unless `force=True`.
      - **`force=True`:** ignores finished CoreJobLog entries so you can
        re-process hours after fixing thresholds, KPI names, or data.
    """
    now = now or datetime.now()
    data_min, data_max, data_count = _data_date_range()
    _LOG.info("[CORE JOB] Data coverage: %s rows · %s → %s", data_count, data_min, data_max)

    if from_date or to_date:
        f = from_date or data_min or now.date()
        t = to_date or data_max or now.date()
        start_dt = datetime(f.year, f.month, f.day, 0, 0, 0)
        end_dt = datetime(t.year, t.month, t.day, 23, 0, 0)
        # HARD CAP: never go past the current wall-clock completed hour.
        wall_dt = datetime(now.year, now.month, now.day, now.hour, 0, 0)
        if now.minute < 55:
            wall_dt -= timedelta(hours=1)
        if end_dt > wall_dt:
            end_dt = wall_dt
        hours = []
        cursor = start_dt
        while cursor <= end_dt:
            hours.append((cursor.date(), cursor.hour))
            cursor += timedelta(hours=1)
        _LOG.info("[CORE JOB] Manual range %s → %s (capped at wall-clock %s) — %d hours total (force=%s)",
                  f, t, wall_dt.strftime('%Y-%m-%d %H:00'), len(hours), force)
    else:
        # CRITICAL: only consider log entries whose hour is at or before
        # wall-clock now. A stray future-dated entry (e.g. left over from a
        # previous wide-range rebuild) would otherwise pin `last_processed`
        # past the wall clock and the catch-up would queue 0 hours forever.
        cur_h = now.hour - (0 if now.minute >= 55 else 1)
        cur_d = now.date()
        last = (CoreJobLog.query
                .filter(CoreJobLog.finished_at.isnot(None))
                .filter(db.or_(
                    CoreJobLog.hour_date < cur_d,
                    db.and_(CoreJobLog.hour_date == cur_d, CoreJobLog.hour_value <= cur_h),
                ))
                .order_by(CoreJobLog.hour_date.desc(), CoreJobLog.hour_value.desc())
                .first())
        last_tuple = (last.hour_date, last.hour_value) if last else None
        hours = _hours_to_process(now, last_tuple, data_min, data_max)
        _LOG.info("[CORE JOB] Auto catch-up: last_processed=%s (wall-clock cap %s %02d:00) — %d hours queued",
                  last_tuple, cur_d, cur_h, len(hours))
        print(f"[CORE JOB] Auto catch-up: last_processed={last_tuple}  "
              f"wall_clock_cap={cur_d} {cur_h:02d}:00  hours_queued={len(hours)}")

    if not force and hours:
        # Drop hours already finished
        done = {(l.hour_date, l.hour_value) for l in
                CoreJobLog.query.filter(CoreJobLog.finished_at.isnot(None)).all()}
        before = len(hours)
        hours = [h for h in hours if h not in done]
        if before != len(hours):
            _LOG.info("[CORE JOB] %d hours already finished — skipped (use force=true to re-run)",
                      before - len(hours))

    if not hours:
        _LOG.info("[CORE JOB] Nothing to process.")
        return 0, 0

    if force:
        # Wipe finished flags so _process_hour will re-run for these hours
        keys = [(d, h) for d, h in hours]
        for d, h in keys:
            CoreJobLog.query.filter_by(hour_date=d, hour_value=h).delete()
        db.session.commit()

    total_alerts = 0
    total_tickets = 0
    processed = 0
    for d, h in hours:
        try:
            a, t = _process_hour(d, h)
            total_alerts += a
            total_tickets += t
            processed += 1
        except Exception as e:
            _LOG.error("[CORE JOB] Failed for %s hour %s: %s", d, h, e)
            db.session.rollback()
    _LOG.info("[CORE JOB] Done — %d/%d hours processed, %d alerts, %d tickets",
              processed, len(hours), total_alerts, total_tickets)
    return total_alerts, total_tickets


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler — runs every 60 seconds, fires the job at MM:55 of each hour and
# also on startup to back-fill any missed hours.
# ─────────────────────────────────────────────────────────────────────────────
def schedule_core_hourly_job(app):
    import threading, time

    def _loop():
        time.sleep(10)
        last_run_hour_key = None    # (date, hour)
        last_heartbeat_minute = -1  # so the heartbeat prints at most once per minute change
        with app.app_context():
            try:
                # Step 1: delete any tickets/alerts/log entries that live in
                # the future (left over from earlier "process all data" runs).
                try:
                    delete_future_tickets()
                except Exception as _de:
                    _LOG.warning("[CORE STARTUP] future cleanup failed: %s", _de)
                # Step 2: reconcile remaining open tickets vs current thresholds.
                try:
                    reconcile_open_tickets(dry_run=False)
                except Exception as _re:
                    _LOG.warning("[CORE STARTUP] reconcile failed: %s", _re)
                print("=" * 70)
                print("[CORE JOB STARTUP] catch-up running (only past hours)…")
                print("=" * 70)
                _LOG.info("[CORE JOB STARTUP] catch-up running…")
                a, t = run_core_hourly_job()
                print(f"[CORE JOB STARTUP] done — {a} alerts, {t} tickets")
                _LOG.info("[CORE JOB STARTUP] done — %d alerts, %d tickets", a, t)
                # Step 3: force-replay TODAY's already-completed hours so the
                # dashboard reflects the current threshold catalogue.
                try:
                    today_result = run_todays_job(force=True)
                    print(f"[CORE TODAY STARTUP] {today_result['date']} — "
                          f"{today_result['alerts_created']} alerts, "
                          f"{today_result['tickets_created']} tickets")
                except Exception as _te:
                    _LOG.warning("[CORE TODAY STARTUP] failed: %s", _te)
            except Exception as e:
                _LOG.error("[CORE JOB STARTUP] failed: %s", e)
                import traceback as _tb; _tb.print_exc()

        while True:
            try:
                now = datetime.now()
                key = (now.date(), now.hour)

                # Heartbeat once per minute when minute >= 50, so the operator
                # can see the scheduler is alive in the run-up to the firing.
                if now.minute >= 50 and now.minute != last_heartbeat_minute:
                    mins_to_fire = max(0, 55 - now.minute)
                    print(f"[CORE JOB] heartbeat {now.strftime('%H:%M:%S')} — "
                          f"firing in ~{mins_to_fire} min (next run will cover hour {now.hour:02d}:00)")
                    last_heartbeat_minute = now.minute

                # Fire at minute 55..59 once per hour
                if now.minute >= 55 and last_run_hour_key != key:
                    print("=" * 70)
                    print(f"[CORE JOB] HOURLY RUN starting at {now.strftime('%Y-%m-%d %H:%M:%S')} "
                          f"(processing hour {now.hour:02d}:00 + any missed hours)")
                    print("=" * 70)
                    _LOG.info("[CORE JOB] hourly run @ %s", now.isoformat())
                    with app.app_context():
                        # Sweep any stale future tickets/alerts before running
                        try:
                            delete_future_tickets()
                        except Exception as _de:
                            _LOG.warning("[CORE JOB] future cleanup failed: %s", _de)
                        a, t = run_core_hourly_job(now)
                        print(f"[CORE JOB] HOURLY RUN done — {a} alerts, {t} tickets created.")
                        _LOG.info("[CORE JOB] hour %s — %d alerts, %d tickets", key, a, t)
                    last_run_hour_key = key
                    last_heartbeat_minute = -1
            except Exception as e:
                _LOG.error("[CORE JOB] scheduler error: %s", e)
                import traceback as _tb; _tb.print_exc()
            time.sleep(30)

    threading.Thread(target=_loop, daemon=True).start()
    msg = "Core hourly scheduler started — heartbeat at MM:50–59, fires at MM:55 every hour"
    print(">>> " + msg)
    _LOG.info(msg)


# ─────────────────────────────────────────────────────────────────────────────
# OpenAI helpers
# ─────────────────────────────────────────────────────────────────────────────
def _strip_md(text: str) -> str:
    if not text:
        return ""
    import re
    t = text.replace("**", "").replace("***", "")
    t = re.sub(r"^#{1,6}\s*", "", t, flags=re.MULTILINE)
    t = re.sub(r"^[-─━═]{3,}$", "", t, flags=re.MULTILINE)
    t = re.sub(r"^[•●◦▪]\s*", "", t, flags=re.MULTILINE)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _llm(prompt: str, system: str = "You are a senior 4G/5G core network engineer.",
         max_tokens: int = 1200, strip_markdown: bool = True) -> str:
    if not _LLM_CLIENT:
        return "AI service is not configured."
    try:
        resp = _LLM_CLIENT.chat.completions.create(
            model=_LLM_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=max_tokens,
        )
        content = resp.choices[0].message.content or ""
        return _strip_md(content) if strip_markdown else content
    except Exception as e:
        _LOG.warning("LLM call failed: %s", e)
        return f"AI generation failed: {e}"


def _extract_json_object(text: str) -> dict | None:
    """Robust JSON-object extractor for LLM responses.
    Handles: bare JSON, ```json fenced blocks, leading/trailing prose, and
    accidental nested braces by walking the string and balancing braces."""
    if not text:
        return None
    s = text.strip()
    # Strip code fences
    if s.startswith("```"):
        s = s.strip("`")
        if s.lower().startswith("json"):
            s = s[4:].lstrip()
        if s.endswith("```"):
            s = s[:-3].rstrip()
    # Try direct parse first
    try:
        v = json.loads(s)
        if isinstance(v, dict):
            return v
    except Exception:
        pass
    # Walk and balance braces, taking strings into account
    start = s.find("{")
    while start != -1:
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(s)):
            ch = s[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = s[start:i+1]
                    try:
                        v = json.loads(candidate)
                        if isinstance(v, dict):
                            return v
                    except Exception:
                        break
        start = s.find("{", start + 1)
    return None


def _trend_for_ticket(ticket: CoreTicket, hours_back: int = 24):
    """Return a 24-hour trend window for the degraded KPI + every related KPI
    listed by the operator's _RELATED_KPIS map. Window is exactly the 24 hours
    ending at ticket.timestamp_at (the hour the degradation was flagged), so
    the chart shows the run-up to the outage rather than what happened after.

    Returned values are auto-scaled (fractional 0–1 percentages → 0–100) so
    the line and the threshold reference lines render on the same scale.
    Pulls data across ALL CoreComponentKpi rows whose kpi_name normalises to
    the canonical name we want, so upload drift doesn't lose data.
    """
    end_dt = ticket.timestamp_at or datetime.now()
    start_dt = end_dt - timedelta(hours=hours_back)

    primary = ticket.kpi_name
    related = _related_kpis(ticket.component_type, primary)
    wanted_canonicals = [primary] + related
    wanted_norms = {_norm_kpi(n): n for n in wanted_canonicals}

    # Pull every row for this component_id in the window, then group in Python
    # so we can fuzzy-match the kpi_name on the way in.
    rows = (
        db.session.query(
            CoreComponentKpi.kpi_name,
            CoreComponentKpi.date,
            CoreComponentKpi.hour,
            db.func.avg(CoreComponentKpi.value).label("avg_val"),
        )
        .filter(
            CoreComponentKpi.component_type == ticket.component_type,
            CoreComponentKpi.component_id == ticket.component_id,
            CoreComponentKpi.value.isnot(None),
            CoreComponentKpi.date >= start_dt.date(),
            CoreComponentKpi.date <= end_dt.date(),
        )
        .group_by(CoreComponentKpi.kpi_name, CoreComponentKpi.date, CoreComponentKpi.hour)
        .all()
    )

    # Bucket each db row to a canonical kpi name (or skip if not wanted)
    series: dict[str, list[dict]] = {n: [] for n in wanted_canonicals}
    for r in rows:
        ts = datetime(r.date.year, r.date.month, r.date.day, r.hour, 0, 0) + timedelta(hours=1)
        if ts < start_dt or ts > end_dt:
            continue
        # Fuzzy resolution: exact, then synonym, then keyword overlap
        resolved = _resolve_kpi_name(ticket.component_type, r.kpi_name)
        canonical = resolved if resolved in series else None
        if not canonical:
            n = _norm_kpi(r.kpi_name)
            canonical = wanted_norms.get(n)
        if not canonical:
            # Last-ditch: substring match against wanted canonicals
            rn = _norm_kpi(r.kpi_name)
            for w_norm, w_name in wanted_norms.items():
                if rn and (rn in w_norm or w_norm in rn):
                    canonical = w_name
                    break
        if not canonical or canonical not in series:
            continue
        scaled = _scale_value(ticket.component_type, canonical, r.avg_val)
        if scaled is None:
            continue
        series[canonical].append({"ts": ts.isoformat(), "avg": round(scaled, 4)})

    for k in series:
        series[k].sort(key=lambda x: x["ts"])
    return series


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints — common helpers
# ─────────────────────────────────────────────────────────────────────────────
def _user_or_none():
    try:
        return db.session.get(User, int(get_jwt_identity()))
    except Exception:
        return None


def _alert_dict(a: CoreAlert):
    ag = db.session.get(User, a.assigned_agent) if a.assigned_agent else None
    cfg = KPI_RANGES.get(a.component_type, {}).get(a.kpi_name, {})
    return {
        "id": a.id,
        "component_type": a.component_type,
        "component_id": a.component_id,
        "kpi_name": a.kpi_name,
        "hour_date": a.hour_date.isoformat() if a.hour_date else None,
        "hour_value": a.hour_value,
        "avg_value": a.avg_value,
        "unit": cfg.get("unit", ""),
        "color": cfg.get("color", "#64748b"),
        "severity": a.severity,
        "forecast_minutes": a.forecast_minutes,
        "forecast_message": a.forecast_message,
        "acknowledged": a.acknowledged,
        "agent_id": a.assigned_agent,
        "agent_name": ag.name if ag else "",
        "agent_email": ag.email if ag else "",
        "created_at": a.created_at.isoformat() if a.created_at else None,
    }


def _ticket_dict(t: CoreTicket, include_agent=True):
    ag = db.session.get(User, t.assigned_agent) if (include_agent and t.assigned_agent) else None
    cfg = KPI_RANGES.get(t.component_type, {}).get(t.kpi_name, {})
    now = datetime.now()
    sla_remaining = None
    sla_pct = None
    # Self-heal old tickets whose sla_deadline was pegged to a future-dated
    # data hour (so the timer used to read 27d / 588h). If wall-clock remaining
    # exceeds the SLA budget we rebase the deadline to "now + sla_hours" and
    # persist it. This is logged so the operator can see when it happens.
    if (t.status not in ("resolved", "closed") and t.sla_deadline and t.sla_hours):
        rem_hours = (t.sla_deadline - now).total_seconds() / 3600
        if rem_hours > float(t.sla_hours) + 1.0:  # 1h slack to avoid jitter
            new_deadline = now + timedelta(hours=float(t.sla_hours))
            try:
                _LOG.info("[CORE SLA REBASE] Ticket %s — old deadline %s "
                          "(rem=%.1fh, budget=%.1fh) → new deadline %s",
                          t.reference_number, t.sla_deadline, rem_hours,
                          t.sla_hours, new_deadline)
                print(f"[CORE SLA REBASE] {t.reference_number} → {t.sla_hours}h "
                      f"countdown restarted at {now.strftime('%H:%M:%S')}")
                t.sla_deadline = new_deadline
                t.created_at = now
                db.session.commit()
            except Exception as _re:
                db.session.rollback()
                _LOG.warning("[CORE SLA REBASE] failed for %s: %s", t.reference_number, _re)
    if t.sla_deadline and t.status not in ("resolved", "closed"):
        rem = (t.sla_deadline - now).total_seconds() / 3600
        sla_remaining = round(rem, 2)
        if t.sla_hours:
            sla_pct = round(min(((t.sla_hours - max(rem, 0)) / max(t.sla_hours, 0.01)) * 100, 100), 1)
    # Display-time auto-scaling so old tickets stored with fractional values
    # (e.g. 0.999) render as 99.9% on the UI without needing a re-run.
    avg_disp = _scale_value(t.component_type, t.kpi_name, t.avg_value)
    thr_disp = _scale_value(t.component_type, t.kpi_name, t.threshold_value)
    return {
        "id": t.id,
        "reference_number": t.reference_number,
        "component_type": t.component_type,
        "component_id": t.component_id,
        "kpi_name": t.kpi_name,
        "hour_date": t.hour_date.isoformat() if t.hour_date else None,
        "hour_value": t.hour_value,
        "avg_value": avg_disp,
        "threshold_value": thr_disp,
        "unit": cfg.get("unit", ""),
        "color": cfg.get("color", "#64748b"),
        "priority": t.priority,
        "sla_hours": t.sla_hours,
        "sla_deadline": t.sla_deadline.isoformat() if t.sla_deadline else None,
        "sla_remaining_hours": sla_remaining,
        "sla_pct_elapsed": sla_pct,
        "status": t.status,
        "agent_id": t.assigned_agent,
        "agent_name": ag.name if ag else "",
        "agent_email": ag.email if ag else "",
        "root_cause": t.root_cause or "",
        "recommendation": t.recommendation or "",
        "resolution_notes": t.resolution_notes or "",
        "timestamp_at": t.timestamp_at.isoformat() if t.timestamp_at else None,
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "resolved_at": t.resolved_at.isoformat() if t.resolved_at else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────
@core_tickets_bp.route("/api/core/kpi-config", methods=["GET"])
@jwt_required()
def core_kpi_config():
    """Catalogue used by the frontend to label/color KPIs."""
    return jsonify({"components": COMPONENT_TYPES, "ranges": KPI_RANGES})


@core_tickets_bp.route("/api/core/alerts", methods=["GET"])
@jwt_required()
def list_alerts():
    """Return all CoreAlerts assigned to the current human_agent (only theirs).
    Manager/CTO/admin see all alerts."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    component_type = (request.args.get("component_type") or "").upper().strip()
    only_open = request.args.get("only_open", "true").lower() in ("1", "true", "yes")

    q = CoreAlert.query
    if component_type:
        q = q.filter(CoreAlert.component_type == component_type)
    if only_open:
        q = q.filter(CoreAlert.acknowledged == False)  # noqa: E712
    if user.role == "human_agent":
        q = q.filter(CoreAlert.assigned_agent == user.id)
    q = q.order_by(CoreAlert.created_at.desc()).limit(500)

    return jsonify({"alerts": [_alert_dict(a) for a in q.all()]})


@core_tickets_bp.route("/api/core/alerts/<int:alert_id>/ack", methods=["POST"])
@jwt_required()
def ack_alert(alert_id):
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    a = db.session.get(CoreAlert, alert_id)
    if not a:
        return jsonify({"error": "Not found"}), 404
    if user.role == "human_agent" and a.assigned_agent and a.assigned_agent != user.id:
        return jsonify({"error": "Not your alert"}), 403
    a.acknowledged = True
    a.acknowledged_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"success": True})


@core_tickets_bp.route("/api/core/tickets", methods=["GET"])
@jwt_required()
def list_tickets():
    """Tickets visible:
       - human_agent: only those assigned to them
       - manager/cto/admin: all
    Filters: ?component_type=MME&status=open"""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    component_type = (request.args.get("component_type") or "").upper().strip()
    status = (request.args.get("status") or "").strip()

    q = CoreTicket.query
    if component_type:
        q = q.filter(CoreTicket.component_type == component_type)
    if status:
        q = q.filter(CoreTicket.status == status)
    if user.role == "human_agent":
        q = q.filter(CoreTicket.assigned_agent == user.id)
    q = q.order_by(CoreTicket.created_at.desc()).limit(500)

    return jsonify({"tickets": [_ticket_dict(t) for t in q.all()]})


@core_tickets_bp.route("/api/core/tickets/<int:ticket_id>", methods=["GET"])
@jwt_required()
def get_ticket(ticket_id):
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    t = db.session.get(CoreTicket, ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404
    if user.role == "human_agent" and t.assigned_agent != user.id:
        return jsonify({"error": "Not your ticket"}), 403
    return jsonify(_ticket_dict(t))


@core_tickets_bp.route("/api/core/tickets/<int:ticket_id>/trend", methods=["GET"])
@jwt_required()
def ticket_trend(ticket_id):
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    t = db.session.get(CoreTicket, ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404
    series = _trend_for_ticket(t, hours_back=int(request.args.get("hours", 24)))
    # Per-KPI config the chart needs to draw threshold lines + bands.
    config = {}
    for kpi_name in series.keys():
        cfg = KPI_RANGES.get(t.component_type, {}).get(kpi_name, {}) or {}
        if cfg.get("direction") == "higher_is_better":
            t_low = cfg.get("critical_low")
            t_high = cfg.get("normal_low")
        else:
            t_low = cfg.get("normal_high")
            t_high = cfg.get("critical_high")
        config[kpi_name] = {
            "direction": cfg.get("direction"),
            "unit": cfg.get("unit", ""),
            "color": cfg.get("color", "#475569"),
            "threshold_low": _to_float(t_low),
            "threshold_high": _to_float(t_high),
            "normal_low": _to_float(cfg.get("normal_low")),
            "normal_high": _to_float(cfg.get("normal_high")),
            "critical_low": _to_float(cfg.get("critical_low")),
            "critical_high": _to_float(cfg.get("critical_high")),
        }
    return jsonify({
        "ticket_id": ticket_id,
        "primary_kpi": t.kpi_name,
        "component_id": t.component_id,
        "series": series,
        "config": config,
    })


@core_tickets_bp.route("/api/core/tickets/<int:ticket_id>/rca", methods=["POST"])
@jwt_required()
def ticket_rca(ticket_id):
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    t = db.session.get(CoreTicket, ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404

    # Build a thorough trend summary so the LLM can reason like a network
    # engineer rather than guess. We include every hourly point we have for
    # the degraded KPI plus one-line summaries (range/avg/last) for siblings.
    series = _trend_for_ticket(t, hours_back=24)
    cfg = _effective_kpi_cfg(t.component_type, t.kpi_name) or {}
    unit = cfg.get("unit", "")

    # SCALE the ticket's avg_value BEFORE using it in math. Old tickets may
    # have been stored in raw fractional form (e.g. 0.876 instead of 87.6) —
    # if we don't scale, severity_pct is computed against a value on the
    # wrong axis and the LLM ends up citing nonsense ratios.
    ticket_avg = _scale_value(t.component_type, t.kpi_name, t.avg_value) or 0.0
    ticket_thr = _scale_value(t.component_type, t.kpi_name, t.threshold_value) or 0.0

    primary_points = series.get(t.kpi_name, [])
    primary_vals = [p["avg"] for p in primary_points]
    primary_summary = ""
    if primary_vals:
        primary_summary = (
            f"  • {len(primary_vals)} hourly samples → "
            f"min={min(primary_vals):.4f}{unit}, max={max(primary_vals):.4f}{unit}, "
            f"avg={(sum(primary_vals)/len(primary_vals)):.4f}{unit}\n"
            f"  • Hourly trace (oldest→newest): "
            + ", ".join(f"{v:.3f}" for v in primary_vals[-24:])
        )

    sibling_lines = []
    for kpi_name, points in series.items():
        if kpi_name == t.kpi_name or not points:
            continue
        sib_vals = [p["avg"] for p in points]
        sib_cfg = _effective_kpi_cfg(t.component_type, kpi_name) or {}
        sib_unit = sib_cfg.get("unit", "")
        last_n = sib_vals[-6:]
        # Mark each sibling's band so the LLM knows whether to treat it as
        # corroborating evidence (stressed) or a "still-healthy" signal.
        last_band, _ = _classify(t.component_type, kpi_name, sib_vals[-1])
        sibling_lines.append(
            f"  • {kpi_name}: avg={(sum(sib_vals)/len(sib_vals)):.4f}{sib_unit}, "
            f"min={min(sib_vals):.4f}{sib_unit}, max={max(sib_vals):.4f}{sib_unit}, "
            f"last 6h={[round(v,3) for v in last_n]} — current band: {last_band}"
        )

    # Threshold bands so the AI knows what 'normal/degradation/outage' means
    if cfg.get("direction") == "higher_is_better":
        bands = (f"  • Direction: higher is better. "
                 f"Normal ≥ {cfg.get('normal_low')}{unit}, "
                 f"Degradation [{cfg.get('critical_low')}{unit}, {cfg.get('normal_low')}{unit}), "
                 f"Outage < {cfg.get('critical_low')}{unit}.")
    else:
        bands = (f"  • Direction: lower is better. "
                 f"Normal ≤ {cfg.get('normal_high')}{unit}, "
                 f"Degradation ({cfg.get('normal_high')}{unit}, {cfg.get('critical_high')}{unit}], "
                 f"Outage > {cfg.get('critical_high')}{unit}.")

    # Severity expressed two ways so the LLM has no excuse to invent ratios:
    #   - abs_gap : absolute distance from the critical line, in unit
    #   - rel_pct : % of the critical threshold the avg has moved off by
    abs_gap = None
    severity_pct = None
    try:
        if cfg.get("direction") == "higher_is_better":
            crit = float(cfg.get("critical_low") or 0.0)
            abs_gap = crit - ticket_avg
            severity_pct = (abs_gap / max(crit, 1e-9)) * 100
        else:
            crit = float(cfg.get("critical_high") or 0.0)
            abs_gap = ticket_avg - crit
            severity_pct = (abs_gap / max(crit, 1e-9)) * 100
    except Exception:
        abs_gap = None
        severity_pct = None

    delta_str = ""
    if len(primary_vals) >= 4:
        recent = sum(primary_vals[-3:]) / 3
        baseline = sum(primary_vals[:max(len(primary_vals) - 3, 1)]) / max(len(primary_vals) - 3, 1)
        delta_str = (f"  • Recent 3-hour avg = {recent:.3f}{unit}, "
                     f"earlier baseline = {baseline:.3f}{unit}, "
                     f"delta = {(recent - baseline):+.3f}{unit}.")

    expert = _kpi_expert(t.component_type, t.kpi_name) or {}
    expert_block = ""
    if expert:
        expert_block = (
            "── KPI domain context (you MUST use this when reasoning) ──\n"
            f"  Protocol/interface : {expert.get('protocol','')}\n"
            f"  Role               : {expert.get('role','')}\n"
            f"  What it measures   : {expert.get('measures','')}\n"
            f"  Common failure modes: {'; '.join(expert.get('failure_modes', []))}\n"
            f"  Diagnostic signals : {', '.join(expert.get('signals', []))}\n"
            f"  Customer impact path: {expert.get('impact','')}\n"
            f"  Governing parameters: {', '.join(expert.get('parameters', []))}\n\n"
        )

    severity_str = ""
    if severity_pct is not None and abs_gap is not None:
        severity_str = (f" The avg is {abs_gap:+.3f}{unit} past the critical line "
                        f"(~{abs(severity_pct):.1f}% of the threshold value).")

    prompt = (
        f"You are diagnosing a {t.component_type} core-network outage on instance "
        f"{t.component_id} for KPI \"{t.kpi_name}\". The hourly mean for "
        f"{t.hour_date} {t.hour_value:02d}:00–{(t.hour_value+1):02d}:00 was "
        f"{ticket_avg:.4f}{unit}, breaching the critical threshold of "
        f"{ticket_thr}{unit}." + severity_str + "\n\n"
        + expert_block
        + "── Threshold bands ──\n" + bands + "\n\n"
        "── Primary KPI (degraded) ──\n" + (primary_summary or "  (no historical samples)") + "\n"
        + (delta_str + "\n" if delta_str else "")
        + "\n── Sibling KPIs on the same instance (with current band) ──\n"
        + ("\n".join(sibling_lines) if sibling_lines else "  (none)")
        + "\n\n"
        "═══════════════════════════════════════════════════════════════════════\n"
        "TASK — write a fact-based Root Cause Analysis as a senior 4G/5G EPC engineer.\n"
        "═══════════════════════════════════════════════════════════════════════\n\n"
        "HARD RULES (your output will be auto-rejected if any rule is broken):\n"
        "  R1. DO NOT INVENT NUMBERS. Every numeric value in your output MUST appear\n"
        "      verbatim in the data block above (or be computed plainly from it like\n"
        "      min, max, count of consecutive hours). NEVER cite a percentage you did\n"
        "      not see in the data. If you need a severity figure, use ONLY the values\n"
        "      already given in the header sentence (the gap and threshold above).\n"
        "  R2. STRICT HEADERS — the 5 point headers MUST be the exact strings below.\n"
        "      Do NOT rename point 5 to 'Remedial Actions' or anything else.\n"
        "      The point identifies the parameter root cause; the fix is implicit.\n"
        "  R3. SIBLING INTERPRETATION — when a sibling KPI is still in the NORMAL band\n"
        "      while the primary KPI is in OUTAGE, the correct reading is:\n"
        "         existing flows / established sessions are persisting, but new\n"
        "         procedures that use the failing layer are starting to fail.\n"
        "      NEVER call that 'surprising' or 'inconsistent'. Explain it that way.\n"
        "  R4. PROTOCOL NAMES — name the actual protocol(s) / interface(s) from the\n"
        "      KPI domain context above (S1AP/NAS over SCTP, Diameter S6a, GTP-C v2,\n"
        "      Gx, GTP-U, etc.). Use them precisely; don't say 'SCTP heartbeats' if\n"
        "      the failure mode is path flap, etc.\n"
        "  R5. PLAIN LANGUAGE GLOSS — every technical acronym must have a 3-6 word\n"
        "      plain-language note in parentheses the first time it appears, so the\n"
        "      analysis is readable by non-experts too. Example:\n"
        "         'SCTP (the reliable transport carrying S1AP between MME and eNodeBs)'\n"
        "  R6. EVIDENCE PER POINT — every point cites at least ONE concrete value or\n"
        "      observation from the data block (min/max/latest, baseline-vs-recent\n"
        "      delta, consecutive hours past threshold, a sibling's current band).\n"
        "  R7. CAUSAL CHAIN — point 5 must name ONE parameter from the 'Governing\n"
        "      parameters' list in the domain context (or a closely-related one) and\n"
        "      justify the choice with reference to the failure signature in point 1\n"
        "      and the mechanism in point 3.\n"
        "  R8. NO HYPE — don't use words like 'catastrophic', 'severe', 'devastating'.\n"
        "      Stay measured: 'sustained', 'sharp', 'sub-threshold for N consecutive\n"
        "      hours' are fine.\n\n"
        "OUTPUT FORMAT — follow EXACTLY:\n"
        "  - EXACTLY 5 points, numbered 1. through 5.\n"
        "  - Each point starts with the EXACT header below, followed by ' — ', then body.\n"
        "  - BODY LENGTH: 45–70 words per point. Tight, dense, evidence-led.\n"
        "  - Always finish every sentence with a full stop.\n"
        "  - Put a BLANK line between consecutive points.\n"
        "  - No markdown, no bullet symbols, no extra prose outside the 5 points.\n\n"
        "THE FIVE POINTS (use these headers verbatim):\n\n"
        "1. Failure Signature — describe the pattern visible in the primary KPI's hourly\n"
        "   trace (sustained climb, cliff, oscillation). State min, max, latest, baseline-\n"
        "   before-the-breach, and the number of consecutive hours past the critical line.\n\n"
        "2. Cross-KPI Correlation — pick 2-3 sibling KPIs from the data block by NAME and\n"
        "   NUMERIC value with their current band. Explain what each correlation means.\n"
        "   When a sibling is still NORMAL, apply rule R3.\n\n"
        f"3. Component-Specific Mechanism — describe the precise {t.component_type} signalling\n"
        "   / control-plane path that produces this failure, using the protocol names from\n"
        "   the domain context. Point at the exact failure mode from the 'Common failure\n"
        "   modes' list that best matches the trace.\n\n"
        "4. Likely Trigger and Customer Impact — state the most probable trigger right now\n"
        "   (traffic surge, peer failover, IP path flap, capacity ceiling, configuration\n"
        "   drift, DB lag, GC pause) AND the concrete subscriber-facing impact from the\n"
        "   'Customer impact path' in the domain context. Be specific about which subscribers\n"
        "   are affected and which procedures fail vs. which still work.\n\n"
        "5. Most Probable Parameter Root Cause — name ONE parameter from the 'Governing\n"
        "   parameters' list (or a closely related one) that has the strongest causal link\n"
        "   to the observed signature. Justify with reference to point 1's pattern and\n"
        "   point 3's mechanism. Do NOT rename this header to 'Remedial Actions' or similar."
    )
    # MAX_TOKENS is set to 8192 — the largest budget the gpt-4o-mini /
    # gemini-2.5-flash deployments accept in a single response. This is the
    # ceiling for "give the maximum tokens" the operator asked for.
    text = _llm(
        prompt,
        system=(
            "You are a senior 4G/5G EPC core network engineer writing a Root "
            "Cause Analysis that must withstand peer review by another expert. "
            "ANTI-HALLUCINATION RULES (non-negotiable):\n"
            "  - Use ONLY numbers that appear verbatim in the user's data block "
            "or are trivially derivable from it (min, max, count). NEVER invent "
            "percentages. If the user gave you a 'gap' and a 'threshold', use "
            "those exact values; do not transform them into novel ratios.\n"
            "  - Use the EXACT 5 headers the user prescribed. NEVER rename them.\n"
            "  - When a sibling KPI is in the NORMAL band while the primary KPI "
            "is in OUTAGE, the correct interpretation is: 'established sessions "
            "persist; new procedures fail'. Never describe this as 'surprising'.\n"
            "  - Use real protocol names (S1AP, NAS, SCTP, Diameter S6a, GTP-C, "
            "Gx, GTP-U, etc.) with a short plain-language gloss in parentheses "
            "the first time each appears.\n"
            "  - Avoid hype words (catastrophic, devastating, severe). Stay\n"
            "    measured: 'sustained', 'sharp', 'sub-threshold for N hours'.\n"
            "Always produce ALL 5 numbered points; never end mid-sentence; if "
            "running out of tokens, shorten earlier points to save room for "
            "point 5. No markdown, no bullets, no extra prose."
        ),
        max_tokens=8192,
    )

    def _looks_complete(s: str) -> bool:
        s = (s or "").rstrip()
        if not s:
            return False
        # Must end on terminal punctuation
        if not s.endswith((".", "!", "?", '"', "'", ")")):
            return False
        # Must contain a numbered point 5
        return bool(_re.search(r"(?:^|\n)\s*5\s*[.)]\s", s))

    # Up to 3 continuation passes if needed
    for attempt in range(3):
        if _looks_complete(text):
            break
        _LOG.info("[CORE RCA] Continuation pass %d (text ends: %r)", attempt + 1, text[-80:].replace("\n", " "))
        try:
            cont = _llm(
                f"You are continuing an unfinished Root Cause Analysis. "
                f"The text below stops mid-sentence or before point 5 is "
                f"complete. Resume EXACTLY where it left off — do not repeat "
                f"any words already present. Continue until point 5 has been "
                f"written and ends with a full stop. Output ONLY the missing "
                f"tail, no preamble, no markdown.\n\n"
                f"PARTIAL TEXT:\n{text}",
                system="Continue the RCA in plain text. Finish every sentence. "
                       "Never repeat content. End with a period.",
                max_tokens=4096,
            )
            if cont and not cont.lower().startswith("ai generation failed"):
                # Glue: keep partial as-is, append continuation
                text = (text.rstrip() + " " + cont.lstrip()).strip()
            else:
                break
        except Exception as _ce:
            _LOG.warning("[CORE RCA] continuation pass failed: %s", _ce)
            break
    t.root_cause = text
    db.session.commit()
    return jsonify({"root_cause": text})


@core_tickets_bp.route("/api/core/tickets/<int:ticket_id>/recommendation", methods=["POST"])
@jwt_required()
def ticket_recommendation(ticket_id):
    """Use OpenAI + uploaded CoreParameter rows to recommend the most appropriate
    parameter to change, its current and proposed values, and why."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    t = db.session.get(CoreTicket, ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404

    # Same fuzzy + tiered lookup the change-parameter list uses, so the
    # recommendation engine sees the parameters even when the uploaded
    # kpi_name string differs slightly from our canonical name.
    # Case-insensitive component_type so 'mme'/'MME'/' MME ' all match.
    rows_for_type = (CoreParameter.query
                     .filter(db.func.upper(db.func.trim(CoreParameter.component_type)) == t.component_type.upper())
                     .all())

    kpi_match_keys = {_norm_kpi(t.kpi_name)} if t.kpi_name else set()
    canonical = _resolve_kpi_name(t.component_type, t.kpi_name) if t.kpi_name else None
    if canonical:
        kpi_match_keys.add(_norm_kpi(canonical))

    def _kpi_matches(p) -> bool:
        if not kpi_match_keys or not p.kpi_name:
            return False
        if _norm_kpi(p.kpi_name) in kpi_match_keys:
            return True
        resolved = _resolve_kpi_name(t.component_type, p.kpi_name)
        if resolved and _norm_kpi(resolved) in kpi_match_keys:
            return True
        return False

    # Tier 1: same component_id + same kpi
    params_kpi = [p for p in rows_for_type if p.component_id == t.component_id and _kpi_matches(p)]
    # Tier 2: any component_id + same kpi
    if not params_kpi:
        params_kpi = [p for p in rows_for_type if _kpi_matches(p)]
    # Tier 3: same component_id, any kpi
    if not params_kpi:
        params_kpi = [p for p in rows_for_type if p.component_id == t.component_id]
    # Tier 4: any param for this component_type
    if not params_kpi:
        params_kpi = rows_for_type

    param_lines = []
    for p in params_kpi[:120]:
        param_lines.append(
            f"- group={p.parameter_group or '-'}  name={p.parameter_name}  "
            f"current={p.current_value}  unit={p.unit or '-'}  "
            f"component_id={p.component_id}  related_kpi={p.kpi_name or '-'}"
        )

    cfg = KPI_RANGES.get(t.component_type, {}).get(t.kpi_name, {}) or {}
    unit = cfg.get("unit", "")

    if not param_lines:
        # No parameters uploaded — return a structured "needs upload" response
        rec_obj = {
            "parameter_group": "",
            "parameter_name": "",
            "current_value": "",
            "proposed_value": "",
            "unit": "",
            "direction": "",
            "reason": (f"No core parameters have been uploaded for {t.component_type}. "
                       "Ask admin to upload the Core Parameter workbook so OpenAI can recommend a fix."),
        }
        t.recommendation = json.dumps(rec_obj)
        db.session.commit()
        return jsonify({"recommendation": rec_obj, "raw": ""})

    related = _related_kpis(t.component_type, t.kpi_name)
    related_str = ", ".join(related) if related else "(none specified)"

    prompt = (
        f"You are a 4G/5G core optimisation engineer. A {t.component_type} instance "
        f"{t.component_id} has KPI \"{t.kpi_name}\" in OUTAGE:\n"
        f"  - Hour {t.hour_value:02d}:00 of {t.hour_date}\n"
        f"  - Hourly average = {float(t.avg_value):.4f}{unit}\n"
        f"  - Critical threshold = {t.threshold_value}{unit}\n"
        f"  - Direction = {cfg.get('direction','?')}\n"
        f"  - KPIs related to this issue (per ops manual): {related_str}\n\n"
        f"Available CORE PARAMETERS for this component (current values):\n"
        + "\n".join(param_lines) + "\n\n"
        "TASK\n"
        "  1. Pick the single parameter from the list above most likely to fix this KPI.\n"
        "  2. Explain in 3-4 short bullet points why this parameter, what to change it to, "
        "     and how it cures the degradation.\n"
        "  3. Be specific and use the parameter's exact name, current value, unit, and a "
        "     concrete numeric proposed value.\n\n"
        "OUTPUT FORMAT — return ONE complete JSON object, nothing else. No prose, no "
        "markdown, no code fences. Keep total length under 1200 tokens.\n\n"
        "{\n"
        '  "parameter_group": "<copy verbatim from list>",\n'
        '  "parameter_name":  "<copy verbatim from list>",\n'
        '  "current_value":   "<copy verbatim>",\n'
        '  "proposed_value":  "<single numeric value, no ranges>",\n'
        '  "unit":            "<copy from list, can be empty>",\n'
        '  "direction":       "increase" or "decrease",\n'
        '  "reason":          "<3-4 sentences, plain text, no quotes inside, '
        'why THIS parameter cures THIS KPI given the related KPIs>",\n'
        '  "bullets":         ["<bullet 1, ~25 words>", "<bullet 2>", "<bullet 3>"]\n'
        "}\n\n"
        "Constraints:\n"
        "  - parameter_name MUST appear verbatim in the parameter list above.\n"
        "  - current_value MUST be copied verbatim from the matching parameter row.\n"
        "  - proposed_value MUST be a single numeric value, not a range, not text.\n"
        "  - bullets array has exactly 3 or 4 items, each plain text, no nested JSON.\n"
    )
    raw = _llm(
        prompt,
        system=("You are a senior 4G/5G core network optimization engineer. "
                "Respond with ONE complete JSON object only. Never produce prose, "
                "markdown, or code fences. Do NOT embed JSON or quotes inside text fields."),
        max_tokens=1800,
    )

    rec_obj = _extract_json_object(raw)

    # If JSON extraction failed (truncation, prose preamble, or schema drift),
    # ask the LLM once to repair the output into strict JSON.
    if not rec_obj:
        _LOG.info("[CORE REC] First pass returned non-JSON; attempting repair.")
        repair_prompt = (
            "The following text was meant to be a JSON object recommending a "
            "parameter change but is malformed or truncated. Produce a corrected "
            "JSON object with EXACTLY these keys: parameter_group, parameter_name, "
            "current_value, proposed_value, unit, direction, reason. Output ONLY "
            "the JSON object, nothing else. Keep reason under 150 characters. "
            "If the original is hopeless, infer values from the parameter list below.\n\n"
            f"ORIGINAL OUTPUT:\n{raw}\n\n"
            f"PARAMETER LIST:\n" + "\n".join(param_lines[:60])
        )
        raw2 = _llm(
            repair_prompt,
            system="Output a single complete JSON object only. No prose. No markdown.",
            max_tokens=1200,
        )
        rec_obj = _extract_json_object(raw2)

    # Coerce to expected schema and strip any nested-JSON noise from reason
    REQUIRED_KEYS = ["parameter_group", "parameter_name", "current_value",
                     "proposed_value", "unit", "direction", "reason"]
    if rec_obj and isinstance(rec_obj, dict):
        bullets_raw = rec_obj.get("bullets") or []
        if not isinstance(bullets_raw, list):
            bullets_raw = []
        bullets = [str(b).strip() for b in bullets_raw if str(b).strip()][:6]
        clean = {k: rec_obj.get(k, "") for k in REQUIRED_KEYS}
        # If reason itself looks like JSON, replace it with something readable
        if isinstance(clean.get("reason"), str) and clean["reason"].strip().startswith("{"):
            clean["reason"] = ("AI returned nested JSON in the reason field; "
                               "selected parameter shown above.")
        # Stringify any non-string field
        for k in REQUIRED_KEYS:
            if clean[k] is None:
                clean[k] = ""
            elif not isinstance(clean[k], str):
                clean[k] = str(clean[k])
        clean["bullets"] = bullets
        rec_obj = clean

        # Validate parameter_name is actually in our list
        valid_names = {p.parameter_name for p in params_kpi}
        if rec_obj["parameter_name"] and rec_obj["parameter_name"] not in valid_names:
            # Fuzzy match on the AI's choice
            picked = None
            target = rec_obj["parameter_name"].lower()
            for p in params_kpi:
                if p.parameter_name.lower() == target:
                    picked = p
                    break
            if not picked:
                for p in params_kpi:
                    if target in p.parameter_name.lower() or p.parameter_name.lower() in target:
                        picked = p
                        break
            if picked:
                rec_obj["parameter_name"] = picked.parameter_name
                rec_obj["parameter_group"] = picked.parameter_group or ""
                rec_obj["current_value"] = picked.current_value or ""
                rec_obj["unit"] = picked.unit or rec_obj.get("unit", "")
    else:
        rec_obj = {
            "parameter_group": "",
            "parameter_name": "",
            "current_value": "",
            "proposed_value": "",
            "unit": "",
            "direction": "",
            "reason": (raw or "AI did not return a structured recommendation. "
                              "Try generating again."),
        }

    t.recommendation = json.dumps(rec_obj)
    db.session.commit()
    return jsonify({"recommendation": rec_obj, "raw": raw})


@core_tickets_bp.route("/api/core/tickets/<int:ticket_id>/resolve", methods=["POST"])
@jwt_required()
def resolve_ticket(ticket_id):
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    t = db.session.get(CoreTicket, ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404
    if user.role == "human_agent" and t.assigned_agent != user.id:
        return jsonify({"error": "Not your ticket"}), 403
    notes = (request.json or {}).get("notes", "").strip()
    t.status = "resolved"
    t.resolved_at = datetime.now(timezone.utc)
    t.resolution_notes = notes
    db.session.commit()
    return jsonify({"success": True})


@core_tickets_bp.route("/api/core/tickets/<int:ticket_id>/status", methods=["PUT"])
@jwt_required()
def update_ticket_status(ticket_id):
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    t = db.session.get(CoreTicket, ticket_id)
    if not t:
        return jsonify({"error": "Not found"}), 404
    new = ((request.json or {}).get("status") or "").strip()
    if new not in ("open", "in_progress", "resolved", "closed"):
        return jsonify({"error": "Invalid status"}), 400
    t.status = new
    if new == "resolved":
        t.resolved_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"success": True, "status": new})


# ─── Hourly routing — visible to everyone (manager/CTO/admin/human_agent) ───
@core_tickets_bp.route("/api/core/job-log", methods=["GET"])
@jwt_required()
def core_job_log():
    """Recent CoreJobLog activity — used by the Hourly Routing panel to show
    when the job ran, how many alerts/tickets it produced, and which hours
    are still pending. Visible to all logged-in users."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    limit = int(request.args.get("limit", 48))
    rows = (CoreJobLog.query
            .order_by(CoreJobLog.hour_date.desc(), CoreJobLog.hour_value.desc())
            .limit(limit)
            .all())

    # Derive REAL per-hour ticket and alert counts directly from CoreTicket /
    # CoreAlert (grouped by hour_date, hour_value). The CoreJobLog counter
    # only captures `tickets_created` at the moment of run, but if a ticket
    # was de-duped or the log row failed to commit, the count drifts.
    # Querying the actual tables gives the truth.
    ticket_counts: dict[tuple, dict] = {}
    try:
        rows_t = (db.session.query(
                    CoreTicket.hour_date, CoreTicket.hour_value,
                    CoreTicket.created_at, CoreTicket.updated_at,
                  ).all())
        # Compute per-hour: tickets whose hour_date+hour_value matches; split
        # into "new this hour" (created_at == updated_at) vs "updated".
        for hd, hv, ca, ua in rows_t:
            if hd is None or hv is None:
                continue
            key = (hd, hv)
            d = ticket_counts.setdefault(key, {"total": 0, "new": 0, "updated": 0})
            d["total"] += 1
            if ca and ua and ca == ua:
                d["new"] += 1
            else:
                d["updated"] += 1
    except Exception as e:
        _LOG.warning("[CORE JOB LOG] ticket count derivation failed: %s", e)

    alert_counts: dict[tuple, int] = {}
    try:
        rows_a = (db.session.query(
                    CoreAlert.hour_date, CoreAlert.hour_value,
                    db.func.count(CoreAlert.id),
                  ).group_by(CoreAlert.hour_date, CoreAlert.hour_value).all())
        for hd, hv, cnt in rows_a:
            if hd is None or hv is None:
                continue
            alert_counts[(hd, hv)] = int(cnt or 0)
    except Exception as e:
        _LOG.warning("[CORE JOB LOG] alert count derivation failed: %s", e)

    out = []
    for r in rows:
        key = (r.hour_date, r.hour_value)
        tc = ticket_counts.get(key, {"total": 0, "new": 0, "updated": 0})
        ac = alert_counts.get(key, 0)
        out.append({
            "hour_date": r.hour_date.isoformat() if r.hour_date else None,
            "hour_value": r.hour_value,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "status": "finished" if r.finished_at else "running",
            # Backward-compat: still expose alerts_created / tickets_created.
            # Now use the DERIVED counts so cards always reflect reality.
            "alerts_created": ac,
            "tickets_created": tc["new"],
            # New richer fields
            "tickets_total": tc["total"],
            "tickets_new": tc["new"],
            "tickets_updated": tc["updated"],
        })
    # Also: tickets created/updated in last 24h (for the activity feed)
    cutoff = datetime.now() - timedelta(hours=24)
    recent = (CoreTicket.query
              .filter(CoreTicket.updated_at >= cutoff)
              .order_by(CoreTicket.updated_at.desc())
              .limit(80)
              .all())
    activity = []
    for t in recent:
        ag = db.session.get(User, t.assigned_agent) if t.assigned_agent else None
        activity.append({
            "reference_number": t.reference_number,
            "component_type": t.component_type,
            "component_id": t.component_id,
            "kpi_name": t.kpi_name,
            "priority": t.priority,
            "status": t.status,
            "agent_name": ag.name if ag else "Unassigned",
            "agent_email": ag.email if ag else "",
            "hour_date": t.hour_date.isoformat() if t.hour_date else None,
            "hour_value": t.hour_value,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "updated_at": t.updated_at.isoformat() if t.updated_at else None,
            "is_new": (t.created_at == t.updated_at) if (t.created_at and t.updated_at) else True,
        })
    return jsonify({
        "logs": out,
        "activity": activity,
        "now": datetime.now().isoformat(),
    })


@core_tickets_bp.route("/api/core/hourly-routing", methods=["GET"])
@jwt_required()
def hourly_routing():
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    # Today's routing — show every ticket whose hour-end (timestamp_at)
    # falls today OR which was created/updated today. The OR catches both
    # tickets pertaining to today's data AND tickets created/updated today
    # from past- or future-dated demo data.
    today = _date.today()
    start_dt = datetime.combine(today, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)

    q = (CoreTicket.query
         .filter(db.or_(
             db.and_(CoreTicket.timestamp_at >= start_dt, CoreTicket.timestamp_at < end_dt),
             db.and_(CoreTicket.created_at >= start_dt, CoreTicket.created_at < end_dt),
             db.and_(CoreTicket.updated_at >= start_dt, CoreTicket.updated_at < end_dt),
         ))
         .order_by(CoreTicket.updated_at.desc())
         .all())

    by_type = {ct: [] for ct in COMPONENT_TYPES}
    for t in q:
        ag = db.session.get(User, t.assigned_agent) if t.assigned_agent else None
        by_type.setdefault(t.component_type, []).append({
            "ticket_id": t.id,
            "reference_number": t.reference_number,
            "component_type": t.component_type,
            "component_id": t.component_id,
            "kpi_name": t.kpi_name,
            "hour": t.hour_value,
            "hour_iso": t.timestamp_at.isoformat() if t.timestamp_at else None,
            "priority": t.priority,
            "status": t.status,
            "agent_name": ag.name if ag else "Unassigned",
            "agent_email": ag.email if ag else "",
            "agent_id": t.assigned_agent,
            "color": KPI_RANGES.get(t.component_type, {}).get(t.kpi_name, {}).get("color", "#64748b"),
        })

    return jsonify({"date": today.isoformat(), "by_type": by_type})


# ─── Manual reconcile trigger — admin / cto only ─────────────────────────────
@core_tickets_bp.route("/api/core/reconcile-tickets", methods=["POST"])
@jwt_required()
def reconcile_tickets_endpoint():
    """Re-check every open ticket and alert against current thresholds.
    Tickets/alerts whose KPI is no longer monitored, or whose stored avg no
    longer breaches the critical band, are DELETED."""
    user = _user_or_none()
    if not user or user.role not in ("admin", "cto"):
        return jsonify({"error": "Admin/CTO only"}), 403
    body = request.json or {}
    result = reconcile_open_tickets(dry_run=bool(body.get("dry_run", False)))
    return jsonify(result)


@core_tickets_bp.route("/api/core/rebuild-tickets", methods=["POST"])
@jwt_required()
def rebuild_tickets_endpoint():
    """Wipe core_job_log + force-replay every hour against the current
    thresholds. Use this after uploading new thresholds when the catch-up
    would otherwise skip old "finished" hours."""
    user = _user_or_none()
    if not user or user.role not in ("admin", "cto"):
        return jsonify({"error": "Admin/CTO only"}), 403
    # Reconcile first to clear stale tickets/alerts, then rebuild.
    rec = reconcile_open_tickets(dry_run=False)
    rebuild = rebuild_tickets_from_data()
    return jsonify({"reconciliation": rec, "rebuild": rebuild})


@core_tickets_bp.route("/api/core/diagnose-today", methods=["GET"])
@jwt_required()
def diagnose_today_endpoint():
    """For every (component_type, component_id, monitored_kpi), return today's
    hourly averages + band classification. Lets the operator see at a glance
    whether 'no tickets' means the data is healthy or there's no data."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    today = _date.today()

    # All distinct (component_type, component_id) pairs that have data today
    pairs_q = (db.session.query(
                    CoreComponentKpi.component_type,
                    CoreComponentKpi.component_id,
                )
               .filter(CoreComponentKpi.date == today,
                       CoreComponentKpi.value.isnot(None))
               .distinct()
               .all())

    # Hourly averages keyed by (ct, cid, kpi, hour)
    hourly_q = (db.session.query(
                    CoreComponentKpi.component_type,
                    CoreComponentKpi.component_id,
                    CoreComponentKpi.kpi_name,
                    CoreComponentKpi.hour,
                    db.func.avg(CoreComponentKpi.value).label("avg_val"),
                )
                .filter(CoreComponentKpi.date == today,
                        CoreComponentKpi.value.isnot(None))
                .group_by(CoreComponentKpi.component_type, CoreComponentKpi.component_id,
                          CoreComponentKpi.kpi_name, CoreComponentKpi.hour)
                .all())

    # Bucket: { (ct, cid, canonical_kpi): { hour: scaled_avg } }
    buckets: dict = {}
    for r in hourly_q:
        ct_ = (r.component_type or "").upper()
        cid = (r.component_id or "").strip()
        if not cid or cid.lower() in ("none", "nan", "null"):
            continue
        canonical = _resolve_kpi_name(ct_, r.kpi_name) or r.kpi_name
        scaled = _scale_value(ct_, canonical, r.avg_val)
        buckets.setdefault((ct_, cid, canonical), {})[r.hour] = scaled

    # Build per-instance × per-monitored-KPI summary
    out = {}
    for ct_, cid in sorted({(p[0].upper(), p[1]) for p in pairs_q if p[1]}):
        if not _is_monitored(ct_, _MONITORED_KPIS.get(ct_, [None])[0]):
            continue
        out.setdefault(ct_, {})[cid] = []
        for mk in _MONITORED_KPIS.get(ct_, []):
            hours = buckets.get((ct_, cid, mk), {})
            n_hours = len(hours)
            counts = {"normal": 0, "degradation": 0, "outage": 0}
            samples = []
            for h in sorted(hours.keys()):
                v = hours[h]
                cat, _cfg = _classify(ct_, mk, v)
                counts[cat if cat in counts else "normal"] += 1
                samples.append({"hour": h, "avg": v, "band": cat})
            cfg = _effective_kpi_cfg(ct_, mk) or {}
            out[ct_][cid].append({
                "kpi": mk,
                "unit": cfg.get("unit", ""),
                "direction": cfg.get("direction", ""),
                "thresholds": {
                    "normal_low": cfg.get("normal_low"),
                    "normal_high": cfg.get("normal_high"),
                    "critical_low": cfg.get("critical_low"),
                    "critical_high": cfg.get("critical_high"),
                },
                "hours_with_data": n_hours,
                "counts": counts,
                "samples": samples,  # list of {hour, avg, band}
            })

    # Today's job-log entries
    job_logs = (CoreJobLog.query.filter(CoreJobLog.hour_date == today)
                .order_by(CoreJobLog.hour_value).all())

    return jsonify({
        "date": today.isoformat(),
        "by_component": out,
        "job_log": [{
            "hour": l.hour_value,
            "alerts_created": l.alerts_created,
            "tickets_created": l.tickets_created,
            "finished_at": l.finished_at.isoformat() if l.finished_at else None,
        } for l in job_logs],
    })


@core_tickets_bp.route("/api/core/cleanup-future", methods=["POST"])
@jwt_required()
def cleanup_future_endpoint():
    """Delete any tickets / alerts / job-log entries whose hour is in the
    future relative to wall-clock now. Tickets only get created when their
    hour actually arrives — this just enforces that invariant."""
    user = _user_or_none()
    if not user or user.role not in ("admin", "cto"):
        return jsonify({"error": "Admin/CTO only"}), 403
    return jsonify(delete_future_tickets())


@core_tickets_bp.route("/api/core/run-todays-job", methods=["POST"])
@jwt_required()
def run_todays_job_endpoint():
    """Force-replay every completed hour of TODAY (wall-clock).
    Available to admin / cto / manager."""
    user = _user_or_none()
    if not user or user.role not in ("admin", "cto", "manager"):
        return jsonify({"error": "Unauthorized"}), 403
    body = request.json or {}
    force = body.get("force", True)
    return jsonify(run_todays_job(force=bool(force)))


# ─── Manual job trigger — admin / cto / manager ──────────────────────────────
@core_tickets_bp.route("/api/core/run-job", methods=["POST"])
@jwt_required()
def run_job_now():
    """Trigger the hourly job immediately.

    Optional JSON body:
        {"from_date": "YYYY-MM-DD", "to_date": "YYYY-MM-DD"}
    When omitted, runs the auto catch-up (last-log → now), bounded to the
    data range that actually exists in core_component_kpi.
    """
    user = _user_or_none()
    if not user or user.role not in ("admin", "cto", "manager"):
        return jsonify({"error": "Unauthorized"}), 403

    body = request.json or {}
    from_d = body.get("from_date")
    to_d = body.get("to_date")
    force = bool(body.get("force"))
    fd = td = None
    try:
        if from_d:
            fd = datetime.strptime(from_d, "%Y-%m-%d").date()
        if to_d:
            td = datetime.strptime(to_d, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Use YYYY-MM-DD for from_date / to_date"}), 400

    a, t = run_core_hourly_job(from_date=fd, to_date=td, force=force)
    return jsonify({"alerts_created": a, "tickets_created": t,
                    "from_date": fd.isoformat() if fd else None,
                    "to_date": td.isoformat() if td else None,
                    "force": force})


# ─── Coverage diagnostic — what dates/hours of data are present ───────────────
@core_tickets_bp.route("/api/core/hour-detail", methods=["GET"])
@jwt_required()
def hour_detail():
    """Detailed breakdown for one (date, hour). Returns the tickets and alerts
    whose hour_date+hour_value matches, split into NEW vs UPDATED for tickets,
    plus the agent each was routed to. Used by the clickable hour cards in
    the Hourly Routing modal."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    date_str = request.args.get("date") or ""
    hour_str = request.args.get("hour") or ""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        h = int(hour_str)
    except (TypeError, ValueError):
        return jsonify({"error": "Use date=YYYY-MM-DD&hour=H"}), 400

    tickets = (CoreTicket.query
               .filter(CoreTicket.hour_date == d, CoreTicket.hour_value == h)
               .order_by(CoreTicket.component_type, CoreTicket.component_id)
               .all())
    alerts = (CoreAlert.query
              .filter(CoreAlert.hour_date == d, CoreAlert.hour_value == h)
              .order_by(CoreAlert.component_type, CoreAlert.component_id)
              .all())

    out_tickets_new = []
    out_tickets_upd = []
    for t in tickets:
        ag = db.session.get(User, t.assigned_agent) if t.assigned_agent else None
        cfg = KPI_RANGES.get(t.component_type, {}).get(t.kpi_name, {})
        item = {
            "id": t.id,
            "reference_number": t.reference_number,
            "component_type": t.component_type,
            "component_id": t.component_id,
            "kpi_name": t.kpi_name,
            "avg_value": _scale_value(t.component_type, t.kpi_name, t.avg_value),
            "threshold_value": _scale_value(t.component_type, t.kpi_name, t.threshold_value),
            "unit": cfg.get("unit", ""),
            "priority": t.priority,
            "status": t.status,
            "agent_name": ag.name if ag else "Unassigned",
            "agent_email": ag.email if ag else "",
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "updated_at": t.updated_at.isoformat() if t.updated_at else None,
        }
        is_new = (t.created_at and t.updated_at and t.created_at == t.updated_at)
        (out_tickets_new if is_new else out_tickets_upd).append(item)

    out_alerts = []
    for a in alerts:
        ag = db.session.get(User, a.assigned_agent) if a.assigned_agent else None
        cfg = KPI_RANGES.get(a.component_type, {}).get(a.kpi_name, {})
        out_alerts.append({
            "id": a.id,
            "component_type": a.component_type,
            "component_id": a.component_id,
            "kpi_name": a.kpi_name,
            "avg_value": _scale_value(a.component_type, a.kpi_name, a.avg_value),
            "unit": cfg.get("unit", ""),
            "forecast_minutes": a.forecast_minutes,
            "forecast_message": a.forecast_message,
            "acknowledged": a.acknowledged,
            "agent_name": ag.name if ag else "Unassigned",
            "agent_email": ag.email if ag else "",
            "created_at": a.created_at.isoformat() if a.created_at else None,
        })

    return jsonify({
        "date": d.isoformat(),
        "hour": h,
        "tickets_new": out_tickets_new,
        "tickets_updated": out_tickets_upd,
        "alerts": out_alerts,
        "totals": {
            "tickets_new": len(out_tickets_new),
            "tickets_updated": len(out_tickets_upd),
            "alerts": len(out_alerts),
        },
    })


@core_tickets_bp.route("/api/core/coverage", methods=["GET"])
@jwt_required()
def core_coverage():
    """Return a summary of what's available in core_component_kpi and the
    job-log so the operator can see whether the catch-up is finding the right
    data."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    data_min, data_max, data_count = _data_date_range()
    # Per-component-type row counts and unique component_ids
    per_type = []
    try:
        rows = (db.session.query(
                    CoreComponentKpi.component_type,
                    db.func.count(CoreComponentKpi.id),
                    db.func.count(db.distinct(CoreComponentKpi.component_id)),
                    db.func.min(CoreComponentKpi.date),
                    db.func.max(CoreComponentKpi.date),
                ).group_by(CoreComponentKpi.component_type).all())
        for r in rows:
            per_type.append({
                "component_type": r[0],
                "rows": int(r[1] or 0),
                "component_ids": int(r[2] or 0),
                "min_date": r[3].isoformat() if r[3] else None,
                "max_date": r[4].isoformat() if r[4] else None,
            })
    except Exception as e:
        _LOG.warning("coverage per_type failed: %s", e)

    # KPI-name match: this is the most common failure mode. We list every
    # distinct (component_type, kpi_name) in the data, with min/avg/max value,
    # and tell the operator whether KPI_RANGES has a threshold for it.
    kpi_audit = []
    try:
        krows = (db.session.query(
                    CoreComponentKpi.component_type,
                    CoreComponentKpi.kpi_name,
                    db.func.count(CoreComponentKpi.id),
                    db.func.min(CoreComponentKpi.value),
                    db.func.avg(CoreComponentKpi.value),
                    db.func.max(CoreComponentKpi.value),
                 )
                 .filter(CoreComponentKpi.value.isnot(None))
                 .group_by(CoreComponentKpi.component_type, CoreComponentKpi.kpi_name)
                 .order_by(CoreComponentKpi.component_type, CoreComponentKpi.kpi_name)
                 .all())
        for r in krows:
            ct_name = r[0]
            kn = r[1]
            resolved = _resolve_kpi_name(ct_name, kn)
            matched = resolved is not None
            avg_val = _to_float(r[4])
            classification = None
            if matched and avg_val is not None:
                classification, _ = _classify(ct_name, resolved, avg_val)
            kpi_audit.append({
                "component_type": ct_name,
                "kpi_name": kn,
                "resolved_to": resolved,
                "rows": int(r[2] or 0),
                "min_value": _to_float(r[3]),
                "avg_value": avg_val,
                "max_value": _to_float(r[5]),
                "matched_in_thresholds": matched,
                "expected_kpi_names": list(KPI_RANGES.get(ct_name, {}).keys()) if not matched else None,
                "avg_band": classification,  # normal | degradation | outage | None
            })
    except Exception as e:
        _LOG.warning("coverage kpi_audit failed: %s", e)
    # Job log summary
    last_log = (CoreJobLog.query
                .filter(CoreJobLog.finished_at.isnot(None))
                .order_by(CoreJobLog.hour_date.desc(), CoreJobLog.hour_value.desc())
                .first())
    total_logs = CoreJobLog.query.count()
    finished_logs = CoreJobLog.query.filter(CoreJobLog.finished_at.isnot(None)).count()
    return jsonify({
        "rows_total": data_count,
        "min_date": data_min.isoformat() if data_min else None,
        "max_date": data_max.isoformat() if data_max else None,
        "by_type": per_type,
        "kpi_audit": kpi_audit,
        "expected_thresholds": KPI_RANGES,
        "last_log": {
            "hour_date": last_log.hour_date.isoformat() if last_log and last_log.hour_date else None,
            "hour_value": last_log.hour_value if last_log else None,
            "alerts_created": last_log.alerts_created if last_log else None,
            "tickets_created": last_log.tickets_created if last_log else None,
        } if last_log else None,
        "logs_total": total_logs,
        "logs_finished": finished_logs,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Admin: KPI Threshold Excel upload — operator-supplied normal/degrade/critical
# bands per (component_type, kpi_name). Each sheet name = component_type.
# Recognised columns (case- and whitespace-insensitive):
#   kpi_name | kpi
#   normal | normal_range | normal (industry std)         e.g. "99.80–99.95%"
#   degradation | degradation/action | degrade            e.g. "99.50–99.80%"
#   critical | critical_threshold | critical_concerning   e.g. "< 99.50%"
#   unit                                                  e.g. "%", "ms", "count"
#   direction (optional)                                  auto-detected if absent
# Numeric variants are also accepted:
#   normal_low/normal_high, degrade_low/degrade_high, critical_low/critical_high
# ─────────────────────────────────────────────────────────────────────────────
def _parse_band(raw: str) -> tuple[float | None, float | None, str | None]:
    """Parse a free-form band string like '99.80–99.95%', '≥ 99.90%', '< 99.50%',
    '> 80% sustained', '0.30–1.00%', 'Near-zero 0–5'.
    Returns (lower, upper, op) where op is one of '>=', '<=', '>', '<', 'range', None.
    """
    if raw is None:
        return None, None, None
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "—", "-", "n/a", "na"):
        return None, None, None
    # Normalise unicode operators and dashes
    s = (s.replace("≥", ">=").replace("≤", "<=")
           .replace("–", "-").replace("—", "-")
           .replace(" ", " "))
    # Strip trailing units / qualifiers like "%", "% UP", "ms", "(action)", "sustained", "avg"
    # We just care about numbers and operators; everything else is decoration.
    import re as _re2
    nums = _re2.findall(r"[-+]?\d*\.?\d+", s)
    nums = [float(n) for n in nums if n not in ("", "-", "+", ".")]
    op = None
    if ">=" in s:
        op = ">="
    elif "<=" in s:
        op = "<="
    elif ">" in s:
        op = ">"
    elif "<" in s:
        op = "<"
    elif len(nums) >= 2:
        op = "range"
    if not nums:
        return None, None, op
    if op == "range":
        return min(nums[:2]), max(nums[:2]), "range"
    if op in (">=", ">"):
        return nums[0], None, op
    if op in ("<=", "<"):
        return None, nums[0], op
    if len(nums) == 1:
        return nums[0], nums[0], None
    return min(nums[:2]), max(nums[:2]), "range"


def _infer_thresholds(normal_str, degrade_str, critical_str):
    """From the three free-form band strings, infer:
       direction, normal_low, normal_high, degrade_low, degrade_high,
       critical_low, critical_high.

    Logic:
      - If critical is "< X" or "<= X"  →  higher_is_better, critical_low = X
      - If critical is "> X" or ">= X"  →  lower_is_better,  critical_high = X
      - The normal band's bounds set normal_low/high accordingly.
    """
    n_lo, n_hi, n_op = _parse_band(normal_str)
    d_lo, d_hi, d_op = _parse_band(degrade_str)
    c_lo, c_hi, c_op = _parse_band(critical_str)

    direction = None
    if c_op in ("<", "<="):
        direction = "higher_is_better"
    elif c_op in (">", ">="):
        direction = "lower_is_better"
    elif n_op in ("<", "<="):
        direction = "lower_is_better"
    elif n_op in (">", ">="):
        direction = "higher_is_better"
    else:
        # Fall back to numeric ordering: if normal band is numerically higher
        # than the critical band, higher_is_better.
        n_ref = (n_lo or n_hi or 0)
        c_ref = (c_lo or c_hi or 0)
        if n_ref >= c_ref:
            direction = "higher_is_better"
        else:
            direction = "lower_is_better"

    out = {"direction": direction,
           "normal_low": n_lo, "normal_high": n_hi,
           "degrade_low": d_lo, "degrade_high": d_hi,
           "critical_low": None, "critical_high": None}

    if direction == "higher_is_better":
        # critical_low is the floor below which we declare outage
        if c_op in ("<", "<="):
            out["critical_low"] = c_lo if c_lo is not None else c_hi
        elif c_lo is not None or c_hi is not None:
            out["critical_low"] = c_hi if c_hi is not None else c_lo
        # normal_low = the lower bound of the normal band
        if n_lo is not None:
            out["normal_low"] = n_lo
        elif n_op in (">", ">=") and n_hi is None:
            # "≥ 99.90%" parsed as (lo=99.90, hi=None)
            out["normal_low"] = n_hi
    else:  # lower_is_better
        if c_op in (">", ">="):
            out["critical_high"] = c_hi if c_hi is not None else c_lo
        elif c_lo is not None or c_hi is not None:
            out["critical_high"] = c_lo if c_lo is not None else c_hi
        if n_hi is not None:
            out["normal_high"] = n_hi
        elif n_op in ("<", "<=") and n_lo is None:
            out["normal_high"] = n_hi or n_lo

    return out


@core_tickets_bp.route("/api/admin/upload-core-thresholds", methods=["POST"])
@jwt_required()
def upload_core_thresholds():
    """Upload an Excel workbook of KPI thresholds. One sheet per component.
    Replaces previous overrides for that component.
    """
    user = _user_or_none()
    if not user or user.role != "admin":
        return jsonify({"error": "Admin only"}), 403
    if "file" not in request.files:
        return jsonify({"error": "Missing file"}), 400
    f = request.files["file"]
    if not f or not f.filename:
        return jsonify({"error": "Empty file"}), 400
    try:
        import pandas as pd
        import uuid
    except Exception as e:
        return jsonify({"error": f"Pandas required: {e}"}), 500

    try:
        xls = pd.ExcelFile(f)
    except Exception as e:
        return jsonify({"error": f"Could not read Excel: {e}"}), 400

    upper_components = {c.upper(): c for c in COMPONENT_TYPES}
    batch = uuid.uuid4().hex[:24]
    summary = {}
    total_inserted = 0

    for sheet in xls.sheet_names:
        comp_key = (sheet or "").strip().upper()
        match = None
        for ck, cval in upper_components.items():
            if comp_key.startswith(ck):
                match = cval
                break
        if not match:
            summary[sheet] = {"skipped": "sheet name does not start with a component type"}
            continue

        df = pd.read_excel(xls, sheet_name=sheet)
        df.columns = [str(c).strip() for c in df.columns]
        col_lookup = {c.lower(): c for c in df.columns}

        def _col(*opts):
            for n in opts:
                if n in col_lookup:
                    return col_lookup[n]
            return None

        kpi_col = _col("kpi_name", "kpi name", "kpi", "kpiname")
        normal_col = _col("normal (industry std)", "normal industry std", "normal", "normal_range")
        degrade_col = _col("degradation / action", "degradation/action", "degradation", "degrade",
                           "degradation action", "degradationaction")
        critical_col = _col("critical threshold", "critical / concerning", "critical/concerning",
                            "critical_concerning", "critical concerning", "critical")
        unit_col = _col("unit", "units")
        direction_col = _col("direction")

        if not kpi_col:
            summary[match] = {"sheet": sheet, "skipped": "missing kpi_name column"}
            continue

        # Replace prior overrides for this component_type
        try:
            db.session.query(CoreKpiThreshold).filter_by(component_type=match).delete()
        except Exception:
            db.session.rollback()

        rows_added = 0
        unmatched = []
        for _, row in df.iterrows():
            kpi_v = str(row[kpi_col]).strip() if kpi_col and row[kpi_col] is not None else ""
            if not kpi_v or kpi_v.lower() == "nan":
                continue
            unit_v = (str(row[unit_col]).strip() if unit_col and row[unit_col] is not None else "") if unit_col else ""
            normal_v = row[normal_col] if normal_col else None
            degrade_v = row[degrade_col] if degrade_col else None
            critical_v = row[critical_col] if critical_col else None

            inferred = _infer_thresholds(normal_v, degrade_v, critical_v)
            if direction_col and row[direction_col]:
                d = str(row[direction_col]).strip().lower().replace(" ", "_")
                if d in ("higher_is_better", "higher", "high", "up"):
                    inferred["direction"] = "higher_is_better"
                elif d in ("lower_is_better", "lower", "low", "down"):
                    inferred["direction"] = "lower_is_better"

            canonical = _resolve_kpi_name(match, kpi_v) or kpi_v
            cfg_default = KPI_RANGES.get(match, {}).get(canonical, {})

            row_obj = CoreKpiThreshold(
                component_type=match,
                kpi_name=canonical,
                direction=inferred["direction"] or cfg_default.get("direction") or "higher_is_better",
                normal_low=inferred["normal_low"] if inferred["normal_low"] is not None else cfg_default.get("normal_low"),
                normal_high=inferred["normal_high"] if inferred["normal_high"] is not None else cfg_default.get("normal_high"),
                degrade_low=inferred["degrade_low"],
                degrade_high=inferred["degrade_high"],
                critical_low=inferred["critical_low"] if inferred["critical_low"] is not None else cfg_default.get("critical_low"),
                critical_high=inferred["critical_high"] if inferred["critical_high"] is not None else cfg_default.get("critical_high"),
                unit=unit_v or cfg_default.get("unit") or "",
                color=cfg_default.get("color") or "#64748b",
                upload_batch=batch,
            )
            db.session.add(row_obj)
            rows_added += 1
            if not KPI_RANGES.get(match, {}).get(canonical):
                unmatched.append(kpi_v)

        db.session.commit()
        total_inserted += rows_added
        summary[match] = {"sheet": sheet, "rows": rows_added, "unmatched_kpis": unmatched[:10]}

    _bust_threshold_cache()
    print(f"[CORE THRESHOLDS] Upload complete — {total_inserted} rows across {len(summary)} components")

    # Reconcile open tickets/alerts against the new thresholds — anything
    # that no longer breaches the critical band gets deleted automatically.
    try:
        rec = reconcile_open_tickets(dry_run=False)
    except Exception as _re:
        _LOG.warning("Auto-reconcile after threshold upload failed: %s", _re)
        rec = {"error": str(_re)}

    # Rebuild — wipe job log + force-replay every hour so tickets reflect
    # the new thresholds (otherwise catch-up sees old hours as "finished"
    # and skips them, leaving the recreated set empty).
    try:
        rebuild = rebuild_tickets_from_data()
    except Exception as _rbe:
        _LOG.warning("Auto-rebuild after threshold upload failed: %s", _rbe)
        rebuild = {"error": str(_rbe)}

    return jsonify({
        "success": True, "total_inserted": total_inserted,
        "by_component": summary, "batch": batch,
        "reconciliation": rec,
        "rebuild": rebuild,
    })


@core_tickets_bp.route("/api/admin/core-thresholds/status", methods=["GET"])
@jwt_required()
def core_thresholds_status():
    """List all uploaded thresholds (overrides). The frontend uses this to
    show what's active and to confirm uploads."""
    rows = (CoreKpiThreshold.query
            .order_by(CoreKpiThreshold.component_type, CoreKpiThreshold.kpi_name)
            .all())
    grouped = {}
    for r in rows:
        grouped.setdefault(r.component_type, []).append({
            "kpi_name": r.kpi_name,
            "direction": r.direction,
            "normal_low": r.normal_low, "normal_high": r.normal_high,
            "degrade_low": r.degrade_low, "degrade_high": r.degrade_high,
            "critical_low": r.critical_low, "critical_high": r.critical_high,
            "unit": r.unit,
        })
    return jsonify({"by_component": grouped, "total": len(rows)})


# ─────────────────────────────────────────────────────────────────────────────
# Admin: Core Parameter Excel upload
# ─────────────────────────────────────────────────────────────────────────────
@core_tickets_bp.route("/api/admin/upload-core-parameters", methods=["POST"])
@jwt_required()
def upload_core_parameters():
    """Upload parameters Excel.
    - Each sheet name (case-insensitive) must match a component type: MME / SGW / PGW / HSS / PCRF.
    - Required columns: kpi_name, parameter_group, parameter_name, unit
    - Component-id columns: any column whose header starts with the component type
      (e.g. MME1, MME2, …) holds the current value of that parameter for that component.
    Existing rows for that component_type are deleted and replaced.
    """
    user = _user_or_none()
    if not user or user.role != "admin":
        return jsonify({"error": "Admin only"}), 403

    if "file" not in request.files:
        return jsonify({"error": "Missing file field"}), 400
    f = request.files["file"]
    if not f or not f.filename:
        return jsonify({"error": "Empty file"}), 400

    try:
        import pandas as pd
        import uuid
    except Exception as e:
        return jsonify({"error": f"Pandas required: {e}"}), 500

    batch = uuid.uuid4().hex[:24]

    try:
        xls = pd.ExcelFile(f)
    except Exception as e:
        return jsonify({"error": f"Could not read Excel: {e}"}), 400

    upper_components = {c.upper(): c for c in COMPONENT_TYPES}
    summary = {}
    total_inserted = 0

    for sheet in xls.sheet_names:
        comp_key = (sheet or "").strip().upper()
        # tolerate variants like "mme1", "MME-Params" → take leading letters
        match = None
        for ck, cval in upper_components.items():
            if comp_key.startswith(ck):
                match = cval
                break
        if not match:
            continue

        df = pd.read_excel(xls, sheet_name=sheet)
        # Normalise column names (lower, strip, _)
        df.columns = [str(c).strip() for c in df.columns]
        col_lookup = {c.lower(): c for c in df.columns}

        def _col(name_options):
            for n in name_options:
                if n in col_lookup:
                    return col_lookup[n]
            return None

        kpi_col = _col(["kpi_name", "kpi", "kpiname", "kpi name"])
        group_col = _col(["parameter_group", "param_group", "group", "parameter group"])
        name_col = _col(["parameter_name", "param_name", "parameter", "parameter name"])
        unit_col = _col(["unit", "units"])

        if not name_col:
            summary[match] = {"sheet": sheet, "skipped": "missing parameter_name column"}
            continue

        # Component-id columns: any whose name starts with component prefix and isn't a fixed column
        fixed_cols = {kpi_col, group_col, name_col, unit_col}
        comp_cols = []
        for c in df.columns:
            if c in fixed_cols or c is None:
                continue
            up = c.upper()
            if up.startswith(match):
                comp_cols.append(c)
        # Fallback: if no component-id columns, treat any extra columns as values keyed by themselves
        if not comp_cols:
            comp_cols = [c for c in df.columns if c not in fixed_cols and c is not None]

        # Replace previous parameters for this component_type (atomic)
        try:
            db.session.query(CoreParameter).filter_by(component_type=match).delete()
        except Exception:
            db.session.rollback()

        rows_added = 0
        for _, row in df.iterrows():
            kpi_v = (str(row[kpi_col]).strip() if kpi_col and row[kpi_col] is not None else "") if kpi_col else ""
            grp_v = (str(row[group_col]).strip() if group_col and row[group_col] is not None else "") if group_col else ""
            nm_v  = str(row[name_col]).strip() if name_col else ""
            unit_v = (str(row[unit_col]).strip() if unit_col and row[unit_col] is not None else "") if unit_col else ""
            if not nm_v or nm_v.lower() == "nan":
                continue

            for cc in comp_cols:
                val = row[cc]
                # Skip empty cells
                if val is None or (isinstance(val, float) and (val != val)):  # NaN
                    continue
                try:
                    v_str = str(val).strip()
                except Exception:
                    continue
                if not v_str or v_str.lower() == "nan":
                    continue
                cp = CoreParameter(
                    component_type=match,
                    component_id=str(cc).strip(),
                    kpi_name=kpi_v or None,
                    parameter_group=grp_v or None,
                    parameter_name=nm_v,
                    current_value=v_str,
                    unit=unit_v or None,
                    upload_batch=batch,
                )
                db.session.add(cp)
                rows_added += 1

        db.session.commit()
        total_inserted += rows_added
        summary[match] = {"sheet": sheet, "rows": rows_added, "components": comp_cols}

    return jsonify({"success": True, "total_inserted": total_inserted, "by_component": summary, "batch": batch})


@core_tickets_bp.route("/api/admin/core-parameters/status", methods=["GET"])
@jwt_required()
def core_parameters_status():
    rows = (db.session.query(
                CoreParameter.component_type,
                db.func.count(CoreParameter.id),
                db.func.count(db.distinct(CoreParameter.component_id)),
                db.func.count(db.distinct(CoreParameter.parameter_name)),
            )
            .group_by(CoreParameter.component_type).all())
    return jsonify({
        "summary": [
            {
                "component_type": r[0],
                "total_rows": r[1],
                "component_ids": r[2],
                "unique_parameters": r[3],
            }
            for r in rows
        ]
    })


@core_tickets_bp.route("/api/admin/core-parameters", methods=["GET"])
@jwt_required()
def list_core_parameters():
    """Used by Change-Parameter dropdowns. Filters by component_type + component_id
    + (optional) kpi_name with progressive fallback so the UI never shows
    "no parameters" when data really does exist for the component:

      Tier 1: exact match on (component_type, component_id, kpi_name)
              — using fuzzy KPI-name normalisation so 'SCTP Status' matches
                'SCTP Association Status'
      Tier 2: (component_type, kpi_name) — same KPI on other instances
      Tier 3: (component_type, component_id) — every parameter for this instance
      Tier 4: (component_type) — every parameter for this component family

    Each row is annotated with `match_tier` so the frontend can flag wider matches.
    """
    component_type = (request.args.get("component_type") or "").upper().strip()
    component_id = (request.args.get("component_id") or "").strip()
    kpi_name = (request.args.get("kpi_name") or "").strip()

    if not component_type:
        return jsonify({"parameters": [], "match_tier": 0})

    # Case-insensitive component_type so 'mme', 'MME', 'Mme' all match
    base = CoreParameter.query.filter(
        db.func.upper(db.func.trim(CoreParameter.component_type)) == component_type
    )

    # Build a set of normalised kpi-name keys we'll accept as the "same" kpi.
    # Includes: the requested name itself, the canonical name, the requested
    # name normalised, and any synonym-table hits.
    kpi_match_keys = set()
    if kpi_name:
        kpi_match_keys.add(_norm_kpi(kpi_name))
        canonical = _resolve_kpi_name(component_type, kpi_name)
        if canonical:
            kpi_match_keys.add(_norm_kpi(canonical))
        # Also accept any uploaded kpi_name whose normalised form matches the
        # requested or canonical normalised form. Build that lookup once below.

    rows_for_type = base.all()

    def _kpi_matches(p) -> bool:
        if not kpi_match_keys or not p.kpi_name:
            return False
        n = _norm_kpi(p.kpi_name)
        if n in kpi_match_keys:
            return True
        # Also try resolving the stored kpi_name against the synonyms table
        resolved = _resolve_kpi_name(component_type, p.kpi_name)
        if resolved and _norm_kpi(resolved) in kpi_match_keys:
            return True
        return False

    cid_norm = _norm_kpi(component_id) if component_id else ""
    def _cid_matches(p) -> bool:
        if not component_id:
            return True
        return _norm_kpi(p.component_id or "") == cid_norm

    # Tier 1: same component_id + same kpi (fuzzy)
    tier1 = [p for p in rows_for_type if _cid_matches(p) and _kpi_matches(p)] if kpi_name else []
    # Tier 2: any component_id + same kpi (fuzzy)
    tier2 = [p for p in rows_for_type if _kpi_matches(p)] if kpi_name else []
    # Tier 3: same component_id, any kpi
    tier3 = [p for p in rows_for_type if _cid_matches(p)] if component_id else []
    # Tier 4: any param for this component_type
    tier4 = rows_for_type

    if tier1:
        chosen, tier_used = tier1, 1
    elif tier2:
        chosen, tier_used = tier2, 2
    elif tier3:
        chosen, tier_used = tier3, 3
    else:
        chosen, tier_used = tier4, 4

    chosen.sort(key=lambda p: (p.parameter_group or "", p.parameter_name or ""))
    out = []
    for p in chosen:
        out.append({
            "id": p.id,
            "component_type": p.component_type,
            "component_id": p.component_id,
            "kpi_name": p.kpi_name,
            "parameter_group": p.parameter_group,
            "parameter_name": p.parameter_name,
            "current_value": p.current_value,
            "unit": p.unit,
            "match_tier": tier_used,
        })
    return jsonify({
        "parameters": out,
        "match_tier": tier_used,
        "tier_label": {1: "exact (component+kpi)", 2: "same kpi, other instances",
                       3: "same instance, other kpis", 4: "component-wide fallback"}.get(tier_used, "none"),
        "total_for_component_type": len(rows_for_type),
    })


# ─── Core Parameter Change — request, list, approve / disapprove ──────────────
def _classify_cr_type(ticket_priority: str) -> tuple[str, bool]:
    """Map the source ticket priority to a CR change_type and whether CTO
    approval is required (matches the RF/worst-cell CR flow)."""
    p = (ticket_priority or "").lower()
    if p == "critical":
        return "emergency", True   # Critical → emergency, CTO required
    if p == "high":
        return "urgent", True      # High → urgent, CTO required
    if p == "medium":
        return "normal", False     # Medium → normal, manager only
    return "standard", False        # Low → standard, manager only


def _pick_manager() -> User | None:
    """Pick a manager to route the CR to. Prefers the least-loaded manager."""
    try:
        managers = User.query.filter(User.role == "manager").all()
    except Exception:
        return None
    if not managers:
        return None
    def load(m):
        try:
            return CoreParameterChange.query.filter(
                CoreParameterChange.assigned_manager_id == m.id,
                CoreParameterChange.status.in_(("pending", "pending_cto")),
            ).count()
        except Exception:
            return 0
    return sorted(managers, key=load)[0]


def _pick_cto() -> User | None:
    try:
        ctos = User.query.filter(User.role == "cto").all()
    except Exception:
        return None
    return ctos[0] if ctos else None


@core_tickets_bp.route("/api/core/parameter-change/preview-routing", methods=["GET"])
@jwt_required()
def preview_routing():
    """Tells the UI which manager (and CTO if applicable) the CR will be
    routed to before the agent submits."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    ticket_id = request.args.get("core_ticket_id")
    t = db.session.get(CoreTicket, int(ticket_id)) if ticket_id else None
    change_type, cto_required = _classify_cr_type(t.priority if t else "medium")
    mgr = _pick_manager()
    cto = _pick_cto() if cto_required else None
    return jsonify({
        "change_type": change_type,
        "cto_required": cto_required,
        "manager": {"id": mgr.id, "name": mgr.name, "email": mgr.email} if mgr else None,
        "cto": {"id": cto.id, "name": cto.name, "email": cto.email} if cto else None,
        "ticket_priority": t.priority if t else None,
    })


@core_tickets_bp.route("/api/core/parameter-change", methods=["POST"])
@jwt_required()
def submit_parameter_change():
    """Agent submits a parameter change request linked to a CoreTicket."""
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    body = request.json or {}
    required = ["core_ticket_id", "component_type", "component_id", "parameter_name", "proposed_value"]
    for k in required:
        if not body.get(k):
            return jsonify({"error": f"Missing {k}"}), 400
    t = db.session.get(CoreTicket, int(body["core_ticket_id"]))
    if not t:
        return jsonify({"error": "Ticket not found"}), 404

    change_type, cto_required = _classify_cr_type(t.priority)
    mgr = _pick_manager()
    cto = _pick_cto() if cto_required else None

    pc = CoreParameterChange(
        core_ticket_id=t.id,
        component_type=body["component_type"],
        component_id=body["component_id"],
        parameter_group=(body.get("parameter_group") or "")[:120] or None,
        parameter_name=str(body["parameter_name"])[:200],
        current_value=(str(body.get("current_value") or "")[:120]) or None,
        proposed_value=str(body["proposed_value"])[:120],
        unit=(body.get("unit") or "")[:40] or None,
        reason=body.get("reason") or "",
        agent_id=user.id,
        change_type=change_type,
        cto_required=cto_required,
        assigned_manager_id=mgr.id if mgr else None,
        assigned_cto_id=cto.id if cto else None,
        status="pending",
    )
    db.session.add(pc)
    db.session.commit()
    print(f"[CORE CR] Submitted #{pc.id} — {pc.parameter_name} ({change_type}) "
          f"on {t.reference_number} → manager={mgr.name if mgr else '—'} "
          f"cto_required={cto_required}")
    _LOG.info("[CORE CR] Submitted %d type=%s manager=%s cto_required=%s",
              pc.id, change_type, mgr.name if mgr else "-", cto_required)
    return jsonify({
        "success": True,
        "id": pc.id,
        "change_type": change_type,
        "cto_required": cto_required,
        "manager": {"name": mgr.name, "email": mgr.email} if mgr else None,
        "cto": {"name": cto.name, "email": cto.email} if cto else None,
    })


@core_tickets_bp.route("/api/core/parameter-change", methods=["GET"])
@jwt_required()
def list_parameter_changes():
    """List CR requests:
       - manager/cto/admin: see all
       - human_agent: see only their own
    """
    user = _user_or_none()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    q = CoreParameterChange.query
    if user.role == "human_agent":
        q = q.filter(CoreParameterChange.agent_id == user.id)
    status = request.args.get("status")
    if status:
        q = q.filter(CoreParameterChange.status == status)
    q = q.order_by(CoreParameterChange.created_at.desc()).limit(500)

    out = []
    for pc in q.all():
        ag = db.session.get(User, pc.agent_id)
        rev = db.session.get(User, pc.reviewed_by) if pc.reviewed_by else None
        t = db.session.get(CoreTicket, pc.core_ticket_id)
        mgr = db.session.get(User, pc.assigned_manager_id) if pc.assigned_manager_id else None
        cto = db.session.get(User, pc.assigned_cto_id) if pc.assigned_cto_id else None
        out.append({
            "id": pc.id,
            "core_ticket_id": pc.core_ticket_id,
            "ticket_reference": t.reference_number if t else "",
            "ticket_kpi": t.kpi_name if t else "",
            "ticket_priority": t.priority if t else "",
            "component_type": pc.component_type,
            "component_id": pc.component_id,
            "parameter_group": pc.parameter_group,
            "parameter_name": pc.parameter_name,
            "current_value": pc.current_value,
            "proposed_value": pc.proposed_value,
            "unit": pc.unit,
            "reason": pc.reason,
            "agent_id": pc.agent_id,
            "agent_name": ag.name if ag else "",
            "status": pc.status,
            "change_type": pc.change_type or "standard",
            "cto_required": bool(pc.cto_required),
            "manager_id": pc.assigned_manager_id,
            "manager_name": mgr.name if mgr else "",
            "manager_email": mgr.email if mgr else "",
            "cto_id": pc.assigned_cto_id,
            "cto_name": cto.name if cto else "",
            "cto_email": cto.email if cto else "",
            "manager_note": pc.manager_note,
            "cto_note": pc.cto_note,
            "reviewed_by_name": rev.name if rev else "",
            "reviewed_at": pc.reviewed_at.isoformat() if pc.reviewed_at else None,
            "created_at": pc.created_at.isoformat() if pc.created_at else None,
        })
    return jsonify({"requests": out})


@core_tickets_bp.route("/api/core/parameter-change/<int:cr_id>/decision", methods=["POST"])
@jwt_required()
def decide_parameter_change(cr_id):
    user = _user_or_none()
    if not user or user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403
    pc = db.session.get(CoreParameterChange, cr_id)
    if not pc:
        return jsonify({"error": "Not found"}), 404
    body = request.json or {}
    decision = (body.get("decision") or "").lower()  # approve|disapprove
    note = body.get("note", "")
    if decision not in ("approve", "disapprove"):
        return jsonify({"error": "Invalid decision"}), 400

    is_manager = user.role in ("manager", "admin")
    is_cto = user.role == "cto"

    # ── Manager step ──────────────────────────────────────────────────────
    if is_manager:
        pc.manager_note = note
        if decision == "disapprove":
            pc.status = "disapproved"
        else:
            # If CTO approval is required, route to CTO; otherwise final-approve.
            if pc.cto_required:
                pc.status = "pending_cto"
                if not pc.assigned_cto_id:
                    cto = _pick_cto()
                    pc.assigned_cto_id = cto.id if cto else None
                print(f"[CORE CR] Manager approved #{pc.id}; routed to CTO "
                      f"(cto_id={pc.assigned_cto_id})")
                _LOG.info("[CORE CR] Manager approved %d → pending_cto", pc.id)
            else:
                pc.status = "approved"
                print(f"[CORE CR] Manager approved #{pc.id} (no CTO required)")
                _LOG.info("[CORE CR] Manager approved %d", pc.id)
    # ── CTO step ──────────────────────────────────────────────────────────
    elif is_cto:
        # CTO can only act when a manager has already approved a CR that
        # needs CTO sign-off.
        if pc.status != "pending_cto":
            return jsonify({"error": "Not awaiting CTO decision"}), 400
        pc.cto_note = note
        if decision == "approve":
            pc.status = "cto_approved"
            print(f"[CORE CR] CTO approved #{pc.id}")
            _LOG.info("[CORE CR] CTO approved %d", pc.id)
        else:
            pc.status = "cto_rejected"
            print(f"[CORE CR] CTO rejected #{pc.id}")
            _LOG.info("[CORE CR] CTO rejected %d", pc.id)
    pc.reviewed_by = user.id
    pc.reviewed_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"success": True, "status": pc.status})


# ─────────────────────────────────────────────────────────────────────────────
# Table creation helper
# ─────────────────────────────────────────────────────────────────────────────
def ensure_tables(app):
    with app.app_context():
        for m in (CoreAlert, CoreTicket, CoreParameter, CoreParameterChange, CoreJobLog, CoreKpiThreshold):
            try:
                m.__table__.create(bind=db.engine, checkfirst=True)
            except Exception as e:
                _LOG.warning("Failed to create %s: %s", m.__tablename__, e)
        # ── In-place migrations for CoreParameterChange (new CR fields) ──
        try:
            insp = sa_inspect(db.engine)
            if insp.has_table("core_parameter_changes"):
                cols = {c["name"] for c in insp.get_columns("core_parameter_changes")}
                add = [
                    ("change_type",         "VARCHAR(20) DEFAULT 'standard'"),
                    ("cto_required",        "BOOLEAN DEFAULT FALSE"),
                    ("assigned_manager_id", "INTEGER"),
                    ("assigned_cto_id",     "INTEGER"),
                ]
                with db.engine.connect() as conn:
                    for name, ddl in add:
                        if name not in cols:
                            try:
                                conn.execute(sa_text(f"ALTER TABLE core_parameter_changes ADD COLUMN {name} {ddl}"))
                                conn.commit()
                                _LOG.info("Added column %s to core_parameter_changes", name)
                            except Exception as e:
                                _LOG.warning("Migration add %s failed: %s", name, e)
        except Exception as me:
            _LOG.warning("CoreParameterChange migration probe failed: %s", me)
