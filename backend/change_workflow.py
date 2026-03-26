"""
Change Workflow Blueprint — ITIL-aligned Change Request lifecycle.

Endpoints for creating, classifying, validating, approving, implementing,
and closing Change Requests (CRs) across Agent, Manager, and CTO roles.
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text as sa_text
from werkzeug.utils import secure_filename
from models import db, User, ChangeRequest, CRAuditTrail, ParameterChange

_LOG = logging.getLogger("change_workflow")

change_workflow_bp = Blueprint("change_workflow", __name__)

# ─── Upload directory ─────────────────────────────────────────────────────────
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads", "cr_pdfs")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ─── Category / Subcategory / Domain Mapping ──────────────────────────────────
CATEGORY_SUBCATEGORY_MAP = {
    "Mobile Services (Prepaid / Postpaid)": {
        "subcategories": [
            "Network / Signal Problems", "Call Drop Issues", "Data Speed Issues",
            "SIM Card Issues", "Roaming Issues", "Plan / Pack Related",
            "VAS / Value Added Services", "Number Portability",
        ],
        "primary_domain": "RAN",
        "secondary_domains": ["Core", "Transport"],
    },
    "Broadband / Internet Services": {
        "subcategories": [
            "Slow Speed / No Connectivity", "Modem / Router Issues",
            "Frequent Disconnection", "Installation / Activation Delay",
            "Plan Upgrade / Downgrade", "Wi-Fi Coverage Issues",
        ],
        "primary_domain": "Transport",
        "secondary_domains": ["Core", "RAN"],
    },
    "DTH / Cable TV Services": {
        "subcategories": [
            "No Signal / Channel Missing", "Picture / Audio Quality Issues",
            "Set-Top Box Malfunction", "Channel Pack / Subscription Issues",
            "Recording / DVR Problems", "Signal Interference",
        ],
        "primary_domain": "Transmission",
        "secondary_domains": ["Core"],
    },
    "Landline / Fixed Line Services": {
        "subcategories": [
            "No Dial Tone / Dead Line", "Call Quality / Noise Issues",
            "Caller ID / Call Features", "Billing / Usage Disputes",
            "New Connection / Transfer", "Cable / Infrastructure Damage",
        ],
        "primary_domain": "Core",
        "secondary_domains": ["Transport", "Transmission"],
    },
    "Enterprise / Business Solutions": {
        "subcategories": [
            "Dedicated Line / Leased Line Issues", "SLA Breach / Downtime",
            "Cloud / VPN / MPLS Issues", "Bulk SMS / API Issues",
            "Video Conferencing / UC Issues", "Managed Services Support",
        ],
        "primary_domain": "Transport",
        "secondary_domains": ["Core", "IMS"],
    },
    "Network Performance (AI Detected)": {
        "subcategories": [
            "Worst Cell / Drop Rate Degradation",
            "CSSR Failure / Access Issues",
            "Throughput Degradation / Low SINR",
            "RF Parameter Optimization",
            "Capacity Exhaustion / High RRC",
            "Inter-Cell Interference / Pilot Pollution",
        ],
        "primary_domain": "RAN",
        "secondary_domains": ["Core", "Transport"],
    },
}

# SLA percentage of parent ticket remaining time per classification
SLA_PCT = {"standard": 0.30, "normal": 0.20, "urgent": 0.10, "emergency": 0.10}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _generate_pcr_number():
    """Generate sequential PCR-XXXX number."""
    count = db.session.query(db.func.count(ChangeRequest.id)).scalar() or 0
    return f"PCR-{count + 1:04d}"


def _record_audit(cr_id, action, user_id, old_status, new_status, notes=""):
    """Record an audit trail entry."""
    entry = CRAuditTrail(
        cr_id=cr_id, action=action, performed_by=user_id,
        old_status=old_status, new_status=new_status, notes=notes,
    )
    db.session.add(entry)


def _get_ticket_sla_remaining(cr):
    """Get remaining SLA hours from parent ticket or network issue."""
    now = datetime.utcnow()  # naive UTC to match DB datetimes
    if cr.ticket_id:
        from models import Ticket
        ticket = db.session.get(Ticket, cr.ticket_id)
        if ticket and ticket.sla_deadline:
            dl = ticket.sla_deadline.replace(tzinfo=None) if ticket.sla_deadline.tzinfo else ticket.sla_deadline
            rem = (dl - now).total_seconds() / 3600
            return max(rem, 1)
    if cr.network_issue_id:
        from network_issues import NetworkIssueTicket
        ni = db.session.get(NetworkIssueTicket, cr.network_issue_id)
        if ni and ni.deadline_time:
            dl = ni.deadline_time.replace(tzinfo=None) if ni.deadline_time.tzinfo else ni.deadline_time
            rem = (dl - now).total_seconds() / 3600
            return max(rem, 1)
    return 8  # default 8 hours if no parent


def _get_user():
    """Get current authenticated user."""
    user_id = int(get_jwt_identity())
    return db.session.get(User, user_id), user_id


def _get_rf_params(site_id):
    """Fetch current RF parameters for a site from telecom_sites."""
    try:
        row = db.session.execute(sa_text("""
            SELECT AVG(bandwidth_mhz) AS bw, AVG(antenna_gain_dbi) AS gain,
                   AVG(rf_power_eirp_dbm) AS eirp, AVG(e_tilt_degree) AS tilt,
                   AVG(crs_gain) AS crs, AVG(antenna_height_agl_m) AS height
            FROM telecom_sites WHERE site_id = :sid
        """), {"sid": site_id}).mappings().first()
        if row:
            return {
                "bandwidth": float(row["bw"]) if row["bw"] else None,
                "antenna_gain": float(row["gain"]) if row["gain"] else None,
                "eirp": float(row["eirp"]) if row["eirp"] else None,
                "etilt": float(row["tilt"]) if row["tilt"] else None,
                "crs_gain": float(row["crs"]) if row["crs"] else None,
                "antenna_height": float(row["height"]) if row["height"] else None,
            }
    except Exception as e:
        _LOG.warning("RF param fetch failed for %s: %s", site_id, e)
    return {}


# ─── Endpoints ────────────────────────────────────────────────────────────────

@change_workflow_bp.route("/api/cr/categories", methods=["GET"])
@jwt_required()
def get_categories():
    """Return category/subcategory/domain mapping for form dropdowns."""
    return jsonify({"categories": CATEGORY_SUBCATEGORY_MAP})


@change_workflow_bp.route("/api/cr/cto-info", methods=["GET"])
@jwt_required()
def get_cto_info():
    """Return first CTO user's name and email for display."""
    cto = User.query.filter_by(role="cto").first()
    if cto:
        return jsonify({"cto_name": cto.name, "cto_email": cto.email or ""})
    return jsonify({"cto_name": "CTO", "cto_email": ""})


@change_workflow_bp.route("/api/cr/site-rf-params", methods=["GET"])
@jwt_required()
def get_site_rf_params():
    """Fetch current RF params for a site."""
    site_id = request.args.get("site_id", "")
    if not site_id:
        return jsonify({"error": "site_id required"}), 400
    params = _get_rf_params(site_id)
    return jsonify({"site_id": site_id, "params": params})


@change_workflow_bp.route("/api/cr/create", methods=["POST"])
@jwt_required()
def create_cr():
    """Agent creates a new Change Request with full form data."""
    print("[CR CREATE] Endpoint hit")
    user, user_id = _get_user()
    if not user or user.role != "human_agent":
        return jsonify({"error": "Unauthorized"}), 403

    try:
        data = request.json or {}
        print(f"[CR CREATE] User={user_id}, category={data.get('category')}")

        # Required fields
        title = (data.get("title") or "Parameter Change Request").strip()
        change_type = (data.get("change_type") or "standard").lower()
        if change_type not in SLA_PCT:
            return jsonify({"error": "change_type must be standard/normal/urgent/emergency"}), 400

        # Domain mapping
        category = data.get("category", "")
        subcategory = data.get("subcategory", "")
        cat_info = CATEGORY_SUBCATEGORY_MAP.get(category, {})
        primary_domain = cat_info.get("primary_domain", data.get("telecom_domain_primary", ""))
        secondary_domains = ",".join(cat_info.get("secondary_domains", []))

        # Generate CR number
        cr_number = _generate_pcr_number()

        # Safe float parser
        def _sf(key):
            v = data.get(key)
            if v is None: return None
            try: return float(v)
            except (ValueError, TypeError): return None

        # Build CR — ticket_id/network_issue_id must be int or None
        ticket_id = data.get("ticket_id")
        network_issue_id = data.get("network_issue_id")
        if ticket_id is not None:
            try: ticket_id = int(ticket_id)
            except (ValueError, TypeError): ticket_id = None
        if network_issue_id is not None:
            try: network_issue_id = int(network_issue_id)
            except (ValueError, TypeError): network_issue_id = None

        cr = ChangeRequest(
            cr_number=cr_number,
            ticket_id=ticket_id,
            network_issue_id=network_issue_id,
            raised_by=user_id,
            title=title,
            description=data.get("description") or data.get("justification") or "",
            justification=data.get("justification", ""),
            impact_assessment=data.get("impact_assessment", ""),
            rollback_plan=data.get("rollback_plan", ""),
            category=category,
            subcategory=subcategory,
            telecom_domain_primary=primary_domain,
            telecom_domain_secondary=secondary_domains,
            zone=data.get("zone", ""),
            location=data.get("location", ""),
            nearest_site_id=data.get("nearest_site_id", ""),
            customer_type=data.get("customer_type", ""),
            # RF Parameters (safely parsed)
            rf_bandwidth_current=_sf("rf_bandwidth_current"),
            rf_bandwidth_proposed=_sf("rf_bandwidth_proposed"),
            rf_antenna_gain_current=_sf("rf_antenna_gain_current"),
            rf_antenna_gain_proposed=_sf("rf_antenna_gain_proposed"),
            rf_eirp_current=_sf("rf_eirp_current"),
            rf_eirp_proposed=_sf("rf_eirp_proposed"),
            rf_antenna_height_current=_sf("rf_antenna_height_current"),
            rf_antenna_height_proposed=_sf("rf_antenna_height_proposed"),
            rf_etilt_current=_sf("rf_etilt_current"),
            rf_etilt_proposed=_sf("rf_etilt_proposed"),
            rf_crs_gain_current=_sf("rf_crs_gain_current"),
            rf_crs_gain_proposed=_sf("rf_crs_gain_proposed"),
            status="created",
            change_type=change_type,
        )

        db.session.add(cr)
        db.session.flush()  # get cr.id
        print(f"[CR CREATE] CR {cr.cr_number} flushed, id={cr.id}")

        # Calculate SLA (use naive UTC to match DB)
        parent_sla_remaining = _get_ticket_sla_remaining(cr)
        print(f"[CR CREATE] SLA remaining: {parent_sla_remaining}h")
        pct = SLA_PCT.get(change_type, 0.30)
        cr_sla_hours = max(round(parent_sla_remaining * pct, 2), 0.5)
        cr.cr_sla_hours = cr_sla_hours
        cr.cr_sla_deadline = datetime.utcnow() + timedelta(hours=cr_sla_hours)

        # Assign manager — use simple query to avoid deadlock during flush
        print("[CR CREATE] Assigning manager...")
        try:
            mgr = User.query.filter_by(role="manager").first()
            if mgr:
                cr.assigned_manager_id = mgr.id
                print(f"[CR CREATE] Assigned to manager: {mgr.name}")
            else:
                print("[CR CREATE] No manager found")
        except Exception as e:
            print(f"[CR CREATE] Manager routing failed: {e}")

        # Audit trail
        _record_audit(cr.id, "created", user_id, "", "created", "CR raised — pending classification")
        print("[CR CREATE] Committing...")

        db.session.commit()
        print("[CR CREATE] Committed! Serializing...")

        result = cr.to_dict()
        print(f"[CR CREATE] Done! CR={cr.cr_number}")

        return jsonify({
            "cr": result,
            "message": f"Change Request {cr_number} created successfully",
        }), 201

    except Exception as e:
        db.session.rollback()
        _LOG.error("CR creation failed: %s", e, exc_info=True)
        return jsonify({"error": f"CR creation failed: {str(e)}"}), 500


@change_workflow_bp.route("/api/cr/<int:cr_id>/upload-pdf", methods=["POST"])
@jwt_required()
def upload_pdf(cr_id):
    """Upload a supporting PDF document for a CR."""
    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "CR not found"}), 404

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400

    filename = secure_filename(f"{cr.cr_number}_{f.filename}")
    path = os.path.join(UPLOAD_DIR, filename)
    f.save(path)

    cr.pdf_filename = filename
    cr.pdf_path = path
    cr.updated_at = datetime.now(timezone.utc)
    db.session.commit()

    return jsonify({"filename": filename, "message": "PDF uploaded"})


@change_workflow_bp.route("/api/cr/<int:cr_id>/pdf", methods=["GET"])
@jwt_required()
def download_pdf(cr_id):
    """Download the PDF attached to a CR."""
    cr = db.session.get(ChangeRequest, cr_id)
    if not cr or not cr.pdf_path or not os.path.exists(cr.pdf_path):
        return jsonify({"error": "PDF not found"}), 404
    return send_file(cr.pdf_path, as_attachment=True, download_name=cr.pdf_filename)


@change_workflow_bp.route("/api/cr/<int:cr_id>/classify", methods=["POST"])
@jwt_required()
def classify_cr(cr_id):
    """Agent classifies a CR as standard/normal/urgent/emergency (phase 2 after form submit)."""
    user, user_id = _get_user()
    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    if cr.status != "created":
        return jsonify({"error": f"Cannot classify CR in status '{cr.status}'"}), 409

    data = request.json or {}
    change_type = (data.get("change_type") or "standard").lower()
    if change_type not in SLA_PCT:
        return jsonify({"error": "change_type must be standard/normal/urgent/emergency"}), 400

    old_status = cr.status
    cr.change_type = change_type
    cr.status = "classified"
    cr.classified_by = user_id
    cr.classified_at = datetime.now(timezone.utc)

    # Recalculate SLA based on chosen classification
    parent_sla_remaining = _get_ticket_sla_remaining(cr)
    pct = SLA_PCT[change_type]
    cr_sla_hours = max(round(parent_sla_remaining * pct, 2), 0.5)
    cr.cr_sla_hours = cr_sla_hours
    cr.cr_sla_deadline = datetime.now(timezone.utc) + timedelta(hours=cr_sla_hours)
    cr.updated_at = datetime.now(timezone.utc)

    _record_audit(cr.id, "classified", user_id, old_status, "classified", f"Classified as {change_type}")
    db.session.commit()

    return jsonify({"cr": cr.to_dict()})


@change_workflow_bp.route("/api/cr/agent-list", methods=["GET"])
@jwt_required()
def agent_list():
    """List CRs raised by current agent."""
    user, user_id = _get_user()
    if not user:
        return jsonify({"error": "Unauthorized"}), 403
    crs = ChangeRequest.query.filter_by(raised_by=user_id).order_by(ChangeRequest.created_at.desc()).all()
    return jsonify({"crs": [c.to_dict() for c in crs], "total": len(crs)})


@change_workflow_bp.route("/api/cr/manager-list", methods=["GET"])
@jwt_required()
def manager_list():
    """List CRs assigned to current manager."""
    user, user_id = _get_user()
    if not user or user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    status_filter = request.args.get("status")
    q = ChangeRequest.query.filter_by(assigned_manager_id=user_id)
    if status_filter == "needs_action":
        q = q.filter(ChangeRequest.status.in_(["classified", "invalid", "validated", "implemented", "rolled_back"]))
    elif status_filter:
        q = q.filter_by(status=status_filter)

    crs = q.order_by(ChangeRequest.created_at.desc()).all()

    # Stats
    all_mgr = ChangeRequest.query.filter_by(assigned_manager_id=user_id).all()
    stats = {
        "total": len(all_mgr),
        "needs_action": sum(1 for c in all_mgr if c.status in ("classified", "invalid", "validated", "implemented", "rolled_back")),
        "approved": sum(1 for c in all_mgr if c.status in ("approved", "pending_cto", "cto_approved")),
        "closed": sum(1 for c in all_mgr if c.status == "closed"),
    }

    return jsonify({"crs": [c.to_dict() for c in crs], "stats": stats, "total": len(crs)})


@change_workflow_bp.route("/api/cr/cto-list", methods=["GET"])
@jwt_required()
def cto_list():
    """List CRs requiring CTO approval (urgent + emergency only)."""
    user, user_id = _get_user()
    if not user or user.role not in ("cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    crs = ChangeRequest.query.filter(
        ChangeRequest.change_type.in_(["urgent", "emergency"]),
        ChangeRequest.cto_approval_required == True,
    ).order_by(ChangeRequest.created_at.desc()).all()

    return jsonify({"crs": [c.to_dict() for c in crs], "total": len(crs)})


@change_workflow_bp.route("/api/cr/<int:cr_id>", methods=["GET"])
@jwt_required()
def get_cr_detail(cr_id):
    """Get full CR detail."""
    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    entries = CRAuditTrail.query.filter_by(cr_id=cr_id).order_by(CRAuditTrail.created_at).all()
    result = cr.to_dict()
    result["audit_trail"] = [e.to_dict() for e in entries]
    return jsonify(result)


@change_workflow_bp.route("/api/cr/<int:cr_id>/audit-trail", methods=["GET"])
@jwt_required()
def get_audit_trail(cr_id):
    """Get audit trail for a CR."""
    entries = CRAuditTrail.query.filter_by(cr_id=cr_id).order_by(CRAuditTrail.created_at).all()
    return jsonify({"entries": [e.to_dict() for e in entries]})


@change_workflow_bp.route("/api/cr/<int:cr_id>/validate", methods=["PUT"])
@jwt_required()
def validate_cr(cr_id):
    """Manager validates or invalidates a CR."""
    user, user_id = _get_user()
    if not user or user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    if cr.status not in ("classified", "invalid", "created"):
        return jsonify({"error": f"Cannot validate CR in status '{cr.status}'"}), 409

    data = request.json or {}
    decision = (data.get("decision") or "").lower()
    remark = (data.get("remark") or "").strip()

    if decision not in ("valid", "invalid"):
        return jsonify({"error": "decision must be 'valid' or 'invalid'"}), 400

    old_status = cr.status

    if decision == "valid":
        cr.status = "validated"
        cr.validation_remark = remark
        cr.validated_by = user_id
        cr.validated_at = datetime.now(timezone.utc)
        _record_audit(cr.id, "validated", user_id, old_status, "validated", remark)
    else:
        cr.rejection_count = (cr.rejection_count or 0) + 1
        cr.validation_remark = remark
        cr.validated_by = user_id
        cr.validated_at = datetime.now(timezone.utc)
        if cr.rejection_count >= 2:
            cr.status = "auto_rejected"
            _record_audit(cr.id, "auto_rejected", user_id, old_status, "auto_rejected", f"Auto-rejected after {cr.rejection_count} validations. {remark}")
        else:
            cr.status = "invalid"
            _record_audit(cr.id, "invalid", user_id, old_status, "invalid", remark)

    cr.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"cr": cr.to_dict()})


@change_workflow_bp.route("/api/cr/<int:cr_id>/approve", methods=["PUT"])
@jwt_required()
def approve_cr(cr_id):
    """Manager approves, rejects, or modifies a CR."""
    user, user_id = _get_user()
    if not user or user.role not in ("manager", "cto", "admin"):
        return jsonify({"error": "Unauthorized"}), 403

    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    if cr.status != "validated":
        return jsonify({"error": f"Cannot approve CR in status '{cr.status}'"}), 409

    data = request.json or {}
    decision = (data.get("decision") or "").lower()
    remark = (data.get("remark") or "").strip()

    if decision not in ("approved", "rejected", "modified"):
        return jsonify({"error": "decision must be 'approved', 'rejected', or 'modified'"}), 400

    old_status = cr.status
    cr.approval_remark = remark
    cr.approved_by = user_id
    cr.approved_at = datetime.now(timezone.utc)

    if decision == "rejected":
        cr.status = "rejected"
        _record_audit(cr.id, "rejected", user_id, old_status, "rejected", remark)
    elif decision == "modified":
        cr.manager_proposed_changes = (data.get("proposed_changes") or "").strip()
        # Modified approval: still approves but with proposed changes
        if cr.change_type in ("urgent", "emergency"):
            cr.status = "pending_cto"
            cr.cto_approval_required = True
            _record_audit(cr.id, "approved_with_modifications", user_id, old_status, "pending_cto", f"Approved with modifications, escalated to CTO. {remark}")
        else:
            cr.status = "approved"
            _record_audit(cr.id, "approved_with_modifications", user_id, old_status, "approved", f"Approved with modifications. {remark}")
    else:  # approved
        if cr.change_type in ("urgent", "emergency"):
            cr.status = "pending_cto"
            cr.cto_approval_required = True
            _record_audit(cr.id, "approved", user_id, old_status, "pending_cto", f"Manager approved, escalated to CTO. {remark}")
        else:
            cr.status = "approved"
            _record_audit(cr.id, "approved", user_id, old_status, "approved", remark)

    cr.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"cr": cr.to_dict()})


@change_workflow_bp.route("/api/cr/<int:cr_id>/cto-approve", methods=["PUT"])
@jwt_required()
def cto_approve_cr(cr_id):
    """CTO approves or rejects a CR."""
    user, user_id = _get_user()
    if not user or user.role not in ("cto", "admin"):
        return jsonify({"error": "Unauthorized — CTO role required"}), 403

    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    if cr.status != "pending_cto":
        return jsonify({"error": f"Cannot CTO-review CR in status '{cr.status}'"}), 409

    data = request.json or {}
    decision = (data.get("decision") or "").lower()
    remark = (data.get("remark") or "").strip()

    if decision not in ("approved", "rejected"):
        return jsonify({"error": "decision must be 'approved' or 'rejected'"}), 400

    old_status = cr.status
    cr.cto_approved_by = user_id
    cr.cto_approved_at = datetime.now(timezone.utc)
    cr.cto_remark = remark

    if decision == "approved":
        cr.status = "cto_approved"
        cr.cto_status = "cto_approved"
        _record_audit(cr.id, "cto_approved", user_id, old_status, "cto_approved", remark)
    else:
        cr.status = "cto_rejected"
        cr.cto_status = "cto_rejected"
        _record_audit(cr.id, "cto_rejected", user_id, old_status, "cto_rejected", remark)

    cr.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"cr": cr.to_dict()})


@change_workflow_bp.route("/api/cr/<int:cr_id>/implement", methods=["PUT"])
@jwt_required()
def implement_cr(cr_id):
    """Agent marks CR as implemented."""
    user, user_id = _get_user()
    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    if cr.status not in ("approved", "cto_approved"):
        return jsonify({"error": f"Cannot implement CR in status '{cr.status}'"}), 409

    data = request.json or {}
    notes = (data.get("notes") or "").strip()

    old_status = cr.status
    cr.status = "implemented"
    cr.implementation_notes = notes
    cr.implemented_by = user_id
    cr.implemented_at = datetime.now(timezone.utc)
    cr.updated_at = datetime.now(timezone.utc)

    _record_audit(cr.id, "implemented", user_id, old_status, "implemented", notes)
    db.session.commit()
    return jsonify({"cr": cr.to_dict()})


@change_workflow_bp.route("/api/cr/<int:cr_id>/close", methods=["PUT"])
@jwt_required()
def close_cr(cr_id):
    """Agent or Manager closes a CR."""
    user, user_id = _get_user()
    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    if cr.status not in ("implemented", "rolled_back"):
        return jsonify({"error": f"Cannot close CR in status '{cr.status}'"}), 409

    data = request.json or {}
    notes = (data.get("notes") or "").strip()

    old_status = cr.status
    cr.status = "closed"
    cr.closure_notes = notes
    cr.closed_at = datetime.now(timezone.utc)
    cr.updated_at = datetime.now(timezone.utc)

    _record_audit(cr.id, "closed", user_id, old_status, "closed", notes)
    db.session.commit()
    return jsonify({"cr": cr.to_dict()})


@change_workflow_bp.route("/api/cr/<int:cr_id>/resubmit", methods=["PUT"])
@jwt_required()
def resubmit_cr(cr_id):
    """Agent resubmits a CR after it was marked invalid or CTO-rejected."""
    user, user_id = _get_user()
    cr = db.session.get(ChangeRequest, cr_id)
    if not cr:
        return jsonify({"error": "Not found"}), 404
    if cr.status not in ("invalid", "cto_rejected"):
        return jsonify({"error": f"Cannot resubmit CR in status '{cr.status}'"}), 409

    data = request.json or {}
    justification = (data.get("justification") or "").strip()
    if justification:
        cr.justification = justification

    old_status = cr.status
    cr.status = "classified"
    cr.updated_at = datetime.now(timezone.utc)

    _record_audit(cr.id, "resubmitted", user_id, old_status, "classified", justification or "Resubmitted for review")
    db.session.commit()
    return jsonify({"cr": cr.to_dict()})
