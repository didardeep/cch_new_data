"""
auth_utils.py — Reusable JWT authentication decorators
=======================================================
Provides two decorators that replace inline role checks across all routes:
  - @token_required   : Validates JWT, attaches user to request context
  - @role_required(*) : Restricts access to specific roles

Usage:
    from auth_utils import token_required, role_required

    @app.route("/api/admin/users")
    @token_required
    def list_users():
        user = request.current_user  # attached by decorator
        ...

    @app.route("/api/admin/dashboard")
    @role_required("admin")
    def admin_dashboard():
        user = request.current_user
        ...

    @app.route("/api/manager/tickets")
    @role_required("manager", "cto", "admin")
    def manager_tickets():
        ...
"""

from functools import wraps
from flask import request, jsonify
from flask_jwt_extended import decode_token
from models import db, User


def _extract_and_validate_token():
    """
    Extract JWT from Authorization header, decode it, and return the User.
    Returns (user, None) on success or (None, error_response) on failure.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None, (jsonify({"error": "Missing or invalid Authorization header"}), 401)

    token = auth_header.split(" ", 1)[1]
    try:
        decoded = decode_token(token)
    except Exception:
        return None, (jsonify({"error": "Invalid or expired token"}), 401)

    user_id = decoded.get("sub")
    if not user_id:
        return None, (jsonify({"error": "Invalid token payload"}), 401)

    user = db.session.get(User, int(user_id))
    if not user:
        return None, (jsonify({"error": "User not found"}), 404)

    return user, None


def token_required(fn):
    """
    Decorator that validates JWT and attaches the user to request.current_user.
    Replaces @jwt_required() + manual User.query.get(get_jwt_identity()).
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user, error = _extract_and_validate_token()
        if error:
            return error
        request.current_user = user
        return fn(*args, **kwargs)
    return wrapper


def role_required(*allowed_roles):
    """
    Decorator that validates JWT AND checks the user's role.
    Combines @token_required + inline role check into one decorator.

    Usage:
        @role_required("admin")
        @role_required("manager", "cto", "admin")
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user, error = _extract_and_validate_token()
            if error:
                return error
            if user.role not in allowed_roles:
                return jsonify({"error": "Forbidden — insufficient role"}), 403
            request.current_user = user
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def validate_socket_token(token_str):
    """
    Validate a JWT token string for WebSocket connections.
    Returns the User on success or None on failure.
    Used by SocketIO connect handler.
    """
    if not token_str:
        return None
    try:
        decoded = decode_token(token_str)
        user_id = decoded.get("sub")
        if not user_id:
            return None
        return db.session.get(User, int(user_id))
    except Exception:
        return None
