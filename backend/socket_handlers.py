"""
socket_handlers.py — SocketIO event handlers with JWT authentication
=====================================================================
Validates JWT on every WebSocket connect. The frontend must pass the token
in the `auth` payload: io(URL, { auth: { token: "..." } })

Usage (in app.py):
    from socket_handlers import register_socket_handlers
    register_socket_handlers(socketio, app)
"""

from flask_socketio import join_room
from auth_utils import validate_socket_token


def register_socket_handlers(socketio, app):
    """Register all SocketIO event handlers on the given socketio instance."""

    @socketio.on("connect")
    def handle_socket_connect(auth_data=None):
        """Reject WebSocket connections without a valid JWT."""
        token = None
        if auth_data and isinstance(auth_data, dict):
            token = auth_data.get("token")
        if not token:
            return False

        with app.app_context():
            user = validate_socket_token(token)
            if not user:
                return False

        from flask import request as flask_request
        flask_request.socket_user = user.to_dict()

    @socketio.on("join_session")
    def handle_join_session(data):
        """Join a chat room. Token already validated on connect."""
        session_id = data.get("session_id") if data else None
        if session_id:
            join_room(f"session_{session_id}")
