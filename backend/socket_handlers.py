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
    def handle_socket_connect(auth=None):
        """
        Validate JWT on WebSocket connect.
        Flask-SocketIO 5.x passes the client's auth payload as the first arg.
        If token is missing or invalid, reject the connection (return False).
        """
        token = None
        if auth and isinstance(auth, dict):
            token = auth.get("token")
        if not token:
            return False

        user = validate_socket_token(token)
        if not user:
            return False
        # Connection accepted — user is authenticated

    @socketio.on("join_session")
    def handle_join_session(data):
        """Join a chat room so this client receives session-specific messages."""
        session_id = data.get("session_id") if data else None
        if session_id:
            join_room(f"session_{session_id}")

    @socketio.on("join_ai_session")
    def handle_join_ai_session(data):
        """Join an AI chat room for receiving query progress updates."""
        session_id = data.get("session_id") if data else None
        if session_id:
            join_room(f"ai_session_{session_id}")
