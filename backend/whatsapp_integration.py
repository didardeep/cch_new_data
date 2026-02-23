"""
WhatsApp Integration using Twilio API
"""

import os
from twilio.rest import Client

# Twilio credentials from environment variables
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")

# Initialize Twilio client
client = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    except Exception as e:
        print(f"⚠️  Twilio initialization failed: {e}")


def send_whatsapp_message(to_number, message_body):
    """
    Send WhatsApp message via Twilio

    Args:
        to_number (str): Phone number with country code (e.g., +919876543210)
        message_body (str): Message text to send

    Returns:
        dict: {"success": bool, "message_sid": str or None, "error": str or None}
    """
    if not client:
        return {
            "success": False,
            "message_sid": None,
            "error": "Twilio client not initialized. Check credentials."
        }

    # Ensure phone number has 'whatsapp:' prefix
    if not to_number.startswith("whatsapp:"):
        to_number = f"whatsapp:{to_number}"

    try:
        message = client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            body=message_body,
            to=to_number
        )
        return {
            "success": True,
            "message_sid": message.sid,
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "message_sid": None,
            "error": str(e)
        }


def format_chat_summary_for_whatsapp(session, user_name):
    """
    Format chat summary as WhatsApp message (matches email content).

    Args:
        session: ChatSession object
        user_name (str): User's name

    Returns:
        str: Formatted message
    """
    message = f"🤖 *TeleBot - Chat Summary Report*\n\n"
    message += f"Hello {user_name}!\n"
    message += f"Here is the summary of your recent support chat session:\n\n"
    message += f"📋 *Session #{session.id}*\n"
    message += f"📂 Category: {session.sector_name or 'N/A'}\n"
    message += f"🔧 Issue: {session.subprocess_name or 'N/A'}\n"
    message += f"📊 Status: {session.status.upper()}\n"
    message += f"🌐 Language: {session.language or 'English'}\n"
    if session.created_at:
        message += f"📅 Date: {session.created_at.strftime('%B %d, %Y at %I:%M %p')}\n"

    if session.summary:
        message += f"\n📝 *Chat Summary:*\n{session.summary}\n"

    if session.query_text:
        message += f"\n❓ *Your Query:*\n{session.query_text}\n"

    # Include ticket reference if a ticket was raised
    ticket = getattr(session, 'ticket', None)
    if ticket:
        message += f"\n🎫 *Escalation Ticket*\n"
        message += f"Reference: {ticket.reference_number}\n"
        message += f"Priority: {ticket.priority.upper()}\n"

    message += f"\nIf you have further questions, feel free to start a new chat session anytime.\nThank you for using TeleBot! 🙏"

    return message


def format_ticket_alert_for_whatsapp(ticket, user_name, session=None):
    """
    Format ticket escalation alert as WhatsApp message

    Args:
        ticket: Ticket object
        user_name (str): User's name
        session: Optional ChatSession object

    Returns:
        str: Formatted message
    """
    message = f"🎫 *TeleBot - Support Ticket Created*\n\n"
    message += f"Hello {user_name}!\n\n"
    message += f"Your issue has been escalated to our support team.\n\n"
    message += f"📌 *Reference:* {ticket.reference_number}\n"
    message += f"📂 *Category:* {ticket.category}\n"
    message += f"🔧 *Issue:* {ticket.subcategory}\n"
    message += f"⚡ *Priority:* {ticket.priority.upper()}\n"
    message += f"📊 *Status:* {ticket.status.upper()}\n\n"

    if ticket.description:
        message += f"📝 *Description:*\n{ticket.description[:200]}...\n\n"

    message += f"Our team will contact you soon! 📞\n"
    message += f"Track your ticket on the dashboard."

    return message