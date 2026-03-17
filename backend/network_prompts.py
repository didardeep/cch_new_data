"""
network_prompts.py
==================
All AI prompt logic for Mobile Network / Signal problems.
Imported by app.py — do not run directly.

Contains:
- generate_resolution()         — original single-attempt resolver (used by /api/resolve)
- build_mobile_system_prompt()  — mobile-specific prompt used by generate_single_solution()
- analyze_signal_screenshot()   — vision-based signal screenshot analyser
- detect_greeting()             — greeting detection
- classify_user_response()      — satisfaction + signal mention classifier
- detect_language()             — language detection
- is_telecom_related()          — telecom intent classifier
- identify_subprocess()         — subprocess matcher
- translate_text()              — translation helper
- generate_chat_summary()       — chat summary generator
- _friendly_ai_error()          — user-friendly error fallback
"""

import re
import json
from datetime import datetime


# ─── These are injected from app.py at import time ───────────────────────────
# Call init(client, deployment_name, telecom_menu) from app.py after creating
# the AzureOpenAI client so this module can use them.
_client = None
_deployment = None
_telecom_menu = None


def init(client, deployment_name, telecom_menu):
    """Called once from app.py after AzureOpenAI client is set up."""
    global _client, _deployment, _telecom_menu
    _client = client
    _deployment = deployment_name
    _telecom_menu = telecom_menu


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _friendly_ai_error(err: Exception) -> str:
    return (
        "I'm having trouble reaching the AI service right now. "
        "Please try again in a few moments."
    )


def translate_text(text: str, target_language: str) -> str:
    if target_language.lower() in ("english", "en"):
        return text
    try:
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": f"Translate the following text to {target_language}. Keep formatting intact. Return ONLY the translation."},
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=500,
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return text


def get_subprocess_details(sector_key: str) -> str:
    sector = _telecom_menu[sector_key]
    details = []
    for k, v in sector["subprocesses"].items():
        if isinstance(v, dict) and v["name"] != "Others":
            details.append(f'SUBPROCESS: "{v["name"]}"\n  Typical issues: {v["semantic_scope"]}')
    return "\n\n".join(details)


def get_subprocess_name(sector_key: str, subprocess_key: str) -> str:
    sector = _telecom_menu.get(sector_key, {})
    sp = sector.get("subprocesses", {}).get(subprocess_key, {})
    if isinstance(sp, dict):
        return sp.get("name", "Others")
    return sp if isinstance(sp, str) else "Others"


# ─── Intent & language classifiers ───────────────────────────────────────────

def is_telecom_related(query: str, sector_name=None, subprocess_name=None) -> bool:
    context_block = ""
    if sector_name:
        context_block = (
            f"\n\n── USER'S MENU NAVIGATION ──\n"
            f'The user already selected telecom sector: "{sector_name}"'
        )
        if subprocess_name:
            context_block += f'\nThey also selected subprocess: "{subprocess_name}"'
        context_block += (
            "\n\nBecause the user navigated a TELECOM complaint menu to reach this point, "
            "their query is almost certainly telecom-related. Generic complaints like "
            " 'money deducted', 'service not working', 'bad experience', 'want refund', "
            " 'not getting what I paid for' etc. should be interpreted in the telecom context.\n"
            "Only classify as NOT telecom if the query is EXPLICITLY about a completely "
            "different industry."
        )
    try:
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": (
                    "You are a semantic intent classifier for a TELECOM complaint chatbot.\n\n"
                    "Your job is to determine whether the user's query is related to telecommunications.\n\n"
                    "TELECOM includes (but is not limited to):\n"
                    "- Mobile phone services (calls, SMS, data, prepaid, postpaid)\n"
                    "- Internet/broadband/WiFi/fiber services\n"
                    "- DTH/cable TV/satellite TV\n"
                    "- Landline/fixed-line telephone\n"
                    "- Enterprise telecom (leased lines, VPN, MPLS, SLA)\n"
                    "- ANY billing, payment, refund, service quality, or customer care issue "
                    "related to any of the above\n\n"
                    "SEMANTIC REASONING RULES:\n"
                    "1. Focus on the USER'S INTENT, not just the words they used.\n"
                    "2. 'Money deducted' in a telecom context = telecom billing issue.\n"
                    "3. 'Service not working' in a telecom context = telecom service disruption.\n"
                    "4. Vague complaints ARE telecom if the user came through the telecom menu.\n"
                    "5. Only reject if the query is CLEARLY about a non-telecom industry.\n"
                    + context_block +
                    '\n\nRespond with ONLY this JSON (no extra text):\n'
                    '{"reasoning": "<one sentence about why>", "is_telecom": true/false}'
                )},
                {"role": "user", "content": query},
            ],
            temperature=0,
            max_tokens=120,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return result.get("is_telecom", False)
    except Exception:
        return True if sector_name else False


def identify_subprocess(query: str, sector_key: str) -> str:
    sector = _telecom_menu[sector_key]
    subprocess_details = get_subprocess_details(sector_key)
    try:
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": (
                    f"You are a semantic complaint classifier for: {sector['name']}.\n"
                    f"Sector description: {sector.get('description', '')}\n\n"
                    "Below are the available subprocesses:\n\n"
                    f"{subprocess_details}\n\n"
                    "Analyze the user's complaint and determine which subprocess it belongs to.\n\n"
                    "Respond with ONLY this JSON:\n"
                    '{"reasoning": "<brief explanation>", "matched_subprocess": "<exact name>", "confidence": <0.0 to 1.0>}'
                )},
                {"role": "user", "content": query},
            ],
            temperature=0,
            max_tokens=200,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return result.get("matched_subprocess", "General Inquiry")
    except Exception:
        return "General Inquiry"


def detect_greeting(text: str) -> bool:
    try:
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": (
                    "Determine if the user's message is a greeting or salutation in ANY language or mixed language. "
                    "A greeting includes (but is not limited to): hello, hi, hey, hiya, howdy, good morning, "
                    "good afternoon, good evening, namaste, namaskar, salaam, assalamu alaikum, "
                    "bonjour, hola, ciao, salam, sat sri akal, vanakkam, adab, greetings, what's up, "
                    "yo, sup, hii, helo, hai, or informal/phonetic variants in any script. "
                    "Mixed-language greetings (e.g. 'hello aur kaise ho', 'hi there bhai') also count. "
                    'Respond with ONLY valid JSON: {"is_greeting": true} or {"is_greeting": false}'
                )},
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=20,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return bool(result.get("is_greeting", True))
    except Exception:
        return True


def classify_user_response(text: str) -> dict:
    try:
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": (
                    "You are classifying a customer's response in a telecom support chat. "
                    "The customer was just given a solution and asked 'Did this help?'\n\n"
                    "Determine:\n"
                    "1. is_satisfied: Is the user saying the issue is resolved / they are happy / it worked / thank you / yes it helped? (true/false)\n"
                    "2. mentions_signal: Does the user's message semantically relate to network signal, coverage, "
                    "poor reception, no signal, weak signal, call drops, slow internet speed, network not available, "
                    "data not working, or similar signal/network connectivity issues? (true/false)\n\n"
                    'Respond with ONLY valid JSON: {"is_satisfied": true/false, "mentions_signal": true/false}'
                )},
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=30,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        return json.loads(raw)
    except Exception:
        return {"is_satisfied": False, "mentions_signal": False}


def detect_language(text: str) -> str:
    try:
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": (
                    "Detect the language of the following text. "
                    'Respond with ONLY: {"language": "<language_name>", "code": "<iso_code>"}'
                )},
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=50,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        return result.get("language", "English")
    except Exception:
        return "English"


def generate_chat_summary(messages_list, sector_name, subprocess_name):
    try:
        conversation = "\n".join([f"{m['sender']}: {m['content']}" for m in messages_list])
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": (
                    "Summarize this telecom support chat in 3-4 sentences. "
                    f"Category: {sector_name} > {subprocess_name}. "
                    "Include: what the issue was, what resolution was provided, and the outcome."
                )},
                {"role": "user", "content": conversation},
            ],
            temperature=0.3,
            max_tokens=200,
        )
        return response.choices[0].message.content.strip()
    except Exception:
        return f"Chat about {sector_name} - {subprocess_name}. Customer query handled."


# ─── Mobile / Network Signal prompts ─────────────────────────────────────────

def generate_resolution(query, sector_name, subprocess_name, language):
    """Original single-attempt resolver used by /api/resolve."""
    try:
        response = _client.chat.completions.create(
            model=_deployment,
            messages=[
                {"role": "system", "content": (
                    f"You are a senior telecom network support specialist. The customer has reported an issue "
                    f"under: '{sector_name}' > '{subprocess_name}'.\n"
                    "You must scope all solutions strictly to this subprocess. Do NOT mix solutions from sibling subprocesses.\n\n"
                    "RESPONSE FORMAT:\n"
                    "1. One-line empathetic acknowledgment of the specific issue.\n"
                    "2. ONE precise, field-proven solution with 3-5 numbered steps.\n\n"
                    "STEP QUALITY RULES — every step must be:\n"
                    "• Specific: include exact menu paths, setting names, dial codes, or field values.\n"
                    "• Actionable: tell the user exactly what to tap, toggle, enter, or dial.\n"
                    "• Technically grounded: use industry-standard methods.\n\n"
                    "BANNED SUGGESTIONS (never include):\n"
                    "- Restart phone / toggle airplane mode\n"
                    "- Restart router or modem\n"
                    "- Move to open area or near a window\n"
                    "- Wait for network congestion\n"
                    "- Contact customer support / call care / raise ticket / visit service center\n\n"
                    "ISSUE-SPECIFIC TECHNICAL GUIDANCE:\n"
                    "Mobile data not working: Manually configure APN via Settings → SIM & Network → Access Point Names → Add New APN (enter operator APN name/type: default,supl; MCC/MNC per operator). Check Preferred Network Type (Settings → Mobile Network → set to LTE/4G), SIM slot assignment, and Data Roaming flag.\n"
                    "Call drops / poor voice: Enable VoLTE at Settings → Mobile Network → VoLTE Calls → ON. Enable VoWiFi at Settings → Mobile Network → Wi-Fi Calling → ON. To check/lock band: dial *#2263# (Samsung) and select preferred band (Band 3 1800MHz / Band 40 2300MHz TDD-LTE per operator).\n"
                    "Billing / wrong deduction: Dial *121# or *199# for itemised balance; *121*1# for data pack status; *123# for talktime ledger. To dispute: open carrier app → My Account → Bill Details → Dispute Transaction.\n"
                    "Plan/pack not activated: Check provisioning via *199*2# or *121*2#. For eSIM: Settings → Cellular → Add eSIM → rescan operator QR. For prepaid: dial *444# to verify active pack.\n"
                    "DTH signal loss: Check signal strength in TV menu (target >60%). Re-scan transponders: Dish TV → Setup → Edit TP → 11090 V 30000; Tata Play → 12515 H 22000.\n"
                    "MNP / Port-in stuck: Regenerate UPC by sending SMS 'PORT <10-digit number>' to 1900 (valid 4 days). Check port status: SMS 'PORTSTATUS' to 1900.\n\n"
                    "Do NOT include any URLs or hyperlinks.\n"
                    f"Respond entirely in {language}. Be concise, precise, and technically accurate."
                )},
                {"role": "user", "content": query},
            ],
            temperature=0.4,
            max_tokens=1000,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return _friendly_ai_error(e)


def build_mobile_system_prompt(sector_name, subprocess_name, language, attempt,
                                query_block="", context_block="", diagnosis_block="", prev_block=""):
    """
    Builds the mobile network system prompt.
    Used by generate_single_solution() in app.py when sector is NOT broadband.
    """
    return (
        f"You are a senior telecom network support specialist. The customer has an issue "
        f"under: '{sector_name}' > '{subprocess_name}'. This is solution attempt #{attempt}.\n"
        "Stay strictly within this subprocess. Do NOT mix steps from sibling subprocesses "
        "(e.g., if subprocess is 'Network / Signal Problems – Internet / Mobile Data', do not mention call-drop or call-failure steps).\n\n"
        "Provide exactly ONE precise, field-proven solution with 3-5 numbered steps. Each step must:\n"
        "• Include exact menu paths, setting names, dial codes, or field values.\n"
        "• Tell the user exactly what to tap, toggle, enter, or dial — no vague instructions.\n"
        "• Use industry-standard troubleshooting methods (APN config, VoLTE/VoWiFi toggle, band locking, USSD codes, etc.).\n\n"
        "BANNED SUGGESTIONS (never include):\n"
        "- Restart phone / toggle airplane mode\n"
        "- Restart router or modem\n"
        "- Move to open area or near a window\n"
        "- Wait for network congestion\n"
        "- Contact support / call care / raise ticket / visit service center\n\n"
        "ISSUE-SPECIFIC TECHNICAL GUIDANCE (apply relevant section):\n"
        "Mobile data: Configure APN (Settings → SIM & Network → Access Point Names → New APN → enter name/type/MCC/MNC). Set Preferred Network Type to LTE/4G. Check SIM slot assignment and Data Roaming flag.\n"
        "Call drops/voice: VoLTE: Settings → Mobile Network → VoLTE Calls → ON. VoWiFi: Settings → Mobile Network → Wi-Fi Calling → ON. Band lock: *#2263# (Samsung) → select Band 3/40 per operator.\n"
        "Billing: Balance: *121# or *199#. Data pack: *121*1#. Talktime: *123#. Dispute via carrier app → My Account → Bill Details → Dispute Transaction. CDR from app → Usage History.\n"
        "Plan activation: Provisioning: *199*2#. eSIM: Settings → Cellular → Add eSIM → rescan QR or generate new QR via self-care app. Prepaid pack: *444# to verify; retry via USSD post top-up.\n"
        "DTH: Signal check via TV menu (>60%). Dish TV transponder: 11090 V 30000. Tata Play: 12515 H 22000. Smart card: carrier app → Manage Device → Reactivate.\n"
        "MNP/port-in: UPC: SMS 'PORT <number>' to 1900. Status: SMS 'PORTSTATUS' to 1900. HLR refresh after 7 days via self-care → Port Request Status.\n\n"
        "Do NOT include any URLs or hyperlinks.\n"
        + query_block
        + context_block
        + diagnosis_block
        + prev_block
        + f"\n\nRespond entirely in {language}. Be concise, precise, and technically accurate."
    )


# ─── Signal screenshot analyser ───────────────────────────────────────────────

def analyze_signal_screenshot(image_base64):
    """Use Azure OpenAI Vision to extract signal metrics from a screenshot."""
    clean_b64 = re.sub(r"^data:image/[^;]+;base64,", "", image_base64)

    response = _client.chat.completions.create(
        model=_deployment,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a telecom signal analysis expert. Extract signal metrics from "
                    "the provided screenshot of a phone's service mode or signal information screen.\n\n"
                    "Extract these values:\n"
                    "- RSRP (Reference Signal Received Power) in dBm\n"
                    "- SINR (Signal to Interference plus Noise Ratio) in dB\n"
                    "- Cell ID (the cell identifier)\n\n"
                    "Return ONLY valid JSON in this exact format:\n"
                    '{"rsrp": <number or null>, "sinr": <number or null>, "cell_id": <string or null>}\n\n'
                    "If a value is not visible or cannot be determined, use null.\n"
                    "For RSRP, return just the number (e.g., -95, not '-95 dBm').\n"
                    "For SINR, return just the number (e.g., 12, not '12 dB').\n"
                    "For Cell ID, return the string value as shown."
                ),
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract RSRP, SINR, and Cell ID values from this signal information screenshot."},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{clean_b64}", "detail": "high"}},
                ],
            },
        ],
        temperature=0,
        max_tokens=200,
    )

    raw = response.choices[0].message.content.strip()
    json_match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not json_match:
        raise ValueError("Could not parse AI response")
    extracted = json.loads(json_match.group())

    rsrp = extracted.get("rsrp")
    sinr = extracted.get("sinr")
    cell_id = extracted.get("cell_id")

    if rsrp is not None:
        rsrp = float(rsrp)
        if -105 <= rsrp <= -40:
            rsrp_status, rsrp_label = "green", "Good"
        elif -115 <= rsrp < -105:
            rsrp_status, rsrp_label = "amber", "Moderate"
        else:
            rsrp_status, rsrp_label = "red", "Weak"
    else:
        rsrp_status, rsrp_label = "unknown", "Not detected"

    if sinr is not None:
        sinr = float(sinr)
        if sinr > 5:
            sinr_status, sinr_label = "green", "Good"
        elif sinr >= 0:
            sinr_status, sinr_label = "amber", "Moderate"
        else:
            sinr_status, sinr_label = "red", "Weak"
    else:
        sinr_status, sinr_label = "unknown", "Not detected"

    now = datetime.now()
    current_hour = now.hour
    is_busy_hour = (9 <= current_hour < 11) or (18 <= current_hour < 21)

    statuses = [s for s in [rsrp_status, sinr_status] if s != "unknown"]
    if not statuses:
        overall, overall_label = "unknown", "Unable to determine signal quality"
    elif any(s == "red" for s in statuses):
        overall, overall_label = "red", "Poor"
    elif any(s == "amber" for s in statuses):
        overall, overall_label = "amber", "Moderate"
    else:
        overall, overall_label = "green", "Good"

    if overall == "green":
        summary = "Your signal strength is good. You should have stable connectivity in your area."
    elif overall == "amber":
        summary = "Your signal strength is moderate. You may experience occasional slowdowns or drops."
    elif overall == "red":
        summary = "Your signal strength is poor. This is likely causing the connectivity issues you're experiencing."
    else:
        summary = "We could not fully determine your signal quality from the screenshot."

    if is_busy_hour:
        summary += (
            f" Note: You are currently in peak network hours ({now.strftime('%I:%M %p')}). "
            "Network congestion during 9-11 AM and 6-9 PM can further degrade signal quality and speeds."
        )

    return {
        "rsrp": rsrp, "rsrp_status": rsrp_status, "rsrp_label": rsrp_label,
        "sinr": sinr, "sinr_status": sinr_status, "sinr_label": sinr_label,
        "cell_id": str(cell_id) if cell_id is not None else None,
        "overall_status": overall, "overall_label": overall_label,
        "is_busy_hour": is_busy_hour, "summary": summary,
    }
