"""
network_ai.py
=============
Flask Blueprint: Network AI Chat & Query Engine
Extracted from network_analytics.py — handles all AI/LLM query logic,
rule-based NL→SQL fallback, and session CRUD for the Network AI Chat page.

Mount in app.py:
    from network_ai import network_ai_bp
    app.register_blueprint(network_ai_bp)
"""

import os
import json
import math
import logging
from datetime import datetime, timezone

from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import text as sa_text

from models import db, User, NetworkAiSession, NetworkAiMessage

_LOG = logging.getLogger("network_ai")

# ─────────────────────────────────────────────────────────────────────────────
network_ai_bp = Blueprint("network_ai", __name__)


# ─── Shared helpers (imported lazily from network_analytics) ─────────────────

def _sql(query: str, params: dict = None) -> list:
    """Execute raw SQL and return list of dicts."""
    with db.engine.connect() as conn:
        result = conn.execute(sa_text(query), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def _get_dynamic_time_filter():
    """Import the time filter helper from network_analytics at call time."""
    from network_analytics import _dynamic_time_filter
    return _dynamic_time_filter


def _ensure_ai_session_tables():
    """Create network_ai_sessions / network_ai_messages tables if they don't exist."""
    try:
        NetworkAiSession.__table__.create(db.engine, checkfirst=True)
        NetworkAiMessage.__table__.create(db.engine, checkfirst=True)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/network/ai-query
# ─────────────────────────────────────────────────────────────────────────────
@network_ai_bp.route("/api/network/ai-query", methods=["POST"])
@jwt_required()
def ai_query():
    uid = get_jwt_identity()
    user = db.session.get(User, int(uid))
    if not user:
        return jsonify({"error": "Forbidden"}), 403

    body    = request.get_json(silent=True) or {}
    prompt  = str(body.get("prompt", "")).strip()
    context = body.get("context", {})
    filters = context.get("filters", {})
    time_range = filters.get("time_range", "24h")
    time_filter = _get_dynamic_time_filter()(time_range)

    if not prompt:
        return jsonify({"error": "No prompt provided"}), 400

    # ── Session context (optional) ────────────────────────────────────────────
    session_id = body.get("session_id")
    ai_session = None
    conversation_history = []

    if session_id:
        _ensure_ai_session_tables()
        ai_session = db.session.get(NetworkAiSession, int(session_id))
        if ai_session and ai_session.user_id == int(uid):
            recent_msgs = (NetworkAiMessage.query
                          .filter_by(session_id=session_id)
                          .order_by(NetworkAiMessage.created_at.desc())
                          .limit(20)
                          .all())
            recent_msgs.reverse()
            for m in recent_msgs:
                conversation_history.append({
                    "role": m.role if m.role == "user" else "assistant",
                    "content": m.content,
                })
            user_msg = NetworkAiMessage(
                session_id=session_id, role="user", content=prompt,
            )
            db.session.add(user_msg)
            ai_session.last_message_at = datetime.now(timezone.utc)
            db.session.commit()
        else:
            ai_session = None

    # ── Try LLM providers ─────────────────────────────────────────────────────
    provider = None
    ai_result = None

    # ── Comprehensive KPI mapping for accurate NL→SQL ─────────────────────────
    SCHEMA_HINT = """
Tables:
1. kpi_data(id, site_id, kpi_name, value, date, hour, data_level, cell_id, cell_site_id)
   - data_level = 'site' for site-level, 'cell' for cell-level
   - IMPORTANT: kpi_name values are EXACT strings. Use these EXACTLY as listed below.

   === RAN KPI Names (EXACT values in kpi_name column) ===
   'LTE RRC Setup Success Rate'        -- RRC success rate, accessibility
   'LTE Call Setup Success Rate'        -- Call setup success, CSSR
   'LTE E-RAB Setup Success Rate'       -- E-RAB setup
   'E-RAB Call Drop Rate_1'             -- Call drop rate, CDR
   'CSFB Access Success Rate'           -- CSFB fallback
   'LTE Intra-Freq HO Success Rate'    -- Intra-frequency handover
   'Intra-eNB HO Success Rate'         -- Intra-eNB handover
   'Inter-eNBX2HO Success Rate'        -- Inter-eNB X2 handover
   'Inter-eNBS1HO Success Rate'        -- Inter-eNB S1 handover
   'LTE DL - Cell Ave Throughput'       -- DL cell throughput (Mbps)
   'LTE UL - Cell Ave Throughput'       -- UL cell throughput
   'LTE DL - Usr Ave Throughput'        -- DL user throughput (Mbps)
   'LTE UL - User Ave Throughput'       -- UL user throughput
   'Average Latency Downlink'           -- Latency (ms)
   'DL Data Total Volume'               -- DL data volume (GB)
   'UL Data Total Volume'               -- UL data volume
   'VoLTE Traffic Erlang'               -- VoLTE traffic in Erlang
   'VoLTE Traffic UL'                   -- VoLTE UL traffic
   'VoLTE Traffic DL'                   -- VoLTE DL traffic
   'Ave RRC Connected Ue'               -- Average connected users
   'Max RRC Connected Ue'               -- Max connected users
   'Average Act UE DL Per Cell'         -- Active DL users per cell
   'Average Act UE UL Per Cell'         -- Active UL users per cell
   'Availability'                       -- Site availability %
   'Average NI of Carrier-'             -- Noise/interference
   'DL PRB Utilization (1BH)'           -- DL PRB utilization %, congestion
   'UL PRB Utilization (1BH)'           -- UL PRB utilization %

2. telecom_sites(site_id, cell_id, latitude, longitude, zone)
   - JOIN: kpi_data k JOIN telecom_sites ts ON k.site_id = ts.site_id
   - zone column has values like zone names / cluster names

3. flexible_kpi_uploads(site_id, kpi_name, kpi_type, column_name, num_value, str_value)
   - kpi_type = 'core' for core KPIs: Authentication Success Rate, CPU Utilization, Attach Success Rate, PDP Bearer Setup Success Rate
   - kpi_type = 'revenue' for revenue data

=== Natural Language → KPI Mapping Guide ===
User says "call drop" / "drop rate" / "CDR" / "call failure" / "कॉल ड्रॉप" → 'E-RAB Call Drop Rate_1'
User says "throughput" / "speed" / "download speed" / "स्पीड" / "थ्रूपुट" → 'LTE DL - Usr Ave Throughput' (user) or 'LTE DL - Cell Ave Throughput' (cell)
User says "PRB" / "congestion" / "load" / "utilization" / "भीड़" / "लोड" → 'DL PRB Utilization (1BH)'
User says "availability" / "uptime" / "downtime" / "उपलब्धता" → 'Availability'
User says "connected users" / "RRC users" / "active users" / "यूजर्स" → 'Ave RRC Connected Ue'
User says "handover" / "HO" / "हैंडओवर" → 'LTE Intra-Freq HO Success Rate' (or specific HO type)
User says "VoLTE" / "voice" / "वॉइस" → 'VoLTE Traffic Erlang'
User says "latency" / "delay" / "ping" / "लेटेंसी" → 'Average Latency Downlink'
User says "data volume" / "traffic volume" / "डेटा वॉल्यूम" → 'DL Data Total Volume'
User says "call setup" / "CSSR" → 'LTE Call Setup Success Rate'
User says "RRC" / "accessibility" / "access" → 'LTE RRC Setup Success Rate'
User says "noise" / "interference" → 'Average NI of Carrier-'
User says "sabse kharab" / "worst" / "bottom" → ORDER ASC + LIMIT N
User says "sabse accha" / "best" / "top" → ORDER DESC + LIMIT N
User says "last 7 days" / "पिछले 7 दिन" → AND k.date >= CURRENT_DATE - INTERVAL '7 days' AND k.date <= CURRENT_DATE
User says "last month" / "पिछला महीना" → AND k.date >= CURRENT_DATE - INTERVAL '1 month' AND k.date <= CURRENT_DATE
User says "today" / "आज" → AND k.date = CURRENT_DATE
ALWAYS add AND k.date <= CURRENT_DATE when any date range is used, to exclude future data.

Example CASE WHEN pivot pattern (ALWAYS use this for multi-KPI queries):
  SELECT k.site_id, ts.zone,
    AVG(CASE WHEN k.kpi_name='DL PRB Utilization (1BH)' THEN k.value END) AS dl_prb,
    AVG(CASE WHEN k.kpi_name='LTE DL - Usr Ave Throughput' THEN k.value END) AS dl_tput
  FROM kpi_data k JOIN telecom_sites ts ON k.site_id=ts.site_id
  WHERE k.data_level='site' AND k.value IS NOT NULL
  GROUP BY k.site_id, ts.zone ORDER BY dl_prb DESC LIMIT 10
"""

    LLM_SYSTEM = f"""You are a telecom network analytics SQL generator. Your ONLY job is to convert the user's natural-language query into an EXACT, STRICT SQL query that fetches PRECISELY what was asked — nothing more, nothing less.

READ THE USER QUERY VERY CAREFULLY. Understand EVERY part of it before generating SQL.

The user may write in English, Hindi, Hinglish, or any language. Understand the intent and translate to SQL.

{SCHEMA_HINT}

═══════════════════════════════════════════════════════════
CRITICAL RULE #0 — MULTI-PART / COMPOUND QUERIES:
═══════════════════════════════════════════════════════════

Users often ask for MULTIPLE things in ONE query. You MUST handle ALL parts.

**How to detect multi-part queries:**
- Words like "and", "also", "along with", "as well as", "plus", "both"
- Multiple site IDs mentioned: "site A ... and site B ..."
- Multiple KPIs mentioned: "CSSR ... and throughput ..."
- Multiple time ranges: "last 18 days ... last 10 days ..."
- Separate clauses: "show X for Y" + "and Z for W"

**How to handle multi-part queries with DIFFERENT sites/time ranges:**
Use UNION ALL to combine separate sub-queries into one result set.

Example: "trend of CSSR for GUR_LTE_1500 last 18 days AND throughput for GUR_LTE_1400 last 10 days"
→ This is asking for TWO SEPARATE trends. Use UNION ALL:

SELECT k.date::text AS date, k.site_id,
       AVG(k.value) AS value,
       'LTE Call Setup Success Rate' AS kpi_name
FROM kpi_data k
WHERE k.kpi_name = 'LTE Call Setup Success Rate'
  AND k.site_id = 'GUR_LTE_1500'
  AND k.data_level = 'site' AND k.value IS NOT NULL
  AND k.date >= CURRENT_DATE - INTERVAL '18 days' AND k.date <= CURRENT_DATE
GROUP BY k.date, k.site_id
UNION ALL
SELECT k.date::text AS date, k.site_id,
       AVG(k.value) AS value,
       'LTE DL - Cell Ave Throughput' AS kpi_name
FROM kpi_data k
WHERE k.kpi_name = 'LTE DL - Cell Ave Throughput'
  AND k.site_id = 'GUR_LTE_1400'
  AND k.data_level = 'site' AND k.value IS NOT NULL
  AND k.date >= CURRENT_DATE - INTERVAL '10 days' AND k.date <= CURRENT_DATE
GROUP BY k.date, k.site_id
ORDER BY date

For such multi-part queries:
- chart_type: "composed" (if 2 metrics) or "line" (if same metric, different sites)
- y_axes: ["value"] and use kpi_name/site_id to differentiate series
- title: mention BOTH parts
- response: describe BOTH parts

**How to handle multi-part queries with SAME site/time range but different KPIs:**
Use CASE WHEN pivot pattern (already described in schema hint).

**NEVER ignore part of the user's query. If they ask for 2 things, return BOTH.**

═══════════════════════════════════════════════════════════
STRICTNESS RULES — FOLLOW THESE EXACTLY:
═══════════════════════════════════════════════════════════

1. ONLY query the EXACT KPI(s) the user asked about. Do NOT add extra KPIs.
   - User asks "throughput for site X" → query ONLY throughput, NOT PRB or drop rate
   - User asks "PRB and drop rate" → query ONLY those two, nothing else

2. ONLY filter by what the user specified:
   - User says "site GUR_LTE_1400" → WHERE k.site_id = 'GUR_LTE_1400'  (exact match)
   - User says "last 7 days" → AND k.date >= CURRENT_DATE - INTERVAL '7 days' AND k.date <= CURRENT_DATE
   - User says "last 18 days" → AND k.date >= CURRENT_DATE - INTERVAL '18 days' AND k.date <= CURRENT_DATE
   - User says "last N days" → AND k.date >= CURRENT_DATE - INTERVAL 'N days' AND k.date <= CURRENT_DATE
   - User says "last month" → AND k.date >= CURRENT_DATE - INTERVAL '1 month' AND k.date <= CURRENT_DATE
   - IMPORTANT: TODAY's date is {datetime.now().strftime('%Y-%m-%d')}. ALWAYS cap date ranges with AND k.date <= CURRENT_DATE to exclude future data.
   - User says "zone CBD" → AND ts.zone ILIKE '%CBD%'
   - User says NO date filter → do NOT add date filter, query all available data
   - User says NO site filter → do NOT filter by site
   - EXTRACT THE EXACT NUMBER OF DAYS mentioned. "last 18 days" = 18 days, NOT 7 or 30.

3. ONLY return the number of results asked:
   - User says "top 5" → LIMIT 5
   - User says "worst 10" → LIMIT 10
   - User does NOT specify a number → use sensible default: 10 for rankings, all dates for trends

4. SQL PERFORMANCE — MANDATORY:
   - ALWAYS: WHERE k.data_level='site' AND k.value IS NOT NULL
   - ALWAYS: AND k.kpi_name IN ('exact_name_1', 'exact_name_2') or = 'exact_name'
   - For single KPI + single site trend: use WHERE k.kpi_name = 'X' directly (no CASE WHEN needed)
   - For multiple KPIs: use CASE WHEN pivot pattern or UNION ALL
   - LIMIT max 100 rows

5. KPI names are CASE-SENSITIVE — copy EXACTLY from the list above. Never modify them.

6. JOIN telecom_sites only when you need zone/geo data. Skip it for simple single-site queries.

═══════════════════════════════════════════════════════════
CHART TYPE — MUST MATCH THE DATA SHAPE:
═══════════════════════════════════════════════════════════

Pick the chart that BEST fits the query type. This is critical — wrong chart = useless visual.

- "line"     → Time series for 1 site or network average over dates (x=date, y=metric)
                USE THIS for: "trend of X", "X over time", "X for site Y last N days"
- "bar"      → Ranking/comparison of sites (x=site_id, y=metric value)
                USE THIS for: "top N sites", "worst N sites", "compare sites"
- "composed" → Two different metrics on same time axis (e.g. PRB + throughput over time)
                USE THIS for: "show X and Y together over time", "two metrics for two sites"
                ALSO USE THIS for: multi-part queries combining 2 different KPIs/sites on same date axis
- "area"     → Network-wide aggregated trend (x=date, y=avg metric across all sites)
                USE THIS for: "network average trend", "overall X over time"
- "pie"      → Distribution/proportion (max 8 slices)
                USE THIS for: "distribution by zone", "breakdown by category"
- "scatter"  → Correlation between 2 numeric KPIs (x=metric1, y=metric2)
                USE THIS for: "correlation between X and Y", "X vs Y"
- "radar"    → Multi-KPI profile for comparing a FEW sites (max 5)
                USE THIS for: "compare all KPIs of site A vs B"

COMMON MISTAKES TO AVOID:
- Do NOT use "area" for a single site trend → use "line"
- Do NOT use "bar" for time series → use "line" or "area"
- Do NOT add KPIs the user didn't ask about
- Do NOT add date filters the user didn't specify
- Do NOT use CASE WHEN for single-KPI queries — just use WHERE kpi_name = 'X'
- Do NOT ignore any part of the user query — if they ask for 2 trends, show BOTH
- Do NOT change "last 18 days" to "last 7 days" — use the EXACT number the user said
- If user mentions specific site IDs, use those EXACT IDs, do NOT return all sites

═══════════════════════════════════════════════════════════

Respond ONLY with valid JSON (no markdown, no code fences, no extra text):
{{
  "sql": "SELECT ...",
  "title": "Short title describing exactly what is shown (max 60 chars)",
  "response": "1-2 sentence description in the user's language of EXACTLY what this shows",
  "chart_type": "line|bar|composed|area|pie|scatter|radar",
  "x_axis": "column_name_for_x_axis",
  "y_axes": ["metric_col_1"],
  "chart_config": {{
    "x_label": "human readable x label",
    "y_label": "human readable y label with unit",
    "threshold": null,
    "threshold_dir": "above|below",
    "color_scheme": "sequential"
  }},
  "filter_update": {{}}
}}"""

    def _strip_json(raw):
        """Extract JSON from LLM response, stripping markdown fences and extra text."""
        raw = raw.strip()
        if "```" in raw:
            parts = raw.split("```")
            for part in parts[1:]:
                part = part.strip()
                if part.lower().startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    raw = part
                    break
        idx = raw.find("{")
        if idx > 0:
            raw = raw[idx:]
        depth = 0
        end = -1
        for i, c in enumerate(raw):
            if c == "{": depth += 1
            elif c == "}": depth -= 1
            if depth == 0 and c == "}":
                end = i
                break
        if end > 0:
            raw = raw[:end+1]
        return raw.strip()

    def _parse_ai_result(raw_text):
        """Parse LLM response into dict, normalise fields, fill defaults."""
        parsed = json.loads(_strip_json(raw_text))
        ct = parsed.get("chart_type") or parsed.get("chart") or parsed.get("query_type") or "bar"
        parsed["chart_type"] = ct
        parsed["query_type"] = ct
        if "title" not in parsed:
            parsed["title"] = prompt[:60]
        if "response" not in parsed:
            parsed["response"] = parsed.get("title", "Results")
        if "x_axis" not in parsed:
            parsed["x_axis"] = "date" if ct in ("line","area","composed") else "site_id"
        if "y_axes" not in parsed:
            parsed["y_axes"] = []
        if "chart_config" not in parsed:
            parsed["chart_config"] = {}
        return parsed

    # Build user prompt with active filters
    user_prompt = f"User query: {prompt}"
    if filters.get("cluster"):
        user_prompt += f"\nActive filter — Zone: {filters['cluster']}"
    if filters.get("time_range") and filters["time_range"] != "24h":
        user_prompt += f"\nActive filter — Time range: {filters['time_range']}"

    # Build LLM messages with optional conversation history
    llm_messages = [{"role": "system", "content": LLM_SYSTEM}]
    llm_messages.extend(conversation_history)
    llm_messages.append({"role": "user", "content": user_prompt})

    # ── Auto-detect available LLM providers from .env config ──────────────────
    _cfg = lambda k, default="": current_app.config.get(k, "") or os.environ.get(k, "") or default

    azure_key        = _cfg("AZURE_OPENAI_API_KEY")
    azure_endpoint   = _cfg("AZURE_OPENAI_ENDPOINT")
    azure_deployment = _cfg("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
    azure_version    = _cfg("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
    gemini_key       = _cfg("GEMINI_API_KEY")
    gemini_model     = _cfg("OPENAI_MODEL", "gemini-2.0-flash")
    openai_key       = _cfg("OPENAI_API_KEY")

    _providers = []
    if azure_key and azure_endpoint:
        _providers.append(("azure", azure_key, azure_endpoint, azure_deployment, azure_version))
    if gemini_key:
        _providers.append(("gemini", gemini_key, gemini_model))
    if openai_key:
        _providers.append(("openai", openai_key))

    _LOG.info("AI providers available: %s", [p[0] for p in _providers])

    for _prov in _providers:
        if ai_result:
            break
        ptype = _prov[0]
        try:
            if ptype == "azure":
                from openai import AzureOpenAI as _AzureOpenAI
                az_client = _AzureOpenAI(
                    api_key=_prov[1], api_version=_prov[4],
                    azure_endpoint=_prov[2], timeout=25.0,
                )
                az_resp = az_client.chat.completions.create(
                    model=_prov[3],
                    messages=llm_messages,
                    temperature=0.1,
                    max_tokens=2000,
                    response_format={"type": "json_object"},
                )
                raw_content = az_resp.choices[0].message.content
                if raw_content:
                    ai_result = _parse_ai_result(raw_content)
                    provider = {"provider": f"azure-{_prov[3]}"}
                    _LOG.info("AI query handled by Azure OpenAI (%s)", _prov[3])

            elif ptype == "gemini":
                from openai import OpenAI as _OpenAI
                gem_client = _OpenAI(
                    api_key=_prov[1],
                    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
                    timeout=25.0,
                )
                gem_resp = gem_client.chat.completions.create(
                    model=_prov[2],
                    messages=llm_messages,
                    temperature=0.1,
                    max_tokens=2000,
                )
                raw_content = gem_resp.choices[0].message.content
                if raw_content:
                    ai_result = _parse_ai_result(raw_content)
                    provider = {"provider": _prov[2]}
                    _LOG.info("AI query handled by Gemini (%s)", _prov[2])

            elif ptype == "openai":
                from openai import OpenAI as _OpenAI2
                oai_client = _OpenAI2(api_key=_prov[1], timeout=25.0)
                oai_resp = oai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=llm_messages,
                    temperature=0.1,
                    max_tokens=2000,
                )
                raw_content = oai_resp.choices[0].message.content
                if raw_content:
                    ai_result = _parse_ai_result(raw_content)
                    provider = {"provider": "openai-gpt-4o-mini"}
                    _LOG.info("AI query handled by OpenAI direct")

        except json.JSONDecodeError as je:
            _LOG.warning("%s LLM returned bad JSON, using rule-based fallback: %s", ptype.upper(), str(je)[:100])
            continue
        except Exception as e:
            err_str = str(e).lower()
            if "429" in str(e) or "quota" in err_str or "rate" in err_str or "resource_exhausted" in err_str:
                _LOG.warning("%s quota/rate limit hit — skipping to rule-based", ptype.upper())
                break
            _LOG.warning("%s LLM failed (will try next): %s", ptype.upper(), str(e)[:200])
            continue

    # ── Fallback: rule-based query engine ─────────────────────────────────────
    if not ai_result:
        # Extract previous context from session for follow-up detection
        prev_context = None
        if ai_session and session_id:
            try:
                last_asst = (NetworkAiMessage.query
                             .filter_by(session_id=session_id, role="assistant")
                             .order_by(NetworkAiMessage.created_at.desc())
                             .first())
                if last_asst and last_asst.content_json:
                    prev_context = last_asst.content_json
            except Exception:
                pass
        ai_result = _rule_based_query(prompt, time_filter, prev_context=prev_context)
        if not provider:
            provider = {"provider": "rule-based"}
        _LOG.info("AI query handled by rule-based fallback")

    # ── Helper functions ──────────────────────────────────────────────────────
    def _sql_with_timeout(query, timeout_sec=10):
        """Execute SQL with a statement timeout to prevent long-running queries."""
        with db.engine.connect() as conn:
            conn.execute(sa_text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))
            result = conn.execute(sa_text(query))
            cols = list(result.keys())
            return [dict(zip(cols, row)) for row in result.fetchall()]

    def _serial(v):
        if v is None: return None
        if hasattr(v, "isoformat"): return v.isoformat()
        if isinstance(v, float) and math.isnan(v): return None
        try:    return float(v)
        except: return str(v)

    # ── MULTI-CHART: execute each chart's SQL separately ───────────────────────
    if ai_result.get("multi_chart") and ai_result.get("charts"):
        charts_out = []
        for chart_spec in ai_result["charts"]:
            c_sql = chart_spec.get("sql", "")
            try:
                c_rows = _sql_with_timeout(c_sql, timeout_sec=15)
            except Exception as e:
                _LOG.warning("Multi-chart SQL failed: %s — %s", e, c_sql[:150])
                c_rows = []
            c_cols = list(c_rows[0].keys()) if c_rows else []
            c_safe = [{k: _serial(v) for k, v in r.items()} for r in c_rows]
            charts_out.append({
                "title":      chart_spec.get("title", ""),
                "chart_type": chart_spec.get("chart_type", "line"),
                "x_axis":     chart_spec.get("x_axis", c_cols[0] if c_cols else "date"),
                "y_axes":     chart_spec.get("y_axes", c_cols[1:] if c_cols else []),
                "data":       c_safe,
                "columns":    c_cols,
                "row_count":  len(c_rows),
                "sql":        c_sql,
            })

        resp_text = ai_result.get("response", f"Here are {len(charts_out)} charts.")
        resp_title = ai_result.get("title", prompt[:70])

        if ai_session:
            try:
                if ai_session.title == "New Chat":
                    ai_session.title = (resp_title or prompt[:60])[:200]
                assistant_msg = NetworkAiMessage(
                    session_id=ai_session.id, role="assistant",
                    content=resp_text,
                    content_json={
                        "title": resp_title, "chart_type": "multi_chart",
                        "charts": charts_out, "response": resp_text,
                        "provider": provider["provider"] if provider else "rule-based",
                    },
                )
                db.session.add(assistant_msg)
                db.session.commit()
            except Exception as e:
                _LOG.error("Failed to persist AI message: %s", e)

        return jsonify({
            "response":     resp_text,
            "query_type":   "multi_chart",
            "chart_type":   "multi_chart",
            "title":        resp_title,
            "charts":       charts_out,
            "data":         [],  # no single data array for multi-chart
            "columns":      [],
            "row_count":    sum(c["row_count"] for c in charts_out),
            "provider":     provider["provider"] if provider else "rule-based",
            "session_id":   ai_session.id if ai_session else None,
        })

    # ── SINGLE CHART: execute SQL normally ─────────────────────────────────────
    sql = ai_result.get("sql", "")
    if not sql or not sql.strip().upper().startswith("SELECT"):
        return jsonify({"error": "Could not generate a safe query"}), 400

    try:
        rows = _sql_with_timeout(sql, timeout_sec=15)
    except Exception as e:
        _LOG.warning("AI SQL execution failed: %s — SQL: %s", e, sql[:200])
        try:
            fallback = _rule_based_query(prompt, time_filter)
            sql2 = fallback.get("sql", "")
            if sql2 and sql2.strip().upper().startswith("SELECT"):
                rows = _sql_with_timeout(sql2, timeout_sec=10)
                ai_result.update(fallback)
                sql = sql2
                if not provider:
                    provider = {"provider": "rule-based-fallback"}
            else:
                rows = []
        except Exception:
            rows = []

    columns = list(rows[0].keys()) if rows else []
    has_geo = any(r.get("lat") or r.get("latitude") for r in rows)
    safe_rows = [{k: _serial(v) for k, v in r.items()} for r in rows]
    y_axes = ai_result.get("y_axes") or [
        c for c in columns[1:5]
        if c not in ("lat", "lng", "latitude", "longitude", "site_id", "cell_id", "cluster", "region", "technology")
    ]

    # ── Persist assistant message to session ────────────────────────────────────
    resp_text = ai_result.get("response", f"Found {len(rows)} results.")
    resp_title = ai_result.get("title", prompt[:70])
    resp_chart = ai_result.get("chart_type", ai_result.get("query_type", "bar"))

    if ai_session:
        try:
            if ai_session.title == "New Chat":
                ai_session.title = (resp_title or prompt[:60])[:200]
            assistant_msg = NetworkAiMessage(
                session_id=ai_session.id,
                role="assistant",
                content=resp_text,
                content_json={
                    "title": resp_title,
                    "data": safe_rows,
                    "columns": columns,
                    "x_axis": ai_result.get("x_axis", columns[0] if columns else ""),
                    "y_axes": y_axes,
                    "chart_type": resp_chart,
                    "chart_config": ai_result.get("chart_config", {}),
                    "row_count": len(rows),
                    "sql": sql,
                    "provider": provider["provider"] if provider else "rule-based",
                    "response": resp_text,
                },
            )
            db.session.add(assistant_msg)
            db.session.commit()
        except Exception as e:
            _LOG.error("Failed to persist AI message: %s", e)

    return jsonify({
        "response":      resp_text,
        "query_type":    ai_result.get("query_type", "bar"),
        "chart_type":    resp_chart,
        "chart_config":  ai_result.get("chart_config", {}),
        "title":         resp_title,
        "x_axis":        ai_result.get("x_axis", columns[0] if columns else ""),
        "y_axes":        y_axes,
        "data":          safe_rows,
        "columns":       columns,
        "row_count":     len(rows),
        "sql":           sql,
        "filter_update": ai_result.get("filter_update", {}),
        "tab":           ai_result.get("tab"),
        "show_map":      has_geo or ai_result.get("show_map", False),
        "provider":      provider["provider"] if provider else "rule-based",
        "session_id":    ai_session.id if ai_session else None,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Follow-up / Conversation Context Handling
# ─────────────────────────────────────────────────────────────────────────────

# Words that indicate a follow-up referencing the previous chart/result
_FOLLOWUP_CUES = {
    'scale', 'zoom', 'resize', 'bigger', 'smaller', 'enlarge', 'expand',
    'change', 'modify', 'update', 'adjust', 'convert', 'switch', 'make it',
    'the graph', 'the chart', 'this graph', 'this chart', 'that chart',
    'same', 'previous', 'last one', 'above', 'earlier',
    'bar chart', 'line chart', 'pie chart', 'area chart', 'table',
    'add', 'include', 'also show', 'overlay',
    'remove', 'hide', 'exclude',
    'more days', 'fewer days', 'last 30', 'last 60', 'extend',
    'different color', 'colour',
    'yes', 'ok', 'sure', 'do it', 'go ahead', 'please do',
}

def _is_followup(prompt_lower: str) -> bool:
    """Check if query is a follow-up referencing a previous chart/result."""
    import re
    # If it has specific site IDs or strong KPI keywords with day count,
    # it's a NEW query, not a follow-up
    has_site = bool(re.findall(r'[a-z]{2,}[_\-][a-z]{2,}[_\-]\d{3,}', prompt_lower))
    has_days = bool(re.search(r'last\s+\d+\s*days?', prompt_lower))
    if has_site and has_days:
        return False
    # Check for follow-up cues
    for cue in _FOLLOWUP_CUES:
        if cue in prompt_lower:
            return True
    # Very short queries (< 8 words) without KPI keywords are likely follow-ups
    words = prompt_lower.split()
    if len(words) <= 6:
        return True
    return False


def _handle_followup(prompt_orig: str, p: str, prev: dict, time_filter: str) -> dict:
    """
    Handle a follow-up query using the previous assistant message's context.
    prev = content_json from the last assistant message (has sql, title, chart_type, etc.)
    Returns a result dict or None if it can't handle the follow-up.
    """
    import re

    prev_sql = prev.get("sql", "")
    prev_title = prev.get("title", "")
    prev_chart = prev.get("chart_type", "bar")
    prev_y = prev.get("y_axes", [])
    prev_x = prev.get("x_axis", "date")
    prev_response = prev.get("response", "")

    # If prev was multi_chart, get the charts list
    prev_charts = prev.get("charts", [])

    if not prev_sql and not prev_charts:
        return None

    # ── Chart type change ──────────────────────────────────────────────────
    new_chart = None
    if 'bar' in p and 'chart' in p:
        new_chart = 'bar'
    elif 'line' in p and ('chart' in p or 'graph' in p):
        new_chart = 'line'
    elif 'pie' in p:
        new_chart = 'pie'
    elif 'area' in p and ('chart' in p or 'graph' in p):
        new_chart = 'area'
    elif 'table' in p and ('show' in p or 'as' in p or 'view' in p or 'convert' in p):
        new_chart = 'bar'  # table view handled by frontend

    if new_chart and prev_sql:
        return {
            "sql": prev_sql,
            "query_type": new_chart,
            "chart_type": new_chart,
            "title": prev_title,
            "x_axis": prev_x,
            "y_axes": prev_y,
            "response": f"Changed chart type to {new_chart}.",
        }

    # ── Time range change ──────────────────────────────────────────────────
    new_days = None
    m = re.search(r'(?:last|past|recent)\s+(\d+)\s*days?', p)
    if m:
        new_days = int(m.group(1))
    elif re.search(r'(\d+)\s*days?', p):
        new_days = int(re.search(r'(\d+)\s*days?', p).group(1))
    elif 'more days' in p or 'extend' in p or 'longer' in p:
        new_days = 30  # default extend

    if new_days and prev_sql:
        # Replace the date interval in the SQL
        updated_sql = re.sub(
            r"INTERVAL\s+'(\d+)\s+days?'",
            f"INTERVAL '{new_days} days'",
            prev_sql
        )
        # Update title
        updated_title = re.sub(r'\(last \d+d\)', f'(last {new_days}d)', prev_title)
        if updated_title == prev_title:
            updated_title = prev_title + f" (last {new_days}d)"

        # For multi_chart, update each chart's SQL
        if prev_charts:
            new_charts = []
            for ch in prev_charts:
                ch_sql = re.sub(
                    r"INTERVAL\s+'(\d+)\s+days?'",
                    f"INTERVAL '{new_days} days'",
                    ch.get("sql", "")
                )
                ch_title = re.sub(r'\(last \d+d\)', f'(last {new_days}d)', ch.get("title", ""))
                new_charts.append({**ch, "sql": ch_sql, "title": ch_title})
            return {
                "multi_chart": True,
                "charts": new_charts,
                "sql": new_charts[0]["sql"],
                "query_type": "multi_chart",
                "chart_type": "multi_chart",
                "title": " & ".join(c["title"] for c in new_charts)[:80],
                "x_axis": "date",
                "y_axes": ["value"],
                "response": f"Updated time range to last {new_days} days.",
            }

        return {
            "sql": updated_sql,
            "query_type": prev_chart,
            "chart_type": prev_chart,
            "title": updated_title,
            "x_axis": prev_x,
            "y_axes": prev_y,
            "response": f"Updated to show last {new_days} days.",
        }

    # ── Scale / zoom / resize — parse specific scale requests ────────────────
    if any(w in p for w in ['scale', 'zoom', 'resize', 'bigger', 'smaller',
                             'enlarge', 'expand', 'y axis', 'y-axis', 'range',
                             'difference', 'interval', 'step', 'tick']):
        # Parse a numeric scale/interval: "scale to 10", "10 difference", "interval 5"
        scale_num = None
        sm = re.search(r'(?:to|of|at|interval|difference|step|every)\s*(\d+)', p)
        if sm:
            scale_num = int(sm.group(1))
        elif re.search(r'(\d+)\s*(?:difference|interval|step|tick|unit|gap)', p):
            scale_num = int(re.search(r'(\d+)\s*(?:difference|interval|step|tick|unit|gap)', p).group(1))

        cfg = prev.get("chart_config", {}) or {}
        if scale_num:
            cfg["y_tick_interval"] = scale_num
            resp_msg = f"Changed Y-axis scale to intervals of {scale_num}."
        else:
            # No specific number — just reset to auto-scale
            cfg.pop("y_tick_interval", None)
            resp_msg = "Re-rendered chart with auto-scaled axes."

        if prev_charts:
            for ch in prev_charts:
                ch_cfg = ch.get("chart_config", {}) or {}
                if scale_num:
                    ch_cfg["y_tick_interval"] = scale_num
                ch["chart_config"] = ch_cfg
            return {
                "multi_chart": True,
                "charts": prev_charts,
                "sql": prev_charts[0].get("sql", ""),
                "query_type": "multi_chart",
                "chart_type": "multi_chart",
                "title": prev_title,
                "x_axis": "date",
                "y_axes": ["value"],
                "chart_config": cfg,
                "response": resp_msg,
            }
        return {
            "sql": prev_sql,
            "query_type": prev_chart,
            "chart_type": prev_chart,
            "title": prev_title,
            "x_axis": prev_x,
            "y_axes": prev_y,
            "chart_config": cfg,
            "response": resp_msg,
        }

    # ── Generic follow-up: just re-run the previous query ──────────────────
    # For very short queries like "yes", "ok", "show me", etc. re-render previous
    words = p.split()
    if len(words) <= 5 or any(w in p for w in ['same', 'again', 'previous', 'repeat',
                                                 'the graph', 'the chart', 'this',
                                                 'that', 'above', 'earlier']):
        if prev_charts:
            return {
                "multi_chart": True,
                "charts": prev_charts,
                "sql": prev_charts[0].get("sql", ""),
                "query_type": "multi_chart",
                "chart_type": "multi_chart",
                "title": prev_title,
                "x_axis": "date",
                "y_axes": ["value"],
                "response": prev_response or "Here are the previous results again.",
            }
        return {
            "sql": prev_sql,
            "query_type": prev_chart,
            "chart_type": prev_chart,
            "title": prev_title,
            "x_axis": prev_x,
            "y_axes": prev_y,
            "response": prev_response or "Here are the previous results.",
        }

    return None  # Could not handle as follow-up; fall through to normal processing


# ─────────────────────────────────────────────────────────────────────────────
# Rule-based NL → SQL fallback engine
# ─────────────────────────────────────────────────────────────────────────────
def _rule_based_query(prompt: str, time_filter: str = '1=1', prev_context: dict = None) -> dict:
    """
    Smart NL → SQL rule-based engine.
    Supports: specific site IDs, exact day counts, multi-part/compound queries,
    single KPI trend requests, and FOLLOW-UP queries using prev_context.
    """
    import re
    p = prompt.lower()

    # ── Follow-up detection ──────────────────────────────────────────────────
    # If the user query is vague/contextual AND we have previous context,
    # treat it as a follow-up referencing the previous chart/result.
    if prev_context and _is_followup(p):
        result = _handle_followup(prompt, p, prev_context, time_filter)
        if result:
            return result

    # ── Extract site IDs (e.g. GUR_LTE_1500) ────────────────────────────────
    site_ids = re.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', prompt)
    site_ids = list(dict.fromkeys(site_ids))  # unique, preserve order

    # ── KPI keyword → (kpi_name, alias) mapping ─────────────────────────────
    KPI_MAP = {
        'cssr':       ('LTE Call Setup Success Rate', 'cssr'),
        'call setup': ('LTE Call Setup Success Rate', 'cssr'),
        'rrc':        ('LTE RRC Setup Success Rate', 'rrc_sr'),
        'drop':       ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'cdr':        ('E-RAB Call Drop Rate_1', 'drop_rate'),
        'throughput':  ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'tput':       ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'dl throughput': ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'speed':      ('LTE DL - Cell Ave Throughput', 'dl_tput'),
        'prb':        ('DL PRB Utilization (1BH)', 'dl_prb'),
        'congestion': ('DL PRB Utilization (1BH)', 'dl_prb'),
        'availability': ('Availability', 'availability'),
        'availab':    ('Availability', 'availability'),
        'latency':    ('Average Latency Downlink', 'latency'),
        'delay':      ('Average Latency Downlink', 'latency'),
        'volte':      ('VoLTE Traffic Erlang', 'volte_erl'),
        'handover':   ('LTE Intra-Freq HO Success Rate', 'ho_sr'),
        'volume':     ('DL Data Total Volume', 'dl_volume'),
        'traffic':    ('DL Data Total Volume', 'dl_volume'),
        'connected':  ('Ave RRC Connected Ue', 'avg_rrc_ue'),
        'users':      ('Ave RRC Connected Ue', 'avg_rrc_ue'),
    }

    def _detect_kpis(text):
        """Detect KPIs mentioned in text, return list of (kpi_name, alias)."""
        found = []
        seen = set()
        t = text.lower()
        # Check longer phrases first
        for kw in sorted(KPI_MAP.keys(), key=len, reverse=True):
            if kw in t and KPI_MAP[kw][0] not in seen:
                found.append(KPI_MAP[kw])
                seen.add(KPI_MAP[kw][0])
        return found

    def _extract_days(text):
        """Extract 'last N days' from text. Returns int or None."""
        m = re.search(r'last\s+(\d+)\s*days?', text.lower())
        return int(m.group(1)) if m else None

    # ── Detect if this is a compound query ───────────────────────────────────
    # Split on " and " / " also " between distinct request parts
    # Only split if both halves have KPI keywords or site IDs
    def _try_split_compound(prompt_text):
        """Try to split compound queries like 'X for site A and Y for site B'."""
        # Split on ' and ' that separates distinct requests
        for sep in [' and ', ' also ', ' plus ', ' along with ']:
            if sep not in prompt_text.lower():
                continue
            idx = prompt_text.lower().index(sep)
            part1 = prompt_text[:idx].strip()
            part2 = prompt_text[idx+len(sep):].strip()
            # Both parts need some substance (KPI or site reference)
            kpis1 = _detect_kpis(part1)
            kpis2 = _detect_kpis(part2)
            sites1 = re.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', part1)
            sites2 = re.findall(r'[A-Za-z]{2,}[_\-][A-Za-z]{2,}[_\-]\d{3,}', part2)
            # Valid compound ONLY if both parts have a KPI AND at least one part has a site
            # This prevents "throughput and prb for SITE" from being split
            # (that should be handled as multi-KPI single-site query instead)
            if kpis1 and kpis2 and (sites1 or sites2):
                # If one part lacks a site, inherit from the other or from global site_ids
                if not sites1 and sites2:
                    sites1 = site_ids[:1] if site_ids else sites2[:1]
                elif not sites2 and sites1:
                    sites2 = site_ids[:1] if site_ids else sites1[:1]
                return [(part1, kpis1, sites1), (part2, kpis2, sites2)]
        return None

    # ── Check data source ────────────────────────────────────────────────────
    try:
        cnt = _sql("SELECT COUNT(*) AS n FROM kpi_data")
        USE_KD = int((cnt[0].get("n") or 0) if cnt else 0) > 0
    except Exception:
        USE_KD = False

    if not USE_KD:
        # Legacy fallback — simple queries against network_kpi_timeseries
        return _rule_based_legacy(p, time_filter)

    GEO_JOIN = "LEFT JOIN telecom_sites ts ON LOWER(k.site_id) = LOWER(ts.site_id)"

    # ── Try compound query first ─────────────────────────────────────────────
    # Returns multi_chart: each part gets its own separate chart
    compound = _try_split_compound(prompt)
    if compound and len(compound) >= 2:
        charts = []
        for part_text, part_kpis, part_sites in compound:
            days = _extract_days(part_text)
            kpi = part_kpis[0] if part_kpis else ('DL PRB Utilization (1BH)', 'dl_prb')
            site = part_sites[0] if part_sites else None
            date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE" if days else "AND k.date <= CURRENT_DATE"
            site_clause = f"AND k.site_id = '{site}'" if site else ""

            chart_title = kpi[0]
            if site:
                chart_title += f" — {site}"
            if days:
                chart_title += f" (last {days}d)"

            charts.append({
                "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {kpi[1]}
                    FROM kpi_data k
                    WHERE k.kpi_name = '{kpi[0]}'
                      AND k.data_level = 'site' AND k.value IS NOT NULL
                      {site_clause} {date_clause}
                    GROUP BY k.date ORDER BY k.date""",
                "chart_type": "line",
                "title": chart_title,
                "x_axis": "date",
                "y_axes": [kpi[1]],
            })

        chart_labels = [c["title"] for c in charts]
        return {
            "multi_chart": True,
            "charts": charts,
            "sql": charts[0]["sql"],  # primary SQL for logging
            "query_type": "multi_chart",
            "chart_type": "multi_chart",
            "title": " & ".join(chart_labels)[:80],
            "x_axis": "date",
            "y_axes": ["value"],
            "response": f"Here are {len(charts)} charts as requested: {', '.join(chart_labels)}.",
        }

    # ── Single query with specific site + KPI + days ─────────────────────────
    detected_kpis = _detect_kpis(p)
    days = _extract_days(p)

    # If specific site(s) mentioned with trend/days keywords → time series
    is_trend = bool(days) or 'trend' in p or 'over time' in p or 'history' in p or 'daily' in p or 'last' in p
    if site_ids and detected_kpis and is_trend:
        if len(site_ids) == 1 and len(detected_kpis) == 1:
            # Single site, single KPI trend
            kpi_name, alias = detected_kpis[0]
            site = site_ids[0]
            date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE" if days else "AND k.date <= CURRENT_DATE"
            return {
                "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {alias}
                    FROM kpi_data k
                    WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                      AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                    GROUP BY k.date ORDER BY k.date""",
                "query_type": "line", "chart_type": "line",
                "title": f"{kpi_name} — {site}" + (f" (last {days}d)" if days else ""),
                "x_axis": "date", "y_axes": [alias],
                "response": f"Trend of {kpi_name} for site {site}" + (f" over last {days} days." if days else "."),
            }

        # Multiple KPIs for single site → composed
        if len(site_ids) == 1 and len(detected_kpis) >= 2:
            site = site_ids[0]
            date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE" if days else "AND k.date <= CURRENT_DATE"
            parts_sql = []
            for kpi_name, alias in detected_kpis[:3]:
                parts_sql.append(f"""SELECT k.date::text AS date, k.site_id,
                       AVG(k.value) AS value, '{kpi_name}' AS kpi_name
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi_name}' AND k.site_id = '{site}'
                  AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date, k.site_id""")
            return {
                "sql": "\nUNION ALL\n".join(parts_sql) + "\nORDER BY date",
                "query_type": "composed", "chart_type": "composed",
                "title": f"{site} — " + " & ".join(k[1] for k in detected_kpis[:3]),
                "x_axis": "date", "y_axes": ["value"],
                "response": f"Showing {', '.join(k[0] for k in detected_kpis[:3])} for {site}.",
            }

        # Multiple sites, possibly different KPIs → UNION ALL
        if len(site_ids) >= 2:
            parts_sql = []
            titles = []
            for i, site in enumerate(site_ids[:4]):
                kpi = detected_kpis[min(i, len(detected_kpis)-1)]
                date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE" if days else "AND k.date <= CURRENT_DATE"
                parts_sql.append(f"""SELECT k.date::text AS date, k.site_id,
                       AVG(k.value) AS value, '{kpi[0]}' AS kpi_name
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi[0]}' AND k.site_id = '{site}'
                  AND k.data_level = 'site' AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date, k.site_id""")
                titles.append(f"{kpi[1]} {site}")
            return {
                "sql": "\nUNION ALL\n".join(parts_sql) + "\nORDER BY date",
                "query_type": "composed", "chart_type": "composed",
                "title": " & ".join(titles)[:60],
                "x_axis": "date", "y_axes": ["value"],
                "response": f"Comparing trends for {', '.join(site_ids[:4])}.",
            }

    # ── Single site, no trend → bar with specific KPIs ───────────────────────
    if site_ids and detected_kpis and not is_trend:
        site = site_ids[0]
        cases = ", ".join(f"AVG(CASE WHEN k.kpi_name = '{kn}' THEN k.value END) AS {al}" for kn, al in detected_kpis[:5])
        in_cl = ", ".join(f"'{kn}'" for kn, _ in detected_kpis[:5])
        return {
            "sql": f"""SELECT k.site_id, {cases}
                FROM kpi_data k
                WHERE k.site_id = '{site}' AND k.data_level = 'site' AND k.value IS NOT NULL
                  AND k.kpi_name IN ({in_cl})
                GROUP BY k.site_id""",
            "query_type": "bar", "chart_type": "bar",
            "title": f"KPIs for {site}",
            "x_axis": "site_id", "y_axes": [al for _, al in detected_kpis[:5]],
            "response": f"Showing requested KPIs for site {site}.",
        }

    # ── Generic trend (no specific site) ─────────────────────────────────────
    if is_trend and detected_kpis:
        kpi_name, alias = detected_kpis[0]
        date_clause = f"AND k.date >= CURRENT_DATE - INTERVAL '{days} days' AND k.date <= CURRENT_DATE" if days else "AND k.date <= CURRENT_DATE"
        return {
            "sql": f"""SELECT k.date::text AS date, AVG(k.value) AS {alias},
                       MIN(k.value) AS min_val, MAX(k.value) AS max_val
                FROM kpi_data k
                WHERE k.kpi_name = '{kpi_name}' AND k.data_level = 'site'
                  AND k.value IS NOT NULL {date_clause}
                GROUP BY k.date ORDER BY k.date LIMIT 60""",
            "query_type": "line", "chart_type": "line",
            "title": f"{kpi_name} Daily Trend",
            "x_axis": "date", "y_axes": [alias, "min_val", "max_val"],
            "response": f"Daily trend of {kpi_name} across the network.",
        }

    # ── Keyword-based fallback (no specific site/days) ───────────────────────
    N = 10
    nums = re.findall(r'\b(\d+)\b', p)
    if nums:
        N = min(int(nums[0]), 100)

    def _kd_site_query(kpi_names_and_aliases, order_col, order_dir="DESC"):
        case_parts = []
        kpi_names_for_in = []
        for kpi_name, alias in kpi_names_and_aliases:
            case_parts.append(f"AVG(CASE WHEN k.kpi_name = '{kpi_name}' THEN k.value END) AS {alias}")
            kpi_names_for_in.append(f"'{kpi_name}'")
        cases = ",\n                   ".join(case_parts)
        in_clause = ", ".join(kpi_names_for_in)
        return f"""SELECT k.site_id, MAX(ts.zone) AS cluster,
                       AVG(ts.latitude) AS lat, AVG(ts.longitude) AS lng,
                       {cases}
                FROM kpi_data k {GEO_JOIN}
                WHERE k.data_level = 'site' AND k.value IS NOT NULL
                  AND k.kpi_name IN ({in_clause})
                GROUP BY k.site_id
                ORDER BY {order_col} {order_dir} NULLS LAST LIMIT {N}"""

    if 'rrc' in p or 'accessibility' in p:
        sql = _kd_site_query([("LTE RRC Setup Success Rate","lte_rrc_setup_sr"),("LTE E-RAB Setup Success Rate","erab_setup_sr"),("LTE Call Setup Success Rate","lte_call_setup_sr"),("DL PRB Utilization (1BH)","dl_prb_util")],"lte_rrc_setup_sr","ASC")
        return {"sql":sql,"query_type":"bar","title":f"RRC / Accessibility — Bottom {N}","x_axis":"site_id","y_axes":["lte_rrc_setup_sr","erab_setup_sr"],"response":f"Showing {N} sites with lowest RRC Setup Success Rate."}

    if 'volte' in p:
        sql = _kd_site_query([("VoLTE Traffic Erlang","volte_traffic_erl"),("VoLTE Traffic DL","volte_dl"),("VoLTE Traffic UL","volte_ul")],"volte_traffic_erl","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} Sites — VoLTE Traffic","x_axis":"site_id","y_axes":["volte_traffic_erl"],"response":f"Showing {N} sites by VoLTE Erlang traffic."}

    if 'handover' in p or ' ho ' in p or 'hsr' in p:
        sql = _kd_site_query([("LTE Intra-Freq HO Success Rate","intra_freq_ho_sr"),("LTE RRC Setup Success Rate","lte_rrc_setup_sr"),("DL PRB Utilization (1BH)","dl_prb_util")],"intra_freq_ho_sr","ASC")
        return {"sql":sql,"query_type":"bar","title":f"Bottom {N} — HO Success Rate","x_axis":"site_id","y_axes":["intra_freq_ho_sr"],"response":f"Showing {N} sites with worst Handover Success Rate."}

    if 'drop' in p or 'cdr' in p:
        sql = _kd_site_query([("E-RAB Call Drop Rate_1","erab_drop_rate"),("DL PRB Utilization (1BH)","dl_prb_util"),("LTE DL - Cell Ave Throughput","dl_cell_tput")],"erab_drop_rate","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} Call Drop Offenders","x_axis":"site_id","y_axes":["erab_drop_rate","dl_prb_util"],"response":f"Showing {N} sites with highest E-RAB call drop rate."}

    if 'prb' in p or 'congestion' in p or 'congested' in p or 'overload' in p:
        sql = _kd_site_query([("DL PRB Utilization (1BH)","dl_prb_util"),("UL PRB Utilization (1BH)","ul_prb_util"),("LTE DL - Cell Ave Throughput","dl_cell_tput"),("Ave RRC Connected Ue","avg_rrc_ue")],"dl_prb_util","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} Congested Sites (PRB)","x_axis":"site_id","y_axes":["dl_prb_util","ul_prb_util","dl_cell_tput"],"response":f"Showing top {N} sites by DL PRB Utilization."}

    if 'throughput' in p or 'tput' in p or 'speed' in p or 'mbps' in p:
        order = "ASC" if any(w in p for w in ['worst','low','bad','poor']) else "DESC"
        sql = _kd_site_query([("LTE DL - Cell Ave Throughput","dl_cell_tput"),("LTE UL - Cell Ave Throughput","ul_cell_tput"),("DL PRB Utilization (1BH)","dl_prb_util")],"dl_cell_tput",order)
        return {"sql":sql,"query_type":"bar","title":f"{'Bottom' if order=='ASC' else 'Top'} {N} — DL Throughput","x_axis":"site_id","y_axes":["dl_cell_tput","ul_cell_tput"],"response":f"{'Worst' if order=='ASC' else 'Best'} {N} sites by throughput."}

    if 'cssr' in p or 'call setup' in p or 'setup success' in p:
        sql = _kd_site_query([("LTE Call Setup Success Rate","lte_cssr"),("LTE E-RAB Setup Success Rate","erab_setup_sr"),("E-RAB Call Drop Rate_1","erab_drop_rate")],"lte_cssr","ASC")
        return {"sql":sql,"query_type":"bar","title":f"Bottom {N} — Call Setup Success","x_axis":"site_id","y_axes":["lte_cssr","erab_setup_sr"],"response":f"Showing {N} sites with lowest CSSR."}

    if 'zone' in p or 'cluster' in p or 'cbd' in p or 'urban' in p or 'compar' in p:
        return {"sql":f"""SELECT ts.zone AS cluster, COUNT(DISTINCT k.site_id) AS sites,
                       AVG(CASE WHEN k.kpi_name='DL PRB Utilization (1BH)' THEN k.value END) AS avg_prb,
                       AVG(CASE WHEN k.kpi_name='LTE DL - Cell Ave Throughput' THEN k.value END) AS avg_tput,
                       AVG(CASE WHEN k.kpi_name='E-RAB Call Drop Rate_1' THEN k.value END) AS avg_drop,
                       AVG(CASE WHEN k.kpi_name='LTE RRC Setup Success Rate' THEN k.value END) AS avg_rrc_sr
                FROM kpi_data k {GEO_JOIN}
                WHERE k.data_level='site' AND k.value IS NOT NULL AND ts.zone IS NOT NULL
                  AND k.kpi_name IN ('DL PRB Utilization (1BH)','LTE DL - Cell Ave Throughput','E-RAB Call Drop Rate_1','LTE RRC Setup Success Rate')
                GROUP BY ts.zone ORDER BY avg_prb DESC NULLS LAST""",
                "query_type":"bar","title":"Zone-wise KPI Comparison","x_axis":"cluster","y_axes":["avg_prb","avg_tput","avg_drop"],"response":"Zone-level KPI comparison."}

    if 'availab' in p or 'downtime' in p or 'uptime' in p:
        sql = _kd_site_query([("Availability","availability"),("DL PRB Utilization (1BH)","dl_prb_util")],"availability","ASC")
        return {"sql":sql,"query_type":"bar","title":"Sites with Lowest Availability","x_axis":"site_id","y_axes":["availability"],"response":"Sites with lowest availability."}

    if 'latency' in p or 'delay' in p or 'ping' in p:
        sql = _kd_site_query([("Average Latency Downlink","avg_latency"),("LTE DL - Usr Ave Throughput","dl_usr_tput")],"avg_latency","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} High Latency Sites","x_axis":"site_id","y_axes":["avg_latency"],"response":f"Showing {N} sites with highest latency."}

    if 'volume' in p or 'data volume' in p:
        sql = _kd_site_query([("DL Data Total Volume","dl_volume"),("UL Data Total Volume","ul_volume"),("DL PRB Utilization (1BH)","dl_prb_util")],"dl_volume","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} by Data Volume","x_axis":"site_id","y_axes":["dl_volume","ul_volume"],"response":f"Showing {N} sites with highest data volume."}

    if 'user' in p or 'ue' in p or 'connected' in p:
        sql = _kd_site_query([("Ave RRC Connected Ue","avg_rrc_ue"),("Max RRC Connected Ue","max_rrc_ue"),("DL PRB Utilization (1BH)","dl_prb_util")],"avg_rrc_ue","DESC")
        return {"sql":sql,"query_type":"bar","title":f"Top {N} by Connected Users","x_axis":"site_id","y_axes":["avg_rrc_ue","max_rrc_ue"],"response":f"Showing {N} sites with most users."}

    # ── Default
    sql = _kd_site_query([("DL PRB Utilization (1BH)","dl_prb_util"),("LTE DL - Cell Ave Throughput","dl_cell_tput"),("E-RAB Call Drop Rate_1","erab_drop_rate"),("LTE RRC Setup Success Rate","lte_rrc_setup_sr")],"dl_prb_util","DESC")
    return {"sql":sql,"query_type":"bar","title":f"Top {N} Sites by PRB Utilization","x_axis":"site_id","y_axes":["dl_prb_util","dl_cell_tput","erab_drop_rate"],"response":f"Top {N} sites by PRB utilization."}


def _rule_based_legacy(p: str, time_filter: str) -> dict:
    """Legacy fallback using network_kpi_timeseries table."""
    import re
    N = 10
    nums = re.findall(r'\b(\d+)\b', p)
    if nums:
        N = min(int(nums[0]), 100)
    TBL = "network_kpi_timeseries"
    base = f"FROM {TBL} WHERE {time_filter}"

    if 'rrc' in p or 'accessibility' in p or 'access' in p:
        return {"sql": f"SELECT site_id, cluster, AVG(lte_rrc_setup_sr) as lte_rrc_setup_sr, AVG(erab_setup_sr) as erab_setup_sr, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY lte_rrc_setup_sr ASC NULLS LAST LIMIT {N}", "query_type":"bar","title":f"RRC — Bottom {N} Sites","x_axis":"site_id","y_axes":["lte_rrc_setup_sr","erab_setup_sr"],"response":f"Bottom {N} sites by RRC SR."}
    if 'volte' in p:
        return {"sql": f"SELECT site_id, cluster, AVG(volte_traffic_erl) as volte_traffic_erl, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY volte_traffic_erl DESC NULLS LAST LIMIT {N}", "query_type":"bar","title":f"VoLTE — Top {N}","x_axis":"site_id","y_axes":["volte_traffic_erl"],"response":f"Top {N} sites by VoLTE Erlang."}
    if 'drop' in p or 'cdr' in p:
        return {"sql": f"SELECT site_id, cluster, AVG(COALESCE(erab_drop_rate,call_drop_rate,0)) as erab_drop_rate, AVG(COALESCE(dl_prb_util,prb_utilization)) as avg_prb, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY erab_drop_rate DESC NULLS LAST LIMIT {N}", "query_type":"mixed","title":f"Call Drop — Top {N}","x_axis":"site_id","y_axes":["erab_drop_rate","avg_prb"],"response":f"Top {N} call drop sites."}
    if 'zone' in p or 'compar' in p or 'cluster' in p:
        return {"sql": f"SELECT cluster, COUNT(DISTINCT site_id) as sites, AVG(COALESCE(dl_prb_util,prb_utilization)) as avg_prb, AVG(COALESCE(dl_cell_tput,throughput_dl)) as avg_tput {base} GROUP BY cluster ORDER BY avg_prb DESC", "query_type":"bar","title":"Zone Comparison","x_axis":"cluster","y_axes":["avg_prb","avg_tput"],"response":"Zone KPI comparison."}
    return {"sql": f"SELECT site_id, cluster, AVG(COALESCE(dl_prb_util,prb_utilization)) as avg_prb, AVG(COALESCE(dl_cell_tput,throughput_dl)) as avg_tput, AVG(COALESCE(erab_drop_rate,call_drop_rate,0)) as avg_drop, AVG(latitude) as lat, AVG(longitude) as lng {base} GROUP BY site_id,cluster ORDER BY avg_prb DESC NULLS LAST LIMIT {N}", "query_type":"mixed","title":f"Top {N} Sites by PRB","x_axis":"site_id","y_axes":["avg_prb","avg_tput"],"response":f"Top {N} sites by PRB utilization."}


# ─────────────────────────────────────────────────────────────────────────────
# Network AI Session CRUD endpoints
# ─────────────────────────────────────────────────────────────────────────────

@network_ai_bp.route("/api/network/ai-sessions", methods=["GET"])
@jwt_required()
def list_ai_sessions():
    _ensure_ai_session_tables()
    uid = int(get_jwt_identity())
    sessions = (NetworkAiSession.query
                .filter_by(user_id=uid)
                .order_by(NetworkAiSession.last_message_at.desc())
                .limit(50)
                .all())
    return jsonify({"sessions": [s.to_dict() for s in sessions]})


@network_ai_bp.route("/api/network/ai-sessions", methods=["POST"])
@jwt_required()
def create_ai_session():
    _ensure_ai_session_tables()
    uid = int(get_jwt_identity())
    session = NetworkAiSession(user_id=uid, title="New Chat")
    db.session.add(session)
    db.session.commit()
    return jsonify({"session": session.to_dict()}), 201


@network_ai_bp.route("/api/network/ai-sessions/<int:session_id>/messages", methods=["GET"])
@jwt_required()
def get_ai_session_messages(session_id):
    uid = int(get_jwt_identity())
    session = db.session.get(NetworkAiSession, session_id)
    if not session or session.user_id != uid:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "session": session.to_dict(),
        "messages": [m.to_dict() for m in session.messages],
    })


@network_ai_bp.route("/api/network/ai-sessions/<int:session_id>", methods=["PUT"])
@jwt_required()
def update_ai_session(session_id):
    uid = int(get_jwt_identity())
    session = db.session.get(NetworkAiSession, session_id)
    if not session or session.user_id != uid:
        return jsonify({"error": "Not found"}), 404
    body = request.get_json(silent=True) or {}
    if "title" in body:
        session.title = body["title"][:200]
    db.session.commit()
    return jsonify({"session": session.to_dict()})


@network_ai_bp.route("/api/network/ai-sessions/<int:session_id>", methods=["DELETE"])
@jwt_required()
def delete_ai_session(session_id):
    uid = int(get_jwt_identity())
    session = db.session.get(NetworkAiSession, session_id)
    if not session or session.user_id != uid:
        return jsonify({"error": "Not found"}), 404
    NetworkAiMessage.query.filter_by(session_id=session_id).delete()
    db.session.delete(session)
    db.session.commit()
    return jsonify({"success": True})
