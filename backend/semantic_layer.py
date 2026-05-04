"""
semantic_layer.py — Semantic Layer for Network AI Chatbot
=========================================================
Maps human-readable metric concepts to physical column names across
different device types, enabling correct UNION/JOIN queries across
heterogeneous schemas.

Public API:
  - seed_catalog_from_existing_data()  — auto-discover & cluster KPIs into concepts
  - resolve_concepts(query, schema_cache) — NL query → matched concepts with mappings
  - compose_sql(concepts, intent, filters) — concepts → valid SQL
  - embed_schema_objects()              — generate & store vector embeddings
"""

import os
import json
import logging
import time
from typing import Optional

from dotenv import load_dotenv
from flask import current_app
from sqlalchemy import text as sa_text

load_dotenv()

_LOG = logging.getLogger("semantic_layer")

# ─── DB helper (reuse pattern from network_ai) ──────────────────────────────

def _get_db():
    """Lazy import to avoid circular dependency with network_ai."""
    from network_ai import db
    return db


def _sql(query: str, params: dict = None) -> list:
    """Execute raw SQL and return list of dicts."""
    db = _get_db()
    with db.engine.connect() as conn:
        result = conn.execute(sa_text(query), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def _sql_exec(query: str, params: dict = None):
    """Execute a write SQL statement (INSERT/UPDATE/DELETE)."""
    db = _get_db()
    with db.engine.connect() as conn:
        conn.execute(sa_text(query), params or {})
        conn.commit()


# ─── LLM client helper ──────────────────────────────────────────────────────

def _get_llm_client():
    """Build or reuse LLM client. Returns (client, model_name)."""
    from app import _build_llm_client
    return _build_llm_client()


def _llm_chat(messages: list, temperature: float = 0.1, max_tokens: int = 2000) -> str:
    """Send chat completion and return response text."""
    client, model = _get_llm_client()
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content.strip()


def _llm_embeddings(texts: list) -> list:
    """Generate embeddings using sentence-transformers (local, free, no API calls)."""
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")
        embeddings = model.encode(texts, show_progress_bar=False)
        return [emb.tolist() for emb in embeddings]
    except Exception as e:
        _LOG.warning("Embeddings generation failed: %s", e)
        return []


def _get_all_data_tables() -> list:
    """Dynamically discover all user data tables from the database (excludes system/migration tables)."""
    EXCLUDED = {"alembic_version", "metric_catalog", "metric_physical_mapping",
                "schema_embeddings", "spatial_ref_sys"}
    try:
        rows = _sql("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        return [r["table_name"] for r in rows if r["table_name"] not in EXCLUDED]
    except Exception:
        return []


# Columns that are structural (not metric data) — skip when looking for embeddable metrics
_STRUCTURAL_COLUMNS = frozenset({
    "id", "created_at", "updated_at", "deleted_at",
})


# ═══════════════════════════════════════════════════════════════════════════════
# 1. embed_schema_objects()
# ═══════════════════════════════════════════════════════════════════════════════

def embed_schema_objects():
    """
    Generate vector embeddings for all metric_catalog concepts and
    distinct kpi_names from the database. Upserts into schema_embeddings.

    Returns dict with counts.
    """
    _LOG.info("[SEMANTIC] Starting embed_schema_objects...")
    t0 = time.time()

    # Gather objects to embed
    objects_to_embed = []

    # 1a. Concepts from metric_catalog
    concepts = _sql("SELECT id, concept_name, display_name, description FROM metric_catalog")
    for c in concepts:
        text = f"{c['display_name']} ({c['concept_name']})"
        if c.get('description'):
            text += f" - {c['description']}"
        objects_to_embed.append({
            "object_type": "concept",
            "object_name": c["concept_name"],
            "source_table": "metric_catalog",
            "text": text,
        })

    # 1b. Distinct kpi_names from kpi_data (if table exists)
    try:
        kpi_names = _sql("SELECT DISTINCT kpi_name FROM kpi_data ORDER BY kpi_name")
        for row in kpi_names:
            name = row["kpi_name"]
            objects_to_embed.append({
                "object_type": "kpi_name",
                "object_name": name,
                "source_table": "kpi_data",
                "text": name,
            })
    except Exception:
        pass

    # 1c. Columns from all data tables (dynamically discovered)
    all_tables = _get_all_data_tables()
    for table in all_tables:
        try:
            cols = _sql("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = :tbl AND table_schema = 'public'
                ORDER BY ordinal_position
            """, {"tbl": table})
            for col in cols:
                col_name = col["column_name"]
                if col_name not in _STRUCTURAL_COLUMNS:
                    objects_to_embed.append({
                        "object_type": "column",
                        "object_name": col_name,
                        "source_table": table,
                        "text": f"{col_name} (column in {table})",
                    })
        except Exception:
            continue

    if not objects_to_embed:
        _LOG.warning("[SEMANTIC] No objects to embed")
        return {"embedded": 0, "elapsed": 0}

    # Generate embeddings in batches of 100
    batch_size = 100
    total_embedded = 0

    for i in range(0, len(objects_to_embed), batch_size):
        batch = objects_to_embed[i:i + batch_size]
        texts = [obj["text"] for obj in batch]
        embeddings = _llm_embeddings(texts)

        if not embeddings:
            _LOG.warning("[SEMANTIC] Embedding batch %d failed, skipping", i // batch_size)
            continue

        for obj, emb in zip(batch, embeddings):
            emb_json = json.dumps(emb)
            # Upsert: delete existing then insert
            _sql_exec("""
                DELETE FROM schema_embeddings
                WHERE object_type = :otype AND object_name = :oname AND source_table = :stbl
            """, {"otype": obj["object_type"], "oname": obj["object_name"], "stbl": obj["source_table"]})
            _sql_exec("""
                INSERT INTO schema_embeddings (object_type, object_name, source_table, embedding)
                VALUES (:otype, :oname, :stbl, :emb::jsonb)
            """, {"otype": obj["object_type"], "oname": obj["object_name"],
                  "stbl": obj["source_table"], "emb": emb_json})
            total_embedded += 1

    elapsed = time.time() - t0
    _LOG.info("[SEMANTIC] Embedded %d objects in %.1fs", total_embedded, elapsed)
    return {"embedded": total_embedded, "elapsed": round(elapsed, 2)}


# ═══════════════════════════════════════════════════════════════════════════════
# 2. seed_catalog_from_existing_data()
# ═══════════════════════════════════════════════════════════════════════════════

def seed_catalog_from_existing_data():
    """
    Auto-discover KPI names and columns from the database, use LLM to
    cluster them into semantic concepts, and populate metric_catalog +
    metric_physical_mapping.

    Returns dict with counts.
    """
    _LOG.info("[SEMANTIC] Starting seed_catalog_from_existing_data...")
    t0 = time.time()

    # Gather all metric-like things from the DB
    kpi_names = _sql("SELECT DISTINCT kpi_name FROM kpi_data ORDER BY kpi_name")
    kpi_list = [r["kpi_name"] for r in kpi_names]

    # Also gather numeric columns from all data tables (dynamically discovered)
    other_metrics = {}
    all_tables = _get_all_data_tables()
    for table in all_tables:
        try:
            cols = _sql("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = :tbl AND table_schema = 'public'
                  AND data_type IN ('numeric', 'double precision', 'real', 'integer', 'bigint')
                  AND column_name NOT IN ('id')
                ORDER BY ordinal_position
            """, {"tbl": table})
            col_list = [c["column_name"] for c in cols if c["column_name"] not in _STRUCTURAL_COLUMNS]
            if col_list:
                other_metrics[table] = col_list
        except Exception:
            continue

    # Ask LLM to cluster into concepts — fully dynamic, domain-agnostic prompt
    prompt = f"""You are a data engineer. Given these metric/column names from a database, group them into
semantic concepts (metrics that measure the same thing but may have different names across tables/sources).

KPI names from kpi_data table (EAV pattern — each row has a kpi_name string and a numeric value):
{json.dumps(kpi_list[:200], indent=2)}

Numeric columns from other tables in the same database:
{json.dumps(other_metrics, indent=2)}

Return a JSON array of concepts. Each concept should have:
- "concept_name": snake_case canonical name
- "display_name": human-readable name
- "unit": measurement unit if inferrable, or null
- "description": brief description
- "mappings": array of physical mappings, each with:
  - "table_name": exact table name from the lists above
  - "column_expr": column name or expression (for kpi_data EAV rows use "value")
  - "filter_expr": SQL WHERE filter (for kpi_data rows: "kpi_name = '<exact_name>'"), null for direct columns
  - "device_type": inferred source/device/vendor type, or "generic" if unknown

Group metrics that measure the same thing under one concept even if names differ.
Only output the JSON array, nothing else."""

    messages = [
        {"role": "system", "content": "You are a data engineering assistant. Output valid JSON only."},
        {"role": "user", "content": prompt},
    ]

    response = _llm_chat(messages, temperature=0.1, max_tokens=4000)

    # Parse LLM response
    try:
        # Strip markdown code fences if present
        cleaned = response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
            cleaned = cleaned.rsplit("```", 1)[0]
        concepts = json.loads(cleaned)
    except json.JSONDecodeError as e:
        _LOG.error("[SEMANTIC] Failed to parse LLM response: %s", e)
        return {"error": f"LLM response not valid JSON: {str(e)}", "raw": response[:500]}

    # Insert into database
    inserted_concepts = 0
    inserted_mappings = 0

    for concept in concepts:
        cname = concept.get("concept_name", "").strip()
        dname = concept.get("display_name", cname).strip()
        if not cname:
            continue

        # Upsert concept
        existing = _sql("SELECT id FROM metric_catalog WHERE concept_name = :cn", {"cn": cname})
        if existing:
            concept_id = existing[0]["id"]
        else:
            _sql_exec("""
                INSERT INTO metric_catalog (concept_name, display_name, unit, description)
                VALUES (:cn, :dn, :unit, :desc)
            """, {"cn": cname, "dn": dname, "unit": concept.get("unit"), "desc": concept.get("description")})
            new_row = _sql("SELECT id FROM metric_catalog WHERE concept_name = :cn", {"cn": cname})
            concept_id = new_row[0]["id"]
            inserted_concepts += 1

        # Insert mappings
        for mapping in concept.get("mappings", []):
            tbl = mapping.get("table_name", "").strip()
            col = mapping.get("column_expr", "").strip()
            if not tbl or not col:
                continue
            filt = mapping.get("filter_expr")
            dev = mapping.get("device_type", "generic")

            try:
                _sql_exec("""
                    INSERT INTO metric_physical_mapping (concept_id, table_name, column_expr, filter_expr, device_type)
                    VALUES (:cid, :tbl, :col, :filt, :dev)
                    ON CONFLICT (concept_id, table_name, device_type) DO UPDATE
                    SET column_expr = EXCLUDED.column_expr, filter_expr = EXCLUDED.filter_expr
                """, {"cid": concept_id, "tbl": tbl, "col": col, "filt": filt, "dev": dev})
                inserted_mappings += 1
            except Exception as e:
                _LOG.warning("[SEMANTIC] Mapping insert failed: %s", e)

    elapsed = time.time() - t0
    _LOG.info("[SEMANTIC] Seeded %d concepts, %d mappings in %.1fs", inserted_concepts, inserted_mappings, elapsed)
    return {
        "concepts_inserted": inserted_concepts,
        "mappings_inserted": inserted_mappings,
        "elapsed": round(elapsed, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. compose_sql()
# ═══════════════════════════════════════════════════════════════════════════════

def compose_sql(resolved_concepts: list, intent: str = "trend", filters: dict = None) -> str:
    """
    Given resolved concepts (with physical mappings), compose a SQL query
    that correctly pulls data from all mapped physical locations.

    Args:
        resolved_concepts: list of dicts from resolve_concepts()
        intent: query intent — "trend", "comparison", "threshold", "aggregate"
        filters: dict with optional keys: sites (list), date_range (int days), start_date, end_date

    Returns:
        SQL string ready to execute
    """
    filters = filters or {}
    sites = filters.get("sites", [])
    date_range = filters.get("date_range")
    start_date = filters.get("start_date")
    end_date = filters.get("end_date")

    # Build date filter clause
    date_clause = ""
    if start_date and end_date:
        date_clause = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
    elif date_range:
        date_clause = f"AND date >= CURRENT_DATE - INTERVAL '{date_range} days'"
    else:
        date_clause = "AND date >= CURRENT_DATE - INTERVAL '30 days'"

    # Build site filter clause
    site_clause = ""
    if sites:
        site_list = ", ".join(f"'{s}'" for s in sites)
        site_clause = f"AND site_id IN ({site_list})"

    parts = []

    for concept in resolved_concepts:
        concept_name = concept.get("concept_name", "unknown")
        display_name = concept.get("display_name", concept_name)
        mappings = concept.get("mappings", [])

        for mapping in mappings:
            table_name = mapping.get("table_name", "kpi_data")
            column_expr = mapping.get("column_expr", "value")
            filter_expr = mapping.get("filter_expr", "")
            device_type = mapping.get("device_type", "generic")

            # Build WHERE clause
            where_parts = ["1=1"]
            if filter_expr:
                where_parts.append(filter_expr)
            if date_clause:
                where_parts.append(date_clause.lstrip("AND "))
            if site_clause:
                where_parts.append(site_clause.lstrip("AND "))

            where_sql = " AND ".join(where_parts)

            if intent == "trend":
                sql = f"""SELECT date, '{display_name}' AS metric, '{device_type}' AS device_type,
    AVG({column_expr}) AS avg_value, site_id
FROM {table_name}
WHERE {where_sql}
GROUP BY date, site_id
ORDER BY date"""
            elif intent == "comparison":
                sql = f"""SELECT site_id, '{display_name}' AS metric, '{device_type}' AS device_type,
    AVG({column_expr}) AS avg_value
FROM {table_name}
WHERE {where_sql}
GROUP BY site_id
ORDER BY avg_value DESC"""
            elif intent == "threshold":
                sql = f"""SELECT site_id, date, '{display_name}' AS metric, '{device_type}' AS device_type,
    {column_expr} AS value
FROM {table_name}
WHERE {where_sql}
ORDER BY {column_expr} DESC
LIMIT 100"""
            else:  # aggregate
                sql = f"""SELECT '{display_name}' AS metric, '{device_type}' AS device_type,
    AVG({column_expr}) AS avg_value,
    MIN({column_expr}) AS min_value,
    MAX({column_expr}) AS max_value,
    COUNT(*) AS sample_count
FROM {table_name}
WHERE {where_sql}"""

            parts.append(sql)

    if not parts:
        return "SELECT 'No mappings found' AS error"

    # Combine with UNION ALL
    if len(parts) == 1:
        return parts[0]

    return "\nUNION ALL\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. resolve_concepts()
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_concepts(user_query: str, schema_cache: dict = None) -> list:
    """
    Given a user's natural language query, identify which metric concepts
    are being referenced and return their physical mappings.

    Returns list of dicts:
    [
        {
            "concept_name": str,
            "display_name": str,
            "confidence": float (0-1),
            "mappings": [{"table_name", "column_expr", "filter_expr", "device_type"}]
        }
    ]
    """
    # Get available concepts from catalog
    concepts = _sql("""
        SELECT mc.id, mc.concept_name, mc.display_name, mc.unit, mc.description
        FROM metric_catalog mc
        ORDER BY mc.concept_name
    """)

    if not concepts:
        return []

    # Build concept list for LLM
    concept_list = "\n".join(
        f"- {c['concept_name']}: {c['display_name']}" + (f" ({c['unit']})" if c.get('unit') else "")
        for c in concepts
    )

    # Also include raw kpi_names if available
    kpi_names_str = ""
    if schema_cache and schema_cache.get("kpi_names_list"):
        kpi_names_str = f"\n\nRaw KPI names in database:\n{', '.join(schema_cache['kpi_names_list'][:100])}"

    prompt = f"""Given this user query about data analytics:
"{user_query}"

Available metric concepts in our catalog:
{concept_list}
{kpi_names_str}

Which concepts from the catalog does this query reference? Consider synonyms and related terms.

Return a JSON array of matched concepts:
[
  {{
    "concept_name": "exact_name_from_catalog",
    "confidence": 0.0-1.0
  }}
]

Rules:
- Only return concepts that the user is clearly asking about
- confidence >= 0.9 for exact matches, 0.7-0.9 for synonym/related matches
- Return empty array [] if no concepts match
- Output ONLY valid JSON, nothing else"""

    messages = [
        {"role": "system", "content": "You are a metric concept resolver. Output valid JSON only."},
        {"role": "user", "content": prompt},
    ]

    response = _llm_chat(messages, temperature=0.0, max_tokens=500)

    # Parse response
    try:
        cleaned = response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
            cleaned = cleaned.rsplit("```", 1)[0]
        matched = json.loads(cleaned)
    except json.JSONDecodeError:
        _LOG.warning("[SEMANTIC] Failed to parse resolve_concepts response")
        return []

    if not matched:
        return []

    # Enrich with physical mappings
    result = []
    for match in matched:
        cname = match.get("concept_name", "")
        confidence = match.get("confidence", 0.0)

        # Look up concept + mappings
        concept_row = _sql("""
            SELECT id, concept_name, display_name, unit
            FROM metric_catalog WHERE concept_name = :cn
        """, {"cn": cname})

        if not concept_row:
            # Try pgvector fallback for fuzzy match
            fallback = _vector_search(cname, top_k=1)
            if fallback:
                concept_row = _sql("""
                    SELECT id, concept_name, display_name, unit
                    FROM metric_catalog WHERE concept_name = :cn
                """, {"cn": fallback[0]["object_name"]})
                confidence = min(confidence, 0.7)  # Lower confidence for fuzzy

        if not concept_row:
            continue

        concept_id = concept_row[0]["id"]
        mappings = _sql("""
            SELECT table_name, column_expr, filter_expr, device_type, priority
            FROM metric_physical_mapping
            WHERE concept_id = :cid
            ORDER BY priority DESC
        """, {"cid": concept_id})

        result.append({
            "concept_name": concept_row[0]["concept_name"],
            "display_name": concept_row[0]["display_name"],
            "unit": concept_row[0].get("unit"),
            "confidence": confidence,
            "mappings": [dict(m) for m in mappings],
        })

    return result


# ─── Vector search fallback (cosine similarity in Python, JSONB storage) ─────

def _cosine_similarity(a: list, b: list) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _vector_search(query_text: str, top_k: int = 5) -> list:
    """
    Search schema_embeddings by cosine similarity.
    Embeddings stored as JSONB arrays, similarity computed in Python.
    """
    try:
        # Generate embedding for query
        embeddings = _llm_embeddings([query_text])
        if not embeddings:
            return []

        query_emb = embeddings[0]

        # Fetch all embeddings from DB (small table — schema names only)
        rows = _sql("""
            SELECT object_type, object_name, source_table, embedding
            FROM schema_embeddings
            WHERE embedding IS NOT NULL
        """)

        if not rows:
            return []

        # Compute cosine similarity for each row
        scored = []
        for row in rows:
            stored_emb = row["embedding"]
            if isinstance(stored_emb, str):
                stored_emb = json.loads(stored_emb)
            if not isinstance(stored_emb, list):
                continue
            sim = _cosine_similarity(query_emb, stored_emb)
            if sim > 0.5:
                scored.append({
                    "object_type": row["object_type"],
                    "object_name": row["object_name"],
                    "source_table": row["source_table"],
                    "similarity": round(sim, 4),
                })

        # Sort by similarity descending, return top_k
        scored.sort(key=lambda x: x["similarity"], reverse=True)
        return scored[:top_k]
    except Exception as e:
        _LOG.debug("[SEMANTIC] Vector search failed: %s", e)
        return []
