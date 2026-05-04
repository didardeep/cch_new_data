"""
migrate_add_semantic_layer.py
-----------------------------
One-time migration to create the semantic layer tables:
  1. metric_catalog        — canonical metric concepts
  2. metric_physical_mapping — concept → physical column/table/filter
  3. schema_embeddings     — JSONB embeddings for fuzzy concept matching

Run once:
    cd backend && python migrate_add_semantic_layer.py
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/telecom_cch")


def run():
    print(f"Connecting to: {DB_URL}")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # ── 1. metric_catalog ────────────────────────────────────────────────────
    print("[1] Creating metric_catalog table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metric_catalog (
            id SERIAL PRIMARY KEY,
            concept_name TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            unit TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    print("    ✓ metric_catalog ready")

    # ── 2. metric_physical_mapping ───────────────────────────────────────────
    print("[2] Creating metric_physical_mapping table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metric_physical_mapping (
            id SERIAL PRIMARY KEY,
            concept_id INTEGER REFERENCES metric_catalog(id) ON DELETE CASCADE,
            table_name TEXT NOT NULL,
            column_expr TEXT NOT NULL,
            filter_expr TEXT,
            device_type TEXT,
            priority INTEGER DEFAULT 0,
            UNIQUE(concept_id, table_name, device_type)
        )
    """)
    print("    ✓ metric_physical_mapping ready")

    # ── 3. schema_embeddings (JSONB for vector storage) ──────────────────────
    print("[3] Creating schema_embeddings table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_embeddings (
            id SERIAL PRIMARY KEY,
            object_type TEXT NOT NULL,
            object_name TEXT NOT NULL,
            source_table TEXT,
            embedding JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    print("    ✓ schema_embeddings ready (JSONB embedding storage)")

    # ── 4. Helper indexes ────────────────────────────────────────────────────
    print("[4] Creating helper indexes...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metric_catalog_concept
            ON metric_catalog (concept_name)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metric_physical_mapping_concept
            ON metric_physical_mapping (concept_id)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_schema_embeddings_type_name
            ON schema_embeddings (object_type, object_name)
    """)
    print("    ✓ indexes created")

    conn.commit()
    cur.close()
    conn.close()
    print("\n✅ Semantic layer migration complete!")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\n❌ Migration failed: {e}", file=sys.stderr)
        sys.exit(1)
