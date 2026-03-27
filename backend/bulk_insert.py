"""
High-speed bulk insert for KPI data.
Uses raw psycopg2 COPY protocol (fastest possible PostgreSQL ingestion).

Usage from app.py:
    from bulk_insert import bulk_insert_kpi_rows
    bulk_insert_kpi_rows(db, rows, data_level="site")
"""

import io
import csv
import logging
from contextlib import contextmanager

_LOG = logging.getLogger("bulk_insert")

COPY_SQL = (
    "COPY kpi_data (site_id, kpi_name, date, hour, value, data_level, cell_id, cell_site_id) "
    "FROM STDIN WITH (FORMAT csv, NULL '')"
)

# 50k rows per COPY batch — sweet spot for memory vs speed
COPY_CHUNK = 50_000


@contextmanager
def _raw_conn(db):
    """Get a raw psycopg2 connection from SQLAlchemy, auto-close on exit."""
    conn = db.engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()


def _rows_to_csv_buffer(rows):
    """Convert list of tuples to an in-memory CSV buffer for COPY."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in rows:
        writer.writerow([v if v is not None else '' for v in r])
    buf.seek(0)
    return buf


def bulk_insert_kpi_rows(db, rows_iter, total_hint=0):
    """
    Insert KPI rows at maximum speed using PostgreSQL COPY protocol.

    rows_iter: iterable of tuples:
        (site_id, kpi_name, date, hour, value, data_level, cell_id, cell_site_id)
    total_hint: optional expected total for progress logging

    Returns: total rows inserted
    """
    inserted = 0

    with _raw_conn(db) as conn:
        cur = conn.cursor()
        batch = []

        for row in rows_iter:
            batch.append(row)

            if len(batch) >= COPY_CHUNK:
                buf = _rows_to_csv_buffer(batch)
                cur.copy_expert(COPY_SQL, buf)
                conn.commit()
                inserted += len(batch)
                if total_hint and inserted % 200_000 == 0:
                    pct = inserted / total_hint * 100
                    _LOG.info(f"  bulk_insert: {inserted:>12,} / {total_hint:,} ({pct:.1f}%)")
                batch = []

        if batch:
            buf = _rows_to_csv_buffer(batch)
            cur.copy_expert(COPY_SQL, buf)
            conn.commit()
            inserted += len(batch)

        cur.close()

    _LOG.info(f"  bulk_insert complete: {inserted:,} rows")
    return inserted


def bulk_insert_from_sheet_site(db, ws, kpi_name, date_columns):
    """
    Stream rows from an openpyxl worksheet (site-level) and bulk insert.
    ws: openpyxl worksheet (already past header row via iter_rows)
    Returns: rows inserted
    """
    from datetime import datetime

    def row_gen():
        for row in ws:
            site_id = str(row[0]).strip() if row[0] else None
            if not site_id or site_id == "None":
                continue
            for col_idx, date_val in date_columns:
                if col_idx < len(row) and row[col_idx] is not None:
                    try:
                        val = float(row[col_idx])
                    except (ValueError, TypeError):
                        continue
                    yield (site_id, kpi_name, date_val, 0, val, "site", None, None)

    return bulk_insert_kpi_rows(db, row_gen())


def bulk_insert_from_sheet_cell(db, ws, kpi_name, date_columns):
    """
    Stream rows from an openpyxl worksheet (cell-level) and bulk insert.
    Returns: rows inserted
    """
    def row_gen():
        for row in ws:
            site_id = str(row[0]).strip() if row[0] else None
            cell_id = str(row[1]).strip() if row[1] else None
            cell_site_id = str(row[2]).strip() if row[2] else None
            if not site_id or site_id == "None":
                continue
            for col_idx, date_val in date_columns:
                if col_idx < len(row) and row[col_idx] is not None:
                    try:
                        val = float(row[col_idx])
                    except (ValueError, TypeError):
                        continue
                    yield (site_id, kpi_name, date_val, 0, val, "cell", cell_id, cell_site_id)

    return bulk_insert_kpi_rows(db, row_gen())
