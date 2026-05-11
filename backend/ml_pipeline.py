"""
ml_pipeline.py — ML Categorization Pipeline for Telecom KPI Data
=================================================================
Uses unsupervised ML to categorize 22.5M+ rows of raw KPI data into
meaningful semantic labels that the Network AI chatbot can query.

Models used:
  1. K-Means (per KPI)     → health_label: 'healthy' / 'degraded' / 'critical'
  2. Isolation Forest       → is_anomaly: True / False (outlier detection)
  3. K-Means (multi-KPI)   → site_tier: 'top_performer' / 'good' / 'average' / 'underperformer'
  4. Composite Health Score → health_score: 0-100 (weighted across KPIs)

Architecture:
  Raw kpi_data (22.5M rows)
    → Daily aggregation (AVG/MIN/MAX/STDDEV per site per KPI per day)
    → ML models run on aggregated data (~150K rows)
    → Results stored in site_kpi_summary table
    → Network AI queries site_kpi_summary instead of raw data

Usage:
    from ml_pipeline import run_ml_pipeline, get_pipeline_status
    result = run_ml_pipeline(app)  # runs full pipeline
"""

import logging
import time
import threading
from datetime import datetime, timezone

import numpy as np
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sqlalchemy import text as sa_text

_LOG = logging.getLogger("ml_pipeline")
PIPELINE_VERSION = "2026-04-16-v1"

# ── Pipeline state (thread-safe) ────────────────────────────────────────────
_pipeline_lock = threading.Lock()
_pipeline_status = {
    "running": False,
    "last_run": None,
    "last_duration_sec": None,
    "last_error": None,
    "rows_processed": 0,
    "stage": "idle",
}

# ── KPI polarity: True = higher is better, False = lower is better ──────────
# This is critical for K-Means label assignment — determines which cluster
# gets labeled 'healthy' vs 'critical'
_KPI_POLARITY = {
    'LTE RRC Setup Success Rate': True,
    'LTE Call Setup Success Rate': True,
    'LTE E-RAB Setup Success Rate': True,
    'E-RAB Call Drop Rate_1': False,          # Lower drop rate = better
    'CSFB Access Success Rate': True,
    'LTE Intra-Freq HO Success Rate': True,
    'Intra-eNB HO Success Rate': True,
    'Inter-eNBX2HO Success Rate': True,
    'Inter-eNBS1HO Success Rate': True,
    'LTE DL - Cell Ave Throughput': True,
    'LTE UL - Cell Ave Throughput': True,
    'LTE DL - Usr Ave Throughput': True,
    'LTE UL - User Ave Throughput': True,
    'Average Latency Downlink': False,        # Lower latency = better
    'DL Data Total Volume': True,
    'UL Data Total Volume': True,
    'VoLTE Traffic Erlang': True,
    'VoLTE Traffic UL': True,
    'VoLTE Traffic DL': True,
    'Ave RRC Connected Ue': True,
    'Max RRC Connected Ue': True,
    'Average Act UE DL Per Cell': True,
    'Average Act UE UL Per Cell': True,
    'Availability': True,
    'Average NI of Carrier-': False,          # Lower noise = better
    'DL PRB Utilization (1BH)': False,        # Lower utilization = less congested
    'UL PRB Utilization (1BH)': False,
}

# ── Weights for composite health score ──────────────────────────────────────
# These determine how much each KPI contributes to the overall site health
_HEALTH_WEIGHTS = {
    'E-RAB Call Drop Rate_1': 0.18,
    'LTE Call Setup Success Rate': 0.15,
    'LTE DL - Usr Ave Throughput': 0.12,
    'Availability': 0.12,
    'DL PRB Utilization (1BH)': 0.10,
    'Average Latency Downlink': 0.08,
    'LTE Intra-Freq HO Success Rate': 0.08,
    'LTE RRC Setup Success Rate': 0.07,
    'LTE E-RAB Setup Success Rate': 0.05,
    'Ave RRC Connected Ue': 0.05,
}


def get_pipeline_status():
    """Return current pipeline status (thread-safe)."""
    with _pipeline_lock:
        return dict(_pipeline_status)


def _update_status(stage, **kwargs):
    """Update pipeline status."""
    with _pipeline_lock:
        _pipeline_status["stage"] = stage
        _pipeline_status.update(kwargs)
    _LOG.info("[ML-PIPELINE] Stage: %s %s", stage, kwargs if kwargs else "")


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Aggregate raw kpi_data → daily summaries
# ─────────────────────────────────────────────────────────────────────────────
def _aggregate_daily(db):
    """Aggregate 22.5M rows into daily site-level summaries.
    Returns list of dicts with: site_id, date, kpi_name, avg_value, min, max, stddev, count."""
    _update_status("aggregating")

    sql = """
    SELECT
        k.site_id,
        k.date,
        k.kpi_name,
        AVG(k.value)       AS avg_value,
        MIN(k.value)       AS min_value,
        MAX(k.value)       AS max_value,
        STDDEV(k.value)    AS stddev_value,
        COUNT(*)           AS sample_count
    FROM kpi_data k
    WHERE k.data_level = 'site'
      AND k.value IS NOT NULL
    GROUP BY k.site_id, k.kpi_name, k.date
    ORDER BY k.date, k.site_id, k.kpi_name
    """
    with db.engine.connect() as conn:
        conn.execute(sa_text("SET LOCAL statement_timeout = '120000'"))  # 2 min
        result = conn.execute(sa_text(sql))
        cols = list(result.keys())
        rows = [dict(zip(cols, row)) for row in result.fetchall()]

    _LOG.info("[AGGREGATE] Got %d daily aggregated rows", len(rows))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: K-Means per KPI → health_label
# ─────────────────────────────────────────────────────────────────────────────
def _kmeans_per_kpi(rows):
    """Run K-Means (k=3) on each KPI's avg_value to create 3 clusters.
    Then auto-label clusters as 'healthy', 'degraded', 'critical' based on
    centroid ordering and KPI polarity."""
    _update_status("clustering_per_kpi")

    # Group rows by kpi_name
    by_kpi = {}
    for r in rows:
        kpi = r['kpi_name']
        if kpi not in by_kpi:
            by_kpi[kpi] = []
        by_kpi[kpi].append(r)

    for kpi, kpi_rows in by_kpi.items():
        values = np.array([r['avg_value'] for r in kpi_rows if r['avg_value'] is not None])
        if len(values) < 10:
            # Not enough data for clustering — default to 'unknown'
            for r in kpi_rows:
                r['health_label'] = 'unknown'
                r['health_confidence'] = 0.0
            continue

        # Reshape for sklearn
        X = values.reshape(-1, 1)

        # K-Means with 3 clusters
        n_clusters = min(3, len(np.unique(values)))
        if n_clusters < 2:
            for r in kpi_rows:
                r['health_label'] = 'healthy'
                r['health_confidence'] = 1.0
            continue

        km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        km.fit(X)
        labels = km.labels_
        centroids = km.cluster_centers_.flatten()

        # Sort centroids to determine label mapping
        # For "higher is better" KPIs: highest centroid = healthy
        # For "lower is better" KPIs: lowest centroid = healthy
        higher_is_better = _KPI_POLARITY.get(kpi, True)

        sorted_indices = np.argsort(centroids)
        if higher_is_better:
            # Ascending order: [0]=lowest=critical, [1]=middle=degraded, [2]=highest=healthy
            label_map = {}
            label_names = ['critical', 'degraded', 'healthy']
            for rank, idx in enumerate(sorted_indices):
                label_map[idx] = label_names[min(rank, len(label_names) - 1)]
        else:
            # Ascending order: [0]=lowest=healthy, [1]=middle=degraded, [2]=highest=critical
            label_map = {}
            label_names = ['healthy', 'degraded', 'critical']
            for rank, idx in enumerate(sorted_indices):
                label_map[idx] = label_names[min(rank, len(label_names) - 1)]

        # Assign labels back to rows
        value_idx = 0
        for r in kpi_rows:
            if r['avg_value'] is None:
                r['health_label'] = 'unknown'
                r['health_confidence'] = 0.0
                continue
            cluster = labels[value_idx]
            # Distance to centroid as confidence (invert: closer = higher confidence)
            dist = abs(r['avg_value'] - centroids[cluster])
            max_dist = max(abs(centroids.max() - centroids.min()), 1e-6)
            confidence = max(0.0, 1.0 - (dist / max_dist))

            r['health_label'] = label_map.get(cluster, 'unknown')
            r['health_confidence'] = round(confidence, 4)
            value_idx += 1

    _LOG.info("[K-MEANS] Labeled %d KPIs across %d rows", len(by_kpi), len(rows))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Isolation Forest → anomaly detection
# ─────────────────────────────────────────────────────────────────────────────
def _detect_anomalies(rows):
    """Run Isolation Forest on each KPI's values to flag anomalies.
    contamination=0.05 means ~5% of data points are expected to be anomalies."""
    _update_status("detecting_anomalies")

    by_kpi = {}
    for r in rows:
        kpi = r['kpi_name']
        if kpi not in by_kpi:
            by_kpi[kpi] = []
        by_kpi[kpi].append(r)

    total_anomalies = 0
    for kpi, kpi_rows in by_kpi.items():
        values = [r['avg_value'] for r in kpi_rows if r['avg_value'] is not None]
        if len(values) < 20:
            for r in kpi_rows:
                r['is_anomaly'] = False
                r['anomaly_score'] = 0.0
            continue

        X = np.array(values).reshape(-1, 1)

        iso = IsolationForest(
            contamination=0.05,
            random_state=42,
            n_estimators=100,
        )
        iso.fit(X)
        predictions = iso.predict(X)      # 1 = normal, -1 = anomaly
        scores = iso.decision_function(X)  # more negative = more anomalous

        value_idx = 0
        for r in kpi_rows:
            if r['avg_value'] is None:
                r['is_anomaly'] = False
                r['anomaly_score'] = 0.0
                continue
            r['is_anomaly'] = bool(predictions[value_idx] == -1)
            r['anomaly_score'] = round(float(scores[value_idx]), 4)
            if r['is_anomaly']:
                total_anomalies += 1
            value_idx += 1

    _LOG.info("[ISOLATION-FOREST] Found %d anomalies in %d rows", total_anomalies, len(rows))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Multi-KPI site clustering → site_tier
# ─────────────────────────────────────────────────────────────────────────────
def _cluster_sites(rows):
    """Build a feature vector per site (avg of each KPI across all dates)
    and cluster sites into 4 tiers using K-Means."""
    _update_status("clustering_sites")

    # Build per-site feature vectors
    site_kpis = {}  # site_id → {kpi_name: avg_value}
    for r in rows:
        sid = r['site_id']
        if sid not in site_kpis:
            site_kpis[sid] = {}
        kpi = r['kpi_name']
        if kpi not in site_kpis[sid]:
            site_kpis[sid][kpi] = []
        if r['avg_value'] is not None:
            site_kpis[sid][kpi].append(r['avg_value'])

    # Get all KPI names for consistent feature ordering
    all_kpis = sorted(set(kpi for s in site_kpis.values() for kpi in s.keys()))
    if len(all_kpis) < 2 or len(site_kpis) < 4:
        _LOG.warning("[SITE-TIER] Not enough data for site clustering")
        for r in rows:
            r['site_tier'] = 'unknown'
        return rows

    # Build feature matrix: each row = one site, each col = one KPI's mean
    site_ids = sorted(site_kpis.keys())
    feature_matrix = []
    for sid in site_ids:
        features = []
        for kpi in all_kpis:
            vals = site_kpis[sid].get(kpi, [])
            avg = np.mean(vals) if vals else 0.0
            # Normalize polarity: flip "lower is better" KPIs so higher = always better
            if not _KPI_POLARITY.get(kpi, True):
                avg = -avg
            features.append(avg)
        feature_matrix.append(features)

    X = np.array(feature_matrix)

    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # K-Means with 4 clusters
    n_clusters = min(4, len(site_ids))
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    km.fit(X_scaled)

    # Label clusters by mean score (sum of normalized features)
    cluster_scores = {}
    for i in range(n_clusters):
        mask = km.labels_ == i
        cluster_scores[i] = X_scaled[mask].mean()

    sorted_clusters = sorted(cluster_scores.keys(), key=lambda c: cluster_scores[c])
    tier_names = ['underperformer', 'average', 'good', 'top_performer']
    tier_map = {}
    for rank, cluster_id in enumerate(sorted_clusters):
        tier_map[cluster_id] = tier_names[min(rank, len(tier_names) - 1)]

    # Map back: site_id → tier
    site_tier_map = {}
    for idx, sid in enumerate(site_ids):
        site_tier_map[sid] = tier_map.get(km.labels_[idx], 'unknown')

    # Assign to rows
    for r in rows:
        r['site_tier'] = site_tier_map.get(r['site_id'], 'unknown')

    tier_counts = {}
    for tier in site_tier_map.values():
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    _LOG.info("[SITE-TIER] %d sites classified: %s", len(site_tier_map), tier_counts)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Composite health score (0-100)
# ─────────────────────────────────────────────────────────────────────────────
def _compute_health_scores(rows):
    """Compute a 0-100 health score per site per day.
    Uses MinMax normalization per KPI then weighted sum."""
    _update_status("computing_health_scores")

    # Collect all values per KPI for normalization bounds
    kpi_bounds = {}  # kpi_name → (min_val, max_val)
    for r in rows:
        kpi = r['kpi_name']
        v = r['avg_value']
        if v is None:
            continue
        if kpi not in kpi_bounds:
            kpi_bounds[kpi] = [v, v]
        kpi_bounds[kpi][0] = min(kpi_bounds[kpi][0], v)
        kpi_bounds[kpi][1] = max(kpi_bounds[kpi][1], v)

    # Group rows by (site_id, date)
    by_site_date = {}
    for r in rows:
        key = (r['site_id'], str(r['date']))
        if key not in by_site_date:
            by_site_date[key] = []
        by_site_date[key].append(r)

    for key, group in by_site_date.items():
        weighted_sum = 0.0
        total_weight = 0.0

        for r in group:
            kpi = r['kpi_name']
            weight = _HEALTH_WEIGHTS.get(kpi, 0.0)
            if weight == 0.0 or r['avg_value'] is None:
                continue

            bounds = kpi_bounds.get(kpi)
            if not bounds or bounds[0] == bounds[1]:
                normalized = 0.5
            else:
                # Normalize to 0-1
                normalized = (r['avg_value'] - bounds[0]) / (bounds[1] - bounds[0])
                # Flip for "lower is better" KPIs
                if not _KPI_POLARITY.get(kpi, True):
                    normalized = 1.0 - normalized

            weighted_sum += normalized * weight
            total_weight += weight

        # Scale to 0-100
        health_score = (weighted_sum / total_weight * 100) if total_weight > 0 else 50.0
        health_score = round(max(0.0, min(100.0, health_score)), 2)

        for r in group:
            r['health_score'] = health_score

    _LOG.info("[HEALTH-SCORE] Computed scores for %d (site, date) combinations", len(by_site_date))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Resolve zone from telecom_sites
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_zones(rows, db):
    """Look up zone for each site_id from telecom_sites table."""
    _update_status("resolving_zones")
    try:
        zone_rows = []
        with db.engine.connect() as conn:
            result = conn.execute(sa_text(
                "SELECT DISTINCT site_id, zone FROM telecom_sites WHERE zone IS NOT NULL"
            ))
            zone_rows = [(r[0], r[1]) for r in result.fetchall()]
        zone_map = {sid: zone for sid, zone in zone_rows}

        for r in rows:
            r['zone'] = zone_map.get(r['site_id'])

        _LOG.info("[ZONES] Resolved zones for %d sites", len(zone_map))
    except Exception as e:
        _LOG.warning("[ZONES] Failed to resolve zones: %s", e)
        for r in rows:
            r.setdefault('zone', None)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Write results to site_kpi_summary table
# ─────────────────────────────────────────────────────────────────────────────
def _py(v):
    """Convert numpy scalar to native Python type for psycopg2 compatibility."""
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v


def _write_summary(rows, db):
    """Upsert all categorized rows into site_kpi_summary table.
    Uses batch INSERT with ON CONFLICT to handle re-runs."""
    _update_status("writing_results", rows_processed=len(rows))

    # Ensure table exists
    from models import SiteKpiSummary
    SiteKpiSummary.__table__.create(db.engine, checkfirst=True)

    batch_size = 1000
    inserted = 0

    upsert_sql = """
    INSERT INTO site_kpi_summary (
        site_id, date, kpi_name,
        avg_value, min_value, max_value, stddev_value, sample_count,
        health_label, health_confidence,
        is_anomaly, anomaly_score,
        site_tier, health_score, zone,
        pipeline_version, created_at
    ) VALUES (
        :site_id, :date, :kpi_name,
        :avg_value, :min_value, :max_value, :stddev_value, :sample_count,
        :health_label, :health_confidence,
        :is_anomaly, :anomaly_score,
        :site_tier, :health_score, :zone,
        :pipeline_version, :created_at
    )
    ON CONFLICT (site_id, date, kpi_name)
    DO UPDATE SET
        avg_value = EXCLUDED.avg_value,
        min_value = EXCLUDED.min_value,
        max_value = EXCLUDED.max_value,
        stddev_value = EXCLUDED.stddev_value,
        sample_count = EXCLUDED.sample_count,
        health_label = EXCLUDED.health_label,
        health_confidence = EXCLUDED.health_confidence,
        is_anomaly = EXCLUDED.is_anomaly,
        anomaly_score = EXCLUDED.anomaly_score,
        site_tier = EXCLUDED.site_tier,
        health_score = EXCLUDED.health_score,
        zone = EXCLUDED.zone,
        pipeline_version = EXCLUDED.pipeline_version,
        created_at = EXCLUDED.created_at
    """

    now = datetime.now(timezone.utc)

    with db.engine.connect() as conn:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            params = []
            for r in batch:
                params.append({
                    'site_id': r['site_id'],
                    'date': r['date'],
                    'kpi_name': r['kpi_name'],
                    'avg_value': _py(r.get('avg_value')),
                    'min_value': _py(r.get('min_value')),
                    'max_value': _py(r.get('max_value')),
                    'stddev_value': _py(r.get('stddev_value')),
                    'sample_count': _py(r.get('sample_count', 0)),
                    'health_label': r.get('health_label', 'unknown'),
                    'health_confidence': _py(r.get('health_confidence', 0.0)),
                    'is_anomaly': _py(r.get('is_anomaly', False)),
                    'anomaly_score': _py(r.get('anomaly_score', 0.0)),
                    'site_tier': r.get('site_tier', 'unknown'),
                    'health_score': _py(r.get('health_score', 50.0)),
                    'zone': r.get('zone'),
                    'pipeline_version': PIPELINE_VERSION,
                    'created_at': now,
                })
            conn.execute(sa_text(upsert_sql), params)
            inserted += len(batch)

            if inserted % 10000 == 0:
                _LOG.info("[WRITE] %d / %d rows written", inserted, len(rows))

        conn.commit()

    _LOG.info("[WRITE] Completed: %d rows written to site_kpi_summary", inserted)
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline orchestrator
# ─────────────────────────────────────────────────────────────────────────────
def run_ml_pipeline(app):
    """Run the full ML categorization pipeline.
    Must be called within Flask app context.
    Returns a dict with results summary."""

    with _pipeline_lock:
        if _pipeline_status["running"]:
            return {"error": "Pipeline is already running", "status": _pipeline_status}
        _pipeline_status["running"] = True
        _pipeline_status["last_error"] = None

    start_time = time.time()

    try:
        with app.app_context():
            from models import db as _db

            # Step 1: Aggregate
            rows = _aggregate_daily(_db)
            if not rows:
                _update_status("complete", running=False, last_error="No data to process")
                return {"error": "No KPI data found in database"}

            _update_status("ml_processing", rows_processed=len(rows))

            # Step 2: K-Means per KPI → health labels
            rows = _kmeans_per_kpi(rows)

            # Step 3: Isolation Forest → anomaly detection
            rows = _detect_anomalies(rows)

            # Step 4: Multi-KPI site clustering → tiers
            rows = _cluster_sites(rows)

            # Step 5: Composite health scores
            rows = _compute_health_scores(rows)

            # Step 6: Resolve zones
            rows = _resolve_zones(rows, _db)

            # Step 7: Write to database
            written = _write_summary(rows, _db)

            duration = round(time.time() - start_time, 2)
            _update_status("complete",
                           running=False,
                           last_run=datetime.now(timezone.utc).isoformat(),
                           last_duration_sec=duration,
                           rows_processed=written)

            # Collect summary stats
            health_counts = {}
            tier_counts = {}
            anomaly_count = 0
            for r in rows:
                hl = r.get('health_label', 'unknown')
                health_counts[hl] = health_counts.get(hl, 0) + 1
                st = r.get('site_tier', 'unknown')
                tier_counts[st] = tier_counts.get(st, 0) + 1
                if r.get('is_anomaly'):
                    anomaly_count += 1

            result = {
                "status": "success",
                "pipeline_version": PIPELINE_VERSION,
                "rows_processed": written,
                "duration_seconds": duration,
                "health_distribution": health_counts,
                "tier_distribution": tier_counts,
                "anomalies_detected": anomaly_count,
                "unique_sites": len(set(r['site_id'] for r in rows)),
                "unique_kpis": len(set(r['kpi_name'] for r in rows)),
            }
            _LOG.info("[PIPELINE-COMPLETE] %s", result)
            return result

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        error_msg = str(e)
        _update_status("failed",
                       running=False,
                       last_error=error_msg,
                       last_duration_sec=duration)
        _LOG.error("[PIPELINE-FAILED] %s (%.2fs)", error_msg, duration)
        return {"error": error_msg, "duration_seconds": duration}


def run_ml_pipeline_async(app):
    """Run the ML pipeline in a background thread.
    Returns immediately with status."""
    with _pipeline_lock:
        if _pipeline_status["running"]:
            return {"error": "Pipeline is already running"}

    thread = threading.Thread(
        target=run_ml_pipeline,
        args=(app,),
        daemon=True,
        name="ml-pipeline",
    )
    thread.start()
    return {"status": "started", "message": "ML pipeline running in background"}
