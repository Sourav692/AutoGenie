"""
Genie360 — Module 1: Query History Ingestion
=============================================
Queries system.query.history filtered by genie_space_id, normalizes records,
deduplicates by SQL fingerprint, estimates costs, and enriches metadata.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import sqlglot

from genie360.models.schemas import (
    DurationTier,
    GenieQueryRecord,
    VolumeTier,
)


# ═════════════════════════════════════════════════════════════════════════════
# Constants
# ═════════════════════════════════════════════════════════════════════════════

_CLASSIC_DBU_PER_HOUR = {
    "2X-Small": 2, "X-Small": 4, "Small": 8, "Medium": 16,
    "Large": 32, "X-Large": 64, "2X-Large": 128, "3X-Large": 256, "4X-Large": 512,
}
_SERVERLESS_COST_PER_DBU = 0.70
_CLASSIC_COST_PER_DBU = 1.50

_DURATION_THRESHOLDS_MS = {
    DurationTier.FAST: 2_000,
    DurationTier.MODERATE: 10_000,
    DurationTier.SLOW: 60_000,
}

_VOLUME_THRESHOLDS_BYTES = {
    VolumeTier.SMALL: 100 * 1024 * 1024,       # 100 MB
    VolumeTier.MEDIUM: 1024 * 1024 * 1024,      # 1 GB
    VolumeTier.LARGE: 10 * 1024 * 1024 * 1024,  # 10 GB
}


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: Query History Retrieval
# ═════════════════════════════════════════════════════════════════════════════

def get_genie_queries(
    spark: Any,
    space_id: str | None = None,
    lookback_days: int = 30,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """
    Queries system.query.history filtered by Genie space_id.
    Returns raw query records including statement_text, execution_duration_ms,
    read_bytes, read_rows, spilled_local_bytes, and execution_status.
    """
    space_filter = (
        f"AND query_source.genie_space_id = '{space_id}'"
        if space_id else ""
    )

    query = f"""
    SELECT
        statement_id                        AS query_id,
        statement_text,
        query_source.genie_space_id         AS genie_space_id,
        execution_status,
        total_duration_ms,
        execution_duration_ms,
        read_bytes,
        read_rows,
        produced_rows,
        spilled_local_bytes,
        from_result_cache,
        compute.warehouse_id                AS warehouse_id,
        start_time,
        executed_by
    FROM system.query.history
    WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL {lookback_days} DAYS
      AND statement_type = 'SELECT'
      AND error_message IS NULL
      AND query_source.genie_space_id IS NOT NULL
      {space_filter}
    ORDER BY total_duration_ms DESC
    LIMIT {limit}
    """
    return [row.asDict() for row in spark.sql(query).collect()]


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: Record Normalization
# ═════════════════════════════════════════════════════════════════════════════

def normalize_query_record(raw_record: dict[str, Any]) -> GenieQueryRecord:
    """
    Flattens nested compute, query_source, and query_parameters structs
    into a flat GenieQueryRecord. Handles NULL fields gracefully.
    """
    return GenieQueryRecord(
        query_id=raw_record.get("query_id", ""),
        statement_text=raw_record.get("statement_text", ""),
        genie_space_id=raw_record.get("genie_space_id"),
        warehouse_id=raw_record.get("warehouse_id"),
        warehouse_type=raw_record.get("warehouse_type"),
        execution_status=raw_record.get("execution_status"),
        execution_duration_ms=raw_record.get("execution_duration_ms"),
        total_duration_ms=raw_record.get("total_duration_ms"),
        read_bytes=raw_record.get("read_bytes"),
        read_rows=raw_record.get("read_rows"),
        produced_rows=raw_record.get("produced_rows"),
        spilled_local_bytes=raw_record.get("spilled_local_bytes"),
        from_result_cache=bool(raw_record.get("from_result_cache", False)),
        start_time=raw_record.get("start_time"),
        executed_by=raw_record.get("executed_by"),
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: Deduplication
# ═════════════════════════════════════════════════════════════════════════════

def _compute_sql_fingerprint(sql_text: str) -> str:
    """
    Normalizes SQL by stripping literals and whitespace to produce
    a canonical fingerprint for deduplication.
    """
    normalized = sql_text.strip().upper()
    normalized = re.sub(r"'[^']*'", "'?'", normalized)
    normalized = re.sub(r"\b\d+\b", "?", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def deduplicate_queries(
    query_list: list[GenieQueryRecord],
) -> list[GenieQueryRecord]:
    """
    Deduplicates queries by semantic fingerprint (normalized SQL hash).
    Keeps the most recent execution of structurally identical queries.
    """
    seen: dict[str, GenieQueryRecord] = {}
    for record in query_list:
        fp = _compute_sql_fingerprint(record.statement_text)
        record.sql_fingerprint = fp
        existing = seen.get(fp)
        if existing is None:
            seen[fp] = record
        elif record.start_time and existing.start_time and record.start_time > existing.start_time:
            seen[fp] = record

    return list(seen.values())


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4: Cost Estimation
# ═════════════════════════════════════════════════════════════════════════════

def calculate_query_cost_estimate(
    read_bytes: int | None,
    duration_ms: int | None,
    warehouse_type: str = "SERVERLESS",
) -> float:
    """
    Estimates USD cost based on DBU pricing for CLASSIC vs SERVERLESS.
    Uses read_bytes and duration_ms as proxy signals.

    Heuristic: cost ≈ (duration_hours * dbu_rate) * cost_per_dbu
    For serverless, uses a simplified per-second billing model.
    """
    if duration_ms is None or duration_ms <= 0:
        return 0.0

    duration_hours = duration_ms / 3_600_000

    if warehouse_type.upper() == "SERVERLESS":
        dbu_rate = 10  # approximate serverless rate
        cost_per_dbu = _SERVERLESS_COST_PER_DBU
    else:
        dbu_rate = _CLASSIC_DBU_PER_HOUR.get("Medium", 16)
        cost_per_dbu = _CLASSIC_COST_PER_DBU

    estimated_dbu = duration_hours * dbu_rate
    return round(estimated_dbu * cost_per_dbu, 4)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5: Metadata Enrichment
# ═════════════════════════════════════════════════════════════════════════════

def _classify_duration(ms: int | None) -> DurationTier:
    if ms is None:
        return DurationTier.FAST
    if ms < _DURATION_THRESHOLDS_MS[DurationTier.FAST]:
        return DurationTier.FAST
    if ms < _DURATION_THRESHOLDS_MS[DurationTier.MODERATE]:
        return DurationTier.MODERATE
    if ms < _DURATION_THRESHOLDS_MS[DurationTier.SLOW]:
        return DurationTier.SLOW
    return DurationTier.CRITICAL


def _classify_volume(read_bytes: int | None) -> VolumeTier:
    if read_bytes is None:
        return VolumeTier.SMALL
    if read_bytes < _VOLUME_THRESHOLDS_BYTES[VolumeTier.SMALL]:
        return VolumeTier.SMALL
    if read_bytes < _VOLUME_THRESHOLDS_BYTES[VolumeTier.MEDIUM]:
        return VolumeTier.MEDIUM
    if read_bytes < _VOLUME_THRESHOLDS_BYTES[VolumeTier.LARGE]:
        return VolumeTier.LARGE
    return VolumeTier.MASSIVE


def enrich_query_metadata(query_record: GenieQueryRecord) -> GenieQueryRecord:
    """
    Adds derived fields: cost estimate, duration tier, data volume tier,
    cache hit flag, and SQL fingerprint.
    """
    query_record.estimated_cost_usd = calculate_query_cost_estimate(
        read_bytes=query_record.read_bytes,
        duration_ms=query_record.total_duration_ms,
        warehouse_type=query_record.warehouse_type or "SERVERLESS",
    )
    query_record.duration_tier = _classify_duration(query_record.total_duration_ms)
    query_record.volume_tier = _classify_volume(query_record.read_bytes)

    if not query_record.sql_fingerprint:
        query_record.sql_fingerprint = _compute_sql_fingerprint(
            query_record.statement_text
        )

    return query_record
