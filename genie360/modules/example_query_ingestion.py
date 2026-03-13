"""
Genie360 — Module 7: Example Query Ingestion (Upstream)
========================================================
Fetches example SQL queries from a Genie Space's Knowledge Store via the
Management API, converts them into records compatible with the optimization
pipeline, and supports writing optimized versions back.

This powers the "Upstream Injection" pattern: optimize example queries
*before* Genie uses them to generate SQL.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any

from genie360.models.schemas import ExampleQueryRecord
from genie360.modules.genie_space_injection import (
    deserialize_space_config,
    get_genie_space,
    serialize_space_config,
    update_genie_space,
)


def fetch_example_queries(
    space_id: str,
    workspace_url: str | None = None,
    token: str | None = None,
) -> tuple[list[ExampleQueryRecord], dict[str, Any]]:
    """
    Fetches example SQL queries from a Genie Space's Knowledge Store.

    Returns:
        (records, space_config) — the parsed example queries and the raw
        deserialized space config (needed later for in-place updates).
    """
    space_data = get_genie_space(space_id, workspace_url, token)
    serialized = space_data.get("serialized_space", "{}")
    space_config = deserialize_space_config(serialized)

    examples = (
        space_config
        .get("instructions", {})
        .get("example_question_sqls", [])
    )

    records: list[ExampleQueryRecord] = []
    for entry in examples:
        entry_id = entry.get("id", "")
        questions = entry.get("question", [])
        sqls = entry.get("sql", [])
        guidance = entry.get("usage_guidance", [])
        question_text = questions[0] if questions else None

        for idx, sql in enumerate(sqls):
            query_id = f"example-{entry_id}-{idx}" if len(sqls) > 1 else f"example-{entry_id}"
            records.append(ExampleQueryRecord(
                query_id=query_id,
                statement_text=sql.strip(),
                question_text=question_text,
                example_entry_id=entry_id,
                usage_guidance=guidance if isinstance(guidance, list) else [],
                genie_space_id=space_id,
            ))

    return records, space_config


def inject_upstream_rewrites(
    space_id: str,
    space_config: dict[str, Any],
    rewrites: list[dict[str, Any]],
    workspace_url: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    """
    Writes optimized SQL back into the Genie Space's example queries,
    replacing original SQL in-place while preserving question text and
    metadata.

    Args:
        space_id:     Target Genie Space.
        space_config: The deserialized space config from fetch_example_queries.
        rewrites:     List of dicts with keys:
                        - example_entry_id: str
                        - original_sql: str
                        - optimized_sql: str
                        - confidence_score: float
                        - rules_applied: list[str]

    Returns:
        Summary dict with counts and the previous serialized state.
    """
    examples = (
        space_config
        .get("instructions", {})
        .get("example_question_sqls", [])
    )
    entry_index = {e.get("id", ""): e for e in examples}

    previous_serialized = serialize_space_config(space_config)
    injected = 0
    skipped = 0

    for rw in rewrites:
        entry_id = rw["example_entry_id"]
        entry = entry_index.get(entry_id)
        if entry is None:
            skipped += 1
            continue

        old_sql = rw["original_sql"]
        new_sql = rw["optimized_sql"]
        confidence = rw.get("confidence_score", 0.0)
        rules = rw.get("rules_applied", [])

        sql_list = entry.get("sql", [])
        replaced = False
        for i, existing_sql in enumerate(sql_list):
            if _sql_fingerprint(existing_sql) == _sql_fingerprint(old_sql):
                sql_list[i] = new_sql
                replaced = True
                break

        if not replaced:
            sql_list[0] = new_sql if sql_list else None
            if not sql_list:
                entry["sql"] = [new_sql]
                replaced = True

        if replaced:
            guidance = entry.get("usage_guidance", [])
            if not isinstance(guidance, list):
                guidance = []
            meta = (
                f"[Genie360 upstream-optimized: {', '.join(rules)}, "
                f"confidence={confidence:.1%}, at={datetime.utcnow().isoformat()}]"
            )
            guidance.append(meta)
            entry["usage_guidance"] = guidance
            injected += 1
        else:
            skipped += 1

    space_config["instructions"]["example_question_sqls"] = examples
    new_serialized = serialize_space_config(space_config)
    update_genie_space(space_id, new_serialized, workspace_url, token)

    return {
        "injected": injected,
        "skipped": skipped,
        "previous_serialized_space": previous_serialized,
    }


def _sql_fingerprint(sql: str) -> str:
    normalized = sql.strip().upper()
    normalized = re.sub(r"'[^']*'", "'?'", normalized)
    normalized = re.sub(r"\b\d+\b", "?", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]
