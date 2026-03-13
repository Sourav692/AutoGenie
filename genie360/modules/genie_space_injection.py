"""
Genie360 — Module 5: Genie Space Injection
============================================
Full CRUD wrapper for the Genie Space Management API. Handles fetching
space config, injecting optimized SQL as example queries, bulk operations,
and rollback capability.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from copy import deepcopy
from datetime import datetime
from typing import Any

import requests
import sqlglot
from sqlglot import exp

from genie360.models.schemas import RewriteCandidate, SpaceInjectionResult


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: Space Retrieval
# ═════════════════════════════════════════════════════════════════════════════

def _get_api_config() -> tuple[str, str]:
    """Returns (workspace_url, token) from environment."""
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    return host, token


def get_genie_space(
    space_id: str,
    workspace_url: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    """
    Calls the Genie GET API (GET /api/2.0/genie/spaces/{id}) to retrieve
    the serialized_space JSON — the complete space definition including
    instructions, example SQL, and JOIN relationships.
    """
    if workspace_url is None or token is None:
        workspace_url, token = _get_api_config()

    url = f"{workspace_url}/api/2.0/genie/spaces/{space_id}"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"include_serialized_space": "true"}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: Config Deserialization / Serialization
# ═════════════════════════════════════════════════════════════════════════════

def deserialize_space_config(serialized_space_json: str) -> dict[str, Any]:
    """
    Parses the serialized_space JSON string into a Python dict for
    manipulation. Validates schema version and structure.
    """
    config = json.loads(serialized_space_json)

    if config.get("version") not in (1, 2):
        print(f"Warning: Unexpected serialized_space version: {config.get('version')}")

    if "instructions" not in config:
        config["instructions"] = {}

    if "example_question_sqls" not in config.get("instructions", {}):
        config["instructions"]["example_question_sqls"] = []

    return config


def serialize_space_config(space_config: dict[str, Any]) -> str:
    """
    Re-serializes the modified space config back to the serialized_space
    JSON string format required by the Update API.
    """
    return json.dumps(space_config, ensure_ascii=False)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: Example SQL Matching
# ═════════════════════════════════════════════════════════════════════════════

def _sql_fingerprint(sql: str) -> str:
    """Normalize SQL for comparison."""
    import re
    normalized = sql.strip().upper()
    normalized = re.sub(r"'[^']*'", "'?'", normalized)
    normalized = re.sub(r"\b\d+\b", "?", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def find_matching_example_sql(
    space_config: dict[str, Any],
    original_sql: str,
) -> dict[str, Any] | None:
    """
    Searches the space's example_question_sqls array to find the entry
    matching the original query. Uses SQL hash fingerprinting for exact match,
    fuzzy similarity for near-matches.
    """
    examples = space_config.get("instructions", {}).get("example_question_sqls", [])
    target_fp = _sql_fingerprint(original_sql)

    for example in examples:
        sql_list = example.get("sql", [])
        for sql in sql_list:
            if _sql_fingerprint(sql) == target_fp:
                return example

    target_upper = original_sql.strip().upper()
    for example in examples:
        sql_list = example.get("sql", [])
        for sql in sql_list:
            if _simple_similarity(sql.strip().upper(), target_upper) > 0.85:
                return example

    return None


def _simple_similarity(a: str, b: str) -> float:
    """Token-based Jaccard similarity."""
    if not a or not b:
        return 0.0
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union) if union else 0.0


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4: Example SQL Update
# ═════════════════════════════════════════════════════════════════════════════

def _generate_hex_id() -> str:
    return uuid.uuid4().hex


def update_example_sql_entry(
    space_config: dict[str, Any],
    old_sql: str,
    new_sql: str,
    question: str | None = None,
    change_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Replaces the matched example SQL entry with the optimized version,
    or creates a new entry if no match exists. Appends change metadata.
    """
    examples = space_config.get("instructions", {}).get("example_question_sqls", [])
    existing = find_matching_example_sql(space_config, old_sql)

    meta_note = ""
    if change_metadata:
        meta_note = (
            f"[Genie360 optimized: {change_metadata.get('rewrite_reason', 'rule+llm')}, "
            f"confidence={change_metadata.get('confidence_score', 'N/A')}, "
            f"at={datetime.utcnow().isoformat()}]"
        )

    if existing:
        existing["sql"] = [new_sql]
        if meta_note:
            guidance = existing.get("usage_guidance", [])
            if isinstance(guidance, list):
                guidance.append(meta_note)
            else:
                guidance = [meta_note]
            existing["usage_guidance"] = guidance
    else:
        new_entry = {
            "id": _generate_hex_id(),
            "question": [question or "Optimized query"],
            "sql": [new_sql],
            "usage_guidance": [meta_note] if meta_note else [],
        }
        examples.append(new_entry)

    examples.sort(key=lambda x: x.get("id", ""))
    space_config["instructions"]["example_question_sqls"] = examples
    return space_config


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5: Space Update API
# ═════════════════════════════════════════════════════════════════════════════

def update_genie_space(
    space_id: str,
    serialized_space: str,
    workspace_url: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    """
    Calls the Genie Update API (PUT /api/2.0/genie/spaces/{id}) with
    the modified serialized space. Returns success/failure status.
    """
    if workspace_url is None or token is None:
        workspace_url, token = _get_api_config()

    url = f"{workspace_url}/api/2.0/genie/spaces/{space_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"serialized_space": serialized_space}

    resp = requests.put(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6: Batch Injection
# ═════════════════════════════════════════════════════════════════════════════

def batch_inject_rewrites(
    space_id: str,
    approved_rewrites: list[RewriteCandidate],
    workspace_url: str | None = None,
    token: str | None = None,
) -> SpaceInjectionResult:
    """
    Processes a batch of approved rewrites for a single space in a single
    API round-trip. Minimizes API calls by bundling all changes into one
    Update call.
    """
    result = SpaceInjectionResult(space_id=space_id)

    try:
        space_data = get_genie_space(space_id, workspace_url, token)
        serialized_space = space_data.get("serialized_space", "{}")
        result.previous_serialized_space = serialized_space
        space_config = deserialize_space_config(serialized_space)

        injected = 0
        failed = 0
        for rewrite in approved_rewrites:
            try:
                if rewrite.final_sql:
                    question = _infer_question_from_sql(rewrite.original_sql)
                    space_config = update_example_sql_entry(
                        space_config=space_config,
                        old_sql=rewrite.original_sql,
                        new_sql=rewrite.final_sql,
                        question=question,
                        change_metadata={
                            "rewrite_reason": ", ".join(rewrite.rules_applied),
                            "confidence_score": rewrite.confidence_score,
                        },
                    )
                    rewrite.status = "injected"
                    injected += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

        new_serialized = serialize_space_config(space_config)
        update_genie_space(space_id, new_serialized, workspace_url, token)

        result.rewrites_injected = injected
        result.rewrites_failed = failed
        result.new_serialized_space = new_serialized
        result.success = True

    except Exception as e:
        result.success = False
        result.error_message = str(e)

    return result


def _infer_question_from_sql(sql: str) -> str:
    """Derives a likely business question from a SQL query's structure."""
    tables = []
    aggs = []
    try:
        ast = sqlglot_parse_safe(sql)
        if ast:
            tables = [t.name for t in ast.find_all(exp.Table)]
            aggs = [a.sql() for a in ast.find_all(exp.AggFunc)]
    except Exception:
        pass

    parts = []
    if aggs:
        parts.append("Show aggregated metrics")
    else:
        parts.append("Query data")
    if tables:
        parts.append(f"from {', '.join(tables)}")

    return " ".join(parts)


def sqlglot_parse_safe(sql: str) -> exp.Expression | None:
    try:
        stmts = sqlglot.parse(sql, read="databricks")
        return stmts[0] if stmts else None
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 7: Rollback
# ═════════════════════════════════════════════════════════════════════════════

def rollback_space_update(
    space_id: str,
    previous_serialized_space: str,
    workspace_url: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    """
    Restores a Genie Space to its previous serialized_space state.
    Called automatically on injection failure or manual rollback request.
    """
    return update_genie_space(
        space_id=space_id,
        serialized_space=previous_serialized_space,
        workspace_url=workspace_url,
        token=token,
    )
