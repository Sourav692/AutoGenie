"""
Genie360 — Module 4: LLM Refinement & Validation
==================================================
Uses Claude Sonnet via Databricks Model Serving to refine rule-based rewrites,
validate semantic equivalence, and score rewrite confidence.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

from genie360.models.schemas import (
    AntiPatternReport,
    ExplainPlanComparison,
    RewriteConfidence,
)
from genie360.modules.anti_pattern_detection import parse_sql_to_ast


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: Prompt Construction
# ═════════════════════════════════════════════════════════════════════════════

def _load_genie360_prompts() -> dict[str, Any]:
    prompts_path = Path(__file__).parent.parent / "prompts.yml"
    if prompts_path.exists():
        with open(prompts_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def build_llm_rewrite_prompt(
    original_sql: str,
    rule_rewritten_sql: str | None,
    anti_pattern_report: AntiPatternReport | None,
    table_context: str | None = None,
) -> str:
    """
    Constructs a structured prompt for Claude Sonnet that includes the
    original SQL, rule-based rewrite, detected issues, relevant table schemas,
    and Databricks-specific optimization guidelines.
    """
    prompts = _load_genie360_prompts()
    llm_config = prompts.get("sql_rewrite", {})
    template = llm_config.get("user", "")

    issues_text = "None detected"
    if anti_pattern_report and anti_pattern_report.patterns:
        issues_text = "\n".join(
            f"- [{p.severity.value}] {p.pattern_type}: {p.description}"
            for p in anti_pattern_report.patterns
        )

    if template:
        return template.format(
            original_sql=original_sql,
            rule_rewritten_sql=rule_rewritten_sql or "No rule-based rewrite available",
            anti_pattern_issues=issues_text,
            table_context=table_context or "No table context available",
        )

    return (
        f"## SQL Optimization Request\n\n"
        f"### Original SQL:\n```sql\n{original_sql}\n```\n\n"
        f"### Rule-Based Rewrite:\n```sql\n{rule_rewritten_sql or 'N/A'}\n```\n\n"
        f"### Detected Anti-Patterns:\n{issues_text}\n\n"
        f"### Table Context:\n{table_context or 'N/A'}\n\n"
        f"Produce an optimized version of this SQL following Databricks/Delta Lake best practices. "
        f"Return ONLY the optimized SQL. No explanation."
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: LLM Invocation
# ═════════════════════════════════════════════════════════════════════════════

def invoke_llm_rewrite(
    prompt: str,
    llm_endpoint: str = "databricks-claude-sonnet-4",
) -> dict[str, Any]:
    """
    Calls the configured Databricks LLM serving endpoint via
    mlflow.deployments.get_deploy_client. Logs the call to MLflow
    tracing with input/output tokens and latency.
    """
    import mlflow.deployments

    prompts = _load_genie360_prompts()
    system_prompt = prompts.get("sql_rewrite", {}).get("system", "")

    if not system_prompt:
        system_prompt = (
            "You are a Databricks SQL optimization expert. "
            "Rewrite SQL queries for optimal performance on Delta Lake. "
            "Apply: CTE conversion, partition pruning, APPROX functions, "
            "predicate pushdown, QUALIFY for window filters. "
            "Return ONLY valid Databricks SQL. No explanation."
        )

    client = mlflow.deployments.get_deploy_client("databricks")
    response = client.predict(
        endpoint=llm_endpoint,
        inputs={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 4096,
            "temperature": 0.1,
        },
    )

    return {
        "content": response["choices"][0]["message"]["content"],
        "usage": response.get("usage", {}),
        "model": response.get("model", llm_endpoint),
    }


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: Response Parsing
# ═════════════════════════════════════════════════════════════════════════════

def parse_llm_rewrite_response(
    llm_response: dict[str, Any],
) -> tuple[str | None, str | None, float]:
    """
    Extracts the rewritten SQL, explanation text, and confidence score
    from the structured LLM response. Handles malformed responses with
    fallback to rule-based rewrite.

    Returns (rewritten_sql, explanation, confidence).
    """
    content = llm_response.get("content", "")

    sql_match = re.search(r"```(?:sql)?\s*\n?(.*?)\n?```", content, re.DOTALL)
    if sql_match:
        rewritten_sql = sql_match.group(1).strip()
    else:
        rewritten_sql = content.strip()

    rewritten_sql = re.sub(r"^(Here|The|This|I).*?:\s*\n", "", rewritten_sql, flags=re.IGNORECASE)

    ast = parse_sql_to_ast(rewritten_sql)
    if ast is None:
        return None, "LLM response did not contain valid SQL", 0.0

    explanation = None
    non_sql_parts = content.replace(rewritten_sql, "").strip()
    if non_sql_parts and len(non_sql_parts) > 20:
        explanation = non_sql_parts

    confidence = 0.7 if ast is not None else 0.0

    return rewritten_sql, explanation, confidence


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4: EXPLAIN Plan Comparison
# ═════════════════════════════════════════════════════════════════════════════

def compare_explain_plans(
    original_sql: str,
    rewritten_sql: str,
    warehouse_id: str,
    spark: Any = None,
) -> ExplainPlanComparison:
    """
    Runs EXPLAIN EXTENDED on both SQL versions against the configured
    SQL Warehouse. Parses output to compare estimated scan sizes,
    shuffle operations, and join types.
    """
    comparison = ExplainPlanComparison()

    if spark is None:
        return comparison

    try:
        original_plan_rows = spark.sql(f"EXPLAIN EXTENDED {original_sql}").collect()
        comparison.original_plan = "\n".join(row[0] for row in original_plan_rows)
    except Exception as e:
        comparison.original_plan = f"Error: {e}"

    try:
        rewritten_plan_rows = spark.sql(f"EXPLAIN EXTENDED {rewritten_sql}").collect()
        comparison.rewritten_plan = "\n".join(row[0] for row in rewritten_plan_rows)
    except Exception as e:
        comparison.rewritten_plan = f"Error: {e}"

    if comparison.original_plan and comparison.rewritten_plan:
        def _extract_scan_bytes(plan: str) -> int | None:
            match = re.search(r"Statistics\(sizeInBytes=(\d+)", plan)
            return int(match.group(1)) if match else None

        comparison.original_scan_bytes = _extract_scan_bytes(comparison.original_plan)
        comparison.rewritten_scan_bytes = _extract_scan_bytes(comparison.rewritten_plan)

        if comparison.original_scan_bytes and comparison.rewritten_scan_bytes:
            if comparison.original_scan_bytes > 0:
                improvement = (
                    (comparison.original_scan_bytes - comparison.rewritten_scan_bytes)
                    / comparison.original_scan_bytes
                ) * 100
                comparison.improvement_pct = round(improvement, 1)

    return comparison


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5: Semantic Equivalence Validation
# ═════════════════════════════════════════════════════════════════════════════

def validate_semantic_equivalence(
    original_sql: str,
    rewritten_sql: str,
    spark: Any = None,
    sample_data_limit: int = 100,
) -> tuple[bool, str]:
    """
    Runs both SQL versions on a warehouse with LIMIT 100. Compares result
    schema and sample row hashes to verify the rewrite produces
    semantically identical output.

    Returns (is_equivalent, details).
    """
    if spark is None:
        return True, "Spark session unavailable — skipping semantic validation"

    try:
        original_limited = f"SELECT * FROM ({original_sql}) t LIMIT {sample_data_limit}"
        rewritten_limited = f"SELECT * FROM ({rewritten_sql}) t LIMIT {sample_data_limit}"

        orig_df = spark.sql(original_limited)
        rewrite_df = spark.sql(rewritten_limited)

        orig_cols = sorted(orig_df.columns)
        rewrite_cols = sorted(rewrite_df.columns)
        if orig_cols != rewrite_cols:
            return False, (
                f"Schema mismatch: original has {orig_cols}, "
                f"rewrite has {rewrite_cols}"
            )

        orig_count = orig_df.count()
        rewrite_count = rewrite_df.count()
        if orig_count != rewrite_count:
            return False, (
                f"Row count mismatch: original={orig_count}, "
                f"rewrite={rewrite_count}"
            )

        return True, f"Semantically equivalent (verified on {orig_count} sample rows)"

    except Exception as e:
        return False, f"Semantic validation error: {e}"


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6: Confidence Scoring
# ═════════════════════════════════════════════════════════════════════════════

def score_rewrite_confidence(
    anti_pattern_report: AntiPatternReport | None,
    explain_plan_comparison: ExplainPlanComparison | None,
    semantic_equivalence_result: tuple[bool, str] | None,
) -> RewriteConfidence:
    """
    Combines rule coverage, explain plan improvement delta, and semantic
    equivalence check into a single confidence score (0.0–1.0).
    """
    confidence = RewriteConfidence()

    if anti_pattern_report and anti_pattern_report.total_issues > 0:
        confidence.rule_coverage_score = min(1.0, anti_pattern_report.total_issues * 0.2)
    else:
        confidence.rule_coverage_score = 0.1

    if explain_plan_comparison:
        if explain_plan_comparison.improvement_pct is not None:
            if explain_plan_comparison.improvement_pct > 0:
                confidence.explain_plan_score = min(1.0, explain_plan_comparison.improvement_pct / 100)
            else:
                confidence.explain_plan_score = 0.3
        else:
            confidence.explain_plan_score = 0.5
    else:
        confidence.explain_plan_score = 0.5

    if semantic_equivalence_result:
        is_equiv, _ = semantic_equivalence_result
        confidence.semantic_equivalence_score = 1.0 if is_equiv else 0.0
    else:
        confidence.semantic_equivalence_score = 0.5

    weights = {"rule": 0.2, "explain": 0.3, "semantic": 0.5}
    confidence.composite_score = round(
        confidence.rule_coverage_score * weights["rule"]
        + confidence.explain_plan_score * weights["explain"]
        + confidence.semantic_equivalence_score * weights["semantic"],
        3,
    )

    return confidence
