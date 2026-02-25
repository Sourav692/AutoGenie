"""
Genie360 Pipeline — End-to-End SQL Optimization
=================================================
Two pipeline modes:

**Downstream (reactive)** — ``run_optimization_pipeline()``
  1. Ingest Genie queries from system.query.history
  2. Detect anti-patterns via sqlglot AST analysis
  3. Apply rule-based SQL rewrites
  4. Refine with LLM (Claude Sonnet) + validate semantic equivalence
  5. Generate Markdown optimization report (before/after SQL)
  6. Inject optimized SQL back into Genie Space

**Upstream (proactive)** — ``run_upstream_optimization_pipeline()``
  1. Fetch example SQL queries from the Genie Space Knowledge Store
  2. Detect anti-patterns in those example queries
  3. Apply rule-based SQL rewrites
  4. Refine with LLM + validate semantic equivalence
  5. Generate Markdown before/after report
  6. Inject optimized examples back into the Knowledge Store

The upstream pattern controls what Genie generates *before* it ever
produces bad SQL: "If you can't intercept after generation, control
what gets generated in the first place."

Both pipelines expose a generator that yields
(log_message, result_dict_or_None) tuples for streaming progress.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Generator

import yaml

from genie360.models.schemas import (
    AnalysisResponse,
    AntiPatternReport,
    RewriteCandidate,
    SpaceInjectionResult,
)
from genie360.modules.anti_pattern_detection import run_anti_pattern_suite
from genie360.modules.genie_space_injection import (
    batch_inject_rewrites,
    rollback_space_update,
)
from genie360.modules.example_query_ingestion import (
    fetch_example_queries,
    inject_upstream_rewrites,
)
from genie360.modules.report_generator import generate_optimization_report
from genie360.modules.llm_refinement import (
    build_llm_rewrite_prompt,
    compare_explain_plans,
    invoke_llm_rewrite,
    parse_llm_rewrite_response,
    score_rewrite_confidence,
    validate_semantic_equivalence,
)
from genie360.modules.query_history_ingestion import (
    deduplicate_queries,
    enrich_query_metadata,
    get_genie_queries,
    normalize_query_record,
)
from genie360.modules.sql_rewrite_engine import (
    apply_rule_based_rewrites,
    calculate_rewrite_impact,
)


# ═════════════════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════════════════

def load_genie360_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Loads Genie360 configuration from config.yml."""
    if config_path is None:
        config_path = Path(__file__).parent / "config.yml"
    config_path = Path(config_path)
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


# ═════════════════════════════════════════════════════════════════════════════
# Stage 1: Query Ingestion
# ═════════════════════════════════════════════════════════════════════════════

def _stage_ingest(
    spark: Any,
    space_id: str,
    config: dict[str, Any],
) -> Generator[tuple[str, Any], None, list[dict]]:
    """Ingests and enriches Genie queries from system.query.history."""
    lookback = config.get("lookback_days", 30)
    limit = config.get("max_queries_per_space", 500)

    yield "Stage 1: Fetching Genie queries from system.query.history...", None

    raw_queries = get_genie_queries(spark, space_id, lookback, limit)
    yield f"  Retrieved {len(raw_queries)} raw queries", None

    records = [normalize_query_record(r) for r in raw_queries]
    yield f"  Normalized {len(records)} query records", None

    deduped = deduplicate_queries(records)
    yield f"  Deduplicated to {len(deduped)} unique queries", None

    enriched = [enrich_query_metadata(r) for r in deduped]
    yield f"  Enriched metadata for {len(enriched)} queries", None

    return enriched


# ═════════════════════════════════════════════════════════════════════════════
# Stage 2: Anti-Pattern Detection
# ═════════════════════════════════════════════════════════════════════════════

def _stage_detect(
    enriched_queries: list,
    table_stats: dict[str, dict[str, Any]] | None = None,
    column_metadata: dict[str, dict[str, str]] | None = None,
) -> Generator[tuple[str, Any], None, list[AntiPatternReport]]:
    """Runs anti-pattern detection on all ingested queries."""
    yield "Stage 2: Running anti-pattern detection...", None

    reports = []
    for record in enriched_queries:
        report = run_anti_pattern_suite(
            sql_text=record.statement_text,
            table_stats=table_stats,
            column_metadata=column_metadata,
        )
        report.query_id = record.query_id
        reports.append(report)

    with_issues = [r for r in reports if r.total_issues > 0]
    total_issues = sum(r.total_issues for r in reports)
    yield f"  Analyzed {len(reports)} queries, found {total_issues} issues in {len(with_issues)} queries", None

    return reports


# ═════════════════════════════════════════════════════════════════════════════
# Stage 3: Rule-Based Rewriting
# ═════════════════════════════════════════════════════════════════════════════

def _stage_rewrite(
    reports: list[AntiPatternReport],
    config: dict[str, Any],
    column_metadata: dict[str, list[str]] | None = None,
    column_types: dict[str, dict[str, str]] | None = None,
    partition_columns: dict[str, list[str]] | None = None,
) -> Generator[tuple[str, Any], None, list[RewriteCandidate]]:
    """Applies rule-based rewrites to queries with detected anti-patterns."""
    yield "Stage 3: Applying rule-based SQL rewrites...", None

    candidates = []
    safe_limit = config.get("safe_limit", 10000)
    date_hint = config.get("date_range_hint_days", 30)

    actionable_reports = [r for r in reports if r.total_issues > 0]

    for report in actionable_reports:
        rewritten_sql, rules_applied = apply_rule_based_rewrites(
            sql_text=report.original_sql,
            column_metadata=column_metadata,
            column_types=column_types,
            partition_columns=partition_columns,
            safe_limit=safe_limit,
            date_range_hint=date_hint,
        )

        candidate = RewriteCandidate(
            query_id=report.query_id,
            original_sql=report.original_sql,
            rule_rewritten_sql=rewritten_sql if rules_applied else None,
            anti_pattern_report=report,
            rules_applied=rules_applied,
        )
        candidates.append(candidate)

    rewritten_count = sum(1 for c in candidates if c.rule_rewritten_sql)
    yield f"  Generated {rewritten_count} rule-based rewrites from {len(actionable_reports)} queries", None

    return candidates


# ═════════════════════════════════════════════════════════════════════════════
# Stage 4: LLM Refinement & Validation
# ═════════════════════════════════════════════════════════════════════════════

def _stage_llm_refine(
    candidates: list[RewriteCandidate],
    config: dict[str, Any],
    spark: Any = None,
    table_context: str | None = None,
) -> Generator[tuple[str, Any], None, list[RewriteCandidate]]:
    """Refines rule-based rewrites with LLM and validates equivalence."""
    yield "Stage 4: LLM refinement and validation...", None

    llm_endpoint = config.get("llm_endpoint", "databricks-claude-sonnet-4")
    warehouse_id = config.get("warehouse_id", "")
    min_confidence = config.get("min_confidence_for_injection", 0.85)

    refined_count = 0
    for candidate in candidates:
        if not candidate.rule_rewritten_sql:
            continue

        try:
            prompt = build_llm_rewrite_prompt(
                original_sql=candidate.original_sql,
                rule_rewritten_sql=candidate.rule_rewritten_sql,
                anti_pattern_report=candidate.anti_pattern_report,
                table_context=table_context,
            )

            llm_response = invoke_llm_rewrite(prompt, llm_endpoint)
            llm_sql, explanation, llm_confidence = parse_llm_rewrite_response(llm_response)

            if llm_sql:
                candidate.llm_rewritten_sql = llm_sql
                candidate.final_sql = llm_sql
            else:
                candidate.final_sql = candidate.rule_rewritten_sql

            explain_comparison = None
            if spark and warehouse_id:
                explain_comparison = compare_explain_plans(
                    candidate.original_sql,
                    candidate.final_sql,
                    warehouse_id,
                    spark,
                )

            semantic_result = None
            if spark:
                semantic_result = validate_semantic_equivalence(
                    candidate.original_sql,
                    candidate.final_sql,
                    spark,
                )

            confidence = score_rewrite_confidence(
                candidate.anti_pattern_report,
                explain_comparison,
                semantic_result,
            )
            candidate.confidence_score = confidence.composite_score

            impact = calculate_rewrite_impact(
                candidate.original_sql,
                candidate.final_sql,
            )
            candidate.estimated_duration_improvement_pct = impact.get("estimated_improvement_pct")

            refined_count += 1

        except Exception as e:
            candidate.final_sql = candidate.rule_rewritten_sql
            candidate.confidence_score = 0.5
            yield f"  Warning: LLM refinement failed for query {candidate.query_id}: {e}", None

    yield f"  Refined {refined_count} candidates with LLM", None

    approved = [c for c in candidates if c.confidence_score >= min_confidence]
    yield f"  {len(approved)} rewrites meet confidence threshold ({min_confidence})", None

    return candidates


# ═════════════════════════════════════════════════════════════════════════════
# Stage 5: Optimization Report Generation
# ═════════════════════════════════════════════════════════════════════════════

def _stage_report(
    space_id: str,
    reports: list[AntiPatternReport],
    candidates: list[RewriteCandidate],
    config: dict[str, Any],
) -> Generator[tuple[str, Any], None, str | None]:
    """Generates a Markdown report with before/after SQL for every optimized query."""
    yield "Stage 5: Generating optimization report...", None

    try:
        output_dir = config.get("report_output_path") or config.get("output_path")
        report_path = generate_optimization_report(
            space_id=space_id,
            reports=reports,
            candidates=candidates,
            config=config,
            output_dir=output_dir,
        )
        yield f"  Report written to: {report_path}", None
        return report_path
    except Exception as e:
        yield f"  Warning: Report generation failed: {e}", None
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Stage 6: Injection (Optional)
# ═════════════════════════════════════════════════════════════════════════════

def _stage_inject(
    space_id: str,
    candidates: list[RewriteCandidate],
    config: dict[str, Any],
) -> Generator[tuple[str, Any], None, SpaceInjectionResult | None]:
    """Injects approved rewrites into the Genie Space."""
    auto_inject = config.get("auto_inject_enabled", False)
    min_confidence = config.get("min_confidence_for_injection", 0.85)
    max_rewrites = config.get("max_rewrites_per_injection", 10)

    approved = [
        c for c in candidates
        if c.confidence_score >= min_confidence and c.final_sql
    ][:max_rewrites]

    if not approved:
        yield "Stage 6: No rewrites meet injection criteria — skipping injection", None
        return None

    if not auto_inject:
        yield (
            f"Stage 6: {len(approved)} rewrites ready for injection "
            f"(auto_inject=false, use POST /inject to apply)"
        ), None
        return None

    yield f"Stage 6: Injecting {len(approved)} approved rewrites...", None

    result = batch_inject_rewrites(space_id, approved)

    if result.success:
        yield f"  Successfully injected {result.rewrites_injected} rewrites", None
    else:
        yield f"  Injection failed: {result.error_message}", None

    return result


# ═════════════════════════════════════════════════════════════════════════════
# Full Pipeline
# ═════════════════════════════════════════════════════════════════════════════

def run_optimization_pipeline(
    spark: Any,
    space_id: str,
    config: dict[str, Any] | None = None,
    table_stats: dict[str, dict[str, Any]] | None = None,
    column_metadata: dict[str, list[str]] | None = None,
    column_types: dict[str, dict[str, str]] | None = None,
    partition_columns: dict[str, list[str]] | None = None,
    table_context: str | None = None,
) -> Generator[tuple[str, dict | None], None, None]:
    """
    Runs the complete Genie360 optimization pipeline.

    Yields (log_message, result_dict_or_None) tuples for streaming progress.
    Final yield includes the complete pipeline result.
    """
    if config is None:
        config = load_genie360_config()

    yield "=" * 60, None
    yield "Genie360 — SQL Query Optimization Pipeline", None
    yield f"Space ID: {space_id}", None
    yield "=" * 60, None

    # Stage 1: Ingest
    enriched_queries = []
    gen = _stage_ingest(spark, space_id, config)
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        enriched_queries = e.value or []

    if not enriched_queries:
        yield "No Genie queries found — pipeline complete", None
        return

    # Stage 2: Detect
    reports = []
    gen = _stage_detect(enriched_queries, table_stats, column_metadata)
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        reports = e.value or []

    # Stage 3: Rule-based Rewrite
    candidates = []
    gen = _stage_rewrite(reports, config, column_metadata, column_types, partition_columns)
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        candidates = e.value or []

    # Stage 4: LLM Refinement
    gen = _stage_llm_refine(candidates, config, spark, table_context)
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        candidates = e.value or candidates

    # Stage 5: Report Generation
    report_path = None
    gen = _stage_report(space_id, reports, candidates, config)
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        report_path = e.value

    # Stage 6: Injection
    injection_result = None
    gen = _stage_inject(space_id, candidates, config)
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        injection_result = e.value

    # Final Summary
    yield "=" * 60, None
    yield "Pipeline Complete", None

    analysis = AnalysisResponse(
        space_id=space_id,
        queries_analyzed=len(enriched_queries),
        anti_pattern_reports=reports,
    )
    analysis.compute_summary()

    result = {
        "space_id": space_id,
        "queries_analyzed": len(enriched_queries),
        "total_issues": analysis.total_issues_found,
        "critical_issues": analysis.critical_count,
        "high_issues": analysis.high_count,
        "candidates_generated": len(candidates),
        "candidates_above_threshold": len([
            c for c in candidates
            if c.confidence_score >= config.get("min_confidence_for_injection", 0.85)
        ]),
        "report_path": report_path,
        "injection_result": injection_result.model_dump() if injection_result else None,
        "rewrites": [c.model_dump() for c in candidates],
    }

    yield f"  Queries analyzed: {result['queries_analyzed']}", None
    yield f"  Total issues found: {result['total_issues']}", None
    yield f"  Rewrite candidates: {result['candidates_generated']}", None
    yield f"  Above confidence threshold: {result['candidates_above_threshold']}", None
    yield "=" * 60, result


# ═════════════════════════════════════════════════════════════════════════════
# Upstream Optimization Pipeline
# ═════════════════════════════════════════════════════════════════════════════

def run_upstream_optimization_pipeline(
    space_id: str,
    config: dict[str, Any] | None = None,
    spark: Any = None,
    table_stats: dict[str, dict[str, Any]] | None = None,
    column_metadata: dict[str, list[str]] | None = None,
    column_types: dict[str, dict[str, str]] | None = None,
    partition_columns: dict[str, list[str]] | None = None,
    table_context: str | None = None,
    auto_inject: bool = False,
) -> Generator[tuple[str, dict | None], None, None]:
    """
    Upstream Injection Pipeline — optimizes the Genie Space's own example
    SQL queries so that Genie generates better SQL from the start.

    Flow:
      1. Fetch example queries from the Genie Space Knowledge Store
      2. Detect anti-patterns in those example queries
      3. Apply rule-based SQL rewrites
      4. Refine with LLM + confidence scoring
      5. Generate before/after Markdown report
      6. Inject optimized examples back into the Knowledge Store

    Yields (log_message, result_dict_or_None) for streaming progress.
    """
    if config is None:
        config = load_genie360_config()

    yield "=" * 60, None
    yield "Genie360 — Upstream Injection Pipeline", None
    yield f"Space ID: {space_id}", None
    yield "Strategy: Optimize example queries BEFORE Genie generates SQL", None
    yield "=" * 60, None

    # ── Stage 1: Fetch example queries from the Knowledge Store ──────────
    yield "Stage 1: Fetching example queries from Genie Space Knowledge Store...", None

    try:
        example_records, space_config = fetch_example_queries(space_id)
    except Exception as e:
        yield f"  ERROR: Failed to fetch Genie Space: {e}", None
        return

    if not example_records:
        yield "  No example queries found in the Knowledge Store — nothing to optimize", None
        return

    yield f"  Found {len(example_records)} example queries in the Knowledge Store", None

    for rec in example_records:
        q_label = rec.question_text or "(no question)"
        yield f"    • {q_label}: {rec.statement_text[:80]}...", None

    # ── Stage 2: Anti-pattern detection ──────────────────────────────────
    yield "", None
    yield "Stage 2: Running anti-pattern detection on example queries...", None

    reports: list[AntiPatternReport] = []
    for record in example_records:
        report = run_anti_pattern_suite(
            sql_text=record.statement_text,
            table_stats=table_stats,
            column_metadata=column_metadata,
        )
        report.query_id = record.query_id
        reports.append(report)

    with_issues = [r for r in reports if r.total_issues > 0]
    total_issues = sum(r.total_issues for r in reports)
    yield f"  Analyzed {len(reports)} example queries, found {total_issues} issues in {len(with_issues)} queries", None

    if not with_issues:
        yield "  All example queries are clean — no anti-patterns detected!", None

        report_path = _generate_upstream_report(
            space_id, reports, [], config, example_records,
        )
        if report_path:
            yield f"  Report written to: {report_path}", None

        yield "=" * 60, {"space_id": space_id, "example_queries": len(example_records),
                         "total_issues": 0, "rewrites_generated": 0, "report_path": report_path}
        return

    # ── Stage 3: Rule-based rewriting ────────────────────────────────────
    yield "", None
    gen = _stage_rewrite(reports, config, column_metadata, column_types, partition_columns)
    candidates: list[RewriteCandidate] = []
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        candidates = e.value or []

    # ── Stage 4: LLM refinement ──────────────────────────────────────────
    yield "", None
    gen = _stage_llm_refine(candidates, config, spark, table_context)
    try:
        while True:
            msg, data = next(gen)
            yield msg, data
    except StopIteration as e:
        candidates = e.value or candidates

    # ── Stage 5: Report generation ───────────────────────────────────────
    yield "", None
    yield "Stage 5: Generating upstream optimization report...", None

    report_path = _generate_upstream_report(
        space_id, reports, candidates, config, example_records,
    )
    if report_path:
        yield f"  Report written to: {report_path}", None

    # ── Stage 6: Upstream injection ──────────────────────────────────────
    yield "", None
    min_confidence = config.get("min_confidence_for_injection", 0.85)
    approved = [c for c in candidates if c.confidence_score >= min_confidence and c.final_sql]

    record_index = {r.query_id: r for r in example_records}
    injection_result = None

    if not approved:
        yield "Stage 6: No rewrites meet confidence threshold — skipping injection", None
    elif not auto_inject:
        yield (
            f"Stage 6: {len(approved)} optimized example queries ready "
            f"(auto_inject=false, set auto_inject=True to apply)"
        ), None
    else:
        yield f"Stage 6: Injecting {len(approved)} optimized example queries into Knowledge Store...", None

        rewrites_payload = []
        for c in approved:
            rec = record_index.get(c.query_id)
            rewrites_payload.append({
                "example_entry_id": rec.example_entry_id if rec else "",
                "original_sql": c.original_sql,
                "optimized_sql": c.final_sql,
                "confidence_score": c.confidence_score,
                "rules_applied": c.rules_applied,
            })

        try:
            injection_result = inject_upstream_rewrites(
                space_id=space_id,
                space_config=space_config,
                rewrites=rewrites_payload,
            )
            yield (
                f"  Injected {injection_result['injected']} optimized examples, "
                f"skipped {injection_result['skipped']}"
            ), None
        except Exception as e:
            yield f"  ERROR: Upstream injection failed: {e}", None

    # ── Final summary ────────────────────────────────────────────────────
    yield "", None
    yield "=" * 60, None
    yield "Upstream Pipeline Complete", None

    result = {
        "space_id": space_id,
        "pipeline_mode": "upstream",
        "example_queries_found": len(example_records),
        "total_issues": total_issues,
        "queries_with_issues": len(with_issues),
        "rewrites_generated": len(candidates),
        "candidates_above_threshold": len(approved),
        "report_path": report_path,
        "injection_result": injection_result,
        "rewrites": [c.model_dump() for c in candidates],
    }

    yield f"  Example queries in Knowledge Store: {result['example_queries_found']}", None
    yield f"  Anti-patterns found: {result['total_issues']}", None
    yield f"  Rewrites generated: {result['rewrites_generated']}", None
    yield f"  Above confidence threshold: {result['candidates_above_threshold']}", None
    if report_path:
        yield f"  Report: {report_path}", None
    yield "=" * 60, result


def _generate_upstream_report(
    space_id: str,
    reports: list[AntiPatternReport],
    candidates: list[RewriteCandidate],
    config: dict[str, Any],
    example_records: list,
) -> str | None:
    """Generates the Markdown report with question context for upstream queries."""
    try:
        question_map = {r.query_id: r.question_text for r in example_records if r.question_text}
        output_dir = config.get("report_output_path") or config.get("output_path")
        report_path = generate_optimization_report(
            space_id=space_id,
            reports=reports,
            candidates=candidates,
            config=config,
            output_dir=output_dir,
            report_mode="upstream",
            question_map=question_map,
        )
        return report_path
    except Exception:
        return None
