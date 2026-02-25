"""
Genie360 — FastAPI Application
================================
REST API for the Genie360 SQL optimization engine.

Endpoints:
  POST /api/v1/spaces/{space_id}/analyze            — Downstream analysis (query history)
  POST /api/v1/spaces/{space_id}/upstream-optimize   — Upstream: optimize example queries in Knowledge Store
  GET  /api/v1/spaces/{space_id}/rewrites            — Get pending rewrites with scores
  POST /api/v1/spaces/{space_id}/inject              — Approve and inject rewrites
  POST /api/v1/spaces/{space_id}/rollback            — Restore space to pre-injection state
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from genie360.models.schemas import (
    AnalysisResponse,
    AntiPatternReport,
    InjectionResponse,
    RewriteCandidate,
    RewritesResponse,
    RollbackResponse,
    SpaceInjectionResult,
)
from genie360.modules.anti_pattern_detection import run_anti_pattern_suite
from genie360.modules.genie_space_injection import (
    batch_inject_rewrites,
    get_genie_space,
    rollback_space_update,
)
from genie360.modules.query_history_ingestion import (
    calculate_query_cost_estimate,
    deduplicate_queries,
    enrich_query_metadata,
    get_genie_queries,
    normalize_query_record,
)
from genie360.modules.example_query_ingestion import (
    fetch_example_queries,
    inject_upstream_rewrites,
)
from genie360.modules.sql_rewrite_engine import apply_rule_based_rewrites
from genie360.pipeline import load_genie360_config

load_dotenv(Path(__file__).parent / ".env")

app = FastAPI(
    title="Genie360",
    description="Genie Performance & Cost Optimization Engine",
    version="0.1.0",
)

_config = load_genie360_config()

_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

_rewrite_store: dict[str, list[RewriteCandidate]] = {}
_rollback_store: dict[str, str] = {}


# ═════════════════════════════════════════════════════════════════════════════
# UI
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    """Serves the Genie360 frontend."""
    return FileResponse(str(_STATIC_DIR / "index.html"))


# ═════════════════════════════════════════════════════════════════════════════
# Request / Response Models
# ═════════════════════════════════════════════════════════════════════════════

class AnalyzeRequest(BaseModel):
    lookback_days: int | None = None
    table_stats: dict[str, dict[str, Any]] | None = None
    column_metadata: dict[str, dict[str, str]] | None = None


class InjectRequest(BaseModel):
    rewrite_ids: list[str] | None = None
    inject_all_above_threshold: bool = False
    min_confidence: float | None = None


class UpstreamOptimizeRequest(BaseModel):
    auto_inject: bool = False
    warehouse_id: str | None = None
    table_stats: dict[str, dict[str, Any]] | None = None
    column_metadata: dict[str, dict[str, str]] | None = None
    min_confidence: float | None = None


class RollbackRequest(BaseModel):
    confirm: bool = False


# ═════════════════════════════════════════════════════════════════════════════
# Health Check
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health_check():
    return {"status": "healthy", "version": "0.1.0", "module": "genie360"}


# ═════════════════════════════════════════════════════════════════════════════
# POST /api/v1/spaces/{space_id}/analyze
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/v1/spaces/{space_id}/analyze", response_model=AnalysisResponse)
def analyze_space(space_id: str, request: AnalyzeRequest | None = None):
    """
    Triggers full analysis pipeline on a space. Fetches Genie queries from
    system.query.history, runs anti-pattern detection, generates rewrites,
    and returns AntiPatternReport.
    """
    req = request or AnalyzeRequest()
    lookback = req.lookback_days or _config.get("lookback_days", 30)
    table_stats = req.table_stats
    column_metadata = req.column_metadata

    try:
        spark = _get_spark()
    except Exception:
        spark = None

    reports: list[AntiPatternReport] = []
    candidates: list[RewriteCandidate] = []

    if spark:
        raw = get_genie_queries(spark, space_id, lookback)
        records = [normalize_query_record(r) for r in raw]
        deduped = deduplicate_queries(records)
        enriched = [enrich_query_metadata(r) for r in deduped]

        for record in enriched:
            report = run_anti_pattern_suite(
                record.statement_text, table_stats, column_metadata
            )
            report.query_id = record.query_id
            reports.append(report)

            if report.total_issues > 0:
                rewritten_sql, rules = apply_rule_based_rewrites(record.statement_text)
                candidate = RewriteCandidate(
                    query_id=record.query_id,
                    original_sql=record.statement_text,
                    rule_rewritten_sql=rewritten_sql if rules else None,
                    final_sql=rewritten_sql if rules else None,
                    anti_pattern_report=report,
                    rules_applied=rules,
                    confidence_score=0.7 if rules else 0.0,
                )
                candidates.append(candidate)

        _rewrite_store[space_id] = candidates

    response = AnalysisResponse(
        space_id=space_id,
        queries_analyzed=len(reports),
        anti_pattern_reports=reports,
    )
    response.compute_summary()
    return response


# ═════════════════════════════════════════════════════════════════════════════
# POST /api/v1/spaces/{space_id}/upstream-optimize
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/v1/spaces/{space_id}/upstream-optimize")
def upstream_optimize(space_id: str, request: UpstreamOptimizeRequest | None = None):
    """
    Upstream Injection: fetches the Genie Space's own example SQL queries,
    detects anti-patterns, rewrites them, scores confidence, generates a
    before/after Markdown report, and optionally injects the optimized
    examples back into the Knowledge Store.
    """
    req = request or UpstreamOptimizeRequest()
    min_conf = req.min_confidence or _config.get("min_confidence_for_injection", 0.85)

    if req.warehouse_id:
        os.environ["WAREHOUSE_ID"] = req.warehouse_id

    try:
        example_records, space_config = fetch_example_queries(space_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Genie Space: {e}")

    if not example_records:
        return {
            "space_id": space_id,
            "pipeline_mode": "upstream",
            "example_queries_found": 0,
            "message": "No example queries found in the Knowledge Store.",
        }

    reports: list[AntiPatternReport] = []
    candidates: list[RewriteCandidate] = []

    for record in example_records:
        report = run_anti_pattern_suite(
            record.statement_text, req.table_stats, req.column_metadata
        )
        report.query_id = record.query_id
        reports.append(report)

        if report.total_issues > 0:
            rewritten_sql, rules = apply_rule_based_rewrites(record.statement_text)
            candidate = RewriteCandidate(
                query_id=record.query_id,
                original_sql=record.statement_text,
                rule_rewritten_sql=rewritten_sql if rules else None,
                final_sql=rewritten_sql if rules else None,
                anti_pattern_report=report,
                rules_applied=rules,
                confidence_score=0.7 if rules else 0.0,
            )
            candidates.append(candidate)

    _rewrite_store[space_id] = candidates

    question_map = {r.query_id: r.question_text for r in example_records if r.question_text}

    from genie360.modules.report_generator import generate_optimization_report
    report_path = None
    try:
        output_dir = _config.get("report_output_path") or _config.get("output_path")
        report_path = generate_optimization_report(
            space_id=space_id,
            reports=reports,
            candidates=candidates,
            config=_config,
            output_dir=output_dir,
            report_mode="upstream",
            question_map=question_map,
        )
    except Exception:
        pass

    injection_result = None
    if req.auto_inject:
        approved = [
            c for c in candidates
            if c.confidence_score >= min_conf and c.final_sql
        ]
        if approved:
            record_index = {r.query_id: r for r in example_records}
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
                if space_config:
                    from genie360.modules.genie_space_injection import serialize_space_config
                    _rollback_store[space_id] = serialize_space_config(space_config)
            except Exception as e:
                injection_result = {"error": str(e)}

    analysis = AnalysisResponse(
        space_id=space_id,
        queries_analyzed=len(reports),
        anti_pattern_reports=reports,
    )
    analysis.compute_summary()

    return {
        "space_id": space_id,
        "pipeline_mode": "upstream",
        "example_queries_found": len(example_records),
        "queries_with_issues": sum(1 for r in reports if r.total_issues > 0),
        "total_issues": analysis.total_issues_found,
        "critical_count": analysis.critical_count,
        "high_count": analysis.high_count,
        "rewrites_generated": len(candidates),
        "above_confidence_threshold": sum(1 for c in candidates if c.confidence_score >= min_conf),
        "report_path": report_path,
        "injection_result": injection_result,
    }


# ═════════════════════════════════════════════════════════════════════════════
# GET /api/v1/spaces/{space_id}/rewrites
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/spaces/{space_id}/rewrites", response_model=RewritesResponse)
def get_rewrites(space_id: str):
    """
    Returns pending rewrites for a space with confidence scores and
    impact estimates.
    """
    candidates = _rewrite_store.get(space_id, [])
    pending = [c for c in candidates if c.status == "pending"]
    total_savings = sum(c.estimated_cost_savings_usd or 0 for c in pending)

    return RewritesResponse(
        space_id=space_id,
        pending_rewrites=pending,
        total_estimated_savings_usd=total_savings,
    )


# ═════════════════════════════════════════════════════════════════════════════
# POST /api/v1/spaces/{space_id}/inject
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/v1/spaces/{space_id}/inject", response_model=InjectionResponse)
def inject_rewrites(space_id: str, request: InjectRequest | None = None):
    """
    Approves and injects a batch of rewrites into a Genie Space.
    """
    req = request or InjectRequest()
    candidates = _rewrite_store.get(space_id, [])

    if not candidates:
        raise HTTPException(
            status_code=404,
            detail=f"No rewrites found for space {space_id}. Run /analyze first.",
        )

    min_conf = req.min_confidence or _config.get("min_confidence_for_injection", 0.85)

    if req.rewrite_ids:
        to_inject = [c for c in candidates if c.query_id in req.rewrite_ids]
    elif req.inject_all_above_threshold:
        to_inject = [c for c in candidates if c.confidence_score >= min_conf and c.final_sql]
    else:
        to_inject = [c for c in candidates if c.confidence_score >= min_conf and c.final_sql]

    max_rewrites = _config.get("max_rewrites_per_injection", 10)
    to_inject = to_inject[:max_rewrites]

    if not to_inject:
        raise HTTPException(
            status_code=400,
            detail="No rewrites meet injection criteria.",
        )

    result = batch_inject_rewrites(space_id, to_inject)

    if result.previous_serialized_space:
        _rollback_store[space_id] = result.previous_serialized_space

    return InjectionResponse(space_id=space_id, result=result)


# ═════════════════════════════════════════════════════════════════════════════
# POST /api/v1/spaces/{space_id}/rollback
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/v1/spaces/{space_id}/rollback", response_model=RollbackResponse)
def rollback(space_id: str, request: RollbackRequest | None = None):
    """
    Restores space to pre-injection state.
    """
    req = request or RollbackRequest()

    if not req.confirm:
        raise HTTPException(
            status_code=400,
            detail="Set confirm=true to proceed with rollback.",
        )

    previous = _rollback_store.get(space_id)
    if not previous:
        raise HTTPException(
            status_code=404,
            detail=f"No rollback state found for space {space_id}.",
        )

    try:
        rollback_space_update(space_id, previous)
        del _rollback_store[space_id]
        return RollbackResponse(
            space_id=space_id,
            success=True,
            message="Space successfully restored to pre-injection state.",
        )
    except Exception as e:
        return RollbackResponse(
            space_id=space_id,
            success=False,
            message=f"Rollback failed: {e}",
        )


# ═════════════════════════════════════════════════════════════════════════════
# GET /api/v1/report — Download a generated Markdown report
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/report")
def download_report(path: str = Query(..., description="Server-side path to the report file")):
    """Returns a generated Markdown report file for download."""
    report_file = Path(path)
    if not report_file.exists() or not report_file.is_file():
        raise HTTPException(status_code=404, detail="Report file not found.")
    return FileResponse(
        str(report_file),
        media_type="text/markdown",
        filename=report_file.name,
    )


# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════

def _get_spark():
    """Attempts to get a Spark session — works in Databricks or with Connect."""
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        pass
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except ImportError:
        raise RuntimeError("No Spark session available")


# ═════════════════════════════════════════════════════════════════════════════
# Entry Point
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
