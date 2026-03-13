"""
Genie360 Data Models
====================
Pydantic schemas for structured data flowing through the optimization pipeline.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ═════════════════════════════════════════════════════════════════════════════
# Enums
# ═════════════════════════════════════════════════════════════════════════════

class AntiPatternSeverity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class DurationTier(str, Enum):
    FAST = "FAST"          # < 2s
    MODERATE = "MODERATE"  # 2s–10s
    SLOW = "SLOW"          # 10s–60s
    CRITICAL = "CRITICAL"  # > 60s


class VolumeTier(str, Enum):
    SMALL = "SMALL"    # < 100 MB
    MEDIUM = "MEDIUM"  # 100 MB–1 GB
    LARGE = "LARGE"    # 1 GB–10 GB
    MASSIVE = "MASSIVE" # > 10 GB


class CostTier(str, Enum):
    SILENT = "SILENT"    # < $1
    INFO = "INFO"        # $1–$10
    CONFIRM = "CONFIRM"  # $10–$50
    BLOCK = "BLOCK"      # > $50


# ═════════════════════════════════════════════════════════════════════════════
# Query History Models
# ═════════════════════════════════════════════════════════════════════════════

class GenieQueryRecord(BaseModel):
    """Flattened, enriched query record from system.query.history."""

    query_id: str
    statement_text: str
    genie_space_id: str | None = None
    warehouse_id: str | None = None
    warehouse_type: str | None = None
    execution_status: str | None = None
    execution_duration_ms: int | None = None
    total_duration_ms: int | None = None
    read_bytes: int | None = None
    read_rows: int | None = None
    produced_rows: int | None = None
    spilled_local_bytes: int | None = None
    from_result_cache: bool = False
    start_time: datetime | None = None
    executed_by: str | None = None

    # Derived / enriched fields
    estimated_cost_usd: float | None = None
    duration_tier: DurationTier | None = None
    volume_tier: VolumeTier | None = None
    sql_fingerprint: str | None = None

    model_config = {"extra": "allow"}


# ═════════════════════════════════════════════════════════════════════════════
# Genie Space Example Query Models
# ═════════════════════════════════════════════════════════════════════════════

class ExampleQueryRecord(BaseModel):
    """An example SQL query extracted from a Genie Space's Knowledge Store."""

    query_id: str
    statement_text: str
    question_text: str | None = None
    example_entry_id: str | None = None
    usage_guidance: list[str] = Field(default_factory=list)
    genie_space_id: str | None = None

    model_config = {"extra": "allow"}


# ═════════════════════════════════════════════════════════════════════════════
# Anti-Pattern Models
# ═════════════════════════════════════════════════════════════════════════════

class AntiPattern(BaseModel):
    """A single detected anti-pattern in a SQL query."""

    pattern_type: str
    severity: AntiPatternSeverity
    description: str
    line_number: int | None = None
    suggested_fix: str | None = None
    estimated_impact: str | None = None


class AntiPatternReport(BaseModel):
    """Aggregated anti-pattern analysis for a single query."""

    query_id: str | None = None
    original_sql: str
    patterns: list[AntiPattern] = Field(default_factory=list)
    total_issues: int = 0
    max_severity: AntiPatternSeverity = AntiPatternSeverity.INFO
    parse_success: bool = True
    parse_error: str | None = None

    def add_pattern(self, pattern: AntiPattern) -> None:
        self.patterns.append(pattern)
        self.total_issues = len(self.patterns)
        severity_order = list(AntiPatternSeverity)
        if severity_order.index(pattern.severity) < severity_order.index(self.max_severity):
            self.max_severity = pattern.severity


# ═════════════════════════════════════════════════════════════════════════════
# Rewrite Models
# ═════════════════════════════════════════════════════════════════════════════

class RewriteCandidate(BaseModel):
    """A proposed SQL rewrite with before/after and confidence scoring."""

    query_id: str | None = None
    original_sql: str
    rule_rewritten_sql: str | None = None
    llm_rewritten_sql: str | None = None
    final_sql: str | None = None
    anti_pattern_report: AntiPatternReport | None = None
    rules_applied: list[str] = Field(default_factory=list)
    confidence_score: float = 0.0
    estimated_cost_savings_usd: float | None = None
    estimated_duration_improvement_pct: float | None = None
    status: str = "pending"  # pending | approved | injected | rejected | rolled_back
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ExplainPlanComparison(BaseModel):
    """Side-by-side EXPLAIN plan comparison for original vs rewritten SQL."""

    original_plan: str | None = None
    rewritten_plan: str | None = None
    original_scan_bytes: int | None = None
    rewritten_scan_bytes: int | None = None
    improvement_pct: float | None = None
    plans_equivalent: bool = True


class RewriteConfidence(BaseModel):
    """Composite confidence score for a rewrite candidate."""

    rule_coverage_score: float = 0.0
    explain_plan_score: float = 0.0
    semantic_equivalence_score: float = 0.0
    composite_score: float = 0.0


# ═════════════════════════════════════════════════════════════════════════════
# Genie Space Injection Models
# ═════════════════════════════════════════════════════════════════════════════

class SpaceInjectionResult(BaseModel):
    """Result of injecting optimized SQL into a Genie Space."""

    space_id: str
    rewrites_injected: int = 0
    rewrites_failed: int = 0
    previous_serialized_space: str | None = None
    new_serialized_space: str | None = None
    success: bool = False
    error_message: str | None = None
    injected_at: datetime = Field(default_factory=datetime.utcnow)


# ═════════════════════════════════════════════════════════════════════════════
# API Response Models
# ═════════════════════════════════════════════════════════════════════════════

class AnalysisResponse(BaseModel):
    """Response from the /analyze endpoint."""

    space_id: str
    queries_analyzed: int = 0
    anti_pattern_reports: list[AntiPatternReport] = Field(default_factory=list)
    total_issues_found: int = 0
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0

    def compute_summary(self) -> None:
        self.total_issues_found = sum(r.total_issues for r in self.anti_pattern_reports)
        self.critical_count = sum(
            1 for r in self.anti_pattern_reports
            for p in r.patterns if p.severity == AntiPatternSeverity.CRITICAL
        )
        self.high_count = sum(
            1 for r in self.anti_pattern_reports
            for p in r.patterns if p.severity == AntiPatternSeverity.HIGH
        )
        self.medium_count = sum(
            1 for r in self.anti_pattern_reports
            for p in r.patterns if p.severity == AntiPatternSeverity.MEDIUM
        )


class RewritesResponse(BaseModel):
    """Response from the /rewrites endpoint."""

    space_id: str
    pending_rewrites: list[RewriteCandidate] = Field(default_factory=list)
    total_estimated_savings_usd: float = 0.0


class InjectionResponse(BaseModel):
    """Response from the /inject endpoint."""

    space_id: str
    result: SpaceInjectionResult | None = None


class RollbackResponse(BaseModel):
    """Response from the /rollback endpoint."""

    space_id: str
    success: bool = False
    message: str = ""
