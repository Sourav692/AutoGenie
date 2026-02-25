"""
Genie360 — Module 6: Optimization Report Generator
====================================================
Generates a structured Markdown report showing every analyzed query
with before/after SQL, detected anti-patterns, applied rules,
confidence scores, and estimated impact.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from genie360.models.schemas import (
    AntiPatternReport,
    AntiPatternSeverity,
    RewriteCandidate,
)


SEVERITY_EMOJI = {
    AntiPatternSeverity.CRITICAL: "🔴",
    AntiPatternSeverity.HIGH: "🟠",
    AntiPatternSeverity.MEDIUM: "🟡",
    AntiPatternSeverity.LOW: "🔵",
    AntiPatternSeverity.INFO: "⚪",
}

CONFIDENCE_LABEL = {
    (0.85, 1.01): ("High", "✅"),
    (0.60, 0.85): ("Medium", "⚠️"),
    (0.00, 0.60): ("Low", "❌"),
}


def _confidence_badge(score: float) -> str:
    for (lo, hi), (label, icon) in CONFIDENCE_LABEL.items():
        if lo <= score < hi:
            return f"{icon} **{label}** ({score:.1%})"
    return f"({score:.1%})"


def _format_sql_block(sql: str) -> str:
    return f"```sql\n{sql.strip()}\n```"


def _build_severity_summary(reports: list[AntiPatternReport]) -> dict[str, int]:
    counts: dict[str, int] = {s.value: 0 for s in AntiPatternSeverity}
    for report in reports:
        for p in report.patterns:
            counts[p.severity.value] += 1
    return counts


def _build_pattern_frequency(reports: list[AntiPatternReport]) -> dict[str, int]:
    freq: dict[str, int] = {}
    for report in reports:
        for p in report.patterns:
            freq[p.pattern_type] = freq.get(p.pattern_type, 0) + 1
    return dict(sorted(freq.items(), key=lambda kv: kv[1], reverse=True))


def generate_optimization_report(
    space_id: str,
    reports: list[AntiPatternReport],
    candidates: list[RewriteCandidate],
    config: dict[str, Any] | None = None,
    output_dir: str | Path | None = None,
    report_mode: str = "downstream",
    question_map: dict[str, str] | None = None,
) -> str:
    """
    Builds a Markdown report with before/after SQL for every query that
    had anti-patterns detected.  Returns the absolute path of the written
    file.

    Args:
        report_mode: "downstream" (query history) or "upstream" (example queries).
        question_map: Maps query_id -> business question text (upstream mode).

    The report contains:
      - Executive summary (queries analyzed, issues, severity breakdown)
      - Pattern-frequency table
      - Per-query detail cards with original vs optimised SQL
      - Confidence scoring & estimated impact
    """
    config = config or {}
    question_map = question_map or {}
    is_upstream = report_mode == "upstream"
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    if output_dir is None:
        output_dir = config.get("output_path", os.path.join(os.getcwd(), "genie360_reports"))
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    mode_tag = "upstream" if is_upstream else "downstream"
    filename = f"genie360_{mode_tag}_report_{space_id}_{datetime.utcnow():%Y%m%d_%H%M%S}.md"
    report_path = output_dir / filename

    candidate_by_query = {c.query_id: c for c in candidates if c.query_id}

    total_queries = len(reports)
    queries_with_issues = sum(1 for r in reports if r.total_issues > 0)
    total_issues = sum(r.total_issues for r in reports)
    rewrites_generated = sum(1 for c in candidates if c.final_sql)
    min_conf = config.get("min_confidence_for_injection", 0.85)
    above_threshold = sum(1 for c in candidates if c.confidence_score >= min_conf)

    severity_counts = _build_severity_summary(reports)
    pattern_freq = _build_pattern_frequency(reports)

    lines: list[str] = []

    # ── Title ────────────────────────────────────────────────────────────
    if is_upstream:
        lines.append("# Genie360 — Upstream SQL Optimization Report")
        lines.append("")
        lines.append("> **Strategy:** Optimize example queries in the Knowledge Store")
        lines.append("> *before* Genie uses them to generate SQL.  ")
    else:
        lines.append("# Genie360 — SQL Optimization Report")
        lines.append("")

    lines.append(f"> **Genie Space:** `{space_id}`  ")
    lines.append(f"> **Generated:** {now}  ")
    lines.append(f"> **Pipeline mode:** {mode_tag}  ")
    lines.append(f"> **Confidence threshold:** {min_conf:.0%}")
    lines.append("")

    # ── Executive Summary ────────────────────────────────────────────────
    lines.append("---")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    query_noun = "Example queries in Knowledge Store" if is_upstream else "Queries analysed"
    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| {query_noun} | {total_queries} |")
    lines.append(f"| Queries with issues | {queries_with_issues} |")
    lines.append(f"| Total anti-patterns found | {total_issues} |")
    lines.append(f"| Rewrites generated | {rewrites_generated} |")
    lines.append(f"| Above confidence threshold | {above_threshold} |")
    lines.append("")

    # ── Severity Breakdown ───────────────────────────────────────────────
    lines.append("### Severity Breakdown")
    lines.append("")
    lines.append("| Severity | Count |")
    lines.append("|----------|------:|")
    for sev in AntiPatternSeverity:
        count = severity_counts.get(sev.value, 0)
        if count > 0:
            emoji = SEVERITY_EMOJI.get(sev, "")
            lines.append(f"| {emoji} {sev.value} | {count} |")
    lines.append("")

    # ── Pattern Frequency ────────────────────────────────────────────────
    if pattern_freq:
        lines.append("### Anti-Pattern Frequency")
        lines.append("")
        lines.append("| Pattern | Occurrences |")
        lines.append("|---------|------------:|")
        for pat, cnt in pattern_freq.items():
            lines.append(f"| `{pat}` | {cnt} |")
        lines.append("")

    # ── Per-Query Detail Cards ───────────────────────────────────────────
    lines.append("---")
    lines.append("")
    section_title = "Example Query Optimization Details" if is_upstream else "Query Optimization Details"
    lines.append(f"## {section_title}")
    lines.append("")

    actionable = [r for r in reports if r.total_issues > 0]

    if not actionable:
        lines.append("*No anti-patterns detected — all queries look healthy.*")
        lines.append("")
    else:
        for idx, report in enumerate(actionable, start=1):
            candidate = candidate_by_query.get(report.query_id)

            query_label = report.query_id or f"query-{idx}"
            lines.append(f"### {idx}. Query `{query_label}`")
            lines.append("")

            question = question_map.get(report.query_id)
            if question:
                lines.append(f"> **Business question:** *\"{question}\"*")
                lines.append("")

            # Detected anti-patterns
            lines.append("**Detected Anti-Patterns:**")
            lines.append("")
            for p in report.patterns:
                emoji = SEVERITY_EMOJI.get(p.severity, "")
                lines.append(f"- {emoji} **{p.pattern_type}** ({p.severity.value}): {p.description}")
                if p.suggested_fix:
                    lines.append(f"  - *Fix:* {p.suggested_fix}")
                if p.estimated_impact:
                    lines.append(f"  - *Impact:* {p.estimated_impact}")
            lines.append("")

            # Before / After
            lines.append("#### Before (Original SQL)")
            lines.append("")
            lines.append(_format_sql_block(report.original_sql))
            lines.append("")

            if candidate and candidate.final_sql:
                lines.append("#### After (Optimized SQL)")
                lines.append("")
                lines.append(_format_sql_block(candidate.final_sql))
                lines.append("")

                # Rules & confidence
                if candidate.rules_applied:
                    lines.append(f"**Rules applied:** {', '.join(f'`{r}`' for r in candidate.rules_applied)}")
                    lines.append("")

                lines.append(f"**Confidence:** {_confidence_badge(candidate.confidence_score)}")
                lines.append("")

                if candidate.estimated_duration_improvement_pct is not None:
                    lines.append(
                        f"**Estimated improvement:** {candidate.estimated_duration_improvement_pct:.1f}% "
                        f"fewer anti-patterns"
                    )
                    lines.append("")

                if candidate.llm_rewritten_sql and candidate.llm_rewritten_sql != candidate.rule_rewritten_sql:
                    lines.append("<details>")
                    lines.append("<summary>Rule-based rewrite (before LLM refinement)</summary>")
                    lines.append("")
                    lines.append(_format_sql_block(candidate.rule_rewritten_sql or report.original_sql))
                    lines.append("")
                    lines.append("</details>")
                    lines.append("")

                status_icon = "✅" if candidate.confidence_score >= min_conf else "⚠️"
                lines.append(f"**Status:** {status_icon} {'Ready for injection' if candidate.confidence_score >= min_conf else 'Below confidence threshold — manual review recommended'}")
                lines.append("")

            else:
                lines.append("#### After (Optimized SQL)")
                lines.append("")
                lines.append("*No rewrite generated for this query.*")
                lines.append("")

            lines.append("---")
            lines.append("")

    # ── Footer ───────────────────────────────────────────────────────────
    lines.append("## Legend")
    lines.append("")
    lines.append("| Symbol | Meaning |")
    lines.append("|--------|---------|")
    lines.append("| 🔴 | Critical severity |")
    lines.append("| 🟠 | High severity |")
    lines.append("| 🟡 | Medium severity |")
    lines.append("| 🔵 | Low severity |")
    lines.append("| ✅ | Above confidence threshold |")
    lines.append("| ⚠️ | Below confidence threshold |")
    lines.append("")
    lines.append("---")
    if is_upstream:
        lines.append(
            f"*Upstream optimization report generated by Genie360 v1.0 on {now} — "
            f"\"Control what gets generated in the first place.\"*"
        )
    else:
        lines.append(f"*Report generated by Genie360 v1.0 on {now}*")
    lines.append("")

    report_content = "\n".join(lines)
    report_path.write_text(report_content, encoding="utf-8")

    return str(report_path)
