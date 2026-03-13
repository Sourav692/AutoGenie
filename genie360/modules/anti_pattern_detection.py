"""
Genie360 — Module 2: Anti-Pattern Detection
=============================================
Uses sqlglot AST analysis to detect 8 categories of SQL anti-patterns
in Genie-generated queries. Produces structured AntiPatternReport per query.
"""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp

from genie360.models.schemas import (
    AntiPattern,
    AntiPatternReport,
    AntiPatternSeverity,
)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: SQL Parsing
# ═════════════════════════════════════════════════════════════════════════════

def parse_sql_to_ast(
    sql_text: str,
    dialect: str = "databricks",
) -> exp.Expression | None:
    """
    Uses sqlglot to parse SQL into an AST. Returns the parsed expression
    tree for downstream rule evaluation, or None on parse failure.
    """
    try:
        stmts = sqlglot.parse(sql_text, read=dialect)
        if stmts and stmts[0]:
            return stmts[0]
        return None
    except sqlglot.errors.ParseError:
        return None


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: Individual Detection Rules
# ═════════════════════════════════════════════════════════════════════════════

def detect_select_star(ast_node: exp.Expression) -> AntiPattern | None:
    """
    Traverses AST for SELECT * patterns. Flags the affected table references.
    Severity: MEDIUM.
    """
    for select in ast_node.find_all(exp.Select):
        for star in select.find_all(exp.Star):
            tables = [t.name for t in ast_node.find_all(exp.Table)]
            return AntiPattern(
                pattern_type="SELECT_STAR",
                severity=AntiPatternSeverity.MEDIUM,
                description=f"SELECT * detected on table(s): {', '.join(tables)}. "
                            "Fetches all columns including potentially unused ones.",
                suggested_fix="Replace SELECT * with explicit column list to reduce data transfer.",
                estimated_impact="10-40% reduction in bytes scanned",
            )
    return None


def detect_missing_limit(ast_node: exp.Expression) -> AntiPattern | None:
    """
    Checks if the top-level SELECT has no LIMIT clause.
    Severity: HIGH for queries that could produce large result sets.
    """
    top_select = ast_node.find(exp.Select)
    if top_select is None:
        return None

    has_limit = ast_node.find(exp.Limit) is not None
    has_agg = bool(list(ast_node.find_all(exp.AggFunc)))

    if not has_limit and not has_agg:
        return AntiPattern(
            pattern_type="MISSING_LIMIT",
            severity=AntiPatternSeverity.HIGH,
            description="No LIMIT clause on non-aggregate SELECT. "
                        "May return unbounded result set from large tables.",
            suggested_fix="Add LIMIT clause (e.g., LIMIT 10000) as a safety guard.",
            estimated_impact="Prevents runaway result set transfers",
        )
    return None


def detect_full_table_scan(
    ast_node: exp.Expression,
    table_stats: dict[str, dict[str, Any]] | None = None,
) -> AntiPattern | None:
    """
    Checks for table references with no WHERE predicates and no partition
    filter on partitioned Delta tables. Cross-references Unity Catalog stats.
    Severity: HIGH.
    """
    tables = [t.name for t in ast_node.find_all(exp.Table)]
    has_where = ast_node.find(exp.Where) is not None

    if not has_where and tables:
        partitioned_tables = []
        if table_stats:
            for t in tables:
                stats = table_stats.get(t, {})
                if stats.get("partition_columns"):
                    partitioned_tables.append(t)

        desc = "No WHERE clause detected — full table scan on: " + ", ".join(tables)
        if partitioned_tables:
            desc += f". Partitioned table(s) {partitioned_tables} lose partition pruning benefit."

        return AntiPattern(
            pattern_type="FULL_TABLE_SCAN",
            severity=AntiPatternSeverity.HIGH,
            description=desc,
            suggested_fix="Add WHERE clause with partition column filters to enable file pruning.",
            estimated_impact="Up to 90% reduction in files scanned with partition pruning",
        )
    return None


def detect_correlated_subquery(ast_node: exp.Expression) -> AntiPattern | None:
    """
    Identifies subqueries in WHERE/HAVING that reference outer query columns —
    the classic N+1 problem in SQL. Severity: CRITICAL.
    """
    for where_node in ast_node.find_all(exp.Where):
        for subquery in where_node.find_all(exp.Subquery):
            outer_columns = {c.name for c in ast_node.find_all(exp.Column)
                           if c.find_ancestor(exp.Subquery) is None}
            inner_columns = {c.name for c in subquery.find_all(exp.Column)}
            correlated = outer_columns & inner_columns
            if correlated:
                return AntiPattern(
                    pattern_type="CORRELATED_SUBQUERY",
                    severity=AntiPatternSeverity.CRITICAL,
                    description=f"Correlated subquery references outer columns: "
                                f"{', '.join(correlated)}. Causes row-by-row execution.",
                    suggested_fix="Rewrite correlated subquery as a JOIN or CTE for set-based execution.",
                    estimated_impact="10-100x performance improvement depending on table size",
                )

    for having_node in ast_node.find_all(exp.Having):
        for subquery in having_node.find_all(exp.Subquery):
            return AntiPattern(
                pattern_type="CORRELATED_SUBQUERY",
                severity=AntiPatternSeverity.CRITICAL,
                description="Subquery in HAVING clause detected. May cause correlated execution.",
                suggested_fix="Rewrite as CTE with JOIN to avoid per-group subquery execution.",
                estimated_impact="Significant improvement for high-cardinality GROUP BY",
            )
    return None


def detect_count_distinct_inefficiency(ast_node: exp.Expression) -> AntiPattern | None:
    """
    Flags COUNT(DISTINCT col) expressions on large tables where
    APPROX_COUNT_DISTINCT would suffice with ~2% error. Severity: MEDIUM.
    """
    sql_upper = ast_node.sql().upper()
    if "COUNT(DISTINCT" not in sql_upper and "COUNT (DISTINCT" not in sql_upper:
        return None

    for func in ast_node.find_all(exp.Count):
        if func.args.get("distinct"):
            return AntiPattern(
                pattern_type="COUNT_DISTINCT_INEFFICIENCY",
                severity=AntiPatternSeverity.MEDIUM,
                description="COUNT(DISTINCT) is expensive at scale. "
                            "For analytical/exploratory queries, consider approximate counting.",
                suggested_fix="Replace with APPROX_COUNT_DISTINCT(col) for ~2% error and 10-100x speedup.",
                estimated_impact="10-100x faster for high-cardinality columns",
            )
    return None


def detect_cross_join_risk(ast_node: exp.Expression) -> AntiPattern | None:
    """
    Detects implicit cross joins (comma-separated tables without JOIN conditions)
    or explicit CROSS JOINs on large tables. Severity: CRITICAL.
    """
    for join in ast_node.find_all(exp.Join):
        join_sql = join.sql().upper()
        if "CROSS JOIN" in join_sql:
            return AntiPattern(
                pattern_type="CROSS_JOIN",
                severity=AntiPatternSeverity.CRITICAL,
                description="CROSS JOIN detected — produces cartesian product of both tables.",
                suggested_fix="Replace CROSS JOIN with INNER/LEFT JOIN on a meaningful key, "
                              "or add WHERE clause to bound the result set.",
                estimated_impact="Prevents exponential row explosion",
            )

    from_clause = ast_node.find(exp.From)
    if from_clause:
        tables_in_from = list(from_clause.find_all(exp.Table))
        if len(tables_in_from) > 1:
            has_join = bool(list(ast_node.find_all(exp.Join)))
            if not has_join:
                table_names = [t.name for t in tables_in_from]
                return AntiPattern(
                    pattern_type="IMPLICIT_CROSS_JOIN",
                    severity=AntiPatternSeverity.CRITICAL,
                    description=f"Implicit cross join: tables {', '.join(table_names)} "
                                f"in FROM without JOIN condition.",
                    suggested_fix="Use explicit JOIN syntax with ON clause.",
                    estimated_impact="Prevents cartesian product explosion",
                )
    return None


def detect_implicit_type_cast(
    ast_node: exp.Expression,
    column_metadata: dict[str, dict[str, str]] | None = None,
) -> AntiPattern | None:
    """
    Identifies WHERE predicates that compare a string literal against an integer
    column or vice versa — kills partition pruning. Severity: HIGH.
    """
    if column_metadata is None:
        return None

    for where_node in ast_node.find_all(exp.Where):
        for eq in where_node.find_all(exp.EQ):
            left = eq.left
            right = eq.right
            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                col_name = left.name
                col_type = column_metadata.get(col_name, {}).get("data_type", "").upper()
                if col_type in ("INT", "INTEGER", "BIGINT", "LONG", "DOUBLE", "FLOAT"):
                    if right.is_string:
                        return AntiPattern(
                            pattern_type="IMPLICIT_TYPE_CAST",
                            severity=AntiPatternSeverity.HIGH,
                            description=f"String literal compared to numeric column '{col_name}' "
                                        f"(type: {col_type}). Implicit cast prevents index/partition usage.",
                            suggested_fix=f"Use numeric literal or explicit CAST: "
                                          f"CAST('{right.this}' AS {col_type})",
                            estimated_impact="Restores partition pruning and predicate pushdown",
                        )
    return None


def detect_repeated_subquery(ast_node: exp.Expression) -> AntiPattern | None:
    """
    Detects the same subquery expression appearing more than once —
    a candidate for CTE extraction. Severity: MEDIUM.
    """
    subqueries = list(ast_node.find_all(exp.Subquery))
    if len(subqueries) < 2:
        return None

    sql_hashes: dict[str, int] = {}
    for sq in subqueries:
        normalized = sq.sql().strip().upper()
        sql_hashes[normalized] = sql_hashes.get(normalized, 0) + 1

    repeated = {k: v for k, v in sql_hashes.items() if v >= 2}
    if repeated:
        return AntiPattern(
            pattern_type="REPEATED_SUBQUERY",
            severity=AntiPatternSeverity.MEDIUM,
            description=f"Same subquery appears {max(repeated.values())} times. "
                        "Redundant computation that should be extracted to a CTE.",
            suggested_fix="Extract repeated subquery into a WITH (CTE) clause for single evaluation.",
            estimated_impact="Eliminates redundant computation proportional to subquery cost",
        )
    return None


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: Orchestration
# ═════════════════════════════════════════════════════════════════════════════

def run_anti_pattern_suite(
    sql_text: str,
    table_stats: dict[str, dict[str, Any]] | None = None,
    column_metadata: dict[str, dict[str, str]] | None = None,
) -> AntiPatternReport:
    """
    Orchestrates all individual detection functions. Returns a structured
    AntiPatternReport with severity rankings, estimated impact, and fix suggestions.
    """
    report = AntiPatternReport(original_sql=sql_text)

    ast_node = parse_sql_to_ast(sql_text)
    if ast_node is None:
        report.parse_success = False
        report.parse_error = "Failed to parse SQL — skipping anti-pattern detection."
        return report

    detectors = [
        lambda: detect_select_star(ast_node),
        lambda: detect_missing_limit(ast_node),
        lambda: detect_full_table_scan(ast_node, table_stats),
        lambda: detect_correlated_subquery(ast_node),
        lambda: detect_count_distinct_inefficiency(ast_node),
        lambda: detect_cross_join_risk(ast_node),
        lambda: detect_implicit_type_cast(ast_node, column_metadata),
        lambda: detect_repeated_subquery(ast_node),
    ]

    for detector in detectors:
        try:
            pattern = detector()
            if pattern is not None:
                report.add_pattern(pattern)
        except Exception:
            pass

    return report
