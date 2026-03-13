"""
Auto-Genie Utilities
====================
Shared functions and constants used across all Auto-Genie pipeline notebooks.

Modules:
    - Configuration & Environment
    - Metadata Discovery & Profiling
    - Relationship Discovery
    - Business Domain Intelligence (constants + detection)
    - Instruction Generation
"""

from __future__ import annotations

import os
import re
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
from typing import Any

import yaml
import pandas as pd
from dotenv import load_dotenv


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: Configuration & Environment
# ═══════════════════════════════════════════════════════════════════════════════

def _find_file(filename: str) -> Path:
    """Search cwd, parent, and grandparent for *filename*."""
    for directory in [Path(os.getcwd()), Path(os.getcwd()).parent, Path(os.getcwd()).parent.parent]:
        candidate = directory / filename
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"{filename} not found in cwd or ancestor directories")


def load_yaml_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load application config from *config.yml* and credentials from *.env*.

    Non-secret tunables (catalog, schema, tables, etc.) live in config.yml.
    Credentials (DATABRICKS_HOST / DATABRICKS_TOKEN) are loaded from .env.
    """
    if config_path is None:
        config_path = _find_file("config.yml")
    config_path = Path(config_path)

    with open(config_path) as fh:
        cfg = yaml.safe_load(fh)
    print(f"✅ Loaded config from {config_path}")

    # Load .env for credentials (search from config.yml's directory)
    for candidate in [config_path.parent / ".env", config_path.parent.parent / ".env"]:
        if candidate.exists():
            load_dotenv(candidate, override=True)
            print(f"✅ Loaded credentials from {candidate}")
            break

    return {
        "workspace_url": os.getenv("DATABRICKS_HOST", ""),
        "databricks_token": os.getenv("DATABRICKS_TOKEN", ""),
        "cluster_id": cfg.get("cluster_id", ""),
        "catalog": cfg.get("catalog", "main"),
        "schema": cfg.get("schema", "sales"),
        "tables": cfg.get("tables", []),
        "genie_warehouse_id": cfg.get("warehouse_id", ""),
        "lookback_days": int(cfg.get("lookback_days", 90)),
        "confidence_threshold": float(cfg.get("confidence_threshold", 0.75)),
        "output_path": cfg.get("output_path", "/dbfs/tmp/auto_genie_outputs"),
    }


def load_prompts(prompts_path: str | Path | None = None) -> dict[str, Any]:
    """Load LLM prompts from *prompts.yml*."""
    if prompts_path is None:
        prompts_path = _find_file("prompts.yml")

    with open(Path(prompts_path)) as fh:
        prompts = yaml.safe_load(fh)
    print(f"✅ Loaded prompts from {prompts_path}")
    return prompts


def load_env_config() -> dict[str, Any]:
    """Load .env file and return the standard Auto-Genie config dict.

    Searches the current directory and its parent for a .env file.
    Falls back to existing environment variables if none is found.
    """
    env_path = Path(os.getcwd())
    for candidate in [env_path / ".env", env_path.parent / ".env"]:
        if candidate.exists():
            load_dotenv(candidate, override=True)
            print(f"✅ Loaded environment from {candidate}")
            break
    else:
        print("⚠️  No .env file found — falling back to existing environment variables")

    return {
        "workspace_url": os.getenv("DATABRICKS_HOST", ""),
        "databricks_token": os.getenv("DATABRICKS_TOKEN", ""),
        "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", ""),
        "catalog": os.getenv("AUTO_GENIE_CATALOG", "main"),
        "schema": os.getenv("AUTO_GENIE_SCHEMA", "sales"),
        "tables": os.getenv(
            "AUTO_GENIE_TABLES",
            "orders,customers,products,order_items,warehouses",
        ).split(","),
        "genie_warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
        "lookback_days": int(os.getenv("AUTO_GENIE_LOOKBACK_DAYS", "90")),
        "confidence_threshold": float(
            os.getenv("AUTO_GENIE_CONFIDENCE_THRESHOLD", "0.75")
        ),
        "output_path": os.getenv(
            "AUTO_GENIE_OUTPUT_PATH", "/dbfs/tmp/auto_genie_outputs"
        ),
    }


def print_config(config: dict[str, Any]) -> None:
    """Pretty-print the configuration, masking sensitive fields."""
    print("=" * 60)
    print("AUTO-GENIE CONFIGURATION")
    print("=" * 60)
    for key, value in config.items():
        display_val = "***" if "token" in key else value
        print(f"{key:25s}: {display_val}")
    print("=" * 60)


def get_spark_session(config: dict[str, Any] | None = None):
    """Return a Spark session — reuses the cluster-provided one or creates
    a local DatabricksSession via Databricks Connect.
    """
    try:
        return spark  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        from databricks.connect import DatabricksSession

        cfg = config or {}
        return (
            DatabricksSession.builder.remote(
                host=cfg.get("workspace_url") or os.getenv("DATABRICKS_HOST"),
                token=cfg.get("databricks_token") or os.getenv("DATABRICKS_TOKEN"),
                cluster_id=cfg.get("cluster_id") or os.getenv("DATABRICKS_CLUSTER_ID"),
            )
            .getOrCreate()
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: Metadata Discovery & Profiling
# ═══════════════════════════════════════════════════════════════════════════════

def extract_table_metadata(
    spark_session,
    catalog: str,
    schema: str,
    table_names: list[str],
) -> dict[str, dict]:
    """Extract comprehensive metadata for the specified tables from Unity Catalog.

    Returns a dict keyed by fully-qualified table name (catalog.schema.table).
    """
    metadata: dict[str, dict] = {}

    for table_name in table_names:
        fqn = f"{catalog}.{schema}.{table_name}"

        info = spark_session.sql(
            f"SELECT table_catalog, table_schema, table_name, table_type, comment "
            f"FROM system.information_schema.tables "
            f"WHERE table_catalog = '{catalog}' "
            f"  AND table_schema = '{schema}' "
            f"  AND table_name = '{table_name}'"
        ).collect()

        if not info:
            print(f"⚠️ Table {fqn} not found — skipping")
            continue

        columns = spark_session.sql(
            f"SELECT column_name, data_type, is_nullable, column_default, comment "
            f"FROM system.information_schema.columns "
            f"WHERE table_catalog = '{catalog}' "
            f"  AND table_schema = '{schema}' "
            f"  AND table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        ).collect()

        detail = spark_session.sql(
            f"DESCRIBE DETAIL {fqn}"
        ).collect()[0].asDict()

        row_count = spark_session.sql(
            f"SELECT COUNT(*) as cnt FROM {fqn}"
        ).collect()[0]["cnt"]

        constraints_rows = spark_session.sql(
            f"SELECT constraint_name, constraint_type "
            f"FROM system.information_schema.table_constraints "
            f"WHERE table_catalog = '{catalog}' "
            f"  AND table_schema = '{schema}' "
            f"  AND table_name = '{table_name}'"
        ).collect()

        metadata[fqn] = {
            "table_info": info[0].asDict(),
            "columns": [col.asDict() for col in columns],
            "row_count": row_count,
            "size_bytes": detail.get("sizeInBytes", 0),
            "last_modified": detail.get("lastModified", ""),
            "constraints": [c.asDict() for c in constraints_rows],
        }
        print(f"✅ {fqn} — {row_count:,} rows, {len(columns)} columns")

    return metadata


def profile_column_statistics(
    spark_session,
    table_metadata: dict[str, dict],
) -> dict[str, list[dict]]:
    """Profile column-level statistics: cardinality, null %, PII detection.

    Returns a dict keyed by fully-qualified table name, each value being a
    list of per-column profile dicts.
    """
    profiles: dict[str, list[dict]] = {}

    for fqn, meta in table_metadata.items():
        tbl_profiles: list[dict] = []
        for col in meta["columns"]:
            col_name = col["column_name"]
            col_type = col["data_type"]

            try:
                result = spark_session.sql(
                    f"SELECT COUNT(DISTINCT `{col_name}`) AS dc, "
                    f"COUNT(*) AS tc, "
                    f"SUM(CASE WHEN `{col_name}` IS NULL THEN 1 ELSE 0 END) AS nc "
                    f"FROM {fqn}"
                ).collect()[0]

                cardinality = result["dc"]
                null_pct = (
                    round(result["nc"] / result["tc"] * 100, 2) if result["tc"] else 0
                )

                if cardinality <= 10:
                    cardinality_cat = "very_low"
                elif cardinality <= 100:
                    cardinality_cat = "low"
                elif cardinality <= 1000:
                    cardinality_cat = "medium"
                else:
                    cardinality_cat = "high"

                pii_type = None
                if re.search(r"email", col_name, re.IGNORECASE):
                    pii_type = "email"
                elif re.search(r"phone|mobile", col_name, re.IGNORECASE):
                    pii_type = "phone"
                elif re.search(r"ssn|social", col_name, re.IGNORECASE):
                    pii_type = "ssn"

                tbl_profiles.append(
                    {
                        "column": col_name,
                        "data_type": col_type,
                        "distinct_count": cardinality,
                        "null_percentage": null_pct,
                        "cardinality_category": cardinality_cat,
                        "recommend_entity_matching": cardinality_cat != "high",
                        "pii_detected": pii_type,
                        "recommend_masking": pii_type is not None,
                    }
                )
            except Exception:
                pass

        profiles[fqn] = tbl_profiles

    return profiles


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: Relationship Discovery
# ═══════════════════════════════════════════════════════════════════════════════

def discover_declared_relationships(
    spark_session,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Extract declared foreign key relationships from Unity Catalog."""
    relationships: list[dict] = []

    fk_query = f"""
    SELECT
        tc.table_name        AS source_table,
        kcu.column_name      AS source_column,
        rc.referenced_table_name AS target_table,
        rc.referenced_column_name AS target_column
    FROM system.information_schema.table_constraints tc
    JOIN system.information_schema.key_column_usage kcu
        ON  tc.constraint_name = kcu.constraint_name
        AND tc.table_catalog   = kcu.table_catalog
        AND tc.table_schema    = kcu.table_schema
        AND tc.table_name      = kcu.table_name
    JOIN (
        SELECT constraint_name,
               table_name  AS referenced_table_name,
               column_name AS referenced_column_name
        FROM system.information_schema.key_column_usage
        WHERE table_catalog = '{catalog}' AND table_schema = '{schema}'
    ) rc ON tc.constraint_name = rc.constraint_name
    WHERE tc.table_catalog = '{catalog}'
      AND tc.table_schema  = '{schema}'
      AND tc.constraint_type = 'FOREIGN KEY'
    """

    try:
        for row in spark_session.sql(fk_query).collect():
            relationships.append(
                {
                    "source_table": f"{catalog}.{schema}.{row['source_table']}",
                    "source_column": row["source_column"],
                    "target_table": f"{catalog}.{schema}.{row['target_table']}",
                    "target_column": row["target_column"],
                    "confidence": 1.0,
                    "method": "declared_fk",
                }
            )
    except Exception as exc:
        print(f"⚠️ No declared foreign keys found or error: {exc}")

    return relationships


def discover_naming_pattern_relationships(
    table_metadata: dict[str, dict],
) -> list[dict]:
    """Discover relationships based on column naming patterns (_id, _key, fk_*)."""
    relationships: list[dict] = []

    all_columns: dict[str, list[tuple[str, str]]] = {}
    for table_name, meta in table_metadata.items():
        all_columns[table_name] = [
            (col["column_name"], col["data_type"]) for col in meta["columns"]
        ]

    id_patterns = [r"(.+)_id$", r"(.+)_key$", r"^fk_(.+)$"]

    for source_table, source_cols in all_columns.items():
        for source_col, source_type in source_cols:
            for pattern in id_patterns:
                match = re.search(pattern, source_col, re.IGNORECASE)
                if not match:
                    continue
                entity_name = match.group(1)

                for target_table, target_cols in all_columns.items():
                    if source_table == target_table:
                        continue
                    target_simple = target_table.split(".")[-1]
                    if entity_name not in target_simple and target_simple not in entity_name:
                        continue

                    for target_col, target_type in target_cols:
                        if (
                            target_col.endswith("_id")
                            or target_col.endswith("_key")
                            or target_col == "id"
                        ) and source_type == target_type:
                            confidence = 0.85 if source_col == target_col else 0.70
                            relationships.append(
                                {
                                    "source_table": source_table,
                                    "source_column": source_col,
                                    "target_table": target_table,
                                    "target_column": target_col,
                                    "confidence": confidence,
                                    "method": "naming_pattern",
                                    "evidence": f"Pattern '{pattern}', types match ({source_type})",
                                }
                            )

    return relationships


def discover_query_pattern_relationships(
    spark_session,
    catalog: str,
    schema: str,
    lookback_days: int = 90,
) -> list[dict]:
    """Mine query history for JOIN patterns to discover implicit relationships."""
    relationships: list[dict] = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    history_query = f"""
    SELECT statement_text, executed_by, start_time, total_duration_ms
    FROM system.query.history
    WHERE start_time >= '{start_date.strftime("%Y-%m-%d")}'
      AND statement_type = 'SELECT'
      AND error_message IS NULL
      AND LOWER(statement_text) LIKE '%{schema}%'
      AND LOWER(statement_text) LIKE '%join%'
    ORDER BY start_time DESC
    LIMIT 5000
    """

    try:
        queries = spark_session.sql(history_query).collect()
        print(f"ℹ️ Analyzing {len(queries)} queries with JOINs...")

        join_patterns: Counter = Counter()
        join_re = re.compile(
            r"(\w+)\s+(?:inner\s+|left\s+|right\s+)?join\s+(\w+)"
            r"\s+(?:as\s+\w+\s+)?on\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)",
            re.IGNORECASE,
        )

        for query in queries:
            for m in join_re.findall(query["statement_text"].lower()):
                if len(m) == 6:
                    tables = tuple(sorted([m[0], m[1]]))
                    columns = (m[3], m[5])
                    join_patterns[(tables, columns)] += 1

        for (tables, columns), count in join_patterns.items():
            if count >= 3:
                confidence = min(0.95, 0.60 + (count / 100))
                relationships.append(
                    {
                        "source_table": f"{catalog}.{schema}.{tables[0]}",
                        "source_column": columns[0],
                        "target_table": f"{catalog}.{schema}.{tables[1]}",
                        "target_column": columns[1],
                        "confidence": round(confidence, 2),
                        "method": "query_pattern",
                        "query_count": count,
                        "evidence": f"Found in {count} queries",
                    }
                )

    except Exception as exc:
        print(f"⚠️ Error analyzing query history: {exc}")

    return relationships


def merge_and_rank_relationships(
    declared: list[dict],
    naming: list[dict],
    query_patterns: list[dict],
    threshold: float = 0.75,
) -> list[dict]:
    """Merge all discovered relationships, deduplicate, and rank by confidence."""
    all_rels: dict[str, dict] = {}

    for rel in declared:
        key = (
            f"{rel['source_table']}.{rel['source_column']}"
            f"->{rel['target_table']}.{rel['target_column']}"
        )
        all_rels[key] = {**rel, "methods": ["declared_fk"], "evidence_count": 1}

    for rel in naming:
        key = (
            f"{rel['source_table']}.{rel['source_column']}"
            f"->{rel['target_table']}.{rel['target_column']}"
        )
        if key in all_rels:
            all_rels[key]["methods"].append("naming_pattern")
            all_rels[key]["evidence_count"] += 1
        else:
            all_rels[key] = {**rel, "methods": ["naming_pattern"], "evidence_count": 1}

    for rel in query_patterns:
        key = (
            f"{rel['source_table']}.{rel['source_column']}"
            f"->{rel['target_table']}.{rel['target_column']}"
        )
        if key in all_rels:
            all_rels[key]["methods"].append("query_pattern")
            all_rels[key]["evidence_count"] += 1
            all_rels[key]["confidence"] = min(
                1.0, all_rels[key]["confidence"] + 0.05
            )
            all_rels[key]["query_count"] = rel.get("query_count", 0)
        else:
            all_rels[key] = {**rel, "methods": ["query_pattern"], "evidence_count": 1}

    ranked = sorted(
        all_rels.values(),
        key=lambda x: (x["evidence_count"], x["confidence"]),
        reverse=True,
    )

    filtered = [r for r in ranked if r["confidence"] >= threshold]
    for i, rel in enumerate(filtered, 1):
        rel["rank"] = i

    return filtered


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: Business Domain Intelligence — Constants
# ═══════════════════════════════════════════════════════════════════════════════

DOMAIN_SIGNALS: dict[str, dict] = {
    "sales_pipeline": {
        "table_keywords": ["opportunity", "pipeline", "deal", "lead", "forecast", "quote"],
        "column_keywords": [
            "stagename", "forecastcategory", "probability", "closedate",
            "pipeline", "deal", "leadsource", "win",
        ],
        "label": "Sales Pipeline & CRM",
        "description": "sales pipeline management, opportunity tracking, and revenue forecasting",
    },
    "ecommerce": {
        "table_keywords": ["order", "cart", "product", "catalog", "sku", "shipment", "invoice"],
        "column_keywords": [
            "order_id", "sku", "quantity", "shipping", "cart", "checkout", "unit_price",
        ],
        "label": "E-Commerce & Retail",
        "description": "order management, product catalog, and customer transactions",
    },
    "finance": {
        "table_keywords": ["transaction", "ledger", "journal", "balance", "payment", "invoice", "billing"],
        "column_keywords": ["debit", "credit", "balance", "gl_account", "fiscal", "ledger"],
        "label": "Finance & Accounting",
        "description": "financial transactions, accounting records, and payment processing",
    },
    "hr": {
        "table_keywords": ["employee", "department", "payroll", "benefit", "leave", "attendance"],
        "column_keywords": [
            "employee_id", "hire_date", "salary", "department", "manager_id", "position",
        ],
        "label": "Human Resources",
        "description": "employee management, payroll, and organizational structure",
    },
    "marketing": {
        "table_keywords": ["campaign", "impression", "click", "conversion", "audience", "channel"],
        "column_keywords": [
            "campaign_id", "ctr", "impressions", "clicks", "conversions", "ad_spend",
        ],
        "label": "Marketing Analytics",
        "description": "campaign performance, audience engagement, and marketing ROI",
    },
    "support": {
        "table_keywords": ["ticket", "incident", "case", "escalation", "sla", "resolution"],
        "column_keywords": [
            "ticket_id", "priority", "severity", "resolution_time", "sla", "assigned_to",
        ],
        "label": "Customer Support",
        "description": "support ticket management, SLA tracking, and resolution analytics",
    },
}

BUSINESS_COLUMN_LABELS: dict[str, str] = {
    "stagename": "Pipeline Stage",
    "stage_name": "Pipeline Stage",
    "forecastcategory": "Forecast Category",
    "forecast_category": "Forecast Category",
    "probability": "Win Probability (%)",
    "closedate": "Expected Close Date",
    "close_date": "Expected Close Date",
    "createddate": "Deal Created Date",
    "created_date": "Record Created Date",
    "amount": "Deal Value ($)",
    "annualrevenue": "Annual Revenue ($)",
    "annual_revenue": "Annual Revenue ($)",
    "industry": "Industry Vertical",
    "region": "Sales Region",
    "region__c": "Sales Region",
    "region_hq__c": "HQ Region",
    "company_size_segment__c": "Company Size Segment",
    "business_type__c": "Business Type (New/Expansion)",
    "leadsource": "Lead Source",
    "lead_source": "Lead Source",
    "type": "Record Type",
    "name": "Name",
    "account_name": "Account Name",
    "ownerid": "Deal Owner (Sales Rep)",
    "owner_id": "Deal Owner (Sales Rep)",
    "accountid": "Parent Account",
    "account_id": "Parent Account",
    "managerid": "Reporting Manager",
    "manager_id": "Reporting Manager",
    "opportunityid": "Opportunity ID",
    "opportunity_id": "Opportunity ID",
    "role__c": "Job Role",
    "segment__c": "Market Segment",
    "days_to_close": "Sales Cycle Length (Days)",
    "opportunity_amount": "Opportunity Amount ($)",
    "sumdealamount": "Aggregated Deal Amount ($)",
    "dealstage": "Deal Stage (Historical)",
    "status": "Status",
    "description": "Description",
    "revenue": "Revenue ($)",
    "total": "Total ($)",
    "price": "Price ($)",
    "cost": "Cost ($)",
    "quantity": "Quantity",
    "discount": "Discount",
    "order_date": "Order Date",
    "ship_date": "Ship Date",
    "customer_id": "Customer",
    "product_id": "Product",
    "category": "Category",
    "email": "Email Address",
    "phone": "Phone Number",
}

TABLE_PURPOSE_PATTERNS: dict[str, str] = {
    "account": (
        "Master list of customer accounts with firmographic and demographic data. "
        "Use for account segmentation, regional distribution, and industry analysis."
    ),
    "customer": (
        "Customer master data with contact details and segmentation attributes. "
        "Use for customer analysis and segmentation."
    ),
    "opportunity": (
        "Individual sales deals tracked through pipeline stages from creation to close. "
        "Core table for pipeline analysis, forecasting, and rep performance."
    ),
    "opportunityhistory": (
        "Historical snapshots of pipeline data capturing how deals and stages evolve "
        "over time. Use for trend analysis and pipeline movement tracking."
    ),
    "user": (
        "Sales team members including reps and managers with role and segment "
        "assignments. Use for team structure analysis and rep-level performance rollups."
    ),
    "order": (
        "Transaction records capturing purchases with dates, amounts, and associated "
        "entities. Core table for revenue and order analysis."
    ),
    "product": (
        "Product catalog with names, categories, pricing, and attributes. "
        "Use for product mix analysis and category performance."
    ),
    "invoice": (
        "Billing records with line items, amounts, and payment status. "
        "Use for AR analysis and revenue recognition."
    ),
    "employee": (
        "Employee records with organizational and compensation data. "
        "Use for headcount, turnover, and workforce analytics."
    ),
    "campaign": (
        "Marketing campaign records with performance metrics. "
        "Use for campaign ROI and channel effectiveness analysis."
    ),
    "ticket": (
        "Support tickets with priority, assignment, and resolution data. "
        "Use for SLA compliance and support workload analysis."
    ),
}


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: Business Domain Intelligence — Functions
# ═══════════════════════════════════════════════════════════════════════════════

def detect_domain(
    table_metadata: dict[str, dict],
    column_profiles: dict[str, list[dict]],
) -> dict[str, str]:
    """Infer the business domain from table and column names."""
    all_tables = " ".join(t.split(".")[-1].lower() for t in table_metadata)
    all_columns = " ".join(
        c.get("column", c.get("column_name", "")).lower()
        for profiles in column_profiles.values()
        for c in profiles
    )
    combined = all_tables + " " + all_columns

    scores: dict[str, int] = {}
    for domain_key, signals in DOMAIN_SIGNALS.items():
        score = sum(2 for kw in signals["table_keywords"] if kw in combined)
        score += sum(1 for kw in signals["column_keywords"] if kw in combined)
        scores[domain_key] = score

    best = max(scores, key=scores.get) if scores else "general"
    if scores.get(best, 0) < 2:
        return {
            "key": "general",
            "label": "Business Analytics",
            "description": "general business data analysis and reporting",
        }
    return {
        "key": best,
        "label": DOMAIN_SIGNALS[best]["label"],
        "description": DOMAIN_SIGNALS[best]["description"],
    }


def get_table_purpose(simple_name: str) -> str:
    """Match a table name to a business purpose description."""
    lower = simple_name.lower()
    for pattern, purpose in TABLE_PURPOSE_PATTERNS.items():
        if pattern in lower:
            return purpose
    return f"Contains {simple_name} data for analysis and reporting."


def get_column_business_label(col_name: str) -> str | None:
    """Get a human-friendly business label for a column, or None."""
    return BUSINESS_COLUMN_LABELS.get(col_name.lower())


def classify_column_role(
    col_name: str,
    col_type: str,
    profile: dict | None,
) -> str:
    """Classify a column into a business role based on name and statistics."""
    lower = col_name.lower()
    ctype = col_type.upper()

    if lower in ("id",) or lower.endswith("_id") or lower == "opportunityid":
        return "identifier"
    if any(
        kw in lower
        for kw in ["amount", "revenue", "price", "cost", "total", "value", "booking", "sumdeal"]
    ):
        return "monetary"
    if any(
        kw in lower
        for kw in ["probability", "rate", "percentage", "pct", "ratio", "days_to_close", "days", "duration", "count"]
    ):
        return "metric"
    if ctype in ("DATE", "TIMESTAMP") or "date" in lower:
        return "temporal"
    if (
        profile
        and profile.get("cardinality_category") in ("very_low", "low")
        and "STRING" in ctype
    ):
        return "dimension"
    if lower in ("name", "title", "description", "label", "account_name"):
        return "descriptive"
    if lower.startswith("_"):
        return "system"
    return "attribute"


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: Instruction Generation
# ═══════════════════════════════════════════════════════════════════════════════

def generate_table_detail_block(
    table_name: str,
    metadata: dict,
    profiles: list[dict],
    relationships: list[dict],
) -> str:
    """Generate a rich business description block for a single table."""
    simple = table_name.split(".")[-1]
    row_count = metadata["row_count"]
    purpose = get_table_purpose(simple)

    lines = [
        f"## {simple.upper()} ({row_count:,} records)",
        f"Purpose: {purpose}",
    ]

    buckets: dict[str, list[dict]] = {
        "monetary": [],
        "metric": [],
        "temporal": [],
        "dimension": [],
        "descriptive": [],
    }

    for col in metadata["columns"]:
        cn = col["column_name"]
        ct = col["data_type"]
        if cn.startswith("_"):
            continue
        prof = next(
            (p for p in profiles if p.get("column", p.get("column_name", "")) == cn),
            None,
        )
        role = classify_column_role(cn, ct, prof)
        biz_label = get_column_business_label(cn) or cn.replace("_", " ").title()
        entry = {
            "name": cn,
            "label": biz_label,
            "type": ct,
            "profile": prof,
            "comment": col.get("comment", ""),
        }
        if role in buckets:
            buckets[role].append(entry)

    section_map = {
        "monetary": "Key Monetary Fields",
        "metric": "Key Metrics",
        "temporal": "Date Fields",
        "dimension": "Segmentation Dimensions",
        "descriptive": "Descriptive Fields",
    }

    for role_key, section_title in section_map.items():
        cols = buckets[role_key]
        if not cols:
            continue
        display_cols = cols[:3] if role_key == "descriptive" else cols
        lines.append(f"{section_title}:")
        for c in display_cols:
            detail = f"  - {c['name']} — {c['label']}"
            if role_key == "monetary" and c["profile"] and c["profile"].get("null_percentage", 0) > 0:
                detail += f" ({c['profile']['null_percentage']:.0f}% null)"
            if role_key == "dimension" and c["profile"]:
                detail += f" ({c['profile']['distinct_count']} unique values)"
            lines.append(detail)

    table_rels = [
        r
        for r in relationships
        if table_name in (r.get("source_table", ""), r.get("target_table", ""))
    ]
    if table_rels:
        lines.append("Relationships:")
        for rel in table_rels:
            src_simple = rel["source_table"].split(".")[-1]
            tgt_simple = rel["target_table"].split(".")[-1]
            if rel["source_table"] == table_name:
                lines.append(f"  - {rel['source_column']} → {tgt_simple}.{rel['target_column']}")
            else:
                lines.append(f"  - {rel['target_column']} ← {src_simple}.{rel['source_column']}")

    return "\n".join(lines)


def generate_kpi_section(
    table_metadata: dict[str, dict],
    column_profiles: dict[str, list[dict]],
    domain: dict[str, str],
) -> list[str]:
    """Infer business KPIs from monetary and metric columns."""
    kpis: list[str] = []

    for table_name, metadata in table_metadata.items():
        simple = table_name.split(".")[-1]
        profiles = column_profiles.get(table_name, [])

        for col in metadata["columns"]:
            cn = col["column_name"]
            ct = col["data_type"].upper()
            prof = next(
                (p for p in profiles if p.get("column", p.get("column_name", "")) == cn),
                None,
            )
            role = classify_column_role(cn, ct, prof)

            if role == "monetary":
                biz_label = get_column_business_label(cn) or cn.replace("_", " ").title()
                kpis.append(f"- Total {biz_label} = SUM({simple}.{cn})")
                kpis.append(f"- Average {biz_label} = AVG({simple}.{cn})")

    if domain.get("key") == "sales_pipeline":
        kpis.extend(
            [
                "- Win Rate = COUNT(deals WHERE stagename = 'Closed Won') / COUNT(all closed deals) × 100",
                "- Pipeline Value = SUM(amount) WHERE stagename NOT IN ('Closed Won', 'Closed Lost')",
                "- Weighted Pipeline = SUM(amount × probability / 100) for open deals",
                "- Sales Cycle Length = AVG(days_to_close) for closed deals",
            ]
        )

    seen: set[str] = set()
    unique: list[str] = []
    for k in kpis:
        normalized = k.lower().strip()
        if normalized not in seen:
            seen.add(normalized)
            unique.append(k)
    return unique


def generate_relationship_narrative(
    relationships: list[dict],
    table_metadata: dict[str, dict],
) -> list[dict]:
    """Generate business-meaningful relationship descriptions."""
    narratives: list[dict] = []

    for rel in relationships:
        src_simple = rel["source_table"].split(".")[-1]
        tgt_simple = rel["target_table"].split(".")[-1]
        src_col = rel["source_column"]
        join_col_lower = src_col.lower()

        purpose_map = {
            "account": "Links deals to customer accounts for account-level revenue analysis and segmentation.",
            "owner": "Links deals to their sales rep owners for individual and team performance analysis.",
            "manager": "Maps the management hierarchy for roll-up reporting and team pipeline views.",
            "customer": "Associates transactions with customer records for customer-level analysis.",
            "product": "Links line items to the product catalog for product mix and category analysis.",
            "order": "Connects line items to their parent orders for order-level aggregation.",
        }

        purpose = f"Joins {src_simple} with {tgt_simple} for combined analysis."
        for keyword, desc in purpose_map.items():
            if keyword in join_col_lower:
                purpose = desc
                break
        if join_col_lower in ("id",):
            purpose = f"Shared identifier linking {src_simple} and {tgt_simple} records."

        src_rows = table_metadata.get(rel["source_table"], {}).get("row_count", 0)
        tgt_rows = table_metadata.get(rel["target_table"], {}).get("row_count", 0)
        if src_rows > tgt_rows:
            cardinality = "many-to-one"
        elif src_rows < tgt_rows:
            cardinality = "one-to-many"
        else:
            cardinality = "one-to-one"

        narratives.append(
            {
                "join": f"{src_simple}.{src_col} = {tgt_simple}.{rel['target_column']}",
                "purpose": purpose,
                "cardinality": cardinality,
                "src_simple": src_simple,
                "tgt_simple": tgt_simple,
            }
        )

    return narratives


def generate_filtering_guidance(
    table_metadata: dict[str, dict],
    column_profiles: dict[str, list[dict]],
) -> list[str]:
    """Generate filtering tips from low-cardinality dimensions and date columns."""
    guidance: list[str] = []

    for table_name, metadata in table_metadata.items():
        simple = table_name.split(".")[-1]
        profiles = column_profiles.get(table_name, [])

        for col in metadata["columns"]:
            cn = col["column_name"]
            ct = col["data_type"].upper()
            if cn.startswith("_"):
                continue
            prof = next(
                (p for p in profiles if p.get("column", p.get("column_name", "")) == cn),
                None,
            )

            if (
                prof
                and prof.get("cardinality_category") in ("very_low", "low")
                and "STRING" in ct
            ):
                biz_label = get_column_business_label(cn) or cn.replace("_", " ").title()
                guidance.append(
                    f"- {biz_label} ({simple}.{cn}): "
                    f"{prof['distinct_count']} unique values — ideal for segmentation and filtering"
                )

            if ct in ("DATE", "TIMESTAMP") and not cn.startswith("_"):
                biz_label = get_column_business_label(cn) or cn.replace("_", " ").title()
                guidance.append(
                    f"- {biz_label} ({simple}.{cn}): "
                    f"Use for time-based filtering (e.g., 'last 30 days', 'Q4 2025', 'YTD')"
                )

    return guidance


def generate_domain_best_practices(
    domain: dict[str, str],
    table_metadata: dict[str, dict],
    relationships: list[dict] | None = None,
) -> list[str]:
    """Generate domain-specific best practices from the data structure."""
    practices: list[str] = []
    table_names_lower = [t.split(".")[-1].lower() for t in table_metadata]

    if domain.get("key") == "sales_pipeline":
        practices.extend(
            [
                "When analyzing active pipeline, always exclude 'Closed Won' and 'Closed Lost' "
                "stages unless doing historical win/loss analysis.",
                "For revenue forecasting, use the forecastcategory field to group deals into "
                "Commit, Best Case, and Pipeline buckets.",
                "To calculate weighted pipeline, multiply deal amount by probability "
                "(amount × probability / 100).",
            ]
        )
        if any("history" in t or "cube" in t for t in table_names_lower):
            practices.append(
                "For trend and historical analysis, use the history/cube table rather than "
                "the current opportunity snapshot."
            )
        if any("user" in t for t in table_names_lower):
            practices.append(
                "To analyze sales rep performance, join opportunities to users via the owner ID relationship."
            )
        if any("account" in t for t in table_names_lower):
            practices.append(
                "For account-level rollups (total pipeline per account), join opportunities "
                "to accounts via the account ID."
            )
    elif domain.get("key") == "ecommerce":
        practices.extend(
            [
                "When calculating revenue, use line-item level amounts rather than order-level "
                "totals to account for partial fulfillment.",
                "For cohort analysis, use the customer's first order date as the cohort key.",
                "Exclude cancelled and returned orders from revenue metrics unless doing return-rate analysis.",
            ]
        )
    elif domain.get("key") == "finance":
        practices.extend(
            [
                "Always balance debits and credits when validating journal entries.",
                "Use fiscal period fields rather than calendar dates for period-over-period comparisons.",
                "Filter by posting status to exclude draft or reversed entries from financial reports.",
            ]
        )

    practices.extend(
        [
            "Be specific about date ranges (e.g., 'last 30 days', 'Q4 2025') rather than open-ended queries.",
            "Specify the exact metric you need (e.g., 'total revenue', 'count of deals', 'average deal size').",
            "When a column exists in multiple tables, specify the table name to avoid ambiguity.",
            "Filter results to improve performance — avoid selecting all historical data without date constraints.",
        ]
    )

    return practices


def categorize_questions(
    example_queries: list[dict],
) -> dict[str, list[str]]:
    """Organize example questions into business-relevant categories."""
    categories: dict[str, list[str]] = {
        "Pipeline & Revenue": [],
        "Performance & Rankings": [],
        "Segmentation & Distribution": [],
        "Trends & Historical": [],
        "Account & Customer": [],
        "Team & Organizational": [],
        "Other": [],
    }

    cat_keywords = {
        "Pipeline & Revenue": [
            "pipeline", "amount", "revenue", "forecast", "weighted", "value", "booking", "total",
        ],
        "Performance & Rankings": [
            "top", "best", "worst", "rank", "performance", "win rate", "rep",
        ],
        "Segmentation & Distribution": [
            "by region", "by industry", "by stage", "distribution", "breakdown",
            "segment", "bucket", "classify",
        ],
        "Trends & Historical": [
            "trend", "monthly", "quarterly", "over time", "history", "evolved", "growth",
        ],
        "Account & Customer": ["account", "customer", "client"],
        "Team & Organizational": ["manager", "team", "user", "role"],
    }

    for eq in example_queries:
        question = eq.get("natural_language", "").lower()
        placed = False
        for cat_name, keywords in cat_keywords.items():
            if any(kw in question for kw in keywords):
                categories[cat_name].append(eq["natural_language"])
                placed = True
                break
        if not placed:
            categories["Other"].append(eq["natural_language"])

    return {k: v for k, v in categories.items() if v}


def generate_business_driven_instructions(
    table_metadata: dict[str, dict],
    column_profiles: dict[str, list[dict]],
    relationships: list[dict],
    example_queries: list[dict],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Generate rich, business-driven Genie instructions without any LLM calls.

    Uses metadata, column profiles, relationships, and example queries to
    synthesize domain-aware, business-friendly instructions.

    Returns a dict with keys: global_instructions, domain, kpis, relationship_narratives.
    """
    schema_name = config.get("schema", "data")
    domain = detect_domain(table_metadata, column_profiles)
    schema_label = schema_name.replace("_", " ").title()

    sections: list[str] = []

    sections.append(
        f"{schema_label.upper()} ANALYTICS\n"
        f"Domain: {domain['label']}\n"
        f"This analytics space provides comprehensive access to the {schema_label} dataset, "
        f"enabling data-driven decision-making for {domain['description']}."
    )

    overview_lines = ["DATA MODEL OVERVIEW:"]
    sorted_tables = sorted(
        table_metadata.items(), key=lambda x: x[1]["row_count"], reverse=True
    )
    for fqn, meta in sorted_tables:
        simple = fqn.split(".")[-1]
        overview_lines.append(f"  - {simple} ({meta['row_count']:,} records)")
    total_rows = sum(m["row_count"] for m in table_metadata.values())
    overview_lines.append(
        f"  Total: {total_rows:,} records across {len(table_metadata)} tables"
    )
    sections.append("\n".join(overview_lines))

    sections.append("TABLE DETAILS:")
    for fqn, meta in sorted_tables:
        profiles = column_profiles.get(fqn, [])
        block = generate_table_detail_block(fqn, meta, profiles, relationships)
        sections.append(block)

    rel_narratives: list[dict] = []
    if relationships:
        rel_narratives = generate_relationship_narrative(relationships, table_metadata)
        rel_lines = ["KEY RELATIONSHIPS:"]
        for rn in rel_narratives:
            rel_lines.append(f"- {rn['join']} ({rn['cardinality']})")
            rel_lines.append(f"  {rn['purpose']}")
        sections.append("\n".join(rel_lines))

    kpis = generate_kpi_section(table_metadata, column_profiles, domain)
    if kpis:
        sections.append("BUSINESS METRICS & KPIs:\n" + "\n".join(kpis))

    filter_guidance = generate_filtering_guidance(table_metadata, column_profiles)
    if filter_guidance:
        sections.append("AVAILABLE FILTERS & DIMENSIONS:\n" + "\n".join(filter_guidance))

    if example_queries:
        categorized = categorize_questions(example_queries)
        eq_lines = ["COMMON BUSINESS QUESTIONS:"]
        for cat, questions in categorized.items():
            eq_lines.append(f"  {cat}:")
            for q in questions[:4]:
                eq_lines.append(f"    - {q}")
        sections.append("\n".join(eq_lines))

    practices = generate_domain_best_practices(domain, table_metadata, relationships)
    if practices:
        bp_lines = ["BEST PRACTICES:"]
        for i, p in enumerate(practices, 1):
            bp_lines.append(f"  {i}. {p}")
        sections.append("\n".join(bp_lines))

    sections.append(
        f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')} by Auto-Genie "
        f"(LLM-agnostic metadata-driven approach)."
    )

    return {
        "global_instructions": "\n\n".join(sections),
        "domain": domain,
        "kpis": kpis,
        "relationship_narratives": rel_narratives,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: SQL Expression Generation
# ═══════════════════════════════════════════════════════════════════════════════

def generate_sql_expressions(
    table_metadata: dict[str, dict],
    column_profiles: dict[str, list[dict]],
) -> dict[str, list[dict]]:
    """Auto-generate common measures, filters, and dimensions from metadata."""
    expressions: dict[str, list[dict]] = {
        "measures": [],
        "filters": [],
        "dimensions": [],
    }

    for table_name, metadata in table_metadata.items():
        simple_name = table_name.split(".")[-1]
        profiles = column_profiles.get(table_name, [])

        for col in metadata["columns"]:
            col_name = col["column_name"]
            col_type = col["data_type"].upper()

            if any(t in col_type for t in ["BIGINT", "INT", "DECIMAL", "DOUBLE", "FLOAT", "LONG"]):
                if any(kw in col_name.lower() for kw in ["amount", "price", "revenue", "cost", "total"]):
                    expressions["measures"].append(
                        {
                            "name": f"Total {col_name.replace('_', ' ').title()}",
                            "expression": f"SUM({table_name}.`{col_name}`)",
                            "description": f"Sum of all {col_name.replace('_', ' ')} values",
                            "table": simple_name,
                        }
                    )
                if any(kw in col_name.lower() for kw in ["amount", "value", "price"]):
                    expressions["measures"].append(
                        {
                            "name": f"Average {col_name.replace('_', ' ').title()}",
                            "expression": f"AVG({table_name}.`{col_name}`)",
                            "description": f"Average {col_name.replace('_', ' ')} per record",
                            "table": simple_name,
                        }
                    )
                if col_name.endswith("_id") or col_name == "id":
                    entity = col_name.replace("_id", "").replace("_", " ").title()
                    expressions["measures"].append(
                        {
                            "name": f"{entity} Count",
                            "expression": f"COUNT(DISTINCT {table_name}.`{col_name}`)",
                            "description": f"Number of unique {entity.lower()}s",
                            "table": simple_name,
                        }
                    )

            if col_type in ("DATE", "TIMESTAMP"):
                expressions["filters"].append(
                    {
                        "name": f"Recent {simple_name.title()} (Last 30 Days)",
                        "expression": f"{table_name}.`{col_name}` >= CURRENT_DATE - INTERVAL 30 DAYS",
                        "description": f"Filter {simple_name} from the last 30 days",
                        "table": simple_name,
                    }
                )
                expressions["filters"].append(
                    {
                        "name": f"Year to Date {simple_name.title()}",
                        "expression": f"YEAR({table_name}.`{col_name}`) = YEAR(CURRENT_DATE)",
                        "description": f"Filter {simple_name} from current year",
                        "table": simple_name,
                    }
                )
                expressions["dimensions"].append(
                    {
                        "name": f"{col_name.replace('_', ' ').title()} - Year",
                        "expression": f"YEAR({table_name}.`{col_name}`)",
                        "description": f"Year from {col_name}",
                        "table": simple_name,
                    }
                )
                expressions["dimensions"].append(
                    {
                        "name": f"{col_name.replace('_', ' ').title()} - Month",
                        "expression": f"DATE_TRUNC('MONTH', {table_name}.`{col_name}`)",
                        "description": f"Month from {col_name}",
                        "table": simple_name,
                    }
                )
                expressions["dimensions"].append(
                    {
                        "name": f"{col_name.replace('_', ' ').title()} - Quarter",
                        "expression": (
                            f"CONCAT('Q', QUARTER({table_name}.`{col_name}`), "
                            f"' ', YEAR({table_name}.`{col_name}`))"
                        ),
                        "description": f"Quarter from {col_name}",
                        "table": simple_name,
                    }
                )

            if "amount" in col_name.lower() and any(
                t in col_type for t in ["DECIMAL", "DOUBLE", "FLOAT"]
            ):
                expressions["filters"].append(
                    {
                        "name": f"High Value {simple_name.title()}",
                        "expression": f"{table_name}.`{col_name}` > 1000",
                        "description": f"Filter {simple_name} with {col_name} exceeding $1,000",
                        "table": simple_name,
                    }
                )

            col_profile = next(
                (p for p in profiles if p["column"] == col_name), None
            )
            if (
                col_profile
                and col_profile["cardinality_category"] == "very_low"
                and "STRING" in col_type
            ):
                expressions["filters"].append(
                    {
                        "name": f"{col_name.replace('_', ' ').title()} Filter",
                        "expression": f"{table_name}.`{col_name}` = '{{{{value}}}}'",
                        "description": f"Filter by specific {col_name.replace('_', ' ')}",
                        "table": simple_name,
                    }
                )

    return expressions


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: Table & Join Instruction Generation
# ═══════════════════════════════════════════════════════════════════════════════

def generate_table_instructions(
    table_metadata: dict[str, dict],
    column_profiles: dict[str, list[dict]],
    relationships: list[dict],
    example_queries: list[dict],
) -> dict[str, dict]:
    """Generate human-readable instructions for each table."""
    instructions: dict[str, dict] = {}

    for table_name, metadata in table_metadata.items():
        simple_name = table_name.split(".")[-1]
        row_count = metadata["row_count"]
        last_modified = str(metadata["last_modified"])

        desc_parts = [
            f"The {simple_name} table contains {row_count:,} records as of {last_modified}."
        ]

        lower_name = simple_name.lower()
        if "order" in lower_name:
            desc_parts.append(
                "This table tracks transaction records including dates, amounts, and customer associations."
            )
        elif "customer" in lower_name or "account" in lower_name:
            desc_parts.append(
                "This table stores customer/account information including contact details and status."
            )
        elif "product" in lower_name:
            desc_parts.append(
                "This table contains product catalog information including names, categories, and pricing."
            )
        elif "user" in lower_name:
            desc_parts.append(
                "This table stores user information and access details."
            )
        elif "opportunit" in lower_name:
            desc_parts.append(
                "This table contains sales opportunity and pipeline data."
            )

        key_columns: dict[str, str] = {}
        profiles = column_profiles.get(table_name, [])
        for col in metadata["columns"][:10]:
            col_name = col["column_name"]
            col_comment = col.get("comment", "")
            col_profile = next(
                (p for p in profiles if p["column"] == col_name), None
            )
            description = col_comment if col_comment else f"{col_name} column"
            if col_profile:
                if col_profile["recommend_entity_matching"]:
                    description += (
                        f" (Low cardinality: {col_profile['distinct_count']} unique values"
                        " - good for filtering)"
                    )
                if col_profile.get("pii_detected"):
                    description += f" ⚠️ Contains {col_profile['pii_detected'].upper()}"
            key_columns[col_name] = description

        table_rels = [
            r
            for r in relationships
            if table_name in [r.get("source_table"), r.get("target_table")]
        ]
        join_hints: list[str] = []
        for rel in table_rels[:5]:
            other = (
                rel["target_table"]
                if rel["source_table"] == table_name
                else rel["source_table"]
            )
            join_hints.append(
                f"Join to {other.split('.')[-1]} using {rel['source_column']}"
            )

        related_examples = [
            ex
            for ex in example_queries
            if simple_name in ex.get("tables_used", [])
        ]
        use_cases = [ex["natural_language"] for ex in related_examples[:3]]

        instructions[table_name] = {
            "description": " ".join(desc_parts),
            "key_columns": key_columns,
            "join_hints": join_hints,
            "common_use_cases": use_cases,
            "row_count": row_count,
            "last_updated": last_modified,
        }

    return instructions


def _build_optimization_context(knowledge_store: dict) -> dict[str, str]:
    """Build the context strings needed by the instruction optimization prompt."""
    table_summary_lines = []
    current_desc_lines = []
    for tbl in knowledge_store.get("tables", []):
        name = tbl.get("table_name", tbl.get("full_name", "").split(".")[-1])
        row_count = tbl.get("row_count", 0)
        col_names = [c["name"] for c in tbl.get("columns", [])[:15]]
        row_label = f"{row_count:,}" if isinstance(row_count, (int, float)) else str(row_count)
        table_summary_lines.append(
            f"- {name} ({row_label} rows): columns = {', '.join(col_names)}"
        )
        current_desc_lines.append(f"- {name}: {tbl.get('description', 'No description')}")

    measures = knowledge_store.get("sql_expressions", {}).get("measures", [])
    measures_summary = ", ".join(m["name"] for m in measures[:10]) or "None"

    filters = knowledge_store.get("sql_expressions", {}).get("filters", [])
    filters_summary = ", ".join(f["name"] for f in filters[:10]) or "None"

    example_queries = knowledge_store.get("example_queries", [])
    eq_lines = [f"- {eq['natural_language']}" for eq in example_queries[:10]]
    eq_summary = "\n".join(eq_lines) or "None"

    return {
        "table_summary": "\n".join(table_summary_lines),
        "current_table_descriptions": "\n".join(current_desc_lines),
        "measures_summary": measures_summary,
        "filters_summary": filters_summary,
        "example_queries_summary": eq_summary,
    }


def optimize_genie_instructions(
    knowledge_store: dict,
    prompts_cfg: dict[str, Any],
) -> dict:
    """Use an LLM to optimize Genie Space instructions per Databricks best practices.

    Separates table-level content from global instructions and produces:
      - Concise, narrative-focused global text instructions
      - Enriched per-table descriptions incorporating column semantics

    Falls back to a rule-based optimization if the LLM call fails.
    """
    from databricks_langchain import ChatDatabricks
    from langchain_core.messages import HumanMessage, SystemMessage

    opt_prompts = prompts_cfg.get("instruction_optimization")
    if not opt_prompts:
        print("  ⚠️  No instruction_optimization prompts found — using rule-based fallback")
        return _rule_based_optimize(knowledge_store)

    model = prompts_cfg.get("llm_model", "databricks-claude-opus-4-6")
    current_instructions = knowledge_store.get("global_instructions", "")
    context = _build_optimization_context(knowledge_store)

    user_prompt = opt_prompts["user"].format(
        current_instructions=current_instructions,
        **context,
    )
    system_prompt = opt_prompts["system"]

    print(f"  🔄 Optimizing instructions via LLM ({model})…")
    print(f"     Original instruction length: {len(current_instructions):,} chars")

    try:
        llm = ChatDatabricks(model=model)
        raw = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]).content.strip()

        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        result = json.loads(raw)

        optimized_text = result.get("optimized_instructions", "")
        table_descs = result.get("table_descriptions", {})

        if not optimized_text or len(optimized_text) < 50:
            print("  ⚠️  LLM returned insufficient instructions — using rule-based fallback")
            return _rule_based_optimize(knowledge_store)

        knowledge_store["global_instructions"] = optimized_text

        for tbl in knowledge_store.get("tables", []):
            simple_name = tbl.get("table_name", tbl.get("full_name", "").split(".")[-1])
            if simple_name in table_descs:
                tbl["description"] = table_descs[simple_name][:500]

        print(f"  ✅ Optimized instruction length: {len(optimized_text):,} chars")
        print(f"     Reduction: {len(current_instructions) - len(optimized_text):,} chars "
              f"({(1 - len(optimized_text) / max(len(current_instructions), 1)) * 100:.0f}%)")
        print(f"     Enriched table descriptions: {len(table_descs)}")

        return knowledge_store

    except json.JSONDecodeError as e:
        print(f"  ⚠️  LLM returned invalid JSON ({e}) — using rule-based fallback")
        return _rule_based_optimize(knowledge_store)
    except Exception as e:
        print(f"  ⚠️  LLM optimization failed ({type(e).__name__}: {e}) — using rule-based fallback")
        return _rule_based_optimize(knowledge_store)


def _rule_based_optimize(knowledge_store: dict) -> dict:
    """Deterministic fallback: strip table-detail blocks from global instructions
    and enrich table descriptions with relevant content extracted from them."""

    current = knowledge_store.get("global_instructions", "")
    if not current:
        return knowledge_store

    table_names = {
        tbl.get("table_name", tbl.get("full_name", "").split(".")[-1]).upper()
        for tbl in knowledge_store.get("tables", [])
    }

    lines = current.split("\n")
    keep_lines: list[str] = []
    skip_sections = {
        "DATA MODEL OVERVIEW:", "TABLE DETAILS:", "BUSINESS METRICS & KPIS:",
        "AVAILABLE FILTERS & DIMENSIONS:", "COMMON BUSINESS QUESTIONS:",
    }

    skipping = False
    for line in lines:
        stripped = line.strip()

        if any(stripped.startswith(s) for s in skip_sections):
            skipping = True
            continue

        if stripped.startswith("##") and any(tn in stripped for tn in table_names):
            skipping = True
            continue

        is_section_header = (
            stripped.isupper()
            and len(stripped) > 3
            and not stripped.startswith("-")
            and not stripped.startswith("  ")
        )
        if is_section_header and skipping:
            if stripped not in skip_sections:
                skipping = False

        if not skipping:
            keep_lines.append(line)

    optimized = "\n".join(keep_lines).strip()

    while "\n\n\n" in optimized:
        optimized = optimized.replace("\n\n\n", "\n\n")

    if len(optimized) > 2000:
        optimized = optimized[:1997] + "…"

    knowledge_store["global_instructions"] = optimized

    for tbl in knowledge_store.get("tables", []):
        simple_name = tbl.get("table_name", tbl.get("full_name", "").split(".")[-1])
        desc = tbl.get("description", "")

        col_details = []
        for col_info in tbl.get("key_columns", {}).items():
            col_name, col_desc = col_info
            if col_desc and col_desc != f"{col_name} column":
                col_details.append(f"{col_name}: {col_desc}")

        use_cases = tbl.get("common_use_cases", [])
        if use_cases:
            desc += " Use cases: " + "; ".join(use_cases[:2]) + "."

        if col_details:
            desc += " Key fields: " + "; ".join(col_details[:5]) + "."

        tbl["description"] = desc[:500]

    print(f"  ✅ Rule-based optimization applied")
    print(f"     Optimized length: {len(optimized):,} chars")

    return knowledge_store


def generate_join_instructions(
    relationships: list[dict],
) -> list[dict]:
    """Convert relationships to Genie-compatible join instructions."""
    join_instructions: list[dict] = []

    for rel in relationships:
        source_table = rel["source_table"]
        target_table = rel["target_table"]
        source_col = rel["source_column"]
        target_col = rel["target_column"]
        src_simple = source_table.split(".")[-1]
        tgt_simple = target_table.split(".")[-1]

        join_type = "INNER"

        explanations = []
        if "order" in src_simple and "customer" in tgt_simple:
            explanations.append(
                f"Join {src_simple} with {tgt_simple} to get customer demographics and contact information."
            )
            explanations.append(
                "Use INNER JOIN to exclude orders with invalid customer references."
            )
        elif "order_item" in src_simple and "order" in tgt_simple:
            explanations.append(
                f"Join {src_simple} with {tgt_simple} to get order-level details like dates and customer."
            )
            explanations.append(
                "Always use INNER JOIN since every order item must have a parent order."
            )
        elif "order_item" in src_simple and "product" in tgt_simple:
            explanations.append(
                f"Join {src_simple} with {tgt_simple} to get product names, categories, and descriptions."
            )
            explanations.append(
                "Use INNER JOIN for product analysis, LEFT JOIN if you need to detect orphaned items."
            )
        else:
            explanations.append(
                f"Join {src_simple} with {tgt_simple} using the {source_col} relationship."
            )

        join_instructions.append(
            {
                "source_table": source_table,
                "source_column": source_col,
                "target_table": target_table,
                "target_column": target_col,
                "join_definition": f"{source_table}.{source_col} = {target_table}.{target_col}",
                "recommended_join_type": join_type,
                "explanation": " ".join(explanations),
                "confidence": rel.get("confidence", 0.0),
                "methods": rel.get("methods", []),
            }
        )

    return join_instructions
