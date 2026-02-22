# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5: Instruction Generation
# MAGIC **Auto-Genie MVP - Stage 4: Instruction Generation**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Generating human-readable table descriptions and key-column documentation
# MAGIC - Converting relationships into Genie-compatible join instructions
# MAGIC - Auto-creating SQL measures, filters, and dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, re, json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
from dotenv import load_dotenv
import pandas as pd

_env_path = Path(os.getcwd())
for _candidate in [_env_path / ".env", _env_path.parent / ".env"]:
    if _candidate.exists():
        load_dotenv(_candidate, override=True)
        print(f"✅ Loaded environment from {_candidate}")
        break
else:
    print("⚠️  No .env file found — falling back to existing environment variables")

config = {
    "workspace_url": os.getenv("DATABRICKS_HOST", ""),
    "catalog": os.getenv("AUTO_GENIE_CATALOG", "main"),
    "schema": os.getenv("AUTO_GENIE_SCHEMA", "sales"),
    "tables": os.getenv("AUTO_GENIE_TABLES", "orders,customers,products,order_items,warehouses").split(","),
    "genie_warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
    "lookback_days": int(os.getenv("AUTO_GENIE_LOOKBACK_DAYS", "90")),
    "confidence_threshold": float(os.getenv("AUTO_GENIE_CONFIDENCE_THRESHOLD", "0.75")),
    "output_path": os.getenv("AUTO_GENIE_OUTPUT_PATH", "/dbfs/tmp/auto_genie_outputs"),
}

try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.remote(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
        cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
    ).getOrCreate()

print(f"✅ Config loaded — targeting {config['catalog']}.{config['schema']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5.0b: Load Prerequisites (Metadata, Profiles, Relationships, Examples)

# COMMAND ----------

# DBTITLE 1,Load Prerequisites
# --- table_metadata (from Notebook 2) ---
def _load_table_metadata():
    metadata = {}
    for table_name in config['tables']:
        fqn = f"{config['catalog']}.{config['schema']}.{table_name}"
        info = spark.sql(f"SELECT table_catalog,table_schema,table_name,table_type,comment FROM system.information_schema.tables WHERE table_catalog='{config['catalog']}' AND table_schema='{config['schema']}' AND table_name='{table_name}'").collect()
        if not info:
            continue
        cols = spark.sql(f"SELECT column_name,data_type,is_nullable,column_default,comment FROM system.information_schema.columns WHERE table_catalog='{config['catalog']}' AND table_schema='{config['schema']}' AND table_name='{table_name}' ORDER BY ordinal_position").collect()
        detail = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict()
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fqn}").collect()[0]['cnt']
        metadata[fqn] = {"table_info": info[0].asDict(), "columns": [c.asDict() for c in cols], "row_count": row_count, "size_bytes": detail.get('sizeInBytes', 0), "last_modified": detail.get('lastModified', ''), "constraints": []}
        print(f"  ✅ {fqn}")
    return metadata

print("Loading table metadata...")
table_metadata = _load_table_metadata()

# --- column_profiles (from Notebook 2) ---
def _profile_columns():
    profiles = {}
    for fqn, meta in table_metadata.items():
        tbl_profiles = []
        for col in meta['columns']:
            cn = col['column_name']
            try:
                r = spark.sql(f"SELECT COUNT(DISTINCT `{cn}`) as dc, COUNT(*) as tc, SUM(CASE WHEN `{cn}` IS NULL THEN 1 ELSE 0 END) as nc FROM {fqn}").collect()[0]
                card = r['dc']
                cat = "very_low" if card <= 10 else ("low" if card <= 100 else ("medium" if card <= 1000 else "high"))
                pii = "email" if re.search(r'email', cn, re.I) else ("phone" if re.search(r'phone|mobile', cn, re.I) else None)
                tbl_profiles.append({"column": cn, "data_type": col['data_type'], "distinct_count": card, "null_percentage": round(r['nc']/r['tc']*100, 2) if r['tc'] else 0, "cardinality_category": cat, "recommend_entity_matching": cat != "high", "pii_detected": pii, "recommend_masking": pii is not None})
            except Exception:
                pass
        profiles[fqn] = tbl_profiles
    return profiles

print("Profiling columns...")
column_profiles = _profile_columns()

# --- final_relationships (from Notebook 3) ---
print("Discovering relationships...")
final_relationships = []

# --- example_queries placeholder (from Notebook 4) ---
example_queries = []

print(f"\n✅ Prerequisites loaded: {len(table_metadata)} tables, {sum(len(v) for v in column_profiles.values())} column profiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5.1: Generate Table Instructions

# COMMAND ----------

# DBTITLE 1,Generate Table Instructions
def generate_table_instructions(table_metadata, column_profiles, relationships, example_queries):
    """Generate human-readable instructions for each table."""
    instructions = {}

    for table_name, metadata in table_metadata.items():
        simple_name = table_name.split('.')[-1]
        row_count = metadata['row_count']
        last_modified = str(metadata['last_modified'])

        description_parts = [f"The {simple_name} table contains {row_count:,} records as of {last_modified}."]

        if 'order' in simple_name.lower():
            description_parts.append("This table tracks transaction records including dates, amounts, and customer associations.")
        elif 'customer' in simple_name.lower() or 'account' in simple_name.lower():
            description_parts.append("This table stores customer/account information including contact details and status.")
        elif 'product' in simple_name.lower():
            description_parts.append("This table contains product catalog information including names, categories, and pricing.")
        elif 'user' in simple_name.lower():
            description_parts.append("This table stores user information and access details.")

        key_columns = {}
        profiles = column_profiles.get(table_name, [])
        for col in metadata['columns'][:10]:
            col_name = col['column_name']
            col_comment = col.get('comment', '')
            col_profile = next((p for p in profiles if p['column'] == col_name), None)
            description = col_comment if col_comment else f"{col_name} column"
            if col_profile:
                if col_profile['recommend_entity_matching']:
                    description += f" (Low cardinality: {col_profile['distinct_count']} unique values - good for filtering)"
                if col_profile['pii_detected']:
                    description += f" ⚠️ Contains {col_profile['pii_detected'].upper()}"
            key_columns[col_name] = description

        table_rels = [r for r in relationships if table_name in [r.get('source_table'), r.get('target_table')]]
        join_hints = []
        for rel in table_rels[:5]:
            if rel.get('source_table') == table_name:
                join_hints.append(f"Join to {rel['target_table'].split('.')[-1]} using {rel['source_column']}")
            else:
                join_hints.append(f"Join from {rel['source_table'].split('.')[-1]} using {rel['target_column']}")

        related_examples = [ex for ex in example_queries if simple_name in ex.get('tables_used', [])]
        use_cases = [ex['natural_language'] for ex in related_examples[:3]]

        instructions[table_name] = {
            'description': ' '.join(description_parts),
            'key_columns': key_columns,
            'join_hints': join_hints,
            'common_use_cases': use_cases,
            'row_count': row_count,
            'last_updated': last_modified,
        }

    return instructions

print("=" * 60)
print("GENERATING TABLE INSTRUCTIONS")
print("=" * 60)
table_instructions = generate_table_instructions(
    table_metadata, column_profiles, final_relationships, example_queries
)
print(f"✅ Generated instructions for {len(table_instructions)} tables\n")

sample_table = list(table_instructions.keys())[0] if table_instructions else None
if sample_table:
    instr = table_instructions[sample_table]
    print("=" * 70)
    print(f"SAMPLE INSTRUCTION: {sample_table.split('.')[-1]} table")
    print("=" * 70)
    print(f"\n📖 DESCRIPTION:\n  {instr['description']}")
    print(f"\n🔑 KEY COLUMNS ({len(instr['key_columns'])} total):")
    for col, desc in list(instr['key_columns'].items())[:5]:
        print(f"  • {col}: {desc}")
    if instr['join_hints']:
        print(f"\n🔗 JOIN HINTS:")
        for hint in instr['join_hints']:
            print(f"  • {hint}")
    if instr['common_use_cases']:
        print(f"\n💼 COMMON USE CASES:")
        for uc in instr['common_use_cases']:
            print(f"  • {uc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5.2: Generate Join Instructions

# COMMAND ----------

# DBTITLE 1,Generate Join Instructions
def generate_join_instructions(relationships):
    """Convert relationships to Genie-compatible join instructions."""
    join_instructions = []

    for rel in relationships:
        source_table = rel['source_table']
        target_table = rel['target_table']
        source_col = rel['source_column']
        target_col = rel['target_column']
        source_simple = source_table.split('.')[-1]
        target_simple = target_table.split('.')[-1]

        join_type = "INNER"

        explanations = []
        if 'order' in source_simple and 'customer' in target_simple:
            explanations.append(f"Join {source_simple} with {target_simple} to get customer demographics and contact information.")
            explanations.append("Use INNER JOIN to exclude orders with invalid customer references.")
        elif 'order_item' in source_simple and 'order' in target_simple:
            explanations.append(f"Join {source_simple} with {target_simple} to get order-level details like dates and customer.")
            explanations.append("Always use INNER JOIN since every order item must have a parent order.")
        elif 'order_item' in source_simple and 'product' in target_simple:
            explanations.append(f"Join {source_simple} with {target_simple} to get product names, categories, and descriptions.")
            explanations.append("Use INNER JOIN for product analysis, LEFT JOIN if you need to detect orphaned items.")
        else:
            explanations.append(f"Join {source_simple} with {target_simple} using the {source_col} relationship.")

        join_instructions.append({
            'source_table': source_table,
            'source_column': source_col,
            'target_table': target_table,
            'target_column': target_col,
            'join_definition': f"{source_table}.{source_col} = {target_table}.{target_col}",
            'recommended_join_type': join_type,
            'explanation': ' '.join(explanations),
            'confidence': rel.get('confidence', 0.0),
            'methods': rel.get('methods', []),
        })

    return join_instructions

print("=" * 60)
print("GENERATING JOIN INSTRUCTIONS")
print("=" * 60)
join_instructions = generate_join_instructions(final_relationships)
print(f"✅ Generated {len(join_instructions)} join instructions\n")

for i, ji in enumerate(join_instructions, 1):
    print(f"\n{'=' * 70}")
    print(f"JOIN INSTRUCTION #{i}")
    print(f"{'=' * 70}")
    print(f"📋 Definition: {ji['join_definition']}")
    print(f"🔗 Type: {ji['recommended_join_type']} JOIN")
    print(f"📊 Confidence: {ji['confidence']:.2f}")
    print(f"\n💡 Explanation:\n  {ji['explanation']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5.3: Generate SQL Expressions

# COMMAND ----------

# DBTITLE 1,Generate SQL Expressions
def generate_sql_expressions(table_metadata, column_profiles, query_history=None):
    """Auto-generate common measures, filters, and dimensions."""
    expressions = {'measures': [], 'filters': [], 'dimensions': []}

    for table_name, metadata in table_metadata.items():
        simple_name = table_name.split('.')[-1]

        for col in metadata['columns']:
            col_name = col['column_name']
            col_type = col['data_type'].upper()

            if any(t in col_type for t in ['BIGINT', 'INT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'LONG']):
                if any(kw in col_name.lower() for kw in ['amount', 'price', 'revenue', 'cost', 'total']):
                    expressions['measures'].append({'name': f"Total {col_name.replace('_', ' ').title()}", 'expression': f"SUM({table_name}.`{col_name}`)", 'description': f"Sum of all {col_name.replace('_', ' ')} values", 'table': simple_name})
                if any(kw in col_name.lower() for kw in ['amount', 'value', 'price']):
                    expressions['measures'].append({'name': f"Average {col_name.replace('_', ' ').title()}", 'expression': f"AVG({table_name}.`{col_name}`)", 'description': f"Average {col_name.replace('_', ' ')} per record", 'table': simple_name})
                if col_name.endswith('_id') or col_name == 'id':
                    entity = col_name.replace('_id', '').replace('_', ' ').title()
                    expressions['measures'].append({'name': f"{entity} Count", 'expression': f"COUNT(DISTINCT {table_name}.`{col_name}`)", 'description': f"Number of unique {entity.lower()}s", 'table': simple_name})

            if col_type in ['DATE', 'TIMESTAMP']:
                expressions['filters'].append({'name': f"Recent {simple_name.title()} (Last 30 Days)", 'expression': f"{table_name}.`{col_name}` >= CURRENT_DATE - INTERVAL 30 DAYS", 'description': f"Filter {simple_name} from the last 30 days", 'table': simple_name})
                expressions['filters'].append({'name': f"Year to Date {simple_name.title()}", 'expression': f"YEAR({table_name}.`{col_name}`) = YEAR(CURRENT_DATE)", 'description': f"Filter {simple_name} from current year", 'table': simple_name})
                expressions['dimensions'].append({'name': f"{col_name.replace('_', ' ').title()} - Year", 'expression': f"YEAR({table_name}.`{col_name}`)", 'description': f"Year from {col_name}", 'table': simple_name})
                expressions['dimensions'].append({'name': f"{col_name.replace('_', ' ').title()} - Month", 'expression': f"DATE_TRUNC('MONTH', {table_name}.`{col_name}`)", 'description': f"Month from {col_name}", 'table': simple_name})
                expressions['dimensions'].append({'name': f"{col_name.replace('_', ' ').title()} - Quarter", 'expression': f"CONCAT('Q', QUARTER({table_name}.`{col_name}`), ' ', YEAR({table_name}.`{col_name}`))", 'description': f"Quarter from {col_name}", 'table': simple_name})

            if 'amount' in col_name.lower() and any(t in col_type for t in ['DECIMAL', 'DOUBLE', 'FLOAT']):
                expressions['filters'].append({'name': f"High Value {simple_name.title()}", 'expression': f"{table_name}.`{col_name}` > 1000", 'description': f"Filter {simple_name} with {col_name} exceeding $1,000", 'table': simple_name})

            profiles = column_profiles.get(table_name, [])
            col_profile = next((p for p in profiles if p['column'] == col_name), None)
            if col_profile and col_profile['cardinality_category'] == 'very_low' and 'STRING' in col_type:
                expressions['filters'].append({'name': f"{col_name.replace('_', ' ').title()} Filter", 'expression': f"{table_name}.`{col_name}` = '{{{{value}}}}'", 'description': f"Filter by specific {col_name.replace('_', ' ')}", 'table': simple_name})

    return expressions

print("=" * 60)
print("GENERATING SQL EXPRESSIONS")
print("=" * 60)
sql_expressions = generate_sql_expressions(table_metadata, column_profiles)

print(f"✅ Generated SQL expressions:")
print(f"  • {len(sql_expressions['measures'])} measures")
print(f"  • {len(sql_expressions['filters'])} filters")
print(f"  • {len(sql_expressions['dimensions'])} dimensions")

print("\n" + "=" * 70)
print("SAMPLE MEASURES (Top 5)")
print("=" * 70)
for m in sql_expressions['measures'][:5]:
    print(f"\n📊 {m['name']}\n  Expression: {m['expression']}\n  Table: {m['table']}")

print("\n" + "=" * 70)
print("SAMPLE FILTERS (Top 5)")
print("=" * 70)
for f in sql_expressions['filters'][:5]:
    print(f"\n🔍 {f['name']}\n  Expression: {f['expression']}\n  Table: {f['table']}")

print("\n" + "=" * 70)
print("SAMPLE DIMENSIONS (Top 5)")
print("=" * 70)
for d in sql_expressions['dimensions'][:5]:
    print(f"\n📅 {d['name']}\n  Expression: {d['expression']}\n  Table: {d['table']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5.4: Generate Business-Driven Instructions (LLM-Agnostic)

# COMMAND ----------

# DBTITLE 1,Generate Business-Driven Instructions

# ── Domain Detection ──────────────────────────────────────────────────────────

DOMAIN_SIGNALS = {
    'sales_pipeline': {
        'table_keywords': ['opportunity', 'pipeline', 'deal', 'lead', 'forecast', 'quote'],
        'column_keywords': ['stagename', 'forecastcategory', 'probability', 'closedate', 'pipeline', 'deal', 'leadsource', 'win'],
        'label': 'Sales Pipeline & CRM',
        'description': 'sales pipeline management, opportunity tracking, and revenue forecasting',
    },
    'ecommerce': {
        'table_keywords': ['order', 'cart', 'product', 'catalog', 'sku', 'shipment', 'invoice'],
        'column_keywords': ['order_id', 'sku', 'quantity', 'shipping', 'cart', 'checkout', 'unit_price'],
        'label': 'E-Commerce & Retail',
        'description': 'order management, product catalog, and customer transactions',
    },
    'finance': {
        'table_keywords': ['transaction', 'ledger', 'journal', 'balance', 'payment', 'invoice', 'billing'],
        'column_keywords': ['debit', 'credit', 'balance', 'gl_account', 'fiscal', 'ledger'],
        'label': 'Finance & Accounting',
        'description': 'financial transactions, accounting records, and payment processing',
    },
    'hr': {
        'table_keywords': ['employee', 'department', 'payroll', 'benefit', 'leave', 'attendance'],
        'column_keywords': ['employee_id', 'hire_date', 'salary', 'department', 'manager_id', 'position'],
        'label': 'Human Resources',
        'description': 'employee management, payroll, and organizational structure',
    },
    'marketing': {
        'table_keywords': ['campaign', 'impression', 'click', 'conversion', 'audience', 'channel'],
        'column_keywords': ['campaign_id', 'ctr', 'impressions', 'clicks', 'conversions', 'ad_spend'],
        'label': 'Marketing Analytics',
        'description': 'campaign performance, audience engagement, and marketing ROI',
    },
    'support': {
        'table_keywords': ['ticket', 'incident', 'case', 'escalation', 'sla', 'resolution'],
        'column_keywords': ['ticket_id', 'priority', 'severity', 'resolution_time', 'sla', 'assigned_to'],
        'label': 'Customer Support',
        'description': 'support ticket management, SLA tracking, and resolution analytics',
    },
}

BUSINESS_COLUMN_LABELS = {
    'stagename': 'Pipeline Stage',
    'stage_name': 'Pipeline Stage',
    'forecastcategory': 'Forecast Category',
    'forecast_category': 'Forecast Category',
    'probability': 'Win Probability (%)',
    'closedate': 'Expected Close Date',
    'close_date': 'Expected Close Date',
    'createddate': 'Deal Created Date',
    'created_date': 'Record Created Date',
    'amount': 'Deal Value ($)',
    'annualrevenue': 'Annual Revenue ($)',
    'annual_revenue': 'Annual Revenue ($)',
    'industry': 'Industry Vertical',
    'region': 'Sales Region',
    'region__c': 'Sales Region',
    'region_hq__c': 'HQ Region',
    'company_size_segment__c': 'Company Size Segment',
    'business_type__c': 'Business Type (New/Expansion)',
    'leadsource': 'Lead Source',
    'lead_source': 'Lead Source',
    'type': 'Record Type',
    'name': 'Name',
    'account_name': 'Account Name',
    'ownerid': 'Deal Owner (Sales Rep)',
    'owner_id': 'Deal Owner (Sales Rep)',
    'accountid': 'Parent Account',
    'account_id': 'Parent Account',
    'managerid': 'Reporting Manager',
    'manager_id': 'Reporting Manager',
    'opportunityid': 'Opportunity ID',
    'opportunity_id': 'Opportunity ID',
    'role__c': 'Job Role',
    'segment__c': 'Market Segment',
    'days_to_close': 'Sales Cycle Length (Days)',
    'opportunity_amount': 'Opportunity Amount ($)',
    'sumdealamount': 'Aggregated Deal Amount ($)',
    'dealstage': 'Deal Stage (Historical)',
    'status': 'Status',
    'description': 'Description',
    'revenue': 'Revenue ($)',
    'total': 'Total ($)',
    'price': 'Price ($)',
    'cost': 'Cost ($)',
    'quantity': 'Quantity',
    'discount': 'Discount',
    'order_date': 'Order Date',
    'ship_date': 'Ship Date',
    'customer_id': 'Customer',
    'product_id': 'Product',
    'category': 'Category',
    'email': 'Email Address',
    'phone': 'Phone Number',
}

TABLE_PURPOSE_PATTERNS = {
    'account': 'Master list of customer accounts with firmographic and demographic data. Use for account segmentation, regional distribution, and industry analysis.',
    'customer': 'Customer master data with contact details and segmentation attributes. Use for customer analysis and segmentation.',
    'opportunity': 'Individual sales deals tracked through pipeline stages from creation to close. Core table for pipeline analysis, forecasting, and rep performance.',
    'opportunityhistory': 'Historical snapshots of pipeline data capturing how deals and stages evolve over time. Use for trend analysis and pipeline movement tracking instead of the opportunity table.',
    'user': 'Sales team members including reps and managers with role and segment assignments. Use for team structure analysis and rep-level performance rollups.',
    'order': 'Transaction records capturing purchases with dates, amounts, and associated entities. Core table for revenue and order analysis.',
    'product': 'Product catalog with names, categories, pricing, and attributes. Use for product mix analysis and category performance.',
    'invoice': 'Billing records with line items, amounts, and payment status. Use for AR analysis and revenue recognition.',
    'employee': 'Employee records with organizational and compensation data. Use for headcount, turnover, and workforce analytics.',
    'campaign': 'Marketing campaign records with performance metrics. Use for campaign ROI and channel effectiveness analysis.',
    'ticket': 'Support tickets with priority, assignment, and resolution data. Use for SLA compliance and support workload analysis.',
}


def _detect_domain(table_metadata, column_profiles):
    """Infer the business domain from table and column names."""
    all_tables = ' '.join(t.split('.')[-1].lower() for t in table_metadata)
    all_columns = ' '.join(
        c.get('column', c.get('column_name', '')).lower()
        for profiles in column_profiles.values()
        for c in profiles
    )
    combined = all_tables + ' ' + all_columns

    scores = {}
    for domain_key, signals in DOMAIN_SIGNALS.items():
        score = 0
        for kw in signals['table_keywords']:
            if kw in combined:
                score += 2
        for kw in signals['column_keywords']:
            if kw in combined:
                score += 1
        scores[domain_key] = score

    best = max(scores, key=scores.get) if scores else 'general'
    if scores.get(best, 0) < 2:
        return {
            'key': 'general',
            'label': 'Business Analytics',
            'description': 'general business data analysis and reporting',
        }
    return {
        'key': best,
        'label': DOMAIN_SIGNALS[best]['label'],
        'description': DOMAIN_SIGNALS[best]['description'],
    }


def _get_table_purpose(simple_name):
    """Match a table name to a business purpose description."""
    lower = simple_name.lower()
    for pattern, purpose in TABLE_PURPOSE_PATTERNS.items():
        if pattern in lower:
            return purpose
    return f"Contains {simple_name} data for analysis and reporting."


def _get_column_business_label(col_name):
    """Get a human-friendly business label for a column."""
    return BUSINESS_COLUMN_LABELS.get(col_name.lower(), None)


def _classify_column_role(col_name, col_type, profile):
    """Classify a column into a business role based on name and statistics."""
    lower = col_name.lower()
    ctype = col_type.upper()

    if lower in ('id',) or lower.endswith('_id') or lower == 'opportunityid':
        return 'identifier'
    if any(kw in lower for kw in ['amount', 'revenue', 'price', 'cost', 'total', 'value', 'booking', 'sumdeal']):
        return 'monetary'
    if any(kw in lower for kw in ['probability', 'rate', 'percentage', 'pct', 'ratio']):
        return 'metric'
    if lower in ('days_to_close',) or 'days' in lower or 'duration' in lower or 'count' in lower:
        return 'metric'
    if ctype in ('DATE', 'TIMESTAMP') or 'date' in lower:
        return 'temporal'
    if profile and profile['cardinality_category'] in ('very_low', 'low') and 'STRING' in ctype:
        return 'dimension'
    if lower in ('name', 'title', 'description', 'label', 'account_name'):
        return 'descriptive'
    if lower.startswith('_'):
        return 'system'
    return 'attribute'


def _generate_table_detail_block(table_name, metadata, profiles, relationships):
    """Generate a rich business description block for a single table."""
    simple = table_name.split('.')[-1]
    row_count = metadata['row_count']
    purpose = _get_table_purpose(simple)

    lines = [
        f"## {simple.upper()} ({row_count:,} records)",
        f"Purpose: {purpose}",
    ]

    monetary_cols = []
    metric_cols = []
    temporal_cols = []
    dimension_cols = []
    descriptive_cols = []

    for col in metadata['columns']:
        cn = col['column_name']
        ct = col['data_type']
        if cn.startswith('_'):
            continue
        prof = next((p for p in profiles if p['column'] == cn), None)
        role = _classify_column_role(cn, ct, prof)
        biz_label = _get_column_business_label(cn) or cn.replace('_', ' ').title()

        entry = {'name': cn, 'label': biz_label, 'type': ct, 'profile': prof, 'comment': col.get('comment', '')}

        if role == 'monetary':
            monetary_cols.append(entry)
        elif role == 'metric':
            metric_cols.append(entry)
        elif role == 'temporal':
            temporal_cols.append(entry)
        elif role == 'dimension':
            dimension_cols.append(entry)
        elif role == 'descriptive':
            descriptive_cols.append(entry)

    if monetary_cols:
        lines.append("Key Monetary Fields:")
        for c in monetary_cols:
            detail = f"  - {c['name']} — {c['label']}"
            if c['profile'] and c['profile']['null_percentage'] > 0:
                detail += f" ({c['profile']['null_percentage']:.0f}% null)"
            lines.append(detail)

    if metric_cols:
        lines.append("Key Metrics:")
        for c in metric_cols:
            detail = f"  - {c['name']} — {c['label']}"
            lines.append(detail)

    if temporal_cols:
        lines.append("Date Fields:")
        for c in temporal_cols:
            lines.append(f"  - {c['name']} — {c['label']}")

    if dimension_cols:
        lines.append("Segmentation Dimensions:")
        for c in dimension_cols:
            distinct = c['profile']['distinct_count'] if c['profile'] else '?'
            lines.append(f"  - {c['name']} — {c['label']} ({distinct} unique values)")

    if descriptive_cols:
        lines.append("Descriptive Fields:")
        for c in descriptive_cols[:3]:
            lines.append(f"  - {c['name']} — {c['label']}")

    table_rels = [r for r in relationships if table_name in (r.get('source_table', ''), r.get('target_table', ''))]
    if table_rels:
        lines.append("Relationships:")
        for rel in table_rels:
            src_simple = rel['source_table'].split('.')[-1]
            tgt_simple = rel['target_table'].split('.')[-1]
            src_col = rel['source_column']
            tgt_col = rel['target_column']
            if rel['source_table'] == table_name:
                lines.append(f"  - {src_col} → {tgt_simple}.{tgt_col}")
            else:
                lines.append(f"  - {tgt_col} ← {src_simple}.{src_col}")

    return '\n'.join(lines)


def _generate_kpi_section(table_metadata, column_profiles, domain):
    """Infer business KPIs from monetary and metric columns."""
    kpis = []

    for table_name, metadata in table_metadata.items():
        simple = table_name.split('.')[-1]
        profiles = column_profiles.get(table_name, [])

        for col in metadata['columns']:
            cn = col['column_name']
            ct = col['data_type'].upper()
            prof = next((p for p in profiles if p['column'] == cn), None)
            role = _classify_column_role(cn, ct, prof)

            if role == 'monetary':
                biz_label = _get_column_business_label(cn) or cn.replace('_', ' ').title()
                kpis.append(f"- Total {biz_label} = SUM({simple}.{cn})")
                kpis.append(f"- Average {biz_label} = AVG({simple}.{cn})")

    if domain['key'] == 'sales_pipeline':
        kpis.extend([
            "- Win Rate = COUNT(deals WHERE stagename = 'Closed Won') / COUNT(all closed deals) × 100",
            "- Pipeline Value = SUM(amount) WHERE stagename NOT IN ('Closed Won', 'Closed Lost')",
            "- Weighted Pipeline = SUM(amount × probability / 100) for open deals",
            "- Sales Cycle Length = AVG(days_to_close) for closed deals",
        ])

    seen = set()
    unique_kpis = []
    for k in kpis:
        normalized = k.lower().strip()
        if normalized not in seen:
            seen.add(normalized)
            unique_kpis.append(k)
    return unique_kpis


def _generate_relationship_narrative(relationships, table_metadata):
    """Generate business-meaningful relationship descriptions."""
    narratives = []
    for rel in relationships:
        src_simple = rel['source_table'].split('.')[-1]
        tgt_simple = rel['target_table'].split('.')[-1]
        src_col = rel['source_column']
        tgt_col = rel['target_column']

        src_rows = table_metadata.get(rel['source_table'], {}).get('row_count', 0)
        tgt_rows = table_metadata.get(rel['target_table'], {}).get('row_count', 0)

        join_col_lower = src_col.lower()

        if 'account' in join_col_lower:
            purpose = "Links deals to customer accounts for account-level revenue analysis and segmentation."
        elif 'owner' in join_col_lower:
            purpose = "Links deals to their sales rep owners for individual and team performance analysis."
        elif 'manager' in join_col_lower:
            purpose = "Maps the management hierarchy for roll-up reporting and team pipeline views."
        elif 'customer' in join_col_lower:
            purpose = "Associates transactions with customer records for customer-level analysis."
        elif 'product' in join_col_lower:
            purpose = "Links line items to the product catalog for product mix and category analysis."
        elif 'order' in join_col_lower:
            purpose = "Connects line items to their parent orders for order-level aggregation."
        elif join_col_lower in ('id',):
            purpose = f"Shared identifier linking {src_simple} and {tgt_simple} records."
        else:
            purpose = f"Joins {src_simple} with {tgt_simple} for combined analysis."

        cardinality = "many-to-one" if src_rows > tgt_rows else ("one-to-many" if src_rows < tgt_rows else "one-to-one")

        narratives.append({
            'join': f"{src_simple}.{src_col} = {tgt_simple}.{tgt_col}",
            'purpose': purpose,
            'cardinality': cardinality,
            'src_simple': src_simple,
            'tgt_simple': tgt_simple,
        })
    return narratives


def _generate_filtering_guidance(table_metadata, column_profiles):
    """Generate filtering tips from low-cardinality dimensions and date columns."""
    guidance = []

    for table_name, metadata in table_metadata.items():
        simple = table_name.split('.')[-1]
        profiles = column_profiles.get(table_name, [])

        for col in metadata['columns']:
            cn = col['column_name']
            ct = col['data_type'].upper()
            if cn.startswith('_'):
                continue
            prof = next((p for p in profiles if p['column'] == cn), None)

            if prof and prof['cardinality_category'] in ('very_low', 'low') and 'STRING' in ct:
                biz_label = _get_column_business_label(cn) or cn.replace('_', ' ').title()
                guidance.append(
                    f"- {biz_label} ({simple}.{cn}): {prof['distinct_count']} unique values — ideal for segmentation and filtering"
                )

            if ct in ('DATE', 'TIMESTAMP') and not cn.startswith('_'):
                biz_label = _get_column_business_label(cn) or cn.replace('_', ' ').title()
                guidance.append(
                    f"- {biz_label} ({simple}.{cn}): Use for time-based filtering (e.g., 'last 30 days', 'Q4 2025', 'YTD')"
                )

    return guidance


def _generate_domain_best_practices(domain, table_metadata, relationships):
    """Generate domain-specific best practices from the data structure."""
    practices = []

    table_names_lower = [t.split('.')[-1].lower() for t in table_metadata]

    if domain['key'] == 'sales_pipeline':
        practices.extend([
            "When analyzing active pipeline, always exclude 'Closed Won' and 'Closed Lost' stages unless doing historical win/loss analysis.",
            "For revenue forecasting, use the forecastcategory field to group deals into Commit, Best Case, and Pipeline buckets.",
            "To calculate weighted pipeline, multiply deal amount by probability (amount × probability / 100).",
        ])
        if any('history' in t or 'cube' in t for t in table_names_lower):
            practices.append(
                "For trend and historical analysis, use the history/cube table rather than the current opportunity snapshot."
            )
        if any('user' in t for t in table_names_lower):
            practices.append(
                "To analyze sales rep performance, join opportunities to users via the owner ID relationship."
            )
        if any('account' in t for t in table_names_lower):
            practices.append(
                "For account-level rollups (total pipeline per account), join opportunities to accounts via the account ID."
            )

    elif domain['key'] == 'ecommerce':
        practices.extend([
            "When calculating revenue, use line-item level amounts rather than order-level totals to account for partial fulfillment.",
            "For cohort analysis, use the customer's first order date as the cohort key.",
            "Exclude cancelled and returned orders from revenue metrics unless doing return-rate analysis.",
        ])

    elif domain['key'] == 'finance':
        practices.extend([
            "Always balance debits and credits when validating journal entries.",
            "Use fiscal period fields rather than calendar dates for period-over-period comparisons.",
            "Filter by posting status to exclude draft or reversed entries from financial reports.",
        ])

    practices.extend([
        "Be specific about date ranges (e.g., 'last 30 days', 'Q4 2025') rather than open-ended queries.",
        "Specify the exact metric you need (e.g., 'total revenue', 'count of deals', 'average deal size').",
        "When a column exists in multiple tables, specify the table name to avoid ambiguity.",
        "Filter results to improve performance — avoid selecting all historical data without date constraints.",
    ])

    return practices


def _generate_sample_questions_by_category(example_queries, domain, table_metadata):
    """Organize example questions into business-relevant categories."""
    categories = {
        'Pipeline & Revenue': [],
        'Performance & Rankings': [],
        'Segmentation & Distribution': [],
        'Trends & Historical': [],
        'Account & Customer': [],
        'Team & Organizational': [],
        'Other': [],
    }

    category_keywords = {
        'Pipeline & Revenue': ['pipeline', 'amount', 'revenue', 'forecast', 'weighted', 'value', 'booking', 'total'],
        'Performance & Rankings': ['top', 'best', 'worst', 'rank', 'performance', 'win rate', 'rep'],
        'Segmentation & Distribution': ['by region', 'by industry', 'by stage', 'distribution', 'breakdown', 'segment', 'bucket', 'classify'],
        'Trends & Historical': ['trend', 'monthly', 'quarterly', 'over time', 'history', 'evolved', 'growth'],
        'Account & Customer': ['account', 'customer', 'client'],
        'Team & Organizational': ['manager', 'team', 'user', 'role'],
    }

    for eq in example_queries:
        question = eq.get('natural_language', '').lower()
        placed = False
        for cat_name, keywords in category_keywords.items():
            if any(kw in question for kw in keywords):
                categories[cat_name].append(eq['natural_language'])
                placed = True
                break
        if not placed:
            categories['Other'].append(eq['natural_language'])

    return {k: v for k, v in categories.items() if v}


def generate_business_driven_instructions(
    table_metadata, column_profiles, relationships, example_queries, config
):
    """Generate rich, business-driven Genie instructions without any LLM calls.

    Uses metadata, column profiles, relationships, and example queries to
    synthesize domain-aware, business-friendly instructions that help Genie
    understand the dataset's purpose, structure, and common usage patterns.

    Returns:
        dict with keys:
            - global_instructions: str — the full instruction text
            - domain: dict — detected business domain info
            - kpis: list — inferred KPI definitions
            - relationship_narratives: list — business-explained joins
    """
    schema_name = config.get('schema', 'data')
    domain = _detect_domain(table_metadata, column_profiles)

    schema_label = schema_name.replace('_', ' ').title()

    sections = []

    # ── Header ────────────────────────────────────────────────────────────────
    sections.append(
        f"{schema_label.upper()} ANALYTICS\n"
        f"Domain: {domain['label']}\n"
        f"This analytics space provides comprehensive access to the {schema_label} dataset, "
        f"enabling data-driven decision-making for {domain['description']}."
    )

    # ── Data Model Overview ───────────────────────────────────────────────────
    overview_lines = ["DATA MODEL OVERVIEW:"]
    sorted_tables = sorted(
        table_metadata.items(),
        key=lambda x: x[1]['row_count'],
        reverse=True,
    )
    for fqn, meta in sorted_tables:
        simple = fqn.split('.')[-1]
        overview_lines.append(f"  - {simple} ({meta['row_count']:,} records)")
    total_rows = sum(m['row_count'] for m in table_metadata.values())
    overview_lines.append(f"  Total: {total_rows:,} records across {len(table_metadata)} tables")
    sections.append('\n'.join(overview_lines))

    # ── Table Details ─────────────────────────────────────────────────────────
    sections.append("TABLE DETAILS:")
    for fqn, meta in sorted_tables:
        profiles = column_profiles.get(fqn, [])
        block = _generate_table_detail_block(fqn, meta, profiles, relationships)
        sections.append(block)

    # ── Key Relationships ─────────────────────────────────────────────────────
    if relationships:
        rel_narratives = _generate_relationship_narrative(relationships, table_metadata)
        rel_lines = ["KEY RELATIONSHIPS:"]
        for rn in rel_narratives:
            rel_lines.append(f"- {rn['join']} ({rn['cardinality']})")
            rel_lines.append(f"  {rn['purpose']}")
        sections.append('\n'.join(rel_lines))
    else:
        rel_narratives = []

    # ── Business KPIs ─────────────────────────────────────────────────────────
    kpis = _generate_kpi_section(table_metadata, column_profiles, domain)
    if kpis:
        sections.append("BUSINESS METRICS & KPIs:\n" + '\n'.join(kpis))

    # ── Filtering Guidance ────────────────────────────────────────────────────
    filter_guidance = _generate_filtering_guidance(table_metadata, column_profiles)
    if filter_guidance:
        sections.append("AVAILABLE FILTERS & DIMENSIONS:\n" + '\n'.join(filter_guidance))

    # ── Example Questions by Category ─────────────────────────────────────────
    if example_queries:
        categorized = _generate_sample_questions_by_category(example_queries, domain, table_metadata)
        eq_lines = ["COMMON BUSINESS QUESTIONS:"]
        for cat, questions in categorized.items():
            eq_lines.append(f"  {cat}:")
            for q in questions[:4]:
                eq_lines.append(f"    - {q}")
        sections.append('\n'.join(eq_lines))

    # ── Best Practices ────────────────────────────────────────────────────────
    practices = _generate_domain_best_practices(domain, table_metadata, relationships)
    if practices:
        bp_lines = ["BEST PRACTICES:"]
        for i, p in enumerate(practices, 1):
            bp_lines.append(f"  {i}. {p}")
        sections.append('\n'.join(bp_lines))

    # ── Footer ────────────────────────────────────────────────────────────────
    sections.append(
        f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')} by Auto-Genie "
        f"(LLM-agnostic metadata-driven approach)."
    )

    global_instructions = '\n\n'.join(sections)

    return {
        'global_instructions': global_instructions,
        'domain': domain,
        'kpis': kpis,
        'relationship_narratives': rel_narratives if relationships else [],
    }


print("=" * 60)
print("GENERATING BUSINESS-DRIVEN INSTRUCTIONS")
print("=" * 60)
business_result = generate_business_driven_instructions(
    table_metadata, column_profiles, final_relationships, example_queries, config
)
print(f"✅ Domain detected: {business_result['domain']['label']}")
print(f"✅ KPIs inferred: {len(business_result['kpis'])}")
print(f"✅ Relationship narratives: {len(business_result['relationship_narratives'])}")
print(f"✅ Instruction length: {len(business_result['global_instructions']):,} characters")

print("\n" + "=" * 70)
print("BUSINESS-DRIVEN INSTRUCTIONS PREVIEW")
print("=" * 70)
preview = business_result['global_instructions']
if len(preview) > 3000:
    print(preview[:3000])
    print(f"\n... ({len(preview) - 3000:,} more characters)")
else:
    print(preview)
