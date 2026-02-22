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
