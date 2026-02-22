# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 6: Knowledge Store Assembly & Validation
# MAGIC **Auto-Genie MVP - Stage 5 & 6: Assembly & Validation**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Assembling all generated artifacts into a Genie API-compatible JSON knowledge store
# MAGIC - Validating structure completeness and consistency
# MAGIC - Validating SQL syntax for joins, expressions, and example queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, re, json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
from dotenv import load_dotenv
import pandas as pd
import sqlglot

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
# MAGIC ## Cell 6.0b: Load All Prerequisites (Metadata, Instructions, Expressions, Queries)

# COMMAND ----------

# DBTITLE 1,Load All Prerequisites
# ── table_metadata (from Notebook 2) ──────────────────────────────────────────
def _load_table_metadata():
    metadata = {}
    for table_name in config['tables']:
        fqn = f"{config['catalog']}.{config['schema']}.{table_name}"
        info = spark.sql(
            f"SELECT table_catalog,table_schema,table_name,table_type,comment "
            f"FROM system.information_schema.tables "
            f"WHERE table_catalog='{config['catalog']}' AND table_schema='{config['schema']}' AND table_name='{table_name}'"
        ).collect()
        if not info:
            print(f"  ⚠️  Table {fqn} not found — skipping")
            continue
        cols = spark.sql(
            f"SELECT column_name,data_type,is_nullable,column_default,comment "
            f"FROM system.information_schema.columns "
            f"WHERE table_catalog='{config['catalog']}' AND table_schema='{config['schema']}' AND table_name='{table_name}' "
            f"ORDER BY ordinal_position"
        ).collect()
        detail = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict()
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fqn}").collect()[0]['cnt']
        metadata[fqn] = {
            "table_info": info[0].asDict(),
            "columns": [c.asDict() for c in cols],
            "row_count": row_count,
            "size_bytes": detail.get('sizeInBytes', 0),
            "last_modified": detail.get('lastModified', ''),
            "constraints": [],
        }
        print(f"  ✅ {fqn} — {row_count:,} rows")
    return metadata

print("Loading table metadata...")
table_metadata = _load_table_metadata()

# ── column_profiles (from Notebook 2) ─────────────────────────────────────────
def _profile_columns():
    profiles = {}
    for fqn, meta in table_metadata.items():
        tbl_profiles = []
        for col in meta['columns']:
            cn = col['column_name']
            try:
                r = spark.sql(
                    f"SELECT COUNT(DISTINCT `{cn}`) as dc, COUNT(*) as tc, "
                    f"SUM(CASE WHEN `{cn}` IS NULL THEN 1 ELSE 0 END) as nc FROM {fqn}"
                ).collect()[0]
                card = r['dc']
                cat = ("very_low" if card <= 10 else
                       ("low" if card <= 100 else
                        ("medium" if card <= 1000 else "high")))
                pii = ("email" if re.search(r'email', cn, re.I)
                       else ("phone" if re.search(r'phone|mobile', cn, re.I) else None))
                tbl_profiles.append({
                    "column": cn, "data_type": col['data_type'],
                    "distinct_count": card,
                    "null_percentage": round(r['nc'] / r['tc'] * 100, 2) if r['tc'] else 0,
                    "cardinality_category": cat,
                    "recommend_entity_matching": cat != "high",
                    "pii_detected": pii,
                    "recommend_masking": pii is not None,
                })
            except Exception:
                pass
        profiles[fqn] = tbl_profiles
    return profiles

print("Profiling columns...")
column_profiles = _profile_columns()

# ── relationships (from Notebook 3 — naming-convention detection) ─────────────
print("Discovering relationships...")
final_relationships = []
fqn_list = list(table_metadata.keys())
for i_idx in range(len(fqn_list)):
    for j_idx in range(i_idx + 1, len(fqn_list)):
        fqn_a, fqn_b = fqn_list[i_idx], fqn_list[j_idx]
        cols_a = {c['column_name'] for c in table_metadata[fqn_a]['columns']}
        cols_b = {c['column_name'] for c in table_metadata[fqn_b]['columns']}
        shared_ids = [c for c in cols_a & cols_b if c.endswith('_id') or c == 'id']
        for jc in shared_ids:
            final_relationships.append({
                'source_table': fqn_a, 'source_column': jc,
                'target_table': fqn_b, 'target_column': jc,
                'confidence': 0.85, 'methods': ['naming_pattern'],
            })

# ── table_instructions (from Notebook 5) ──────────────────────────────────────
def _generate_table_instructions():
    instructions = {}
    for table_name, metadata in table_metadata.items():
        simple_name = table_name.split('.')[-1]
        row_count = metadata['row_count']
        last_modified = str(metadata['last_modified'])
        desc_parts = [f"The {simple_name} table contains {row_count:,} records as of {last_modified}."]
        if 'order' in simple_name.lower():
            desc_parts.append("This table tracks transaction records including dates, amounts, and customer associations.")
        elif 'customer' in simple_name.lower() or 'account' in simple_name.lower():
            desc_parts.append("This table stores customer/account information including contact details and status.")
        elif 'product' in simple_name.lower():
            desc_parts.append("This table contains product catalog information including names, categories, and pricing.")
        elif 'user' in simple_name.lower():
            desc_parts.append("This table stores user information and access details.")
        elif 'opportunit' in simple_name.lower():
            desc_parts.append("This table contains sales opportunity and pipeline data.")

        key_columns = {}
        profiles = column_profiles.get(table_name, [])
        for col in metadata['columns'][:10]:
            cn = col['column_name']
            col_profile = next((p for p in profiles if p['column'] == cn), None)
            desc = col.get('comment') or f"{cn} column"
            if col_profile and col_profile['recommend_entity_matching']:
                desc += f" (Low cardinality: {col_profile['distinct_count']} unique values)"
            key_columns[cn] = desc

        table_rels = [r for r in final_relationships if table_name in [r.get('source_table'), r.get('target_table')]]
        join_hints = []
        for rel in table_rels[:5]:
            other = rel['target_table'] if rel['source_table'] == table_name else rel['source_table']
            join_hints.append(f"Join to {other.split('.')[-1]} using {rel['source_column']}")

        instructions[table_name] = {
            'description': ' '.join(desc_parts),
            'key_columns': key_columns,
            'join_hints': join_hints,
            'common_use_cases': [],
            'row_count': row_count,
            'last_updated': last_modified,
        }
    return instructions

print("Generating table instructions...")
table_instructions = _generate_table_instructions()

# ── join_instructions (from Notebook 5) ───────────────────────────────────────
join_instructions = []
for rel in final_relationships:
    src_simple = rel['source_table'].split('.')[-1]
    tgt_simple = rel['target_table'].split('.')[-1]
    join_instructions.append({
        'source_table': rel['source_table'],
        'source_column': rel['source_column'],
        'target_table': rel['target_table'],
        'target_column': rel['target_column'],
        'join_definition': f"{rel['source_table']}.{rel['source_column']} = {rel['target_table']}.{rel['target_column']}",
        'recommended_join_type': 'INNER',
        'explanation': f"Join {src_simple} with {tgt_simple} using the {rel['source_column']} relationship.",
        'confidence': rel.get('confidence', 0.0),
        'methods': rel.get('methods', []),
    })

# ── sql_expressions (from Notebook 5) ─────────────────────────────────────────
def _generate_sql_expressions():
    expressions = {'measures': [], 'filters': [], 'dimensions': []}
    for table_name, metadata in table_metadata.items():
        simple_name = table_name.split('.')[-1]
        for col in metadata['columns']:
            cn, ct = col['column_name'], col['data_type'].upper()
            if any(t in ct for t in ['BIGINT', 'INT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'LONG']):
                if any(kw in cn.lower() for kw in ['amount', 'price', 'revenue', 'cost', 'total']):
                    expressions['measures'].append({'name': f"Total {cn.replace('_',' ').title()}", 'expression': f"SUM({table_name}.`{cn}`)", 'description': f"Sum of all {cn.replace('_',' ')} values", 'table': simple_name})
                if cn.endswith('_id') or cn == 'id':
                    entity = cn.replace('_id', '').replace('_', ' ').title()
                    expressions['measures'].append({'name': f"{entity} Count", 'expression': f"COUNT(DISTINCT {table_name}.`{cn}`)", 'description': f"Number of unique {entity.lower()}s", 'table': simple_name})
            if ct in ['DATE', 'TIMESTAMP']:
                expressions['filters'].append({'name': f"Recent {simple_name.title()} (Last 30 Days)", 'expression': f"{table_name}.`{cn}` >= CURRENT_DATE - INTERVAL 30 DAYS", 'description': f"Filter {simple_name} from the last 30 days", 'table': simple_name})
                expressions['dimensions'].append({'name': f"{cn.replace('_',' ').title()} - Year", 'expression': f"YEAR({table_name}.`{cn}`)", 'description': f"Year from {cn}", 'table': simple_name})
                expressions['dimensions'].append({'name': f"{cn.replace('_',' ').title()} - Month", 'expression': f"DATE_TRUNC('MONTH', {table_name}.`{cn}`)", 'description': f"Month from {cn}", 'table': simple_name})
            profiles = column_profiles.get(table_name, [])
            col_profile = next((p for p in profiles if p['column'] == cn), None)
            if col_profile and col_profile['cardinality_category'] == 'very_low' and 'STRING' in ct:
                expressions['filters'].append({'name': f"{cn.replace('_',' ').title()} Filter", 'expression': f"{table_name}.`{cn}` = '{{{{value}}}}'", 'description': f"Filter by specific {cn.replace('_',' ')}", 'table': simple_name})
    return expressions

print("Generating SQL expressions...")
sql_expressions = _generate_sql_expressions()

# ── example_queries (from Notebook 4 — saved JSON) ───────────────────────────
_eq_path = Path(config['output_path'].replace('/dbfs', '')) / "example_queries.json"
if _eq_path.exists():
    with open(_eq_path) as _f:
        example_queries = json.load(_f)
    print(f"  ✅ Loaded {len(example_queries)} example queries from {_eq_path}")
else:
    example_queries = []
    print(f"  ⚠️  No example_queries.json found at {_eq_path} — using empty list")

print(f"\n✅ All prerequisites loaded:")
print(f"   Tables: {len(table_metadata)} | Instructions: {len(table_instructions)}")
print(f"   Relationships: {len(final_relationships)} | Join instructions: {len(join_instructions)}")
print(f"   Measures: {len(sql_expressions['measures'])} | Filters: {len(sql_expressions['filters'])} | Dimensions: {len(sql_expressions['dimensions'])}")
print(f"   Example queries: {len(example_queries)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6.1: Assemble Knowledge Store

# COMMAND ----------

# DBTITLE 1,Assemble Knowledge Store
def assemble_knowledge_store(config, table_metadata, table_instructions, join_instructions, sql_expressions, example_queries):
    """Assemble all components into Genie-compatible knowledge store."""
    knowledge_store = {
        'space_name': f"{config['schema'].replace('_', ' ').title()} Analytics Space",
        'space_description': f"Auto-generated Genie space for {config['catalog']}.{config['schema']} schema",
        'catalog': config['catalog'],
        'schema': config['schema'],
        'warehouse_id': config['genie_warehouse_id'],
        'created_at': datetime.now().isoformat(),
        'configuration': {
            'lookback_days': config['lookback_days'],
            'confidence_threshold': config['confidence_threshold'],
            'tables_included': config['tables'],
        },
        'tables': [],
        'joins': [],
        'sql_expressions': {'measures': [], 'filters': [], 'dimensions': []},
        'example_queries': [],
        'global_instructions': "",
    }

    for table_name, instruction in table_instructions.items():
        simple_name = table_name.split('.')[-1]
        metadata = table_metadata[table_name]
        table_entry = {
            'catalog': config['catalog'],
            'schema': config['schema'],
            'table_name': simple_name,
            'full_name': table_name,
            'description': instruction['description'],
            'row_count': instruction['row_count'],
            'last_updated': instruction['last_updated'],
            'columns': [{'name': c['column_name'], 'type': c['data_type'], 'nullable': c['is_nullable'], 'comment': c.get('comment', '')} for c in metadata['columns']],
            'key_columns': instruction['key_columns'],
            'join_hints': instruction['join_hints'],
            'common_use_cases': instruction['common_use_cases'],
        }
        knowledge_store['tables'].append(table_entry)

    for ji in join_instructions:
        knowledge_store['joins'].append({
            'source_table': ji['source_table'],
            'source_column': ji['source_column'],
            'target_table': ji['target_table'],
            'target_column': ji['target_column'],
            'join_type': ji['recommended_join_type'],
            'explanation': ji['explanation'],
            'confidence': ji['confidence'],
        })

    knowledge_store['sql_expressions']['measures'] = sql_expressions['measures']
    knowledge_store['sql_expressions']['filters'] = sql_expressions['filters']
    knowledge_store['sql_expressions']['dimensions'] = sql_expressions['dimensions']

    for ex in example_queries:
        knowledge_store['example_queries'].append({
            'natural_language': ex['natural_language'],
            'sql': ex['sql'],
            'frequency': ex['frequency'],
            'avg_execution_time_ms': ex['avg_execution_time_ms'],
            'tables_used': ex['tables_used'],
        })

    table_list = "\n".join([f"• {t['table_name']}: {t['description'][:100]}..." for t in knowledge_store['tables']])
    example_list = "\n".join([f"• {eq['natural_language']}" for eq in example_queries[:5]])

    knowledge_store['global_instructions'] = f"""
This Genie space provides natural language access to the {config['schema']} dataset.

AVAILABLE TABLES:
{table_list}

COMMON QUERIES:
{example_list}

TIPS FOR BEST RESULTS:
• Be specific about date ranges (e.g., "last 30 days", "Q4 2025")
• Specify what metrics you want (e.g., "total revenue", "count of orders")
• Use table names when ambiguous (e.g., "customer name from customers table")
• Filter data to improve performance (avoid querying all historical data)

This space was automatically generated on {datetime.now().strftime('%Y-%m-%d %H:%M')} using Auto-Genie MVP.
"""
    return knowledge_store

print("=" * 60)
print("ASSEMBLING KNOWLEDGE STORE")
print("=" * 60)
knowledge_store = assemble_knowledge_store(
    config, table_metadata, table_instructions,
    join_instructions, sql_expressions, example_queries
)
print(f"✅ Knowledge store assembled successfully\n")

print("📊 KNOWLEDGE STORE SUMMARY:")
print(f"  Space Name: {knowledge_store['space_name']}")
print(f"  Catalog.Schema: {knowledge_store['catalog']}.{knowledge_store['schema']}")
print(f"  Tables: {len(knowledge_store['tables'])}")
print(f"  Joins: {len(knowledge_store['joins'])}")
print(f"  Measures: {len(knowledge_store['sql_expressions']['measures'])}")
print(f"  Filters: {len(knowledge_store['sql_expressions']['filters'])}")
print(f"  Dimensions: {len(knowledge_store['sql_expressions']['dimensions'])}")
print(f"  Example Queries: {len(knowledge_store['example_queries'])}")
print(f"  Created: {knowledge_store['created_at']}")

output_dir = Path(config['output_path'].replace('/dbfs', ''))
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "knowledge_store.json"
with open(output_file, 'w') as f:
    json.dump(knowledge_store, f, indent=2, default=str)
print(f"\n💾 Saved to: {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6.2: Validate Knowledge Store Structure

# COMMAND ----------

# DBTITLE 1,Validate Knowledge Store Structure
def validate_knowledge_store_structure(knowledge_store):
    """Validate the structure and completeness of the knowledge store."""
    results = {'status': 'passed', 'checks': [], 'warnings': [], 'errors': []}

    for field in ['space_name', 'catalog', 'schema', 'tables', 'joins']:
        if field not in knowledge_store or not knowledge_store[field]:
            results['errors'].append(f"Missing required field: {field}")
            results['status'] = 'failed'
        else:
            results['checks'].append({'check': f'required_field_{field}', 'status': 'passed', 'details': f'Field {field} is present'})

    if len(knowledge_store['tables']) == 0:
        results['errors'].append("No tables defined")
        results['status'] = 'failed'
    elif len(knowledge_store['tables']) < 3:
        results['warnings'].append(f"Only {len(knowledge_store['tables'])} tables — may limit usefulness")
    results['checks'].append({'check': 'table_count', 'status': 'passed', 'details': f"{len(knowledge_store['tables'])} tables defined"})

    no_desc = [t['table_name'] for t in knowledge_store['tables'] if not t.get('description')]
    if no_desc:
        results['warnings'].append(f"Tables without descriptions: {', '.join(no_desc)}")
    results['checks'].append({'check': 'table_descriptions', 'status': 'passed' if not no_desc else 'warning', 'details': f"{len(knowledge_store['tables']) - len(no_desc)}/{len(knowledge_store['tables'])} tables have descriptions"})

    results['checks'].append({'check': 'join_definitions', 'status': 'passed', 'details': f"{len(knowledge_store['joins'])} join relationships defined"})
    if not knowledge_store['joins']:
        results['warnings'].append("No joins defined — users cannot query across tables")

    total_expr = sum(len(knowledge_store['sql_expressions'][k]) for k in ['measures', 'filters', 'dimensions'])
    results['checks'].append({'check': 'sql_expressions', 'status': 'passed', 'details': f'{total_expr} SQL expressions defined'})

    eq_count = len(knowledge_store.get('example_queries', []))
    if eq_count == 0:
        results['warnings'].append("No example queries — Genie may have lower accuracy")
    elif eq_count < 5:
        results['warnings'].append(f"Only {eq_count} example queries — recommend 10+")
    results['checks'].append({'check': 'example_queries', 'status': 'passed', 'details': f'{eq_count} example queries defined'})

    gi_len = len(knowledge_store.get('global_instructions', ''))
    if gi_len < 100:
        results['warnings'].append("Global instructions are very brief")
    results['checks'].append({'check': 'global_instructions', 'status': 'passed', 'details': f'Global instructions: {gi_len} characters'})

    return results

print("=" * 60)
print("VALIDATING KNOWLEDGE STORE STRUCTURE")
print("=" * 60)
validation_results = validate_knowledge_store_structure(knowledge_store)

print(f"\n{'=' * 70}")
print(f"VALIDATION RESULTS: {validation_results['status'].upper()}")
print(f"{'=' * 70}\n")

print(f"✅ PASSED CHECKS ({len([c for c in validation_results['checks'] if c['status'] == 'passed'])}):")
for check in validation_results['checks']:
    if check['status'] == 'passed':
        print(f"  • {check['check']}: {check['details']}")

if validation_results['warnings']:
    print(f"\n⚠️  WARNINGS ({len(validation_results['warnings'])}):")
    for w in validation_results['warnings']:
        print(f"  • {w}")

if validation_results['errors']:
    print(f"\n❌ ERRORS ({len(validation_results['errors'])}):")
    for e in validation_results['errors']:
        print(f"  • {e}")

print(f"\n{'=' * 70}")
print(f"Overall Status: {'✅ READY FOR DEPLOYMENT' if validation_results['status'] == 'passed' else '❌ REQUIRES FIXES'}")
print(f"{'=' * 70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6.3: Validate SQL Syntax

# COMMAND ----------

# DBTITLE 1,Validate SQL Syntax
def validate_sql_syntax(knowledge_store):
    """Validate SQL syntax for joins, expressions, and example queries."""
    results = {k: {'passed': 0, 'failed': 0, 'errors': []} for k in ['joins', 'measures', 'filters', 'dimensions', 'example_queries']}

    print("Validating SQL syntax...\n")

    print("Checking joins...", end=" ")
    for j in knowledge_store['joins']:
        try:
            sqlglot.parse_one(f"SELECT * FROM {j['source_table']} {j['join_type']} JOIN {j['target_table']} ON {j['source_table']}.{j['source_column']} = {j['target_table']}.{j['target_column']} LIMIT 1", read='databricks')
            results['joins']['passed'] += 1
        except Exception as e:
            results['joins']['failed'] += 1
            results['joins']['errors'].append({'join': f"{j['source_table']}.{j['source_column']}", 'error': str(e)})
    total_j = results['joins']['passed'] + results['joins']['failed']
    print(f"✅ {results['joins']['passed']}/{total_j}")

    cat_schema = f"{knowledge_store['catalog']}.{knowledge_store['schema']}"
    print("Checking measures...", end=" ")
    for m in knowledge_store['sql_expressions']['measures']:
        try:
            sqlglot.parse_one(f"SELECT {m['expression']} as r FROM {cat_schema}.{m['table']} LIMIT 1", read='databricks')
            results['measures']['passed'] += 1
        except Exception:
            results['measures']['failed'] += 1
    total_m = results['measures']['passed'] + results['measures']['failed']
    print(f"✅ {results['measures']['passed']}/{total_m}")

    print("Checking filters...", end=" ")
    for flt in knowledge_store['sql_expressions']['filters']:
        try:
            expr = flt['expression'].replace('{value}', 'test').replace('{{value}}', 'test')
            sqlglot.parse_one(f"SELECT * FROM {cat_schema}.{flt['table']} WHERE {expr} LIMIT 1", read='databricks')
            results['filters']['passed'] += 1
        except Exception:
            results['filters']['failed'] += 1
    total_f = results['filters']['passed'] + results['filters']['failed']
    print(f"✅ {results['filters']['passed']}/{total_f}")

    print("Checking dimensions...", end=" ")
    for d in knowledge_store['sql_expressions']['dimensions']:
        try:
            sqlglot.parse_one(f"SELECT {d['expression']} as r FROM {cat_schema}.{d['table']} LIMIT 1", read='databricks')
            results['dimensions']['passed'] += 1
        except Exception:
            results['dimensions']['failed'] += 1
    total_d = results['dimensions']['passed'] + results['dimensions']['failed']
    print(f"✅ {results['dimensions']['passed']}/{total_d}")

    print("Checking example queries...", end=" ")
    for eq in knowledge_store['example_queries']:
        try:
            sqlglot.parse_one(eq['sql'], read='databricks')
            results['example_queries']['passed'] += 1
        except Exception:
            results['example_queries']['failed'] += 1
    total_eq = results['example_queries']['passed'] + results['example_queries']['failed']
    print(f"✅ {results['example_queries']['passed']}/{total_eq}")

    return results

print("=" * 60)
print("VALIDATING SQL SYNTAX")
print("=" * 60)
print()
sql_validation = validate_sql_syntax(knowledge_store)

total_passed = sum(v['passed'] for v in sql_validation.values())
total_failed = sum(v['failed'] for v in sql_validation.values())
print(f"\n{'=' * 70}")
print("SQL VALIDATION SUMMARY")
print(f"{'=' * 70}")
print(f"\n📊 Overall: {total_passed} passed, {total_failed} failed")
for k, v in sql_validation.items():
    print(f"  • {k.replace('_', ' ').title()}: {v['passed']}/{v['passed'] + v['failed']}")

if total_failed > 0:
    print(f"\n❌ Found {total_failed} SQL syntax errors — review and fix before deployment")
else:
    print(f"\n✅ All SQL syntax validation passed!")
