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
print("⚠️  This notebook expects table_instructions, join_instructions, sql_expressions,")
print("   example_queries, and table_metadata to be available from prior notebooks.")

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
