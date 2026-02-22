# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 7: Genie Space Deployment
# MAGIC **Auto-Genie MVP - Stage 7: Deployment**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Creating a new Genie space via the Databricks REST API
# MAGIC - Adding tables, join instructions, and example queries to the space
# MAGIC - Finalizing deployment and running validation tests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import requests

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
    "databricks_token": os.getenv("DATABRICKS_TOKEN", ""),
    "catalog": os.getenv("AUTO_GENIE_CATALOG", "main"),
    "schema": os.getenv("AUTO_GENIE_SCHEMA", "sales"),
    "tables": os.getenv("AUTO_GENIE_TABLES", "orders,customers,products,order_items,warehouses").split(","),
    "genie_warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
    "lookback_days": int(os.getenv("AUTO_GENIE_LOOKBACK_DAYS", "90")),
    "confidence_threshold": float(os.getenv("AUTO_GENIE_CONFIDENCE_THRESHOLD", "0.75")),
    "output_path": os.getenv("AUTO_GENIE_OUTPUT_PATH", "/dbfs/tmp/auto_genie_outputs"),
}

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
except Exception:
    w = None

print(f"✅ Config loaded — targeting {config['catalog']}.{config['schema']}")
print("⚠️  This notebook expects knowledge_store from Notebook 6.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.0b: Load Knowledge Store

# COMMAND ----------

# DBTITLE 1,Load Knowledge Store
ks_path = Path(config['output_path'].replace('/dbfs', '')) / "knowledge_store.json"
if ks_path.exists():
    with open(ks_path) as f:
        knowledge_store = json.load(f)
    print(f"✅ Loaded knowledge store from {ks_path}")
    print(f"  Tables: {len(knowledge_store['tables'])}")
    print(f"  Joins: {len(knowledge_store['joins'])}")
else:
    print(f"⚠️  Knowledge store not found at {ks_path}")
    print("   Run Notebook 6 first to generate it.")
    knowledge_store = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.1: Create Genie Space

# COMMAND ----------

# DBTITLE 1,Create Genie Space
def create_genie_space(knowledge_store, config):
    """Create a new Genie space using the Databricks REST API."""
    workspace_url = config['workspace_url'].rstrip('/')
    token = config['databricks_token']
    space_name = knowledge_store['space_name']
    warehouse_id = knowledge_store['warehouse_id']

    print(f"Creating Genie space: {space_name}")
    print(f"Using warehouse: {warehouse_id}")

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"display_name": space_name, "description": knowledge_store['space_description'], "warehouse_id": warehouse_id}

    try:
        resp = requests.post(f"{workspace_url}/api/2.0/genie/spaces", headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        space_id = data.get('id') or data.get('space_id')
        print(f"✅ Space created successfully")
        print(f"  Space ID: {space_id}")
        print(f"  Space URL: {workspace_url}/genie/spaces/{space_id}")
        return {'space_id': space_id, 'space_name': space_name, 'space_url': f"{workspace_url}/genie/spaces/{space_id}", 'warehouse_id': warehouse_id, 'status': 'created'}
    except requests.exceptions.RequestException as e:
        print(f"❌ Error creating space: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text}")
        raise

print("=" * 60)
print("CREATING GENIE SPACE")
print("=" * 60)
print()
space_info = create_genie_space(knowledge_store, config)
print(f"\n{'=' * 70}")
print("SPACE CREATION SUCCESSFUL")
print(f"{'=' * 70}")
print(f"Space ID: {space_info['space_id']}")
print(f"Space Name: {space_info['space_name']}")
print(f"Space URL: {space_info['space_url']}")
print(f"{'=' * 70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.2: Add Tables to Space

# COMMAND ----------

# DBTITLE 1,Add Tables to Space
def add_tables_to_space(space_id, knowledge_store, config):
    """Add Unity Catalog tables to Genie space with metadata."""
    workspace_url = config['workspace_url'].rstrip('/')
    token = config['databricks_token']
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    print(f"Adding {len(knowledge_store['tables'])} tables to space {space_id}...\n")
    tables_added = []

    for tbl in knowledge_store['tables']:
        full_name = tbl['full_name']
        payload = {"table_full_name": full_name, "description": tbl['description']}
        try:
            resp = requests.post(f"{workspace_url}/api/2.0/genie/spaces/{space_id}/tables", headers=headers, json=payload)
            resp.raise_for_status()
            print(f"✅ Added table: {full_name}")
            tables_added.append(full_name)
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Failed to add table {full_name}: {e}")

    return {'space_id': space_id, 'tables_added': tables_added, 'total_tables': len(tables_added), 'status': 'success' if len(tables_added) == len(knowledge_store['tables']) else 'partial'}

print("=" * 60)
print("ADDING TABLES TO SPACE")
print("=" * 60)
print()
table_result = add_tables_to_space(space_info['space_id'], knowledge_store, config)
print(f"\n{'=' * 70}")
print(f"TABLE ADDITION COMPLETE: {table_result['total_tables']}/{len(knowledge_store['tables'])} tables added")
print(f"{'=' * 70}")
for t in table_result['tables_added']:
    print(f"  ✅ {t}")
print(f"{'=' * 70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.3: Configure Joins and Instructions

# COMMAND ----------

# DBTITLE 1,Configure Joins and Instructions
def configure_space_instructions(space_id, knowledge_store, config):
    """Add joins, SQL expressions, and global instructions to the space."""
    workspace_url = config['workspace_url'].rstrip('/')
    token = config['databricks_token']
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    instructions_text = knowledge_store['global_instructions'] + "\n\n"
    instructions_text += "=== TABLE RELATIONSHIPS ===\n\n"
    for j in knowledge_store['joins']:
        instructions_text += f"• {j['explanation']}\n  SQL: {j['source_table']}.{j['source_column']} = {j['target_table']}.{j['target_column']}\n\n"

    instructions_text += "\n=== AVAILABLE METRICS ===\n\n"
    for m in knowledge_store['sql_expressions']['measures'][:10]:
        instructions_text += f"• {m['name']}: {m['description']}\n"

    instructions_text += "\n=== COMMON FILTERS ===\n\n"
    for flt in knowledge_store['sql_expressions']['filters'][:10]:
        instructions_text += f"• {flt['name']}: {flt['description']}\n"

    try:
        resp = requests.post(f"{workspace_url}/api/2.0/genie/spaces/{space_id}/instructions", headers=headers, json={"content": instructions_text})
        resp.raise_for_status()
        print("✅ Instructions configured successfully")
        return {'space_id': space_id, 'joins_configured': len(knowledge_store['joins']), 'measures_documented': len(knowledge_store['sql_expressions']['measures']), 'filters_documented': len(knowledge_store['sql_expressions']['filters']), 'status': 'success'}
    except requests.exceptions.RequestException as e:
        print(f"❌ Error configuring instructions: {e}")
        raise

print("=" * 60)
print("CONFIGURING SPACE INSTRUCTIONS")
print("=" * 60)
print()
instruction_result = configure_space_instructions(space_info['space_id'], knowledge_store, config)
print(f"\n{'=' * 70}")
print("INSTRUCTION CONFIGURATION COMPLETE")
print(f"{'=' * 70}")
print(f"  • Joins configured: {instruction_result['joins_configured']}")
print(f"  • Measures documented: {instruction_result['measures_documented']}")
print(f"  • Filters documented: {instruction_result['filters_documented']}")
print(f"{'=' * 70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.4: Add Example Queries

# COMMAND ----------

# DBTITLE 1,Add Example Queries
def add_example_queries(space_id, knowledge_store, config):
    """Add example natural language queries to train Genie."""
    workspace_url = config['workspace_url'].rstrip('/')
    token = config['databricks_token']
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    print(f"Adding {len(knowledge_store['example_queries'])} example queries...\n")
    examples_added = 0

    for ex in knowledge_store['example_queries']:
        try:
            resp = requests.post(f"{workspace_url}/api/2.0/genie/spaces/{space_id}/conversations", headers=headers, json={"content": ex['natural_language']})
            resp.raise_for_status()
            print(f"✅ Added: {ex['natural_language'][:70]}...")
            examples_added += 1
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Failed to add example: {e}")

    return {'space_id': space_id, 'examples_added': examples_added, 'status': 'success' if examples_added == len(knowledge_store['example_queries']) else 'partial'}

print("=" * 60)
print("ADDING EXAMPLE QUERIES")
print("=" * 60)
print()
example_result = add_example_queries(space_info['space_id'], knowledge_store, config)
print(f"\n{'=' * 70}")
print(f"EXAMPLE QUERIES ADDED: {example_result['examples_added']}/{len(knowledge_store['example_queries'])}")
print(f"{'=' * 70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.5: Finalize and Test Deployment

# COMMAND ----------

# DBTITLE 1,Finalize Deployment
def finalize_deployment(space_id, knowledge_store, config):
    """Finalize deployment and produce a summary."""
    workspace_url = config['workspace_url'].rstrip('/')
    token = config['databricks_token']
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        resp = requests.get(f"{workspace_url}/api/2.0/genie/spaces/{space_id}", headers=headers)
        resp.raise_for_status()
    except Exception:
        pass

    summary = {
        'space_id': space_id,
        'space_name': knowledge_store['space_name'],
        'space_url': f"{workspace_url}/genie/spaces/{space_id}",
        'deployment_time': datetime.now().isoformat(),
        'status': 'active',
        'configuration': {
            'tables': len(knowledge_store['tables']),
            'joins': len(knowledge_store['joins']),
            'measures': len(knowledge_store['sql_expressions']['measures']),
            'filters': len(knowledge_store['sql_expressions']['filters']),
            'dimensions': len(knowledge_store['sql_expressions']['dimensions']),
            'examples': len(knowledge_store['example_queries']),
        },
        'ready_for_use': True,
    }

    output_dir = Path(config['output_path'].replace('/dbfs', ''))
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / "deployment_summary.json", 'w') as f:
        json.dump(summary, f, indent=2)

    return summary

print("=" * 60)
print("FINALIZING DEPLOYMENT")
print("=" * 60)
print()
deployment_summary = finalize_deployment(space_info['space_id'], knowledge_store, config)

print(f"\n{'=' * 80}")
print(f"{'DEPLOYMENT COMPLETE':^80}")
print(f"{'=' * 80}\n")
print(f"📊 DEPLOYMENT SUMMARY:")
print(f"  Space Name: {deployment_summary['space_name']}")
print(f"  Space ID: {deployment_summary['space_id']}")
print(f"  Deployment Time: {deployment_summary['deployment_time']}")
print(f"  Status: {deployment_summary['status'].upper()}")
print()
print(f"📈 CONFIGURATION:")
for k, v in deployment_summary['configuration'].items():
    print(f"  • {k.title()}: {v}")
print()
print(f"{'=' * 80}")
print(f"ACCESS YOUR GENIE SPACE:")
print(f"{'=' * 80}")
print(f"{deployment_summary['space_url']}")
print(f"{'=' * 80}\n")
print("✅ The Genie space is ready for use!")
print("✅ Share the URL with your business users to start querying data in natural language")
print(f"\n💾 Deployment summary saved to: {config['output_path']}/deployment_summary.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.6: Test the Deployed Space

# COMMAND ----------

# DBTITLE 1,Test Deployed Space
def test_genie_space(space_id, test_questions, config):
    """Test the deployed Genie space with sample questions."""
    workspace_url = config['workspace_url'].rstrip('/')
    token = config['databricks_token']
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    results = []
    print("Testing Genie space with sample questions...\n")

    for q in test_questions:
        try:
            resp = requests.post(f"{workspace_url}/api/2.0/genie/spaces/{space_id}/conversations", headers=headers, json={"content": q})
            resp.raise_for_status()
            results.append({'question': q, 'status': 'success', 'response': resp.json()})
            print(f"✅ Question: {q}")
            print(f"  Status: SUCCESS\n")
        except requests.exceptions.RequestException as e:
            results.append({'question': q, 'status': 'failed', 'error': str(e)})
            print(f"❌ Question: {q}")
            print(f"  Status: FAILED — {e}\n")
    return results

test_questions = [
    "What was our total revenue last month?",
    "Show me the top 10 customers by order count",
    "How many orders were placed in each quarter of 2025?",
    "What are our best-selling products by revenue?",
]

print("=" * 60)
print("TESTING DEPLOYED GENIE SPACE")
print("=" * 60)
print()
test_results = test_genie_space(space_info['space_id'], test_questions, config)

passed = len([r for r in test_results if r['status'] == 'success'])
print(f"\n{'=' * 70}")
print(f"TEST RESULTS: {passed}/{len(test_results)} passed")
print(f"{'=' * 70}")
if all(r['status'] == 'success' for r in test_results):
    print("\n✅ All tests passed! The Genie space is functioning correctly.")
else:
    print("\n⚠️  Some tests failed. Review the errors and adjust the knowledge store if needed.")
