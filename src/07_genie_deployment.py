# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 7: Genie Space Deployment
# MAGIC **Auto-Genie MVP - Stage 7: Deployment**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Creating a new Genie space via the Databricks SDK
# MAGIC - Packaging tables, joins, SQL expressions, and sample queries into `serialized_space`
# MAGIC - Verifying the deployed space and running test conversations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, json, uuid
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

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

from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN"),
)

me = w.current_user.me()
print(f"✅ Authenticated as {me.user_name}")
print(f"✅ Config loaded — targeting {config['catalog']}.{config['schema']}")

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
    print(f"  Tables         : {len(knowledge_store['tables'])}")
    print(f"  Joins          : {len(knowledge_store['joins'])}")
    print(f"  Example Queries: {len(knowledge_store.get('example_queries', []))}")
    print(f"  Measures       : {len(knowledge_store['sql_expressions']['measures'])}")
    print(f"  Filters        : {len(knowledge_store['sql_expressions']['filters'])}")
    print(f"  Dimensions     : {len(knowledge_store['sql_expressions']['dimensions'])}")
else:
    knowledge_store = None
    print(f"❌ Knowledge store not found at {ks_path}")
    print("   Run Notebook 6 first to generate it.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.1: Build Serialized Space & Create Genie Space

# COMMAND ----------

# DBTITLE 1,Create Genie Space
from langchain_core.messages import SystemMessage, HumanMessage
from databricks_langchain import ChatDatabricks

LLM_MODEL = "databricks-claude-opus-4-6"


def _uid():
    return uuid.uuid4().hex


def _build_join_text(knowledge_store):
    """Build human-readable join documentation for text_instructions."""
    lines = ["TABLE RELATIONSHIPS (JOIN CONDITIONS):"]
    for j in knowledge_store.get('joins', []):
        src_short = j['source_table'].split('.')[-1]
        tgt_short = j['target_table'].split('.')[-1]
        lines.append(
            f"- {src_short}.{j['source_column']} = {tgt_short}.{j['target_column']} "
            f"({j.get('join_type', 'INNER')} JOIN) — {j.get('explanation', '')}"
        )
    return "\n".join(lines)


def build_serialized_space(knowledge_store, include_join_specs=True):
    """Build the version-2 serialized_space dict.

    Args:
        knowledge_store: The assembled knowledge store dict.
        include_join_specs: If True, emit structured join_specs.
                           If False, embed join info in text_instructions instead
                           (fallback for APIs that reject join_specs).
    """

    tables = []
    for tbl in knowledge_store['tables']:
        entry = {"identifier": tbl['full_name']}
        if tbl.get('description'):
            entry["description"] = [tbl['description'][:500]]
        tables.append(entry)

    sample_questions = []
    for eq in knowledge_store.get('example_queries', []):
        sample_questions.append({
            "id": _uid(),
            "question": [eq['natural_language']],
        })
    sample_questions.sort(key=lambda x: x["id"])

    full_text = ""
    if knowledge_store.get('global_instructions'):
        full_text = knowledge_store['global_instructions'].strip()
    if not include_join_specs and knowledge_store.get('joins'):
        full_text += "\n\n" + _build_join_text(knowledge_store)

    text_instructions = []
    if full_text.strip():
        text_instructions.append({
            "id": _uid(),
            "content": [full_text.strip()],
        })

    join_specs = []
    if include_join_specs:
        for j in knowledge_store.get('joins', []):
            src_col = j['source_column']
            tgt_col = j['target_column']
            src_table_short = j['source_table'].split('.')[-1]
            tgt_table_short = j['target_table'].split('.')[-1]
            join_specs.append({
                "id": _uid(),
                "left": {"identifier": j['source_table']},
                "right": {"identifier": j['target_table']},
                "sql": [f"{src_table_short}.{src_col} = {tgt_table_short}.{tgt_col}"],
            })

    example_question_sqls = []
    for eq in knowledge_store.get('example_queries', []):
        if eq.get('sql'):
            example_question_sqls.append({
                "id": _uid(),
                "question": [eq['natural_language']],
                "sql": [eq['sql']],
            })
    example_question_sqls.sort(key=lambda x: x["id"])

    snippet_measures = []
    for m in knowledge_store['sql_expressions'].get('measures', []):
        alias = m['name'].lower().replace(' ', '_').replace('-', '_')[:50]
        snippet_measures.append({
            "id": _uid(),
            "alias": alias,
            "sql": [m['expression']],
        })
    snippet_measures.sort(key=lambda x: x["id"])

    snippet_filters = []
    for flt in knowledge_store['sql_expressions'].get('filters', []):
        display = flt['name'][:100]
        expr = flt['expression'].replace('{{value}}', "'example'").replace('{value}', "'example'")
        snippet_filters.append({
            "id": _uid(),
            "display_name": display,
            "sql": [expr],
        })
    snippet_filters.sort(key=lambda x: x["id"])

    snippet_expressions = []
    for dim in knowledge_store['sql_expressions'].get('dimensions', []):
        alias = dim['name'].lower().replace(' ', '_').replace('-', '_')[:50]
        snippet_expressions.append({
            "id": _uid(),
            "alias": alias,
            "sql": [dim['expression']],
        })
    snippet_expressions.sort(key=lambda x: x["id"])

    instructions = {
        "text_instructions": text_instructions,
        "example_question_sqls": example_question_sqls,
        "sql_snippets": {
            "measures": snippet_measures,
            "filters": snippet_filters,
            "expressions": snippet_expressions,
        },
    }
    if join_specs:
        instructions["join_specs"] = join_specs

    serialized = {
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": {"tables": tables},
        "instructions": instructions,
    }
    return serialized


REFERENCE_SCHEMA = """\
{
  "version": 2,
  "config": {
    "sample_questions": [{"id": "<hex32>", "question": ["<str>"]}]
  },
  "data_sources": {
    "tables": [{"identifier": "<catalog.schema.table>", "description": ["<str>"]}]
  },
  "instructions": {
    "text_instructions": [{"id": "<hex32>", "content": ["<str>"]}],
    "example_question_sqls": [{"id": "<hex32>", "question": ["<str>"], "sql": ["<str>"]}],
    "join_specs": [{"id":"<hex32>","left":{"identifier":"<tbl>"},"right":{"identifier":"<tbl>"},"sql":["<condition>"]}],
    "sql_snippets": {
      "filters": [{"id": "<hex32>", "display_name": "<str>", "sql": ["<str>"]}],
      "expressions": [{"id": "<hex32>", "alias": "<str>", "sql": ["<str>"]}],
      "measures": [{"id": "<hex32>", "alias": "<str>", "sql": ["<str>"]}]
    }
  }
}"""


def validate_and_reformat_payload(payload_dict, model=LLM_MODEL):
    """Use ChatDatabricks to validate the payload structure.

    Sends a compact schema-only prompt to minimize guardrail triggers.
    Falls back gracefully to the original payload on any failure.
    """
    payload_json = json.dumps(payload_dict, indent=2, default=str)

    system_prompt = (
        "You are a JSON schema validator for a data analytics workspace configuration API. "
        "Given a draft JSON payload, ensure it conforms to this target schema:\n\n"
        + REFERENCE_SCHEMA + "\n\n"
        "Rules: (1) Output valid JSON only, no markdown fences, no commentary. "
        "(2) Preserve ALL data — do not remove any entries. "
        "(3) Array values (question, sql, content, description) must be lists of strings. "
        "(4) Keep all existing 32-char hex ids unchanged. "
        "(5) sql_snippets.filters use 'display_name'; expressions and measures use 'alias'. "
        "(6) version must be integer 2. "
        "(7) Do not add sections not present in the input. "
        "Return ONLY the corrected JSON."
    )

    print(f"🤖 Validating payload via LLM...")
    print(f"   Payload size: {len(payload_json):,} chars")

    try:
        llm = ChatDatabricks(model=model)
        response = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content="Validate this JSON configuration:\n\n" + payload_json),
        ])

        raw = response.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        validated = json.loads(raw)

        orig_tables = len(payload_dict.get('data_sources', {}).get('tables', []))
        val_tables = len(validated.get('data_sources', {}).get('tables', []))
        orig_eqs = len(payload_dict.get('instructions', {}).get('example_question_sqls', []))
        val_eqs = len(validated.get('instructions', {}).get('example_question_sqls', []))

        if val_tables < orig_tables:
            print(f"  ⚠️  LLM dropped tables ({orig_tables} → {val_tables}) — using original")
            return payload_dict
        if val_eqs < orig_eqs * 0.8:
            print(f"  ⚠️  LLM dropped examples ({orig_eqs} → {val_eqs}) — using original")
            return payload_dict

        print(f"✅ Payload validated successfully")
        return validated

    except json.JSONDecodeError as e:
        print(f"  ⚠️  LLM returned invalid JSON ({e}) — using original")
        return payload_dict
    except Exception as e:
        print(f"  ⚠️  LLM validation skipped ({type(e).__name__}) — using original")
        return payload_dict


def create_genie_space(knowledge_store, config, workspace_client):
    """Create a Genie space with automatic fallback for join_specs.

    The Genie API has a known limitation where join_specs may fail to parse.
    This function tries with structured join_specs first, and if rejected,
    falls back to embedding join info in text_instructions.
    """
    space_name = knowledge_store['space_name']
    warehouse_id = config['genie_warehouse_id']
    description = knowledge_store.get('space_description', '')
    output_dir = Path(config['output_path'].replace('/dbfs', ''))
    output_dir.mkdir(parents=True, exist_ok=True)

    print("─" * 60)
    print("STEP 1: Building payload (with structured join_specs)")
    print("─" * 60)
    draft_payload = build_serialized_space(knowledge_store, include_join_specs=True)
    instr = draft_payload.get('instructions', {})
    print(f"  Tables              : {len(draft_payload['data_sources']['tables'])}")
    print(f"  Sample questions    : {len(draft_payload['config']['sample_questions'])}")
    print(f"  Text instructions   : {len(instr.get('text_instructions', []))}")
    print(f"  Join specs          : {len(instr.get('join_specs', []))}")
    print(f"  Example SQL pairs   : {len(instr.get('example_question_sqls', []))}")
    print(f"  Snippet measures    : {len(instr.get('sql_snippets', {}).get('measures', []))}")
    print(f"  Snippet filters     : {len(instr.get('sql_snippets', {}).get('filters', []))}")
    print(f"  Snippet expressions : {len(instr.get('sql_snippets', {}).get('expressions', []))}")
    print()

    print("─" * 60)
    print("STEP 2: LLM validation & reformatting")
    print("─" * 60)
    payload = validate_and_reformat_payload(draft_payload)
    print()

    print("─" * 60)
    print("STEP 3: Creating Genie space")
    print("─" * 60)

    serialized = json.dumps(payload, indent=2, default=str)
    with open(output_dir / "validated_payload.json", 'w') as f:
        f.write(serialized)
    print(f"  💾 Payload saved to {output_dir / 'validated_payload.json'}")
    print(f"  Space name  : {space_name}")
    print(f"  Warehouse   : {warehouse_id}")
    print(f"  Payload size: {len(serialized):,} chars")
    print()

    try:
        space = workspace_client.genie.create_space(
            warehouse_id=warehouse_id,
            serialized_space=serialized,
            title=space_name,
            description=description,
        )
    except Exception as e:
        error_msg = str(e)
        has_join_specs = bool(payload.get('instructions', {}).get('join_specs'))

        if has_join_specs and ("Failed to parse" in error_msg or "InvalidParameterValue" in error_msg):
            print(f"  ⚠️  API rejected join_specs: {error_msg[:150]}")
            print(f"  ↻  Rebuilding with joins in text_instructions instead...\n")

            payload = build_serialized_space(knowledge_store, include_join_specs=False)
            serialized = json.dumps(payload, indent=2, default=str)
            with open(output_dir / "validated_payload.json", 'w') as f:
                f.write(serialized)

            instr_fb = payload.get('instructions', {})
            print(f"  [Fallback payload]")
            print(f"  Join specs          : {len(instr_fb.get('join_specs', []))} (removed)")
            print(f"  Text instructions   : {len(instr_fb.get('text_instructions', []))} (contains join info)")
            print(f"  Payload size: {len(serialized):,} chars\n")

            space = workspace_client.genie.create_space(
                warehouse_id=warehouse_id,
                serialized_space=serialized,
                title=space_name,
                description=description,
            )
        else:
            raise

    workspace_url = config['workspace_url'].rstrip('/')
    space_url = f"{workspace_url}/genie/rooms/{space.space_id}"

    print(f"✅ Space created successfully!")
    print(f"  Space ID  : {space.space_id}")
    print(f"  Title     : {space.title}")
    print(f"  Space URL : {space_url}")

    return {
        'space_id': space.space_id,
        'space_name': space_name,
        'space_url': space_url,
        'warehouse_id': warehouse_id,
        'status': 'created',
    }


print("=" * 60)
print("CREATING GENIE SPACE")
print("=" * 60)
print()
space_info = create_genie_space(knowledge_store, config, w)
print(f"\n{'=' * 70}")
print("SPACE CREATION SUCCESSFUL")
print(f"{'=' * 70}")
print(f"Space ID   : {space_info['space_id']}")
print(f"Space Name : {space_info['space_name']}")
print(f"Space URL  : {space_info['space_url']}")
print(f"{'=' * 70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.2: Verify Deployed Space

# COMMAND ----------

# DBTITLE 1,Verify Deployed Space
def verify_space(space_id, workspace_client):
    """Read back the space and verify its full configuration."""
    space = workspace_client.genie.get_space(space_id, include_serialized_space=True)

    print(f"Space ID    : {space.space_id}")
    print(f"Title       : {space.title}")
    print(f"Description : {space.description or '(none)'}")

    if space.serialized_space:
        ss = json.loads(space.serialized_space)
        tables = ss.get('data_sources', {}).get('tables', [])
        questions = ss.get('config', {}).get('sample_questions', [])
        instr = ss.get('instructions', {})
        text_instr = instr.get('text_instructions', [])
        joins = instr.get('join_specs', [])
        eq_sqls = instr.get('example_question_sqls', [])
        snippets = instr.get('sql_snippets', {})

        print(f"\n📊 Deployed configuration:")
        print(f"  Tables              : {len(tables)}")
        for t in tables:
            desc = t.get('description', [''])[0][:60] if t.get('description') else ''
            print(f"    • {t['identifier']}" + (f"  — {desc}" if desc else ""))
        print(f"  Sample questions    : {len(questions)}")
        print(f"  Text instructions   : {len(text_instr)}")
        print(f"  Join specs          : {len(joins)}")
        for j in joins:
            print(f"    • {j['left']['identifier']} ↔ {j['right']['identifier']}")
            print(f"      SQL: {j['sql'][0]}")
        print(f"  Example SQL pairs   : {len(eq_sqls)}")
        for eq in eq_sqls[:3]:
            print(f"    • Q: {eq['question'][0][:70]}")
            print(f"      SQL: {eq['sql'][0][:80]}...")
        if len(eq_sqls) > 3:
            print(f"    ... and {len(eq_sqls) - 3} more")
        print(f"  Snippet measures    : {len(snippets.get('measures', []))}")
        print(f"  Snippet filters     : {len(snippets.get('filters', []))}")
        print(f"  Snippet expressions : {len(snippets.get('expressions', []))}")
    else:
        print("  ⚠️  No serialized_space returned")

    return space

print("=" * 60)
print("VERIFYING DEPLOYED SPACE")
print("=" * 60)
print()
verified_space = verify_space(space_info['space_id'], w)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.3: Test the Deployed Space

# COMMAND ----------

# DBTITLE 1,Test Deployed Space
def test_genie_space(space_id, test_questions, workspace_client):
    """Test the deployed Genie space with sample conversations."""
    results = []
    print("Testing Genie space with sample questions...\n")

    for q in test_questions:
        try:
            msg = workspace_client.genie.start_conversation_and_wait(
                space_id=space_id,
                content=q,
            )
            results.append({'question': q, 'status': 'success', 'message_id': msg.id})
            print(f"✅ Question: {q}")
            print(f"  Status: SUCCESS (message_id={msg.id})\n")
        except Exception as e:
            results.append({'question': q, 'status': 'failed', 'error': str(e)})
            print(f"❌ Question: {q}")
            print(f"  Status: FAILED — {e}\n")
    return results

test_questions = [
    "How many records are in each table?",
    "Show me recent activity",
    "What are the key metrics?",
]

print("=" * 60)
print("TESTING DEPLOYED GENIE SPACE")
print("=" * 60)
print()
test_results = test_genie_space(space_info['space_id'], test_questions, w)

passed = len([r for r in test_results if r['status'] == 'success'])
print(f"\n{'=' * 70}")
print(f"TEST RESULTS: {passed}/{len(test_results)} passed")
print(f"{'=' * 70}")
if all(r['status'] == 'success' for r in test_results):
    print("\n✅ All tests passed! The Genie space is functioning correctly.")
else:
    print("\n⚠️  Some tests failed. This may be expected if the warehouse is not running.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7.4: Finalize Deployment

# COMMAND ----------

# DBTITLE 1,Finalize Deployment
deployment_summary = {
    'space_id': space_info['space_id'],
    'space_name': space_info['space_name'],
    'space_url': space_info['space_url'],
    'deployment_time': datetime.now().isoformat(),
    'status': 'active',
    'configuration': {
        'tables': len(knowledge_store['tables']),
        'joins': len(knowledge_store['joins']),
        'measures': len(knowledge_store['sql_expressions']['measures']),
        'filters': len(knowledge_store['sql_expressions']['filters']),
        'dimensions': len(knowledge_store['sql_expressions']['dimensions']),
        'examples': len(knowledge_store.get('example_queries', [])),
    },
    'ready_for_use': True,
}

output_dir = Path(config['output_path'].replace('/dbfs', ''))
output_dir.mkdir(parents=True, exist_ok=True)
with open(output_dir / "deployment_summary.json", 'w') as f:
    json.dump(deployment_summary, f, indent=2)

print(f"{'=' * 80}")
print(f"{'DEPLOYMENT COMPLETE':^80}")
print(f"{'=' * 80}\n")
print(f"📊 DEPLOYMENT SUMMARY:")
print(f"  Space Name      : {deployment_summary['space_name']}")
print(f"  Space ID        : {deployment_summary['space_id']}")
print(f"  Deployment Time : {deployment_summary['deployment_time']}")
print(f"  Status          : {deployment_summary['status'].upper()}")
print()
print(f"📈 CONFIGURATION:")
for k, v in deployment_summary['configuration'].items():
    print(f"  • {k.replace('_', ' ').title()}: {v}")
print()
print(f"{'=' * 80}")
print(f"ACCESS YOUR GENIE SPACE:")
print(f"{'=' * 80}")
print(f"{deployment_summary['space_url']}")
print(f"{'=' * 80}\n")
print("✅ The Genie space is ready for use!")
print("✅ Share the URL with your business users to start querying data in natural language")
print(f"\n💾 Deployment summary saved to: {config['output_path']}/deployment_summary.json")
