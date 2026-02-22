# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4: Query Intelligence & Pattern Mining
# MAGIC **Auto-Genie MVP - Stage 3: Query Intelligence & Pattern Mining**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Loading table metadata and statistics from Notebooks 1-3
# MAGIC - Extracting query history from `system.query.history`
# MAGIC - Parsing SQL and clustering similar queries
# MAGIC - Generating representative example queries from query-history clusters
# MAGIC - Using an LLM (ChatDatabricks) to generate additional sample queries from metadata
# MAGIC - Merging both sources into a final set of 20 validated sample queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, re, json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
from dotenv import load_dotenv
import pandas as pd
import sqlglot
from sqlglot import exp
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN

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
# MAGIC ## Cell 4.0b: Load Table Metadata & Statistics (from Notebooks 2-3)

# COMMAND ----------

# DBTITLE 1,Load Table Metadata and Statistics
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
        print(f"  ✅ {fqn} — {row_count:,} rows, {len([c.asDict() for c in cols])} columns")
    return metadata

def _profile_columns(table_metadata):
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
                tbl_profiles.append({
                    "column": cn,
                    "data_type": col['data_type'],
                    "distinct_count": card,
                    "null_percentage": round(r['nc'] / r['tc'] * 100, 2) if r['tc'] else 0,
                    "cardinality_category": cat,
                })
            except Exception:
                pass
        profiles[fqn] = tbl_profiles
    return profiles

print("=" * 60)
print("LOADING TABLE METADATA & STATISTICS")
print("=" * 60)
table_metadata = _load_table_metadata()
column_profiles = _profile_columns(table_metadata)
print(f"\n✅ Loaded {len(table_metadata)} tables, {sum(len(v) for v in column_profiles.values())} column profiles")

in_scope_tables = set(table_metadata.keys())
in_scope_simple = {fqn.split('.')[-1] for fqn in in_scope_tables}
print(f"📋 In-scope tables: {', '.join(sorted(in_scope_simple))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.1: Extract Query History

# COMMAND ----------

# DBTITLE 1,Extract Query History
def extract_query_history(catalog, schema, lookback_days=90):
    """Extract query history for the target schema."""
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    query = f"""
    SELECT
        query_id,
        statement_text,
        executed_by,
        start_time,
        end_time,
        total_duration_ms,
        statement_type,
        compute.warehouse_id
    FROM system.query.history
    WHERE start_time >= '{start_date}'
      AND statement_type = 'SELECT'
      AND error_message IS NULL
      AND (
          LOWER(statement_text) LIKE '%{catalog}.{schema}%'
          OR LOWER(statement_text) LIKE '%{schema}.%'
      )
    ORDER BY start_time DESC
    LIMIT 10000
    """
    rows = spark.sql(query).collect()
    return [r.asDict() for r in rows]

print("=" * 60)
print("EXTRACTING QUERY HISTORY")
print("=" * 60)
print(f"Date range: {(datetime.now() - timedelta(days=config['lookback_days'])).strftime('%Y-%m-%d')} → {datetime.now().strftime('%Y-%m-%d')}")
print()

query_history = extract_query_history(
    config['catalog'], config['schema'], config['lookback_days']
)
print(f"✅ Extracted {len(query_history)} queries")

if query_history:
    print("\n📋 SAMPLE QUERIES:")
    for i, q in enumerate(query_history[:3], 1):
        print(f"\n{i}. User: {q['executed_by']} | Duration: {q['total_duration_ms']}ms")
        print(f"   {q['statement_text'][:150]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.2: Parse and Cluster Queries

# COMMAND ----------

# DBTITLE 1,Parse and Cluster Queries
def parse_and_cluster_queries(query_history, n_clusters=20):
    """Parse SQL and cluster similar queries."""
    parsed_queries = []
    print("Parsing SQL queries...")

    for i, query in enumerate(query_history):
        try:
            statements = sqlglot.parse(query['statement_text'], read='databricks')
            if statements:
                stmt = statements[0]
                tables = [t.name for t in stmt.find_all(exp.Table)]
                columns = [c.name for c in stmt.find_all(exp.Column)]
                aggregations = [a.sql() for a in stmt.find_all(exp.AggFunc)]
                joins = len(list(stmt.find_all(exp.Join)))
                where_clauses = len(list(stmt.find_all(exp.Where)))
                group_by = len(list(stmt.find_all(exp.Group)))

                parsed_queries.append({
                    'query_id': query['query_id'],
                    'original_sql': query['statement_text'],
                    'tables': tables,
                    'columns': columns,
                    'aggregations': aggregations,
                    'joins': joins,
                    'where_clauses': where_clauses,
                    'group_by': group_by > 0,
                    'user': query['executed_by'],
                    'duration_ms': query['total_duration_ms'],
                })
        except Exception:
            continue

        if (i + 1) % 500 == 0:
            print(f"  Parsed {i + 1}/{len(query_history)} queries...")

    print(f"✅ Successfully parsed {len(parsed_queries)} queries\n")

    if not parsed_queries:
        print("⚠️ No queries parsed — skipping clustering")
        return parsed_queries, {}

    print("Clustering queries...")
    query_features = []
    for q in parsed_queries:
        feature_text = (
            " ".join(q['tables']) + " " +
            " ".join(q['aggregations']) + " " +
            ("join" * q['joins']) + " " +
            ("where" * q['where_clauses']) + " " +
            ("groupby" if q['group_by'] else "")
        )
        query_features.append(feature_text)

    vectorizer = TfidfVectorizer(max_features=50, stop_words='english')
    X = vectorizer.fit_transform(query_features)

    clustering = DBSCAN(eps=0.4, min_samples=2, metric='cosine')
    clusters = clustering.fit_predict(X.toarray())

    clustered_queries = {}
    for idx, cluster_id in enumerate(clusters):
        if cluster_id == -1:
            continue
        clustered_queries.setdefault(cluster_id, []).append(parsed_queries[idx])

    print(f"✅ Identified {len(clustered_queries)} query patterns\n")
    return parsed_queries, clustered_queries

print("=" * 60)
print("PARSING AND CLUSTERING QUERIES")
print("=" * 60)
parsed_queries, clustered_queries = parse_and_cluster_queries(query_history)

if clustered_queries:
    print("📊 CLUSTER SUMMARY:")
    print(f"{'Cluster':<10} {'Size':<10} {'Avg Duration (ms)':<20} {'Common Tables'}")
    print("-" * 80)
    for cid, queries in sorted(clustered_queries.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
        avg_dur = sum(q['duration_ms'] for q in queries) / len(queries)
        all_t = []
        for q in queries:
            all_t.extend(q['tables'])
        common = Counter(all_t).most_common(2)
        print(f"{cid:<10} {len(queries):<10} {avg_dur:<20.0f} {', '.join(f'{t}({n})' for t, n in common)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.3: Generate Example Queries from History Clusters

# COMMAND ----------

# DBTITLE 1,Generate Example Queries from History Clusters
def generate_example_queries(clustered_queries, parsed_queries, top_n=10):
    """Generate representative example queries with NL descriptions from clusters."""
    examples = []
    sorted_clusters = sorted(clustered_queries.items(),
                             key=lambda x: len(x[1]),
                             reverse=True)[:top_n]

    for cluster_id, queries in sorted_clusters:
        template_query = min(queries, key=lambda q: len(q['original_sql']))

        tables = Counter()
        for q in queries:
            tables.update(q['tables'])
        main_tables = [t[0] for t in tables.most_common(2)]

        has_join = template_query['joins'] > 0
        has_agg = len(template_query['aggregations']) > 0
        has_group = template_query['group_by']

        if has_agg and has_group:
            nl = (f"Show aggregated metrics from {' and '.join(main_tables)}" if has_join
                  else f"Calculate summary statistics for {main_tables[0]}")
        elif has_join:
            nl = f"Combine data from {' and '.join(main_tables)}"
        else:
            nl = f"Query {main_tables[0]} table"

        sql = template_query['original_sql']
        sql = re.sub(r"'\d{4}-\d{2}-\d{2}'", "CURRENT_DATE - INTERVAL {{days}} DAYS", sql, count=1)
        sql = re.sub(r">\s*\d+", "> {{threshold}}", sql, count=1)

        examples.append({
            'natural_language': nl,
            'sql': sql,
            'frequency': len(queries),
            'avg_execution_time_ms': sum(q['duration_ms'] for q in queries) / len(queries),
            'tables_used': main_tables,
            'source': 'history_cluster',
        })
    return examples

print("=" * 60)
print("GENERATING EXAMPLE QUERIES FROM HISTORY CLUSTERS")
print("=" * 60)

if clustered_queries:
    history_examples = generate_example_queries(clustered_queries, parsed_queries, top_n=10)
    print(f"✅ Generated {len(history_examples)} queries from history clusters\n")
    for i, ex in enumerate(history_examples[:5], 1):
        print(f"  {i}. {ex['natural_language']} (freq={ex['frequency']})")
else:
    history_examples = []
    print("⚠️ No clustered queries — history-based generation skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.4: Generate Sample Queries via LLM (ChatDatabricks)

# COMMAND ----------

# DBTITLE 1,Generate Sample Queries via LLM
from langchain_core.messages import SystemMessage, HumanMessage
from databricks_langchain import ChatDatabricks

LLM_MODEL = "databricks-claude-opus-4-6"
LLM_QUERY_COUNT = 20


def _build_schema_summary(table_metadata, column_profiles):
    """Build a concise, guardrail-safe schema description for the LLM.

    Column comments from Unity Catalog are excluded because they may contain
    arbitrary user-authored text that can trigger Databricks input guardrails.
    """
    lines = []
    for fqn, meta in table_metadata.items():
        lines.append(f"\nTABLE: {fqn}  (rows: {meta['row_count']:,})")
        for col in meta['columns']:
            col_name = col['column_name']
            col_type = col['data_type']
            nullable = col['is_nullable']
            profile = next(
                (p for p in column_profiles.get(fqn, []) if p['column'] == col_name), None
            )
            extras = []
            if profile:
                extras.append(f"distinct={profile['distinct_count']}")
                extras.append(f"nulls={profile['null_percentage']}%")
            extra_str = f"  -- {', '.join(extras)}" if extras else ""
            lines.append(f"  {col_name} {col_type} {'NULL' if nullable == 'YES' else 'NOT NULL'}{extra_str}")
    return "\n".join(lines)


def _build_minimal_schema(table_metadata):
    """Ultra-minimal schema (table + column names only) for guardrail retry."""
    lines = []
    for fqn, meta in table_metadata.items():
        cols = ", ".join(c['column_name'] for c in meta['columns'])
        lines.append(f"{fqn} ({meta['row_count']:,} rows): {cols}")
    return "\n".join(lines)


def _generate_rule_based_queries(table_metadata, column_profiles, config, n=20):
    """Rule-based fallback when the LLM is blocked by guardrails."""
    catalog = config['catalog']
    schema = config['schema']
    queries = []

    for fqn, meta in table_metadata.items():
        simple = fqn.split('.')[-1]
        cols = meta['columns']
        col_names = [c['column_name'] for c in cols]

        id_cols = [c for c in col_names if c.endswith('_id') or c == 'id']
        date_cols = [c for c in cols if c['data_type'].upper() in ('DATE', 'TIMESTAMP')]
        numeric_cols = [c for c in cols if any(t in c['data_type'].upper() for t in ['INT', 'BIGINT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'LONG'])]
        string_cols = [c for c in cols if 'STRING' in c['data_type'].upper() or 'VARCHAR' in c['data_type'].upper()]

        queries.append({'natural_language': f"Show the first 10 rows from {simple}", 'sql': f"SELECT * FROM {fqn} LIMIT 10", 'tables_used': [simple]})
        queries.append({'natural_language': f"How many records are in {simple}?", 'sql': f"SELECT COUNT(*) AS total_records FROM {fqn}", 'tables_used': [simple]})

        if numeric_cols:
            nc = numeric_cols[0]
            queries.append({'natural_language': f"What is the average {nc['column_name']} in {simple}?", 'sql': f"SELECT AVG(`{nc['column_name']}`) AS avg_value FROM {fqn}", 'tables_used': [simple]})
            queries.append({'natural_language': f"What are the min, max, and average {nc['column_name']} in {simple}?", 'sql': f"SELECT MIN(`{nc['column_name']}`) AS min_val, MAX(`{nc['column_name']}`) AS max_val, AVG(`{nc['column_name']}`) AS avg_val FROM {fqn}", 'tables_used': [simple]})

        if date_cols:
            dc = date_cols[0]['column_name']
            queries.append({'natural_language': f"Show {simple} records from the last 30 days", 'sql': f"SELECT * FROM {fqn} WHERE `{dc}` >= CURRENT_DATE - INTERVAL 30 DAYS ORDER BY `{dc}` DESC LIMIT 100", 'tables_used': [simple]})

        if string_cols:
            sc = string_cols[0]
            queries.append({'natural_language': f"What are the distinct values of {sc['column_name']} in {simple}?", 'sql': f"SELECT DISTINCT `{sc['column_name']}`, COUNT(*) AS cnt FROM {fqn} GROUP BY `{sc['column_name']}` ORDER BY cnt DESC", 'tables_used': [simple]})

    fqns = list(table_metadata.keys())
    for i in range(len(fqns)):
        for j in range(i + 1, len(fqns)):
            cols_i = {c['column_name'] for c in table_metadata[fqns[i]]['columns']}
            cols_j = {c['column_name'] for c in table_metadata[fqns[j]]['columns']}
            shared = cols_i & cols_j
            join_cols = [c for c in shared if c.endswith('_id') or c == 'id']
            if join_cols:
                jc = join_cols[0]
                si, sj = fqns[i].split('.')[-1], fqns[j].split('.')[-1]
                queries.append({
                    'natural_language': f"Join {si} with {sj} on {jc}",
                    'sql': f"SELECT a.*, b.* FROM {fqns[i]} a INNER JOIN {fqns[j]} b ON a.`{jc}` = b.`{jc}` LIMIT 100",
                    'tables_used': [si, sj],
                })

    for q in queries:
        q['source'] = 'rule_based'
        q['frequency'] = 0
        q['avg_execution_time_ms'] = 0
    return queries[:n]


def generate_llm_sample_queries(
    table_metadata,
    column_profiles,
    config,
    n_queries=LLM_QUERY_COUNT,
    model=LLM_MODEL,
):
    """Use ChatDatabricks to generate sample SQL queries from metadata.

    Includes retry logic to handle Databricks input guardrail false positives:
      Attempt 1 — full schema (columns + stats, no UC comments)
      Attempt 2 — minimal schema (table + column names only)
      Fallback  — rule-based query generation (no LLM)
    """
    catalog = config['catalog']
    schema = config['schema']
    fqn_list = ", ".join(sorted(table_metadata.keys()))
    table_list = ", ".join(sorted(fqn.split('.')[-1] for fqn in table_metadata))

    system_prompt = (
        "You are a helpful Databricks SQL assistant for a business analytics team. "
        "Your task is to write example SELECT queries that analysts would find useful.\n\n"
        "RULES:\n"
        f"1. ONLY use these fully-qualified tables: {fqn_list}\n"
        "2. NEVER invent tables or columns not in the schema provided.\n"
        "3. Always use fully-qualified table names (catalog.schema.table).\n"
        "4. Each query must be a valid Databricks SQL SELECT statement.\n"
        "5. Cover diverse patterns: aggregations, joins, filters, "
        "time-based analysis, top-N, comparisons, trends.\n"
        "6. Return ONLY a JSON array. No markdown fences, no explanation."
    )

    response_spec = (
        f"Generate exactly {n_queries} sample SQL queries.\n"
        f"Return a JSON array where each element has:\n"
        f'  "natural_language": a short business question,\n'
        f'  "sql": the Databricks SQL query,\n'
        f'  "tables_used": list of simple table names (e.g. ["{table_list.split(", ")[0]}"])\n\n'
        f"Cover: simple lookups, GROUP BY, JOINs, date filters, "
        f"Top-N rankings, CASE WHEN, CTEs.\n"
        f"Only use tables: {table_list}.\n"
        f"Return raw JSON only."
    )

    attempts = [
        ("full schema", _build_schema_summary(table_metadata, column_profiles)),
        ("minimal schema", _build_minimal_schema(table_metadata)),
    ]

    for attempt_label, schema_text in attempts:
        user_prompt = (
            f"Schema for catalog `{catalog}`, schema `{schema}`:\n\n"
            f"{schema_text}\n\n"
            f"{response_spec}"
        )
        print(f"🤖 Attempt ({attempt_label}): calling {model} for {n_queries} queries...")
        print(f"   Prompt size: {len(system_prompt) + len(user_prompt)} chars\n")

        try:
            llm = ChatDatabricks(model=model)
            response = llm.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ])

            raw = response.content.strip()
            if raw.startswith("```"):
                raw = re.sub(r"^```(?:json)?\s*", "", raw)
                raw = re.sub(r"\s*```$", "", raw)

            queries = json.loads(raw)

            valid = []
            for q in queries:
                used = q.get('tables_used', [])
                if all(t in in_scope_simple for t in used):
                    q['source'] = 'llm'
                    q['frequency'] = 0
                    q['avg_execution_time_ms'] = 0
                    valid.append(q)
                else:
                    print(f"  ⚠️  Dropped out-of-scope query: tables={used}")

            print(f"✅ LLM returned {len(queries)} queries, {len(valid)} passed scope validation")
            return valid

        except Exception as e:
            err_msg = str(e)
            if 'guardrail' in err_msg.lower() or 'BAD_REQUEST' in err_msg:
                print(f"  ⚠️  Input guardrail triggered on {attempt_label} — retrying with simpler prompt...\n")
            else:
                print(f"  ❌ LLM error: {err_msg}\n")

    print("⚠️  All LLM attempts blocked by guardrails — falling back to rule-based generation")
    return _generate_rule_based_queries(table_metadata, column_profiles, config, n=n_queries)


print("=" * 60)
print("GENERATING SAMPLE QUERIES VIA LLM (ChatDatabricks)")
print("=" * 60)
print()
llm_examples = generate_llm_sample_queries(
    table_metadata, column_profiles, config,
    n_queries=LLM_QUERY_COUNT, model=LLM_MODEL,
)
print()
for i, ex in enumerate(llm_examples[:5], 1):
    print(f"  {i}. {ex['natural_language']}")
    print(f"     SQL: {ex['sql'][:120]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.5: Merge & Finalize — 20 Sample Queries

# COMMAND ----------

# DBTITLE 1,Merge and Finalize 20 Sample Queries
FINAL_COUNT = 20


def merge_and_finalize(history_examples, llm_examples, table_metadata, n=FINAL_COUNT):
    """Merge history-based and LLM-generated queries into a final deduplicated set."""
    in_scope = {fqn.split('.')[-1] for fqn in table_metadata}
    all_candidates = []

    # History-based queries get priority (real usage signals)
    for ex in history_examples:
        if all(t in in_scope for t in ex.get('tables_used', [])):
            all_candidates.append(ex)

    # LLM-generated queries fill the remaining slots
    for ex in llm_examples:
        if all(t in in_scope for t in ex.get('tables_used', [])):
            all_candidates.append(ex)

    # Deduplicate by natural_language (case-insensitive)
    seen = set()
    deduped = []
    for ex in all_candidates:
        key = ex['natural_language'].strip().lower()
        if key not in seen:
            seen.add(key)
            deduped.append(ex)

    # Select top-N: favour history (sorted by frequency desc), then LLM
    history = sorted(
        [e for e in deduped if e.get('source') == 'history_cluster'],
        key=lambda x: x.get('frequency', 0), reverse=True,
    )
    llm = [e for e in deduped if e.get('source') == 'llm']

    final = []
    final.extend(history[:n])
    remaining = n - len(final)
    if remaining > 0:
        final.extend(llm[:remaining])

    # Number them for easy reference
    for idx, q in enumerate(final, 1):
        q['query_id'] = idx

    return final


print("=" * 60)
print("MERGING & FINALIZING SAMPLE QUERIES")
print("=" * 60)
print()
print(f"  History-based candidates: {len(history_examples)}")
print(f"  LLM-generated candidates: {len(llm_examples)}")
print()

example_queries = merge_and_finalize(
    history_examples, llm_examples, table_metadata, n=FINAL_COUNT
)

history_count = len([q for q in example_queries if q.get('source') == 'history_cluster'])
llm_count = len([q for q in example_queries if q.get('source') == 'llm'])

print(f"✅ Final set: {len(example_queries)} queries  "
      f"({history_count} from history, {llm_count} from LLM)\n")
print("=" * 80)
print(f"{'#':<4} {'Source':<10} {'Natural Language Question'}")
print("=" * 80)
for q in example_queries:
    src_tag = "HISTORY" if q.get('source') == 'history_cluster' else "LLM"
    print(f"{q['query_id']:<4} {src_tag:<10} {q['natural_language']}")

print(f"\n{'=' * 80}")
print("DETAILED VIEW (first 5)")
print("=" * 80)
for q in example_queries[:5]:
    src_tag = "HISTORY" if q.get('source') == 'history_cluster' else "LLM"
    print(f"\n{'─' * 70}")
    print(f"#{q['query_id']} [{src_tag}] {q['natural_language']}")
    print(f"{'─' * 70}")
    print(f"Tables: {', '.join(q.get('tables_used', []))}")
    sql_display = q['sql'][:300] + "..." if len(q['sql']) > 300 else q['sql']
    print(f"SQL:\n{sql_display}")

# Save for downstream notebooks
output_dir = Path(config['output_path'].replace('/dbfs', ''))
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "example_queries.json"
with open(output_file, 'w') as f:
    json.dump(example_queries, f, indent=2, default=str)
print(f"\n💾 Saved {len(example_queries)} queries to {output_file}")
