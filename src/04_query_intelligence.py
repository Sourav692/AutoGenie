# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4: Query Intelligence & Pattern Mining
# MAGIC **Auto-Genie MVP - Stage 3: Query Intelligence & Pattern Mining**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Extracting query history from `system.query.history`
# MAGIC - Parsing SQL and clustering similar queries
# MAGIC - Generating representative example queries with natural-language descriptions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, re
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
# MAGIC ## Cell 4.1: Extract Query History

# COMMAND ----------

# DBTITLE 1,Extract Query History
def extract_query_history(catalog, schema, lookback_days=90):
    """Extract comprehensive query history for target schema."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    query = f"""
    SELECT
        query_id,
        statement_text,
        executed_by,
        executed_by_user_id,
        start_time,
        end_time,
        total_duration_ms,
        statement_type,
        compute.warehouse_id
    FROM system.query.history
    WHERE start_time >= '{start_date.strftime("%Y-%m-%d")}'
      AND statement_type = 'SELECT'
      AND error_message IS NULL
      AND (
          LOWER(statement_text) LIKE '%{catalog}.{schema}%'
          OR LOWER(statement_text) LIKE '%{schema}.%'
      )
    ORDER BY start_time DESC
    LIMIT 10000
    """
    queries = spark.sql(query).collect()
    return [q.asDict() for q in queries]

print("=" * 60)
print("EXTRACTING QUERY HISTORY")
print("=" * 60)
print(f"Date range: {(datetime.now() - timedelta(days=config['lookback_days'])).strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")
print()

query_history = extract_query_history(
    config['catalog'],
    config['schema'],
    config['lookback_days']
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
                    'duration_ms': query['total_duration_ms']
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
    for i, cluster_id in enumerate(clusters):
        if cluster_id == -1:
            continue
        if cluster_id not in clustered_queries:
            clustered_queries[cluster_id] = []
        clustered_queries[cluster_id].append(parsed_queries[i])

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
    for cluster_id, queries in sorted(clustered_queries.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
        avg_duration = sum(q['duration_ms'] for q in queries) / len(queries)
        all_tables = []
        for q in queries:
            all_tables.extend(q['tables'])
        common_tables = Counter(all_tables).most_common(2)
        table_str = ", ".join([f"{t[0]}({t[1]})" for t in common_tables])
        print(f"{cluster_id:<10} {len(queries):<10} {avg_duration:<20.0f} {table_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4.3: Generate Example Queries

# COMMAND ----------

# DBTITLE 1,Generate Example Queries
def generate_example_queries(clustered_queries, parsed_queries, top_n=10):
    """Generate representative example queries with NL descriptions."""
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
            if has_join:
                nl = f"Show aggregated metrics from {' and '.join(main_tables)}"
            else:
                nl = f"Calculate summary statistics for {main_tables[0]}"
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
            'cluster_id': cluster_id
        })

    return examples

print("=" * 60)
print("GENERATING EXAMPLE QUERIES")
print("=" * 60)

if clustered_queries:
    example_queries = generate_example_queries(clustered_queries, parsed_queries, top_n=10)
    print(f"✅ Generated {len(example_queries)} example queries\n")

    for i, ex in enumerate(example_queries, 1):
        print(f"\n{'=' * 70}")
        print(f"EXAMPLE #{i}")
        print(f"{'=' * 70}")
        print(f"📝 Natural Language: {ex['natural_language']}")
        print(f"📊 Frequency: {ex['frequency']} queries")
        print(f"⏱  Avg Duration: {ex['avg_execution_time_ms']:.0f}ms")
        print(f"📁 Tables: {', '.join(ex['tables_used'])}")
        print(f"\n💻 SQL Template:")
        print(ex['sql'][:200] + "..." if len(ex['sql']) > 200 else ex['sql'])
else:
    example_queries = []
    print("⚠️ No clustered queries — example generation skipped")
