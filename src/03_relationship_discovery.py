# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3: Relationship Discovery
# MAGIC **Auto-Genie MVP - Stage 2: Relationship Discovery**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Extracting declared foreign key relationships from Unity Catalog
# MAGIC - Discovering implicit relationships via column naming patterns
# MAGIC - Mining query history for JOIN-based relationship patterns
# MAGIC - Merging, deduplicating, and ranking all discovered relationships
# MAGIC - Visualizing the relationship graph

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, re
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
from dotenv import load_dotenv
import pandas as pd
import networkx as nx

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

print(f"✅ Config loaded — targeting {config['catalog']}.{config['schema']} ({len(config['tables'])} tables)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3.0b: Load Table Metadata (from Notebook 2)

# COMMAND ----------

# DBTITLE 1,Load Table Metadata
def extract_table_metadata(catalog, schema, table_names):
    """Extract comprehensive metadata for specified tables."""
    metadata = {}
    for table_name in table_names:
        full_table_name = f"{catalog}.{schema}.{table_name}"
        table_info = spark.sql(f"""
            SELECT table_catalog, table_schema, table_name, table_type, comment
            FROM system.information_schema.tables
            WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table_name}'
        """).collect()
        if not table_info:
            print(f"⚠️ Table {full_table_name} not found")
            continue
        columns = spark.sql(f"""
            SELECT column_name, data_type, is_nullable, column_default, comment
            FROM system.information_schema.columns
            WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """).collect()
        detail = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0].asDict()
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]['cnt']
        constraints = spark.sql(f"""
            SELECT constraint_name, constraint_type
            FROM system.information_schema.table_constraints
            WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table_name}'
        """).collect()
        metadata[full_table_name] = {
            "table_info": table_info[0].asDict(),
            "columns": [col.asDict() for col in columns],
            "row_count": row_count,
            "size_bytes": detail.get('sizeInBytes', 0),
            "last_modified": detail.get('lastModified', ''),
            "constraints": [c.asDict() for c in constraints],
        }
        print(f"✅ Loaded metadata for {full_table_name}")
    return metadata

print("=" * 60)
print("LOADING TABLE METADATA (prerequisite for relationship discovery)")
print("=" * 60)
table_metadata = extract_table_metadata(config['catalog'], config['schema'], config['tables'])
print(f"✅ Metadata loaded for {len(table_metadata)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3.1: Discover Declared Relationships

# COMMAND ----------

# DBTITLE 1,Discover Declared Relationships (Foreign Keys)
def discover_declared_relationships(catalog, schema):
    """Extract foreign key relationships from Unity Catalog."""
    relationships = []

    fk_query = f"""
    SELECT
        tc.table_name as source_table,
        kcu.column_name as source_column,
        rc.referenced_table_name as target_table,
        rc.referenced_column_name as target_column
    FROM system.information_schema.table_constraints tc
    JOIN system.information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_catalog = kcu.table_catalog
        AND tc.table_schema = kcu.table_schema
        AND tc.table_name = kcu.table_name
    JOIN (
        SELECT
            constraint_name,
            table_name as referenced_table_name,
            column_name as referenced_column_name
        FROM system.information_schema.key_column_usage
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
    ) rc ON tc.constraint_name = rc.constraint_name
    WHERE tc.table_catalog = '{catalog}'
      AND tc.table_schema = '{schema}'
      AND tc.constraint_type = 'FOREIGN KEY'
    """

    try:
        results = spark.sql(fk_query).collect()
        for row in results:
            relationships.append({
                "source_table": f"{catalog}.{schema}.{row['source_table']}",
                "source_column": row['source_column'],
                "target_table": f"{catalog}.{schema}.{row['target_table']}",
                "target_column": row['target_column'],
                "confidence": 1.0,
                "method": "declared_fk"
            })
    except Exception as e:
        print(f"⚠️ No declared foreign keys found or error: {str(e)}")

    return relationships

print("=" * 60)
print("DISCOVERING DECLARED RELATIONSHIPS")
print("=" * 60)
declared_relationships = discover_declared_relationships(
    config['catalog'],
    config['schema']
)

if declared_relationships:
    for rel in declared_relationships:
        print(f"✅ {rel['source_table']}.{rel['source_column']} → {rel['target_table']}.{rel['target_column']}")
else:
    print("ℹ️ No declared foreign key relationships found")

print(f"\n📊 Found {len(declared_relationships)} declared relationships")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3.2: Discover Naming Pattern Relationships

# COMMAND ----------

# DBTITLE 1,Discover Naming Pattern Relationships
def discover_naming_pattern_relationships(table_metadata):
    """Discover relationships based on column naming patterns (_id, _key, fk_*)."""
    relationships = []

    all_columns = {}
    for table_name, metadata in table_metadata.items():
        all_columns[table_name] = [
            (col['column_name'], col['data_type'])
            for col in metadata['columns']
        ]

    id_patterns = [r'(.+)_id$', r'(.+)_key$', r'^fk_(.+)$']

    for source_table, source_cols in all_columns.items():
        for source_col, source_type in source_cols:
            for pattern in id_patterns:
                match = re.search(pattern, source_col, re.IGNORECASE)
                if match:
                    entity_name = match.group(1)

                    for target_table, target_cols in all_columns.items():
                        if source_table == target_table:
                            continue

                        target_table_simple = target_table.split('.')[-1]
                        if entity_name in target_table_simple or target_table_simple in entity_name:
                            for target_col, target_type in target_cols:
                                if (target_col.endswith('_id') or target_col.endswith('_key') or target_col == 'id') \
                                        and source_type == target_type:
                                    confidence = 0.85 if source_col == target_col else 0.70

                                    relationships.append({
                                        "source_table": source_table,
                                        "source_column": source_col,
                                        "target_table": target_table,
                                        "target_column": target_col,
                                        "confidence": confidence,
                                        "method": "naming_pattern",
                                        "evidence": f"Pattern '{pattern}', types match ({source_type})"
                                    })

    return relationships

print("=" * 60)
print("DISCOVERING NAMING PATTERN RELATIONSHIPS")
print("=" * 60)
naming_relationships = discover_naming_pattern_relationships(table_metadata)

for rel in naming_relationships[:10]:
    print(f"✅ {rel['source_table'].split('.')[-1]}.{rel['source_column']} → "
          f"{rel['target_table'].split('.')[-1]}.{rel['target_column']} "
          f"(confidence: {rel['confidence']:.2f})")

print(f"\n📊 Found {len(naming_relationships)} naming pattern relationships")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3.3: Discover Query Pattern Relationships

# COMMAND ----------

# DBTITLE 1,Discover Query Pattern Relationships
def discover_query_pattern_relationships(catalog, schema, lookback_days=90):
    """Mine query history for JOIN patterns to discover implicit relationships."""
    relationships = []

    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    history_query = f"""
    SELECT
        statement_text,
        executed_by,
        executed_by_user_id,
        start_time,
        total_duration_ms
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
        queries = spark.sql(history_query).collect()
        print(f"ℹ️ Analyzing {len(queries)} queries with JOINs...")

        join_patterns = Counter()

        for query in queries:
            sql_text = query['statement_text'].lower()

            join_pattern = r'(\w+)\s+(?:inner\s+|left\s+|right\s+)?join\s+(\w+)\s+(?:as\s+\w+\s+)?on\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            matches = re.findall(join_pattern, sql_text)

            for match in matches:
                if len(match) == 6:
                    tables = tuple(sorted([match[0], match[1]]))
                    columns = (match[3], match[5])
                    join_sig = (tables, columns)
                    join_patterns[join_sig] += 1

        for (tables, columns), count in join_patterns.items():
            if count >= 3:
                confidence = min(0.95, 0.60 + (count / 100))

                relationships.append({
                    "source_table": f"{catalog}.{schema}.{tables[0]}",
                    "source_column": columns[0],
                    "target_table": f"{catalog}.{schema}.{tables[1]}",
                    "target_column": columns[1],
                    "confidence": round(confidence, 2),
                    "method": "query_pattern",
                    "query_count": count,
                    "evidence": f"Found in {count} queries"
                })

    except Exception as e:
        print(f"⚠️ Error analyzing query history: {str(e)}")

    return relationships

print("=" * 60)
print("DISCOVERING QUERY PATTERN RELATIONSHIPS")
print("=" * 60)
query_relationships = discover_query_pattern_relationships(
    config['catalog'],
    config['schema'],
    config['lookback_days']
)

for rel in query_relationships:
    print(f"✅ {rel['source_table'].split('.')[-1]}.{rel['source_column']} → "
          f"{rel['target_table'].split('.')[-1]}.{rel['target_column']} "
          f"(confidence: {rel['confidence']:.2f}, queries: {rel['query_count']})")

print(f"\n📊 Found {len(query_relationships)} query pattern relationships")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3.4: Merge and Rank All Relationships

# COMMAND ----------

# DBTITLE 1,Merge and Rank All Relationships
def merge_and_rank_relationships(declared, naming, query_patterns, threshold=0.75):
    """Merge all discovered relationships, deduplicate, and rank by confidence."""
    all_relationships = {}

    for rel in declared:
        key = f"{rel['source_table']}.{rel['source_column']}->{rel['target_table']}.{rel['target_column']}"
        all_relationships[key] = rel.copy()
        all_relationships[key]['methods'] = ['declared_fk']
        all_relationships[key]['evidence_count'] = 1

    for rel in naming:
        key = f"{rel['source_table']}.{rel['source_column']}->{rel['target_table']}.{rel['target_column']}"
        if key in all_relationships:
            all_relationships[key]['methods'].append('naming_pattern')
            all_relationships[key]['evidence_count'] += 1
        else:
            all_relationships[key] = rel.copy()
            all_relationships[key]['methods'] = ['naming_pattern']
            all_relationships[key]['evidence_count'] = 1

    for rel in query_patterns:
        key = f"{rel['source_table']}.{rel['source_column']}->{rel['target_table']}.{rel['target_column']}"
        if key in all_relationships:
            all_relationships[key]['methods'].append('query_pattern')
            all_relationships[key]['evidence_count'] += 1
            all_relationships[key]['confidence'] = min(1.0, all_relationships[key]['confidence'] + 0.05)
            all_relationships[key]['query_count'] = rel.get('query_count', 0)
        else:
            all_relationships[key] = rel.copy()
            all_relationships[key]['methods'] = ['query_pattern']
            all_relationships[key]['evidence_count'] = 1

    ranked = list(all_relationships.values())
    ranked.sort(key=lambda x: (x['evidence_count'], x['confidence']), reverse=True)

    filtered = [r for r in ranked if r['confidence'] >= threshold]

    for i, rel in enumerate(filtered, 1):
        rel['rank'] = i

    return filtered

print("=" * 60)
print("MERGING AND RANKING RELATIONSHIPS")
print("=" * 60)
final_relationships = merge_and_rank_relationships(
    declared_relationships,
    naming_relationships,
    query_relationships,
    config['confidence_threshold']
)

print(f"\n📊 FINAL RELATIONSHIP SUMMARY")
print(f"  Total unique relationships: {len(final_relationships)}")
print(f"  Confidence threshold: {config['confidence_threshold']}")
print()

for rel in final_relationships:
    methods_str = ", ".join(rel['methods'])
    print(f"#{rel['rank']} [{rel['confidence']:.2f}] "
          f"{rel['source_table'].split('.')[-1]}.{rel['source_column']} → "
          f"{rel['target_table'].split('.')[-1]}.{rel['target_column']}")
    print(f"  Methods: {methods_str} | Evidence: {rel['evidence_count']} sources")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3.5: Visualize Relationship Graph

# COMMAND ----------

# DBTITLE 1,Visualize Relationship Graph
G = nx.DiGraph()

for rel in final_relationships:
    source = rel['source_table'].split('.')[-1]
    target = rel['target_table'].split('.')[-1]
    G.add_edge(source, target,
               weight=rel['confidence'],
               label=f"{rel['source_column']}→{rel['target_column']}")

edge_trace = []
for edge in G.edges(data=True):
    edge_trace.append({
        'source': edge[0],
        'target': edge[1],
        'confidence': edge[2]['weight'],
        'label': edge[2]['label']
    })

edge_df = pd.DataFrame(edge_trace)

print("=" * 60)
print("RELATIONSHIP GRAPH")
print("=" * 60)
print(edge_df.to_string(index=False))

print(f"\n📊 GRAPH STATISTICS:")
print(f"  Nodes (tables): {G.number_of_nodes()}")
print(f"  Edges (relationships): {G.number_of_edges()}")
if edge_trace:
    print(f"  Average confidence: {sum(e['confidence'] for e in edge_trace)/len(edge_trace):.2f}")
