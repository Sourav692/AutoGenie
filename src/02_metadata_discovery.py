# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2: Metadata Discovery & Profiling
# MAGIC **Auto-Genie MVP - Stage 1: Metadata Discovery & Profiling**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Extracting table-level metadata from Unity Catalog
# MAGIC - Profiling column-level statistics (cardinality, nulls, PII detection)
# MAGIC - Visualizing metadata summaries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2.0: Environment Setup

# COMMAND ----------

# DBTITLE 1,Environment Setup
import os, re
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px

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
    spark  # already available on Databricks clusters
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
# MAGIC ## Cell 2.1: Extract Table Metadata

# COMMAND ----------

# DBTITLE 1,Extract Table Metadata
def extract_table_metadata(catalog, schema, table_names):
    """Extract comprehensive metadata for specified tables."""
    metadata = {}

    for table_name in table_names:
        full_table_name = f"{catalog}.{schema}.{table_name}"

        table_info_query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            comment
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table_name}'
        """
        table_info = spark.sql(table_info_query).collect()

        if not table_info:
            print(f"⚠️ Table {full_table_name} not found")
            continue

        columns_query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            comment
        FROM system.information_schema.columns
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        columns = spark.sql(columns_query).collect()

        detail = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0].asDict()
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]['cnt']

        constraints_query = f"""
        SELECT
            constraint_name,
            constraint_type
        FROM system.information_schema.table_constraints
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table_name}'
        """
        constraints = spark.sql(constraints_query).collect()

        metadata[full_table_name] = {
            "table_info": table_info[0].asDict(),
            "columns": [col.asDict() for col in columns],
            "row_count": row_count,
            "size_bytes": detail.get('sizeInBytes', 0),
            "last_modified": detail.get('lastModified', ''),
            "constraints": [c.asDict() for c in constraints]
        }
        print(f"✅ Extracted metadata for {full_table_name}")

    return metadata

print("=" * 60)
print("EXTRACTING TABLE METADATA")
print("=" * 60)
table_metadata = extract_table_metadata(
    config['catalog'],
    config['schema'],
    config['tables']
)
print(f"\n✅ Metadata extraction complete for {len(table_metadata)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2.2: Display Metadata Summary

# COMMAND ----------

# DBTITLE 1,Display Metadata Summary
summary_data = []
for table_name, metadata in table_metadata.items():
    summary_data.append({
        "Table": table_name.split('.')[-1],
        "Row Count": f"{metadata['row_count']:,}",
        "Size (MB)": f"{metadata['size_bytes'] / (1024*1024):.2f}",
        "Columns": len(metadata['columns']),
        "Constraints": len(metadata['constraints']),
        "Last Modified": str(metadata['last_modified'])
    })

summary_df = pd.DataFrame(summary_data)

print("=" * 80)
print("TABLE METADATA SUMMARY")
print("=" * 80)
print(summary_df.to_string(index=False))

fig = px.bar(
    summary_df,
    x='Table',
    y=[float(s) for s in summary_df['Size (MB)']],
    title='Table Sizes (MB)',
    labels={'y': 'Size (MB)'},
    color='Table'
)
try:
    fig.show()
except Exception:
    fig.show(renderer="png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2.3: Profile Column Statistics

# COMMAND ----------

# DBTITLE 1,Profile Column Statistics
def profile_column_statistics(catalog, schema, table_name):
    """Profile column-level statistics including cardinality, nulls, and PII detection."""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    columns = table_metadata[full_table_name]['columns']
    stats = []

    for col in columns:
        col_name = col['column_name']
        col_type = col['data_type']

        stats_query = f"""
        SELECT
            COUNT(DISTINCT `{col_name}`) as distinct_count,
            COUNT(*) as total_count,
            SUM(CASE WHEN `{col_name}` IS NULL THEN 1 ELSE 0 END) as null_count
        FROM {full_table_name}
        """

        try:
            result = spark.sql(stats_query).collect()[0]
            null_pct = (result['null_count'] / result['total_count'] * 100) if result['total_count'] > 0 else 0
            cardinality = result['distinct_count']

            if cardinality <= 10:
                cardinality_cat = "very_low"
                entity_matching = True
            elif cardinality <= 100:
                cardinality_cat = "low"
                entity_matching = True
            elif cardinality <= 1000:
                cardinality_cat = "medium"
                entity_matching = True
            else:
                cardinality_cat = "high"
                entity_matching = False

            pii_type = None
            if re.search(r'email', col_name, re.IGNORECASE):
                pii_type = "email"
            elif re.search(r'phone|mobile', col_name, re.IGNORECASE):
                pii_type = "phone"
            elif re.search(r'ssn|social', col_name, re.IGNORECASE):
                pii_type = "ssn"

            stats.append({
                "column": col_name,
                "data_type": col_type,
                "distinct_count": cardinality,
                "null_percentage": round(null_pct, 2),
                "cardinality_category": cardinality_cat,
                "recommend_entity_matching": entity_matching,
                "pii_detected": pii_type,
                "recommend_masking": pii_type is not None
            })

        except Exception as e:
            print(f"⚠️ Could not profile {col_name}: {str(e)}")

    return stats

print("=" * 60)
print("PROFILING COLUMN STATISTICS")
print("=" * 60)
column_profiles = {}
for table_name in config['tables']:
    full_name = f"{config['catalog']}.{config['schema']}.{table_name}"
    print(f"Profiling {table_name}...", end=" ")
    column_profiles[full_name] = profile_column_statistics(
        config['catalog'],
        config['schema'],
        table_name
    )
    print(f"✅ {len(column_profiles[full_name])} columns profiled")

print(f"\n✅ Column profiling complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2.4: Display Column Profiles

# COMMAND ----------

# DBTITLE 1,Display Column Profiles
first_table = config['tables'][0]
first_table_fqn = f"{config['catalog']}.{config['schema']}.{first_table}"
first_profile = column_profiles[first_table_fqn]
profile_df = pd.DataFrame(first_profile)

print("=" * 80)
print(f"COLUMN PROFILE: {first_table_fqn}")
print("=" * 80)
print(profile_df[['column', 'data_type', 'distinct_count', 'null_percentage',
                   'cardinality_category', 'recommend_entity_matching', 'pii_detected']].to_string(index=False))

print("\n📊 KEY FINDINGS:")
entity_match_cols = [c['column'] for c in first_profile if c['recommend_entity_matching']]
pii_cols = [c['column'] for c in first_profile if c['pii_detected']]
print(f"  • {len(entity_match_cols)} columns recommended for entity matching: {', '.join(entity_match_cols)}")
print(f"  • {len(pii_cols)} columns contain PII: {', '.join(pii_cols) if pii_cols else 'None'}")
