# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: Configuration & Setup
# MAGIC **Auto-Genie MVP - Stage 0: Configuration & Initialization**
# MAGIC
# MAGIC This notebook handles:
# MAGIC - Installing required dependencies
# MAGIC - Importing libraries
# MAGIC - Loading workspace configuration from `.env`
# MAGIC - Authenticating with the Databricks workspace
# MAGIC - Validating connectivity to Unity Catalog and Genie API

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1.1: Install Required Libraries

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
%pip install databricks-sdk sqlglot scikit-learn plotly pandas numpy python-dotenv --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1.2: Import Libraries

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlglot
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
import re
from collections import Counter

print("✅ All libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1.3: Load Environment & Configuration

# COMMAND ----------

# DBTITLE 1,Load Environment & Configuration
# Walk up from the notebook location to find the project .env file.
# Works whether you run from src/, notebooks/, or the project root.
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
    "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", ""),
    "catalog": os.getenv("AUTO_GENIE_CATALOG", "main"),
    "schema": os.getenv("AUTO_GENIE_SCHEMA", "sales"),
    "tables": os.getenv("AUTO_GENIE_TABLES", "orders,customers,products,order_items,warehouses").split(","),
    "genie_warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
    "lookback_days": int(os.getenv("AUTO_GENIE_LOOKBACK_DAYS", "90")),
    "confidence_threshold": float(os.getenv("AUTO_GENIE_CONFIDENCE_THRESHOLD", "0.75")),
    "output_path": os.getenv("AUTO_GENIE_OUTPUT_PATH", "/dbfs/tmp/auto_genie_outputs"),
}

# DATABRICKS_HOST and DATABRICKS_TOKEN are now in the environment,
# so WorkspaceClient() picks them up automatically.
w = WorkspaceClient()

print("=" * 60)
print("AUTO-GENIE CONFIGURATION")
print("=" * 60)
for key, value in config.items():
    display_val = "***" if "token" in key else value
    print(f"{key:25s}: {display_val}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1.4: Test Database Connectivity

# COMMAND ----------

# DBTITLE 1,Test Database Connectivity
try:
    test_query = f"""
    SELECT COUNT(*) as table_count
    FROM system.information_schema.tables
    WHERE table_catalog = '{config['catalog']}'
      AND table_schema = '{config['schema']}'
    """
    result = spark.sql(test_query).collect()[0]

    print(f"✅ Successfully connected to Unity Catalog")
    print(f"✅ Found {result['table_count']} tables in {config['catalog']}.{config['schema']}")

    current_user = w.current_user.me()
    print(f"✅ Authenticated as: {current_user.executed_by}")
    print(f"✅ User ID: {current_user.id}")

except Exception as e:
    print(f"❌ Connection failed: {str(e)}")
