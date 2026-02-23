# AutoGenie Framework — Leadership Overview

## What is AutoGenie?

AutoGenie is an **intelligent automation framework** that creates production-ready **Databricks AI/BI Genie Spaces** with minimal manual effort. It takes raw data tables in Unity Catalog and automatically transforms them into fully functional, natural-language query interfaces that business users can interact with — no SQL knowledge required.

In simple terms: **AutoGenie turns your data into a conversational analytics experience — automatically.**

---

## The Problem It Solves

Setting up a Databricks Genie Space manually involves:

- Writing table descriptions and column-level documentation
- Defining table relationships and join conditions
- Creating sample SQL queries for business use cases
- Generating SQL measures, filters, and dimensions
- Writing business context instructions and KPIs
- Validating the payload and deploying via the API

This process is **tedious, error-prone, and can take days to weeks per Genie Space** when done manually — especially for schemas with many tables and complex relationships.

AutoGenie **reduces this from days to minutes**.

---

## How It Works — 3 Simple Stages

| Stage | What Happens | Output |
|-------|-------------|--------|
| **1. Query Intelligence** | Mines Unity Catalog metadata and historical query logs, clusters common query patterns using ML, and generates representative sample queries using LLM | `example_queries.json` — 20 validated sample queries |
| **2. Knowledge Assembly** | Builds a complete knowledge store with table instructions, join definitions, SQL expressions (measures/filters/dimensions), KPIs, and business context | `knowledge_store.json` — Genie API-ready payload |
| **3. Deployment** | Validates the payload, deploys the Genie Space via Databricks SDK, verifies deployment, and runs automated test conversations | **Live Genie Space** accessible by business users |

---

## Key Features

### Intelligent Metadata Discovery
- Automatically extracts table and column metadata from **Unity Catalog**
- Profiles every column for cardinality, null percentages, and PII detection
- Flags sensitive fields (email, phone, SSN) for data governance awareness

### Smart Relationship Discovery (3-Layer Approach)
- **Declared Foreign Keys** — reads existing constraints from Unity Catalog
- **Naming Pattern Analysis** — infers joins from column naming conventions (e.g., `account_id` → `accounts`)
- **Query History Mining** — discovers implicit joins by analyzing how users actually query the data
- All relationships are merged, deduplicated, and ranked by confidence score

### Query Intelligence & Pattern Mining
- Analyzes up to **10,000 historical queries** from `system.query.history`
- Parses SQL using `sqlglot` to extract tables, columns, aggregations, and joins
- Clusters queries into patterns using **TF-IDF + DBSCAN** machine learning
- Generates additional queries via LLM with a **3-tier fallback** (full schema → minimal schema → rule-based)
- History-based queries get priority over LLM-generated ones

### Business Domain Intelligence
- Automatically **detects the business domain** (Sales, E-Commerce, Finance, HR, Marketing, Support)
- Generates domain-specific **KPIs and best practices** without any LLM calls
- Creates human-readable business labels for technical column names
- Produces filtering guidance from low-cardinality dimensions

### Automated SQL Expression Generation
- Generates **measures** (SUM, AVG, COUNT) from monetary and numeric columns
- Generates **filters** (last 30 days, YTD, high-value) from date and categorical columns
- Generates **dimensions** (Year, Month, Quarter) for time-based analysis

### Multi-Layer Validation
- **Structure validation** — checks required fields, table counts, and completeness
- **SQL syntax validation** — parses every SQL expression using `sqlglot` Databricks dialect
- **Consistency checks** — verifies description coverage, join completeness, and instruction quality
- **LLM payload validation** — final conformance check against the Genie API schema

### Automated Deployment & Testing
- Deploys directly via the **Databricks SDK** (`WorkspaceClient.genie.create_space`)
- Built-in **fallback handling** — if join specs are rejected by the API, joins are embedded as text instructions
- **Post-deployment verification** — reads back the space config to confirm all components deployed
- **Automated test conversations** — runs 3 sample questions through the deployed Genie Space to verify it works

### Configurable & Extensible
- Simple **YAML-based configuration** — just specify your catalog, schema, and tables
- Separate **prompt configuration** — LLM prompts live in `prompts.yml` for easy tuning
- Adjustable parameters: lookback window, confidence threshold, output paths
- Supports any business domain through automatic domain detection

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Data Platform | Databricks (Unity Catalog, SQL Warehouse) |
| Compute | Databricks Connect (remote Spark) |
| LLM | Claude via ChatDatabricks (Foundation Model API) |
| SQL Parsing | sqlglot (Databricks dialect) |
| ML Clustering | scikit-learn (TF-IDF + DBSCAN) |
| LLM Framework | LangChain |
| Deployment | Databricks SDK (WorkspaceClient) |
| Configuration | YAML + dotenv |

---

## ROI & Business Impact

### Time Savings
| Activity | Manual Effort | With AutoGenie | Savings |
|----------|:------------:|:--------------:|:-------:|
| Table documentation & instructions | 4–8 hours | Automated | ~95% |
| Relationship mapping & join definitions | 2–4 hours | Automated | ~95% |
| Sample query creation | 3–6 hours | Automated | ~95% |
| SQL expression generation (measures, filters, dimensions) | 2–4 hours | Automated | ~95% |
| Business context & KPI definition | 2–3 hours | Automated | ~90% |
| Payload validation & deployment | 1–2 hours | Automated | ~95% |
| **Total per Genie Space** | **14–27 hours (2–4 days)** | **~30 minutes** | **~95%** |

### Quantified ROI
- **Per Genie Space**: Saves **13–26 hours** of analyst/engineer time
- **At scale (10 Genie Spaces)**: Saves **130–260 hours** (~1–1.5 FTE months)
- **At scale (50 Genie Spaces)**: Saves **650–1,300 hours** (~4–8 FTE months)
- Assuming a blended rate of $75–$100/hr for data engineers, each Genie Space saves **$975–$2,600** in labor costs

### Quality & Consistency Benefits
- **Eliminates human error** in SQL expression and join definition authoring
- **Standardized output** — every Genie Space follows the same structure and quality bar
- **History-driven intelligence** — sample queries reflect how users actually use the data, not just theoretical use cases
- **Automated validation** catches issues before deployment, reducing post-deployment fixes
- **Domain-aware** — best practices and KPIs are tailored to the detected business domain

### Strategic Benefits
- **Accelerates AI/BI adoption** — removes the bottleneck of manual Genie Space setup
- **Democratizes data access** — business users can query data in natural language without waiting for engineering support
- **Scales linearly** — adding new Genie Spaces requires only a config change (catalog, schema, tables)
- **Reduces dependency** on specialized data engineers for Genie Space configuration
- **Leverages existing investments** — uses Unity Catalog metadata and query history already being captured

---

## Getting Started

1. Set your Databricks credentials in `.env`
2. Define your target tables in `config.yml`
3. Run the three notebooks in sequence
4. Your Genie Space is live and ready for business users

---

*AutoGenie — From raw data to conversational analytics in minutes, not days.*
