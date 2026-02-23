# AutoGenie Framework — Executive Overview

## What is AutoGenie?

AutoGenie is an **automation framework** that creates production-ready **Databricks AI/BI Genie Spaces** from any Unity Catalog schema — without manual configuration. It reads your data, learns from how people already query it, and deploys a fully configured natural-language analytics interface that business users can start asking questions to immediately.

**In short:** Point it at your data, and it builds a Genie Space that business users can talk to.

---

## The Problem It Solves

Setting up a Databricks AI/BI Genie Space manually requires a data engineer to:

- Write table descriptions and column-level documentation
- Define join relationships between tables
- Create SQL measures, filters, and dimensions
- Come up with representative sample questions
- Build and validate the API payload
- Deploy, verify, and test the space

**This process typically takes 1–2 days per schema.** For organizations with dozens of schemas, this becomes a significant bottleneck blocking self-service analytics adoption.

AutoGenie reduces this to a **single pipeline run (~30 minutes)**, fully automated.

---

## How It Works — Three Stages

### Stage 1: Query Intelligence & Pattern Mining
- Connects to **Unity Catalog** to extract table and column metadata
- Profiles every column (cardinality, null rates, PII detection)
- Mines **90 days of query history** from `system.query.history` to understand what users actually ask
- Parses SQL using **sqlglot** and clusters similar queries using **TF-IDF + DBSCAN** to identify usage patterns
- Uses an **LLM (Claude via ChatDatabricks)** to generate additional sample queries from schema context
- Merges history-based and LLM-generated queries into a validated set of **20 representative sample questions**

### Stage 2: Knowledge Store Assembly & Validation
- Auto-generates **table descriptions** with business context (purpose, key columns, join hints)
- Discovers **table relationships** through three methods: declared foreign keys, naming pattern analysis, and query history mining
- Creates **SQL expressions** — measures (SUM, AVG, COUNT), filters (date ranges, value thresholds), and dimensions (year, month, quarter)
- Detects the **business domain** (Sales, E-Commerce, Finance, HR, Marketing, Support) and generates domain-specific KPIs and best practices
- Assembles everything into a **Genie API-compatible JSON payload**
- Runs **structure validation** (required fields, completeness) and **SQL syntax validation** (every expression parsed through sqlglot) before proceeding

### Stage 3: Genie Space Deployment
- Packages the knowledge store into the Genie **serialized_space v2 format**
- Runs an **LLM-based payload validation** to check schema conformance (with safety guards to preserve table and query counts)
- Deploys the space via the **Databricks SDK** (`WorkspaceClient.genie.create_space()`)
- Includes **automatic fallback logic** — if the API rejects structured join specs, it embeds joins as text instructions and retries
- **Verifies** the deployed space by reading back the configuration from the API
- **Tests** the space with 3 sample conversations to confirm it responds correctly
- Outputs a **deployment summary** with the space URL, ready to share with users

---

## Key Features

| Feature | Description |
|---|---|
| **Zero-touch setup** | Point it at a catalog + schema + table list in `config.yml` and run |
| **Query history mining** | Learns from real user behavior — 90 days of SQL history parsed and clustered |
| **Multi-method relationship discovery** | Finds table joins via foreign keys, column naming patterns, and query history |
| **Automatic domain detection** | Recognizes Sales, E-Commerce, Finance, HR, Marketing, and Support domains |
| **Business KPI generation** | Generates domain-specific KPIs (e.g., Win Rate, Pipeline Value, Weighted Pipeline for Sales) |
| **LLM-augmented intelligence** | Uses Claude to generate sample queries and validate payloads, with rule-based fallback if guardrails trigger |
| **3-tier LLM fallback** | Full schema prompt → minimal schema prompt → rule-based generation (never fails) |
| **Column profiling & PII detection** | Profiles cardinality, null rates, and flags sensitive columns (email, phone, SSN) |
| **SQL syntax validation** | Every expression validated through sqlglot (Databricks dialect) before deployment |
| **Automated deployment & testing** | Creates the space, verifies configuration, and runs test conversations — all automated |
| **API fallback handling** | If structured join_specs fail, automatically rebuilds with text-based instructions |
| **Configurable prompts** | All LLM prompts externalized in `prompts.yml` — tunable without code changes |
| **Reusable utility library** | `auto_genie_utils.py` provides 25+ shared functions usable across projects |

---

## Technology Stack

| Component | Technology |
|---|---|
| Compute | Databricks Connect (remote Spark) |
| Catalog | Unity Catalog (information_schema) |
| Query History | system.query.history |
| SQL Parsing | sqlglot (Databricks dialect) |
| ML Clustering | scikit-learn (TF-IDF + DBSCAN) |
| LLM | ChatDatabricks (Claude) |
| LLM Framework | LangChain |
| Deployment | Databricks SDK (WorkspaceClient) |
| Configuration | YAML + dotenv |

---

## ROI & Business Impact

### Time Savings
| Activity | Manual Effort | With AutoGenie | Savings |
|---|---|---|---|
| Table documentation & descriptions | 2–4 hours | Automated | 100% |
| Relationship mapping & join definitions | 1–2 hours | Automated | 100% |
| SQL expression creation (measures, filters, dimensions) | 2–3 hours | Automated | 100% |
| Sample query curation | 1–2 hours | Automated | 100% |
| Payload assembly & validation | 1–2 hours | Automated | 100% |
| Deployment & testing | 30–60 min | Automated | 100% |
| **Total per schema** | **8–16 hours (1–2 days)** | **~30 minutes** | **~95%** |

### Scaling Impact
| Scenario | Manual Cost | With AutoGenie |
|---|---|---|
| 1 Genie Space | 1–2 days of data engineering | 30 minutes, unattended |
| 10 Genie Spaces | 2–4 weeks | 1 day (batch run) |
| 50 Genie Spaces | 3–6 months | 1 week |

### Quality Improvements
- **Consistency** — Every Genie Space follows the same structure, naming conventions, and best practices
- **Accuracy** — SQL expressions are validated before deployment; no broken queries reach end users
- **Coverage** — Automated profiling catches columns and patterns a manual setup might miss
- **Freshness** — Re-running the pipeline picks up new tables, columns, and query patterns automatically

### Strategic Value
- **Accelerates AI/BI adoption** — Removes the setup bottleneck, enabling business teams to start asking questions sooner
- **Reduces dependency on data engineering** — Business stakeholders no longer need to wait for manual Genie Space setup
- **Democratizes data access** — Makes natural-language analytics available across more schemas and teams faster
- **Leverages existing investment** — Builds on Unity Catalog, query history, and Databricks infrastructure already in place

---

## Sample Output

In its first run against a **Sales Pipeline** schema (4 tables, 52 columns, 283 historical queries), AutoGenie produced:

- 20 validated sample questions (1 from history clusters + 19 LLM-generated)
- 8 SQL measures, 31 filters, 9 dimensions
- 22 business KPIs
- ~10,000 characters of business-driven instructions
- A fully deployed Genie Space, tested and ready for use

---

## Getting Started

1. Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env`
2. Configure your target schema and tables in `config.yml`
3. Run the three notebooks in sequence:
   - `01_query_intelligence.ipynb` — mines queries and generates examples
   - `02_assembly_validation.ipynb` — assembles and validates the knowledge store
   - `03_genie_deployment.ipynb` — deploys and tests the Genie Space
4. Share the Genie Space URL with your business users
