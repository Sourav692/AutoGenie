# Metric View Automation — Implementation Plan

## Objective

Extend AutoGenie to automatically generate a draft Databricks Metric View YAML
(semantic layer) by analyzing existing table structure, column statistics, and
relationships. This gives end users a ready-to-review YAML they can refine and
register in Unity Catalog instead of starting from scratch.

---

## Output Artifact

One `.yml` file per table (or one unified multi-table file) saved to
`output_path/metric_views/`, conforming to the Databricks Metric View spec:

```
/tmp/auto_genie_outputs/metric_views/
  opportunity_metrics.yml
  accounts_metrics.yml
  opportunityhistory_cube_metrics.yml
  user_metrics.yml
  all_metric_views.yml          # unified view (optional, if joins exist)
  metric_view_report.json       # validation + classification summary
```

---

## Pipeline Position

```
Notebook 01 — Query Intelligence        → example_queries.json
Notebook 02 — Assembly & Validation     → knowledge_store.json
Notebook 03 — Genie Deployment          → Live Genie Space
Notebook 04 — Metric View Generation    → metric_views/*.yml   [NEW]
```

Notebook 04 runs independently — it only needs table metadata and column
profiles, so it can run in parallel with Notebook 01 if desired.

The discovered measures and dimensions from Notebook 04 can optionally feed
back into Notebook 02 to enrich the `sql_expressions` section of the Genie
knowledge store, ensuring consistency between the two artifacts.

---

## Tasks

### TASK-01 — Research: Databricks Metric View YAML Spec
**File:** No code change
**Status:** DONE
**Findings:**

#### Confirmed YAML Structure (v1.1)

```yaml
version: 1.1                          # optional, defaults to 1.1 (requires Runtime 17.2+)
comment: "Description of this view"   # optional, top-level description
source: catalog.schema.table          # REQUIRED — FQN table/view, or inline SQL query
filter: column = 'value'              # optional — SQL boolean, applied as WHERE to all queries

joins:                                # optional — star/snowflake schema joins supported
  - name: join_alias
    source: catalog.schema.other_table
    on: "left_table.col = right_table.col"

dimensions:                           # REQUIRED array
  - name: dim_name                    # snake_case identifier
    expr: column_name                 # SQL expression (use backticks for special chars)
    comment: "Business description"   # optional (v1.1+)

measures:                             # REQUIRED array
  - name: measure_name                # snake_case identifier
    expr: SUM(amount)                 # MUST use aggregate function
    comment: "Business description"   # optional (v1.1+)
    filter: status = 'Won'            # optional — measure-level filter
```

**Key syntax rules:**
- Column names with spaces/special chars → wrap in backticks: `` `column name` ``
- Expressions containing colons → wrap in double quotes: `"CASE WHEN x: y"`
- Multi-line expressions → use YAML `|` block scalar with 2-space indent
- No `display_name` or `data_type` fields in the YAML spec (v1.1)
- Measure `expr` MUST use an aggregate function (SUM, COUNT, AVG, MIN, MAX, COUNT DISTINCT)

#### Deployment Method (Confirmed)

No dedicated `WorkspaceClient` method exists. The correct approach is SQL DDL
executed via `spark.sql()` in a notebook or via the Statement Execution REST API:

```sql
CREATE OR REPLACE VIEW catalog.schema.view_name
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: catalog.schema.table
  dimensions:
    - name: stage
      expr: stagename
  measures:
    - name: total_amount
      expr: SUM(amount)
$$
```

This runs on **Databricks Runtime 17.2+** (SQL warehouse or cluster).

#### Answers to Open Questions

1. **Source field name:** `source` (not `source_table`)
2. **Multi-table joins:** Supported via `joins` array in the YAML
3. **SDK support:** No dedicated method — use `spark.sql(CREATE VIEW ... WITH METRICS)`
4. **`data_type` field:** Does NOT exist in the YAML spec; types are inferred
5. **Max dimensions/measures:** No documented limit
6. **Scope:** Metric views are **schema-level** Unity Catalog objects (like tables/views)

#### Plan Corrections (vs. original draft)

- Remove `display_name` and `data_type` from TASK-06 dict builder (not in spec)
- `source` field (not `source_table`) for single-table views
- Deployment cell in NB 04 uses `spark.sql()` not `WorkspaceClient`
- `pyyaml` already present in `requirements.txt` — no new dependency needed

**Acceptance Criteria:** Met — all fields confirmed, deployment method confirmed.

---

### TASK-02 — Config: Add Metric View Settings to `config.yml`
**File:** `config.yml`
**Status:** DONE
**Description:**
Add a new config block for metric view generation:

```yaml
# Metric View Generation
metric_view_mode: "per_table"            # "per_table" | "unified"
metric_view_output_path: "/dbfs/tmp/auto_genie_outputs/metric_views"
metric_view_min_distinct_for_measure: 10 # skip measures with fewer distinct values
metric_view_max_distinct_for_dimension: 500 # skip dimensions with too many distinct values
```

**Acceptance Criteria:**
- Config keys load correctly via `load_yaml_config`.
- Defaults are safe — running without changes does not break existing notebooks.

---

### TASK-03 — Prompts: Add Metric View Prompts to `prompts.yml`
**File:** `prompts.yml`
**Status:** DONE
**Description:**
Add a new `metric_view_generation` section with system + user prompts.

LLM responsibilities (rule-based handles structure; LLM handles semantics):
- Generate business-friendly `display_name` for each column.
- Write a concise `description` per dimension and measure.
- Confirm or adjust the aggregation function (SUM / AVG / COUNT / COUNT DISTINCT / MIN / MAX).
- Flag columns that should be excluded (e.g., raw IDs with no analytical value).

Prompt must instruct the LLM to:
- Return raw JSON only (no markdown fences).
- Never invent columns not present in the provided schema.
- Use only the provided column names in expressions.

**Acceptance Criteria:**
- Prompt loads via `load_prompts()` without errors.
- LLM response is parseable JSON matching the expected schema.

---

### TASK-04 — Utils: Column Classification Function
**File:** `utils/auto_genie_utils.py`
**Function:** `classify_columns_for_metric_view(table_metadata, column_profiles, config)`
**Status:** DONE
**Description:**
Rule-based classification of every column into one of:
- `time_dimension` — DATE or TIMESTAMP type
- `dimension` — STRING/BOOLEAN with distinct count <= `metric_view_max_distinct_for_dimension`
- `measure` — numeric type (INT, BIGINT, DECIMAL, DOUBLE, FLOAT) with distinct
  count >= `metric_view_min_distinct_for_measure`
- `identifier` — high-cardinality strings or columns ending in `_id` / named `id`
- `skip` — columns that are not analytically useful (e.g., raw JSON, binary)

Returns a dict keyed by fully-qualified table name:
```python
{
  "vc_catalog.schema.opportunity": {
    "time_dimensions": [...],
    "dimensions": [...],
    "measures": [...],
    "identifiers": [...],
    "skipped": [...]
  }
}
```

**Acceptance Criteria:**
- All 52 columns across 4 tables are classified without errors.
- Numeric columns like `amount` are classified as measures.
- `createddate`, `closedate` are classified as time dimensions.
- `stagename`, `region`, `industry` are classified as dimensions.
- `opportunityid`, `accountid`, `ownerid` are classified as identifiers.

---

### TASK-05 — Utils: Fact/Dimension Table Detection
**File:** `utils/auto_genie_utils.py`
**Function:** `detect_table_roles(table_metadata, relationships, column_classifications)`
**Status:** DONE
**Description:**
Classify each table as `fact` or `dimension`:
- **Fact table signals:** high row count, many numeric/measure columns, many FK-style
  columns pointing to other tables, presence of date columns.
- **Dimension table signals:** lower row count, many string/categorical columns,
  few or no FK references to other tables, likely referenced by fact tables.

Returns:
```python
{
  "vc_catalog.schema.opportunity": "fact",
  "vc_catalog.schema.accounts": "dimension",
  "vc_catalog.schema.opportunityhistory_cube": "fact",
  "vc_catalog.schema.user": "dimension"
}
```

Used to decide: which tables become metric view sources, which become
dimension lookups in joins.

**Acceptance Criteria:**
- `opportunity` and `opportunityhistory_cube` detected as fact tables.
- `accounts` and `user` detected as dimension tables.
- Works gracefully when relationships list is empty.

---

### TASK-06 — Utils: Metric View Dict Builder
**File:** `utils/auto_genie_utils.py`
**Function:** `build_metric_view_dict(table_fqn, table_role, column_classifications, llm_enrichments, relationships, config)`
**Status:** DONE
**Description:**
Assembles the Python dict that represents a single Metric View. Fields match
the confirmed v1.1 YAML spec (from TASK-01):
- `version`: "1.1"
- `comment`: top-level description (table purpose)
- `source`: fully-qualified table name (single-table mode) or SQL SELECT with
  LEFT JOINs (unified mode, when relationships exist)
- `joins`: list of join dicts (unified mode only)
- `filter`: optional — omit if no global filter applies
- `dimensions` list (each with `name`, `expr`, `comment`)
- `measures` list (each with `name`, `expr`, `comment`, optional `filter`)

NOTE: No `display_name` or `data_type` fields — these are not part of the spec.
LLM enrichment populates `comment` (used as the business description per field).

For the `name` field: snake_case derived from table simple name + `_metrics` suffix.

For `source_sql` (unified mode): generate a LEFT JOIN chain from fact table to
all related dimension tables using discovered relationships.

**Acceptance Criteria:**
- Output dict passes YAML serialization without errors.
- `expr` values reference valid column names from the schema.
- Unified mode generates a syntactically valid SQL JOIN.

---

### TASK-07 — Utils: LLM Enrichment for Semantic Naming
**File:** `utils/auto_genie_utils.py`
**Function:** `enrich_metric_view_with_llm(table_fqn, column_classifications, table_metadata, prompts_cfg, config)`
**Status:** DONE
**Description:**
Calls the LLM to enrich each classified column with:
- `display_name` — human-readable label (e.g., "Close Date", "Total Pipeline Amount")
- `description` — 1-sentence business explanation
- `aggregation` — confirmed/adjusted aggregation for measures (SUM/AVG/COUNT/etc.)
- `exclude` — boolean, LLM can flag columns as not analytically useful

Uses same retry pattern as Notebook 01:
1. Full schema attempt
2. Minimal schema attempt (column names + types only)
3. Rule-based fallback (snake_case → Title Case for display_name, no description)

Returns an enrichment dict keyed by column name.

**Acceptance Criteria:**
- Returns valid enrichment for all classified columns.
- Rule-based fallback produces usable (if plain) output.
- No hallucinated column names in the response.

---

### TASK-08 — Utils: YAML Serializer and Validator
**File:** `utils/auto_genie_utils.py`
**Function:** `serialize_and_validate_metric_view(metric_view_dict, table_metadata)`
**Status:** DONE
**Description:**
Two responsibilities:
1. **Serialize** the dict to a YAML string using `PyYAML` with clean formatting
   (block style, consistent indentation, quoted expressions where needed).
2. **Validate** expressions using `sqlglot`:
   - Wrap each measure `expr` in `SELECT {expr} FROM {source_table} LIMIT 1`
     and parse it.
   - Wrap each dimension `expr` similarly.
   - Return a validation report with pass/fail per expression.

Returns:
```python
{
  "yaml_string": "...",
  "validation": {
    "measures": {"total_pipeline": "passed", ...},
    "dimensions": {"stagename": "passed", ...},
    "time_dimensions": {"createddate": "passed", ...}
  }
}
```

**Acceptance Criteria:**
- Output YAML is parseable by `yaml.safe_load`.
- Validation report correctly flags invalid expressions.
- Valid expressions pass `sqlglot.parse_one` without errors.

---

### TASK-09 — Notebook: Create `scripts/04_metric_view_generation.ipynb`
**File:** `scripts/04_metric_view_generation.ipynb`
**Status:** DONE
**Description:**
New notebook with the following cells:

| Cell | Title | What it does |
|------|-------|--------------|
| 4.0 | Environment & Config | Load config, prompts, spark, imports |
| 4.1 | Load Table Metadata & Profiles | Reuse `extract_table_metadata`, `profile_column_statistics` |
| 4.2 | Rule-Based Column Classification | Call `classify_columns_for_metric_view`, print summary table |
| 4.3 | Table Role Detection | Call `detect_table_roles`, print fact/dim assignment |
| 4.4 | LLM Semantic Enrichment | Call `enrich_metric_view_with_llm` per table, show enrichment |
| 4.5 | Build Metric View Dicts | Call `build_metric_view_dict` for each table |
| 4.6 | Serialize & Validate YAML | Call `serialize_and_validate_metric_view`, print validation report |
| 4.7 | Save Outputs | Write per-table YAMLs + combined file + validation report |

**Acceptance Criteria:**
- Notebook runs end-to-end on the 4 configured tables.
- At least one valid `.yml` file is produced per table.
- Validation report shows all expressions passing or clearly flags failures.
- Human-readable summary printed at the end.

---

### TASK-10 — Integration: Feed Metric View Back into Knowledge Store (Optional)
**File:** `utils/auto_genie_utils.py`, `scripts/02_assembly_validation.ipynb`
**Status:** TODO (lower priority — implement after TASK-09 is validated)
**Description:**
If `metric_views/` output exists, Notebook 02 can optionally load it and:
- Replace the rule-based `sql_expressions.measures` with the validated
  metric view measures (richer descriptions, better naming).
- Replace `sql_expressions.filters` with metric view dimensions as filter hints.
- Add a note in `global_instructions` that a metric view is available.

Controlled by a config flag: `enrich_genie_from_metric_view: true`

**Acceptance Criteria:**
- When enabled, knowledge store measures/dimensions match metric view.
- When disabled or metric view output is absent, Notebook 02 runs unchanged.

---

## Execution Order

1. TASK-01 (Research) — blocks TASK-02 through TASK-09 on spec details
2. TASK-02, TASK-03 — config + prompts (can be done in parallel)
3. TASK-04, TASK-05 — util functions (can be done in parallel)
4. TASK-06 — depends on TASK-04 and TASK-05
5. TASK-07 — depends on TASK-03 and TASK-04
6. TASK-08 — depends on TASK-06
7. TASK-09 — depends on all util tasks (TASK-04 through TASK-08)
8. TASK-10 — depends on TASK-09 being validated

---

## Dependencies (New Packages)

| Package | Already in requirements.txt? | Purpose |
|---------|------------------------------|---------|
| `pyyaml` | YES | YAML serialization |
| `sqlglot` | YES | SQL expression validation |
| `databricks-sdk` | YES | Workspace client (auth, not used for metric view creation) |

No new dependencies required. Metric views are deployed via `spark.sql()` DDL.

---

## Open Questions

All resolved in TASK-01. See findings section above.
