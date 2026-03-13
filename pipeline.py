"""
AutoGenie Pipeline — 3-Stage Genie Space Creation
===================================================
Consolidates the logic from:
  - scripts/01_query_intelligence.ipynb
  - scripts/02_assembly_validation.ipynb
  - scripts/03_genie_deployment.ipynb

Exposes a single generator `run_full_pipeline()` that yields
(log_message, result_dict_or_None) tuples for streaming progress.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Generator

import sqlglot
import yaml
from sqlglot import exp


# ─── Prompts loader ─────────────────────────────────────────────────────────

def load_prompts() -> dict[str, Any]:
    prompts_path = Path(__file__).parent / "prompts.yml"
    if prompts_path.exists():
        with open(prompts_path) as f:
            return yaml.safe_load(f)
    return {}


# ═════════════════════════════════════════════════════════════════════════════
# Stage 1 helpers — Query Intelligence & Pattern Mining
# ═════════════════════════════════════════════════════════════════════════════

def _extract_query_history(spark, catalog: str, schema: str, lookback_days: int):
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    query = f"""
    SELECT query_source.sql_query_id AS query_id, statement_text, executed_by,
           start_time, end_time, total_duration_ms, statement_type, compute.warehouse_id
    FROM system.query.history
    WHERE start_time >= '{start_date}'
      AND statement_type = 'SELECT' AND error_message IS NULL
      AND (LOWER(statement_text) LIKE '%{catalog}.{schema}%'
           OR LOWER(statement_text) LIKE '%{schema}.%')
    ORDER BY start_time DESC LIMIT 10000
    """
    return [r.asDict() for r in spark.sql(query).collect()]


def _parse_and_cluster(query_history: list[dict]):
    from sklearn.cluster import DBSCAN
    from sklearn.feature_extraction.text import TfidfVectorizer

    parsed = []
    for q in query_history:
        try:
            stmts = sqlglot.parse(q["statement_text"], read="databricks")
            if not stmts:
                continue
            s = stmts[0]
            parsed.append({
                "query_id": q["query_id"],
                "original_sql": q["statement_text"],
                "tables": [t.name for t in s.find_all(exp.Table)],
                "columns": [c.name for c in s.find_all(exp.Column)],
                "aggregations": [a.sql() for a in s.find_all(exp.AggFunc)],
                "joins": len(list(s.find_all(exp.Join))),
                "where_clauses": len(list(s.find_all(exp.Where))),
                "group_by": len(list(s.find_all(exp.Group))) > 0,
                "user": q["executed_by"],
                "duration_ms": q["total_duration_ms"],
            })
        except Exception:
            continue

    if not parsed:
        return parsed, {}

    features = []
    for p in parsed:
        features.append(
            " ".join(p["tables"]) + " " + " ".join(p["aggregations"]) + " "
            + ("join" * p["joins"]) + " " + ("where" * p["where_clauses"]) + " "
            + ("groupby" if p["group_by"] else "")
        )
    X = TfidfVectorizer(max_features=50, stop_words="english").fit_transform(features)
    labels = DBSCAN(eps=0.4, min_samples=2, metric="cosine").fit_predict(X.toarray())

    clusters: dict[int, list] = {}
    for idx, cid in enumerate(labels):
        if cid != -1:
            clusters.setdefault(cid, []).append(parsed[idx])
    return parsed, clusters


def _examples_from_clusters(clusters: dict, top_n: int = 10) -> list[dict]:
    examples = []
    for _, queries in sorted(clusters.items(), key=lambda x: len(x[1]), reverse=True)[:top_n]:
        tpl = min(queries, key=lambda q: len(q["original_sql"]))
        tables = Counter()
        for q in queries:
            tables.update(q["tables"])
        main = [t for t, _ in tables.most_common(2)]

        has_join, has_agg, has_grp = tpl["joins"] > 0, bool(tpl["aggregations"]), tpl["group_by"]
        if has_agg and has_grp:
            nl = (f"Show aggregated metrics from {' and '.join(main)}" if has_join
                  else f"Calculate summary statistics for {main[0]}")
        elif has_join:
            nl = f"Combine data from {' and '.join(main)}"
        else:
            nl = f"Query {main[0]} table"

        sql = tpl["original_sql"]
        sql = re.sub(r"'\d{4}-\d{2}-\d{2}'", "CURRENT_DATE - INTERVAL {{days}} DAYS", sql, count=1)
        sql = re.sub(r">\s*\d+", "> {{threshold}}", sql, count=1)

        examples.append({
            "natural_language": nl, "sql": sql, "frequency": len(queries),
            "avg_execution_time_ms": sum(q["duration_ms"] for q in queries) / len(queries),
            "tables_used": main, "source": "history_cluster",
        })
    return examples


def _schema_summary(table_metadata, column_profiles):
    lines = []
    for fqn, meta in table_metadata.items():
        lines.append(f"\nTABLE: {fqn}  (rows: {meta['row_count']:,})")
        for col in meta["columns"]:
            cn, ct, nl = col["column_name"], col["data_type"], col["is_nullable"]
            prof = next((p for p in column_profiles.get(fqn, []) if p["column"] == cn), None)
            extras = []
            if prof:
                extras += [f"distinct={prof['distinct_count']}", f"nulls={prof['null_percentage']}%"]
            e = f"  -- {', '.join(extras)}" if extras else ""
            lines.append(f"  {cn} {ct} {'NULL' if nl == 'YES' else 'NOT NULL'}{e}")
    return "\n".join(lines)


def _minimal_schema(table_metadata):
    return "\n".join(
        f"{fqn} ({m['row_count']:,} rows): {', '.join(c['column_name'] for c in m['columns'])}"
        for fqn, m in table_metadata.items()
    )


def _rule_based_queries(table_metadata, column_profiles, config, n=20):
    queries: list[dict] = []
    for fqn, meta in table_metadata.items():
        simple = fqn.split(".")[-1]
        cols = meta["columns"]
        numeric = [c for c in cols if any(t in c["data_type"].upper() for t in ("INT", "BIGINT", "DECIMAL", "DOUBLE", "FLOAT", "LONG"))]
        dates = [c for c in cols if c["data_type"].upper() in ("DATE", "TIMESTAMP")]
        strings = [c for c in cols if "STRING" in c["data_type"].upper() or "VARCHAR" in c["data_type"].upper()]

        queries.append({"natural_language": f"Show the first 10 rows from {simple}", "sql": f"SELECT * FROM {fqn} LIMIT 10", "tables_used": [simple]})
        queries.append({"natural_language": f"How many records are in {simple}?", "sql": f"SELECT COUNT(*) AS total_records FROM {fqn}", "tables_used": [simple]})
        if numeric:
            nc = numeric[0]["column_name"]
            queries.append({"natural_language": f"What is the average {nc} in {simple}?", "sql": f"SELECT AVG(`{nc}`) AS avg_value FROM {fqn}", "tables_used": [simple]})
        if dates:
            dc = dates[0]["column_name"]
            queries.append({"natural_language": f"Show {simple} records from the last 30 days", "sql": f"SELECT * FROM {fqn} WHERE `{dc}` >= CURRENT_DATE - INTERVAL 30 DAYS ORDER BY `{dc}` DESC LIMIT 100", "tables_used": [simple]})
        if strings:
            sc = strings[0]["column_name"]
            queries.append({"natural_language": f"Distinct values of {sc} in {simple}", "sql": f"SELECT DISTINCT `{sc}`, COUNT(*) AS cnt FROM {fqn} GROUP BY `{sc}` ORDER BY cnt DESC", "tables_used": [simple]})

    fqns = list(table_metadata.keys())
    for i in range(len(fqns)):
        for j in range(i + 1, len(fqns)):
            shared = {c["column_name"] for c in table_metadata[fqns[i]]["columns"]} & {c["column_name"] for c in table_metadata[fqns[j]]["columns"]}
            jcols = [c for c in shared if c.endswith("_id") or c == "id"]
            if jcols:
                jc = jcols[0]
                si, sj = fqns[i].split(".")[-1], fqns[j].split(".")[-1]
                queries.append({"natural_language": f"Join {si} with {sj} on {jc}", "sql": f"SELECT a.*, b.* FROM {fqns[i]} a INNER JOIN {fqns[j]} b ON a.`{jc}` = b.`{jc}` LIMIT 100", "tables_used": [si, sj]})

    for q in queries:
        q.update({"source": "rule_based", "frequency": 0, "avg_execution_time_ms": 0})
    return queries[:n]


def _llm_queries(table_metadata, column_profiles, config, prompts_cfg, in_scope_simple, n=20):
    from databricks_langchain import ChatDatabricks
    from langchain_core.messages import HumanMessage, SystemMessage

    model = prompts_cfg.get("llm_model", "databricks-claude-opus-4-6")
    fqn_list = ", ".join(sorted(table_metadata.keys()))
    table_list = ", ".join(sorted(fqn.split(".")[-1] for fqn in table_metadata))
    first_table = table_list.split(", ")[0]

    sys_tpl = prompts_cfg.get("query_generation", {}).get("system", "")
    usr_tpl = prompts_cfg.get("query_generation", {}).get("user", "")
    sys_prompt = sys_tpl.format(fqn_list=fqn_list)

    for label, schema_text in [("full", _schema_summary(table_metadata, column_profiles)),
                                ("minimal", _minimal_schema(table_metadata))]:
        user_prompt = usr_tpl.format(
            catalog=config["catalog"], schema=config["schema"], schema_text=schema_text,
            n_queries=n, first_table=first_table, table_list=table_list,
        )
        try:
            llm = ChatDatabricks(model=model)
            raw = llm.invoke([SystemMessage(content=sys_prompt), HumanMessage(content=user_prompt)]).content.strip()
            if raw.startswith("```"):
                raw = re.sub(r"^```(?:json)?\s*", "", raw)
                raw = re.sub(r"\s*```$", "", raw)
            qs = json.loads(raw)
            valid = []
            for q in qs:
                if all(t in in_scope_simple for t in q.get("tables_used", [])):
                    q.update({"source": "llm", "frequency": 0, "avg_execution_time_ms": 0})
                    valid.append(q)
            return valid
        except Exception:
            continue
    return _rule_based_queries(table_metadata, column_profiles, config, n)


def _merge_queries(history: list[dict], llm: list[dict], table_metadata, n: int = 20):
    in_scope = {fqn.split(".")[-1] for fqn in table_metadata}
    seen: set[str] = set()
    deduped: list[dict] = []
    for ex in history + llm:
        if not all(t in in_scope for t in ex.get("tables_used", [])):
            continue
        key = ex["natural_language"].strip().lower()
        if key not in seen:
            seen.add(key)
            deduped.append(ex)

    hist = sorted([e for e in deduped if e.get("source") == "history_cluster"],
                  key=lambda x: x.get("frequency", 0), reverse=True)
    rest = [e for e in deduped if e.get("source") != "history_cluster"]

    final = hist[:n]
    remaining = n - len(final)
    if remaining > 0:
        final.extend(rest[:remaining])
    for idx, q in enumerate(final, 1):
        q["query_id"] = idx
    return final


# ═════════════════════════════════════════════════════════════════════════════
# Stage 2 helpers — Knowledge Store Assembly & Validation
# ═════════════════════════════════════════════════════════════════════════════

def _assemble_knowledge_store(config, table_metadata, column_profiles,
                              relationships, table_instructions,
                              join_instructions, sql_expressions,
                              example_queries, biz_instructions):
    ks: dict[str, Any] = {
        "space_name": f"{config['schema'].replace('_', ' ').title()} Analytics Space",
        "space_description": f"Auto-generated Genie space for {config['catalog']}.{config['schema']}",
        "catalog": config["catalog"],
        "schema": config["schema"],
        "warehouse_id": config["genie_warehouse_id"],
        "created_at": datetime.now().isoformat(),
        "configuration": {
            "lookback_days": config["lookback_days"],
            "confidence_threshold": config["confidence_threshold"],
            "tables_included": config["tables"],
        },
        "tables": [],
        "joins": [],
        "sql_expressions": {"measures": [], "filters": [], "dimensions": []},
        "example_queries": [],
        "global_instructions": "",
    }

    for tbl_name, instr in table_instructions.items():
        meta = table_metadata[tbl_name]
        ks["tables"].append({
            "catalog": config["catalog"], "schema": config["schema"],
            "table_name": tbl_name.split(".")[-1], "full_name": tbl_name,
            "description": instr["description"], "row_count": instr["row_count"],
            "last_updated": instr["last_updated"],
            "columns": [{"name": c["column_name"], "type": c["data_type"],
                         "nullable": c["is_nullable"], "comment": c.get("comment", "")}
                        for c in meta["columns"]],
            "key_columns": instr["key_columns"],
            "join_hints": instr["join_hints"],
            "common_use_cases": instr["common_use_cases"],
        })

    for ji in join_instructions:
        ks["joins"].append({
            "source_table": ji["source_table"], "source_column": ji["source_column"],
            "target_table": ji["target_table"], "target_column": ji["target_column"],
            "join_type": ji["recommended_join_type"],
            "explanation": ji["explanation"], "confidence": ji["confidence"],
        })

    ks["sql_expressions"] = sql_expressions

    for ex in example_queries:
        ks["example_queries"].append({
            "natural_language": ex["natural_language"], "sql": ex["sql"],
            "frequency": ex["frequency"],
            "avg_execution_time_ms": ex["avg_execution_time_ms"],
            "tables_used": ex["tables_used"],
        })

    ks["global_instructions"] = biz_instructions["global_instructions"]
    ks["domain"] = biz_instructions["domain"]
    ks["kpis"] = biz_instructions["kpis"]
    return ks


# ═════════════════════════════════════════════════════════════════════════════
# Stage 3 helpers — Genie Space Deployment
# ═════════════════════════════════════════════════════════════════════════════

def _uid():
    return uuid.uuid4().hex


def _join_text(ks):
    lines = ["TABLE RELATIONSHIPS (JOIN CONDITIONS):"]
    for j in ks.get("joins", []):
        src = j["source_table"].split(".")[-1]
        tgt = j["target_table"].split(".")[-1]
        lines.append(f"- {src}.{j['source_column']} = {tgt}.{j['target_column']} "
                     f"({j.get('join_type', 'INNER')} JOIN) — {j.get('explanation', '')}")
    return "\n".join(lines)


def _build_serialized_space(ks, include_join_specs=True):
    tables = []
    for tbl in ks["tables"]:
        entry = {"identifier": tbl["full_name"]}
        if tbl.get("description"):
            entry["description"] = [tbl["description"][:500]]
        tables.append(entry)

    sample_questions = sorted([
        {"id": _uid(), "question": [eq["natural_language"]]}
        for eq in ks.get("example_queries", [])
    ], key=lambda x: x["id"])

    full_text = (ks.get("global_instructions") or "").strip()
    if not include_join_specs and ks.get("joins"):
        full_text += "\n\n" + _join_text(ks)
    text_instructions = [{"id": _uid(), "content": [full_text]}] if full_text else []

    join_specs = []
    if include_join_specs:
        for j in ks.get("joins", []):
            src_short = j["source_table"].split(".")[-1]
            tgt_short = j["target_table"].split(".")[-1]
            join_specs.append({
                "id": _uid(),
                "left": {"identifier": j["source_table"]},
                "right": {"identifier": j["target_table"]},
                "sql": [f"{src_short}.{j['source_column']} = {tgt_short}.{j['target_column']}"],
            })

    eq_sqls = sorted([
        {"id": _uid(), "question": [eq["natural_language"]], "sql": [eq["sql"]]}
        for eq in ks.get("example_queries", []) if eq.get("sql")
    ], key=lambda x: x["id"])

    measures = sorted([
        {"id": _uid(), "alias": m["name"].lower().replace(" ", "_").replace("-", "_")[:50], "sql": [m["expression"]]}
        for m in ks["sql_expressions"].get("measures", [])
    ], key=lambda x: x["id"])

    filters = sorted([
        {"id": _uid(), "display_name": f["name"][:100],
         "sql": [f["expression"].replace("{{value}}", "'example'").replace("{value}", "'example'")]}
        for f in ks["sql_expressions"].get("filters", [])
    ], key=lambda x: x["id"])

    expressions = sorted([
        {"id": _uid(), "alias": d["name"].lower().replace(" ", "_").replace("-", "_")[:50], "sql": [d["expression"]]}
        for d in ks["sql_expressions"].get("dimensions", [])
    ], key=lambda x: x["id"])

    instructions = {
        "text_instructions": text_instructions,
        "example_question_sqls": eq_sqls,
        "sql_snippets": {"measures": measures, "filters": filters, "expressions": expressions},
    }
    if join_specs:
        instructions["join_specs"] = join_specs

    return {"version": 2, "config": {"sample_questions": sample_questions},
            "data_sources": {"tables": tables}, "instructions": instructions}


def _validate_payload_llm(payload, prompts_cfg):
    from databricks_langchain import ChatDatabricks
    from langchain_core.messages import HumanMessage, SystemMessage

    model = prompts_cfg.get("llm_model", "databricks-claude-opus-4-6")
    vp = prompts_cfg.get("payload_validation")
    if not vp:
        return payload

    payload_json = json.dumps(payload, indent=2, default=str)
    ref = vp.get("reference_schema", "").strip()
    sys_prompt = vp["system"].format(reference_schema=ref)

    try:
        raw = ChatDatabricks(model=model).invoke([
            SystemMessage(content=sys_prompt),
            HumanMessage(content=vp["user"].format(payload_json=payload_json)),
        ]).content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        validated = json.loads(raw)

        orig_t = len(payload.get("data_sources", {}).get("tables", []))
        val_t = len(validated.get("data_sources", {}).get("tables", []))
        orig_eq = len(payload.get("instructions", {}).get("example_question_sqls", []))
        val_eq = len(validated.get("instructions", {}).get("example_question_sqls", []))
        if val_t < orig_t or val_eq < orig_eq * 0.8:
            return payload
        return validated
    except Exception:
        return payload


# ═════════════════════════════════════════════════════════════════════════════
# Main Pipeline — Generator
# ═════════════════════════════════════════════════════════════════════════════

def run_full_pipeline(
    config: dict[str, Any],
) -> Generator[tuple[str, dict | None], None, None]:
    """Run the 3-stage pipeline, yielding ``(log_message, result_or_None)``."""

    import sys
    sys.path.insert(0, str(Path(__file__).parent / "utils"))
    from auto_genie_utils import (
        extract_table_metadata,
        generate_business_driven_instructions,
        generate_join_instructions,
        generate_sql_expressions,
        generate_table_instructions,
        get_spark_session,
        discover_naming_pattern_relationships,
        optimize_genie_instructions,
        profile_column_statistics,
    )

    prompts = load_prompts()
    llm_model = prompts.get("llm_model", "databricks-claude-opus-4-6")

    catalog = config["catalog"]
    schema = config["schema"]
    tables = config["tables"]

    yield "🚀 AutoGenie Pipeline Starting…", None
    yield f"   Catalog   : {catalog}", None
    yield f"   Schema    : {schema}", None
    yield f"   Tables    : {', '.join(tables)}", None
    yield f"   Warehouse : {config['genie_warehouse_id']}", None
    yield "", None

    # ── STAGE 1 ──────────────────────────────────────────────────────────────
    yield "═" * 55, None
    yield "  STAGE 1 — Query Intelligence & Pattern Mining", None
    yield "═" * 55, None

    yield "🔄 Connecting to Databricks cluster…", None
    try:
        spark = get_spark_session(config)
        yield "✅ Spark session established", None
    except Exception as e:
        yield f"❌ Spark connection failed: {e}", None
        return

    yield "🔄 Extracting table metadata…", None
    try:
        table_metadata = extract_table_metadata(spark, catalog, schema, tables)
        yield f"✅ Loaded {len(table_metadata)} tables, {sum(len(m['columns']) for m in table_metadata.values())} columns", None
    except Exception as e:
        yield f"❌ Metadata extraction failed: {e}", None
        return

    yield "🔄 Profiling column statistics…", None
    column_profiles = profile_column_statistics(spark, table_metadata)
    yield f"✅ Profiled {sum(len(v) for v in column_profiles.values())} columns", None

    in_scope_simple = {fqn.split(".")[-1] for fqn in table_metadata}

    yield f"🔄 Extracting query history (last {config['lookback_days']} days)…", None
    try:
        qh = _extract_query_history(spark, catalog, schema, config["lookback_days"])
        yield f"✅ Extracted {len(qh)} queries", None
    except Exception as e:
        yield f"⚠️  Query history unavailable: {e}", None
        qh = []

    if qh:
        yield "🔄 Parsing & clustering queries…", None
        parsed, clusters = _parse_and_cluster(qh)
        yield f"✅ Parsed {len(parsed)} queries → {len(clusters)} clusters", None
    else:
        clusters = {}

    history_examples = _examples_from_clusters(clusters) if clusters else []
    if history_examples:
        yield f"✅ {len(history_examples)} history-based example queries", None

    yield f"🔄 Generating sample queries via LLM ({llm_model})…", None
    try:
        llm_examples = _llm_queries(table_metadata, column_profiles, config,
                                    prompts, in_scope_simple,
                                    n=prompts.get("llm_query_count", 20))
        yield f"✅ {len(llm_examples)} LLM-generated queries", None
    except Exception as e:
        yield f"⚠️  LLM generation failed ({e}) — using rule-based fallback", None
        llm_examples = _rule_based_queries(table_metadata, column_profiles, config)
        yield f"✅ {len(llm_examples)} rule-based queries", None

    yield "🔄 Merging & finalising example queries…", None
    example_queries = _merge_queries(history_examples, llm_examples, table_metadata, n=20)
    hist_ct = sum(1 for q in example_queries if q.get("source") == "history_cluster")
    yield f"✅ Final set: {len(example_queries)} queries ({hist_ct} history, {len(example_queries) - hist_ct} generated)", None
    yield "", None

    # ── STAGE 2 ──────────────────────────────────────────────────────────────
    yield "═" * 55, None
    yield "  STAGE 2 — Knowledge Store Assembly & Validation", None
    yield "═" * 55, None

    yield "🔄 Discovering table relationships…", None
    relationships = discover_naming_pattern_relationships(table_metadata)
    yield f"✅ Found {len(relationships)} relationships", None

    yield "🔄 Generating table instructions…", None
    tbl_instr = generate_table_instructions(table_metadata, column_profiles, relationships, example_queries)
    yield f"✅ Instructions for {len(tbl_instr)} tables", None

    yield "🔄 Generating join instructions…", None
    join_instr = generate_join_instructions(relationships)
    yield f"✅ {len(join_instr)} join instructions", None

    yield "🔄 Generating SQL expressions…", None
    sql_expr = generate_sql_expressions(table_metadata, column_profiles)
    yield f"✅ Measures: {len(sql_expr['measures'])} | Filters: {len(sql_expr['filters'])} | Dimensions: {len(sql_expr['dimensions'])}", None

    yield "🔄 Generating business-driven instructions…", None
    biz_instr = generate_business_driven_instructions(
        table_metadata, column_profiles, relationships, example_queries, config,
    )
    yield f"✅ Domain: {biz_instr['domain'].get('label', 'N/A')} | KPIs: {len(biz_instr['kpis'])}", None

    yield "🔄 Assembling knowledge store…", None
    ks = _assemble_knowledge_store(
        config, table_metadata, column_profiles, relationships,
        tbl_instr, join_instr, sql_expr, example_queries, biz_instr,
    )
    yield "✅ Knowledge store assembled", None

    yield "🔄 Optimizing instructions per Genie best practices…", None
    orig_len = len(ks.get("global_instructions", ""))
    try:
        ks = optimize_genie_instructions(ks, prompts)
        opt_len = len(ks.get("global_instructions", ""))
        yield (f"✅ Instructions optimized: {orig_len:,} → {opt_len:,} chars "
               f"({(1 - opt_len / max(orig_len, 1)) * 100:.0f}% reduction)"), None
    except Exception as e:
        yield f"⚠️  Instruction optimization skipped: {e}", None

    yield "", None

    # ── STAGE 3 ──────────────────────────────────────────────────────────────
    yield "═" * 55, None
    yield "  STAGE 3 — Genie Space Deployment", None
    yield "═" * 55, None

    yield "🔄 Building serialised space payload…", None
    draft = _build_serialized_space(ks, include_join_specs=True)
    instr = draft.get("instructions", {})
    yield (f"   Tables: {len(draft['data_sources']['tables'])} | "
           f"Questions: {len(draft['config']['sample_questions'])} | "
           f"Join specs: {len(instr.get('join_specs', []))} | "
           f"SQL pairs: {len(instr.get('example_question_sqls', []))}"), None

    yield "🔄 Validating payload via LLM…", None
    payload = _validate_payload_llm(draft, prompts)
    yield "✅ Payload validated", None

    yield "🔄 Creating Genie Space via Databricks SDK…", None
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN"),
        )
        serialized = json.dumps(payload, indent=2, default=str)

        try:
            space = w.genie.create_space(
                warehouse_id=config["genie_warehouse_id"],
                serialized_space=serialized,
                title=ks["space_name"],
                description=ks.get("space_description", ""),
            )
        except Exception as e:
            err = str(e)
            has_js = bool(payload.get("instructions", {}).get("join_specs"))
            if has_js and ("Failed to parse" in err or "InvalidParameterValue" in err):
                yield "⚠️  join_specs rejected — rebuilding with text instructions…", None
                payload = _build_serialized_space(ks, include_join_specs=False)
                serialized = json.dumps(payload, indent=2, default=str)
                space = w.genie.create_space(
                    warehouse_id=config["genie_warehouse_id"],
                    serialized_space=serialized,
                    title=ks["space_name"],
                    description=ks.get("space_description", ""),
                )
            else:
                raise

        ws_url = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        space_url = f"{ws_url}/genie/rooms/{space.space_id}"

        yield "✅ Genie Space created!", None
        yield "", None
        yield "═" * 55, None
        yield "  PIPELINE COMPLETE", None
        yield "═" * 55, None
        yield f"🔗 {space_url}", None

        yield "", {
            "space_id": space.space_id,
            "space_name": ks["space_name"],
            "space_url": space_url,
            "warehouse_id": config["genie_warehouse_id"],
            "domain": ks.get("domain", {}).get("label", "N/A"),
            "tables": len(ks["tables"]),
            "joins": len(ks["joins"]),
            "measures": len(ks["sql_expressions"]["measures"]),
            "filters": len(ks["sql_expressions"]["filters"]),
            "dimensions": len(ks["sql_expressions"]["dimensions"]),
            "example_queries": len(ks.get("example_queries", [])),
            "kpis": len(ks.get("kpis", [])),
        }

    except Exception as e:
        yield f"❌ Genie Space creation failed: {e}", None
