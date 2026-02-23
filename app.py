"""
AutoGenie — Databricks App
===========================
Gradio-based UI for automated Genie Space creation.

Takes Catalog, Schema, Tables, and Warehouse ID as inputs,
runs the 3-stage pipeline (Query Intelligence → Assembly → Deployment),
and returns the deployed Genie Space URL.
"""

import os
from pathlib import Path

import gradio as gr
from dotenv import load_dotenv

from pipeline import run_full_pipeline

load_dotenv(Path(__file__).parent / ".env", override=True)


# ─── Pipeline wrapper for Gradio streaming ──────────────────────────────────

def create_genie_space(
    catalog: str,
    schema: str,
    tables_str: str,
    warehouse_id: str,
    cluster_id: str,
    lookback_days: float,
    confidence_threshold: float,
    output_path: str,
):
    """Gradio handler — yields ``(log_text, result_text)`` tuples."""
    if not catalog.strip():
        yield "❌ Catalog is required.", ""
        return
    if not schema.strip():
        yield "❌ Schema is required.", ""
        return
    if not tables_str.strip():
        yield "❌ At least one table name is required.", ""
        return
    if not warehouse_id.strip():
        yield "❌ SQL Warehouse ID is required.", ""
        return
    if not cluster_id.strip() and not os.getenv("DATABRICKS_CLUSTER_ID"):
        yield "❌ Cluster ID is required (for Databricks Connect).", ""
        return

    tables = [t.strip() for t in tables_str.split(",") if t.strip()]

    config = {
        "workspace_url": os.getenv("DATABRICKS_HOST", ""),
        "databricks_token": os.getenv("DATABRICKS_TOKEN", ""),
        "cluster_id": cluster_id.strip() or os.getenv("DATABRICKS_CLUSTER_ID", ""),
        "catalog": catalog.strip(),
        "schema": schema.strip(),
        "tables": tables,
        "genie_warehouse_id": warehouse_id.strip(),
        "lookback_days": int(lookback_days),
        "confidence_threshold": float(confidence_threshold),
        "output_path": output_path.strip(),
    }

    logs: list[str] = []
    result_text = ""

    for msg, result in run_full_pipeline(config):
        logs.append(msg)
        if result is not None:
            result_text = _format_result(result)
        yield "\n".join(logs), result_text


def _format_result(r: dict) -> str:
    return (
        f"🎉  Genie Space Created Successfully!\n"
        f"{'─' * 45}\n\n"
        f"Space Name  : {r['space_name']}\n"
        f"Space ID    : {r['space_id']}\n"
        f"Space URL   : {r['space_url']}\n\n"
        f"Domain      : {r['domain']}\n\n"
        f"Configuration\n"
        f"  Tables          : {r['tables']}\n"
        f"  Joins           : {r['joins']}\n"
        f"  Measures        : {r['measures']}\n"
        f"  Filters         : {r['filters']}\n"
        f"  Dimensions      : {r['dimensions']}\n"
        f"  Example Queries : {r['example_queries']}\n"
        f"  KPIs            : {r['kpis']}\n\n"
        f"{'─' * 45}\n"
        f"🔗 Open your Genie Space:\n{r['space_url']}"
    )


# ─── Gradio UI ──────────────────────────────────────────────────────────────

CUSTOM_CSS = """
.header-block { text-align: center; padding: 24px 16px 16px; }
.header-block h1 { font-size: 2rem; margin-bottom: 2px; }
.header-block p  { color: var(--body-text-color-subdued); margin: 2px 0; }
#run-btn { margin-top: 12px; }
"""

with gr.Blocks(title="AutoGenie — Genie Space Creator") as app:

    gr.HTML(
        '<div class="header-block">'
        "<h1>⚡ AutoGenie</h1>"
        "<p>Automated Databricks Genie Space Creator</p>"
        "<p><small>Query Intelligence · LLM Augmentation · Unity Catalog Metadata</small></p>"
        "</div>"
    )

    with gr.Row(equal_height=True):
        # ── Left column: inputs ──────────────────────────────────────────
        with gr.Column(scale=1):
            gr.Markdown("### Databricks Scope")
            catalog_in = gr.Textbox(label="Catalog", placeholder="e.g. main")
            schema_in = gr.Textbox(label="Schema", placeholder="e.g. sales")
            tables_in = gr.Textbox(
                label="Tables (comma-separated)",
                placeholder="e.g. accounts, opportunity, user",
                lines=2,
            )
            warehouse_in = gr.Textbox(
                label="SQL Warehouse ID",
                placeholder="e.g. 862f1d757f0424f7",
            )

            with gr.Accordion("Advanced Settings", open=False):
                cluster_in = gr.Textbox(
                    label="Cluster ID (Databricks Connect)",
                    placeholder="e.g. 0921-143134-dkh98w1g",
                    value=os.getenv("DATABRICKS_CLUSTER_ID", ""),
                )
                lookback_in = gr.Number(
                    label="Lookback Days", value=90, minimum=1, maximum=365,
                )
                confidence_in = gr.Number(
                    label="Confidence Threshold", value=0.75,
                    minimum=0.0, maximum=1.0, step=0.05,
                )
                output_in = gr.Textbox(
                    label="Output Path",
                    value="/dbfs/tmp/auto_genie_outputs",
                )

            run_btn = gr.Button(
                "🚀  Create Genie Space", variant="primary", size="lg",
                elem_id="run-btn",
            )

        # ── Right column: outputs ────────────────────────────────────────
        with gr.Column(scale=1):
            log_out = gr.Textbox(
                label="Pipeline Progress",
                lines=28, max_lines=60, interactive=False,
            )
            result_out = gr.Textbox(
                label="Result",
                lines=18, max_lines=30, interactive=False,
            )

    run_btn.click(
        fn=create_genie_space,
        inputs=[
            catalog_in, schema_in, tables_in, warehouse_in,
            cluster_in, lookback_in, confidence_in, output_in,
        ],
        outputs=[log_out, result_out],
    )


if __name__ == "__main__":
    app.launch(
        server_name="0.0.0.0",
        server_port=8000,
        css=CUSTOM_CSS,
        theme=gr.themes.Soft(primary_hue="indigo", neutral_hue="slate"),
    )
