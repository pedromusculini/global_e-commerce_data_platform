"""Streamlit dashboard to visualize the comparative report.

Run:
    streamlit run app.py

Core dependencies: streamlit, json (pandas optional for future extensions).
"""

from __future__ import annotations

import json, os, subprocess, textwrap, sys
from pathlib import Path
import streamlit as st

REPORT_JSON_PATH = Path("reports/comparative_report.json")
REPORT_MD_PATH = Path("reports/comparative_report.md")
FIGURES_DIR = Path("reports/figures")
BRAND_DIR = Path("brand")
BRAND_LOGO = BRAND_DIR / "png" / "icon_cart_growth_default_256.png"
FAVICON = BRAND_DIR / "png" / "icon_cart_growth_default_32.png"

# Determine python executable for subprocess (prefer current interpreter)
PY_EXEC = Path(sys.executable)
if not PY_EXEC.exists():
    # Fallback to 'python'
    PY_EXEC = Path('python')


@st.cache_data(show_spinner=False)
def load_report() -> dict:
    if not REPORT_JSON_PATH.exists():
        st.error(f"JSON file not found: {REPORT_JSON_PATH}")
        return {}
    try:
        data = json.loads(REPORT_JSON_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        st.error(f"Failed to decode JSON: {exc}")
        return {}
    # Validação mínima de campos essenciais
    required_fields = ["schema_version", "products", "orders", "figures", "narrative"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        st.warning(f"Missing required JSON fields: {missing}")
    return data


def kpi_section(report: dict):
    st.subheader("Key KPIs")
    products = report.get("products", {})
    orders = report.get("orders", {})

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Products", products.get("count", "-"))
    col2.metric("Orders", orders.get("count", "-"))
    col3.metric("Avg Order Value (mean)", f"{orders.get('gmv', {}).get('mean', 0):.2f}" if orders else "-")
    col4.metric("Timespan (days)", orders.get("timespan_days", "-"))

    price_stats = products.get("price", {})
    st.caption(
        f"Price: min {price_stats.get('min', '-')}, p90 {price_stats.get('p90', '-')}, max {price_stats.get('max', '-')}"
    )


def figures_section(report: dict):
    st.subheader("Figures")
    figures = report.get("figures", [])
    if not figures:
        st.info("No figures registered.")
        return
    for fig in figures:
        rel_path = fig.get("path")
        if not rel_path:
            continue
        img_path = Path("reports") / rel_path
        if img_path.exists():
            st.image(str(img_path), caption=fig.get("name"))
        else:
            st.warning(f"Image not found: {img_path}")


def narrative_section(report: dict):
    st.subheader("Narrative")
    narrative = report.get("narrative", {})
    summary = narrative.get("summary")
    details = narrative.get("details")
    if summary:
        st.markdown(f"**Summary:** {summary}")
    if details:
        with st.expander("Details", expanded=False):
            st.markdown(details)


def availability_section(report: dict):
    st.subheader("Data Availability")
    data_av = report.get("data_availability", {})
    if not data_av:
        st.info("data_availability block missing.")
        return
    # Render simples em duas colunas
    cols = st.columns(2)
    items = list(data_av.items())
    for idx, (k, v) in enumerate(items):
        cols[idx % 2].write(f"**{k}**: {v}")


def download_section(report: dict):
    st.subheader("Downloads")
    if REPORT_JSON_PATH.exists():
        st.download_button(
            label="Download JSON", data=REPORT_JSON_PATH.read_bytes(), file_name=REPORT_JSON_PATH.name, mime="application/json"
        )
    if REPORT_MD_PATH.exists():
        st.download_button(
            label="Download Markdown", data=REPORT_MD_PATH.read_text(encoding="utf-8"), file_name=REPORT_MD_PATH.name, mime="text/markdown"
        )


def regenerate_section():
    st.subheader("Regenerate Artifacts (Optional)")
    st.caption(
        "Runs a limited synthetic pipeline and rebuilds figures + report. May take several seconds."
    )
    run_limit = st.number_input("Limit fake rows", min_value=5, max_value=500, value=50, step=5)
    seed = st.number_input("Seed", min_value=0, max_value=10_000, value=42, step=1)
    triple = st.checkbox("Use key-mode=triple", value=True)
    if st.button("Run pipeline + report", type="primary"):
        import time
        start = time.time()
        key_mode = "triple" if triple else "pair"
        try:
            with st.spinner("Running pipeline..."):
                cmd = [str(PY_EXEC), "pipelines/run_pipeline.py", "--providers", "shopify,amazon,ebay", "--fake-only", "--limit", str(run_limit), "--seed", str(seed), "--key-mode", key_mode]
                proc = subprocess.run(cmd, capture_output=True, text=True)
                if proc.returncode != 0:
                    st.error("Pipeline failed")
                    st.code(proc.stdout or "<no stdout>", language="text")
                    st.code(proc.stderr or "<no stderr>", language="bash")
                    return
            with st.spinner("Generating figures..."):
                cmd = [str(PY_EXEC), "scripts/generate_figures.py"]
                proc = subprocess.run(cmd, capture_output=True, text=True)
                if proc.returncode != 0:
                    st.error("Figure generation failed")
                    st.code(proc.stdout or "<no stdout>", language="text")
                    st.code(proc.stderr or "<no stderr>", language="bash")
                    return
            with st.spinner("Generating report..."):
                cmd = [str(PY_EXEC), "scripts/generate_comparative_report.py"]
                proc = subprocess.run(cmd, capture_output=True, text=True)
                if proc.returncode != 0:
                    st.error("Report generation failed")
                    st.code(proc.stdout or "<no stdout>", language="text")
                    st.code(proc.stderr or "<no stderr>", language="bash")
                    return
            st.success(f"Completed in {time.time() - start:.1f}s")
            # Limpa cache para refletir novos dados
            load_report.clear()  # type: ignore[attr-defined]
            st.experimental_rerun()
        except subprocess.CalledProcessError as exc:
            st.error(f"Regeneration failed: {exc}")


def sidebar_info(report: dict):
    st.sidebar.header("Configuration")
    st.sidebar.write(f"Schema Version: {report.get('schema_version', '-')}")
    st.sidebar.write(f"Generated at: {report.get('generated_at', '-')}")
    st.sidebar.markdown("---")
    st.sidebar.caption("Initial dashboard - future interactive analyses can be added.")


def main():
    st.set_page_config(page_title="Comparative Report Dashboard", layout="wide", page_icon=str(FAVICON) if FAVICON.exists() else None)

    # Inject minimal professional CSS theme adjustments
    custom_css = textwrap.dedent(
        """
        <style>
        :root {
            --brand-accent: #2563eb;
            --brand-bg: #0f1115;
        }
        .block-container {padding-top: 1.5rem;}
        header[data-testid="stHeader"] {background: linear-gradient(90deg,#0f1115,#1e293b);}
        .stMetric {background: #1e2530; padding: 0.75rem 0.75rem 0.5rem 0.75rem; border-radius: 8px;}
        .st-emotion-cache-13k62yr p {margin-bottom: 0.5rem;}
        .brand-header {display:flex;align-items:center;gap:0.75rem;margin-bottom:1rem;}
        .brand-header img {border-radius:12px;box-shadow:0 0 0 2px rgba(255,255,255,0.06);}        
        .figure img {border:1px solid #2c3644; border-radius:6px;}
        .stExpander {border: 1px solid #2c3644;}
        .css-1q8dd3e, .stDownloadButton button {background: #1e2530;}
        </style>
        """
    )
    st.markdown(custom_css, unsafe_allow_html=True)

    # Branded header
    col_logo, col_title = st.columns([1,6])
    with col_logo:
        if BRAND_LOGO.exists():
            st.image(str(BRAND_LOGO), caption="", use_container_width=False)
    with col_title:
        st.title("Comparative Report Dashboard")
        st.caption("Unified multi-source commerce ingestion & analytics overview")
    report = load_report()
    if not report:
        st.stop()
    sidebar_info(report)
    kpi_section(report)
    figures_section(report)
    narrative_section(report)
    availability_section(report)
    download_section(report)
    with st.expander("Regenerate Artifacts", expanded=False):
        regenerate_section()


if __name__ == "__main__":
    main()
