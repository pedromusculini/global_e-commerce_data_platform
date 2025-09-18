"""Generate a comparative report consolidating key product and order metrics
and embedding (or linking) existing figures produced by the insights notebook.

Usage (from project root):
    python scripts/generate_comparative_report.py

The script searches normalized data and enriched outputs, computes summary KPIs,
and writes a Markdown file at reports/comparative_report.md.

Idempotent & defensive: skips sections gracefully if data missing.
"""
from __future__ import annotations
import os, json, math, textwrap, argparse, sys, hashlib
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_NORM = PROJECT_ROOT / 'data' / 'normalized'
DATA_ENRICHED = PROJECT_ROOT / 'data' / 'enriched'
FIG_DIR = PROJECT_ROOT / 'reports' / 'figures'
REPORTS_DIR = PROJECT_ROOT / 'reports'
OUTPUT_MD = REPORTS_DIR / 'comparative_report.md'
OUTPUT_JSON = REPORTS_DIR / 'comparative_report.json'

REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# --- CLI Args ---
parser = argparse.ArgumentParser(description="Generate comparative markdown + JSON reports.")
parser.add_argument('--json-only', action='store_true', help='Generate only the JSON (does not overwrite existing markdown).')
parser.add_argument('--md-only', action='store_true', help='Generate only the markdown (skips JSON).')
parser.add_argument('--fail-on-missing', action='store_true', help='Return exit code 2 if any essential dataset is missing.')
parser.add_argument('--validate', action='store_true', help='Validate produced JSON against JSON Schema (schemas/comparative_report.schema.json).')
args = parser.parse_args()

if args.json_only and args.md_only:
    print('Error: do not use --json-only and --md-only together.', file=sys.stderr)
    sys.exit(1)

# --- Helpers ---

def load_df(preferred: Path, fallback: Path) -> pd.DataFrame | None:
    if preferred.exists():
        return pd.read_parquet(preferred)
    if fallback.exists():
        if fallback.suffix.lower() == '.parquet':
            return pd.read_parquet(fallback)
        return pd.read_csv(fallback)
    return None

products = load_df(DATA_NORM / 'products.parquet', DATA_NORM / 'products.csv')
orders = load_df(DATA_NORM / 'orders.parquet', DATA_NORM / 'orders.csv')
products_enriched = load_df(DATA_ENRICHED / 'products_enriched.parquet', DATA_ENRICHED / 'products_enriched.csv')
orders_enriched = load_df(DATA_ENRICHED / 'orders_enriched.parquet', DATA_ENRICHED / 'orders_enriched.csv')

# JSON structure accumulator (schema_version incremented on structure change)
json_report: dict[str, object] = {
    '$schema': str(PROJECT_ROOT / 'schemas' / 'comparative_report.schema.json'),
    'schema_version': '1.1.0',
    'generated_at_utc': datetime.now(timezone.utc).isoformat(),
    # Capture invocation context (basic – refined upstream if pipeline passes env vars)
    'inputs': {
        'providers': [],
        'dedup_key_mode': os.environ.get('DEDUP_KEY_MODE', 'pair'),
        'seed': None,
        'limit': None,
        'fake_mode': os.environ.get('FAKE_MODE', 'none')
    },
    'paths': {
        'products': str(DATA_NORM / 'products.parquet'),
        'orders': str(DATA_NORM / 'orders.parquet'),
        'products_enriched': str(DATA_ENRICHED / 'products_enriched.parquet'),
        'orders_enriched': str(DATA_ENRICHED / 'orders_enriched.parquet'),
        'figures_dir': str(FIG_DIR)
    },
    'figures': [],  # will collect figures used in report
    'products': {},
    'orders': {},
    'enriched': {},
    'run_logs': {},
    'data_availability': {},
    'narrative': {},
    'notes': {}
}

lines: list[str] = []
brand_logo = None
preferred_logo_candidates = [
    PROJECT_ROOT / 'brand' / 'png' / 'icon_cart_growth_default_256.png',
    PROJECT_ROOT / 'brand' / 'png' / 'icon_cart_growth_adaptive_256.png',
]
for cand in preferred_logo_candidates:
    if cand.exists():
        brand_logo = cand
        break
brand_title = 'Comparative Data Report'
lines.append(f'# {brand_title}')
lines.append('Generated: ' + datetime.now(timezone.utc).isoformat())
if brand_logo:
    rel_logo = os.path.relpath(brand_logo, REPORTS_DIR).replace('\\','/')
    lines.append(f"<p align='right'><img src='{rel_logo}' alt='Brand Logo' width='96'/></p>")
lines.append('')
json_report.setdefault('narrative', {})['brand'] = {
    'title': brand_title,
    'logo': str(brand_logo) if brand_logo else None
}

# Attempt to read pipeline run logs (jsonl)
RUN_LOG = PROJECT_ROOT / 'pipeline_runs.jsonl'
run_logs: list[dict] = []
if RUN_LOG.exists():
    with RUN_LOG.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                run_logs.append(json.loads(line))
            except Exception:
                continue

def safe_number(x):
    try:
        return float(x)
    except Exception:
        return math.nan

# --- Product KPIs ---
if products is not None and len(products) > 0:
    lines.append('## Products Summary')
    lines.append(f"Total products: **{len(products)}**")
    prod_section = {
        'count': int(len(products)),
        'distinct_sources': None,
        'top_category': None,
        'price': {},
        'duplicate_logical_key_pairs': None,
        'duplicate_logical_key_ratio': None,
        'outlier_price_ratio': None,
        'sources_top10': []
    }
    if 'source' in products.columns:
        ds = products['source'].nunique()
        lines.append(f"Distinct sources: **{ds}**")
        prod_section['distinct_sources'] = int(ds)
    if 'category' in products.columns and products['category'].notna().any():
        top_cat = products['category'].value_counts().idxmax()
        lines.append(f"Top category: **{top_cat}**")
        prod_section['top_category'] = top_cat
    price_col = None
    for cand in ['price','price_amount']:
        if cand in (products.columns):
            price_col = cand
            break
    if price_col:
        pnum = pd.to_numeric(products[price_col], errors='coerce')
        if pnum.notna().any():
            price_stats = {
                'min': float(pnum.min()),
                'median': float(pnum.median()),
                'mean': float(pnum.mean()),
                'p90': float(pnum.quantile(0.90)),
                'max': float(pnum.max())
            }
            prod_section['price'] = price_stats
            lines.append(f"Price min: **{price_stats['min']:.2f}** | median: **{price_stats['median']:.2f}** | mean: **{price_stats['mean']:.2f}** | p90: **{price_stats['p90']:.2f}** | max: **{price_stats['max']:.2f}**")
    # Duplicate logical key ratio
    if all(c in products.columns for c in ['source','source_id']):
        key_counts = products.groupby(['source','source_id']).size().reset_index(name='dup_count')
        dup_pairs = (key_counts['dup_count'] > 1).sum()
        ratio = dup_pairs/len(key_counts) if len(key_counts)>0 else 0.0
        lines.append(f"Duplicate logical key pairs: **{dup_pairs}** ({ratio:.2%} of keys)")
        prod_section['duplicate_logical_key_pairs'] = int(dup_pairs)
        prod_section['duplicate_logical_key_ratio'] = float(ratio)
    # Outlier detection (IQR) using the resolved price column
    if price_col:
        pnum = pd.to_numeric(products[price_col], errors='coerce')
        if pnum.notna().sum() > 5:
            q1 = pnum.quantile(0.25)
            q3 = pnum.quantile(0.75)
            iqr = q3 - q1 if (q3 - q1) != 0 else 1.0
            upper = q3 + 1.5 * iqr
            lower = q1 - 1.5 * iqr
            outlier_ratio = ((pnum > upper) | (pnum < lower)).mean()
            lines.append(f"Outlier price ratio (IQR fence): **{outlier_ratio:.2%}** (lower={lower:.2f}, upper={upper:.2f})")
            prod_section['outlier_price_ratio'] = float(outlier_ratio)
            prod_section['price']['iqr'] = float(iqr)
            prod_section['price']['fence_lower'] = float(lower)
            prod_section['price']['fence_upper'] = float(upper)

    # Breakdown by source (top 10)
    if 'source' in products.columns:
        src_counts = products['source'].value_counts().head(10)
        lines.append('\n**Products by Source (Top 10)**')
        lines.append('\n')
        lines.append('| Source | Count | % |')
        lines.append('|--------|-------|----|')
        total_prod = len(products)
        for s, c in src_counts.items():
            pct = c/total_prod if total_prod else 0.0
            lines.append(f"| {s} | {c} | {pct:.2%} |")
            prod_section['sources_top10'].append({'source': s, 'count': int(c), 'ratio': pct})

    # Link to figure
    fig_price = FIG_DIR / 'products_price_distributions.png'
    if fig_price.exists():
        rel_path = os.path.relpath(fig_price, REPORTS_DIR).replace('\\','/')
        lines.append(f"![Product Price Distributions]({rel_path})")
        json_report['figures'].append({'name': fig_price.name, 'path': rel_path, 'section': 'products'})
    lines.append('')
    json_report['products'] = prod_section
else:
    lines.append('## Products Summary')
    lines.append('No product data available.')
    lines.append('')

# --- Order KPIs ---
if orders is not None and len(orders) > 0:
    lines.append('## Orders Summary')
    lines.append(f"Total orders: **{len(orders)}**")
    ord_section = {
        'count': int(len(orders)),
        'timespan_days': None,
        'gmv': {},
        'sources_top10': []
    }
    # Date detection
    date_col = None
    for c in ['created_at','order_date','date','timestamp']:
        if c in orders.columns:
            date_col = c; break
    value_col = None
    for c in ['total_price','total','amount','grand_total','price']:
        if c in orders.columns:
            value_col = c; break
    if date_col:
        odates = pd.to_datetime(orders[date_col], errors='coerce')
        if odates.notna().any():
            span_days = (odates.max() - odates.min()).days
            lines.append(f"Timespan (days): **{span_days}**")
            ord_section['timespan_days'] = int(span_days)
    if value_col:
        oval = pd.to_numeric(orders[value_col], errors='coerce')
        if oval.notna().any():
            gmv_stats = {'total': float(oval.sum()), 'mean': float(oval.mean()), 'median': float(oval.median())}
            ord_section['gmv'] = gmv_stats
            lines.append(f"GMV total: **{gmv_stats['total']:.2f}** | mean: **{gmv_stats['mean']:.2f}** | median: **{gmv_stats['median']:.2f}**")
    # Breakdown by source if present
    if 'source' in orders.columns:
        src_counts = orders['source'].value_counts().head(10)
        lines.append('\n**Orders by Source (Top 10)**')
        lines.append('\n')
        lines.append('| Source | Count | % |')
        lines.append('|--------|-------|----|')
        total_ord = len(orders)
        for s, c in src_counts.items():
            pct = c/total_ord if total_ord else 0.0
            lines.append(f"| {s} | {c} | {pct:.2%} |")
            ord_section['sources_top10'].append({'source': s, 'count': int(c), 'ratio': pct})

    fig_ts = FIG_DIR / 'orders_time_series.png'
    if fig_ts.exists():
        rel_path = os.path.relpath(fig_ts, REPORTS_DIR).replace('\\','/')
        lines.append(f"![Orders Time Series]({rel_path})")
        json_report['figures'].append({'name': fig_ts.name, 'path': rel_path, 'section': 'orders'})
    fig_aov = FIG_DIR / 'orders_aov_distribution.png'
    if fig_aov.exists():
        rel_path = os.path.relpath(fig_aov, REPORTS_DIR).replace('\\','/')
        lines.append(f"![AOV Distribution]({rel_path})")
        json_report['figures'].append({'name': fig_aov.name, 'path': rel_path, 'section': 'orders'})
    lines.append('')
    json_report['orders'] = ord_section
else:
    lines.append('## Orders Summary')
    lines.append('No order data available.')
    lines.append('')

# --- Enriched Feature Highlights ---
if products_enriched is not None and len(products_enriched) > 0:
    lines.append('## Enriched Product Features')
    cols = [c for c in products_enriched.columns if c.startswith('price_') or c.endswith('_freq')]
    enr_prod = {'columns': cols}
    if cols:
        lines.append('Derived columns: ' + ', '.join(sorted(cols)[:15]) + (' ...' if len(cols)>15 else ''))
    # Quantile bucket distribution if present
    if 'price_bucket' in products_enriched.columns:
        bucket_counts = products_enriched['price_bucket'].value_counts(dropna=False)
        lines.append('\n**Price Bucket Distribution**')
        lines.append('\n')
        lines.append('| Bucket | Count | % |')
        lines.append('|--------|-------|----|')
        total = bucket_counts.sum()
        bucket_arr = []
        for b, c in bucket_counts.items():
            ratio = c/total if total else 0.0
            lines.append(f"| {b} | {c} | {ratio:.2%} |")
            bucket_arr.append({'bucket': str(b), 'count': int(c), 'ratio': ratio})
        enr_prod['price_bucket_distribution'] = bucket_arr
    lines.append('')
    json_report.setdefault('enriched', {})['products'] = enr_prod
if orders_enriched is not None and len(orders_enriched) > 0:
    lines.append('## Enriched Order Features')
    cols = [c for c in orders_enriched.columns if c.endswith('_7d') or c.startswith('recency') or c=='order_value_num']
    if cols:
        lines.append('Derived columns: ' + ', '.join(sorted(cols)))
    enr_ord = {'columns': cols}
    # Rolling metrics snapshot (latest row)
    latest_metrics = {}
    for mc in ['gmv_7d','orders_7d','aov_7d']:
        if mc in orders_enriched.columns:
            val = pd.to_numeric(orders_enriched[mc], errors='coerce').dropna()
            if not val.empty:
                latest_metrics[mc] = float(val.iloc[-1])
    if latest_metrics:
        lines.append('\nLatest rolling 7d metrics: ' + ', '.join(f"{k}={v:.2f}" for k,v in latest_metrics.items()))
        enr_ord['latest_rolling_7d'] = latest_metrics
    lines.append('')
    json_report.setdefault('enriched', {})['orders'] = enr_ord

# --- Comparative Narrative ---
lines.append('## Comparative Narrative')
_narrative_text = textwrap.dedent('''
The product dataset establishes the commercial catalog footprint (source diversity, category concentration and price dispersion), while the
orders dataset captures temporal demand and monetary performance. Provider (source) breakdowns reveal distribution of catalog and demand.

Engineered features enable downstream tasks:
- Price segmentation and elasticity exploration (`price_bucket`, `price_log`).
- Supplier/source reliability & concentration (`source_freq`).
- Momentum & short‑term commercial monitoring (`gmv_7d`, `orders_7d`, `aov_7d`).
- Lifecycle / churn proxy via recency (`recency_days`).

Data Quality & Risk Observations:
- Outlier ratios contextualize pricing anomalies for potential cleansing or curation steps.
- Duplicate logical key pairs (source + source_id) indicate upstream id uniformity or merge correctness.
- Run log metrics (new_* vs updated_*) help track incremental ingestion health; rising updated/new ratio may signal dataset maturity or stagnation.

Recommended Next Steps:
1. Establish anomaly thresholds for price outliers and rolling GMV deltas.
2. Persist daily aggregate tables for BI dashboards.
3. Integrate simple forecasting (e.g., 7d GMV moving average horizon extension) for operations planning.
''').strip()
lines.append(_narrative_text)
json_report['narrative'] = {
    'summary': _narrative_text.split('\n')[0].strip(),
    'details': _narrative_text
}
lines.append('')

# --- Run Logs Section ---
if run_logs:
    lines.append('## Pipeline Run Logs Overview')
    # Convert to DataFrame for summarization
    rldf = pd.DataFrame(run_logs)
    # Basic counts
    lines.append(f"Total recorded runs: **{len(rldf)}**")
    # Metrics presence
    metric_cols = [c for c in ['new_products','updated_products','new_orders','updated_orders'] if c in rldf.columns]
    if metric_cols:
        # Last run snapshot
        last = rldf.tail(1).iloc[0]
        snapshot = ', '.join(f"{m}={last[m]}" for m in metric_cols)
        lines.append(f"Latest run metrics: {snapshot}")
        # Aggregate sums
        sums = rldf[metric_cols].sum(numeric_only=True)
        lines.append('Cumulative ingest stats: ' + ', '.join(f"{k}={int(v)}" for k,v in sums.items()))
        # Updated/New ratios
        ratios = []
        if 'updated_products' in metric_cols and 'new_products' in metric_cols:
            new_p = rldf['new_products'].sum() or 1
            ratios.append(f"products_update_ratio={rldf['updated_products'].sum()/new_p:.2f}")
        if 'updated_orders' in metric_cols and 'new_orders' in metric_cols:
            new_o = rldf['new_orders'].sum() or 1
            ratios.append(f"orders_update_ratio={rldf['updated_orders'].sum()/new_o:.2f}")
        if ratios:
            lines.append('Update/New ratios: ' + ', '.join(ratios))
        json_report['run_logs'] = {
            'total_runs': int(len(rldf)),
            'latest': {m: last[m] for m in metric_cols},
            'cumulative': {m: int(sums[m]) for m in metric_cols},
            'ratios': {r.split('=')[0]: float(r.split('=')[1]) for r in ratios}
        }
    lines.append('')


# --- Data Availability Matrix ---
lines.append('## Data Availability Matrix')
lines.append('| Dataset | Rows | Enriched |')
lines.append('|---------|------|----------|')
prod_rows = 0 if products is None else len(products)
ord_rows = 0 if orders is None else len(orders)
prod_enr = (products_enriched is not None and len(products_enriched)>0)
ord_enr = (orders_enriched is not None and len(orders_enriched)>0)
lines.append(f"| Products | {prod_rows} | {'Yes' if prod_enr else 'No'} |")
lines.append(f"| Orders | {ord_rows} | {'Yes' if ord_enr else 'No'} |")
json_report['data_availability'] = {
    'products_rows': prod_rows,
    'orders_rows': ord_rows,
    'products_enriched': bool(prod_enr),
    'orders_enriched': bool(ord_enr)
}
lines.append('')

# --- KPI Rollup (for strict JSON schema) ---
try:
    # Build KPI dict safely from earlier computed sections
    kpis = {}
    prod = json_report.get('products') or {}
    ords = json_report.get('orders') or {}
    price_stats = (prod.get('price') if isinstance(prod, dict) else {}) or {}
    gmv_stats = (ords.get('gmv') if isinstance(ords, dict) else {}) or {}
    kpis['products_total'] = prod.get('count') if isinstance(prod, dict) else 0
    # derive products_by_provider from sources_top10 list
    p_by_provider = {}
    for item in prod.get('sources_top10', []) or []:
        if isinstance(item, dict) and 'source' in item and 'count' in item:
            p_by_provider[item['source']] = item['count']
    kpis['products_by_provider'] = p_by_provider
    kpis['products_new'] = None  # not tracked here (pipeline run logs aggregate), left optional
    kpis['products_updated'] = None
    # Derive absolute outlier count from ratio * total (rounded) if available
    try:
        if isinstance(prod, dict) and prod.get('outlier_price_ratio') is not None and prod.get('count') is not None:
            kpis['products_outliers_price'] = int(round(float(prod['outlier_price_ratio']) * int(prod['count'])))
        else:
            kpis['products_outliers_price'] = None
    except Exception:
        kpis['products_outliers_price'] = None
    kpis['orders_total'] = ords.get('count') if isinstance(ords, dict) else 0
    o_by_provider = {}
    for item in ords.get('sources_top10', []) or []:
        if isinstance(item, dict) and 'source' in item and 'count' in item:
            o_by_provider[item['source']] = item['count']
    kpis['orders_by_provider'] = o_by_provider
    kpis['orders_outliers_total'] = None
    kpis['aov_mean'] = gmv_stats.get('mean') if isinstance(gmv_stats, dict) else None
    kpis['aov_median'] = gmv_stats.get('median') if isinstance(gmv_stats, dict) else None
    json_report['kpis'] = kpis
except Exception as _kpi_err:
    json_report['kpis'] = {'error': str(_kpi_err)}

# Write file
missing_essentials = []
if products is None:
    missing_essentials.append('products')
if orders is None:
    missing_essentials.append('orders')

if args.fail_on_missing and missing_essentials:
    print('Missing essential datasets:', ', '.join(missing_essentials), file=sys.stderr)
    sys.exit(2)

if not args.json_only:
    if OUTPUT_MD.exists():
        backup_path = OUTPUT_MD.parent / f"{OUTPUT_MD.name}.backup_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        backup_path.write_text(OUTPUT_MD.read_text(encoding='utf-8'), encoding='utf-8')
    print('Backup created at', backup_path)
    OUTPUT_MD.write_text('\n'.join(lines), encoding='utf-8')
    print('Markdown report written to', OUTPUT_MD)
else:
    print('Markdown preserved (flag --json-only).')

if not args.md_only:
    # Add figure metadata (size + sha256) before writing JSON
    for fig in json_report.get('figures', []):
        fp = REPORTS_DIR / fig['path']
        if fp.exists():
            try:
                blob = fp.read_bytes()
                fig['size_bytes'] = len(blob)
                fig['sha256'] = hashlib.sha256(blob).hexdigest()
            except Exception:
                pass
    # Integrity summary
    figs = json_report.get('figures', []) or []
    expected = [
        'products_price_distributions.png',
        'orders_time_series.png',
        'orders_aov_distribution.png'
    ]
    present_ids = [f.get('name') for f in figs if isinstance(f, dict) and f.get('name')]
    missing = [fid for fid in expected if fid not in present_ids]
    json_report['integrity'] = {
        'figure_count': len(figs),
        'figure_ids': present_ids,
        'expected_figures': expected,
        'missing_figures': missing
    }
    OUTPUT_JSON.write_text(json.dumps(json_report, ensure_ascii=False, indent=2), encoding='utf-8')
    print('JSON report written to', OUTPUT_JSON)

    if args.validate:
        schema_path = PROJECT_ROOT / 'schemas' / 'comparative_report.schema.json'
        if not schema_path.exists():
            print('[validate] Schema file not found at', schema_path, file=sys.stderr)
            sys.exit(3)
        try:
            import jsonschema  # type: ignore
        except ImportError:
            print('[validate] jsonschema package not installed. Add to requirements.txt to enable validation.', file=sys.stderr)
            sys.exit(4)
        try:
            schema = json.loads(schema_path.read_text(encoding='utf-8'))
            data_obj = json.loads(OUTPUT_JSON.read_text(encoding='utf-8'))
            jsonschema.validate(instance=data_obj, schema=schema)
            print('[validate] JSON schema validation: PASS')
        except jsonschema.ValidationError as ve:
            print('[validate] JSON schema validation FAILED:', ve.message, file=sys.stderr)
            sys.exit(5)
        except Exception as e:
            print('[validate] Unexpected validation error:', e, file=sys.stderr)
            sys.exit(6)
else:
    print('JSON skipped (flag --md-only).')

