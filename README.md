# Global E-commerce Data Platform

![CI](https://github.com/pedromusculini/global_e-commerce_data_platform/actions/workflows/ci.yml/badge.svg)
![Coverage](./badges/coverage.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

Modular platform for global e-commerce data ingestion, normalization and analysis:

* EDA notebooks (BR market and international) exportable to JSON/Markdown
* Automated branding asset system (multi-format PNG/SVG/ICO/GIF + manifest & contrast report)
* Official API integrations (Shopify, Amazon PA-API, eBay) with TTL cache, rate limiting and unified CLI
* Idempotent ETL pipeline (raw -> normalized -> Parquet/CSV persistence + execution log)

---

## CI Status & Quality Gates

Automated quality pipeline (`.github/workflows/ci.yml`) executed on every push/PR:

1. Checkout & Python 3.11 setup (dependency cache)
2. Optional system libs (WeasyPrint rendering support – best effort)
3. Install dependencies
4. Deterministic synthetic pipeline run (`--fake-only --seed 42 --limit 50 --key-mode triple`)
5. Figure generation (`reports/figures/*.png`)
6. Comparative report + JSON emission & strict JSON Schema validation (`--validate`)
7. Unit tests (schema, dedup logic, figure integrity)
8. Coverage run (XML, HTML, SVG badge)
9. Artifact upload (coverage, report, figures, pipeline log)

Quality gates: schema validation failure, test failure, or pipeline exception stops the build.

Badges:
* CI (GitHub Actions status for `main`)
* Coverage (SVG generated and versioned in `badges/coverage.svg` on each primary branch build)

Planned enhancements:
* Coverage threshold enforcement
* Lint (ruff) & formatting (black) stage
* Security scanning (pip-audit)
* Nightly schedule for drift detection

---

## Overview

Currently implemented flow:
1. Brand asset generation for consistent distribution
2. Data acquisition via official APIs OR deterministic synthetic fallback (`--fake` / `--fake-only` + `--seed`)
3. Raw storage versioned by timestamp + run_id
4. Normalization into unified product & order schemas (`metadata/product_schema.json`, `metadata/order_schema.json`)
5. Incremental merge with configurable dedup (`--key-mode` pair|triple)
6. Parquet persistence (CSV fallback) + JSONL run logging (`pipeline_runs.jsonl`)
7. Feature engineering (price buckets, source/category frequencies, rolling GMV/AOV, recency)
8. Unified comparative reporting in `reports/comparative_report.md` (legacy Intl vs BR + current multi‑provider KPIs)

### Architecture Diagram (High-Level)

```mermaid
flowchart LR
  subgraph Providers
    A1[Shopify API]:::prov
    A2[Amazon PA-API]:::prov
    A3[eBay Browse]:::prov
    A4[Mock / Fake Gen]:::prov
  end

  A1 --> R[Raw JSON Dumps]
  A2 --> R
  A3 --> R
  A4 --> R

  R --> N[Normalization Layer (products / orders)]
  N --> M[Merge & Dedup (key-mode: triple / pair)]
  M --> P[Parquet Store]
  M --> L[Run Log JSONL]
  P --> EDA[Jupyter / Analytics]

  classDef prov fill=#1e88e5,stroke=#0d47a1,color=#fff;
  classDef default fill=#f5f5f5,stroke=#777;
```

### Data Lineage Snapshot
Provider payload -> raw file (timestamped) -> normalized rows -> merged dataset -> analytical consumption (notebooks / future BI).

---

## Core Structure

```
brand/                    # Branding assets & generator script
integrations/             # API clients + detailed README
pipelines/                # ETL code (storage, normalization, runner)
config/pipeline_config.yaml
metadata/product_schema.json
data/
  raw/<provider>/<resource>/  # Raw JSON dumps
  normalized/                 # products.parquet / csv
scripts/build_assets.py
scripts/fetch_external_data.py
notebooks/ (if applicable)
```

---

## Key Dependencies

See `requirements.txt`. Highlights:
* pandas / numpy – data manipulation
* requests – HTTP APIs
* Pillow – branding image generation
* seaborn / matplotlib – EDA
* pyarrow (optional) – optimized Parquet (fallback to pandas engine if missing)
* PyYAML – reads `pipeline_config.yaml` (graceful fallback if absent)

Install:
```bash
pip install -r requirements.txt
```

---

## Branding Asset Generation

Run: `python scripts/build_assets.py`
Outputs: multiple sizes/variants + JSON manifest + compressed ZIP package.

---

## On-Demand Fetch CLI

Direct call of a single provider/resource without running the full pipeline:
```bash
python scripts/fetch_external_data.py --provider shopify --resource products --limit 50
```

Additional options: `--ids`, `--query`, `--ttl`, `--no-cache`.

---

## ETL Pipeline

Default execution (all providers with env credentials present):
```bash
python pipelines/run_pipeline.py --verbose
```

Filter providers:
```bash
python pipelines/run_pipeline.py --providers shopify,ebay
```

Dry-run (skips persistence & final merge log write):
```bash
python pipelines/run_pipeline.py --dry-run --providers ebay
```

Key parameters:
* `--limit` overrides config resource limit
* `--ttl` overrides cache TTL (seconds)
* `--no-cache` bypasses existing cache
* `--run-id` sets manual identifier (audit facilitation)

Run logs: `pipeline_runs.jsonl` with per-run metrics.

### Deduplication
Incremental merge core key: `(source, source_id)` with optional version discriminator `raw_hash`.

`--key-mode` semantics:
* `triple` (default): `(source, source_id, raw_hash)` preserves multiple payload versions (audit/history friendly).
* `pair`: `(source, source_id)` collapses to latest snapshot; prior versions increment update counters.

Emitted metrics: `new_products`, `updated_products`, `new_orders`, `updated_orders` → consumed by the unified comparative report.

### Orders (Shopify + Synthetic)
Experimental unified order schema (`metadata/order_schema.json`).
Included fields: monetary breakdown (price, tax, discounts, shipping), status fields, hashed customer email, SKU set and line item counts.

#### Order Deduplication and `--key-mode`
Orders now follow the same configurable philosophy used for products:

* `--key-mode triple` (default): keeps multiple versions per `(source, order_id, raw_hash)` allowing a raw change history (e.g. status, amounts). `new_orders_count` counts each distinct version.
* `--key-mode pair`: idempotent overwrite by `(source, order_id)`. Only the latest row (highest `ingested_at`) is retained. `new_orders_count` counts only previously unseen keys; overwritten versions are excluded. Verbose logs show `(added X, Y updated)`.

Rationale:
* `triple` favors full audit & temporal reconstruction.
* `pair` favors compact “current snapshot” analytics.

Planned next steps: explicit `updated_orders_count` metric (implemented) and lightweight SCD-style diffing for key status/price fields.

Config example (in `config/pipeline_config.yaml`):
```yaml
providers:
  shopify:
    resources:
      products:
        limit: 100
        max_pages: 1
      orders:
        limit: 50
        max_pages: 1
        status: any
```

If `orders` is present under `shopify.resources`, the pipeline will:
1. Fetch orders (limited pages)
2. Save raw JSON under `data/raw/shopify/orders/`
3. Normalize to `orders.parquet` (CSV fallback)
4. Deduplicate according to `--key-mode` (see section above)

Future enhancements: full pagination (cursor), partial updates (updated_at > last_run), SCD on financial/fulfillment status.

### Mock Provider (Synthetic Data)
For local development without real API credentials you can use the built-in `mock` provider.

Config snippet (already included):
```yaml
providers:
  mock:
    resources:
      products:
        limit: 25
      orders:
        limit: 10
```

Run only mock (dry-run first):
```bash
python pipelines/run_pipeline.py --providers mock --dry-run --verbose
```

Then persist synthetic datasets:
```bash
python pipelines/run_pipeline.py --providers mock --verbose
```

Artifacts produced:
* Raw: `data/raw/mock/products/` & `data/raw/mock/orders/`
* Normalized products appended into `data/normalized/products.parquet`
* Normalized orders appended into `data/normalized/orders.parquet`

Notes:
* Synthetic product structure intentionally mimics Shopify fields so existing normalizer is reused.
* Useful for testing merge/dedup logic, run log, and storage formats before securing real credentials.
* Regenerate multiple times to simulate new runs (hash will differ if generated values change).

### Fake Mode for Real Providers (`--fake`)
When you include real providers (shopify, amazon, ebay) but lack credentials or they fail (auth, 404, etc.), you can append `--fake` to force synthetic fallback data per provider without changing the config.

Example (attempt real, fallback to synthetic on failure):
```bash
python pipelines/run_pipeline.py --providers shopify,amazon,ebay --fake --verbose --dry-run
```

What happens with `--fake`:
* Shopify failure → generates `products_fake` / `orders_fake` raw dumps under `data/raw/shopify/`
* Amazon failure → generates `items_fake` under `data/raw/amazon/`
* eBay failure → generates `search_fake` results (one per configured query)

All synthetic outputs still normalize into unified product / order datasets — enabling full ETL validation end-to-end.

#### `--fake` vs `--fake-only`
* `--fake`: attempts real API calls; on failure (auth/404/etc.) falls back to on-the-fly synthetic data.
* `--fake-only`: skips all network attempts and generates only synthetic data for the requested providers.

Example offline run (no network calls at all):
```bash
python pipelines/run_pipeline.py --providers shopify,amazon,ebay --fake-only --limit 10 --verbose
```

---

## Unified Comparative Report
Central consolidated analytics: `reports/comparative_report.md`.

Includes:
* Legacy International vs Brazilian visual comparison (figures relocated to `reports/figures/legacy/`).
* Current multi‑provider KPIs: product counts, order span, GMV, AOV, source distribution.
* Feature engineering overview (price buckets, rolling 7d GMV / AOV, frequency & recency metrics).
* Data quality indicators (outlier ratios, duplicate logical key pairs).
* Run log interpretation (new vs updated ratios) + recommended next steps.

Regenerate end-to-end:
1. Run pipeline (real or synthetic): `python pipelines/run_pipeline.py --providers shopify,amazon,ebay --fake-only --limit 50 --seed 51 --key-mode triple --verbose`
2. Execute `notebooks/quick_insights.ipynb` (figures + enriched parquet)
3. Refresh report metrics: `python scripts/generate_comparative_report.py`

Figure layout:
```
reports/
  figures/
    orders_time_series.png
    orders_aov_distribution.png
    legacy/
      sales_distribution.png
      product_category_distribution.png
      order_status_br.png
      delivery_times_br.png
```

Backups retained as `comparative_report.md.backup_legacy` & `reports/comparative_report.md.backup_legacy`.

### Primary Distribution Artifact (Markdown First)
The canonical deliverable for sharing and reviewing insights is the Markdown file:

`reports/comparative_report.md`

Why Markdown is prioritized:
* Version-control friendly (readable diffs in Pull Requests)
* Immediate visualization in GitHub/IDE without build
* Single source for derived exports (HTML / PDF)

Regenerate + export workflow (opcional):
```bash
# 1. Pipeline + figures
python pipelines/run_pipeline.py --providers shopify,amazon,ebay --fake-only --limit 50 --seed 51 --key-mode triple --verbose
python scripts/generate_comparative_report.py

# 2. (Optional) Styled HTML
python scripts/export_report.py --html

# 3. (Optional) PDF (better with WeasyPrint installed)
python scripts/export_report.py --pdf
```

In most portfolio or recruiter review scenarios you can simply point to the Markdown file above—HTML/PDF outputs are complementary, not required.

---

## Environment Variables

Create `.env` (full example in `integrations/README.md`): Shopify, Amazon PA-API, eBay tokens + per-provider RPS (`<PROVIDER>_RPS`).

Loading: each client exposes `from_env()`; if credentials are missing the provider is skipped gracefully.

### Diagnostics
Use the helper script to validate presence and connectivity:
```bash
python scripts/diagnose_env.py --shopify
```
Output sections:
* VARIABLE PRESENCE: shows OK / MISSING (tokens masked)
* Shopify test (/shop.json): returns status code & first bytes of body

Common interpretations:
* 404 on /shop.json – wrong domain, API version not released yet, or token from different store
* 401/403 – token invalid or missing required scopes / app not installed
* 200 – connectivity OK

Full run (future providers) will use `--all`.

---

## Technical Roadmap (Next Steps)

Short Term:
* Execution validation with richer dry-run mocks
* Orders normalization (Shopify first)
* Unit tests (hashing, merge, normalizers)

Mid Term:
* SCD versioning (price/title change history)
* Orchestration (cron, Airflow, or GitHub Actions schedule)
* Alerts (volume anomaly / provider failure)

Long Term:
* Enrichment (external categories, unified taxonomy)
* Derived metrics & model feature generation
* Integrated analytics dashboard (Streamlit / external BI)

See also: [Orchestration Patterns](docs/orchestration.md) for scheduling & operations guidance.

---

## Data Dictionary (Preview)
Full table docs (in progress) will live under `metadata/data_dictionary.md`.

| Entity | Field | Type | Description | Source |
|--------|-------|------|-------------|--------|
| product | source | str | Provider identifier (shopify/amazon/ebay/mock) | normalized |
| product | source_id | str | Provider-native product id | provider payload |
| product | title | str | Title/name snapshot | provider payload |
| product | price_amount | float | First variant/listing price | provider payload |
| product | raw_hash | str | SHA-256 hash of raw JSON file | computed |
| order | order_id | str | Shopify order id | provider payload |
| order | total_price | float | Final total (tax+shipping-discounts) | provider payload |
| order | financial_status | str | Payment status snapshot | provider payload |
| order | raw_hash | str | SHA-256 hash of raw JSON file | computed |

Planned additions: change_type (for SCD), ingestion_run_id, diff_metadata.

---

## Hiring Highlights
Why this repository demonstrates readiness for an entry-level Data / Analytics Engineering role:

* Idempotent & incremental ingestion with configurable version semantics (triple vs pair).
* Offline deterministic simulation (`--fake`, `--fake-only`, `--seed`) enabling fast iteration without secrets.
* Clear separation: acquisition (integrations) vs normalization vs storage.
* Extensibility: adding a new provider only requires raw fetch + normalizer.
* Reproducibility: run logs + hashed raw payloads for audit/backfill potential.
* Test coverage starting on core merge logic (dedup correctness).
* Production-minded touches: graceful env loading, fallback strategies, verbosity & debug switches.
* Portfolio polish: branding assets, architecture diagram, unified comparative report & advanced insights notebook.

Next polish targets for recruiters:
1. Add CI badge & simple GitHub Actions workflow (lint + tests).
2. Expand tests to cover orders and error scenarios.
3. Provide sample analytics notebook with 2–3 insights (price distribution, order volume trend, top SKUs).
4. Add lightweight anomaly check (e.g. sudden drop in new_products).
5. Add coverage badge (even if minimalist) to reinforce quality mindset.

---

## Contributing
Pull requests welcome. Add tests for new public functions when feasible.

---

## License
MIT - see `LICENSE`.

---

## Disclaimer
Integrations use only official APIs; user is responsible for compliance with each platform's ToS.
