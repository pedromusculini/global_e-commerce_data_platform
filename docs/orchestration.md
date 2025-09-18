# Orchestration Patterns

This document outlines options for scheduling and operating the ETL pipeline (`pipelines/run_pipeline.py`).

## 1. Minimal Local Cron (Linux/macOS)
Example: run every 6 hours logging to a dated file.
```
0 */6 * * * /usr/bin/python /path/to/repo/pipelines/run_pipeline.py >> /path/to/repo/logs/pipeline_$(date +\%Y\%m\%d).log 2>&1
```
Rotate logs with `logrotate` or a simple weekly cleanup script.

## 2. Windows Task Scheduler
Command:
```
Program/script:  C:\\Python311\\python.exe
Arguments:       pipelines\\run_pipeline.py --verbose
Start in:        C:\\path\\to\\repo
```
Trigger: Daily repeat every 4 hours, for a duration of 1 day (repeat indefinitely through task settings).

## 3. GitHub Actions Scheduled Workflow
`.github/workflows/pipeline.yml`:
```yaml
name: scheduled-pipeline
on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours UTC
  workflow_dispatch: {}

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install deps
        run: pip install -r requirements.txt --quiet
      - name: Run pipeline
        env:
          SHOPIFY_SHOP_DOMAIN: ${{ secrets.SHOPIFY_SHOP_DOMAIN }}
          SHOPIFY_API_VERSION: ${{ secrets.SHOPIFY_API_VERSION }}
          SHOPIFY_ACCESS_TOKEN: ${{ secrets.SHOPIFY_ACCESS_TOKEN }}
          AMAZON_PAAPI_ACCESS_KEY: ${{ secrets.AMAZON_PAAPI_ACCESS_KEY }}
          AMAZON_PAAPI_SECRET_KEY: ${{ secrets.AMAZON_PAAPI_SECRET_KEY }}
          AMAZON_PAAPI_PARTNER_TAG: ${{ secrets.AMAZON_PAAPI_PARTNER_TAG }}
          AMAZON_PAAPI_HOST: 'webservices.amazon.com'
          AMAZON_PAAPI_REGION: 'us-east-1'
          EBAY_OAUTH_TOKEN: ${{ secrets.EBAY_OAUTH_TOKEN }}
        run: |
          python pipelines/run_pipeline.py --verbose
      - name: Upload normalized dataset artifact
        uses: actions/upload-artifact@v4
        with:
          name: normalized-products
          path: data/normalized/
```

## 4. Airflow DAG (Simplified)
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='taskgrok_products_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 */6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['etl','products']
) as dag:

    run_products = BashOperator(
        task_id='run_pipeline',
        bash_command='python /opt/airflow/repo/pipelines/run_pipeline.py --verbose'
    )
```
Add secrets via Airflow Connections / environment variables in the worker container.

## 5. Makefile Helper
Add to `Makefile`:
```
run-pipeline:
	python pipelines/run_pipeline.py --verbose
```
Then:
```
make run-pipeline
```

## 6. Observability & Guardrails
Recommended additions:
- Append run duration (compute diff between timestamps) and failure stack traces (already partially).
- Add simple anomaly detection: compare `new_products` with rolling median; alert on spike/drop.
- Push run log line to external sink (CloudWatch, OpenSearch, or SQLite) for retention.

## 7. Incremental Improvements Roadmap
| Area | Immediate | Later |
|------|-----------|-------|
| Reliability | Retry per-provider with backoff (already basic) | Circuit breaker per provider |
| Schema | Add orders schema | SCD versioning for products |
| Storage | Partition Parquet by date | Delta/iceberg-like table format |
| Monitoring | Volume anomaly threshold | SLA dashboards |
| Security | Secret scanning pre-commit | Vault/Secrets Manager integration |

## 8. Local Development Tips
- Use `--dry-run` while tweaking normalization logic.
- Temporarily limit providers: `--providers shopify`.
- Override limit for quick tests: `--limit 10`.
- Clear cache: delete `.cache/api/<provider>` or use `--no-cache`.

## 9. Failure Scenarios
| Scenario | Handling | Next Improvement |
|----------|----------|------------------|
| Missing credentials | Provider skipped | Emit structured skip log entry |
| Partial provider failure | Others continue (try/except) | Aggregated error summary per run |
| Schema drift | Downstream may break silently | Add schema validation step before persist |
| Duplicate identical run | Dedup via raw_hash | Versioning for changed but semantically identical items |

## 10. Data Quality Hooks (Future)
Add pre-persist validation:
- Required fields non-null: `source`, `source_id`, `title`
- Price numeric & positive when present
- URL scheme starts with http/https

Fail (or quarantine record) if violations exceed threshold.

---
This document can evolve as the platform grows; treat as a living playbook.
