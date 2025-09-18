# External E-commerce API Integrations

This module provides lightweight clients (requests-based) for official commerce APIs without scraping.

## Implemented / Planned
- Shopify Admin API
- Amazon Product Advertising API (PA-API) (partial: GetItems)
- eBay Browse API (search)

## Environment Variables
Create a `.env` (do not commit) based on `.env.example`:

```env
# Shopify
SHOPIFY_SHOP_DOMAIN=your-shop.myshopify.com
SHOPIFY_API_VERSION=2025-07
SHOPIFY_ACCESS_TOKEN=shpat_xxx
SHOPIFY_RPS=2

# Amazon PA-API
AMAZON_PAAPI_ACCESS_KEY=AKIA...
AMAZON_PAAPI_SECRET_KEY=...
AMAZON_PAAPI_PARTNER_TAG=yourtag-20
AMAZON_PAAPI_HOST=webservices.amazon.com
AMAZON_PAAPI_REGION=us-east-1
AMAZON_PAAPI_RPS=1

# eBay
EBAY_OAUTH_TOKEN=EbayOAuthTokenHere
EBAY_RPS=2
```

## Basic Usage
```python
from integrations.shopify_client import ShopifyClient
client = ShopifyClient.from_env()
products = client.list_products(limit=50)
print(products[:2])
```

## Caching
Responses cached under `.cache/api/<provider>/` hashed by request signature. TTL configurable via CLI or parameters.

## Rate Limiting
Simple in-process delay governed by `<PROVIDER>_RPS` env var.

## Error Handling
Exceptions raised:
- `ApiAuthError`: missing/invalid credentials
- `ApiRateLimitError`: 429 after retries
- `ApiRequestError`: other client/server/network errors

## ETL Pipeline
A unified pipeline normalizes product data from all providers into a single schema.

Structure:
```
data/raw/<provider>/<resource>/TIMESTAMP_runID.json
data/normalized/products.parquet (or CSV fallback)
metadata/pipeline_runs.jsonl
config/pipeline_config.yaml
```

### Config (`config/pipeline_config.yaml`)
Example snippet:
```yaml
providers:
  shopify:
    resources:
      products:
        limit: 100
        max_pages: 1
  amazon:
    resources:
      items:
        asins: ["B000TEST01","B000TEST02"]
  ebay:
    resources:
      search:
        queries:
          - "gaming mouse"
          - "wireless keyboard"
```

### Run Pipeline
```bash
python pipelines/run_pipeline.py --providers shopify,ebay --verbose
```
Options:
- `--providers` filter subset
- `--run-id` custom id (default auto)
- `--dry-run` skip persistence
- `--no-cache` bypass API caches
- `--limit` override config limit
- `--ttl` override cache TTL
- `--verbose` extra logging

### Normalized Schema
See `metadata/product_schema.json` for fields.

### Deduplication Logic
New records merged on `(source, source_id, raw_hash)`; identical hashes skipped.

### Orders (Shopify)
If `orders` resource is added under `shopify.resources` in `pipeline_config.yaml`, the pipeline will also fetch and normalize orders into `orders.parquet` (CSV fallback). Fields defined in `metadata/order_schema.json` include financial/fulfillment status, hashed customer email (SHA256 lowercase), SKU aggregation and monetary totals (subtotal, tax, shipping, discount, total).

Config example extension:
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
Notes:
* PII (raw customer email) not stored; only hash.
* Pagination simplified (max_pages) – extend with cursor parsing for full history.
* Future: incremental fetch using `updated_at_min`.

### Adding New Provider
1. Implement client similar to existing patterns.
2. Add entries in `pipeline_config.yaml`.
3. Extend normalization functions.
4. Re-run `run_pipeline.py`.

## Extending
Add new provider file using pattern:
1. Subclass or compose `BaseClient`.
2. Implement `from_env()` constructor.
3. Add resource-specific methods (pagination, query params, etc.).

## TODO (Next Steps)
- Advanced pagination with Link headers for Shopify
- Additional Amazon operations (SearchItems)
- Historical orders enrichment (incremental by updated_at)
- Alerting on anomalies (volume change)
