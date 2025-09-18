#!/usr/bin/env python
from __future__ import annotations
import argparse
import uuid
import time
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# Ensure project root is on sys.path when running directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables from local .env file (lightweight, no python-dotenv dependency)
def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    try:
        for line in env_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if not k:
                continue
            existing = os.environ.get(k)
            # Overwrite only if not present OR value is empty/whitespace. Preserve any existing non-empty value.
            if existing is None or existing.strip() == '':
                os.environ[k] = v
    except Exception:
    # Silent: parsing failure must not break the pipeline
        pass

_load_env_file(PROJECT_ROOT / '.env')

from integrations.shopify_client import ShopifyClient
from integrations.amazon_paapi_client import AmazonPAAPIClient
from integrations.ebay_client import EbayClient
from integrations.mock_provider import (
    generate_mock_products,
    generate_mock_orders,
    generate_fake_amazon_items,
    generate_fake_ebay_items,
    seed_mock,
)
from integrations.cache import save_cache
from pipelines.storage import (
    save_raw,
    load_existing_products,
    persist_products,
    append_run_log,
    sha256_json,
    utc_now_iso,
    load_existing_orders,
    persist_orders,
)
from pipelines.normalization import (
    normalize_shopify_products,
    normalize_amazon_items,
    normalize_ebay_search,
    merge_products,
    normalize_shopify_orders,
)

CONFIG_PATH = Path('config/pipeline_config.yaml')

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None  # type: ignore


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    if yaml is None:
        # Minimal YAML fallback: treat as empty
        return {}
    with CONFIG_PATH.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def parse_args():
    p = argparse.ArgumentParser(description='Run unified product ETL pipeline')
    p.add_argument('--providers', help='Comma separated provider filter (shopify,amazon,ebay)')
    p.add_argument('--run-id', default='auto')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--no-cache', action='store_true')
    p.add_argument('--limit', type=int, help='Override limit for resources when possible')
    p.add_argument('--ttl', type=int, help='Override cache TTL')
    p.add_argument('--verbose', action='store_true')
    p.add_argument('--fake', action='store_true', help='Generate fake data for failing or all providers (no real API calls)')
    p.add_argument('--fake-only', action='store_true', help='Skip all real API calls and generate synthetic data directly (overrides --fake)')
    p.add_argument('--key-mode', choices=['triple','pair'], default='triple', help='Product dedup mode: triple=(source,source_id,raw_hash), pair=(source,source_id) overwrite')
    p.add_argument('--seed', type=int, help='Deterministic seed for synthetic data (mock/fake)')
    p.add_argument('--debug-env', action='store_true', help='Print environment variables (sanitized) for troubleshooting')
    return p.parse_args()


def provider_enabled(name: str, cfg: Dict[str, Any], filter_list: List[str] | None) -> bool:
    if filter_list and name not in filter_list:
        return False
    return name in (cfg.get('providers') or {})


def main():
    args = parse_args()
    cfg = load_config()
    filter_list = [p.strip() for p in args.providers.split(',')] if args.providers else None
    defaults = cfg.get('defaults', {})

    run_id = args.run_id if args.run_id != 'auto' else uuid.uuid4().hex[:8]
    started_at = utc_now_iso()
    status = 'success'
    raw_files = 0
    new_products_count = 0
    updated_products_count = 0
    new_orders_count = 0
    updated_orders_count = 0

    existing_df = load_existing_products()
    existing_orders_df = load_existing_orders()

    all_records: List[Dict[str, Any]] = []
    order_records: List[Dict[str, Any]] = []

    try:
        if args.seed is not None:
            seed_mock(args.seed)
    # SHOPIFY PRODUCTS
        if provider_enabled('shopify', cfg, filter_list):
            try:
                if args.fake_only:
                    raise RuntimeError('offline-mode')  # force fake path
                if args.debug_env and args.verbose:
                    # Display relevant environment variables (masking sensitive token parts)
                    def _mask(val: str | None):
                        if not val:
                            return val
                        if len(val) <= 8:
                            return '*' * len(val)
                        return val[:4] + '...' + val[-4:]
                    env_snapshot = {k: _mask(os.getenv(k)) for k in ['SHOPIFY_SHOP_DOMAIN','SHOPIFY_API_VERSION','SHOPIFY_ACCESS_TOKEN','SHOPIFY_RPS']}
                    print('[debug] Shopify env ->', env_snapshot)
                shop_provider_cfg = cfg['providers']['shopify']['resources']
                shop_cfg = shop_provider_cfg.get('products', {})
                limit = args.limit or shop_cfg.get('limit') or defaults.get('limit', 50)
                max_pages = shop_cfg.get('max_pages', defaults.get('max_pages', 1))
                client = ShopifyClient.from_env()
                products = client.list_products(limit=limit, max_pages=max_pages, use_cache=not args.no_cache, cache_ttl=args.ttl or defaults.get('cache_ttl',0))
                raw_path = save_raw('shopify','products', products, run_id)
                raw_hash = sha256_json(products)
                recs = normalize_shopify_products(products, str(raw_path), raw_hash)
                all_records.extend(recs)
                raw_files += 1
                # SHOPIFY ORDERS (optional)
                if 'orders' in shop_provider_cfg:
                    orders_cfg = shop_provider_cfg.get('orders', {})
                    orders_limit = orders_cfg.get('limit', limit)
                    orders_pages = orders_cfg.get('max_pages', 1)
                    status_filter = orders_cfg.get('status')
                    shop_orders = client.list_orders(status=status_filter, limit=orders_limit, max_pages=orders_pages, use_cache=not args.no_cache, cache_ttl=args.ttl or defaults.get('cache_ttl',0))
                    if shop_orders:
                        raw_orders_path = save_raw('shopify','orders', shop_orders, run_id)
                        raw_orders_hash = sha256_json(shop_orders)
                        ord_recs = normalize_shopify_orders(shop_orders, str(raw_orders_path), raw_orders_hash)
                        order_records.extend(ord_recs)
                        raw_files += 1
            except Exception as e:
                if args.fake:
                    if args.verbose:
                        print(f'[fake] Shopify real fetch failed ({e}); generating synthetic products/orders')
                    # Quantities derived from config or defaults
                    shop_provider_cfg = (cfg.get('providers', {}).get('shopify') or {}).get('resources', {})
                    prod_limit = args.limit or ((shop_provider_cfg.get('products') or {}).get('limit') or defaults.get('limit', 20))
                    ord_limit = args.limit or (shop_provider_cfg.get('orders') or {}).get('limit', 10)
                    mock_products = generate_mock_products(prod_limit)
                    raw_path = save_raw('shopify','products_fake', mock_products, run_id)
                    raw_hash = sha256_json(mock_products)
                    recs = normalize_shopify_products(mock_products, str(raw_path), raw_hash)
                    all_records.extend(recs)
                    raw_files += 1
                    mock_orders = generate_mock_orders(ord_limit, [p['id'] for p in mock_products])
                    if mock_orders:
                        raw_orders_path = save_raw('shopify','orders_fake', mock_orders, run_id)
                        raw_orders_hash = sha256_json(mock_orders)
                        ord_recs = normalize_shopify_orders(mock_orders, str(raw_orders_path), raw_orders_hash)
                        order_records.extend(ord_recs)
                        raw_files += 1
                else:
                    if args.verbose:
                        print(f'[warn] Shopify skipped: {e}')
        # AMAZON
        if provider_enabled('amazon', cfg, filter_list):
            try:
                if args.fake_only:
                    raise RuntimeError('offline-mode')
                am_cfg = cfg['providers']['amazon']['resources'].get('items', {})
                asins = am_cfg.get('asins', [])[:10]
                if asins:
                    client = AmazonPAAPIClient.from_env()
                    resp = client.get_items(asins)
                    raw_path = save_raw('amazon','items', resp, run_id)
                    raw_hash = sha256_json(resp)
                    recs = normalize_amazon_items(resp, str(raw_path), raw_hash)
                    all_records.extend(recs)
                    raw_files += 1
            except Exception as e:
                if args.fake:
                    if args.verbose:
                        print(f'[fake] Amazon real fetch failed ({e}); generating synthetic items')
                    fake_items = generate_fake_amazon_items( (args.limit or 10) )
                    raw_path = save_raw('amazon','items_fake', fake_items, run_id)
                    raw_hash = sha256_json(fake_items)
                    recs = normalize_amazon_items(fake_items, str(raw_path), raw_hash)
                    all_records.extend(recs)
                    raw_files += 1
                else:
                    if args.verbose:
                        print(f'[warn] Amazon skipped: {e}')
        # EBAY
        if provider_enabled('ebay', cfg, filter_list):
            try:
                if args.fake_only:
                    raise RuntimeError('offline-mode')
                eb_cfg = cfg['providers']['ebay']['resources'].get('search', {})
                queries = eb_cfg.get('queries', [])
                limit_override = args.limit or defaults.get('limit', 50)
                if queries:
                    client = EbayClient.from_env()
                    for q in queries:
                        items = client.search_items(q, limit=limit_override, use_cache=not args.no_cache, cache_ttl=args.ttl or defaults.get('cache_ttl',0))
                        raw_path = save_raw('ebay','search', items, run_id, tag=q.replace(' ','-'))
                        raw_hash = sha256_json(items)
                        recs = normalize_ebay_search(items, str(raw_path), raw_hash)
                        all_records.extend(recs)
                        raw_files += 1
            except Exception as e:
                if args.fake:
                    if args.verbose:
                        print(f'[fake] eBay real fetch failed ({e}); generating synthetic search results')
                    # Use first query or generic label
                    eb_cfg = (cfg.get('providers', {}).get('ebay') or {}).get('resources', {}).get('search', {})
                    queries = eb_cfg.get('queries', ['mock'])
                    limit_override = args.limit or defaults.get('limit', 10)
                    for q in queries:
                        fake_items = generate_fake_ebay_items(limit_override, q)
                        raw_path = save_raw('ebay','search_fake', fake_items, run_id, tag=q.replace(' ','-'))
                        raw_hash = sha256_json(fake_items)
                        recs = normalize_ebay_search(fake_items, str(raw_path), raw_hash)
                        all_records.extend(recs)
                        raw_files += 1
                else:
                    if args.verbose:
                        print(f'[warn] eBay skipped: {e}')

    # MOCK (local synthetic data)
        if provider_enabled('mock', cfg, filter_list):
            try:
                mock_cfg = cfg['providers']['mock']['resources']
                prod_cfg = mock_cfg.get('products', {})
                ord_cfg = mock_cfg.get('orders', {})
                limit_products = args.limit or prod_cfg.get('limit', 20)
                limit_orders = args.limit or ord_cfg.get('limit', 10)
                mock_products = generate_mock_products(limit_products)
                raw_path = save_raw('mock','products', mock_products, run_id)
                raw_hash = sha256_json(mock_products)
                # Reuse Shopify normalizer (synthetic structure kept compatible)
                recs = normalize_shopify_products(mock_products, str(raw_path), raw_hash)
                all_records.extend(recs)
                raw_files += 1
                mock_orders = generate_mock_orders(limit_orders, [p['id'] for p in mock_products])
                if mock_orders:
                    raw_orders_path = save_raw('mock','orders', mock_orders, run_id)
                    raw_orders_hash = sha256_json(mock_orders)
                    ord_recs = normalize_shopify_orders(mock_orders, str(raw_orders_path), raw_orders_hash)
                    order_records.extend(ord_recs)
                    raw_files += 1
            except Exception as e:
                if args.verbose:
                    print(f'[warn] Mock provider failed: {e}')

        if pd is None:
            if args.verbose:
                print('[info] pandas not installed; skipping persistence')
        else:
            # Products
            merged_df, new_products_count, updated_products_count = merge_products(existing_df, all_records, key_mode=args.key_mode)
            if not args.dry_run and (new_products_count > 0 or updated_products_count > 0):
                persist_products(merged_df)
                if args.verbose:
                    msg_extra = '' if updated_products_count == 0 else f', {updated_products_count} updated'
                    print(f'[info] Persisted products, total now {len(merged_df)} (added {new_products_count}{msg_extra})')
            # Orders
            if order_records:
                import pandas as _pd
                new_orders_df = _pd.DataFrame(order_records)
                if existing_orders_df is None or existing_orders_df.empty:
                    # seed initial
                    new_orders_count = len(new_orders_df)
                    if not args.dry_run and new_orders_count > 0:
                        persist_orders(new_orders_df)
                        if args.verbose:
                            print(f'[info] Persisted orders (added {new_orders_count})')
                else:
                    if args.key_mode == 'triple':
                        key_cols = ['source','order_id','raw_hash']
                        existing_keys = set(tuple(r) for r in existing_orders_df[key_cols].values.tolist())
                        mask = [tuple(row[k] for k in key_cols) not in existing_keys for row in new_orders_df.to_dict('records')]
                        new_orders_df = new_orders_df[mask]
                        new_orders_count = len(new_orders_df)
                        if not args.dry_run and new_orders_count > 0:
                            persist_orders(_pd.concat([existing_orders_df, new_orders_df], ignore_index=True))
                            if args.verbose:
                                print(f'[info] Persisted orders (added {new_orders_count})')
                    else:  # pair mode
                        # Keep only latest per (source, order_id)
                        key_cols_pair = ['source','order_id']
                        # Determine new (not previously seen) order ids
                        existing_pair_keys = set(tuple(r) for r in existing_orders_df[key_cols_pair].values.tolist())
                        new_orders_df = (new_orders_df.sort_values('ingested_at')
                                         .drop_duplicates(key_cols_pair, keep='last'))
                        # Count truly new keys
                        batch_keys = [tuple(r[k] for k in key_cols_pair) for r in new_orders_df.to_dict('records')]
                        new_orders_count = len([k for k in batch_keys if k not in existing_pair_keys])
                        if not args.dry_run:
                            # Remove existing rows whose key appears in new batch (overwrite semantics)
                            overwrite_keys = set(batch_keys)
                            updated_orders_count = len([k for k in overwrite_keys if k in existing_pair_keys])
                            existing_filtered = existing_orders_df[[ (row_source, row_oid) not in overwrite_keys for row_source, row_oid in existing_orders_df[key_cols_pair].values ]]
                            persist_orders(_pd.concat([existing_filtered, new_orders_df], ignore_index=True))
                            if args.verbose and (new_orders_count > 0 or updated_orders_count > 0):
                                print(f'[info] Persisted orders (added {new_orders_count}, {updated_orders_count} updated)')

    except Exception as e:
        status = 'error'
        if args.verbose:
            import traceback; traceback.print_exc()

    finished_at = utc_now_iso()
    duration = 0.0  # could compute difference
    log_record = {
        'run_id': run_id,
        'started_at': started_at,
        'finished_at': finished_at,
        'status': status,
        'raw_files': raw_files,
        'new_products': new_products_count,
    'new_orders': new_orders_count,
    'updated_products': updated_products_count,
        'updated_orders': updated_orders_count,
        'providers': filter_list or list((cfg.get('providers') or {}).keys()),
    }
    if not args.dry_run:
        append_run_log(log_record)
    if args.verbose:
        print(json.dumps(log_record, indent=2))

if __name__ == '__main__':
    main()
