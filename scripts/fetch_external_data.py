#!/usr/bin/env python
"""Unified CLI to fetch data from external e-commerce APIs (Shopify, Amazon PA-API, eBay).

Examples:
  python scripts/fetch_external_data.py --provider shopify --resource products --limit 100 --max-pages 2 --out data/shopify_products.json
  python scripts/fetch_external_data.py --provider shopify --resource orders --status any --limit 50 --out data/shopify_orders.json
  python scripts/fetch_external_data.py --provider amazon --resource items --ids B0CXXXXXXX,B0CYYYYYYY --out data/amazon_items.json
  python scripts/fetch_external_data.py --provider ebay --resource search --query "gaming mouse" --limit 20 --out data/ebay_search.json

Options:
  --ttl seconds (cache TTL)
  --no-cache (disable cache)
  --verbose

"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import List
import os

# Carrega .env local se presente (sem depender de python-dotenv)
def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    try:
        for line in env_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=',1)
            k = k.strip(); v = v.strip().strip('"').strip("'")
            existing = os.environ.get(k)
            if existing is None or existing.strip() == '':
                os.environ[k] = v
    except Exception:
        pass

_load_env_file(Path('.env'))

from integrations.shopify_client import ShopifyClient
from integrations.amazon_paapi_client import AmazonPAAPIClient
from integrations.ebay_client import EbayClient
from integrations.cache import load_cache, save_cache


def parse_args():
    p = argparse.ArgumentParser(description='Fetch external e-commerce data')
    p.add_argument('--provider', required=True, choices=['shopify', 'amazon', 'ebay'])
    p.add_argument('--resource', required=True)
    p.add_argument('--limit', type=int, default=50)
    p.add_argument('--max-pages', type=int, default=1)
    p.add_argument('--status')
    p.add_argument('--ids', help='Comma separated ASINs for amazon items')
    p.add_argument('--query', help='Search query for eBay')
    p.add_argument('--out', required=True, help='Output JSON file path')
    p.add_argument('--ttl', type=int, default=0, help='Cache TTL seconds')
    p.add_argument('--no-cache', action='store_true')
    p.add_argument('--verbose', action='store_true')
    return p.parse_args()


def main():
    args = parse_args()
    provider = args.provider
    resource = args.resource.lower()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Composite cache key (resource + params) for top-level call
    cache_key = [provider, resource, str(args.limit), str(args.max_pages or 1)]
    if args.status:
        cache_key.append(args.status)
    if args.ids:
        cache_key.append(args.ids)
    if args.query:
        cache_key.append(args.query)

    if not args.no_cache:
        cached = load_cache('cli', cache_key, ttl_seconds=args.ttl)
        if cached is not None:
            if args.verbose:
                print('[cache-hit] Returning cached result')
            out_path.write_text(json.dumps(cached, ensure_ascii=False, indent=2), encoding='utf-8')
            return

    if provider == 'shopify':
        client = ShopifyClient.from_env()
        if resource == 'products':
            data = client.list_products(limit=args.limit, max_pages=args.max_pages, use_cache=not args.no_cache, cache_ttl=args.ttl)
        elif resource == 'orders':
            data = client.list_orders(status=args.status, limit=args.limit, max_pages=args.max_pages, use_cache=not args.no_cache, cache_ttl=args.ttl)
        else:
            raise SystemExit('Unsupported Shopify resource')
    elif provider == 'amazon':
        client = AmazonPAAPIClient.from_env()
        if resource == 'items':
            if not args.ids:
                raise SystemExit('--ids required for amazon items')
            asin_list: List[str] = [x.strip() for x in args.ids.split(',') if x.strip()]
            data = client.get_items(asin_list)
        else:
            raise SystemExit('Unsupported Amazon resource')
    elif provider == 'ebay':
        client = EbayClient.from_env()
        if resource == 'search':
            if not args.query:
                raise SystemExit('--query required for ebay search')
            data = client.search_items(args.query, limit=args.limit, use_cache=not args.no_cache, cache_ttl=args.ttl)
        else:
            raise SystemExit('Unsupported eBay resource')
    else:
        raise SystemExit('Unknown provider')

    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    if not args.no_cache:
        save_cache('cli', cache_key, data)
    if args.verbose:
        print(f'[done] Wrote {out_path}')

if __name__ == '__main__':
    main()
