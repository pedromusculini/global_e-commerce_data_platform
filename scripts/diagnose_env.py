#!/usr/bin/env python
"""Environment & connectivity diagnostics for API providers.

Usage:
  python scripts/diagnose_env.py [--shopify] [--all]

Without flags runs basic variable presence checks. Use --shopify to test Shopify /shop.json.
"""
from __future__ import annotations
import os, sys, json, textwrap
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Reuse local .env loader logic (duplicated lightweight to avoid import side-effects)

def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    try:
        for line in env_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k,v = line.split('=',1)
            k = k.strip(); v = v.strip().strip('"').strip("'")
            if not k:
                continue
            existing = os.environ.get(k)
            if existing is None or existing.strip() == '':
                os.environ[k] = v
    except Exception:
        pass

load_env_file(PROJECT_ROOT / '.env')

MANDATORY: Dict[str, List[str]] = {
    'shopify': ['SHOPIFY_SHOP_DOMAIN','SHOPIFY_API_VERSION','SHOPIFY_ACCESS_TOKEN'],
    'amazon': ['AMAZON_PAAPI_ACCESS_KEY','AMAZON_PAAPI_SECRET_KEY','AMAZON_PAAPI_PARTNER_TAG'],
    'ebay': ['EBAY_OAUTH_TOKEN'],
}

OPTIONAL_LIMITERS = ['SHOPIFY_RPS','AMAZON_PAAPI_RPS','EBAY_RPS']

def mask(val: str | None) -> str | None:
    if not val:
        return val
    if len(val) <= 6:
        return '*' * len(val)
    return val[:4] + '...' + val[-4:]

def check_presence() -> Dict[str, Dict[str, str]]:
    report: Dict[str, Dict[str, str]] = {}
    for prov, keys in MANDATORY.items():
        prov_map = {}
        for k in keys:
            v = os.getenv(k)
            prov_map[k] = 'OK' if v and v.strip() else 'MISSING'
        report[prov] = prov_map
    return report

def print_report():
    presence = check_presence()
    print('\n[VARIABLE PRESENCE]')
    widest = max(len(k) for keys in MANDATORY.values() for k in keys)
    for prov, mapping in presence.items():
        print(f"- {prov.upper()}:")
        for k, status in mapping.items():
            raw = os.getenv(k)
            print(f"  {k.ljust(widest)} : {status:<8} {'' if status!='OK' else mask(raw)}")
    print('\n[OPTIONAL LIMITERS]')
    for k in OPTIONAL_LIMITERS:
        raw = os.getenv(k)
        if raw:
            print(f"  {k} = {raw}")
    print()

def test_shopify():
    import requests
    domain = os.getenv('SHOPIFY_SHOP_DOMAIN')
    version = os.getenv('SHOPIFY_API_VERSION') or '2024-10'
    token = os.getenv('SHOPIFY_ACCESS_TOKEN')
    missing = [k for k in ['SHOPIFY_SHOP_DOMAIN','SHOPIFY_ACCESS_TOKEN'] if not os.getenv(k)]
    if missing:
        print(f"[shopify] Skipping connectivity test (missing: {', '.join(missing)})")
        return
    url = f"https://{domain}/admin/api/{version}/shop.json"
    print(f"[shopify] GET {url}")
    try:
        resp = requests.get(url, headers={'X-Shopify-Access-Token': token, 'Accept':'application/json'}, timeout=20)
    except Exception as e:
        print(f"[shopify] ERROR network: {e}")
        return
    print(f"[shopify] Status: {resp.status_code}")
    body = resp.text[:300].replace('\n',' ')
    print(f"[shopify] Body  : {body}")
    if resp.status_code == 404:
        print(textwrap.dedent("""
            HINT 404: Check (1) correct shop domain, (2) API version exists (e.g. 2024-10), (3) token belongs to THIS shop, (4) app is installed.
        """))
    elif resp.status_code in (401,403):
        print("HINT 401/403: Invalid token, missing required scopes, or app not installed.")


def main(argv: List[str]):
    flags = set(a for a in argv[1:] if a.startswith('--'))
    print_report()
    if '--shopify' in flags or '--all' in flags:
        test_shopify()
    if '--all' in flags:
        # Future: add lightweight Amazon / eBay connectivity tests
        pass

if __name__ == '__main__':
    main(sys.argv)
