from __future__ import annotations
import os
from typing import Any, Dict, List, Optional
from .base_client import BaseClient
from .cache import load_cache, save_cache

class ShopifyClient(BaseClient):
    """Minimal Shopify Admin API client (products & orders listing)."""
    RATE_LIMIT_RPS_ENV = 'SHOPIFY_RPS'

    def __init__(self, shop_domain: str, api_version: str, access_token: str, timeout: int = 30):
        super().__init__(timeout=timeout)
        self.shop_domain = shop_domain
        self.api_version = api_version
        self.access_token = access_token
        self.BASE_URL = f"https://{shop_domain}/admin/api/{api_version}"

    @classmethod
    def from_env(cls) -> 'ShopifyClient':
        shop_domain = BaseClient.env('SHOPIFY_SHOP_DOMAIN')
        api_version = os.getenv('SHOPIFY_API_VERSION', '2025-07')
        access_token = BaseClient.env('SHOPIFY_ACCESS_TOKEN')
        return cls(shop_domain, api_version, access_token)  # type: ignore[arg-type]

    def _headers(self) -> Dict[str, str]:
        return {
            'X-Shopify-Access-Token': self.access_token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def list_products(self, limit: int = 50, max_pages: int = 1, use_cache: bool = True, cache_ttl: int = 0) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        page_info: Optional[str] = None
        pages_fetched = 0
        while pages_fetched < max_pages:
            key_parts = ['products', str(limit), page_info or 'first']
            if use_cache:
                cached = load_cache('shopify', key_parts, ttl_seconds=cache_ttl)
                if cached is not None:
                    products.extend(cached)
                    pages_fetched += 1
                    if len(cached) < limit:  # last page likely
                        break
                    page_info = cached[-1].get('_page_info_next')  # custom marker if stored
                    continue

            params = {'limit': limit}
            if page_info:
                params['page_info'] = page_info
            data = self._request('GET', 'products.json', params=params, headers=self._headers())
            items = data.get('products', []) if isinstance(data, dict) else []

            # Extract next page from Link header if present (requests stored in response? we used base _request returning json only)
            # Simplification: rely on items length; real cursor requires capturing headers; advanced impl would adapt _request to return resp object.

            save_cache('shopify', key_parts, items)
            products.extend(items)
            pages_fetched += 1
            if len(items) < limit:
                break
            # NOTE: For real cursor-based pagination we'd parse 'Link' header; omitted for brevity.
        return products

    def list_orders(self, status: Optional[str] = None, limit: int = 50, max_pages: int = 1, use_cache: bool = True, cache_ttl: int = 0) -> List[Dict[str, Any]]:
        orders: List[Dict[str, Any]] = []
        page_info: Optional[str] = None
        pages_fetched = 0
        while pages_fetched < max_pages:
            key_parts = ['orders', status or 'any', str(limit), page_info or 'first']
            if use_cache:
                cached = load_cache('shopify', key_parts, ttl_seconds=cache_ttl)
                if cached is not None:
                    orders.extend(cached)
                    pages_fetched += 1
                    if len(cached) < limit:
                        break
                    page_info = cached[-1].get('_page_info_next')
                    continue
            params = {'limit': limit}
            if status:
                params['status'] = status
            if page_info:
                params['page_info'] = page_info
            data = self._request('GET', 'orders.json', params=params, headers=self._headers())
            items = data.get('orders', []) if isinstance(data, dict) else []
            save_cache('shopify', key_parts, items)
            orders.extend(items)
            pages_fetched += 1
            if len(items) < limit:
                break
        return orders
