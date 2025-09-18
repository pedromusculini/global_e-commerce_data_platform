from __future__ import annotations
import os
from typing import Any, Dict, List
from .base_client import BaseClient
from .exceptions import ApiRequestError
from .cache import load_cache, save_cache

class EbayClient(BaseClient):
    RATE_LIMIT_RPS_ENV = 'EBAY_RPS'

    def __init__(self, oauth_token: str, marketplace: str = 'EBAY_US', timeout: int = 30):
        super().__init__(timeout=timeout)
        self.oauth_token = oauth_token
        self.marketplace = marketplace
        self.BASE_URL = 'https://api.ebay.com/buy/browse/v1'

    @classmethod
    def from_env(cls) -> 'EbayClient':
        token = BaseClient.env('EBAY_OAUTH_TOKEN')
        marketplace = os.getenv('EBAY_MARKETPLACE_ID', 'EBAY_US')
        return cls(token, marketplace=marketplace)  # type: ignore[arg-type]

    def _headers(self) -> Dict[str, str]:
        return {
            'Authorization': f"Bearer {self.oauth_token}",
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-EBAY-C-MARKETPLACE-ID': self.marketplace,
        }

    def search_items(self, query: str, limit: int = 10, use_cache: bool = True, cache_ttl: int = 0) -> List[Dict[str, Any]]:
        if not query:
            raise ValueError('query required')
        key = ['search', query, str(limit)]
        if use_cache:
            cached = load_cache('ebay', key, ttl_seconds=cache_ttl)
            if cached is not None:
                return cached
        params = {
            'q': query,
            'limit': str(limit)
        }
        data = self._request('GET', 'item_summary/search', params=params, headers=self._headers())
        items = data.get('itemSummaries', []) if isinstance(data, dict) else []
        save_cache('ebay', key, items)
        return items
