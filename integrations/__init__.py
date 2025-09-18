"""Integration clients for external e-commerce APIs (Shopify, Amazon PA-API, eBay).

Usage example:
    from integrations.shopify_client import ShopifyClient
    client = ShopifyClient.from_env()
    products = client.list_products(limit=100)
"""
from .exceptions import ApiRequestError, ApiAuthError, ApiRateLimitError  # noqa: F401
