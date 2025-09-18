from __future__ import annotations
import random
import hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

_RANDOM = random.Random()


def seed_mock(seed: Optional[int] = None) -> None:
    if seed is not None:
        _RANDOM.seed(seed)


def _now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace('+00:00','Z')

CATEGORIES = ["Electronics","Books","Home","Toys","Sports","Fashion"]
ADJECTIVES = ["Smart","Eco","Ultra","Mini","Pro","Air","Max","Hyper","Nano","Prime"]
NOUNS = ["Speaker","Lamp","Bottle","Backpack","Watch","Camera","Helmet","Router","Shirt","Drone"]
CURRENCIES = ["USD","EUR","GBP","BRL"]


def generate_mock_products(n: int = 20, price_min: float = 5.0, price_max: float = 300.0) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for i in range(n):
        adj = _RANDOM.choice(ADJECTIVES)
        noun = _RANDOM.choice(NOUNS)
        title = f"{adj} {noun}"
        pid = f"mock-prod-{i+1}"
        price = round(_RANDOM.uniform(price_min, price_max), 2)
        currency = _RANDOM.choice(CURRENCIES)
        category = _RANDOM.choice(CATEGORIES)
    # Simulate variations: each product may have 0-2 variants with slightly adjusted prices
        variants = []
        for v in range(_RANDOM.randint(0,2)):
            factor = 1 + _RANDOM.uniform(-0.1, 0.15)
            variants.append({
                'id': f"{pid}-v{v+1}",
                'price': str(round(price * factor, 2)),
                'currency': currency,
            })
        images = [{ 'src': f"https://example.com/img/{pid}.png" }] if _RANDOM.random() < 0.8 else []
        items.append({
            'id': pid,
            'title': title,
            'variants': variants or [{ 'id': pid+'-v1', 'price': str(price), 'currency': currency }],
            'images': images,
            'product_type': category,
            'handle': pid,
            'vendor': 'MockVendor',
            'tags': ','.join(_RANDOM.sample(CATEGORIES, k=_RANDOM.randint(0,2))) if _RANDOM.random()<0.5 else ''
        })
    return items


def _hash_email(email: str) -> str:
    return hashlib.sha256(email.lower().encode('utf-8')).hexdigest()


def generate_mock_orders(m: int = 10, product_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    if product_ids is None:
        product_ids = [f"mock-prod-{i+1}" for i in range(30)]
    orders: List[Dict[str, Any]] = []
    now = datetime.utcnow()
    for i in range(m):
        oid = f"mock-order-{i+1}"
        created = now - timedelta(hours=_RANDOM.randint(1, 240))
        currency = _RANDOM.choice(CURRENCIES)
        line_items = []
        total_price = 0.0
        for li in range(_RANDOM.randint(1,4)):
            pid = _RANDOM.choice(product_ids)
            qty = _RANDOM.randint(1,3)
            unit_price = round(_RANDOM.uniform(5.0, 250.0), 2)
            line_items.append({
                'id': f"{oid}-li{li+1}",
                'sku': pid,
                'quantity': qty,
                'price': str(unit_price),
                'variant_id': pid+"-v1"
            })
            total_price += unit_price * qty
        subtotal_price = total_price
        tax = round(total_price * _RANDOM.uniform(0.0, 0.18), 2)
        shipping_price = round(_RANDOM.uniform(0, 25), 2) if _RANDOM.random()<0.6 else 0.0
        total_price += tax + shipping_price
        email = f"user{i+1}@example.com"
        orders.append({
            'id': oid,
            'order_number': i+1,
            'created_at': created.replace(tzinfo=timezone.utc).isoformat().replace('+00:00','Z'),
            'closed_at': None,
            'currency': currency,
            'total_price': str(round(total_price,2)),
            'subtotal_price': str(round(subtotal_price,2)),
            'total_tax': str(tax),
            'shipping_lines': [{'price': str(shipping_price)}] if shipping_price>0 else [],
            'discount_applications': [],
            'financial_status': _RANDOM.choice(['paid','pending','refunded']),
            'fulfillment_status': _RANDOM.choice(['fulfilled','partial','null']) if _RANDOM.random()<0.7 else None,
            'line_items': line_items,
            'customer': { 'id': f"cust-{i+1}", 'email': email },
            'gateway': 'mock_gateway',
            'processing_method': 'mock',
            'cancelled_at': None,
            'tags': 'mock'
        })
    return orders


def generate_fake_amazon_items(n: int = 10) -> Dict[str, Any]:
    items = []
    for i in range(n):
        asin = f"FAKEASIN{i+1:03d}"
        title = f"{_RANDOM.choice(ADJECTIVES)} {_RANDOM.choice(NOUNS)}"
        amount = round(_RANDOM.uniform(5.0, 400.0), 2)
        currency = _RANDOM.choice(CURRENCIES)
        items.append({
            'ASIN': asin,
            'ItemInfo': { 'Title': { 'DisplayValue': title } },
            'Offers': { 'Listings': [ { 'Price': { 'Amount': amount, 'Currency': currency } } ] },
            'Images': { 'Primary': { 'Small': { 'URL': f"https://example.com/img/{asin}.png" } } },
            'BrowseNodeInfo': { 'BrowseNodes': [] },
            'DetailPageURL': f"https://example.com/dp/{asin}"
        })
    return {'ItemsResult': { 'Items': items }}


def generate_fake_ebay_items(n: int = 10, query: str = 'mock search') -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(n):
        item_id = f"FAKEEBAY{i+1:03d}"
        price = round(_RANDOM.uniform(3.0, 350.0), 2)
        currency = _RANDOM.choice(CURRENCIES)
        out.append({
            'itemId': item_id,
            'title': f"{_RANDOM.choice(ADJECTIVES)} {_RANDOM.choice(NOUNS)} for {query}",
            'price': { 'value': price, 'currency': currency },
            'image': { 'imageUrl': f"https://example.com/img/{item_id}.jpg" },
            'categoryPath': ['Root', _RANDOM.choice(CATEGORIES)],
            'itemWebUrl': f"https://example.com/itm/{item_id}",
            'seller': { 'username': 'mock_seller' },
            'condition': 'NEW'
        })
    return out
