from __future__ import annotations
import json
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone

try:
    import pandas as pd  # type: ignore
except ImportError:  # fallback
    pd = None  # type: ignore


def _now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace('+00:00','Z')


def _limit_additional(obj: Dict[str, Any], max_len: int = 8000) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    if len(raw) > max_len:
        return raw[:max_len] + '...'
    return raw


def normalize_shopify_products(raw_products: List[Dict[str, Any]], raw_file: str, raw_hash: str) -> List[Dict[str, Any]]:
    out = []
    for p in raw_products:
        title = p.get('title')
        pid = str(p.get('id'))
        price_amount = None
        price_currency = None
        try:
            variants = p.get('variants') or []
            if variants:
                first = variants[0]
                price_amount = float(first.get('price')) if first.get('price') else None
                price_currency = first.get('currency') or first.get('presentment_prices', [{}])[0].get('price', {}).get('currency_code')
        except Exception:
            pass
        image_url = None
        images = p.get('images') or []
        if images:
            image_url = images[0].get('src')
        category = p.get('product_type') or (p.get('tags', '').split(',')[0].strip() if p.get('tags') else None)
        rec = {
            'source': 'shopify',
            'source_id': pid,
            'title': title,
            'price_amount': price_amount,
            'price_currency': price_currency,
            'image_url': image_url,
            'category': category,
            'url': None,
            'collected_at': _now_iso(),
            'raw_hash': raw_hash,
            'raw_file': raw_file,
            'additional': _limit_additional({'handle': p.get('handle'), 'vendor': p.get('vendor')})
        }
        out.append(rec)
    return out


def normalize_amazon_items(response: Dict[str, Any], raw_file: str, raw_hash: str) -> List[Dict[str, Any]]:
    items = response.get('ItemsResult', {}).get('Items', [])
    out = []
    for it in items:
        asin = it.get('ASIN')
        title = it.get('ItemInfo', {}).get('Title', {}).get('DisplayValue')
        offers = it.get('Offers', {}).get('Listings', [])
        price_amount = None
        price_currency = None
        if offers:
            price = offers[0].get('Price') or {}
            price_amount = price.get('Amount')
            price_currency = price.get('Currency')
        image_url = it.get('Images', {}).get('Primary', {}).get('Small', {}).get('URL')
        category = None
        url = it.get('DetailPageURL')
        rec = {
            'source': 'amazon',
            'source_id': asin,
            'title': title,
            'price_amount': price_amount,
            'price_currency': price_currency,
            'image_url': image_url,
            'category': category,
            'url': url,
            'collected_at': _now_iso(),
            'raw_hash': raw_hash,
            'raw_file': raw_file,
            'additional': _limit_additional({'browseNodeInfo': it.get('BrowseNodeInfo')})
        }
        out.append(rec)
    return out


def normalize_ebay_search(items: List[Dict[str, Any]], raw_file: str, raw_hash: str) -> List[Dict[str, Any]]:
    out = []
    for it in items:
        item_id = it.get('itemId')
        title = it.get('title')
        price = it.get('price') or {}
        price_amount = price.get('value')
        price_currency = price.get('currency')
        image_url = (it.get('image') or {}).get('imageUrl')
        category = None
        cats = it.get('categoryPath')
        if cats:
            if isinstance(cats, list):
                category = cats[-1]
        url = it.get('itemWebUrl')
        rec = {
            'source': 'ebay',
            'source_id': item_id,
            'title': title,
            'price_amount': price_amount,
            'price_currency': price_currency,
            'image_url': image_url,
            'category': category,
            'url': url,
            'collected_at': _now_iso(),
            'raw_hash': raw_hash,
            'raw_file': raw_file,
            'additional': _limit_additional({'seller': it.get('seller'), 'condition': it.get('condition')})
        }
        out.append(rec)
    return out


def merge_products(existing, new_records, key_mode: str = 'triple') -> tuple:
    """Merge new product records into existing dataframe.

    Returns:
      (combined_df_or_records, new_count, updated_count)

    key_mode:
      - 'triple': keep multiple versions distinguished by (source, source_id, raw_hash)
      - 'pair': single latest version per (source, source_id) (older versions removed, new overwrites)
    """
    if pd is None:
        # Fallback sem pandas: considera todos novos (sem updated tracking)
        return new_records, len(new_records), 0
    import pandas as _pd
    df_new = _pd.DataFrame(new_records)
    if existing is None or existing.empty:
        if key_mode == 'pair' and not df_new.empty:
            df_new = df_new.sort_values('collected_at').drop_duplicates(['source','source_id'], keep='last')
        return df_new, len(df_new), 0

    if key_mode == 'triple':
        key_cols = ['source','source_id','raw_hash']
        existing_keys = set(tuple(r) for r in existing[key_cols].values.tolist())
        mask = [tuple(row[k] for k in key_cols) not in existing_keys for row in df_new.to_dict('records')]
        df_filtered = df_new[mask]
        combined = _pd.concat([existing, df_filtered], ignore_index=True)
        return combined, len(df_filtered), 0
    elif key_mode == 'pair':
        new_keys = set((r['source'], r['source_id']) for r in df_new.to_dict('records'))
        if new_keys:
            existing_filtered = existing[[ (row_source, row_source_id) not in new_keys for row_source, row_source_id in existing[['source','source_id']].values ]]
        else:
            existing_filtered = existing
        if 'collected_at' in df_new.columns:
            df_new = df_new.sort_values('collected_at').drop_duplicates(['source','source_id'], keep='last')
        existing_pair_keys = set((r[0], r[1]) for r in existing[['source','source_id']].values)
        new_unique_keys = set((r['source'], r['source_id']) for r in df_new.to_dict('records'))
        truly_new = len([k for k in new_unique_keys if k not in existing_pair_keys])
        updated_count = len([k for k in new_unique_keys if k in existing_pair_keys])
        combined = _pd.concat([existing_filtered, df_new], ignore_index=True)
        return combined, truly_new, updated_count
    else:
        raise ValueError(f"Unknown key_mode: {key_mode}")


def normalize_shopify_orders(raw_orders: List[Dict[str, Any]], raw_file: str, raw_hash: str) -> List[Dict[str, Any]]:
    """Normalize Shopify orders into unified order schema.

    Notes:
      - We avoid storing raw PII (email) and instead hash (lowercase) if present.
      - Discounts/shipping may require aggregation; basic derivation here.
    """
    out: List[Dict[str, Any]] = []
    for o in raw_orders:
        oid = str(o.get('id'))
        number = o.get('order_number') or o.get('name')
        created_at = o.get('created_at')
        closed_at = o.get('closed_at')
        currency = o.get('currency')
        total_price = _safe_float(o.get('total_price'))
        subtotal_price = _safe_float(o.get('subtotal_price'))
        total_tax = _safe_float(o.get('total_tax'))
        total_shipping = None
        try:
            shippings = o.get('shipping_lines') or []
            if shippings:
                total_shipping = sum(_safe_float(s.get('price')) or 0.0 for s in shippings)
        except Exception:
            pass
        # Discounts
        total_discount = None
        try:
            discounts = o.get('discount_applications') or []
            price_rules_total = 0.0
            for d in discounts:
                # Simplified: if value present and target_selection == 'all'
                val = d.get('value')
                if val is not None:
                    try:
                        price_rules_total += float(val)
                    except Exception:
                        pass
            total_discount = price_rules_total if price_rules_total > 0 else None
        except Exception:
            pass
        financial_status = o.get('financial_status')
        fulfillment_status = o.get('fulfillment_status')
        line_items = o.get('line_items') or []
        line_items_count = len(line_items)
        skus = []
        for li in line_items:
            sku = li.get('sku') or li.get('variant_id')
            if sku:
                skus.append(str(sku))
        line_items_skus = ','.join(sorted(set(skus))) if skus else None
        customer_id = None
        email_hash = None
        cust = o.get('customer') or {}
        if cust:
            if cust.get('id'):
                customer_id = str(cust.get('id'))
            email = cust.get('email')
            if email:
                email_hash = _sha256_lower(email)
        additional_obj = {
            'gateway': o.get('gateway'),
            'processing_method': o.get('processing_method'),
            'cancelled_at': o.get('cancelled_at'),
            'tags': o.get('tags'),
        }
        rec = {
            'source': 'shopify',
            'order_id': oid,
            'source_order_number': str(number) if number else None,
            'created_at': created_at,
            'closed_at': closed_at,
            'currency': currency,
            'total_price': total_price,
            'subtotal_price': subtotal_price,
            'total_tax': total_tax,
            'total_discount': total_discount,
            'total_shipping': total_shipping,
            'financial_status': financial_status,
            'fulfillment_status': fulfillment_status,
            'line_items_count': line_items_count,
            'line_items_skus': line_items_skus,
            'customer_id': customer_id,
            'customer_email_hash': email_hash,
            'raw_file': raw_file,
            'raw_hash': raw_hash,
            'ingested_at': _now_iso(),
            'additional': _limit_additional(additional_obj)
        }
        out.append(rec)
    return out


def _safe_float(val: Any) -> float | None:
    try:
        if val is None or val == '':
            return None
        return float(val)
    except Exception:
        return None


def _sha256_lower(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.strip().lower().encode('utf-8')).hexdigest()
