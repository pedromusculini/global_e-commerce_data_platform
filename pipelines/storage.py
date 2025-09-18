from __future__ import annotations
import json
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone

try:  # attempt to import pandas; fallback if unavailable
    import pandas as pd  # type: ignore
except ImportError:  # fallback minimal
    pd = None  # type: ignore

PRODUCTS_FILE_PARQUET = Path('data/normalized/products.parquet')
PRODUCTS_FILE_CSV = Path('data/normalized/products.csv')
ORDERS_FILE_PARQUET = Path('data/normalized/orders.parquet')
ORDERS_FILE_CSV = Path('data/normalized/orders.csv')
RUNS_LOG = Path('metadata/pipeline_runs.jsonl')


def utc_now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')


def sha256_json(data: Any) -> str:
    h = hashlib.sha256()
    h.update(json.dumps(data, sort_keys=True, ensure_ascii=False).encode('utf-8'))
    return h.hexdigest()


def save_raw(provider: str, resource: str, payload: Any, run_id: str, tag: Optional[str] = None) -> Path:
    ts = utc_now_iso().replace(':', '-').replace('.', '-')
    safe_tag = ''
    if tag:
        safe_tag = '_' + tag.replace(' ', '-').replace('/', '-').lower()
    out_dir = Path('data/raw') / provider / resource
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{ts}_{run_id}{safe_tag}.json"
    fpath = out_dir / fname
    fpath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return fpath


def load_existing_products() -> Optional[Any]:
    if pd is None:
        return None
    if PRODUCTS_FILE_PARQUET.exists():
        try:
            return pd.read_parquet(PRODUCTS_FILE_PARQUET)
        except Exception:
            pass
    if PRODUCTS_FILE_CSV.exists():
        try:
            return pd.read_csv(PRODUCTS_FILE_CSV, dtype=str)
        except Exception:
            pass
    return None


def persist_products(df_new: Any, maintain_history: bool = False) -> None:
    if pd is None:
        # fallback: write CSV only
        PRODUCTS_FILE_CSV.parent.mkdir(parents=True, exist_ok=True)
        if PRODUCTS_FILE_CSV.exists():
            # append
            existing = pd.read_csv(PRODUCTS_FILE_CSV, dtype=str)  # type: ignore
            combined = pd.concat([existing, df_new], ignore_index=True)
            combined.to_csv(PRODUCTS_FILE_CSV, index=False)
        else:
            df_new.to_csv(PRODUCTS_FILE_CSV, index=False)
        return
    PRODUCTS_FILE_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df_new.to_parquet(PRODUCTS_FILE_PARQUET, index=False)


def load_existing_orders() -> Optional[Any]:
    if pd is None:
        return None
    if ORDERS_FILE_PARQUET.exists():
        try:
            return pd.read_parquet(ORDERS_FILE_PARQUET)
        except Exception:
            pass
    if ORDERS_FILE_CSV.exists():
        try:
            return pd.read_csv(ORDERS_FILE_CSV, dtype=str)
        except Exception:
            pass
    return None


def persist_orders(df_new: Any) -> None:
    if pd is None:
        ORDERS_FILE_CSV.parent.mkdir(parents=True, exist_ok=True)
        if ORDERS_FILE_CSV.exists():
            existing = pd.read_csv(ORDERS_FILE_CSV, dtype=str)  # type: ignore
            combined = pd.concat([existing, df_new], ignore_index=True)
            combined.to_csv(ORDERS_FILE_CSV, index=False)
        else:
            df_new.to_csv(ORDERS_FILE_CSV, index=False)
        return
    ORDERS_FILE_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df_new.to_parquet(ORDERS_FILE_PARQUET, index=False)


def append_run_log(record: Dict[str, Any]) -> None:
    RUNS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with RUNS_LOG.open('a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')
