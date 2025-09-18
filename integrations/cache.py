from __future__ import annotations
import json
import hashlib
import time
from pathlib import Path
from typing import Any, Optional

CACHE_ROOT = Path('.cache/api')


def _hash_key(parts: list[str]) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode('utf-8'))
    return h.hexdigest()


def cache_path(provider: str, key_parts: list[str]) -> Path:
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    prov_dir = CACHE_ROOT / provider
    prov_dir.mkdir(exist_ok=True)
    return prov_dir / f"{_hash_key(key_parts)}.json"


def load_cache(provider: str, key_parts: list[str], ttl_seconds: int) -> Optional[Any]:
    p = cache_path(provider, key_parts)
    if not p.exists():
        return None
    if ttl_seconds > 0:
        age = time.time() - p.stat().st_mtime
        if age > ttl_seconds:
            return None
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return None


def save_cache(provider: str, key_parts: list[str], data: Any) -> None:
    p = cache_path(provider, key_parts)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
