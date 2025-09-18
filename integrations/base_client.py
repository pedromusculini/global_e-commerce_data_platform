from __future__ import annotations
import os
import time
import json
import logging
from typing import Any, Dict, Optional
import requests
from .exceptions import ApiAuthError, ApiRateLimitError, ApiRequestError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

class BaseClient:
    """Base HTTP client with retry, basic rate limiting and JSON handling."""
    BASE_URL: str = ''
    RATE_LIMIT_RPS_ENV: Optional[str] = None  # environment variable name e.g. SHOPIFY_RPS

    def __init__(self, timeout: int = 30):
        self.session = requests.Session()
        self.timeout = timeout
        self._last_request_ts: float = 0.0

    def _respect_rate_limit(self):
        if not self.RATE_LIMIT_RPS_ENV:
            return
        rps_value = os.getenv(self.RATE_LIMIT_RPS_ENV)
        if not rps_value:
            return
        try:
            rps = float(rps_value)
            if rps <= 0:
                return
        except ValueError:
            return
        min_interval = 1.0 / rps
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_ts = time.time()

    def _request(self, method: str, path: str, *, params: Dict[str, Any] | None = None, headers: Dict[str, str] | None = None, json_body: Any | None = None, retries: int = 2) -> Any:
        url = path if path.startswith('http') else self.BASE_URL.rstrip('/') + '/' + path.lstrip('/')
        attempt = 0
        while True:
            self._respect_rate_limit()
            try:
                resp = self.session.request(method.upper(), url, params=params, headers=headers, json=json_body, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt < retries:
                    attempt += 1
                    time.sleep(2 ** attempt)
                    continue
                raise ApiRequestError(f"Network error: {e}") from e

            if resp.status_code == 401 or resp.status_code == 403:
                raise ApiAuthError(f"Auth error {resp.status_code}: {resp.text[:200]}")
            if resp.status_code == 429:
                if attempt < retries:
                    attempt += 1
                    wait = int(resp.headers.get('Retry-After', '1'))
                    time.sleep(wait or 1)
                    continue
                raise ApiRateLimitError(f"Rate limit hit (429): {resp.text[:200]}")
            if resp.status_code >= 500:
                if attempt < retries:
                    attempt += 1
                    time.sleep(2 ** attempt)
                    continue
                raise ApiRequestError(f"Server error {resp.status_code}: {resp.text[:200]}")
            if resp.status_code >= 400:
                raise ApiRequestError(f"Client error {resp.status_code}: {resp.text[:200]}")

            ctype = resp.headers.get('Content-Type', '')
            if 'application/json' in ctype:
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    raise ApiRequestError('Failed to decode JSON response')
            return resp.text

    @staticmethod
    def env(name: str, required: bool = True) -> Optional[str]:
        val = os.getenv(name)
        if required and (val is None or val.strip() == ''):
            raise ApiAuthError(f"Missing required environment variable: {name}")
        return val
