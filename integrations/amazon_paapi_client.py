from __future__ import annotations
import os
import datetime
import hashlib
import hmac
from typing import Any, Dict, List
from .base_client import BaseClient
from .exceptions import ApiRequestError

# Simplified Amazon PA-API (GetItems) signer (Signature Version 4 for GET)
# Reference: https://webservices.amazon.com/paapi5/documentation/ (conceptual; trimmed here)

class AmazonPAAPIClient(BaseClient):
    RATE_LIMIT_RPS_ENV = 'AMAZON_PAAPI_RPS'

    def __init__(self, access_key: str, secret_key: str, partner_tag: str, host: str, region: str, timeout: int = 30):
        super().__init__(timeout=timeout)
        self.access_key = access_key
        self.secret_key = secret_key
        self.partner_tag = partner_tag
        self.host = host
        self.region = region
        self.service = 'ProductAdvertisingAPI'
        self.BASE_URL = f"https://{host}/paapi5"

    @classmethod
    def from_env(cls) -> 'AmazonPAAPIClient':
        access_key = BaseClient.env('AMAZON_PAAPI_ACCESS_KEY')
        secret_key = BaseClient.env('AMAZON_PAAPI_SECRET_KEY')
        partner_tag = BaseClient.env('AMAZON_PAAPI_PARTNER_TAG')
        host = BaseClient.env('AMAZON_PAAPI_HOST')
        region = BaseClient.env('AMAZON_PAAPI_REGION')
        return cls(access_key, secret_key, partner_tag, host, region)  # type: ignore[arg-type]

    def _sign(self, payload: str, amz_target: str) -> Dict[str, str]:
        # Signature V4
        t = datetime.datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        datestamp = t.strftime('%Y%m%d')
        canonical_uri = '/paapi5/getitems'
        canonical_querystring = ''
        canonical_headers = f"host:{self.host}\n" + f"x-amz-date:{amz_date}\n" + f"x-amz-target:{amz_target}\n"
        signed_headers = 'host;x-amz-date;x-amz-target'
        payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()
        canonical_request = '\n'.join([
            'POST',
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash
        ])
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f"{datestamp}/{self.region}/{self.service}/aws4_request"
        string_to_sign = '\n'.join([
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        ])
        def _sign_key(key, msg):
            return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()
        k_date = _sign_key(('AWS4' + self.secret_key).encode('utf-8'), datestamp)
        k_region = hmac.new(k_date, self.region.encode('utf-8'), hashlib.sha256).digest()
        k_service = hmac.new(k_region, self.service.encode('utf-8'), hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b'aws4_request', hashlib.sha256).digest()
        signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        authorization_header = (
            f"{algorithm} Credential={self.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        )
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Host': self.host,
            'X-Amz-Date': amz_date,
            'X-Amz-Target': amz_target,
            'Authorization': authorization_header,
        }
        return headers

    def get_items(self, asins: List[str], resources: List[str] | None = None) -> Dict[str, Any]:
        if not asins:
            raise ValueError('asins list cannot be empty')
        if len(asins) > 10:
            raise ValueError('Amazon PA-API GetItems max 10 ASINs per call')
        payload_obj = {
            'ItemIds': asins,
            'PartnerTag': self.partner_tag,
            'PartnerType': 'Associates',
            'Resources': resources or [
                'Images.Primary.Small',
                'ItemInfo.Title',
                'Offers.Listings.Price'
            ]
        }
        import json as _json
        payload = _json.dumps(payload_obj, separators=(',', ':'))
        amz_target = 'com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems'
        headers = self._sign(payload, amz_target)
        # Use base _request but with direct URL and POST body
        try:
            # We bypass _request JSON merging because we have strict headers
            import requests
            self._respect_rate_limit()
            resp = requests.post(self.BASE_URL + '/getitems', data=payload, headers=headers, timeout=self.timeout)
            if resp.status_code == 401 or resp.status_code == 403:
                raise ApiRequestError(f'Auth error {resp.status_code}: {resp.text[:200]}')
            if resp.status_code == 429:
                raise ApiRequestError('Rate limited (429)')
            if resp.status_code >= 400:
                raise ApiRequestError(f'HTTP {resp.status_code}: {resp.text[:200]}')
            return resp.json()
        except Exception as e:
            raise ApiRequestError(f'Amazon GetItems failed: {e}') from e
