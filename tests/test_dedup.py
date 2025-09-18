import pandas as pd
from pipelines.normalization import merge_products


def _sample_products(version: int):
    return [
        {
            'source': 'shopify',
            'source_id': 'A',
            'title': f'Prod A v{version}',
            'price_amount': 10.0,
            'price_currency': 'USD',
            'image_url': None,
            'category': 'cat',
            'url': None,
            'collected_at': f'2024-01-01T00:00:0{version}Z',
            'raw_hash': f'hashA_{version}',
            'raw_file': f'f{version}.json',
            'additional': '{}'
        },
        {
            'source': 'shopify',
            'source_id': 'B',
            'title': f'Prod B v{version}',
            'price_amount': 12.0,
            'price_currency': 'USD',
            'image_url': None,
            'category': 'cat',
            'url': None,
            'collected_at': f'2024-01-01T00:00:1{version}Z',
            'raw_hash': f'hashB_{version}',
            'raw_file': f'f{version}.json',
            'additional': '{}'
        }
    ]


def test_triple_mode_versions_accumulate():
    # First batch
    df1, new1, upd1 = merge_products(None, _sample_products(1), key_mode='triple')
    assert new1 == 2 and upd1 == 0
    # Second batch with changed hashes (simulate updates)
    df2, new2, upd2 = merge_products(df1, _sample_products(2), key_mode='triple')
    # In triple mode both new versions count as new
    assert new2 == 2 and upd2 == 0
    assert len(df2) == 4


def test_pair_mode_overwrites():
    df1, new1, upd1 = merge_products(None, _sample_products(1), key_mode='pair')
    assert new1 == 2 and upd1 == 0 and len(df1) == 2
    df2, new2, upd2 = merge_products(df1, _sample_products(2), key_mode='pair')
    # Should overwrite both keys A & B -> 0 truly new, 2 updated
    assert new2 == 0 and upd2 == 2
    assert len(df2) == 2
    # Titles should reflect version 2
    titles = set(df2['title'].tolist())
    assert titles == {'Prod A v2','Prod B v2'}
