import json
from pathlib import Path
import pytest

REPORT_JSON = Path('reports/comparative_report.json')

REQUIRED_TOP_LEVEL = [
    'schema_version', 'generated_at_utc', 'paths', 'figures', 'products', 'orders',
    'enriched', 'data_availability', 'narrative'
]

def test_report_json_exists():
    assert REPORT_JSON.exists(), 'comparative_report.json does not exist – run generate_comparative_report.py first.'

@pytest.mark.dependency()
def test_report_loadable():
    data = json.loads(REPORT_JSON.read_text(encoding='utf-8'))
    assert isinstance(data, dict)

@pytest.mark.dependency(depends=['test_report_loadable'])
@pytest.mark.parametrize('key', REQUIRED_TOP_LEVEL)
def test_required_top_level_keys(key):
    data = json.loads(REPORT_JSON.read_text(encoding='utf-8'))
    assert key in data, f'Missing required top-level key: {key}'

@pytest.mark.dependency(depends=['test_report_loadable'])
def test_schema_version_format():
    data = json.loads(REPORT_JSON.read_text(encoding='utf-8'))
    sv = data.get('schema_version','')
    parts = sv.split('.')
    assert len(parts) == 3 and all(p.isdigit() for p in parts), 'schema_version should follow semantic versioning (X.Y.Z)'

def test_has_timestamp_field():
    data = json.loads(REPORT_JSON.read_text(encoding='utf-8'))
    assert ('generated_at_utc' in data) or ('generated_at' in data), 'Missing generation timestamp field'

@pytest.mark.dependency(depends=['test_report_loadable'])
def test_figures_manifest_integrity_basic():
    data = json.loads(REPORT_JSON.read_text(encoding='utf-8'))
    figs = data.get('figures', [])
    assert isinstance(figs, list)
    for f in figs:
        assert 'name' in f and 'path' in f, 'Each figure entry must contain name and path'

