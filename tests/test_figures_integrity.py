import json
import hashlib
from pathlib import Path
import pytest

REPORT_JSON = Path('reports/comparative_report.json')

@pytest.mark.dependency()
def test_report_exists_for_figures():
    assert REPORT_JSON.exists(), 'comparative_report.json missing – generate report first.'

@pytest.mark.dependency(depends=['test_report_exists_for_figures'])
def test_figure_files_exist_and_hash():
    data = json.loads(REPORT_JSON.read_text(encoding='utf-8'))
    figs = data.get('figures', [])
    for f in figs:
        path = Path('reports') / f['path']
        assert path.exists(), f'Figure file not found: {path}'
        blob = path.read_bytes()
        # If hash present, verify
        if 'sha256' in f:
            calc = hashlib.sha256(blob).hexdigest()
            assert calc == f['sha256'], f'SHA256 mismatch for {f["name"]}'
        if 'size_bytes' in f:
            assert len(blob) == f['size_bytes'], f'Size mismatch for {f["name"]}'

