import json
from pathlib import Path
import pytest

SCHEMA_PATH = Path('schemas/comparative_report.schema.json')
REPORT_JSON = Path('reports/comparative_report.json')

@pytest.mark.dependency()
def test_schema_file_exists():
    assert SCHEMA_PATH.exists(), 'Schema file missing. Expected at schemas/comparative_report.schema.json'

@pytest.mark.dependency(depends=['test_schema_file_exists'])
def test_report_exists_for_schema_validation():
    assert REPORT_JSON.exists(), 'Report JSON missing. Generate it before running schema validation tests.'

@pytest.mark.dependency(depends=['test_schema_file_exists','test_report_exists_for_schema_validation'])
def test_report_validates_against_schema():
    try:
        import jsonschema  # type: ignore
    except ImportError:
        pytest.skip('jsonschema not installed in environment')
    schema = json.loads(SCHEMA_PATH.read_text(encoding='utf-8'))
    data = json.loads(REPORT_JSON.read_text(encoding='utf-8'))
    # jsonschema.validate will raise ValidationError if invalid
    jsonschema.validate(instance=data, schema=schema)
