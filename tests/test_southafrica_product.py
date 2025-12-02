from pathlib import Path

from fastapi.testclient import TestClient

from engine.main import app
from engine.odata.registry import load_config_dir


def setup_module(module):
    """
    Ensure configs are loaded when tests run.
    If your main.py already calls load_config_dir at import time,
    you may not need this, but it's explicit and safe.
    """
    config_dir = Path(__file__).resolve().parents[1] / "config" / "data-products"
    load_config_dir(config_dir)


client = TestClient(app)


def test_metadata_lists_southafrica_product():
    resp = client.get("/odata/$metadata")
    assert resp.status_code == 200
    data = resp.json()

    ids = {p["id"] for p in data}
    assert "southafrica-scheduled-outage-dataset" in ids


def test_southafrica_basic_query():
    resp = client.get("/odata/southafrica-scheduled-outage-dataset?$top=5")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) <= 5

    if data:
        row = data[0]
        # core columns should be present
        assert "id" in row
        assert "province" in row
        assert "city" in row
        assert "suburb" in row
        assert "provider" in row
        assert "block" in row
        assert "day" in row
        assert "stage" in row
        assert "start_time" in row
        assert "end_time" in row


def test_southafrica_filter_by_province():
    resp = client.get(
        "/odata/southafrica-scheduled-outage-dataset?"
        "$filter=province eq 'Eastern Cape' and city eq 'Amahlathi'&$top=5"
    )
    assert resp.status_code == 200
    data = resp.json()

    for row in data:
        assert row["province"] == "Eastern Cape"
        assert row["city"] == "Amahlathi"
