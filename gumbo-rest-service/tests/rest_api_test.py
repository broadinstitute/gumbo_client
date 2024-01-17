from fastapi.testclient import TestClient
import gumbo_rest_service.main
from pytest import fixture
from unittest.mock import create_autospec, MagicMock
import pandas as pd
import json

@fixture
def mock_dao(monkeypatch):
    mock_connection = MagicMock()
    monkeypatch.setattr(gumbo_rest_service.main, "_get_db_connection", lambda: mock_connection)

    mock_dao = create_autospec(gumbo_rest_service.main.GumboDAO)
    monkeypatch.setattr(gumbo_rest_service.main, "_get_gumbo_dao", lambda connection: mock_dao)

    return mock_dao

@fixture
def client(mock_dao):
    return TestClient(gumbo_rest_service.main.app)

def test_get_table(mock_dao, client):
    def _mock_get(tablename):
        if tablename == "sample":
            return pd.DataFrame({"PK": ["X", "Y"], "COL2": [1,2]})
    mock_dao.get = _mock_get

    response = client.get(
        "/table/sample",
    )
    assert response.status_code == 200
    assert json.loads(response.json()) == {"PK":{"0":"X","1":"Y"},"COL2":{"0":1,"1":2}}

# def test_status_summaries(mock_dao, client):
#     def _mock_get_model_condition_status_summaries(peddep_only=False):
#         raise Exception("Not implemented...")
#     mock_dao.get_model_condition_status_summaries = _mock_get_model_condition_status_summaries

#     response = client.get(
#         "/status-summaries",
#     )
#     assert response.status_code == 200
#     assert response.json() == {
#         "id": "foobar",
#         "title": "Foo Bar",
#         "description": "The Foo Barters",
#     }
