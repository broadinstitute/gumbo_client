from fastapi.testclient import TestClient
import rest_api
from pytest import fixture
import sqlite3

@fixture
def in_memory_db(monkeypatch):
    db_connection = sqlite3.connect(":memory:")
    monkeypatch.patch(rest_api, "db_connection", lambda: db_connection)

@fixture
def client():
    return TestClient(rest_api.app)

def test_get_table(client, in_memory_db):
    in_memory_db.execute("CREATE TABLE sample (column1 varchar(100), column2 varchar(100))")

    response = client.get(
        "/table/sample",
    )
    assert response.status_code == 200
    assert response.json() == {
        "id": "foobar",
        "title": "Foo Bar",
        "description": "The Foo Barters",
    }

def test_post_table():
    response = client.post(
        "/table/sample",
    )
    assert response.status_code == 200
    assert response.json() == {
        "id": "foobar",
        "title": "Foo Bar",
        "description": "The Foo Barters",
    }
