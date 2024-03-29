from pytest import fixture
from gumbo_rest_client import Client
from gumbo_rest_client.exceptions import UnknownTable
import gumbo_rest_service.main
import pandas as pd
from fastapi.testclient import TestClient
import psycopg2
import os
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("POSTGRES_TEST_DB") is None,
    reason="Needs name of local test database",
)

# class ResponseAdapter:
#     def __init__(self, response):
#         self.response = response

#     def raise_for_status(self):
#         self.response.raise_for_status()

#     def json(self):
#         return self.response.json()

# class HTTPSessionAdapter:
#     def __init__(self, http_client) -> None:
#         self.http_client = http_client

#     def request(self, method: str, url: str):
#         if method == "GET":
#             method_fn = self.http_client.get
#         elif method == "POST":
#             method_fn = self.http_client.post
#         else:
#             raise NotImplemented()

#         return ResponseAdapter(method_fn(url))


@fixture
def http_client(monkeypatch):
    monkeypatch.setenv("GUMBO_CONNECTION_STRING", os.environ["POSTGRES_TEST_DB"])
    return TestClient(gumbo_rest_service.main.app)


@fixture
def gumbo_client(http_client):
    # the client wants a authsession interface
    # but our http test client has a different interface,
    # use an adapter that has the methods we need
    # return Client(username="testuser", authed_session=HTTPSessionAdapter(http_client))
    return Client(username="testuser", authed_session=(http_client))


import datetime


@fixture
def sample_tables():
    connection = psycopg2.connect(os.environ["POSTGRES_TEST_DB"])
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS sample")
    cursor.execute("DROP TABLE IF EXISTS bulk_update_log")
    cursor.execute(
        "CREATE TABLE sample (ID VARCHAR(10), INTCOL INTEGER, STRCOL VARCHAR(100), FLOATCOL FLOAT, DATECOL DATE, BOOLCOL BOOL, PRIMARY KEY (ID))"
    )
    cursor.execute(
        "INSERT INTO sample (ID, INTCOL, STRCOL, FLOATCOL, DATECOL, BOOLCOL) VALUES (%s, %s, %s, %s, %s, %s)",
        ["id", 1, "str", 1.1, datetime.date(2000, 1, 1), True],
    )
    cursor.execute(
        'CREATE TABLE bulk_update_log (username varchar(100), "timestamp" TIMESTAMP, tablename varchar(100), rows_updated integer, rows_deleted integer, rows_inserted integer, reason varchar(1000))'
    )
    cursor.close()

    yield


def test_get_table(gumbo_client, sample_tables):
    df = gumbo_client.get("sample")
    assert list(df.columns) == [
        "id",
        "intcol",
        "strcol",
        "floatcol",
        "datecol",
        "boolcol",
    ]


def test_get_missing_table(gumbo_client):
    with pytest.raises(UnknownTable):
        gumbo_client.get("missing_table")


def test_update_only(gumbo_client, sample_tables):
    df = pd.DataFrame({"id": ["id"], "strcol": ["updated"], "intcol": [2]})
    gumbo_client.update_only("sample", df, reason="because")

    # make sure update worked
    fetched_df = gumbo_client.get("sample")
    assert list(fetched_df["id"]) == ["id"]
    assert list(fetched_df["strcol"]) == ["updated"]
    assert list(fetched_df["intcol"]) == [2]


def test_insert_only(gumbo_client, sample_tables):
    df = pd.DataFrame({"id": ["id2"], "strcol": ["inserted"], "intcol": [2]})
    gumbo_client.insert_only("sample", df, reason="because")

    # make sure update worked
    fetched_df = gumbo_client.get("sample")
    assert list(fetched_df["id"]) == ["id", "id2"]
    assert list(fetched_df["strcol"]) == ["str", "inserted"]
    assert list(fetched_df["intcol"]) == [1, 2]


# def test_against_local_postgres(tmpdir):
#     config_path = tmpdir.join("config.json")
#     config_path.write(
#         json.dumps(
#             {
#                 "host": "localhost",
#                 "database": os.environ["POSTGRES_TEST_DB"],
#                 "user": "postgres",
#             }
#         )
#     )
#     c = gumbo_client.Client(config_dir=str(tmpdir))

#     cur = c.connection.cursor()
#     cur.execute(
#         "create table test_sample_table (id integer primary key, str_col varchar(100), float_col float)"
#     )
#     cur.execute(
#         "insert into test_sample_table (id, str_col, float_col) values (1, 'a', 1.0), (2, 'b', 2.0)"
#     )
#     cur.close()

#     def check_table(expected):
#         df = c.get("test_sample_table")
#         rows = df.to_dict("records")
#         rows = sorted(rows, key=lambda row: row["id"])
#         assert rows == expected

#     check_table(
#         [
#             {"id": 1, "str_col": "a", "float_col": 1.0},
#             {"id": 2, "str_col": "b", "float_col": 2.0},
#         ]
#     )

#     final_df = pd.DataFrame(
#         [
#             {"id": 1, "str_col": "a2", "float_col": 11.0},
#             {"id": 2, "str_col": "b2", "float_col": 12.0},
#         ]
#     )
#     c.update("test_sample_table", final_df)

#     check_table(
#         [
#             {"id": 1, "str_col": "a2", "float_col": 11.0},
#             {"id": 2, "str_col": "b2", "float_col": 12.0},
#         ]
#     )
