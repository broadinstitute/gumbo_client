import gumbo_dao
import gumbo_dao.gumbo_dao
import sqlite3
import pytest
import pandas as pd


@pytest.fixture
def connection(monkeypatch):
    # a connection to a test database with a single table named "SAMPLE"
    # also construct bulk_update_log table that's used on updates

    connection = sqlite3.connect(":memory:")

    connection.execute("CREATE TABLE SAMPLE(PK VARCHAR(100), COLUMN2 INTEGER)")
    connection.execute(
        'CREATE TABLE bulk_update_log (username varchar(100), "timestamp" TIMESTAMP, tablename varchar(100), rows_updated integer, rows_deleted integer, rows_inserted integer, reason varchar(1000))'
    )

    return connection


@pytest.fixture
def dao(monkeypatch, connection):
    # a fixture which mocks out methods which cannot execute with sqlite
    # _set_username (because no sqlite equivlient)
    # and replaces execute_batch and execute_values with a sqlite equivilent

    dao = gumbo_dao.GumboDAO(connection=connection, sanity_check=True)
    monkeypatch.setattr(dao, "_set_username", lambda name: None)

    # simulate execute_batch and execute_values since these are postgresql specific
    def execute_batch(cursor, sql, params):
        for param in params:
            cursor.execute(sql.replace("%s", "?"), param)

    def execute_values(cursor, sql, values):
        for param in values:
            cursor.execute(
                sql.replace("%s", "(" + (",".join(["?"] * len(param))) + ")"), param
            )

    monkeypatch.setattr(gumbo_dao.gumbo_dao, "execute_batch", execute_batch)
    monkeypatch.setattr(gumbo_dao.gumbo_dao, "execute_values", execute_values)

    def mock_get_pk_column(cursor, table_name):
        assert table_name == "sample"
        return "PK"

    monkeypatch.setattr(gumbo_dao.gumbo_dao, "_get_pk_column", mock_get_pk_column)

    return dao


def test_get(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.commit()

    df = dao.get("sample")
    assert df.shape[0] == 1
    assert df.shape[1] == 2


def test_update_no_delete(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('Y', 2)")
    connection.commit()

    new_df = pd.DataFrame({"PK": ["X"], "COLUMN2": [3]})
    dao.update(
        "username",
        "sample",
        new_df,
        delete_missing_rows=False,
        reason="test_update_no_delete",
    )
    df = dao.get("sample")
    expected_df = pd.DataFrame({"PK": ["X", "Y"], "COLUMN2": [3, 2]})
    assert expected_df.equals(df)


def test_update_with_delete(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('Y', 2)")
    connection.commit()

    new_df = pd.DataFrame({"PK": ["X"], "COLUMN2": [3]})
    dao.update(
        "username",
        "sample",
        new_df,
        delete_missing_rows=True,
        reason="test_update_with_delete",
    )
    df = dao.get("sample")
    assert new_df.equals(df)


def test_insert_only(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.commit()

    new_df = pd.DataFrame({"PK": ["Y"], "COLUMN2": [2]})

    dao.insert_only("username", "sample", new_df, reason="test_insert_only")
    df = dao.get("sample")
    expected_df = pd.DataFrame({"PK": ["X", "Y"], "COLUMN2": [1, 2]})
    assert expected_df.equals(df)


def test_update_only(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('Y', 2)")
    connection.commit()

    new_df = pd.DataFrame({"PK": ["X"], "COLUMN2": [3]})
    dao.update_only("username", "sample", new_df, reason="test_update_only")
    df = dao.get("sample")
    expected_df = pd.DataFrame({"PK": ["X", "Y"], "COLUMN2": [3, 2]})
    assert expected_df.equals(df)
