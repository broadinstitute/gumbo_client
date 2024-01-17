import gumbo_dao
import gumbo_dao.gumbo_dao
import sqlite3
import pytest
import pandas as pd

@pytest.fixture
def connection(monkeypatch):
    def mock_get_pk_column(cursor, table_name):
        assert table_name == "sample"
        return "PK"
    
    monkeypatch.setattr(gumbo_dao.gumbo_dao, "_get_pk_column", mock_get_pk_column)

    connection = sqlite3.connect(":memory:")

    connection.execute("CREATE TABLE SAMPLE(PK VARCHAR(100), COLUMN2 INTEGER)")

    return connection

@pytest.fixture
def dao(monkeypatch, connection):
    dao = gumbo_dao.GumboDAO2(sanity_check=True, connection=connection)
    monkeypatch.setattr(dao, "_set_username",lambda name: None)

    # simulate execute_batch and execute_values since these are postgresql specific
    def execute_batch(cursor, sql, params):
        for param in params:
            cursor.execute(sql.replace("%s", "?"), param)

    def execute_values(cursor, sql, values):
        for param in values:
            cursor.execute(sql.replace("%s", ",".join(["?"]*len(param))), param)

    monkeypatch.setattr(gumbo_dao.gumbo_dao, "execute_batch", execute_batch)
    monkeypatch.setattr(gumbo_dao.gumbo_dao, "execute_values", execute_values)

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

    new_df = pd.DataFrame({"PK":["X"], "COLUMN2": [3]})
    df = dao.update("username", "sample", new_df, delete_missing_rows=False, reason="test_update_no_delete")
    expected_df = pd.DataFrame({"PK": ["X", "Y"], "COLUMN2": [3,2]})
    assert df == expected_df

def test_update_with_delete(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('Y', 2)")
    connection.commit()

    new_df = pd.DataFrame({"PK":["X"], "COLUMN2": [3]})
    df = dao.update("username", "sample", new_df, delete_missing_rows=True, reason="test_update_with_delete")
    assert df == new_df

def test_insert_only(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.commit()

    new_df = pd.DataFrame({"PK":["Y"], "COLUMN2": [2]})

    dao = gumbo_dao.GumboDAO2(sanity_check=True, connection=connection)
    dao.insert_only("username", "sample", new_df, reason="test_insert_only")
    df = dao.get("sample")
    expected_df = pd.DataFrame({"PK": ["X", "Y"], "COLUMN2": [1,2]})
    assert df == expected_df

def test_update_only(connection, dao):
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('X', 1)")
    connection.execute("INSERT INTO SAMPLE (PK, COLUMN2) VALUES ('Y', 2)")
    connection.commit()

    new_df = pd.DataFrame({"PK":["X"], "COLUMN2": [3]})
    dao = gumbo_dao.GumboDAO2(sanity_check=True, connection=connection)
    dao.update_only("username", "sample", new_df, reason="test_update_only")
    df = dao.get("sample")
    expected_df = pd.DataFrame({"PK": ["X", "Y"], "COLUMN2": [3,2]})
    assert df == expected_df
