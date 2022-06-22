import pandas as pd
from gumbo_client.client import _reconcile, _update_table
import gumbo_client
from unittest.mock import MagicMock
import gumbo_client.client
import json
import os
import pytest


def test_reconcile_all_types():
    existing = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    target = pd.DataFrame(
        [
            {"a": 1, "b": 4},  # update this row
            {"a": 4, "b": 5}  # add this row
            # delete row where a==3
        ]
    )
    new_rows, updated_rows, to_delete = _reconcile("a", existing, target)
    assert str(new_rows) == str(pd.DataFrame([{"a": 4, "b": 5}]))
    assert str(updated_rows) == str(pd.DataFrame([{"a": 1, "b": 4}]))
    assert list(to_delete) == [3]


def test_reconcile_subset_of_columns():
    existing = pd.DataFrame([{"a": 1, "b": 2, "c": 3}, {"a": 3, "b": 4, "c": 9}])
    target = pd.DataFrame(
        [
            {"a": 1, "b": 4},  # update this row
            {"a": 4, "b": 5}  # add this row
            # delete row where a==3
        ]
    )
    new_rows, updated_rows, to_delete = _reconcile("a", existing, target)
    assert str(new_rows) == str(pd.DataFrame([{"a": 4, "b": 5}]))
    assert str(updated_rows) == str(pd.DataFrame([{"a": 1, "b": 4}]))
    assert list(to_delete) == [3]


# def test_reconcile_updates_only():
#     existing = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
#     target = pd.DataFrame([{"a": 1, "b": 4}, {"a": 3, "b": 4}])  # update this row
#     new_rows, updated_rows, to_delete = _reconcile("a", existing, target)
#     assert len(new_rows) == 0
#     assert str(updated_rows) == str(pd.DataFrame([{"a": 1, "b": 4}]))
#     assert len(to_delete) == 0


def test_reconcile_inserts_only():
    existing = pd.DataFrame([{"a": 1, "b": 2}])
    target = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])  # insert this row
    new_rows, updated_rows, to_delete = _reconcile("a", existing, target)
    assert str(new_rows) == str(pd.DataFrame([{"a": 3, "b": 4}]))
    assert len(updated_rows) == 0
    assert len(to_delete) == 0


def test_reconcile_updates_only():
    existing = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    target = pd.DataFrame([{"a": 1, "b": 4}, {"a": 3, "b": 4}])  # update this row
    new_rows, updated_rows, to_delete = _reconcile("a", existing, target)
    assert len(new_rows) == 0
    assert str(updated_rows) == str(pd.DataFrame([{"a": 1, "b": 4}]))
    assert len(to_delete) == 0


def test_reconcile_deletes_only():
    existing = pd.DataFrame([{"a": 1, "b": 2}, {"a": 2, "b": 3}])
    target = pd.DataFrame(
        [
            {"a": 1, "b": 2},
        ]
    )
    new_rows, updated_rows, to_delete = _reconcile("a", existing, target)
    assert len(new_rows) == 0
    assert len(updated_rows) == 0
    assert list(to_delete) == [2]


def test_update_table(monkeypatch):
    execute_batch = MagicMock()
    cursor = MagicMock()

    monkeypatch.setattr(gumbo_client.client, "execute_batch", execute_batch)

    def _execute_batch(cur, sql, params):
        assert cur == cursor
        assert sql == "UPDATE tab SET a = %s, b = %s WHERE pk = %s"
        assert params == [[4, 5, 1]]

    execute_batch.side_effect = _execute_batch
    _update_table(cursor, "tab", "pk", pd.DataFrame([{"a": 4, "b": 5, "pk": 1}]))
    assert execute_batch.call_count == 1


@pytest.mark.skipif(
    os.environ.get("POSTGRES_TEST_DB") is None,
    reason="Needs name of local test database",
)
def test_against_local_postgres(tmpdir):
    config_path = tmpdir.join("config.json")
    config_path.write(
        json.dumps(
            {
                "host": "localhost",
                "database": os.environ["POSTGRES_TEST_DB"],
                "user": "postgres",
            }
        )
    )
    c = gumbo_client.Client(config_dir=str(tmpdir))

    cur = c.connection.cursor()
    cur.execute(
        "create table test_sample_table (id integer primary key, str_col varchar(100), float_col float)"
    )
    cur.execute(
        "insert into test_sample_table (id, str_col, float_col) values (1, 'a', 1.0), (2, 'b', 2.0)"
    )
    cur.close()

    def check_table(expected):
        df = c.get("test_sample_table")
        rows = df.to_dict("records")
        rows = sorted(rows, key=lambda row: row["id"])
        assert rows == expected

    check_table(
        [
            {"id": 1, "str_col": "a", "float_col": 1.0},
            {"id": 2, "str_col": "b", "float_col": 2.0},
        ]
    )

    final_df = pd.DataFrame(
        [
            {"id": 1, "str_col": "a2", "float_col": 11.0},
            {"id": 2, "str_col": "b2", "float_col": 12.0},
        ]
    )
    c.update("test_sample_table", final_df)

    check_table(
        [
            {"id": 1, "str_col": "a2", "float_col": 11.0},
            {"id": 2, "str_col": "b2", "float_col": 12.0},
        ]
    )
