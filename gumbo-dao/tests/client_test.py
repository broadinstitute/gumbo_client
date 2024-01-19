import numpy as np
import pandas as pd
from gumbo_dao.gumbo_dao import _reconcile, _update_table
import gumbo_dao.gumbo_dao
from unittest.mock import MagicMock
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


def test_reconcile_null_values():
    existing = pd.DataFrame([{"a": 1, "b": "test"}, {"a": 3, "b": np.nan}])
    target = pd.DataFrame(
        [
            {"a": 1, "b": np.nan},  # update this value to None
            {"a": 4, "b": np.nan}  # add this row
            # delete row where a==3
        ]
    )
    new_rows, updated_rows, to_delete = _reconcile("a", existing, target)
    assert str(new_rows) == str(pd.DataFrame([{"a": 4, "b": None}]))
    assert str(updated_rows) == str(pd.DataFrame([{"a": 1, "b": None}]))
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


def test_update_too_many_columns():
    existing = pd.DataFrame(
        [
            {"a": 1, "b": 2},
        ]
    )
    target = pd.DataFrame(
        [
            {"a": 1, "c": 4},
        ]
    )
    with pytest.raises(
        AssertionError, match=r".*columns to update do not exist in the target.*"
    ):
        _reconcile("a", existing, target)


def test_missing_pk():
    existing = pd.DataFrame(
        [
            {"a": 1, "b": 2, "c": 3},
        ]
    )
    target = pd.DataFrame(
        [
            {"b": 2, "c": 4},
        ]
    )
    with pytest.raises(AssertionError, match=r"Missing primary key column*"):
        _reconcile("a", existing, target)


def test_update_table(monkeypatch):
    execute_batch = MagicMock()
    cursor = MagicMock()
    import gumbo_dao.gumbo_dao

    monkeypatch.setattr(gumbo_dao.gumbo_dao, "execute_batch", execute_batch)

    def _execute_batch(cur, sql, params):
        assert cur == cursor
        assert sql == "UPDATE tab SET a = %s, b = %s WHERE pk = %s"
        assert params == [[4, 5, 1]]

    execute_batch.side_effect = _execute_batch
    _update_table(cursor, "tab", "pk", pd.DataFrame([{"a": 4, "b": 5, "pk": 1}]))
    assert execute_batch.call_count == 1


def test_assert_has_subset_of_rows():
    full_df = pd.DataFrame(
        [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 1, "b": 12}, {"a": 3, "b": 14}]
    )
    subset_df = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    superset_df = pd.DataFrame([{"a": 1, "b": 2}, {"a": 13, "b": 14}])

    gumbo_dao.gumbo_dao._assert_has_subset_of_rows(
        subset_df=subset_df, full_df=full_df
    )  # no exception thrown

    with pytest.raises(Exception) as e_info:
        gumbo_dao.gumbo_dao._assert_has_subset_of_rows(
            subset_df=superset_df, full_df=full_df
        )  # throws exception

    with pytest.raises(Exception) as e_info:
        gumbo_dao.gumbo_dao._assert_has_subset_of_rows(
            subset_df=full_df, full_df=subset_df
        )  # throws exception
