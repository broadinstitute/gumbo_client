import pandas as pd
from . import status
from psycopg2.extras import execute_batch, execute_values

def _reconcile(pk_column, existing_table, target_table):
    "matches the rows by primary key column and returns a dataframe containing new rows, a dataframe containing rows in need of updating and the list of IDs of rows which should be deleted"

    # verify that there are no extra columns in target_table
    extra_columns = set(target_table.columns).difference(existing_table.columns)
    assert (
        len(extra_columns) == 0
    ), f"The following columns to update do not exist in the target table: {extra_columns}"
    assert pk_column in set(
        target_table.columns
    ), f"Missing primary key column in data frame: {pk_column}"

    # verify the column types are the same
    for col in target_table.columns:
        if not (
            target_table[col].isnull().all() or existing_table[col].isnull().all()
        ):  # if cols have non-null values
            assert target_table.dtypes[col] == existing_table.dtypes[col]

    # convert rows to dicts and index by primary key
    existing_rows = {
        row[pk_column]: _to_pythonic_hashable_types(row)
        for row in existing_table.to_dict("records")
    }
    new_rows = []
    updated_rows = []
    pks_to_keep = []

    for row in target_table.to_dict("records"):
        # is this a new row or row in need of updating?
        row = _to_pythonic_hashable_types(row)
        id = row[pk_column]
        if id in existing_rows:
            pks_to_keep.append(id)
            existing_row = existing_rows[id]
            if existing_row != row:
                updated_rows.append(row)
        else:
            new_rows.append(row)

    updated_rows = pd.DataFrame(updated_rows, columns=target_table.columns)
    to_delete = set(existing_rows.keys()).difference(pks_to_keep)
    return pd.DataFrame(new_rows), updated_rows, to_delete


def _to_pythonic_hashable_types(row: dict):
    """Convert a row of values to pythonic hashable types"""
    if type(row) == dict:
        return {k: _to_pythonic_hashable_type(v) for k, v in row.items()}


def _to_pythonic_hashable_type(x):
    """Convert a single value to a pythonic hashable type that can be handled by the database"""
    if type(x) == list:
        # lists are converted to strings here because 
        # 1) lists aren't hashable and 
        # 2) that's the only way psycopg2 is able to handle them
        return  str([_to_pythonic_hashable_type(val) for val in x])
    if pd.isna(x):
        return None
    if hasattr(x, "item"):
        # if the type is a numpy type it'll have an item() method for converting to a native python type
        x = x.item()
    return x


def _update_table(cursor, table_name, pk_column, updated_rows):
    columns = sorted(set(updated_rows.columns).difference([pk_column]))
    column_assignments = ", ".join([f"{col} = %s" for col in columns])
    params = []
    for row in updated_rows.to_records():
        params.append(
            [_to_pythonic_hashable_type(row[col]) for col in columns]
            + [_to_pythonic_hashable_type(row[pk_column])]
        )

    execute_batch(
        cursor,
        f"UPDATE {table_name} SET {column_assignments} WHERE {pk_column} = %s",
        params,
    )


def _insert_table(cursor, table_name, new_rows):
    values = []
    for row in new_rows.to_records():
        values.append([_to_pythonic_hashable_type(row[col]) for col in new_rows.columns])
    column_names = ", ".join(new_rows.columns)

    execute_values(
        cursor, f"INSERT INTO {table_name} ({column_names}) VALUES %s", values
    )


def _delete_rows(cursor, table_name, pk_column, ids):
    params = [[id] for id in ids]
    execute_batch(cursor, f"DELETE FROM {table_name} WHERE {pk_column} = %s", params)

def _both_empty(a, b):
    return (a is None or math.isnan(a)) and (b is None or math.isnan(b))


def _assert_dataframes_match(a, b):
    assert (a.columns == b.columns).all()
    mismatches = 0
    for col in a.columns:
        assert len(a) == len(b)
        for ia, ib in zip(a[col], b[col]):
            if ia != ib and not _both_empty(ia, ib):
                print(f"mismatch in {col}: {ia} != {ib}")
                mismatches += 1
    assert mismatches == 0
    #     matches = new_df[col] == final_df[col]

    #     assert (
    #         matches
    #     ).all(), f'Sanity check failed: After update column "{col}" was different then expected: {new_df[col][~matches]} != {final_df[col][~matches]}'

def _assert_has_subset_of_rows(subset_df, full_df):
    # Convert dataframe values to hashable types
    full_pythonic_df = full_df.applymap(_to_pythonic_hashable_type)
    subset_pythonic_df = subset_df.applymap(_to_pythonic_hashable_type)
    
    assert (subset_pythonic_df.columns == full_pythonic_df.columns).all()
    assert len(subset_pythonic_df.merge(full_pythonic_df)) == len(subset_pythonic_df)

# taken from https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
PRIMARY_KEY_QUERY = """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = %s::regclass
AND    i.indisprimary;"""


def _get_pk_column(cursor, table_name):
    cursor.execute(PRIMARY_KEY_QUERY, [table_name])
    rows = cursor.fetchall()
    assert len(rows) == 1, f"expected 1 row, but got {rows}"
    return rows[0][0]


def _update(connection, table_name, cur_df, new_df, username, delete_missing_rows=False, reason=None):
    cursor = connection.cursor()

    try:
        pk_column = _get_pk_column(cursor, table_name)

        new_rows, updated_rows, removed_rows = _reconcile(pk_column, cur_df, new_df)

        _insert_table(cursor, table_name, new_rows)
        _update_table(cursor, table_name, pk_column, updated_rows)
        if delete_missing_rows:
            _delete_rows(cursor, table_name, pk_column, removed_rows)
        
        deleted_row_count = len(removed_rows) if delete_missing_rows else 0
        _log_bulk_update(connection, username, table_name, updated_rows.shape[0], new_rows.shape[0], deleted_row_count, reason=reason)
    except:
        raise
    finally:
        cursor.close()
    print(
        f"Inserted {len(new_rows)} rows, updated {len(updated_rows)} rows, and deleted {deleted_row_count} rows"
    )


def _log_bulk_update(connection, username, tablename, rows_updated=0, rows_deleted=0, rows_inserted=0, reason=None):
    reason_val = f"'{reason}'" if reason is not None else "null"
    insert_statement = f"""INSERT INTO bulk_update_log 
        (username, "timestamp", tablename, rows_updated, rows_deleted, rows_inserted, reason) 
        VALUES ('{username}', CURRENT_TIMESTAMP, '{tablename}', {rows_updated}, {rows_deleted}, {rows_inserted}, {reason_val});"""
    cursor = connection.cursor()
    cursor.execute(insert_statement)
    cursor.close()

class GumboDAO2:
    def __init__(self, sanity_check, connection):
        self.sanity_check = sanity_check
        self.connection = connection

    def _set_username(self, username):
        with self.connection.cursor() as cursor:
            print("setting username to", self.username)
            cursor.execute("SET my.username=%s", [self.username])

    def get(self, table_name):
        cursor = self.connection.cursor()
        select_query = f"select * from {table_name}"

        # If a primary key exists, use it to sort the table
        # Views don't have primary keys to use here, and that's fine.
        try:
            pk_column = _get_pk_column(cursor, table_name)
            select_query += f" order by {pk_column}"
        except AssertionError:
            pass
        finally:
            cursor.close()
        return pd.read_sql(select_query, self.connection)


    def update(self, username, table_name, new_df, delete_missing_rows=False, reason=None):
        self._set_username(username)

        cur_df = self.get(table_name)

        result = _update(self.connection, table_name, cur_df, new_df, username, delete_missing_rows, reason=reason)
        if self.sanity_check:
            # if we want to be paranoid, fetch the dataframe back and verify that it's the same as what we said we
            # wanted to target. 
            table_df = self.get(table_name)
            # only check the columns that were provided in the target table
            if delete_missing_rows:
                _assert_dataframes_match(new_df, table_df[new_df.columns])
            else: 
                _assert_has_subset_of_rows(new_df, table_df[new_df.columns])
        return result

    # Insert the given rows. Do not update or delete any existing rows.
    # If a column is in the table but missing from the dataframe, it is populated with a default value (typically null)
    # For tables which have auto-generated ID columns, the dataframe does not need to contain ID values.
    # Throw an exception if a given row already exists in the table.
    def insert_only(self, username, table_name, new_rows_df, reason=None):
        self._set_username(username)

        cursor = self.connection.cursor()
        try:
            _insert_table(cursor, table_name, new_rows_df)
            _log_bulk_update(self.connection, username, table_name, rows_inserted=new_rows_df.shape[0], reason=reason)
        except:
            raise
        finally:
            cursor.close()

    # Update the given rows. Do not delete any existing rows or insert any new rows.
    # Throw an exception if a given row does not already exist in the table.
    def update_only(self, username, table_name, updated_rows_df, reason=None):
        self._set_username(username)

        cursor = self.connection.cursor()
        try:
            pk_column = _get_pk_column(cursor, table_name)
            _update_table(cursor, table_name, pk_column, updated_rows_df)
            _log_bulk_update(self.connection, username, table_name, rows_updated=updated_rows_df.shape[0], reason=reason)
        except:
            raise
        finally:
            cursor.close()

    def get_model_condition_status_summaries(self, peddep_only: bool = False):
        # get the set of statuses 
        status_dict = status.init_status_dict(self.connection.cursor(), peddep_only)
        status_dict = status.add_omics_statuses(self.connection.cursor(), status_dict)
        status_dict = status.add_crispr_statuses(self.connection.cursor(), status_dict)
        # convert from dict[string -> MCInfo] to dict[string -> dict]
        return {mc_id: info.to_json_dict() for mc_id, info in status_dict.items()}
