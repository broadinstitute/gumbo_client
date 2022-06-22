import pandas as pd
from psycopg2.extras import execute_batch, execute_values
import psycopg2
import os
import json


def _reconcile(pk_column, existing_table, target_table):
    "matches the rows by primary key column and returns a dataframe containing new rows, a dataframe containing rows in need of updating and the list of IDs of rows which should be deleted"
    # convert rows to dicts and index by primary key
    existing_rows = {row[pk_column]: row for row in existing_table.to_dict("records")}
    new_rows = []
    updated_rows = []
    pks_to_keep = []

    for row in target_table.to_dict("records"):
        # is this a new row or row in need of updating?
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


def _to_pythonic_type(x):
    # if the type is a numpy type it'll have an item() method for converting to a native python type
    if hasattr(x, "item"):
        return x.item()
    return x


def _update_table(cursor, table_name, pk_column, updated_rows):
    columns = sorted(set(updated_rows.columns).difference([pk_column]))
    column_assignments = ", ".join([f"{col} = %s" for col in columns])
    params = []
    for row in updated_rows.to_records():
        params.append(
            [_to_pythonic_type(row[col]) for col in columns]
            + [_to_pythonic_type(row[pk_column])]
        )

    execute_batch(
        cursor,
        f"UPDATE {table_name} SET {column_assignments} WHERE {pk_column} = %s",
        params,
    )


def _insert_table(cursor, table_name, new_rows):
    values = []
    for row in new_rows.to_records():
        values.append([_to_pythonic_type(row[col]) for col in new_rows.columns])
    column_names = ", ".join(new_rows.columns)

    execute_values(
        cursor, f"INSERT INTO {table_name} ({column_names}) VALUES %s", values
    )


def _delete_rows(cursor, table_name, pk_column, ids):
    params = [[id] for id in ids]
    execute_batch(cursor, f"DELETE FROM {table_name} WHERE {pk_column} = %s", params)


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


def _update(connection, table_name, cur_df, new_df):
    cursor = connection.cursor()

    try:
        pk_column = _get_pk_column(cursor, table_name)

        new_rows, updated_rows, removed_rows = _reconcile(pk_column, cur_df, new_df)

        _insert_table(cursor, table_name, new_rows)
        _update_table(cursor, table_name, pk_column, updated_rows)
        _delete_rows(cursor, table_name, pk_column, removed_rows)
    finally:
        cursor.close()


class Client:
    def __init__(self, config_dir="~/.config/gumbo", sanity_check=True):
        config_dir = os.path.expanduser(config_dir)
        self.sanity_check = sanity_check
        with open(os.path.join(config_dir, "config.json"), "rt") as fd:
            config = json.load(fd)

        kwargs = dict(
            host=config["host"],
            database=config["database"],
            user=config["user"],
            password=config.get("password"),
            sslmode=config.get("sslmode"),
            sslrootcert=os.path.join(config_dir, "root.crt"),
            sslcert=os.path.join(config_dir, "postgresql.crt"),
            sslkey=os.path.join(config_dir, "postgresql.key"),
        )

        connection = psycopg2.connect(**kwargs)
        self.connection = connection

    def get(self, table_name):
        df = pd.read_sql(f"select * from {table_name}", self.connection)
        return df

    def update(self, table_name, new_df):
        cur_df = self.get(table_name)

        result = _update(self.connection, table_name, cur_df, new_df)
        if self.sanity_check:
            # if we want to be paranoid, fetch the dataframe back and verify that it's the same as what we said we
            # wanted to target.
            final_df = self.get(table_name)
            # only check the columns that were provided in the target table
            for col in new_df.columns:
                assert (
                    new_df[col] == final_df[col]
                ).all(), f'Sanity check failed: After update column "{col}" was different then expected'
        return result

    def close(self):
        self.connection.close()
