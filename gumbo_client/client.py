from re import match
import pandas as pd
from psycopg2.extras import execute_batch, execute_values
import psycopg2
import os
import json
from .cloud_sql_proxy import get_cloud_sql_proxy_port
import math


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
        if not (target_table[col].isnull().all() or existing_table[col].isnull().all()): # if cols have non-null values
            assert target_table.dtypes[col] == existing_table.dtypes[col]

    # convert rows to dicts and index by primary key
    existing_rows = {
        row[pk_column]: _to_pythonic_type(row)
        for row in existing_table.to_dict("records")
    }
    new_rows = []
    updated_rows = []
    pks_to_keep = []

    for row in target_table.to_dict("records"):
        # is this a new row or row in need of updating?
        row = _to_pythonic_type(row)
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
    if type(x) == dict:
        return {k: _to_pythonic_type(v) for k, v in x.items()}
    if pd.isna(x):
        return None
    if hasattr(x, "item"):
        x = x.item()
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
    print(
        f"Inserted {len(new_rows)} rows, updated {len(updated_rows)} rows, and deleted {len(removed_rows)} rows"
    )

def _build_db_connection(config_dir):
    with open(os.path.join(config_dir, "config.json"), "rt") as fd:
        config = json.load(fd)
    # cloud_sql_proxy_instance = config.get("cloud_sql_proxy_instance")
    # if cloud_sql_proxy_instance:
    #     # if set, use cloud_sql_proxy to connect to DB
    #     host = "localhost"
    #     port = get_cloud_sql_proxy_port(
    #         os.path.join(
    #             config_dir, f"cloud_sql_proxy_{cloud_sql_proxy_instance}.json"
    #         ),
    #         cloud_sql_proxy_instance,
    #     )
    #     sslrootcert = sslcert = sslkey = None
    # else:
    #     # otherwise, connect directly using SSL certs+key
    #     host = config["host"]
    #     # write out the various keys
    #     sslrootcert = os.path.join(config_dir, "server-ca.pem")
    #     sslcert = os.path.join(config_dir, "client-cert.pem")
    #     sslkey = os.path.join(config_dir, "client-key.pem")
    #     def write_prop(name, dest):
    #         with open(dest, "wt") as fd:
    #             fd.write(config[name])

    #     write_prop("sslrootcert", sslrootcert)
    #     write_prop("sslcert", sslcert)
    #     write_prop("sslkey", sslkey)


    database = config["database"]
    user = config["user"]

    kwargs = dict(
        host="localhost",
        database=database,
        user=user,
        port=5432,
        password=config.get("password")
        # sslmode=config.get("sslmode"),
        # sslrootcert=sslrootcert,
        # sslcert=sslcert,
        # sslkey=sslkey,
    )
    print(f"Connecting to the '{database}' database.")
    return psycopg2.connect(**kwargs)


class Client:
    def __init__(self, config_dir="~/.config/gumbo", sanity_check=True, psycopg2_connection=None, username=None):
        config_dir = os.path.expanduser(config_dir)
        self.sanity_check = sanity_check
        if psycopg2_connection is None:
            self.connection = _build_db_connection(config_dir)
        else: 
            self.connection = psycopg2_connection
        # set the username for use in audit logs
        username = username or os.getlogin() + " (py)"
        with self.connection.cursor() as cursor:
            print("setting username to", username)
            cursor.execute("SET my.username=%s", [username])

    def get(self, table_name):
        cursor = self.connection.cursor()

        try:
            pk_column = _get_pk_column(cursor, table_name)
        finally:
            cursor.close()

        df = pd.read_sql(
            f"select * from {table_name} order by {pk_column}", self.connection
        )
        return df

    def update(self, table_name, new_df):
        cur_df = self.get(table_name)

        result = _update(self.connection, table_name, cur_df, new_df)
        if self.sanity_check:
            # if we want to be paranoid, fetch the dataframe back and verify that it's the same as what we said we
            # wanted to target.
            final_df = self.get(table_name)
            # only check the columns that were provided in the target table
            _assert_dataframes_match(new_df, final_df[new_df.columns])
        return result
    
    # Insert the given rows. Do not update or delete any existing rows. 
    # If a column is in the table but missing from the dataframe, it is populated with a default value (typically null)
    # For tables which have auto-generated ID columns, the dataframe does not need to contain ID values. 
    # Throw an exception if a given row already exists in the table.
    def insert_only(self, table_name, new_rows_df):
        cursor = self.connection.cursor()
        _insert_table(cursor, table_name, new_rows_df)
        cursor.close()

    # Update the given rows. Do not delete any existing rows or insert any new rows. 
    # Throw an exception if a given row does not already exist in the table.
    def update_only(self, table_name, updated_rows_df):
        cursor = self.connection.cursor()
        pk_column = _get_pk_column(cursor, table_name)
        _update_table(cursor, table_name, pk_column, updated_rows_df)
        cursor.close()

    def commit(self):
        self.connection.commit()

    def close(self):
        with self.connection.cursor() as cursor:
            print("clearing username")
            cursor.execute("SET my.username='invalid'")
        self.connection.close()