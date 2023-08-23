import os

import pandas as pd


class GumboDAO:
    """Data Access Object for """
    def __init__(
        self,
        psycopg2_connection,
        sanity_check=True,
        username=None,
        autocommit=True
    ):
        self.sanity_check = sanity_check
        self.connection = psycopg2_connection
        self.connection.autocommit = autocommit

        # set the username for use in audit logs
        self.username = username or os.getlogin() + " (py)"
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


def _get_pk_column(cursor, table_name):
    # taken from https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    primary_key_query = """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %s::regclass
        AND    i.indisprimary;"""
    cursor.execute(primary_key_query, [table_name])
    rows = cursor.fetchall()
    assert len(rows) == 1, f"expected 1 row, but got {rows}"
    return rows[0][0]
