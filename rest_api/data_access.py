import os

from fastapi import HTTPException
import pandas as pd


class GumboDAO:
    """Data Access Object for reading and writing from the gumbo database"""
    def __init__(self, psycopg2_connection):
        self.connection = psycopg2_connection


    def get(self, table_name: str) -> pd.DataFrame:
        cursor = self.connection.cursor()
        if table_name in _get_valid_table_names(cursor=cursor):
            select_query = f"select * from {table_name}"
            # If a primary key exists, use it to sort the table
            try:
                pk_column = _get_pk_column(cursor, table_name)
                select_query += f" order by {pk_column}"
            except UnexpectedPrimaryKeyCountException:
                # Views don't have primary keys to use here, and future tables may have 
                # multiple primary keys. Both are fine. Just move on without sorting.
                pass
            finally:
                cursor.close()
            return pd.read_sql(select_query, self.connection)
        else: 
            raise HTTPException(404)


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
    if len(rows) != 1:
        raise UnexpectedPrimaryKeyCountException(f"expected 1 primary key, but got {rows}")
    return rows[0][0]


def _get_valid_table_names(cursor) -> list[str]:
    """Get the names of all tables and views in the pucblic schema."""
    table_name_query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    cursor.execute(table_name_query)
    return [row[0] for row in cursor.fetchall()]

def _set_username(cursor, username):
    """Set the username for use in Gumbo's audit logs. This is only required before writing to the database."""
    print("setting username to", username)
    cursor.execute("SET my.username=%s", [username])
    

class UnexpectedPrimaryKeyCountException(Exception):
    pass
