import json
import math
import os
import time

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch, execute_values

from .cloud_sql_proxy import get_cloud_sql_proxy_port
from gumbo_dao import GumboDAO2

def _build_db_connection(config_dir):
    with open(os.path.join(config_dir, "config.json"), "rt") as fd:
        config = json.load(fd)

    cloud_sql_proxy_instance = config.get("cloud_sql_proxy_instance")
    if cloud_sql_proxy_instance:
        # if set, use cloud_sql_proxy to connect to DB
        host = "localhost"
        port = get_cloud_sql_proxy_port(
            os.path.join(
                config_dir, f"cloud_sql_proxy_{cloud_sql_proxy_instance}.json"
            ),
            cloud_sql_proxy_instance,
        )
        sslrootcert = sslcert = sslkey = None
    else:
        # otherwise, connect directly using SSL certs+key
        host = config["host"]
        # write out the various keys
        sslrootcert = os.path.join(config_dir, "server-ca.pem")
        sslcert = os.path.join(config_dir, "client-cert.pem")
        sslkey = os.path.join(config_dir, "client-key.pem")

        def write_prop(name, dest):
            with open(dest, "wt") as fd:
                fd.write(config[name])

        write_prop("sslrootcert", sslrootcert)
        write_prop("sslcert", sslcert)
        write_prop("sslkey", sslkey)

    database = config["database"]
    user = config["user"]

    kwargs = dict(
        host=host,
        database=database,
        user=user,
        port=port,
        password=config.get("password"),
        sslmode=config.get("sslmode"),
        sslrootcert=sslrootcert,
        sslcert=sslcert,
        sslkey=sslkey,
    )

    print(f"connecting to {user}@{host}:{port}/{database}")
    connection = _connect_with_retry(kwargs)
    return connection


def _connect_with_retry(kwargs, max_attempts=3):
    exceptions = []
    for i in range(max_attempts):
        try:
            return psycopg2.connect(**kwargs)
        except psycopg2.OperationalError as ex:
            print(f"Warning: Connect fail, but will retry: {ex}")
            exceptions.append(ex)
        time.sleep(1)
    raise Exception(f"Failed to connect: f{exceptions}")



class Client:
    def __init__(
        self,
        config_dir="~/.config/gumbo",
        sanity_check=True,
        psycopg2_connection=None,
        username=None,
        autocommit=True
    ):
        config_dir = os.path.expanduser(config_dir)

        if psycopg2_connection is None:
            connection = _build_db_connection(config_dir)
        else:
            connection = psycopg2_connection
        connection.autocommit = autocommit

        # set the username for use in audit logs
        username = username or os.getlogin() + " (py)"
        self.dao = GumboDAO2(sanity_check, connection, username)

    def get(self, table_name):
        return self.dao.get(table_name)

    def update(self, table_name, new_df, delete_missing_rows=False, reason=None):
        return self.dao.update(self.username, table_name, new_df, delete_missing_rows, reason)

    def insert_only(self, table_name, new_rows_df, reason=None):
        return self.dao.insert_only(self.username, table_name, new_rows_df, reason)

    def update_only(self, table_name, updated_rows_df, reason=None):
        return self.dao.update_only(self.username, table_name, updated_rows_df, reason)

    def get_model_condition_status_summaries(self, peddep_only: bool = False):
        return self.dao.get_model_condition_status_summaries(peddep_only)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()
