from os import environ
from typing import Union

from fastapi import FastAPI, Depends
from gumbo_client import Client
from gumbo_client.utils import NameMappingUtils
import numpy as np
import psycopg2
import os
import logging

log = logging.getLogger(__name__)

app = FastAPI()

def get_gumbo_client():
    connection_str = os.environ.get("GUMBO_DB_URL")
    if connection_str is None:
        log.warn("Environment variable GUMBO_DB_URL is not set. Defaulting to use proxy")
        connection = None
    else:
        connection = psycopg2.connect(connection_str)
    return Client(psycopg2_connection=connection, username="gumbo-rest-api")

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/tables")
def get_tables():
    # just instantiating to get the list of table names
    mapping = NameMappingUtils()
    return sorted(mapping.name_mapping.keys())

def _nans_to_none(values):
    return [None if (isinstance(value, float) and np.isnan(value)) else value for value in values]

@app.get("/table/{name}")
def read_item(name: str, client: Client = Depends(get_gumbo_client)):
    tables = get_tables()
    assert name in tables

    table = client.get(name)

    # turn into a dict of columns, replacing nans because those are not valid json values
    table_as_dict = {col: _nans_to_none(table[col]) for col in table.columns}
    
    return table_as_dict
