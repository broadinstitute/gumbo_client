from os import environ
from typing import Union, Optional
import io

from fastapi import FastAPI, Depends
from fastapi.responses import StreamingResponse

from gumbo_client import Client
from gumbo_client.utils import NameMappingUtils
import numpy as np
import psycopg2
import os
import logging
from enum import Enum

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

class ExportFormat(str, Enum):
    csv="csv"
    json="json"

@app.get("/table/{name}")
def read_item(name: str, format: ExportFormat = ExportFormat.json, client: Client = Depends(get_gumbo_client)):
    tables = get_tables()
    assert name in tables


    table = client.get(name)


    if format == ExportFormat.json:
        # turn into a dict of columns, replacing nans because those are not valid json values
        table_as_dict = {col: _nans_to_none(table[col]) for col in table.columns}
        return table_as_dict
    else:
        assert format == ExportFormat.csv
        stream = io.StringIO()
        table.to_csv(stream, index = False)
        response = StreamingResponse(iter([stream.getvalue()]),
                            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = f"attachment; filename={name}.csv"
        return response

        
    
