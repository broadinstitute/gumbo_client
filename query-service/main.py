from os import environ
from typing import Union, Optional
import io

from fastapi import FastAPI, HTTPException, Depends

from fastapi.responses import StreamingResponse

from gumbo_client import Client
import gumbo_client
from gumbo_client.utils import NameMappingUtils
import numpy as np
import psycopg2
import os
import logging
from enum import Enum
import pandas as pd

log = logging.getLogger(__name__)

app = FastAPI()

def get_gumbo_client():
    connection_str = os.environ.get("GUMBO_DB_URL")
    if connection_str is None:
        log.warn("Environment variable GUMBO_DB_URL is not set. Defaulting to use proxy")
        connection = None
    else:
        connection = psycopg2.connect(connection_str)
    return Client(psycopg2_connection=connection, username="query-service")

@app.get("/")
def read_root():
    return {"name": "query-service"}

@app.get("/query/{name}")
def run_query(name: str, key: str = None, client: Client = Depends(get_gumbo_client)):
    cur = client.connection.cursor()
    try:
        cur.execute("select key, query from gumbo_external_query geq where id = %s", [name])
        rows = cur.fetchall()
        if len(rows) == 0:
            raise HTTPException(status_code=404, detail="Unknown query")
        assert len(rows) == 1
        expected_key, sql = rows[0]
    finally:
        cur.close()


    if key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid key")

    table = pd.read_sql(
        sql, client.connection
    )

    stream = io.StringIO()
    table.to_csv(stream, index = False)
    response = StreamingResponse(iter([stream.getvalue()]),
                        media_type="text/csv"
    )
    response.headers["Content-Disposition"] = f"attachment; filename={name}.csv"
    return response



    
