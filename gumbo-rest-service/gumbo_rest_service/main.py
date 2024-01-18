import os
from typing import Annotated

from fastapi import FastAPI, Depends, HTTPException
from dotenv import load_dotenv, find_dotenv
import psycopg2
from gumbo_dao import GumboDAO
from dataframe_json_packing import pack


def _get_db_connection():
    load_dotenv(find_dotenv())
    connection_string = os.environ["GUMBO_CONNECTION_STRING"]
    return psycopg2.connect(connection_string)


def get_db_connection():
    # in tests _get_db_connection will be mocked, so delegate to that
    return _get_db_connection()


def _get_gumbo_dao(connection):
    # in tests _get_gumbo_dao will be mocked, so delegate to that
    dao = GumboDAO(sanity_check=True, connection=connection)
    return dao


async def get_gumbo_dao(connection: Annotated[object, Depends(get_db_connection)]):
    return _get_gumbo_dao(connection)


import re

app = FastAPI()


def _validate_name(name):
    if re.match("[A-Za-z_]", name) is None:
        raise HTTPException(status_code=400)


@app.get("/table/{table_name}")
async def get_table(
    table_name: str, gumbo_dao: Annotated[GumboDAO, Depends(get_gumbo_dao)]
):
    _validate_name(table_name)
    df = gumbo_dao.get(table_name)
    if df is None:
        raise HTTPException(status_code=404)
    result = pack(df)
    return result
