import os
from typing import Annotated

from fastapi import FastAPI, Depends, HTTPException, Header
from dotenv import load_dotenv, find_dotenv
import psycopg2
from gumbo_dao import GumboDAO
from dataframe_json_packing import pack, unpack
from pydantic import BaseModel
from enum import Enum
from typing import Optional, List, Any

import re


def _get_db_connection():
    load_dotenv(find_dotenv())
    connection_string = os.environ["GUMBO_CONNECTION_STRING"]
    connection = psycopg2.connect(connection_string)
    connection.autocommit = True
    return connection


def get_db_connection():
    # in tests _get_db_connection will be mocked, so delegate to that
    return _get_db_connection()


def _get_gumbo_dao(connection):
    # in tests _get_gumbo_dao will be mocked, so delegate to that
    dao = GumboDAO(sanity_check=True, connection=connection)
    return dao


async def get_gumbo_dao(connection: Annotated[object, Depends(get_db_connection)]):
    return _get_gumbo_dao(connection)


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


@app.get("/debug-info")
def get_debug_info():
    try:
        from .deploy_debug_info import files  # pyright: ignore [reportMissingImports]
    except ImportError:
        files = ["No debugging info"]
    return files


class UpdateMode(str, Enum):
    insert_only = "insert_only"
    update_only = "update_only"


class Update(BaseModel):
    mode: UpdateMode
    username: str
    data: Any
    reason: Optional[str] = None


@app.patch("/table/{table_name}")
async def update_table(
    table_name: str,
    update: Update,
    gumbo_dao: Annotated[GumboDAO, Depends(get_gumbo_dao)],
):
    updated_rows_df = unpack(update.data)
    if update.mode == UpdateMode.insert_only:
        gumbo_dao.insert_only(
            update.username, table_name, updated_rows_df, reason=update.reason
        )
    elif update.mode == UpdateMode.update_only:
        gumbo_dao.update_only(
            update.username, table_name, updated_rows_df, reason=update.reason
        )
    else:
        raise Exception(f"Invalid mode {update.mode}")
