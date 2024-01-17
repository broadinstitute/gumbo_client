import os
from typing import Annotated

from fastapi import FastAPI, Depends
from dotenv import load_dotenv, find_dotenv
import psycopg2
from gumbo_client.gumbo_dao import GumboDAO
import status

async def get_db_connection():
    load_dotenv(find_dotenv())
    connection_string = os.environ["GUMBO_CONNECTION_STRING"]
    return psycopg2.connect(connection_string)

async def get_gumbo_dao(connection : Annotated[object, Depends(get_db_connection)]):
    dao = GumboDAO(sanity_check=True, 
                   connection=connection)
    return dao

app = FastAPI()

@app.get("/table/{table_name}")
async def get_table(table_name: str, gumbo_dao: Annotated[GumboDAO, Depends(get_gumbo_dao)]):
    return gumbo_dao.get(table_name).to_json()

@app.get("/status-summaries")
async def get_model_condition_status_summaries(db_connection: Annotated[object, Depends(get_db_connection)], peddep_only: bool = False):
    # get the set of statuses 
    status_dict = status.init_status_dict(db_connection.cursor(), peddep_only)
    status_dict = status.add_omics_statuses(db_connection.cursor(), status_dict)
    status_dict = status.add_crispr_statuses(db_connection.cursor(), status_dict)
    # convert from dict[string -> MCInfo] to dict[string -> dict]
    return {mc_id: info.to_json_dict() for mc_id, info in status_dict.items()}
