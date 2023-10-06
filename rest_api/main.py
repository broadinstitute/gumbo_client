import os

from fastapi import FastAPI
from dotenv import load_dotenv, find_dotenv
import psycopg2

from data_access import GumboDAO
import status


load_dotenv(find_dotenv())
connection_string = os.environ["GUMBO_CONNECTION_STRING"]
db_connection = psycopg2.connect(connection_string)
app = FastAPI()


@app.get("/table/{table_name}")
async def get_table(table_name: str):
    data_access = GumboDAO(psycopg2_connection=db_connection)
    return data_access.get(table_name).to_json()

@app.get("/status-summaries")
async def get_model_condition_status_summaries(peddep_only: bool = False):
    # get the set of statuses 
    status_dict = status.init_status_dict(db_connection.cursor(), peddep_only)
    status_dict = status.add_omics_statuses(db_connection.cursor(), status_dict)
    status_dict = status.add_crispr_statuses(db_connection.cursor(), status_dict)
    # convert from dict[string -> MCInfo] to dict[string -> dict]
    return {mc_id: info.to_json_dict() for mc_id, info in status_dict.items()}
