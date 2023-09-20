import os

from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv, find_dotenv
import psycopg2

from data_access.gumbo_data_access import GumboDAO


load_dotenv(find_dotenv())
connection_string = os.environ["GUMBO_CONNECTION_STRING"]
db_connection = psycopg2.connect(connection_string)
data_access = GumboDAO(
    psycopg2_connection=db_connection, 
    sanity_check=True, 
    username="TEMPORARY", autocommit=True
)

app = FastAPI()


@app.get("/table/{table_name}")
async def get_table(table_name: str):
    return data_access.get(table_name).to_json()
