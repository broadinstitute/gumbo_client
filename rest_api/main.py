import os

from fastapi import FastAPI
from dotenv import load_dotenv, find_dotenv
import psycopg2

from data_access import GumboDAO


load_dotenv(find_dotenv())
connection_string = os.environ["GUMBO_CONNECTION_STRING"]
db_connection = psycopg2.connect(connection_string)
app = FastAPI()


@app.get("/table/{table_name}")
async def get_table(table_name: str):
    data_access = GumboDAO(psycopg2_connection=db_connection)
    return data_access.get(table_name).to_json()
