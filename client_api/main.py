import os

from fastapi import FastAPI
from dotenv import load_dotenv, find_dotenv
import psycopg2

from client import Client

load_dotenv(find_dotenv())
connection_string = os.environ["GUMBO_CONNECTION_STRING"]
db_connection = psycopg2.connect(connection_string)
client = Client(
    psycopg2_connection=db_connection, 
    sanity_check=True, 
    username="TEMPORARY", autocommit=True
)

app = FastAPI()


@app.get("/table/{table_name}")
async def get_table(table_name: str):
    return client.get(table_name).to_json()
    # TODO: return 404 if table doesn't exist

# Notes on rewrite:
# - not sure it makes sense to enable transactions (turning off autocommit) with this rewrite
# - no use in having a "close" function when it's making api requests
# - probably better to just change the api and call it a new thing to avoid confusion