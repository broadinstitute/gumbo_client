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
    try:
        return data_access.get(table_name).to_json()
    except:
        return HTTPException(404)

# Notes future challenges with v2 rewrite:
# - I'm not sure it makes sense to let people do autocommit=False with this rewrite 
#   - it doesn't really make sense in the context of a REST API, and isn't really being used anyway
# - This version won't need a "close" function
#   - probably best to depricate these functions and give a warning when they're used
# - Also, I'm not totally sure how we'll do read-only credentials with this setup (if we even want to)
#   - we could just make a readonly flag on the client constructor