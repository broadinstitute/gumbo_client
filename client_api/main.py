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
# - Not sure it makes sense to enable transactions (turning off autocommit) with this rewrite
#   - no use in having a "close" function when it's making api requests
#   - probably best to depricate these functions and give a warning when they're used
# - Also, I'm not sure how we'll do read-only credentials with this setup
#   - could we make a second service account that only has access to GET endpoints? Is this possible? 
#   - or just make a readonly flag on the client constructor? 