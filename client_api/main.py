import os

from fastapi import FastAPI
from dotenv import load_dotenv, find_dotenv
import psycopg2

load_dotenv(find_dotenv())
connection_string = os.environ["GUMBO_CONNECTION_STRING"]
conn = psycopg2.connect(connection_string)
cur = conn.cursor()

app = FastAPI()


@app.get("/table/{table_name}")
async def get_table(table_name: str):
    return {"message": f"Got {table_name}"}