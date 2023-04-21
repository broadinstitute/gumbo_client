import requests
import urllib

import pandas as pd

url = 'http://localhost:8000'

class Client:
    def __init__(self):
        pass

    def get(self, table_name: str) -> pd.DataFrame:
        response = requests.get(f'{url}/table/{table_name}').json()
        return pd.read_json(response)

    def update(self, table_name, new_df, delete_missing_rows=False, reason=None):
        pass

    # Insert the given rows. Do not update or delete any existing rows.
    # If a column is in the table but missing from the dataframe, it is populated with a default value (typically null)
    # For tables which have auto-generated ID columns, the dataframe does not need to contain ID values.
    # Throw an exception if a given row already exists in the table.
    def insert_only(self, table_name, new_rows_df, reason=None):
        pass

    # Update the given rows. Do not delete any existing rows or insert any new rows.
    # Throw an exception if a given row does not already exist in the table.
    def update_only(self, table_name, updated_rows_df, reason=None):
        pass

    def get_model_condition_status_summaries(self, peddep_only: bool = False):
       pass
