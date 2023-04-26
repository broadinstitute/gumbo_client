import requests
import subprocess
import urllib

import pandas as pd
import os

url = 'https://client-api-dot-depmap-gumbo.uc.r.appspot.com'


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self.token
        return r


class Client:
    def __init__(self):
        output_stream = os.popen("gcloud auth print-access-token")
        self.token = output_stream.read().strip()

    def get(self, table_name: str) -> pd.DataFrame:
        # response = requests.get(f'{url}/table/{table_name}', auth=BearerAuth(self.token))
        response = requests.get(f'{url}/table/{table_name}', headers={"Authorization": f"Bearer {self.token}"})
        # TODO: debug why I'm getting 401 instead of 403 (I should be forbidden, not unauthorized)
        # TODO: I see online that access tokens are usually used as bearer tokens, but Googles example uses an OIDC token - why?
        response.raise_for_status()
        return pd.read_json(response.json())

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
