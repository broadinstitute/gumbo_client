import os
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

import pandas as pd

base_url = 'https://rest-api-dot-depmap-gumbo.uc.r.appspot.com'
gumbo_configs_dir = os.path.expanduser('~') + "/.config/gumbo/"
client_id_filename = 'iap_client_id.txt'
credentials_filename = 'client-iap-auth-sa.json'


class Client:
    def __init__(self):
        # Read secrets from file 
        with open(gumbo_configs_dir + client_id_filename, 'r') as file:
            client_id = file.read().rstrip()
        creds = service_account.IDTokenCredentials.from_service_account_file(
            gumbo_configs_dir + credentials_filename,
            target_audience=client_id)
        # Get an authed session token 
        self.authed_session = AuthorizedSession(creds)

    def get(self, table_name: str) -> pd.DataFrame:
        url = f'{base_url}/table/{table_name}'
        response = self.authed_session.request("GET", url)
        response.raise_for_status()
        return pd.read_json(response.json())

    def update(self, table_name, new_df, delete_missing_rows=False, reason=None):
        raise NotImplementedError

    # Insert the given rows. Do not update or delete any existing rows.
    # If a column is in the table but missing from the dataframe, it is populated with a default value (typically null)
    # For tables which have auto-generated ID columns, the dataframe does not need to contain ID values.
    # Throw an exception if a given row already exists in the table.
    def insert_only(self, table_name, new_rows_df, reason=None):
        raise NotImplementedError

    # Update the given rows. Do not delete any existing rows or insert any new rows.
    # Throw an exception if a given row does not already exist in the table.
    def update_only(self, table_name, updated_rows_df, reason=None):
        raise NotImplementedError

    def get_model_condition_status_summaries(self, peddep_only: bool = False):
        raise NotImplementedError
