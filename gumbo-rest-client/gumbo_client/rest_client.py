import os

from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
import pandas as pd
import os.path
from .exceptions import UnknownTable
from dataframe_json_packing import unpack, pack
import json

base_url = "https://rest-api-v2-dot-depmap-gumbo.uc.r.appspot.com"
default_gumbo_configs_dir = "~/.config/gumbo"
username_filename = "username"
credentials_filename = "client-iap-auth-sa.json"
client_id = "814840278102-rsmqq17h7kh24a01mtg22kbo9fk5c9oi.apps.googleusercontent.com"


class Client:
    def __init__(
        self,
        *,
        config_dir=default_gumbo_configs_dir,
        authed_session=None,
        username=None,
    ):
        config_dir = os.path.expanduser(config_dir)
        if username is None:
            with open(os.path.join(config_dir, username_filename), "r") as file:
                username = file.read().rstrip()

        if not authed_session:
            # Get an authed session token
            creds = service_account.IDTokenCredentials.from_service_account_file(
                os.path.join(config_dir, credentials_filename),
                target_audience=client_id,
            )
            authed_session = AuthorizedSession(creds)

        self.username = username
        self.authed_session = authed_session

    def _check_response_code(self, response):
        if response.status_code == 404:
            raise UnknownTable()
        response.raise_for_status()

    def get(self, table_name: str) -> pd.DataFrame:
        url = f"{base_url}/table/{table_name}"
        response = self.authed_session.request("GET", url)
        self._check_response_code(response)
        return unpack(response.json())

    # def update(self, table_name, new_df, delete_missing_rows=False, reason=None):
    #     raise NotImplementedError

    # Insert the given rows. Do not update or delete any existing rows.
    # If a column is in the table but missing from the dataframe, it is populated with a default value (typically null)
    # For tables which have auto-generated ID columns, the dataframe does not need to contain ID values.
    # Throw an exception if a given row already exists in the table.
    def insert_only(self, table_name, new_rows_df, *, reason=None):
        url = f"{base_url}/table/{table_name}"
        payload = {
            "mode": "insert_only",
            "username": self.username,
            "data": pack(new_rows_df),
            "reason": reason,
        }
        response = self.authed_session.request("PATCH", url, data=json.dumps(payload))
        self._check_response_code(response)

    # Update the given rows. Do not delete any existing rows or insert any new rows.
    # Throw an exception if a given row does not already exist in the table.
    def update_only(self, table_name, updated_rows_df, *, reason=None):
        url = f"{base_url}/table/{table_name}"
        payload = {
            "mode": "update_only",
            "username": self.username,
            "data": pack(updated_rows_df),
            "reason": reason,
        }
        response = self.authed_session.request("PATCH", url, data=json.dumps(payload))
        self._check_response_code(response)
