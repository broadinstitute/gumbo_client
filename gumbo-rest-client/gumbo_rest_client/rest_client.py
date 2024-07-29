import pandas as pd
import os.path
from .exceptions import UnknownTable
from dataframe_json_packing import unpack, pack
import json
from .auth import create_authorized_session
import getpass
from .const import prod_url
import requests


class Client:
    def __init__(self, *, authed_session=None, username=None, base_url=prod_url):
        """
        `username` is purely for informational purposes in the audit log, so provide the name of the program
        if this is being run non-interactively.
        """
        if not authed_session:
            authed_session = create_authorized_session()

        if username is None:
            username = getpass.getuser()
            assert username not in [
                "ubuntu",
                "root",
                None,
            ], "Please provide a username."

        self.username = username
        self.authed_session = authed_session
        self.base_url = base_url

    def _check_response_code(self, response):
        if response.status_code == 404:
            raise UnknownTable()
        if response.status_code != 200:
            raise Exception(
                f"{response.status_code} Error from Gumbo REST Service: {response.text}"
            )

    def get(self, table_name: str) -> pd.DataFrame:
        url = f"{self.base_url}/table/{table_name}"
        response = self.authed_session.request("GET", url)
        self._check_response_code(response)
        return unpack(response.json())

    def insert_only(self, table_name, new_rows_df, *, reason=None):
        """
        Insert the given rows. Do not update or delete any existing rows.

        If a column is in the table but missing from the dataframe, it is populated with a default value (typically null)
        For tables which have auto-generated ID columns, the dataframe does not need to contain ID values.

        Throw an exception if a given row already exists in the table.
        """
        url = f"{self.base_url}/table/{table_name}"
        payload = {
            "mode": "insert_only",
            "username": self.username,
            "data": pack(new_rows_df),
            "reason": reason,
        }
        response = self.authed_session.request("PATCH", url, data=json.dumps(payload))
        self._check_response_code(response)

    def update_only(self, table_name, updated_rows_df, *, reason=None):
        """
        Update the given rows. Do not delete any existing rows or insert any new rows.

        Throw an exception if a given row does not already exist in the table.
        """
        url = f"{self.base_url}/table/{table_name}"
        payload = {
            "mode": "update_only",
            "username": self.username,
            "data": pack(updated_rows_df),
            "reason": reason,
        }
        response = self.authed_session.request("PATCH", url, data=json.dumps(payload))
        self._check_response_code(response)
