from pytest import fixture
from gumbo_client import Client
import gumbo_rest_service.main
import pandas as pd
from fastapi.testclient import TestClient

class ResponseAdapter:
    def __init__(self, response):
        self.response = response

class HTTPSessionAdapter:
    def __init__(self, http_client) -> None:
        self.http_client = http_client

    def request(self, method: str, url: str):
        if method == "GET":
            method_fn = self.http_client.get
        elif method == "POST":
            method_fn = self.http_client.post
        else:
            raise NotImplemented()
        
        return ResponseAdapter(method_fn(url))

@fixture
def http_client():
    return TestClient(gumbo_rest_service.main.app)

@fixture
def gumbo_client(http_client):
    # the client wants a authsession interface 
    # but our http test client has a different interface, 
    # use an adapter that has the methods we need
    return Client(username="testuser", authed_session=HTTPSessionAdapter(http_client))

def test_get_table(client):
    df = client.get("sample")
    expected_df = pd.DataFrame({"PK": ["X", "Y"], "X": [1, 2]})
    assert expected_df.equals(df)

