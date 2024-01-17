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

# @pytest.mark.skipif(
#     os.environ.get("POSTGRES_TEST_DB") is None,
#     reason="Needs name of local test database",
# )
# def test_against_local_postgres(tmpdir):
#     config_path = tmpdir.join("config.json")
#     config_path.write(
#         json.dumps(
#             {
#                 "host": "localhost",
#                 "database": os.environ["POSTGRES_TEST_DB"],
#                 "user": "postgres",
#             }
#         )
#     )
#     c = gumbo_client.Client(config_dir=str(tmpdir))

#     cur = c.connection.cursor()
#     cur.execute(
#         "create table test_sample_table (id integer primary key, str_col varchar(100), float_col float)"
#     )
#     cur.execute(
#         "insert into test_sample_table (id, str_col, float_col) values (1, 'a', 1.0), (2, 'b', 2.0)"
#     )
#     cur.close()

#     def check_table(expected):
#         df = c.get("test_sample_table")
#         rows = df.to_dict("records")
#         rows = sorted(rows, key=lambda row: row["id"])
#         assert rows == expected

#     check_table(
#         [
#             {"id": 1, "str_col": "a", "float_col": 1.0},
#             {"id": 2, "str_col": "b", "float_col": 2.0},
#         ]
#     )

#     final_df = pd.DataFrame(
#         [
#             {"id": 1, "str_col": "a2", "float_col": 11.0},
#             {"id": 2, "str_col": "b2", "float_col": 12.0},
#         ]
#     )
#     c.update("test_sample_table", final_df)

#     check_table(
#         [
#             {"id": 1, "str_col": "a2", "float_col": 11.0},
#             {"id": 2, "str_col": "b2", "float_col": 12.0},
#         ]
#     )
