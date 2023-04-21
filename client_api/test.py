import google.auth
from google.auth.transport.urllib3 import AuthorizedHttp


credentials, project = google.auth.default()

authed_http = AuthorizedHttp(credentials)

response = authed_http.request(
    'GET', 'http://localhost:8000/table/depmap_model_type')

print(response.__dict__)