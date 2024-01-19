import gumbo_client
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.id_token import fetch_id_token_credentials

creds = fetch_id_token_credentials(gumbo_client.client_id)
authed_session = AuthorizedSession(creds)

c = gumbo_client.Client(authed_session=authed_session, username="pmontgom")
print(c.get("model"))
