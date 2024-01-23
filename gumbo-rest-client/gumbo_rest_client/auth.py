from .const import client_iap_id, client_id
import google.auth
import google.oauth2.service_account
import google.auth.impersonated_credentials
import google.auth.transport.requests
import google.oauth2.id_token


def create_authorized_session(credentials=None, use_default_service_account=False):
    if use_default_service_account:
        assert (
            credentials is None
        ), "Cannot provide credentials and set use_default_service_account=True"

        # this is google's recommendation when running with GOOGLE_APPLICATION_CREDENTIALS
        # set or running on compute engine instance, or app engine. This does _not_
        # work with a set of default user credentials.
        #
        # Also this seems kind of slow. maybe we always impersonate itself a single service
        # account.
        id_token_creds = google.oauth2.id_token.fetch_id_token_credentials(client_id)
    else:
        # this is the only path that works with user credentials. The strategy is: Impersonate
        # a service account and then use that service account to get the id_token_creds.
        if credentials is None:
            credentials, _ = google.auth.default()

        impersonated_creds = google.auth.impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=client_iap_id,
            delegates=[],
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        impersonated_creds.refresh(google.auth.transport.requests.Request())

        id_token_creds = google.auth.impersonated_credentials.IDTokenCredentials(
            target_credentials=impersonated_creds,
            target_audience=client_id,
            include_email=True,
        )

    return google.auth.transport.requests.AuthorizedSession(id_token_creds)
