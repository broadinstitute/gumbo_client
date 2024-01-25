# Gumbo Python Client

A python package for reading/writing to the gumbo database


## Installations

*Note: the client has been switched to the v2 of the client which works very
differently. The installation instructions below have changed*

If using poetry, run:

```
poetry source add public-python https://us-central1-python.pkg.dev/cds-artifacts/public-python/simple/
poetry add --source public-python gumbo-rest-service
```

Otherwise, if you don't use poetry, install via pip

```
pip install --extra-index-url=https://us-central1-python.pkg.dev/cds-artifacts/public-python/simple/ gumbo-rest-client
```

This repo now only contains the new version of the gumbo client which no
longer makes direct connections to the database but instead uses a hosted
service. From the users perspective this just means:

1. Simpler setup (no need to download the proxy, and debug differing versions)
2. No need for the client to launch a separate task in the background
3. Fewer connection errors 

To use the new client, import `Client` from `gumbo_rest_client` and then use it as you normally would to read tables. For example:
```
from gumbo_rest_client import Client

client = api_client.Client()
df = client.get("depmap_model_type")
```

Note: If you get an error about "Unable to acquire impersonated credentials ... PERMISSION_DENIED ... iam.serviceAccounts.getAccessToken" you are probably missing a required permission. Make sure your account has been granted "Service Account Token Creator" access on the service account gumbo-client-iap-auth@depmap-gumbo.iam.gserviceaccount.com .

If you are writing your script to run from a non-interactive process, you will need
a service account for it to run under and initialize the client. Also, you should pass in a useful label for username so we know the source of the updates when they
are recorded to the audit log.

Example:

```
from gumbo_rest_client import Client, create_authorized_session

client = api_client.Client(username="my_script_name", 
    authed_session=create_authorized_session(use_default_service_account=True))
df = client.get("depmap_model_type")
```

If you want to test against the staging version, provide a different `base_url`
```
from gumbo_rest_client import Client, staging_url

client = api_client.Client(base_url=staging_url)
df = client.get("depmap_model_type")
```


## Usage

Read or write from the following tables:
- model
- model_condition
- omics_profile
- omics_sequencing
- screen
- screen_sequence

The client will autocommit changes after insertions or updates.

```
from gumbo_rest_client import Client

client = api_client.Client()

# to read
df = client.get("table_name")

# to create new rows and/or update existing rows, modify the dataframe and then run:
client.update("table_name", df)

# to update the table to exactly match the dataframe, run:
client.update("table_name", df, delete_missing_rows=True)

# to only update existing rows:
client.update_only("table_name", df) # throws an exception if a given row doesn't already exist

# to only insert new rows:
client.insert_only("table_name", new_rows_df) # throws an exception if a given row already exists

# finally, close the database connection
client.close()
```

## Running tests

The codebase is organized into a few different python modules, some of which
have dependencies on one another.

After checking out repo, make sure to run `./all_install.sh` to create a
poetry environment for each one.

In the parent run `./all_tests.sh` to run all tests. Alternatively you can
`cd` to a particular module and run the tests for that module via `poetry
run pytest`.


## publishing package to internal package repo

There's nothing in this repo that is private or secret, however, this is an internal
tool, so it doesn't seem like it should be published on Pypi. Instead we can publish to an internal package index. 

To setup for publishing (Based on https://medium.com/google-cloud/python-packages-via-gcps-artifact-registry-ce1714f8e7c1 )

```
poetry self add keyrings.google-artifactregistry-auth
poetry config repositories.public-python https://us-central1-python.pkg.dev/cds-artifacts/public-python/                                                      
# also make sure you've authentication via "gcloud auth login" if you haven't already
```

And then you can publish via:

```
poetry publish --build --repository public-python
```

