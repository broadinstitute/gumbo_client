# Gumbo Python Client

A python package for reading/writing to the gumbo database


## Installations

*Note: the client has been switched to the v2 of the client which works very
differently. The installation instructions below have changed*

If using poetry, add the following to your pyproject.toml :

```
gumbo-rest-client = { git = "ssh://git@github.com:broadinstitute/gumbo_client.git", subdirectory = "gumbo-rest-client" }
```

and run then run `poetry lock --no-update`

Otherwise, if you don't use poetry, install via pip

```
pip install git+https://git@github.com:broadinstitute/gumbo_client.git#egg=gumbo-rest-client&subdirectory=gumbo-rest-client
```

This repo now only contains the new version of the gumbo client which no
longer makes direct connections to the database but instead uses a hosted
service. From the users perspective this just means:

1. Simpler setup (no need to download the proxy, and debug differing versions)
2. No need for the client to launch a separate task in the background
3. Fewer connection errors 

To setup the connection to the API, simply run:
```
gcloud secrets versions access latest --secret='client-iap-auth-sa-json' --project depmap-gumbo > ~/.config/gumbo/client-iap-auth-sa.json
gcloud config get account > ~/.config/gumbo/username
```
And you should be ready to go!

To use the new client, import `Client` instead of `gumbo_client` and then use it as you normally would to read tables. For example:
```
from gumbo_client import Client

client = api_client.Client()
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
import gumbo_client

client = gumbo_client.Client(config_dir="~/.config/gumbo", username="firstInitialLastName")

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


