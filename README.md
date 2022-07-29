# Gumbo Python Client

A python package for reading/writing to the gumbo database

## Installation

First you will need rights in the depmap-gumbo google cloud project. For any user to
access gumbo, that user account will need: 

- Cloud SQL Client
- Secret Manager Secret Accessor

You will then need to create a directory creating the connection information and keys for the production database:

```
mkdir -p ~/.config/gumbo
gcloud secrets versions access latest --secret='gumbo-client-config' --project depmap-gumbo > ~/.config/gumbo/config.json
```

Install the package via:

```
pip install .
```

Install prerequisites and set up environment variables necessary for creating a database connection:
```
sh install_prereqs.sh
```

And you should be good to go! :tada:

## Usage

```
import gumbo_client

client = gumbo_client.Client(username="firstInitialLastName")

# to read
df = client.get("table_name)
# to write
client.update("table_name", df)
# after all writes are done call commit to make those changes permanent
client.commit()

client.close()
```

## Connecting to the Staging Database

Download the connection information for the staging database into a new config file:
```
mkdir -p ~/.config/gumbo-staging
gcloud secrets versions access latest --secret='gumbo-staging-client-config' --project depmap-gumbo > ~/.config/gumbo-staging/config.json
```

Specify the new config file when initializing the client:
```
import gumbo_client

gumbo_client.Client(config_dir="~/.config/gumbo-staging", username="firstInitialLastName")
```

## Running tests

```
pytest
```

