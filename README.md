Client for reading/writing to gumbo DB

# Installation

First you will need rights in the depmap-gumbo google cloud project. For any user to
access gumbo, that user account will need: 

- Cloud SQL Client
- Secret Manager Secret Accessor

They will then need to create a directory creating the connection information and keys:

```
mkdir -p ~/.config/gumbo
gcloud secrets versions access latest --secret='gumbo-client-config' --project depmap-gumbo > ~/.config/gumbo/config.json
```

Install the package via:

```
pip install .
```

And you should be good to go.

# Usage

```
import gumbo_client

client = gumbo_client.Client()

# to read
df = client.get("table_name)
# to write
client.update("table_name", df)
# after all writes are done call commit to make those changes permanent
client.commit()
```

# Setting up for development

```
sh install_prereqs.sh
```

# Running tests

```
pytest
```

