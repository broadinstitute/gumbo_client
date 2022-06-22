Client for reading/writing to gumbo DB

# Installation

Create a directory creating the connection information and keys:

```
mkdir -p ~/.config/gumbo
gcloud something something something (todo: fill this in)
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
```

# Setting up for development

```
sh install_prereqs.sh
```

# Running tests

```
pytest
```

