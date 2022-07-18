Client for reading/writing to gumbo DB

# Installation

Create a directory creating the connection information and keys:

```
mkdir -p ~/.config/gumbo
gcloud secrets versions access latest --secret='gumbo-client-config' --project depmap-gumbo > ~/.config/gumbo/config.json
```

Install the package via:

```
pip install .
```

And you should be good to go.

# Connecting to the Database

Connect using one of two options:
1. Download the SSL keys from Google Secrets Manager
2. Download and use CloudSQL Proxy to authenticate through Google OAuth


## CloudSQL Proxy

Locally connecting to CloudSQL databases requires use of the cloud_sql_proxy. On Mac, you can do the intial setup with the following commands ([instructions for other OSs here](https://cloud.google.com/sql/docs/mysql/sql-proxy#install)):

```
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.darwin.amd64
chmod +x cloud_sql_proxy
gcloud auth login
```
Then run the proxy: 
```
./cloud_sql_proxy -instances=depmap-gumbo:us-central1:gumbo-cloudsql=tcp:5432
```

Leave the proxy running while using the python client. You should now be able to connect to the database as if it were running locally. 

Once finished, close the proxy by keyboard interupting the terminal process. If the process has become orphaned, it can be terminated with the following:
```
lsof -i :5432 # check what process ID is associated with this port
kill <process-id-copied-from-output-above> # terminate the process
```


# Usage

```
import gumbo_client

client = gumbo_client.Client()

# to read
df = client.get("table_name")
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

# Other Notes

When importing this client into a Jupyter notebook, 

