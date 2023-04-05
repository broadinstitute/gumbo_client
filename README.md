# Gumbo Python Client

A python package for reading/writing to the gumbo database


## Installations

Install the package via:

```
pip install git+https://github.com/broadinstitute/gumbo_client.git
```

Note: some Mac M1 users have trouble installing the `psycopg2` python package required by this library. We have hopefully addressed this issue by switching to a variant called `psycopg2-binary`. If you have trouble installing either of these packages, feel free to reach out to Sarah Wessel for help.

### Authenticating with your Google Credentials

If you want the client to create a database connection for you (this is the case for most users), you will also need to run the following bash commands to so that the client can use your google credentials to authenticate through the database's proxy. 

For MacOS 64-bit users:
```
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.darwin.amd64
chmod +x cloud_sql_proxy
sudo mv cloud_sql_proxy /usr/local/bin
```

For Mac M1 users: 
```
curl -o cloud_sql_proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.1.2/cloud-sql-proxy.darwin.arm64
chmod +x cloud_sql_proxy
sudo mv cloud_sql_proxy /usr/local/bin
```

You can learn more about the Cloud SQL Proxy [here](https://cloud.google.com/sql/docs/mysql/sql-proxy). 

You will also need to make sure you have the `gcloud` cli tool installed and authenticated with your broad google account (more info [here](https://cloud.google.com/sql/docs/mysql/connect-auth-proxy#credentials-from-an-authenticated-gcloud-cli-client.)).

## Get Permission to Access the Database

#### Ask Sarah Wessel (or one of the developers) for the following permissions in the depmap-gumbo google cloud project

For any user to have read and write access gumbo, that user account will need to be given: 

- Cloud SQL Client
- Secret Manager Secret Accessor

For read-only access, the user will need to be given:

- Cloud SQL Client
- Viewer access for the `gumbo-client-readonly-config` secret in Secrets Manager

#### Create a directory with the database connection information and keys:

The secrets and configs you use will depend on the type access you want (you can configure multiple and switch between):


- For read and write access to the production database:
    ```
    mkdir -p ~/.config/gumbo
    gcloud secrets versions access latest --secret='gumbo-client-config' --project depmap-gumbo > ~/.config/gumbo/config.json
    ```

- For read-only access to the production database:
    ```
    mkdir -p ~/.config/gumbo-read-only
    gcloud secrets versions access latest --secret='gumbo-client-readonly-config' --project depmap-gumbo > ~/.config/gumbo-read-only/config.json
    ```

- For read and write access to the staging database:
    ```
    mkdir -p ~/.config/gumbo-staging
    gcloud secrets versions access latest --secret='gumbo-staging-client-config' --project depmap-gumbo > ~/.config/gumbo-staging/config.json
    ```

#### Then specify your config file location when you initialize the client:

```
client = gumbo_client.Client(config_dir="~/.config/gumbo-read-only", username="firstInitialLastName")
```

And you should be good to go! :tada:


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

In the parent gumbo_client directory, run `pytest`
