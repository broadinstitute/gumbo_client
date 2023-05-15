# Gumbo Python Client

A python package for reading/writing to the gumbo database


## Installations

Install the package via:

```
pip install git+https://github.com/broadinstitute/gumbo_client.git
```

## Get Permission to Access the Database

Ask Sarah Wessel (or one of the developers) for the following permissions in the depmap-gumbo google cloud project:

- Secret Manager Secret Accessor


### Authenticating with your Google Credentials

You will need to make sure you have the `gcloud` cli tool installed and authenticated with your broad google account (more info [here](https://cloud.google.com/sql/docs/mysql/connect-auth-proxy#credentials-from-an-authenticated-gcloud-cli-client.)).



#### Read connection secrets from Google Secrets Manager:

The secrets and configs you use will depend on the type access you want (you can configure multiple and switch between):


- For read and write access to the production database:
    ```
    mkdir -p ~/.config/gumbo
    gcloud secrets versions access latest --secret='client-iap-client-id' --project depmap-gumbo > ~/.config/gumbo/iap_client_id.txt
    gcloud secrets versions access latest --secret='client-iap-auth-sa-json' --project depmap-gumbo > ~/.config/gumbo/client-iap-auth-sa.json
    ```

- For read-only access to the production database:
    ```
    ...
    ```

- For read and write access to the staging database:
    ```
    ...
    ```

#### Then specify your config file location when you initialize the client:

```
client = gumbo_client.Client(username="firstInitialLastName")
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
