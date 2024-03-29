# Gumbo REST API

An API which handles requests from the v2 gumbo client.

This API currently only supports GET requests for gumbo table data, but will be expanded over time to support more operations.

## Running the API locally
1. Define a `.env` file with the connection string.
2. Run the proxy in the background.
3. Install requirements:

    `poetry install`

4. Run the app:

    `poetry run uvicorn gumbo_rest_service.main:app --reload`

# running tests

Execute: 

```
pytest
```

# Deploying changes

There is a deploy script written in python. Run:

```
./deploy ENV
```

where ENV is either `prod` or `staging`.

