# Gumbo Client API

An API which handles requests from the v2 gumbo client.

This API currently only supports GET requests for gumbo table data, but will be expanded over time to support more operations.

## Running the API locally

```
pip install -r requirements.txt
python main.py runserver
```

# Deploying changes
```
# read the App Engine deployment configs from Secerets Manager
gcloud secrets versions access latest --secret='client-api-deployment-yaml' --project depmap-gumbo > app.yaml

# Deploy to App Engine
gcloud app deploy
``