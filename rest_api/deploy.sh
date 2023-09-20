#!/bin/bash

# Read deployment configs from Google Secrets Manager
gcloud secrets versions access latest --secret=client-api-deployment-yaml --project=depmap-gumbo > app.yaml
# Deploy the local version of the app
gcloud app deploy app.yaml --project=depmap-gumbo
# Delete the oldest version
gcloud app versions delete $(gcloud app versions list --service=rest-api --sort-by '~version' --format 'value(version.id)' | tail -n 1) --quiet