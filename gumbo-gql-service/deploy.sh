#!/bin/zsh

set -euo pipefail

GUMBO_ENV=$1

gcloud run deploy "gumbo-gql-service-${GUMBO_ENV}" \
	--project=depmap-gumbo \
    --image=hasura/graphql-engine:latest \
    --add-cloudsql-instances="depmap-gumbo:us-central1:gumbo-cloudsql" \
    --update-env-vars='HASURA_GRAPHQL_ENABLE_CONSOLE=true' \
    --update-secrets="HASURA_GRAPHQL_DATABASE_URL=hasura-db-connection-string-${GUMBO_ENV}:latest,HASURA_GRAPHQL_ADMIN_SECRET=hasura-admin-secret-${GUMBO_ENV}:latest" \
    --service-account="hasura-service@depmap-gumbo.iam.gserviceaccount.com" \
    --region=us-central1 \
    --cpu=1 \
    --min-instances=1 \
    --max-instances=1 \
    --memory=2048Mi \
    --port=8080 \
    --concurrency=1 \
    --ingress=all \
    --allow-unauthenticated
