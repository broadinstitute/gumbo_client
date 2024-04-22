# Gumbo GraphQL Service

This is a Cloud Run-based deployment of the [Hasura GraphQL Engine](https://hasura.io/docs/latest/index/). 

## Local setup

### Prerequisites

- Docker Compose
- PostgreSQL
- A local gumbopot database (see [Database Backups and Restoration](https://github.com/broadinstitute/gumbo-utils/tree/main/db_backups_and_restoration) for instructions)

### Databases

First, log in to your local Postgres instance as the admin user to set up the Hasura metadata database and user:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE USER hasura WITH PASSWORD 'password';

CREATE DATABASE "hasura-dev";
GRANT ALL ON DATABASE "hasura-dev" TO hasura;

GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO hasura;
GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO hasura;
```

Switch to the local copy of the Gumbo database (e.g. `gumbo-dev`) and run these statements:

```sql
GRANT ALL ON SCHEMA public TO hasura;
GRANT ALL ON ALL TABLES IN SCHEMA public TO hasura;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO hasura;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO hasura;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES to hasura;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES to hasura;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS to hasura;

GRANT ALL ON SCHEMA audit TO hasura;
GRANT ALL ON ALL TABLES IN SCHEMA audit TO hasura;
GRANT ALL ON ALL SEQUENCES IN SCHEMA audit TO hasura;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA audit TO hasura;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT ALL ON TABLES to hasura;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT ALL ON SEQUENCES to hasura;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT ALL ON FUNCTIONS to hasura;
```

Then, log in as the `hasura-dev` user, confirm you can read the tables in `gumbo-dev.public` and `gumbo-dev.audit` and run the following statement in the `hasura-dev` database:

```sql
CREATE SCHEMA IF NOT EXISTS hdb_catalog;
```

### Hasura console

After filling out `.env` based on the provided `.env.dist`, start the service with `docker compose up` and visit [http://localhost:8080](http://localhost:8080) to access the Hasura console. The password is whatever you set as the `HASURA_GRAPHQL_ADMIN_SECRET` variable.

It's recommended to enable all of the feature flags in Settings. Among other things, they enable UI improvements on the Data tab.

### Using the API

Add a Postgres connection via the environment variable `PG_DATABASE_URL`, which maps to your localhost `gumbo-dev` database. Any tables, views, foreign keys, and functions that you choose to track on the Data tab will be available to query on the API tab.

One of these items should be the `public / set_username` function, which sets the `my.username` Postgres runtime setting and is required for running mutations. To perform a mutation, the first block must always be a call to this function, e.g.:

```graphql
mutation MyMutation {
  set_username(args: {_username: "yourname"}) {
    username
  }
  update_model_condition_by_pk(pk_columns: {model_condition_id: "MC-000001-ABCD"}, _set: {comments: "foo"}) {
    model_condition_id
  }
}
```

## Remote deployments

The service is deployed in Gumbo staging and production environments as a Cloud Run instance. Run `./deploy.sh {staging|prod}` to redeploy an instance.
