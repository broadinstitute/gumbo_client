#!/bin/bash
set -ex

( cd dataframe-json-packing && poetry install )
( cd gumbo-dao && poetry install )
( cd gumbo-rest-client/ && poetry install )
( cd gumbo-rest-service && poetry install )

