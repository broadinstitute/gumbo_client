#!/bin/bash
set -ex

( cd gumbo-dao && poetry run pytest )
( cd gumbo-rest-client/ && poetry run pytest )
( cd gumbo-rest-service && poetry run pytest )

