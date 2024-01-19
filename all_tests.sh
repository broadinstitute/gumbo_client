#!/bin/bash
set -ex

for package_name in dataframe-json-packing gumbo-dao gumbo-rest-client gumbo-rest-service ; do
  ( cd $package_name && \
    poetry run pyright && \
    poetry run pytest )
done

