#!/usr/bin/env bash
set -ex
if [ ! -x /usr/local/bin/cloud_sql_proxy ] ; then 
    cat <<EOF
Missing /usr/local/bin/cloud_sql_proxy.
Install by executing:

  curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.darwin.amd64
  chmod +x cloud_sql_proxy
  sudo mv cloud_sql_proxy /usr/local/bin

Aborting
EOF
  exit 1
fi
pip install -e .
pip install -r dev-requirements.txt
pre-commit install --hook-type pre-commit
