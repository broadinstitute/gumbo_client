#!/usr/bin/env bash
PROXY_PATH=/usr/local/bin/cloud-sql-proxy-v2.6.0
set -ex
if [ ! -x $PROXY_PATH ] ; then 
    cat <<EOF

Missing /usr/local/bin/cloud_sql_proxy.
Install by executing:

  curl 'https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.6.0/cloud-sql-proxy.darwin.amd64' --output cloud-sql-proxy-v2.6.0
  chmod +x cloud-sql-proxy-v2.6.0
  sudo mv cloud-sql-proxy-v2.6.0 $PROXY_PATH

Aborting
EOF
  exit 1
fi
pre-commit install --hook-type pre-commit
