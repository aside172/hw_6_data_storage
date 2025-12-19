#!/bin/bash

LOAD_DATE=$1

if [ -z "$LOAD_DATE" ]; then
  echo "Usage: ./03_dds_incremental_load.sh YYYY-MM-DD"
  exit 1
fi

sed "s/\${LOAD_DATE}/${LOAD_DATE}/g" 03_dds_incremental_load.sql | \
docker exec -i trino trino
