#!/bin/sh

# Crea la tabla flights_sample desde una fuente local
REGION="southamerica-west1"
TABLE="dsongcp.flights_tzcorr_sample"
FILE_TYPE="NEWLINE_DELIMITED_JSON"

FOLDER="/home/inspired/data-science-on-gcp/04_streaming/transform/files"
FILE="df06_local_all_flights-00000-of-00001"
SCHEMA="${FOLDER}/flights_tzcorr_schema.json"

bq --location=${REGION} load \
    --source_format=${FILE_TYPE} ${TABLE} ${FILE} ${SCHEMA}
