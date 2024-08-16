#!/bin/sh

# Crea la tabla flights_sample desde una fuente local
REGION="southamerica-west1"
TABLE="dsongcp.flights_sample"
FILE_TYPE="NEWLINE_DELIMITED_JSON"

FOLDER="/home/inspired/data-science-on-gcp/04_streaming/transform/files"
FILE="${FOLDER}/flights_sample_2024.json"
SCHEMA="${FOLDER}/flights_schema.json"

bq --location=${REGION} load \
    --source_format=${FILE_TYPE} ${TABLE} ${FILE} ${SCHEMA}
