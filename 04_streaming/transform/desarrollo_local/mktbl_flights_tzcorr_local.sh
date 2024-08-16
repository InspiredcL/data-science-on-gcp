#!/bin/sh

# Crea o agrega datos a la tabla flights_tzcorr desde una fuente local.

FOLDER="/home/inspired/data-science-on-gcp/04_streaming/transform/files"
SOURCE_DATA_FOLDER="${FOLDER}/data/tzcorr"

# Global flags.
LOCATION="southamerica-west1"
PROJECT="bigquery-manu-407202"
# Local flags and arguments.
DESTINATION_TABLE="dsongcp.flights_tzcorr"
FORMAT="NEWLINE_DELIMITED_JSON"
SOURCE_DATA="${SOURCE_DATA_FOLDER}/all_flights_00000-of-00023"
SCHEMA="${FOLDER}/flights_tzcorr_schema.json"

bq --location=${LOCATION} --project_id=${PROJECT} load \
    --source_format=${FORMAT} ${DESTINATION_TABLE} ${SOURCE_DATA} ${SCHEMA}
