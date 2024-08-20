#!/bin/bash

# Crea o agrega datos a la tabla flights_tzcorr desde una fuente local.

FOLDER="/home/inspired/data-science-on-gcp/04_streaming/transform/files"
SOURCE_DATA_FOLDER="${FOLDER}/data/tzcorr"

# Global flags.
LOCATION="southamerica-west1"
PROJECT="bigquery-manu-407202"
# Local flags and arguments.
DESTINATION_TABLE="dsongcp.flights_tzcorr"
FORMAT="NEWLINE_DELIMITED_JSON"
SCHEMA="${FOLDER}/flights_tzcorr_schema.json"

# Bucle para cargar todos los archivos que coinciden con el patrón
for file in "${SOURCE_DATA_FOLDER}/all_flights_000"[0-9][0-9]"-of-00023"; do
    if [ -f "$file" ]; then
        echo "Cargando archivo: $file"
    fi
done
