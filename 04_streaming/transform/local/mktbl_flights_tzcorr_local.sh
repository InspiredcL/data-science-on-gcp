#!/bin/bash

# Crea o agrega datos a la tabla flights_tzcorr desde una fuente local.

# Borra la tabla existente (No usar --replace en carga local)
#bq --project_id $PROJECT rm -f ${PROJECT}:dsongcp.flights_tzcorr

##############################################################################
############################## Toda la carpeta ###############################
##############################################################################

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
        bq --location=${LOCATION} --project_id=${PROJECT} load \
            --source_format=${FORMAT} ${DESTINATION_TABLE} ${file} ${SCHEMA}
    fi
done
bq show dsongcp.flights_tzcorr

##############################################################################
############################## Solo un archivo ###############################
##############################################################################

# FOLDER="/home/inspired/data-science-on-gcp/04_streaming/transform/files"
# SOURCE_DATA_FOLDER="${FOLDER}/data/tzcorr"

# # Global flags.
# LOCATION="southamerica-west1"
# PROJECT="bigquery-manu-407202"
# # Local flags and arguments.
# DESTINATION_TABLE="dsongcp.flights_tzcorr"
# FORMAT="NEWLINE_DELIMITED_JSON"
# SOURCE_DATA="${SOURCE_DATA_FOLDER}/all_flights_00000-of-00023"
# SCHEMA="${FOLDER}/flights_tzcorr_schema.json"

# bq --location=${LOCATION} --project_id=${PROJECT} load \
#     --source_format=${FORMAT} ${DESTINATION_TABLE} ${SOURCE_DATA} ${SCHEMA}
# bq show dsongcp.flights_tzcorr