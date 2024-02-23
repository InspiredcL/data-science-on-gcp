#!/bin/sh


# bq mk --external_table_definition=./airport_schema.json@CSV=gs://data-science-on-gcp/edition2/raw/airports.csv dsongcp.airports_gcs


# Agregar hacia atrás el zip y podría ser la descarga también
# (cuando sepas como se hace)
CSVFILE = \
"/home/inspired/data-science-on-gcp/04_streaming/design/T_MASTER_CORD.csv"
SCHEMA = \
"home/inspired/data-science-on-gcp/04_streaming/design/airports_schema"
PROJECT = $(gcloud config get project)

bq --project_id=$PROJECT --location=southamerica-west1 load \
            --source_format=CSV \
            --skip_leading_rows=1 \
            ${PROJECT}:dsongcp.airports $CSVFILE $SCHEMA