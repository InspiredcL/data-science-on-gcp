#!/bin/bash

# Crea la tabla "dsongcp.flights_sample" en base a "dsongcp.flights" y
# elegimos un porcentaje de 0.1%

# Guarda la tabla "dsongcp.flights_sample" en el bucket
if test "$#" -ne 1; then
   echo "Usage: ./bq_flight_sample.sh bucket-name"
   echo "   eg: ./bq_flight_sample.sh {PROJECT}-dsongcp"
   exit
fi

BUCKET=$1
PROJECT=$(gcloud config get project)

bq --project_id=$PROJECT query --destination_table \
${PROJECT}:dsongcp.flights_sample --replace --nouse_legacy_sql \
    'SELECT * FROM dsongcp.flights WHERE RAND() < 0.001'

bq --project_id=$PROJECT extract --destination_format=NEWLINE_DELIMITED_JSON \
   dsongcp.flights_sample  gs://${BUCKET}/flights/ch4/flights_sample.json

gsutil cp gs://${BUCKET}/flights/ch4/flights_sample.json .
