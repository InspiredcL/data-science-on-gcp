#!/bin/bash

# Copia el archivo "airports.csv.gz" a un directorio del bucket ingresado, luego crea  la tabla "dsongcp.airports"
if test "$#" -ne 1; then
   echo "Usage: ./stage_airports_file.sh bucket-name"
   echo "   eg: ./stage_airports_file.sh cloud-training-demos-ml"
   exit
fi

BUCKET=$1
PROJECT=$(gcloud config get project)

gsutil cp airports.csv.gz gs://${BUCKET}/flights/airports/airports.csv.gz

bq --project_id=$PROJECT load \
   --autodetect --replace --source_format=CSV \
   dsongcp.airports gs://${BUCKET}/flights/airports/airports.csv.gz
