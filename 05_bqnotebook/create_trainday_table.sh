#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./create_trainday_table.sh  destination-bucket-name"
    exit
fi

BUCKET=$1

cat trainday_table.txt | bq query --nouse_legacy_sql

bq extract dsongcp.trainday gs://${BUCKET}/flights/trainday.csv
