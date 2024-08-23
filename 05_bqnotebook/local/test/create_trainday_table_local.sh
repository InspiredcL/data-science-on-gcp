#!/bin/bash

./create_trainday.py
#cat trainday_table.txt | bq query --use_legacy_sql=False

# bq extract dsongcp.trainday gs://${BUCKET}/flights/trainday.csv
./download_trainday.py

#create_trainday_table_local