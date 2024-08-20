#!/bin/sh

# Crea la tabla airports desde una fuente local
# CSVFILE = "/home/inspired/data-science-on-gcp/04_streaming/design/airports_2024.csv"
# SCHEMA = "/home/inspired/data-science-on-gcp/04_streaming/design/airports_schema.json"
# PROJECT = $(gcloud config get project)

bq --location=southamerica-west1 load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    dsongcp.airports ./airports_2024.csv ./airport_schema.json
