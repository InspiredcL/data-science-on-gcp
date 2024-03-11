#!/bin/sh

# Crea la tabla federada
bq mk --external_table_definition=./airport_schema.json@CSV=gs://data-science-on-gcp/edition2/raw/airports_2024.csv dsongcp.airports_gcs
