#!/bin/sh


# Crea la tabla flights_sample desde una fuente local


bq --location=southamerica-west1 load \
            --source_format=NEWLINE_DELIMITED_JSON \
            dsongcp.flights_sample ./flights_sample_2024.json ./flights_schema.json