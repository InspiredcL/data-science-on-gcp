#!/bin/bash

# Crea la tabla "dsongcp.flights_sample" en base a "dsongcp.flights"
# y elegimos un porcentaje de 0.1%
# Obs. replace borra el sample anterior


PROJECT=$(gcloud config get project)

bq --project_id=$PROJECT query --destination_table \
    ${PROJECT}:dsongcp.flights_sample --replace --nouse_legacy_sql \
    "SELECT * FROM dsongcp.flights WHERE RAND() < 0.001"

# No podemos exportar la tabla directamente a nuestro dispositivo local

# Replicamos el comando bq extract en un archivo python para consultar
# la tabla flights_sample y guardar el contenido como un
# archivo json (delimitado por nueva linea)

./dw_flights_pd_2024.py
# o con cambiando la consulta.
# ./dw_flights_bq
