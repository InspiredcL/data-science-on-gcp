#!/bin/bash

# Copia desde el bucket del curso los archivos fragmentados (27 partes) ya tratados on respecto a la zona de tiempo

if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest_from_crsbucket.sh  destination-bucket-name"
    exit
fi

# Asigna el primer parámetro a la variable BUCKET y define las rutas de origen y destino

BUCKET=$1
FROM=gs://data-science-on-gcp/edition2/flights/tzcorr
TO=gs://$BUCKET/flights/tzcorr

# Archivo fragmentados

CMD="gsutil -m cp "
for SHARD in $(seq -w 0 26); do
    CMD="$CMD ${FROM}/all_flights-000${SHARD}-of-00026"
done
CMD="$CMD $TO"
echo $CMD
$CMD

# Cargar los archivos tzcorr (zona horaria corregida) a  BigQuery

PROJECT=$(gcloud config get-value project)
bq --project_id $PROJECT \
    load --source_format=NEWLINE_DELIMITED_JSON --autodetect ${PROJECT}:dsongcp.flights_tzcorr \
    ${TO}/all_flights-*

# Crea una tabla con el archivo airports.csv
cd transform
./stage_airports_file.sh ${BUCKET}
