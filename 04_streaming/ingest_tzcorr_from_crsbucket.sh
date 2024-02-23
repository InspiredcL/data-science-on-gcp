#!/bin/bash

#######################################
## Obs los archivos son del año 2015 ##
#######################################

# Copia desde el bucket del curso los archivos fragmentados (26 partes) ya tratados on respecto a la zona de tiempo

if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest_tzcorr_from_crsbucket.sh  destination-bucket-name"
    exit
fi

# Asigna el primer parámetro a la variable BUCKET y define las rutas de origen y destino

BUCKET=$1
FROM=gs://data-science-on-gcp/edition2/flights/tzcorr

#####################
## Hacia el Bucket ##
#####################

TO=gs://$BUCKET/flights/tzcorr

# Archivos fragmentados

CMD="gsutil -m cp "
for SHARD in $(seq -w 0 25); do
    CMD="$CMD ${FROM}/all_flights-000${SHARD}-of-00026"
done
CMD="$CMD $TO"
echo $CMD
$CMD

# Cargar los archivos tzcorr (zona horaria corregida) a  BigQuery

bq --project_id $PROJECT \
    load --source_format=NEWLINE_DELIMITED_JSON --autodetect ${PROJECT}:dsongcp.flights_tzcorr \
    ${TO}/all_flights-*

# Crea una tabla con el archivo airports.csv
cd transform
./stage_airports_file.sh ${BUCKET}


##############################################################################

####################
## Carpeta local ##
###################

# Copia los archivos con los nombres asignados hacia la carpeta local
# TO=/home/inspired/data-science-on-gcp/04_streaming/design/tzcorr
# mkdir -p "$TO"
# for SHARD in $(seq -w 0 25); do
#     SOURCE_FILE="${FROM}/all_flights-000${SHARD}-of-00026"
#     DEST_FILE="${TO}/all_flights-000${SHARD}-of-00026"
#     gsutil -m cp "$SOURCE_FILE" "$DEST_FILE"

# done

# Cargar los archivos tzcorr (zona horaria corregida) a  BigQuery

# PROJECT=$(gcloud config get-value project)
# for SHARD in $(seq -w 0 25); do
#     FILE_PATH="${TO}/all_flights-000${SHARD}-of-00026"
#     bq --project_id $PROJECT load --source_format=NEWLINE_DELIMITED_JSON --autodetect ${PROJECT}:dsongcp.flights_tzcorr "$FILE_PATH"
# done
##############################################################################