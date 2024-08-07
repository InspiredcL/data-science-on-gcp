#!/bin/bash

# Copiar los archivos del año 2015 y Enero 2016 desde el bucket del curso
if [ "$#" -ne 1 ]; then
    echo "Uso: ./ingest_from_crsbucket.sh  nombre-del-bucket-destino"
    echo " ej: ./ingest_from_crsbucket.sh  gs://ds-on-gcp-394717-dsongcp"
    exit
fi

# Asignar el nombre del bucket proporcionado como argumento
BUCKET=$1

# Definir las rutas de origen y destino
FROM=gs://data-science-on-gcp/edition2/flights/raw
TO=gs://$BUCKET/flights/raw

# Copiar archivos CSV utilizando gcloud storage para el año 2015 y enero de 2016
for MONTH in $(seq -w 1 12); do
    gcloud storage cp ${FROM}/2015${MONTH}.csv $TO
    # gsutil cp ${FROM}/2015${MONTH}.csv $TO
done

# Copiar el archivo de enero de 2016
gcloud storage cp ${FROM}/201601.csv $TO
# gsutil cp ${FROM}/201601.csv $TO

# Imprimir mensaje de ejecución
echo "Archivos copiados exitosamente a gs://$BUCKET/flights/raw"
