#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./upload.sh  destination-bucket-name"
    echo "   eg: ./upload.sh  gs://ds-on-gcp-394717-dsongcp/"
    exit
fi

BUCKET=$1

# Cargamos los archivos al BUCKET
echo "Cargando al bucket $BUCKET..."
gcloud storage cp *.csv gs://$BUCKET/flights/raw/
##### gsutil es herramienta antigua reemplazada por gcloud storage #####
# gsutil -m cp *.csv gs://$BUCKET/flights/raw/

# Cambiamos permisos ACL usando storage objects update al grupo de usuarios
# (--member) xxxx con permisos de (--role) xxxx del BUCKET en cuestión.

gcloud storage objects update gs://$BUCKET/flights/raw \
    -R --add-acl-grant=entity=AllUsers,role=READER
gcloud storage objects update gs://$BUCKET/flights/raw \
    -R --add-acl-grant=entity=domain:google.com,role=READER

##### gsutil es herramienta antigua reemplazada por gcloud storage #####
# Cambiamos permisos usando access control list (acl) con change (ch) de
# manera recursiva (-R o -r) al grupo de usuarios (-g) allUsers con permisos
# de lectura (:R) del BUCKET en cuestión.
# gsutil -m acl ch -R -g allUsers:R gs://$BUCKET/flights/raw
# gsutil -m acl ch -R -g google.com:R gs://$BUCKET/flights/raw
