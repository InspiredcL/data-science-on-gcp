#!/bin/bash


if [ "$#" -ne 1 ]; then
    echo "Usage: ./upload.sh  destination-bucket-name"
    exit
fi

BUCKET=$1

# Cargamos los archivos al BUCKET
echo "Uploading to bucket $BUCKET..."
gsutil -m cp *.csv gs://$BUCKET/flights/raw/

# Cambiamos permisos usando acces control list (acl) con change (ch) de manera recursiva (-R) al grupo de usuarios (-g) allUsers con permisos de lectura (:R) del BUCKET en cuestión.

gsutil -m acl ch -R -g allUsers:R gs://$BUCKET/flights/raw
gsutil -m acl ch -R -g google.com:R gs://$BUCKET/flights/raw
