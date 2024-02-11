#!/bin/bash

# LLamamos a Cloud Run

# Mismos variables como en 02_deploy_cr.sh
NAME=ingest-flights-monthly
PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-cf-staging

#Obtenemos la URL
URL=$(gcloud run services describe $NAME --format 'value(status.url)')
echo $URL

# Creamos el mensaje para una fecha en particular Nov 2022
echo {\"year\":\"2022\"\,\"month\":\"11\"\,\"bucket\":\"${BUCKET}\"\} >/tmp/message

# --request, -X; --insecure, -k; --header, -H
curl -k -X POST $URL \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type:application/json" --data-binary @/tmp/message
