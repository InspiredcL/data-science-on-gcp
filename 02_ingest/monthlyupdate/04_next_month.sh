#!/bin/bash

# Mismos variables como en 02_deploy_cr.sh
NAME=ingest-flights-monthly
PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-cf-staging

#Obtenemos la URL
URL=$(gcloud run services describe ingest-flights-monthly \
--format 'value(status.url)')
echo $URL

# Mes siguiente
echo "Getting month that follows ... (removing 12 if needed, so there is something to get) "
gsutil rm -rf gs://$BUCKET/flights/raw/201512.csv.gz
gsutil ls gs://$BUCKET/flights/raw

# Creamos el mensaje, solamente con el bucket para obtener el mes siguiente
echo {\"bucket\":\"${BUCKET}\"\} > /tmp/message
cat /tmp/message

# Hacemos la solicitud
curl -k -X POST $URL \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message

echo "Done"
