#!/bin/bash

SVC_ACCT=svc-monthly-ingest
PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-cf-staging
REGION=southamerica-west1
SVC_PRINCIPAL=serviceAccount:${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com

gsutil ls gs://$BUCKET || gsutil mb -l $REGION gs://$BUCKET

gsutil uniformbucketlevelaccess set on gs://$BUCKET

gcloud iam service-accounts create $SVC_ACCT --display-name "flights monthly ingest"

# hacer que la cuenta de servicio sea el administrador del bucket para que
# pueda leer/escribir/listar/borrar, etc. sólo en este bucket.
gsutil iam ch ${SVC_PRINCIPAL}:roles/storage.admin gs://$BUCKET

# posibilidad de crear/eliminar particiones, etc. en la tabla BigQuery
bq --project_id=${PROJECT_ID} query --nouse_legacy_sql \
    "GRANT \`roles/bigquery.dataOwner\` ON SCHEMA dsongcp TO '$SVC_PRINCIPAL' "

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member ${SVC_PRINCIPAL} \
    --role roles/bigquery.jobUser

# En este punto, prueba ejecutando como cuenta de servicio
#gcloud iam service-accounts keys create tempkey.json --iam-account=$SVC_ACCT@$PROJECT_ID.iam.gserviceaccount.com --project_id=$PROJECT_ID
# añade esto a .gcloudignore y .gitignore o ponlo en un directorio diferente
# gcloud auth activate-service-account --key-file tempkey.json
# ./ingest_flights.py --bucket $BUCKET --year 2015 --month 03 --debug
# después de esto, vuelve a ser tu mismo con gcloud auth login

# Asegúrese de que la cuenta de servicio puede invocar funciones de la nube
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member ${SVC_PRINCIPAL} --role roles/run.invoker
