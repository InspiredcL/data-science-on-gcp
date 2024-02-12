#!/bin/bash

# variables similares a las de 01_setup_svc_acct.sh
SERVICE=ingest-flights-monthly
SVC_ACCT=svc-monthly-ingest
PROJECT_ID=$(gcloud config get-value project)
REGION=southamerica-west1
SVC_EMAIL=${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com

gcloud run deploy $SERVICE --region $REGION --source=$(pwd) \
    --platform=managed --service-account ${SVC_EMAIL} \
    --no-allow-unauthenticated \
    --timeout 12m


#gcloud functions deploy $URL \
#    --entry-point ingest_flights --runtime python310 --trigger-http \
#    --timeout 540s --service-account ${SVC_EMAIL} --no-allow-unauthenticated
