#!/bin/bash

# Mismas variables que en 01_setup_svc_acct.sh y 03_call_cr.sh
SERVICE=ingest-flights-monthly
PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-cf-staging
SVC_ACCT=svc-monthly-ingest
SVC_EMAIL=${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com
JOB=monthlyupdate

SVC_URL=$(gcloud run services describe $SERVICE --format 'value(status.url)')
echo $SVC_URL
echo $SVC_EMAIL

# note that there is no year or month. The service looks for next month in that case.
echo {\"bucket\":\"${BUCKET}\"\} > /tmp/message
cat /tmp/message

gcloud scheduler jobs create http $JOB \
  --description "Ingest flights using Cloud Run" \
  --schedule="8 of month 10:00" --time-zone "America/Santiago" \
  --uri=$SVC_URL --http-method POST \
  --oidc-service-account-email $SVC_EMAIL --oidc-token-audience=$SVC_URL \
  --max-backoff=7d \
  --max-retry-attempts=5 \
  --max-retry-duration=2d \
  --min-backoff=12h \
  --headers="Content-Type=application/json" \
  --message-body-from-file=/tmp/message


# To try this out, go to Console and do two things:
#    in Service Accounts, give yourself the ability to impersonate this service account (ServiceAccountUser)
#    in Cloud Scheduler, click "Run Now"
