#!/bin/bash

# Mismas variables que en 01_setup_svc_acct.sh y 03_call_cr.sh
JOB=monthlyupdate
SERVICE=ingest-flights-monthly
PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-cf-staging
SVC_ACCT=svc-monthly-ingest
SVC_EMAIL=${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com

SVC_URL=$(gcloud run services describe $SERVICE --format 'value(status.url)')
echo $SVC_URL
echo $SVC_EMAIL

# Observe que no hay año, ni mes. En ese caso, el servicio busca el mes siguiente.
echo {\"bucket\":\"${BUCKET}\"\} > /tmp/message
cat /tmp/message

gcloud scheduler jobs create http $JOB \
  --description "Ingest flights using Cloud Run" \
  --schedule="0 10 8 * *" --time-zone "America/Santiago" \
  --uri=$SVC_URL --http-method POST \
  --oidc-service-account-email $SVC_EMAIL \
  --oidc-token-audience=$SVC_URL \
  --max-backoff=7d \
  --max-retry-attempts=5 \
  --max-retry-duration=2d \
  --min-backoff=12h \
  --headers="Content-Type=application/json" \
  --message-body-from-file=/tmp/message


# Para probar este script y ejecutar la tarea debemos darnos la posibilidad de suplantar la cuenta de servicio.
# gcloud scheduler jobs run $JOB --impersonate-service-account

