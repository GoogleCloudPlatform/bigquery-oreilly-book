#!/bin/bash

SERVICE=bq-cloud-run
PROJECT=$(gcloud config get-value project)
CONTAINER="gcr.io/${PROJECT}/${SERVICE}"
REGION="us-central1"
PROJECT_NO=$(gcloud projects list --filter="$PROJECT" --format="value(PROJECT_NUMBER)")
SVC_ACCOUNT="${PROJECT_NO}-compute@developer.gserviceaccount.com"

#
gcloud config set run/region $REGION

# Build docker image
#gcloud builds submit --tag ${CONTAINER}

# Deploy to Cloud Run
#gcloud run deploy ${SERVICE} --image $CONTAINER --platform managed

# Create a trigger from BigQuery
gcloud beta eventarc triggers create ${SERVICE}-trigger \
  --location ${REGION} --service-account ${SVC_ACCOUNT} \
  --destination-run-service ${SERVICE}  \
  --matching-criteria type=google.cloud.audit.log.v1.written \
  --matching-criteria methodName=google.cloud.bigquery.v2.TableService.InsertTable \
  --matching-criteria serviceName=bigquery.googleapis.com 
  
#  --matching-criteria resourceName=projects/_/buckets/"$MY_GCS_BUCKET"