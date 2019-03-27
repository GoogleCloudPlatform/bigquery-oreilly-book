#!/bin/bash

PROJECT=$(gcloud config get-value project)

access_token=$(gcloud auth application-default print-access-token)

curl -H "Authorization: Bearer $access_token"  \
    -H "Content-Type: application/json" \
    -X GET "https://www.googleapis.com/bigquery/v2/projects/$PROJECT/datasets/ch04/tables"

