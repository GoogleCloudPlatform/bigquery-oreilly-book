#!/bin/bash

# Copyright 2019 Google LLC.
# SPDX-License-Identifier: Apache-2.0

JOBID=Pontem_BigQuery_WorkloadTester_8adbf3fd-e310-44bb-9c6e-88254958ccac   # CHANGE

access_token=$(gcloud auth application-default print-access-token)
PROJECT=$(gcloud config get-value project)

echo "$request"
curl --silent \
    -H "Authorization: Bearer $access_token"  \
    -X GET \
    "https://www.googleapis.com/bigquery/v2/projects/$PROJECT/jobs/${JOBID}"

