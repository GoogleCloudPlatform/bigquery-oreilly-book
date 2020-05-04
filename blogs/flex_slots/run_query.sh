#!/usr/bin/env bash -x

PROJECT=$(gcloud config get-value project)
LOCATION=US
SLOTS=500


RESERVATION=reservation$$
POOLNAME=slots$$
echo $RESERVATION

# create reservation
bq mk --reservation --project_id=${PROJECT} --location=${LOCATION}  ${RESERVATION}
# buy slots
bq mk --slots=${SLOTS} --plan=ADHOC ${POOLNAME}
# allow our project to use reservation
ASSIGNMENT=$(bq mk --reservation_assignment --reservation_id=${PROJECT}:${LOCATION}.${RESERVATION} \
             --job_type=QUERY --assignee_type=PROJECT --assignee_id=${PROJECT} \
             | tail -2 | head -1 | awk '{print $1}')
echo $ASSIGNMENT


# remove reservation (removes everything)
bq rm --reservation_assignment --project_id=${PROJECT} --location=${LOCATION}  $ASSIGNMENT
bq rm --slots=${SLOTS} --plan=ADHOC ${POOLNAME}
bq rm --reservation --project_id=${PROJECT} --location=${LOCATION}  ${RESERVATION}