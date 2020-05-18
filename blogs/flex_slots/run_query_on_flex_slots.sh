#!/usr/bin/env bash

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

############# CHANGE AS NEEDED
PROJECT=$(gcloud config get-value project)
LOCATION=US
SLOTS=500
run_query() {
  cat ../bqml_recommendations/train.sql | bq query --sync -nouse_legacy_sql --location=${LOCATION}
}
#############

# global variables set as these resources are created
CAPACITY=""
RESERVATION=reservation$$
ASSIGNMENT=""

# cleanup everything
cleanup() {
  echo "Trying to cleanup ... $CAPACITY $RESERVATION $ASSIGNMENT in reverse order"

  bq rm --reservation_assignment --project_id=${PROJECT} --location=${LOCATION} ${ASSIGNMENT} || true
  ASSIGNMENT=""

  bq rm --reservation --project_id=${PROJECT} --location=${LOCATION}  ${RESERVATION} || true
  RESERVATION=""

  until bq rm --location=${LOCATION}  --capacity_commitment ${CAPACITY}
  do
    echo "will try after 30 seconds to delete slots ${CAPACITY}"
    sleep 30
  done
  CAPACITY=""
}

# create slots capacity reservation with flex slots
CAPACITY=$(bq mk --project_id=${PROJECT}  --location=${LOCATION} \
                --capacity_commitment --slots=${SLOTS} --plan=FLEX \
                | tail -2 | head -1 | awk '{print $1}')
echo "Warning! If this script fails, please delete slots ${CAPACITY} from the web console"


# create reservation with the flex slots
bq mk --reservation --project_id=${PROJECT} --slots=${SLOTS} --location=${LOCATION} ${RESERVATION} || cleanup

# allow our project to use reservation
ASSIGNMENT=$((bq mk --reservation_assignment --reservation_id=${PROJECT}:${LOCATION}.${RESERVATION} \
             --job_type=QUERY --assignee_type=PROJECT --assignee_id=${PROJECT} \
             | tail -2 | head -1 | awk '{print $1}') || cleanup )
echo ${ASSIGNMENT}

# give the slots about 30 s to get setup
sleep 30

run_query || true

cleanup


