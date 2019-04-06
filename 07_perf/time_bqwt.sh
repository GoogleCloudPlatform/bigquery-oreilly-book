#!/bin/bash

# Time a query with BigQuery Workload Tester
# https://github.com/GoogleCloudPlatform/pontem/tree/dev/BigQueryWorkloadTester
# Copyright 2019 Google LLC.
# SPDX-License-Identifier: Apache-2.0

BQWT_DIR=./pontem/BigQueryWorkloadTester/   # Where the Workload Tester is installed

RESOURCES_DIR=${BQWT_DIR}/src/main/resources
INDIR=${RESOURCES_DIR}/queries
OUTDIR=${PWD}/results

mkdir -p $INDIR $OUTDIR

# Input SQL and config file
cat <<EOF| tr '\n' ' ' > ${INDIR}/busystations.sql
SELECT 
  start_station_name
  , AVG(duration) as duration
  , COUNT(duration) as num_trips
FROM \`bigquery-public-data\`.london_bicycles.cycle_hire
GROUP BY start_station_name 
ORDER BY num_trips DESC 
LIMIT 5
EOF

PROJECT=$(gcloud config get-value project)
cat <<EOF>${RESOURCES_DIR}/config.yaml
concurrencyLevel: 1
isRatioBasedBenchmark: true
benchmarkRatios: [0.01, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0]
outputFileFolder: $OUTDIR
workloads:
- name: "Busy stations"
  projectId: $PROJECT
  queryFiles:
    - queries/busystations.sql
  outputFileName: busystations.json
EOF

# Run Workload Tester
cd ${BQWT_DIR}
gradle clean :BigQueryWorkloadTester:run
echo "Please look at results in ${OUTDIR}/busystations.json"
