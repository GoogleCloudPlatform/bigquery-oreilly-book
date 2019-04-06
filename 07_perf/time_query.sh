#!/bin/bash

# Copyright 2019 Google LLC.
# SPDX-License-Identifier: Apache-2.0


NUM_TIMES=10

read -d '' QUERY_TEXT << EOF
SELECT 
  start_station_name
  , AVG(duration) as duration
  , COUNT(duration) as num_trips
FROM \`bigquery-public-data\`.london_bicycles.cycle_hire
GROUP BY start_station_name 
ORDER BY num_trips DESC 
LIMIT 5
EOF

read -d '' request_nocache << EOF
{
 "useLegacySql": false,
 "useQueryCache": false,
 "query": \"${QUERY_TEXT}\"
}
EOF
request_nocache=$(echo "$request_nocache" | tr '\n' ' ')
read -d '' request_cache << EOF
{
 "useLegacySql": false,
 "useQueryCache": true,
 "query": \"${QUERY_TEXT}\"
}
EOF
request_cache=$(echo "$request_cache" | tr '\n' ' ')


access_token=$(gcloud auth application-default print-access-token)
PROJECT=$(gcloud config get-value project)

echo $request_nocache
echo $access_token

echo "Running query repeatedly; please divide reported times by $NUM_TIMES"

for request in "$request_nocache" "$request_cache"; do

echo "$request"
time for i in $(seq 1 $NUM_TIMES); do
echo -en "\r ... $i / $NUM_NUMTIMES ..."
curl --silent \
    -H "Authorization: Bearer $access_token"  \
    -H "Content-Type: application/json" \
    -X POST \
    -d "$request" \
    "https://www.googleapis.com/bigquery/v2/projects/$PROJECT/queries" > /dev/null
done

done

