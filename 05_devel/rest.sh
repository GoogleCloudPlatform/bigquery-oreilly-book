#!/bin/bash

PROJECT="cloud-training-demos"
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

read -d '' request << EOF
{
 "kind": "bigquery#queryRequest",
 "useLegacySql": "false",
 "location": "EU",
 "query": \"${QUERY_TEXT}\"
}
EOF

request=$(echo "$request" | tr '\n' ' ')

access_token=$(gcloud auth application-default print-access-token)

echo $request
echo $access_token

curl -H "Authorization: Bearer $access_token"  \
    -H "Content-Type: application/json" \
    -X POST \
    -d "$request" \
    "https://www.googleapis.com/bigquery/v2/projects/$PROJECT/queries"

