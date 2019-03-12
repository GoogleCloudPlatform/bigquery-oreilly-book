#!/bin/bash

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

bq query --use_legacy_sql=false $QUERY_TEXT
