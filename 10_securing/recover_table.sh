#!/bin/bash

# delete the restored_cycle_stations table
bq rm ch10eu.restored_cycle_stations

NOW=$(date +%s)
SNAPSHOT=$(echo "($NOW - 120)*1000" | bc)  # now minus 120 seconds in msec
bq --location=EU cp ch10eu.restored_cycle_stations@$SNAPSHOT ch10eu.restored_table
