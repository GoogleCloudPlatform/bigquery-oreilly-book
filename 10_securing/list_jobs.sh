#!/bin/bash

NOW=$(date +%s)
START_TIME=$(echo "($NOW - 24*60*60)*1000" | bc)  # now minus 1 day in msec

bq ls -j --min_creation_time $START_TIME
