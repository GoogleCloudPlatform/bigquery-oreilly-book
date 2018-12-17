#!/bin/bash

LOC="--location US"
INPUT=gs://bigquery-oreilly-book/college_scorecard.csv

SCHEMA=$(gsutil cat $INPUT | head -1 | awk -F, '{ORS=","}{for (i=1; i <= NF; i++){ print $i":STRING"; }}' | sed 's/,$//g'| cut -b 4- )

bq $LOC query --external_table_definition=cstable::${SCHEMA}@CSV=${INPUT} \
   'SELECT SUM(IF(SAT_AVG != "NULL", 1, 0))/COUNT(SAT_AVG) FROM cstable'

