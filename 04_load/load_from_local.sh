#!/bin/bash

LOC="--location US"

bq $LOC mk ch03

zless ./college_scorecard.csv.gz | sed 's/PrivacySuppressed/NULL/g' | gzip > /tmp/college_scorecard.csv.gz

#SCHEMA="--autodetect"
SCHEMA="--schema=schema.json --skip_leading_rows=1"

bq $LOC \
   load --null_marker=NULL --replace \
   --source_format=CSV $SCHEMA \
   ch03.college_scorecard \
   /tmp/college_scorecard.csv.gz

#   ./college_scorecard.csv.gz \
