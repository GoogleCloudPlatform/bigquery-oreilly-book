#!/bin/bash

LOC="--location US"

#bq $LOC rm -r ch03
bq $LOC mk ch03

bq $LOC \
   load --null_marker=NULL --replace \
   --source_format=CSV --autodetect \
   ch03.college_scorecard \
   gs://cloud-training-demos/tmp/college_scorecard.csv.gz

#   ./college_scorecard.csv.gz \
