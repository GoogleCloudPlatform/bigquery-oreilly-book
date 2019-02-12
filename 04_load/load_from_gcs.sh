#!/bin/bash

LOC="--location US"

#bq $LOC rm -r ch04
bq $LOC mk ch04

bq $LOC \
   load --null_marker=NULL --replace \
   --source_format=CSV --autodetect \
   ch04.college_scorecard \
   gs://cloud-training-demos/tmp/college_scorecard.csv.gz

#   ./college_scorecard.csv.gz \
