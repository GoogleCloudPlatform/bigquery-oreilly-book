#!/bin/bash

bq mk --transfer_config --data_source=google_cloud_storage \
--target_dataset=ch04 --display_name ch04_college_scorecard \   
--params='{"data_path_template":"gs://bigquery-oreilly-book/college_*.csv", "destination_table_name_template":"college_scorecard_dts", "file_format":"CSV", "max_bad_records":"10", "skip_leading_rows":"1", "allow_jagged_rows":"true"}'

