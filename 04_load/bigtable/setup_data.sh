#!/bin/bash

INSTANCE=bqbook-instance

# 1. create instance
gcloud bigtable instances create $INSTANCE --cluster=bqbook-cluster --cluster-zone=us-central1-a --display-name=$INSTANCE --instance-type=DEVELOPMENT

# 2. create table and column family called sales
cbt -instance $INSTANCE createtable logs-table
cbt -instance $INSTANCE createfamily logs-table sales 
cbt -instance $INSTANCE ls logs-table

# 3. Add data to table 
cbt -instance $INSTANCE set logs-table Paris234#20180420-070312 sales:itemid=12345 sales:price=14.23 sales:qty=2
cbt -instance $INSTANCE set logs-table Paris345#20180420-070312 sales:itemid=23451 sales:price=23.24 sales:qty=3
cbt -instance $INSTANCE set logs-table Paris234#20180420-070313 sales:itemid=23451 sales:price=18.23 sales:qty=2
cbt -instance $INSTANCE set logs-table Paris345#20180420-070314 sales:itemid=34521 sales:price=21.43 sales:qty=1
cbt -instance $INSTANCE set logs-table Paris234#20180420-070315 sales:itemid=12345 sales:price=14.23 sales:qty=4

# 4. Read some data
cbt -instance $INSTANCE read logs-table count=3
