#!/bin/bash

python3 -m pip install dbt

ACCTNAME=dbt-svc-acct
PROJECT=$(gcloud config get-value project)
echo "Creating ${ACCTNAME} in ${PROJECT}"

gcloud iam service-accounts create ${ACCTNAME} --description="DBT Service Account"
gcloud iam service-accounts keys create keyfile.json \
  --iam-account ${ACCTNAME}@${PROJECT}.iam.gserviceaccount.com

for role in roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding ${PROJECT} \
    --member serviceAccount:${ACCTNAME}@${PROJECT}.iam.gserviceaccount.com --role ${role}
done

dbt init college-scorecard-tmp  # CHANGE

workdir=$(pwd)
cd ~/.dbt
for file in profiles.yml keyfile.json; do
  rm ${file}
  ln -s ${workdir}/${file} .
done
cd ${workdir}

<< 'multiline-comment'
multiline-comment
