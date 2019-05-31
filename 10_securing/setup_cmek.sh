#!/bin/bash

SVC=$(bq show --encryption_service_account)
gcloud kms keys add-iam-policy-binding \
  --project=[KMS_PROJECT_ID] \
  --member serviceAccount:$SVC
  --role roles/cloudkms.cryptoKeyEncrypterDecrypter \
  --location=US \
  --keyring=customers \
  cust_xyz
