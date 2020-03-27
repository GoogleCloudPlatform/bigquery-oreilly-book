#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "Usage: deploy.sh gcspath region model_name model_version"
    echo "       Make sure region is supported: https://cloud.google.com/ai-platform/prediction/docs/regions"
    exit
fi

PROJECT=$(gcloud config get-value project)
EXPORT_PATH=$1
REGION=$2
MODEL_NAME=$3
VERSION_NAME=$4

BUCKET=$(echo $EXPORT_PATH |  sed 's/\// /g' | awk '{print $2}')

if [[ $(gcloud ai-platform models list --format='value(name)' | grep $MODEL_NAME) ]]; then
    echo "$MODEL_NAME already exists"
else
    # create model
    echo "Creating $MODEL_NAME"
    gcloud ai-platform models create --regions=$REGION $MODEL_NAME
fi

if [[ $(gcloud ai-platform versions list --model $MODEL_NAME --format='value(name)' | grep $VERSION_NAME) ]]; then
    echo "Deleting already existing $MODEL_NAME:$VERSION_NAME ... "
    gcloud ai-platform versions delete --model=$MODEL_NAME $VERSION_NAME
    echo "Please run this cell again if you don't see a Creating message ... "
    sleep 10
fi

# create model
echo "Creating $MODEL_NAME:$VERSION_NAME"
gcloud ai-platform versions create --model=$MODEL_NAME $VERSION_NAME --async \
       --framework=tensorflow --python-version=3.7 --runtime-version=2.1 \
       --origin=$EXPORT_PATH --staging-bucket=gs://$BUCKET

echo "Monitor model creation at https://console.cloud.google.com/ai-platform/models/$MODEL_NAME/versions"