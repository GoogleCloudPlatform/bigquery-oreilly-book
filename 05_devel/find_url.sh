#!/bin/bash

IMAGE=--image-family=tf-latest-cpu
INSTANCE_NAME=dlvm
MAIL=vlakshmanan@google.com  # CHANGE THIS

echo "Looking for Jupyter URL on $INSTANCE_NAME"
while true; do
   proxy=$(gcloud compute instances describe ${INSTANCE_NAME} 2> /dev/null | grep dot-datalab-vm)
   if [ -z "$proxy" ]
   then
      echo -n "."
      sleep 1
   else
      echo "done!"
      echo "$proxy"
      break
   fi
done
