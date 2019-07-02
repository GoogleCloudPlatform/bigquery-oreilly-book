#!/bin/bash
BUCKET=cloud-training-demos-ml  # CHANGE

gsutil cp *.ipynb *.py gs://$BUCKET/notebooks/jupyter
