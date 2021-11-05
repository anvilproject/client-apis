#!/bin/bash
# Enable IG on datastore


unset MISSING
[ -z "$GOOGLE_LOCATION" ] && echo "missing env var: GOOGLE_LOCATION" && MISSING="Y"
[ -z "$GOOGLE_DATASET" ] && echo "missing env var: GOOGLE_DATASET" && MISSING="Y"
[ ! -z "$MISSING" ] &&  echo "please set required env variables" && exit


[ -z "$1" ] &&  echo "please provide data_store parameter" && exit
[ -z "$2" ] &&  echo "please provide project data gs:// directory url parameter" && exit


declare -a subdirs=("public" "protected")
for subdir in "${subdirs[@]}"
do
    echo Attempting to load json...
    echo gcloud healthcare fhir-stores import gcs $1 \
        --location=$GOOGLE_LOCATION --dataset=$GOOGLE_DATASET \
        --gcs-uri=$2/$subdir/*.json \
        --content-structure=resource --async
    gcloud healthcare fhir-stores import gcs $1 \
        --location=$GOOGLE_LOCATION --dataset=$GOOGLE_DATASET \
        --gcs-uri=$2/$subdir/*.json \
        --content-structure=resource --async

done
