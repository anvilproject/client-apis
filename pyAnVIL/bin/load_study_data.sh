#!/bin/bash
# Enable IG on datastore


unset MISSING
[ -z "$GOOGLE_LOCATION" ] && echo "missing env var: GOOGLE_LOCATION" && MISSING="Y"
[ -z "$GOOGLE_DATASET" ] && echo "missing env var: GOOGLE_DATASET" && MISSING="Y"
[ ! -z "$MISSING" ] &&  echo "please set required env variables" && exit


[ -z "$1" ] &&  echo "please provide data_store parameter" && exit
[ -z "$2" ] &&  echo "please provide project data gs:// directory url parameter" && exit


# load study level data
declare -a resourceTypes=("Practitioner" "Organization" "ResearchStudy" "ResearchStudyObservation")
for resourceType in "${resourceTypes[@]}"
do
echo $1:$resourceType
gcloud healthcare fhir-stores import gcs $1 \
    --location=$GOOGLE_LOCATION --dataset=$GOOGLE_DATASET \
    --gcs-uri=$2/$resourceType.json \
    --content-structure=resource-pretty --async
done
