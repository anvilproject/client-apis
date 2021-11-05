#!/bin/bash
# Pre-requisites for Google Healthcare API FHIR server
# exports variables used to contruct URIs and query data

unset MISSING


[ -z "$1" ] && echo "missing: GOOGLE_PROJECT_NAME the name of the project that will host the FHIR service" && MISSING="Y"
[ ! -z "$MISSING" ] &&  echo "usage: fhir_env GOOGLE_PROJECT_NAME [GOOGLE_DATASET] [BILLING_ACCOUNT] [CLIENT_SECRET] [SPREADSHEET_UUID]" && exit 1

export GOOGLE_PROJECT_NAME=$1

if [ -z "$GOOGLE_DATASET" ]; then
    export GOOGLE_DATASET=${2:-anvil-test}
fi

if [ -z "$GOOGLE_BILLING_ACCOUNT" ]; then
    # NIH-Awd.NHGRI.JHU.AnVIL
    export GOOGLE_BILLING_ACCOUNT=${3:-016D13-3B2B1C-E919A9}
fi

if [ -z "$CLIENT_SECRET" ]; then
    export CLIENT_SECRET=${4:-./client_secret.json}
fi

if [ -z "$SPREADSHEET_UUID" ]; then
    export SPREADSHEET_UUID=${5:-17VAXsRSOz9Y2K6RhYwSt2RJMxyeLtJq09M2O2kiSbRo}
fi

if [ -z "$OUTPUT_PATH" ]; then
    export OUTPUT_PATH=${6:-./DATA}
fi

export GOOGLE_PROJECT=$(gcloud projects list --filter=name=$GOOGLE_PROJECT_NAME --format="value(projectId)" )
if [ -z "$GOOGLE_PROJECT" ]; then
    echo "Need to create project " $GOOGLE_PROJECT_NAME
else
    if [ -z "$GOOGLE_BUCKET" ]; then
        export GOOGLE_BUCKET=$GOOGLE_PROJECT
    fi    
fi    

if [ ! -z "$GOOGLE_BUCKET" ]; then
    gsutil ls | grep -q gs://$GOOGLE_BUCKET || echo  "Need to create bucket " $GOOGLE_BUCKET
    if [ $? -eq 0 ]; then
        export GOOGLE_LOCATION=$(gsutil ls -Lb gs://$GOOGLE_BUCKET | grep "Location constraint" | awk '{print tolower($3)}')
    fi
fi    

# point as this project by default.
gcloud config set project $GOOGLE_PROJECT > /dev/null

export TOKEN=$(gcloud auth application-default print-access-token)

export GOOGLE_DATASTORES=$(gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION --format="table[no-heading](ID)" | tr '\n' ',')

export PYANVIL_CACHE_PATH=$OUTPUT_PATH/pyanvil-cache.sqlite

if [ -z "$IMPLEMENTATION_GUIDE_PATH" ]; then
    export IMPLEMENTATION_GUIDE_PATH=$OUTPUT_PATH/IG
fi    
