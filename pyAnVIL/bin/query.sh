#!/bin/bash
# Simple query tool

# # stop on errors
# set -e

# util to log to stderr
echoerr() { echo "$@" 1>&2; }

# check params
unset MISSING
[ -z "$GOOGLE_LOCATION" ] && echoerr "missing: GOOGLE_LOCATION" && MISSING="Y"
[ -z "$GOOGLE_DATASET" ] && echoerr "missing: GOOGLE_DATASET" && MISSING="Y"
[ -z "$GOOGLE_PROJECT" ] && echoerr "missing: GOOGLE_PROJECT" && MISSING="Y"
[ ! -z "$MISSING" ] &&  echoerr "please set required env variables. see fhir_setup.sh" && exit 1

unset MISSING
[ -z "$1" ] && echoerr "missing: \$1 url path.  e.g. /ResearchStudy?_count=1" && MISSING="Y"
[ ! -z "$MISSING" ] &&  echoerr "please set required parameters" && exit 1
url_path=$1


# read the list of datastores if user didn't specify one
data_store=$2
if [ -z "$data_store" ]; then
    echoerr creating GOOGLE_DATASTORES list
    export GOOGLE_DATASTORES=( $(gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION --format="table[no-heading](ID)") )
fi

if [ -z "$TOKEN" ]; then
    echoerr creating TOKEN
    export TOKEN=$(gcloud auth application-default print-access-token)
fi


if [ ! -z "$data_store" ]; then
    # search one datastore
    url=https://healthcare.googleapis.com/v1beta1/projects/$GOOGLE_PROJECT/locations/$GOOGLE_LOCATION/datasets/$GOOGLE_DATASET/fhirStores/$data_store/fhir
    fhir_query --token=$TOKEN --url=$url$url_path
else
    #  search all data stores
    for data_store in "${GOOGLE_DATASTORES[@]}"
    do
        url=https://healthcare.googleapis.com/v1beta1/projects/$GOOGLE_PROJECT/locations/$GOOGLE_LOCATION/datasets/$GOOGLE_DATASET/fhirStores/$data_store/fhir
        fhir_query --token=$TOKEN --url=$url$url_path
    done    
fi
