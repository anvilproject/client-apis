#!/bin/bash
# Enable IG on datastore

unset MISSING
[ -z "$GOOGLE_PROJECT" ] && echo "missing env var: GOOGLE_PROJECT" && MISSING="Y"
[ -z "$GOOGLE_LOCATION" ] && echo "missing env var: GOOGLE_LOCATION" && MISSING="Y"
[ -z "$GOOGLE_DATASET" ] && echo "missing env var: GOOGLE_DATASET" && MISSING="Y"
[ ! -z "$MISSING" ] &&  echo "please set required env variables" && exit

[ ! -z "$1" ] &&  echo "please provide data_store parameter" && exit


export TOKEN=$(gcloud auth application-default print-access-token)


curl -H "Authorization:Bearer $TOKEN" -H "Content-Type: application/json; charset=utf-8" \
    -X PATCH \
    --data '{
        "validationConfig": {
            "enabledImplementationGuides": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/ImplementationGuide/NCPI-FHIR-Implementation-Guide"]
        }
    }' https://healthcare.googleapis.com/v1beta1/projects/$GOOGLE_PROJECT/locations/$GOOGLE_LOCATION/datasets/$GOOGLE_DATASET/fhirStores/$1?updateMask=validationConfig
