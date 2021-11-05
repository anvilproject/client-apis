#!/bin/bash
# Enable IG on datastore, writes all output to stderr, fails if server does not return 200

unset MISSING
[ -z "$GOOGLE_PROJECT" ] && echo "missing env var: GOOGLE_PROJECT" && MISSING="Y"
[ -z "$GOOGLE_LOCATION" ] && echo "missing env var: GOOGLE_LOCATION" && MISSING="Y"
[ -z "$GOOGLE_DATASET" ] && echo "missing env var: GOOGLE_DATASET" && MISSING="Y"
[ ! -z "$MISSING" ] &&  echo "please set required env variables" && exit 1

[ -z "$1" ] &&  echo "please provide data_store parameter" && exit 1

[ -z "$TOKEN" ] &&  export TOKEN=$(gcloud auth application-default print-access-token)

echoerr() { echo "$@" 1>&2; }

echoerr https://healthcare.googleapis.com/v1beta1/projects/$GOOGLE_PROJECT/locations/$GOOGLE_LOCATION/datasets/$GOOGLE_DATASET/fhirStores/$1?updateMask=validationConfig
# capture return code
HTTP_CODE=$(
    curl -s -H "Authorization:Bearer $TOKEN" -H "Content-Type: application/json; charset=utf-8" \
        -X PATCH \
        -w "%{http_code}" \
        -o /dev/stderr \
        --data '{
            "validationConfig": {
                "enabledImplementationGuides": ["https://ncpi-fhir.github.io/ncpi-fhir-ig/ImplementationGuide/NCPI-FHIR-Implementation-Guide"]
            }
        }' https://healthcare.googleapis.com/v1beta1/projects/$GOOGLE_PROJECT/locations/$GOOGLE_LOCATION/datasets/$GOOGLE_DATASET/fhirStores/$1?updateMask=validationConfig
)
# tell caller
if [ ${HTTP_CODE} -ne 200 ] ; then
    exit 1
fi
