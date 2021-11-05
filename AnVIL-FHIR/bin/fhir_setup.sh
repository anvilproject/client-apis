#!/bin/bash
# Pre-requisites for Google Healthcare API FHIR server

unset MISSING


[ -z "$GOOGLE_PROJECT_NAME" ] && echo "missing: GOOGLE_PROJECT_NAME the name of the project that will host the FHIR service" && MISSING="Y"
[ -z "$GOOGLE_DATASET" ] && echo "missing: GOOGLE_DATASET the dataset to host study_data" && MISSING="Y"
[ -z "$IMPLEMENTATION_GUIDE_PATH" ] && echo "missing: IMPLEMENTATION_GUIDE_PATH path to IG json see https://github.com/ncpi-fhir/ncpi-fhir-ig fsh-generated/resources/" && MISSING="Y"

[ ! -z "$MISSING" ] &&  echo "please set required env variables" && exit 1

export GOOGLE_PROJECT=$(gcloud projects list --filter=name=$GOOGLE_PROJECT_NAME --format="value(projectId)" )
if [ -z "$GOOGLE_PROJECT" ]; then
    echo "Need to create project"
    unset MISSING
    [ -z "$GOOGLE_BILLING_ACCOUNT" ] && echo "missing: GOOGLE_BILLING_ACCOUNT billing account for project" && MISSING="Y"
    [ -z "$GOOGLE_LOCATION" ] && echo "missing: GOOGLE_LOCATION the google region for the project & service" && MISSING="Y"
    [ ! -z "$MISSING" ] &&  echo "please set required env variables" && exit 1
    # create the project
    gcloud projects create --name=$GOOGLE_PROJECT_NAME --quiet
    #  capture that ID and assign it to an environmental variable
    export GOOGLE_PROJECT=$(gcloud projects list --filter=name=$GOOGLE_PROJECT_NAME --format="value(projectId)" )
    [ -z "$GOOGLE_PROJECT" ] && echo "Could not create GOOGLE_PROJECT" && exit 1
    # attach a billing to the project
    gcloud beta billing projects link $GOOGLE_PROJECT --billing-account=$GOOGLE_BILLING_ACCOUNT
    # point as this project by default.
    gcloud config set project $GOOGLE_PROJECT
    #  ‘Cloud Healthcare API’ click ‘Enable’ to add the API to the current project.
    gcloud services enable healthcare.googleapis.com
fi

if [ -z "$GOOGLE_BUCKET" ]; then
    echo "Need to create bucket"
    # Create a bucket, in the same location and name as our project
    gsutil mb -p $GOOGLE_PROJECT -c STANDARD -l $GOOGLE_LOCATION gs://$GOOGLE_PROJECT
    export GOOGLE_BUCKET=$GOOGLE_PROJECT
    echo Created $GOOGLE_BUCKET
fi

# point as this project by default.
gcloud config set project $GOOGLE_PROJECT

# get service account
export GOOGLE_SERVICE_ACCOUNT=$(gcloud projects get-iam-policy $GOOGLE_PROJECT --format="value(bindings.members)" --flatten="bindings[]" | grep serviceAccount | sed s/serviceAccount:// | head -1)
[ -z "$GOOGLE_SERVICE_ACCOUNT" ] &&  echo "Unable to set GOOGLE_SERVICE_ACCOUNT ??" && exit 1
# assign bucket reader permissions so that it can be used to read the bucket.
gcloud projects add-iam-policy-binding $GOOGLE_PROJECT --member=serviceAccount:$GOOGLE_SERVICE_ACCOUNT --role=roles/storage.objectViewer
[ $? -ne 0 ] && echo "Unable to set roles/storage.objectViewer" && exit 1
# gcloud projects add-iam-policy-binding $GOOGLE_PROJECT --member=serviceAccount:$GOOGLE_SERVICE_ACCOUNT --role=roles/storage.objects.list
# [ $? -ne 0 ] && echo "Unable to set roles/storage.objects.list" && exit 1
echo Granted roles/storage.objectViewer to $GOOGLE_SERVICE_ACCOUNT on $GOOGLE_BUCKET



# move IG into bucket
# create dir
mkdir -p  $OUTPUT_PATH/IG
cd $OUTPUT_PATH/IG
# clean up old
rm *.json
rm *.internals
# copy from IG build
unzip ../../../fhir/ncpi-fhir-ig/output/definitions.json.zip
unzip ../../../fhir/ncpi-fhir-ig/output/expansions.json.zip
# delete extraneous
rm *.internals
# configure for google
# https://cloud.google.com/healthcare/docs/how-tos/fhir-profiles#configure_your_implementation_guide
cd ../..
fix_ig_for_google 

gsutil -m cp -J -r $OUTPUT_PATH/IG    gs://$GOOGLE_BUCKET
# also need to include all dependencies
curl -s http://hl7.org/fhir/us/core/STU3.1.1/ImplementationGuide-hl7.fhir.us.core.json | gsutil cp - gs://$GOOGLE_BUCKET/IG/ImplementationGuide-hl7.fhir.us.core.json
#curl http://hl7.org/fhir/us/core/STU4/ImplementationGuide-hl7.fhir.us.core.json | gsutil cp - gs://$GOOGLE_BUCKET/IG/ImplementationGuide-hl7.fhir.us.core.json

curl -s https://www.hl7.org/fhir/definitions.json.zip -o /tmp/definitions.json.zip
unzip -p /tmp/definitions.json.zip valuesets.json > /tmp/valuesets.json
cat /tmp/valuesets.json | gsutil cp - gs://$GOOGLE_BUCKET/IG/valuesets/valuesets.json
rm /tmp/definitions.json.zip
rm /tmp/valuesets.json

curl -s https://www.hl7.org/fhir/valueset-mimetypes.json | gsutil cp - gs://$GOOGLE_BUCKET/IG/valuesets/valueset-mimetypes.json


echo "Copied IG to bucket"

#  First create the dataset.
# delete first? gcloud healthcare datasets delete $GOOGLE_DATASET --location=$GOOGLE_LOCATION
if ! gcloud healthcare datasets list --location=$GOOGLE_LOCATION | grep $GOOGLE_DATASET ; then 
    echo creating dataset $GOOGLE_DATASET
    gcloud healthcare datasets create $GOOGLE_DATASET --location=$GOOGLE_LOCATION
else
    echo dataset $GOOGLE_DATASET already exists    
fi


