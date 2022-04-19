
#
# move IG into bucket
#

# create dir
mkdir -p  $OUTPUT_PATH/IG
cd $OUTPUT_PATH/IG
pwd
# clean up old
rm *.json
rm *.internals
# download IG

curl https://nih-ncpi.github.io/ncpi-fhir-ig/definitions.json.zip --output $OUTPUT_PATH/definitions.json.zip 
curl https://nih-ncpi.github.io/ncpi-fhir-ig/expansions.json.zip --output $OUTPUT_PATH/expansions.json.zip 

unzip $OUTPUT_PATH/definitions.json.zip
unzip $OUTPUT_PATHexpansions.json.zip
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
