```


gcloud beta healthcare fhir-stores delete dev --dataset=anvil-test --location=us-west2 --quiet
gcloud beta healthcare fhir-stores create dev --dataset=anvil-test --location=us-west2 --version=R4 --enable-update-create
gcloud beta healthcare fhir-stores import gcs dev --dataset=anvil-test --location=us-west2 --gcs-uri=gs://fhir-test-11-329119/IG/*.json --content-structure=resource-pretty  # --async
gcloud beta healthcare fhir-stores import gcs dev --dataset=anvil-test --location=us-west2 --gcs-uri=gs://fhir-test-11-329119/IG/valuesets/valuesets.json --content-structure=bundle-pretty # --async
gcloud beta healthcare fhir-stores import gcs dev --dataset=anvil-test --location=us-west2 --gcs-uri=gs://fhir-test-11-329119/IG/valuesets/valueset-mimetypes.json --content-structure=resource-pretty  # --async

export TOKEN=$(gcloud auth application-default print-access-token)
bin/enable_implementation_guide.sh dev

```