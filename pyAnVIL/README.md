# pyAnVIL: terra + gen3

A python client integration of gen3 and terra.

For python developers, who have requirements to access both terra and gen3 platforms, pyAnVIL is an integration module that provides SSO (single sign on) using terra as an IDP (identity provider) and manages distribution of dependencies unlike juggling multiple credentials and installs, pyAnVIL provides developer friendly experience.

## Installation

Pre-requisites:

- gcloud cli tools installed and configured [gcloud](https://cloud.google.com/sdk/install).
- Google Id provisioned in both Terra and Gen3:
  - One time Account Linking:
    - Pre-requisite: google account provisioned in both Gen3 and Terra.

    - Log into https://gen3.theanvil.io/
    - Log into https://anvil.terra.bio
    - In Terra, navigate to your profile
      - Under "IDENTITY & EXTERNAL SERVERS", log into `NHGRI AnVIL Data Commons Framework Services`, the system should present you with a Gen3 Oauth flow.
      - Note the google project used for billing
        ![](docs/_static/terra-profile.png)
    - “unlink” my NHGRI AnVIL Data Commons Framework Services  from https://anvil.terra.bio/#profile 
    - open a new window to gen3.theanvil.io and login using my google id 
    - return to the terra profile screen and “renew” the identity 

- Per instance, terra API setup:
  - Use the google account and billing project to setup credentials for the [terra api](https://github.com/broadinstitute/fiss).
    ```
      gcloud auth login <google-account>
      gcloud auth application-default set-quota-project <billing-project-id>
    ```
- Validation

  ```
  gcloud auth print-access-token
  >>> ya29.a0AfH6SMBSPFSt252qQNl.......

  fissfc config
  >>> ....
  root_url	https://broad-bond-prod.appspot.com/
  ```

- Setup
  ```
  pip install pyAnVIL
  ```

## Use cases

### SSO

```
   from anvil.gen3_auth import Gen3TerraAuth
   from gen3.submission import Gen3Submission

   auth = Gen3TerraAuth()
   gen3_endpoint = "https://gen3.theanvil.io"
   submission_client = Gen3Submission(gen3_endpoint, auth)
```

[sso sequence diagram](docs/_static/sequence-diagram.png)

#### API Wrappers

### Gen3

```
   query = '{project(first:0) {code,  subjects {submitter_id}, programs {name}  }}'
   results = submission_client.query(query)
   [p['code'] for p in results['data']['project']]
   >>> ['GTEx', '1000Genomes']
```

### Terra

```
   from anvil.terra import FAPI
   FAPI.whoami()
   >>> 'anvil.user@gmail.com'
```

### FHIR

We incorporated `fhirclient`, a flexible Python client for FHIR servers supporting the SMART on FHIR protocol. 

Note: You will need to install the fhir client separately.  see  https://github.com/smart-on-fhir/client-py/issues/70

```
pip install  git+https://github.com/smart-on-fhir/client-py#egg=fhirclient
```

Example

```

from anvil.fhir.client import FHIRClient
settings = {
    'app_id': 'my_web_app',
    'api_base': 'https://healthcare.googleapis.com/v1/projects/gcp-testing-308520/locations/us-east4/datasets/testset/fhirStores/fhirstore/fhir'
}
smart = FHIRClient(settings=settings)
assert smart.ready, "server should be ready"
# search for all ResearchStudy
import fhirclient.models.researchstudy as rs
[s.title for s in rs.ResearchStudy.where(struct={}).perform_resources(smart.server)]
>>> 
['1000g-high-coverage-2019', 'my NCPI research study example']

```

For more information on usage see [smart-on-fhir/client-py](https://github.com/smart-on-fhir/client-py)


### Data Normalization / FHIR


* Extract and normalize data 

```
# create a working directory for our data
mkdir  -p ./DATA

# transform all consortiums
anvil_etl transform  fhir 2> /tmp/fhir.log
# download latest NCPI ImplementationGuide
anvil_etl load fhir  IG create
# create all data stores
anvil_etl load fhir data-store create

# Verify IG load
export TOKEN=$(gcloud auth application-default print-access-token)
export GOOGLE_DATASTORES=$(gcloud beta healthcare fhir-stores list --dataset=$GOOGLE_DATASET --location=$GOOGLE_LOCATION | awk '(NR>1){print $1}' | sed  's/$/,/g' | tr -d "\n")
fhir_curl --data_store public /ImplementationGuide?_count=1 | jq .

```

* Transform to FHIR

```
anvil_transform transform   --output_path ./DATA --user_project $GOOGLE_BILLING_ACCOUNT

```

* Setup Environment

Set environmental variables by calling `fhir_env`  Provide a project name and region.  Note: please ensure the healthcare API is available in that region. https://cloud.google.com/healthcare-api/docs/concepts/regions

The script will set reasonable values for other environmental variables.  You may override them on the command line.

> usage: fhir_env GOOGLE_PROJECT_NAME GOOGLE_LOCATION [GOOGLE_DATASET] [GOOGLE_DATASTORE] [BILLING_ACCOUNT] [GOOGLE_APPLICATION_CREDENTIALS] [SPREADSHEET_UUID] [OUTPUT_PATH]


```
$ source fhir_env fhir-test-14  us-west2
***** env variables *****
GOOGLE_PROJECT_NAME fhir-test-14 The root for the API, billing, buckets, etc.
GOOGLE_BILLING_ACCOUNT XXXXX-XXXXX-XXXXXX Google Cloud Billing Accounts allow you to configure payment and track spending in GCP.
GOOGLE_LOCATION us-west1 The physical location of the data
GOOGLE_DATASET anvil-test Datasets are top-level containers that are used to organize and control access to your stores.
GOOGLE_DATASTORE test A FHIR store is a data store in the Cloud Healthcare API that holds FHIR resources.
GOOGLE_APPLICATION_CREDENTIALS ./XXXX.json Your google identity - used to retrieve the terra maintained spreadsheet and issue FHIR commands.
SPREADSHEET_UUID 17VAXsRSOz9Y2K6RhYwSt2RJMxyeLtJq09M2O2kiSbRo The spreadsheet key
OUTPUT_PATH ./DATA A directory on your local system, used to store work files.
GOOGLE_BUCKET fhir-test A bucket that will contain you ImplementationGuide and FHIR resources
```



### Resources

Create FHIR endpoints, buckets, etc. by calling `fhir_setup`.  No additional parameters are necessary.

```
# TODO - grant access to terra service account
$ source fhir_setup

```

### Load data

Load FHIR endpoints by calling `fhir_load`.  No additional parameters are necessary.





## Contributing

- set up virtual env

  ```
  python3 -m venv venv
  source venv/bin/activate
  python3 -m pip install -r requirements.txt
  python3 -m pip install -r requirements-dev.txt
  ```

- test gen3 authorization

  ```
  python3 -m pytest --user_email <GMAIL ACCOUNT>  --log-level DEBUG  --gen3_endpoint <GEN3_ENDPOINT>  tests/integration/test_gen3_auth.py
  ```

- continuous integration

  - see [service account setup](https://cloud.google.com/solutions/continuous-delivery-with-travis-ci#create_a_service_account)

  ```
     # see https://github.com/broadinstitute/firecloud-tools/tree/master/scripts/register_service_account
     docker run --rm -it -v "$HOME"/.config:/.config -v /Users/walsbr/client-apis/pyAnVIL/GOOGLE_APPLICATION_CREDENTIALS.json:/svc.json broadinstitute/firecloud-tools python /scripts/register_service_account/register_service_account.py -j /svc.json -e  brian@bwalsh.com
     The service account pyanvil@api-project-807881269549.bwalsh.com.iam.gserviceaccount.com is now registered with FireCloud. You can share workspaces with this address, or use it to call APIs.
  ```

## Distribution

- PyPi

```
# update pypi

export TWINE_USERNAME=  #  the username to use for authentication to the repository.
export TWINE_PASSWORD=  # the password to use for authentication to the repository.

rm -r dist/
python3  setup.py sdist bdist_wheel
twine upload dist/*
```

- Read The Docs

```
https://readthedocs.org/projects/pyanvil/
```
