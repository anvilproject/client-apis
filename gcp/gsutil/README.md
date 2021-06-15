# GSUtil Container

> Docker container to run a `gsutil` command to move `json` files over to Healthcare API

## Setup

Build

```
docker compose build
```

Run

```
docker compose up
```

These scripts heavily rely on [dotenv](https://pypi.org/project/python-dotenv/) to work.
Therefore a `.env` file needs to be created in this folder with these params:

| key             | default    | description                                                    |
| --------------- | ---------- | -------------------------------------------------------------- |
| GCP_PROJECT_ID  | _REQUIRED_ | The GCP project ID                                             |
| GCP_LOCATION    | _REQUIRED_ | The location of the GCP data center                            |
| GCP_DATASET     | _REQUIRED_ | The Healthcare API Dataset name                                |
| GCP_DATASTORE   | _REQUIRED_ | The FHIR Store name                                            |
| GCP_JSON_BUCKET | _REQUIRED_ | The Cloud Storage bucket name to upload the `.ndjson` files to |
| SA_NAME         | _REQUIRED_ | The service account used to authenticate                       |

Additionally, a `creds.json` file must be exported from a service account that has Healthcare API permissions
