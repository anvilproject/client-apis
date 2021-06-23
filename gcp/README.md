# AnVIL GCP Integration

> Python scripts that take an `.avro` upload and loads the data into Google's Healthcare API

## Goals

[x] Create a script that puts test data in Google Healthcare API
[x] Transform script to accept an `.avro` export and insert into Google Healthcare API
[ ] Deploy script to run off bucket trigger on GCP (may require App Engine)
[ ] Deploy IG to Google Healthcare API
[ ] Review service w/ Broad devops & security teams

## Setup

Create a virtual environment

```
cd gcp
python3 -m venv .venv
```

Source your virual environment

```
source .venv/bin/activate
```

Install

```
pip install -r requirements.txt
```

These scripts heavily rely on [dotenv](https://pypi.org/project/python-dotenv/) to work.
Therefore a `.env` file needs to be created in this folder with these params:

| key             | default                      | description                                                    |
| --------------- | ---------------------------- | -------------------------------------------------------------- |
| GCP_PROJECT_ID  | _REQUIRED_                   | The GCP project ID                                             |
| GCP_LOCATION    | _REQUIRED_                   | The location of the GCP data center                            |
| GCP_DATASET     | _REQUIRED_                   | The Healthcare API Dataset name                                |
| GCP_DATASTORE   | _REQUIRED_                   | The FHIR Store name                                            |
| GCP_PFB_BUCKET  | _REQUIRED_                   | The Cloud Storage bucket name containing the PFB exports       |
| GCP_JSON_BUCKET | _REQUIRED_                   | The Cloud Storage bucket name to upload the `.ndjson` files to |
| AVRO_PATH       | `./export_1000_genomes.avro` | The local path to the `.avro` file to be extracted             |
| OUTPUT_PATH     | `./data`                     | The local path to save the `.ndjson` files to                  |
| PORT            | 8080                         | The port to run the Flask server on                            |

## Scripts

> There is a [demo Jupyter Notebook](./demo.ipynb) to show the functionality of these scripts included in the repo

### `pfb_downloader.py`

This script will download the `.avro` file from `GCP_PFB_BUCKET` locally.
This is mainly used in the container to download the uploaded PFB.

### `pfb_extractor.py`

This script will take extract the provided `.avro` file and create `.ndjson` files with extracted information.
Upload these files into a Cloud Storage bucket to prepare for installation

### `data_uploader.py`

This script will upload all files from the `OUTPUT_PATH` to the `GCP_JSON_BUCKET`

### `data_transfer.py`

This script will transfer the `ndjson` files from the `GCP_JSON_BUCKET` over to the Healthcare API

### `json_splitter.py`

This script splits the `.ndjson` files into indivudal `.json` files.
The script is not currently used, but is nice to have if needed
