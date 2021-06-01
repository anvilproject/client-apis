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

> These scripts heavily rely on [dotenv](https://pypi.org/project/python-dotenv/) to work.
> Therefore a `.env` file needs to be created in this folder with these params:

| key                 | default                      | description                                                  |
| ------------------- | ---------------------------- | ------------------------------------------------------------ |
| GCP_BILLING_PROJECT | _REQUIRED_                   | The GCP project to charge to                                 |
| GCP_JSON_BUCKET     | _REQUIRED_                   | A Cloud Storage bucket name to upload the `.ndjson` files to |
| AVRO_PATH           | `./export_1000_genomes.avro` | The path to the `.avro` file to be extracted                 |
| OUTPUT_PATH         | `./data`                     | The path to save the `.ndjson` files to                      |

## Scripts

### `pfb_extractor.py`

This script will take extract the provided `.avro` file and create `.ndjson` files with extracted information.
Upload these files into a Cloud Storage bucket to prepare for installation

### `data_uploader.py`

This script will upload all files from the `OUTPUT_PATH` to the `GCP_JSON_BUCKET`

### `json_splitter.py`

This script splits the `.ndjson` files into indivudal `.json` files.
The script is not currently used, but is nice to have if needed
