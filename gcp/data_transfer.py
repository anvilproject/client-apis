"""
Transfer .ndjson files from GCP_JSON_BUCKET to Healthcare API
"""

import subprocess
import os

from dotenv import load_dotenv

# env constants
load_dotenv()
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "")
GCP_DATASET = os.getenv("GCP_DATASET", "")
GCP_DATASTORE = os.getenv("GCP_DATASTORE", "")
GCP_JSON_BUCKET = os.getenv("GCP_JSON_BUCKET", "")
SA_NAME = os.getenv("SA_NAME", "")


def main():
    # error checking
    try:
        checkEnv()
    except Exception as err:
        raise Exception(f"500 Internal Server Error: {err}")

    # setup gcloud
    try:
        gcloud_cmd = f"gcloud auth activate-service-account {SA_NAME}@{GCP_PROJECT_ID}.iam.gserviceaccount.com --key-file=./creds.json"
        print(f"CMD: {gcloud_cmd}")
        process = subprocess.Popen(gcloud_cmd.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        print(f"OUTPUT: {output}")

        if error:
            raise Exception(f"500 Internal Server Error: {error}")
    except Exception as err:
        raise Exception(f"500 Internal Server Error: {err}")

    # make gsutil call
    try:
        gsutil_cmd = f"gcloud healthcare fhir-stores import gcs {GCP_DATASTORE} --project={GCP_PROJECT_ID} --location={GCP_LOCATION} --dataset={GCP_DATASET} --gcs-uri=gs://{GCP_JSON_BUCKET} --content-structure=resource"
        print(f"CMD: {gsutil_cmd}")
        process = subprocess.Popen(gsutil_cmd.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        print(f"OUTPUT: {output}")

        if error:
            raise Exception(f"500 Internal Server Error: {error}")
    except Exception as err:
        raise Exception(f"500 Internal Server Error: {err}")


def checkEnv():
    if not GCP_PROJECT_ID:
        err = "no GCP_PROJECT_ID in .env"
        raise Exception(f"500 Internal Server Error: {err}")
    if not GCP_LOCATION:
        err = "no GCP_LOCATION in .env"
        raise Exception(f"500 Internal Server Error: {err}")
    if not GCP_DATASET:
        err = "no GCP_DATASET in .env"
        raise Exception(f"500 Internal Server Error: {err}")
    if not GCP_DATASTORE:
        err = "no GCP_DATASTORE in .env"
        raise Exception(f"500 Internal Server Error: {err}")
    if not GCP_JSON_BUCKET:
        err = "no GCP_JSON_BUCKET in .env"
        raise Exception(f"500 Internal Server Error: {err}")
    if not SA_NAME:
        err = "no SA_NAME in .env"
        raise Exception(f"500 Internal Server Error: {err}")
    return


if __name__ == "__main__":
    main()
