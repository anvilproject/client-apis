"""
Downloads .avro file from PubSub envelope
"""

import base64
import json
import os

from google.cloud import storage

from dotenv import load_dotenv

# env constants
load_dotenv()
GCP_PFB_BUCKET = os.getenv("GCP_PFB_BUCKET", "")


def main(envelope):
    # error checking
    if not GCP_PFB_BUCKET:
        err = "no GCP_PFB_BUCKET in .env"
        raise Exception(f"501 Not Implemented: {err}")

    # extract message data
    pubsub_message = envelope["message"]
    message = ""

    # check eventType
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        eventType = pubsub_message["attributes"]["eventType"]
        if eventType != "OBJECT_FINALIZE":
            err = "event not OBJECT_FINALIZE"
            raise Exception(f"412 Precondition Failed: {err}")

    # extract message
    message = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
    message = json.loads(message)

    # check file extension
    avro_file = message["name"]
    print(f"FILE: {avro_file}")
    if not avro_file.endswith(".avro"):
        err = "uploaded file not .avro"
        raise Exception(f"412 Precondition Failed: {err}")

    # download avro
    try:
        # create storage client with service account
        storage_client = storage.Client.from_service_account_json("creds.json")

        print(f"Downloading {avro_file}...")
        with open("export.avro", "w+b") as export_file:
            storage_client.download_blob_to_file(
                f"gs://{GCP_PFB_BUCKET}/{avro_file}", export_file
            )
            export_file.close()
        print(f"Download Completed!")
    except Exception as err:
        raise Exception(f"500 Internal Server Error: {err}")


if __name__ == "__main__":
    main()
