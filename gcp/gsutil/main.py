import subprocess
import base64
import os

from flask import Flask, request

from dotenv import load_dotenv

# env constants
load_dotenv()
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "")
GCP_DATASET = os.getenv("GCP_DATASET", "")
GCP_DATASTORE = os.getenv("GCP_DATASTORE", "")
GCP_JSON_BUCKET = os.getenv("GCP_JSON_BUCKET", "")
SA_NAME = os.getenv("SA_NAME", "")


app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """Handles PubSub POST requests

    Note: We send 202 for error messages to ack PubSub, else it infinitely loops
    """
    envelope = request.get_json()

    # error checking
    try:
        checkEnv()
    except Exception as err:
        print(f"[Error] 501 Not Implemented: {err}")
        return f"[Error] 501 Not Implemented: {err}", 202
    if not envelope:
        err = "no PubSub message received"
        print(f"[Error] 400 Bad Request: {err}")
        return f"[Error] 400 Bad Request: {err}", 400
    if not isinstance(envelope, dict) or "message" not in envelope:
        err = "invalid PubSub message format"
        print(f"[Error] 400 Bad Request: {err}")
        return f"[Error] 400 Bad Request: {err}", 400

    # parse pubsub message
    # TODO: add message checking
    pubsub_message = envelope["message"]
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        pubsub_message = (
            base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
        )

    # setup gcloud
    try:
        gcloud_cmd = f"gcloud auth activate-service-account {SA_NAME}@gcp-testing-308520.iam.gserviceaccount.com --key-file=creds.json"
        print(f"CMD: {gcloud_cmd}")
        process = subprocess.Popen(gcloud_cmd.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        print(f"OUTPUT: {output}")

        if error:
            raise Exception(error)
    except Exception as err:
        print(f"[Error] 500 Internal Server Error: {err}")
        return f"[Error] 500 Internal Server Error: {err}", 202

    # make gsutil call
    try:
        gsutil_cmd = f"gcloud healthcare fhir-stores import gcs {GCP_DATASTORE} --project={GCP_PROJECT_ID} --location={GCP_LOCATION} --dataset={GCP_DATASET} --gcs-uri=gs://{GCP_JSON_BUCKET} --content-structure=resource"
        print(f"CMD: {gsutil_cmd}")
        process = subprocess.Popen(gsutil_cmd.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        print(f"OUTPUT: {output}")

        if error:
            raise Exception(error)
    except Exception as err:
        print(f"[Error] 500 Internal Server Error: {err}")
        return f"[Error] 500 Internal Server Error: {err}", 202

    # successful run
    print(f"[Successful]: files processed without errors")
    return "[Successful]: files processed without errors", 200

def checkEnv():
    if not GCP_PROJECT_ID:
        raise Exception("no GCP_PROJECT_ID in .env")
    if not GCP_LOCATION:
        raise Exception("no GCP_LOCATION in .env")
    if not GCP_DATASET:
        raise Exception("no GCP_DATASET in .env")
    if not GCP_DATASTORE:
        raise Exception("no GCP_DATASTORE in .env")
    if not GCP_JSON_BUCKET:
        raise Exception("no GCP_JSON_BUCKET in .env")
    if not SA_NAME:
        raise Exception("no SA_NAME in .env")
    return

if __name__ == "__main__":
    app.run(
        debug=True, host="127.0.0.1", port=int(os.environ.get("PORT", 8080))
    )
