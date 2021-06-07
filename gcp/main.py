import base64
import json
import os

from flask import Flask, request
from google.cloud import storage

from dotenv import load_dotenv

# env constants
load_dotenv()
GCP_PFB_BUCKET = os.getenv("GCP_PFB_BUCKET", "")

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()

    # error checking
    if not envelope:
        err = "no Pub/Sub message received"
        print(f"error: {err}")
        return f"Bad Request: {err}", 400
    if not isinstance(envelope, dict) or "message" not in envelope:
        err = "invalid Pub/Sub message format"
        print(f"error: {err}")
        return f"Bad Request: {err}", 400
    if not GCP_PFB_BUCKET:
        err = "no GCP_PFB_BUCKET in .env"
        print(f"error: {err}")
        return f"Not Implemented: {err}", 501

    # get gcloud pubsub and process message
    pubsub_message = envelope["message"]
    message = ""
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        event = pubsub_message["attributes"]["eventType"]
        if event != "OBJECT_FINALIZE":
            msg = "Event not OBJECT_FINALIZE"
            print(f"ack: {msg}")
            return f"Accepted: {msg}", 202

        message = (
            base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
        )
    # print(f"PUBSUB: {message}")
    message = json.loads(message)

    # make sure it is an avro
    avro_file = message["name"]
    print(f"FILE: {avro_file}")
    if not avro_file.endswith(".avro"):
        msg = "Uploaded file not .avro"
        print(f"ack: {msg}")
        return f"Accepted: {msg}", 202

    # download avro
    try:
        storage_client = storage.Client()

        with open("export.avro", "w+b") as export_file:
            storage_client.download_blob_to_file(
                f"gs://{GCP_PFB_BUCKET}/{avro_file}", export_file
            )
            export_file.close()

        print(f"DIR: {os.listdir(os.curdir)}")
    except Exception as err:
        print(f"error: {err}")
        return f"Error: {err}", 202

    print(f"Complete: Run successful without errors")
    return "Complete", 200


if __name__ == "__main__":
    app.run(
        debug=True, host="127.0.0.1", port=int(os.environ.get("PORT", 8080))
    )
