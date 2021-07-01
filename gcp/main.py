"""
Handles PubSub requests sent to the container
- This performs all ETL operations and transfers to Healthcare API
"""

import threading
import os

# debugger imports
# from . import pfb_downloader
# from . import pfb_extractor
# from . import data_uploader
# from . import data_transfer

# production imports
import pfb_downloader
import pfb_extractor
import data_uploader
import data_transfer

from flask import Flask, request

from dotenv import load_dotenv

# env constants
load_dotenv()

app = Flask(__name__)
sem = threading.Semaphore()


@app.route("/", methods=["POST"])
def index():
    """Handles PubSub POST requests

    Note: We send 202 for error messages to ack PubSub, else it infinitely loops
    """
    # grabs semaphore
    sem.acquire()

    # gets the pubsub message
    envelope = request.get_json()

    # error checking
    if not envelope:
        err = "no PubSub message received"
        print(f"[Error] 400 Bad Request: {err}")
        sem.release()
        return f"[Error] 400 Bad Request: {err}", 400
    if not isinstance(envelope, dict) or "message" not in envelope:
        err = "invalid PubSub message format"
        print(f"[Error] 400 Bad Request: {err}")
        sem.release()
        return f"[Error] 400 Bad Request: {err}", 400

    # download PFB
    try:
        pfb_downloader.main(envelope)
    except Exception as err:
        print(f"[Error] {err}")
        sem.release()
        return f"[Error] {err}", 202

    # extract PFB
    try:
        pfb_extractor.main()
    except Exception as err:
        print(f"[Error] {err}")
        sem.release()
        return f"[Error] {err}", 202

    # upload ndjson
    try:
        data_uploader.main()
    except Exception as err:
        print(f"[Error] {err}")
        sem.release()
        return f"[Error] {err}", 202

    # transfer ndjson to healthcare API
    try:
        data_transfer.main()
    except Exception as err:
        print(f"[Error] {err}")
        sem.release()
        return f"[Error] {err}", 202

    # release semaphore after successful run
    print("[Successful]: files processed without errors")
    sem.release()
    return "[Successful]: files processed without errors", 200


if __name__ == "__main__":
    app.run(
        debug=True, host="127.0.0.1", port=int(os.environ.get("PORT", 8080))
    )
