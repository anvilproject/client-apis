import os

# debugger imports
# from . import pfb_downloader
# from . import pfb_extractor
# from . import data_uploader

# production imports
import pfb_downloader
import pfb_extractor
import data_uploader

from flask import Flask, request

from dotenv import load_dotenv

# env constants
load_dotenv()

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """Handles PubSub POST requests

    Note: We send 202 for error messages to ack PubSub, else it infinitely loops
    """
    envelope = request.get_json()

    # error checking
    if not envelope:
        err = "no PubSub message received"
        print(f"[Error] 400 Bad Request: {err}")
        return f"[Error] 400 Bad Request: {err}", 400
    if not isinstance(envelope, dict) or "message" not in envelope:
        err = "invalid PubSub message format"
        print(f"[Error] 400 Bad Request: {err}")
        return f"[Error] 400 Bad Request: {err}", 400

    # download PFB
    try:
        pfb_downloader.main(envelope)
    except Exception as err:
        print(f"[Error] {err}")
        return f"[Error] {err}", 202

    # extract PFB
    try:
        pfb_extractor.main()
    except Exception as err:
        print(f"[Error] {err}")
        return f"[Error] {err}", 202

    # upload ndjson
    try:
        data_uploader.main()
    except Exception as err:
        print(f"[Error] {err}")
        return f"[Error] {err}", 202

    # successful run
    print(f"[Successful]: files processed without errors")
    return "[Successful]: files processed without errors", 200


if __name__ == "__main__":
    app.run(
        debug=True, host="127.0.0.1", port=int(os.environ.get("PORT", 8080))
    )
