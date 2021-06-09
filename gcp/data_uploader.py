import os

from google.cloud import storage

from dotenv import load_dotenv

# env constants
load_dotenv()
GCP_JSON_BUCKET = os.getenv("GCP_JSON_BUCKET")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./data")


def main():
    # error checking
    if not GCP_JSON_BUCKET:
        err = "no GCP_JSON_BUCKET in .env"
        raise Exception(f"501 Not Implemented: {err}")

    # create cloud storage client
    client = storage.Client()
    bucket = client.get_bucket(GCP_JSON_BUCKET)

    # upload each .ndjson in OUTPUT_PATH
    try:
        for filename in os.listdir(OUTPUT_PATH):
            local_path = os.path.join(OUTPUT_PATH, filename)
            blob = bucket.blob(filename)
            blob.upload_from_filename(local_path)
            print(f"Uploaded {local_path} to {filename}!")
    except Exception as err:
        raise Exception(f"500 Internal Server Error: {err}")


if __name__ == "__main__":
    main()
