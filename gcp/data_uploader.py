import os

from google.cloud import storage

from dotenv import load_dotenv

load_dotenv()

# env constants
GCP_BUCKET = os.getenv("GCP_JSON_BUCKET")
OUTPUT_DIR = os.getenv("OUTPUT_PATH", "./data")

# create cloud storage client
client = storage.Client()
bucket = client.get_bucket(GCP_BUCKET)

try:
    for filename in os.listdir(OUTPUT_DIR):
        local_path = os.path.join(OUTPUT_DIR, filename)
        blob = bucket.blob(filename)
        blob.upload_from_filename(local_path)
        print(f"Uploaded {local_path} to {filename}")
except Exception as e:
    print(f"Error: {e}")
