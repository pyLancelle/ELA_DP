from google.cloud import storage
import os


def upload_to_gcs_explicit_key(
    bucket_name, source_file_name, destination_blob_name, key_file_path
):
    """Uploads a file to the bucket using an explicitly provided service account key."""
    # This approach is less common for general application credentials but can be used
    # if you manage multiple service accounts in a single application instance.
    storage_client = storage.Client.from_service_account_json(key_file_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}."
    )


# Example Usage:
# BE CAREFUL: Do not hardcode the key file path directly in your source code in production!
# Consider reading it from an environment variable or configuration file.
key_path = os.environ.get(
    "GCS_KEY_FILE_PATH", "gcs_key.json"
)  # Example reading from env var

files_to_send = [
    f for f in os.listdir("data") if f.endswith("spotify_recently_played_raw.jsonl")
]

for file in files_to_send:
    upload_to_gcs_explicit_key(
        "ela-dp-dev",
        f"data/{file}",
        f"spotify/{file}",
        key_path,
    )
