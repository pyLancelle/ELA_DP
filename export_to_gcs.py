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

upload_to_gcs_explicit_key(
    "ela-dp-dev",
    "data/2025_07_06_strava_activities.csv",
    "strava/2025_07_06_strava_activities.csv",
    key_path,
)
