from google.cloud import storage


def move_file_in_gcs(
    bucket_name: str, source_blob_name: str, destination_blob_name: str
):
    """Moves a file from a source location to a destination location within a GCS bucket.

    Args:
        bucket_name (str): The name of the GCS bucket.
        source_blob_name (str): The name of the source blob (file).
        destination_blob_name (str): The name of the destination blob (file).
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    source_blob = bucket.blob(source_blob_name)
    destination_blob = bucket.blob(destination_blob_name)

    if source_blob.exists():
        bucket.rename_blob(source_blob, destination_blob_name)
    else:
        print('Warning: Source blob does not exist: ')
