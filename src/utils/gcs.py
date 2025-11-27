from google.cloud import storage
from loguru import logger


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
        logger.info(
            f"Moving file {source_blob_name} to {destination_blob_name} in bucket {bucket_name}"
        )
        bucket.rename_blob(source_blob, destination_blob_name)
        logger.info(
            f"File {source_blob_name} moved to {destination_blob_name} successfully."
        )
    else:
        logger.warning(f"File {source_blob_name} not found in bucket {bucket_name}.")
