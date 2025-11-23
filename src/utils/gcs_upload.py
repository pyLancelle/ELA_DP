#!/usr/bin/env python3
"""
Generic GCS Upload Script
-------------------------
Moves (uploads and deletes) local *.jsonl files to a specified Google Cloud Storage bucket and path.
"""
import argparse
import logging
import sys
import os
from pathlib import Path
from google.cloud import storage
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def upload_to_gcs(bucket_name: str, source_file: Path, destination_blob_name: str) -> bool:
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(str(source_file))
        
        logger.info(f"✅ Uploaded {source_file.name} to gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to upload {source_file.name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Move local JSONL files to GCS")
    parser.add_argument("--bucket", required=True, help="Target GCS bucket name")
    parser.add_argument("--destination", required=True, help="Target GCS prefix (e.g., garmin/landing/)")
    parser.add_argument("--source-dir", default=".", help="Source directory (default: current)")
    
    args = parser.parse_args()
    
    source_dir = Path(args.source_dir)
    if not source_dir.exists():
        logger.error(f"❌ Source directory does not exist: {source_dir}")
        sys.exit(1)
        
    # Normalize destination prefix
    dest_prefix = args.destination.strip("/")
    if dest_prefix:
        dest_prefix += "/"
        
    logger.info(f"🚀 Starting GCS Upload")
    logger.info(f"   Source: {source_dir.absolute()}")
    logger.info(f"   Target: gs://{args.bucket}/{dest_prefix}")
    
    # Find JSONL files
    files = list(source_dir.glob("*.jsonl"))
    
    if not files:
        logger.info("ℹ️  No *.jsonl files found to upload.")
        return

    logger.info(f"📦 Found {len(files)} files to process")
    
    success_count = 0
    error_count = 0
    
    for file_path in files:
        filename = file_path.name
        destination_blob_name = f"{dest_prefix}{filename}"
        
        if upload_to_gcs(args.bucket, file_path, destination_blob_name):
            try:
                os.remove(file_path)
                logger.info(f"🗑️  Deleted local file: {filename}")
                success_count += 1
            except OSError as e:
                logger.error(f"⚠️  Failed to delete local file {filename}: {e}")
        else:
            error_count += 1
            
    logger.info(f"\n📈 Upload Summary:")
    logger.info(f"✅ Moved: {success_count} files")
    if error_count > 0:
        logger.info(f"❌ Failed: {error_count} files")
        sys.exit(1)
    
    logger.info("🎉 GCS Upload completed successfully")

if __name__ == "__main__":
    main()
