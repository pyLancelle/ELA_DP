#!/usr/bin/env python3
"""
Simple Garmin Ingestion avec Mapping Config
===========================================
Ingère les fichiers Garmin dans BigQuery en utilisant le mapping défini dans metrics.yaml

Logique simple :
1. Lit metrics.yaml pour le mapping file_pattern → table
2. Pour chaque fichier dans GCS landing:
   - Détecte le type selon le nom du fichier
   - Cherche la table correspondante dans le mapping
   - Ingère avec auto-détection dans dataset.table
3. Archive les fichiers traités

Usage:
    python -m src.connectors.garmin.garmin_ingest --env dev
    python -m src.connectors.garmin.garmin_ingest --env prd --dry-run
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class SimpleGarminIngestor:
    """Ingestion simple avec mapping depuis metrics.yaml"""

    def __init__(self, env: str, dry_run: bool = False):
        """
        Initialize simple ingestor

        Args:
            env: Environment (dev or prd)
            dry_run: If True, validate but don't write
        """
        self.env = env
        self.dry_run = dry_run

        # Load configuration
        self.config = self._load_config()

        # GCP clients
        self.project_id = os.getenv('GCP_PROJECT_ID')
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable required")

        self.bq_client = bigquery.Client(project=self.project_id)
        self.storage_client = storage.Client(project=self.project_id)

        # Environment-specific values
        self.bucket = f"ela-dp-{env}"
        self.dataset = self.config['ingestion'][f'dataset_{env}']

        logging.info(f"✨ Simple Garmin Ingestor initialized")
        logging.info(f"   Environment: {env}")
        logging.info(f"   Bucket: {self.bucket}")
        logging.info(f"   Dataset: {self.dataset}")
        logging.info(f"   Dry run: {dry_run}")

    def _load_config(self) -> Dict:
        """Load metrics.yaml configuration"""
        config_path = Path(__file__).parent / 'metrics.yaml'

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Count data types (excluding 'ingestion' key)
        data_types = [k for k in config.keys() if k != 'ingestion']

        logging.info(f"📋 Loaded config from {config_path}")
        logging.info(f"   Found {len(data_types)} data types configured")

        return config

    def detect_file_type(self, filename: str) -> Optional[str]:
        """
        Detect file type from filename

        Args:
            filename: Name of the file

        Returns:
            File type (e.g., 'sleep', 'activities') or None if not found
        """
        filename_lower = filename.lower()

        # Search for each data type in the config (skip 'ingestion' key)
        for data_type in self.config.keys():
            if data_type == 'ingestion':
                continue
            if data_type in filename_lower:
                return data_type

        logging.warning(f"❓ Could not detect type for file: {filename}")
        return None

    def get_table_name(self, file_type: str) -> Optional[str]:
        """
        Get table name for a file type

        Args:
            file_type: File type (e.g., 'sleep')

        Returns:
            Table name (e.g., 'normalized_garmin__sleep') or None
        """
        # Read table from data type config
        if file_type in self.config:
            return self.config[file_type].get('table')
        return None

    def list_files(self) -> List[str]:
        """List files to process from GCS landing"""
        prefix = "garmin/landing/"
        blobs = self.storage_client.list_blobs(self.bucket, prefix=prefix)

        files = []
        for blob in blobs:
            if blob.name.endswith('.jsonl'):
                files.append(f"gs://{self.bucket}/{blob.name}")

        logging.info(f"📂 Found {len(files)} JSONL files in gs://{self.bucket}/{prefix}")
        return files

    def download_file(self, gcs_uri: str) -> Tuple[List[str], str]:
        """
        Download file from GCS

        Args:
            gcs_uri: GCS URI

        Returns:
            Tuple of (lines, filename)
        """
        parts = gcs_uri.replace('gs://', '').split('/')
        bucket_name = parts[0]
        blob_path = '/'.join(parts[1:])
        filename = parts[-1]

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        content = blob.download_as_text()
        lines = content.splitlines()

        return lines, filename

    def parse_jsonl(self, lines: List[str], filename: str) -> List[Dict]:
        """
        Parse JSONL lines

        Args:
            lines: JSONL lines
            filename: Source filename

        Returns:
            Parsed records with metadata
        """
        from .utils import flatten_nested_arrays
        
        records = []
        inserted_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

        for line_num, line in enumerate(lines, 1):
            try:
                record = json.loads(line)
                
                # Clean nested arrays and empty structs for BigQuery compatibility
                record = flatten_nested_arrays(record)

                # Add metadata
                record['_dp_inserted_at'] = inserted_at
                record['_source_file'] = filename

                records.append(record)

            except json.JSONDecodeError as e:
                logging.warning(f"⚠️  Invalid JSON on line {line_num}: {e}")
                continue

        logging.info(f"   Parsed {len(records)} records from {len(lines)} lines")
        return records

    def ingest_to_bigquery(self, records: List[Dict], table_name: str) -> None:
        """
        Ingest records to BigQuery with auto-detection

        Args:
            records: Records to ingest
            table_name: Table name (without dataset/project)
        """
        if not records:
            logging.warning("   No records to ingest")
            return

        table_id = f"{self.project_id}.{self.dataset}.{table_name}"

        # Job config with auto-detection
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,  # ✨ Auto-détection du schéma
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
        )

        logging.info(f"   📊 Ingesting to {table_id}")
        logging.info(f"      Records: {len(records)}")
        logging.info(f"      Auto-detect: True")

        if self.dry_run:
            logging.info("   🔍 DRY RUN - Skipping actual ingestion")
            return

        try:
            job = self.bq_client.load_table_from_json(
                records,
                table_id,
                job_config=job_config
            )

            job.result(timeout=600)

            logging.info(f"   ✅ Successfully ingested {len(records)} records")

        except GoogleCloudError as e:
            logging.error(f"   ❌ BigQuery error: {e}")
            raise

    def move_file(self, gcs_uri: str, destination: str) -> None:
        """
        Move file to archive or rejected

        Args:
            gcs_uri: Source GCS URI
            destination: 'archive' or 'rejected'
        """
        parts = gcs_uri.replace('gs://', '').split('/')
        bucket_name = parts[0]
        source_path = '/'.join(parts[1:])
        filename = parts[-1]

        dest_path = f"garmin/{destination}/{filename}"

        bucket = self.storage_client.bucket(bucket_name)
        source_blob = bucket.blob(source_path)

        bucket.copy_blob(source_blob, bucket, dest_path)
        source_blob.delete()

        logging.info(f"   📁 Moved to {destination}/")

    def process_file(self, gcs_uri: str) -> bool:
        """
        Process a single file

        Args:
            gcs_uri: GCS file URI

        Returns:
            True if successful, False otherwise
        """
        filename = gcs_uri.split('/')[-1]

        logging.info(f"\n{'='*80}")
        logging.info(f"📄 Processing: {filename}")
        logging.info(f"{'='*80}")

        try:
            # 1. Detect type
            file_type = self.detect_file_type(filename)
            if not file_type:
                logging.error(f"   ❌ Unknown file type")
                return False

            logging.info(f"   Type detected: {file_type}")

            # 2. Get table name
            table_name = self.get_table_name(file_type)
            if not table_name:
                logging.error(f"   ❌ No table mapping for type: {file_type}")
                return False

            logging.info(f"   Table: {table_name}")

            # 3. Download file
            lines, _ = self.download_file(gcs_uri)
            logging.info(f"   Downloaded {len(lines)} lines")

            # 4. Parse records
            records = self.parse_jsonl(lines, filename)

            # 5. Ingest to BigQuery
            self.ingest_to_bigquery(records, table_name)

            return True

        except Exception as e:
            logging.error(f"   ❌ Error: {e}")
            return False

    def run(self) -> int:
        """
        Run ingestion pipeline

        Returns:
            Exit code (0 = success, 1 = error)
        """
        logging.info(f"\n{'='*80}")
        logging.info(f"🚀 STARTING SIMPLE GARMIN INGESTION")
        logging.info(f"{'='*80}\n")

        try:
            # List files
            files = self.list_files()

            if not files:
                logging.warning("⚠️  No files to process")
                return 0

            # Process each file
            success_count = 0
            failed_count = 0

            for gcs_uri in files:
                success = self.process_file(gcs_uri)

                if success:
                    success_count += 1

                    # Archive successful file (skip in dry-run)
                    if not self.dry_run:
                        self.move_file(gcs_uri, 'archive')
                else:
                    failed_count += 1

                    # Move failed file (skip in dry-run)
                    if not self.dry_run:
                        self.move_file(gcs_uri, 'rejected')

            # Summary
            logging.info(f"\n{'='*80}")
            logging.info(f"📊 SUMMARY")
            logging.info(f"{'='*80}")
            logging.info(f"✅ Successful: {success_count}")
            logging.info(f"❌ Failed: {failed_count}")
            logging.info(f"Total: {len(files)}")
            logging.info(f"{'='*80}\n")

            return 0 if failed_count == 0 else 1

        except Exception as e:
            logging.error(f"❌ Fatal error: {e}")
            return 1


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Simple Garmin ingestion using metrics.yaml mapping'
    )

    parser.add_argument(
        '--env',
        choices=['dev', 'prd'],
        required=True,
        help='Environment (dev or prd)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate without writing to BigQuery'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )

    args = parser.parse_args()

    # Configure logging
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Run ingestion
    ingestor = SimpleGarminIngestor(env=args.env, dry_run=args.dry_run)
    exit_code = ingestor.run()

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
