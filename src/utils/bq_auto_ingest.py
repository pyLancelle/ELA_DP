#!/usr/bin/env python3
"""
Generic BigQuery Auto-Ingestion Utility
========================================
A universal script to ingest JSONL files into BigQuery with automatic schema detection.

Features:
- Auto-detects schema from JSON data
- Supports GCS and local file paths
- Configurable dataset and table naming
- Partitioning and clustering support
- Archive/rejected file management

Usage:
    # Ingest from GCS with auto-detection
    python -m src.utils.bq_auto_ingest \
        --source gs://ela-dp-dev/garmin/landing/2024_11_22_activities.jsonl \
        --dataset dp_normalized_dev \
        --table lake_garmin__normalized_activities

    # Ingest from local file
    python -m src.utils.bq_auto_ingest \
        --source /path/to/data.jsonl \
        --dataset dp_normalized_dev \
        --table my_table \
        --partition-field timestamp \
        --clustering activity_id,user_id

    # Batch ingest all files from GCS path
    python -m src.utils.bq_auto_ingest \
        --source gs://ela-dp-dev/garmin/landing/ \
        --dataset dp_normalized_dev \
        --table-pattern "lake_garmin__normalized_{data_type}" \
        --archive-path gs://ela-dp-dev/garmin/archive/
"""

import argparse
import json
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
from google.cloud import bigquery, storage
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import GoogleCloudError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class BigQueryAutoIngestor:
    """Generic BigQuery ingestion with auto-detection"""

    def __init__(
        self,
        project_id: Optional[str] = None,
        dry_run: bool = False
    ):
        """
        Initialize BigQuery ingestion utility

        Args:
            project_id: GCP project ID (defaults to GCP_PROJECT_ID env var)
            dry_run: If True, validate but don't write to BigQuery
        """
        self.project_id = project_id or os.getenv('GCP_PROJECT_ID')
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID must be provided or set as environment variable")

        self.dry_run = dry_run
        self.bq_client = bigquery.Client(project=self.project_id)
        self.storage_client = storage.Client(project=self.project_id)

        logging.info(f"Initialized BigQuery Auto-Ingestor")
        logging.info(f"  Project: {self.project_id}")
        logging.info(f"  Dry run: {dry_run}")

    def is_gcs_path(self, path: str) -> bool:
        """Check if path is a GCS URI"""
        return path.startswith('gs://')

    def parse_gcs_uri(self, uri: str) -> Tuple[str, str]:
        """Parse GCS URI into bucket and blob path"""
        parts = uri.replace('gs://', '').split('/', 1)
        bucket_name = parts[0]
        blob_path = parts[1] if len(parts) > 1 else ''
        return bucket_name, blob_path

    def list_gcs_files(self, gcs_uri: str, pattern: str = '*.jsonl') -> List[str]:
        """
        List files in GCS path matching pattern

        Args:
            gcs_uri: GCS directory URI (gs://bucket/path/)
            pattern: File pattern to match (e.g., *.jsonl)

        Returns:
            List of GCS URIs
        """
        bucket_name, prefix = self.parse_gcs_uri(gcs_uri)
        
        # Ensure prefix ends with /
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        blobs = self.storage_client.list_blobs(bucket_name, prefix=prefix)
        
        files = []
        for blob in blobs:
            if blob.name.endswith('.jsonl'):
                files.append(f"gs://{bucket_name}/{blob.name}")

        logging.info(f"Found {len(files)} JSONL files in {gcs_uri}")
        return files

    def download_gcs_file(self, gcs_uri: str) -> List[str]:
        """
        Download JSONL file from GCS

        Args:
            gcs_uri: GCS file URI

        Returns:
            List of lines from the file
        """
        bucket_name, blob_path = self.parse_gcs_uri(gcs_uri)
        
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        content = blob.download_as_text()
        lines = content.splitlines()
        
        logging.info(f"Downloaded {len(lines)} lines from {gcs_uri}")
        return lines

    def read_local_file(self, file_path: str) -> List[str]:
        """
        Read JSONL file from local filesystem

        Args:
            file_path: Local file path

        Returns:
            List of lines from the file
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        logging.info(f"Read {len(lines)} lines from {file_path}")
        return lines

    def read_file(self, source: str) -> Tuple[List[str], str]:
        """
        Read file from GCS or local filesystem

        Args:
            source: GCS URI or local file path

        Returns:
            Tuple of (lines, filename)
        """
        if self.is_gcs_path(source):
            lines = self.download_gcs_file(source)
            filename = source.split('/')[-1]
        else:
            lines = self.read_local_file(source)
            filename = Path(source).name

        return lines, filename

    def parse_jsonl(self, lines: List[str], source_file: str) -> List[Dict[str, Any]]:
        """
        Parse JSONL lines into records

        Args:
            lines: List of JSON lines
            source_file: Source filename for metadata

        Returns:
            List of parsed records
        """
        records = []
        inserted_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

        for line_num, line in enumerate(lines, 1):
            try:
                record = json.loads(line)
                
                # Add metadata
                record['_dp_inserted_at'] = inserted_at
                record['_source_file'] = source_file
                
                records.append(record)

            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON on line {line_num}: {e}")
                continue

        logging.info(f"Parsed {len(records)} valid records from {len(lines)} lines")
        return records

    def _infer_field_type(self, value: Any) -> str:
        """Infer BigQuery type from python value"""
        if isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, dict):
            return "RECORD"
        elif isinstance(value, list):
            # Empty list or list of nulls defaults to STRING if unknown, 
            # but usually we check the first non-null item
            return "REPEATED" 
        return "STRING"

    def _merge_schema_fields(self, existing: List[SchemaField], new: List[SchemaField]) -> List[SchemaField]:
        """Merge two lists of SchemaFields, handling type promotion and nested fields"""
        merged_map = {f.name: f for f in existing}
        
        for field in new:
            if field.name not in merged_map:
                merged_map[field.name] = field
            else:
                # Conflict resolution / Type promotion
                current = merged_map[field.name]
                
                # If types match, check for nested fields merge if RECORD
                if current.field_type == field.field_type:
                    if current.field_type == "RECORD":
                        merged_sub = self._merge_schema_fields(current.fields, field.fields)
                        merged_map[field.name] = SchemaField(
                            name=current.name,
                            field_type="RECORD",
                            mode=current.mode, # Keep existing mode (likely NULLABLE or REPEATED)
                            fields=merged_sub
                        )
                else:
                    # Type promotion logic
                    # RECORD wins over STRING (for empty vs non-empty arrays)
                    if "RECORD" in {current.field_type, field.field_type}:
                        final_type = "RECORD"
                        final_fields = current.fields if current.field_type == "RECORD" else field.fields
                        merged_map[field.name] = SchemaField(
                            name=current.name,
                            field_type="RECORD",
                            mode=current.mode,
                            fields=final_fields
                        )
                    # INTEGER + FLOAT -> FLOAT
                    elif {current.field_type, field.field_type} == {"INTEGER", "FLOAT"}:
                        merged_map[field.name] = SchemaField(
                            name=current.name,
                            field_type="FLOAT",
                            mode=current.mode
                        )
                    # Otherwise fallback to STRING (safest)
                    elif current.field_type != "STRING":
                        merged_map[field.name] = SchemaField(
                            name=current.name,
                            field_type="STRING",
                            mode=current.mode
                        )

        return list(merged_map.values())

    def detect_schema(self, records: List[Dict[str, Any]]) -> List[SchemaField]:
        """
        Scan ALL records to build a complete schema.
        Handles nested records and lists.
        """
        schema_map: Dict[str, SchemaField] = {}

        for record in records:
            for key, value in record.items():
                if value is None:
                    continue

                # Determine mode and type
                mode = "NULLABLE"
                field_type = "STRING"
                fields = ()

                if isinstance(value, list):
                    mode = "REPEATED"
                    if value:
                        # Inspect first non-null item to guess type
                        # (Simplification: assumes homogeneous lists for now)
                        sample = next((x for x in value if x is not None), None)
                        if sample is not None:
                            field_type = self._infer_field_type(sample)
                            if field_type == "RECORD":
                                # Recursively detect schema for the list of dicts
                                # We treat the list of dicts as a list of records to merge
                                sub_schema = self.detect_schema(value)
                                fields = tuple(sub_schema)
                        else:
                            # List of all None or empty, default to STRING
                            field_type = "STRING"
                else:
                    field_type = self._infer_field_type(value)
                    if field_type == "RECORD":
                        sub_schema = self.detect_schema([value])
                        fields = tuple(sub_schema)

                new_field = SchemaField(key, field_type, mode=mode, fields=fields)

                # Merge with existing
                if key not in schema_map:
                    schema_map[key] = new_field
                else:
                    # Merge logic
                    current = schema_map[key]
                    
                    # 1. Mode promotion: NULLABLE wins over REQUIRED (though we default to NULLABLE)
                    # REPEATED is distinct. If one is REPEATED and other is not, that's a schema conflict error usually.
                    # For now assume consistent structure (list vs non-list).
                    
                    # 2. Type promotion
                    final_type = current.field_type
                    final_fields = current.fields

                    if current.field_type != field_type:
                        # Special case: RECORD should win over STRING
                        # This handles the case where empty arrays are detected as STRING
                        # but non-empty arrays are detected as RECORD
                        if "RECORD" in {current.field_type, field_type}:
                            final_type = "RECORD"
                            # Use the fields from whichever one is RECORD
                            final_fields = current.fields if current.field_type == "RECORD" else fields
                        elif {current.field_type, field_type} == {"INTEGER", "FLOAT"}:
                            final_type = "FLOAT"
                        else:
                            final_type = "STRING"
                            final_fields = () # String doesn't have sub-fields
                    
                    # 3. Merge nested fields if both are RECORD
                    if final_type == "RECORD" and current.field_type == "RECORD" and field_type == "RECORD":
                        final_fields = tuple(self._merge_schema_fields(list(current.fields), list(fields)))
                    
                    schema_map[key] = SchemaField(key, final_type, mode=current.mode, fields=final_fields)

        return list(schema_map.values())

    def ingest_to_bigquery(
        self,
        records: List[Dict[str, Any]],
        dataset: str,
        table: str,
        partition_field: Optional[str] = None,
        partition_type: str = 'DAY',
        clustering_fields: Optional[List[str]] = None,
        write_disposition: str = 'WRITE_APPEND'
    ) -> None:
        """
        Ingest records to BigQuery with auto-detection

        Args:
            records: List of records to ingest
            dataset: BigQuery dataset name
            table: BigQuery table name
            partition_field: Field to partition by (optional)
            partition_type: Partition type (DAY, MONTH, YEAR)
            clustering_fields: List of fields to cluster by (optional)
            write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE)
        """
        if not records:
            logging.warning("No records to ingest")
            return

        table_id = f"{self.project_id}.{dataset}.{table}"

        # Configure load job with auto-detection
        # Detect schema from ALL records
        logging.info("Detecting schema from all records...")
        detected_schema = self.detect_schema(records)
        logging.info(f"Detected {len(detected_schema)} top-level fields")

        # Configure load job with explicit schema
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=detected_schema,
            autodetect=False,  # We provide explicit schema
            write_disposition=write_disposition,
            create_disposition='CREATE_IF_NEEDED',
            ignore_unknown_values=True,  # Ignore fields not in schema (e.g. always null fields)
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )

        # Configure partitioning
        if partition_field:
            partition_types = {
                'DAY': bigquery.TimePartitioningType.DAY,
                'MONTH': bigquery.TimePartitioningType.MONTH,
                'YEAR': bigquery.TimePartitioningType.YEAR,
            }
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=partition_types.get(partition_type, bigquery.TimePartitioningType.DAY),
                field=partition_field
            )
            logging.info(f"Partitioning configured: {partition_type} on {partition_field}")

        # Configure clustering
        if clustering_fields:
            job_config.clustering_fields = clustering_fields
            logging.info(f"Clustering configured on: {', '.join(clustering_fields)}")

        # Log ingestion details
        logging.info(f"Ingesting {len(records)} records to {table_id}")
        logging.info(f"  Schema fields: {len(detected_schema)}")
        logging.info(f"  Write disposition: {write_disposition}")
        
        # Debug: Log splitSummaries schema
        for field in detected_schema:
            if field.name == "splitSummaries":
                logging.info(f"  DEBUG - splitSummaries field:")
                logging.info(f"    Type: {field.field_type}, Mode: {field.mode}")
                logging.info(f"    Has nested fields: {len(field.fields) if field.fields else 0}")
                break

        if self.dry_run:
            logging.info("DRY RUN - Skipping actual ingestion")
            logging.info(f"Sample record: {json.dumps(records[0], indent=2, default=str)}")
            return

        # Load to BigQuery
        # IMPORTANT: We must use load_table_from_file with NEWLINE_DELIMITED_JSON
        # because load_table_from_json doesn't properly handle REPEATED RECORD fields
        try:
            import tempfile
            
            # Write records to temporary JSONL file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                for record in records:
                    tmp_file.write(json.dumps(record, default=str) + '\n')
            
            # Load from file
            with open(tmp_path, 'rb') as source_file:
                job = self.bq_client.load_table_from_file(
                    source_file,
                    table_id,
                    job_config=job_config
                )

                # Wait for job to complete
                job.result(timeout=600)

            # Clean up temp file
            import os
            os.unlink(tmp_path)

            logging.info(f"✅ Successfully ingested {len(records)} records to {table_id}")

        except GoogleCloudError as e:
            logging.error(f"❌ BigQuery ingestion failed: {e}")
            raise

    def move_gcs_file(self, source_uri: str, dest_uri: str) -> None:
        """
        Move file from source to destination in GCS

        Args:
            source_uri: Source GCS URI
            dest_uri: Destination GCS URI (can be directory)
        """
        source_bucket, source_path = self.parse_gcs_uri(source_uri)
        dest_bucket, dest_path = self.parse_gcs_uri(dest_uri)

        # If dest is a directory, append filename
        if dest_path.endswith('/'):
            filename = source_path.split('/')[-1]
            dest_path = dest_path + filename

        # Copy and delete
        source_bucket_obj = self.storage_client.bucket(source_bucket)
        source_blob = source_bucket_obj.blob(source_path)

        dest_bucket_obj = self.storage_client.bucket(dest_bucket)
        source_bucket_obj.copy_blob(source_blob, dest_bucket_obj, dest_path)
        source_blob.delete()

        logging.info(f"📁 Moved {source_uri} → {dest_uri}")

    def ingest_file(
        self,
        source: str,
        dataset: str,
        table: str,
        partition_field: Optional[str] = None,
        partition_type: str = 'DAY',
        clustering_fields: Optional[List[str]] = None,
        archive_path: Optional[str] = None,
        rejected_path: Optional[str] = None
    ) -> bool:
        """
        Ingest a single file to BigQuery

        Args:
            source: Source file path (GCS or local)
            dataset: BigQuery dataset
            table: BigQuery table
            partition_field: Field to partition by
            partition_type: Partition type
            clustering_fields: Fields to cluster by
            archive_path: GCS path to archive successful files
            rejected_path: GCS path for failed files

        Returns:
            True if successful, False otherwise
        """
        try:
            # Read file
            lines, filename = self.read_file(source)

            # Parse records
            records = self.parse_jsonl(lines, filename)

            # Ingest to BigQuery
            self.ingest_to_bigquery(
                records=records,
                dataset=dataset,
                table=table,
                partition_field=partition_field,
                partition_type=partition_type,
                clustering_fields=clustering_fields
            )

            # Archive successful file
            if archive_path and self.is_gcs_path(source) and not self.dry_run:
                self.move_gcs_file(source, archive_path)

            return True

        except Exception as e:
            logging.error(f"❌ Failed to ingest {source}: {e}")

            # Move to rejected
            if rejected_path and self.is_gcs_path(source) and not self.dry_run:
                try:
                    self.move_gcs_file(source, rejected_path)
                except Exception as move_error:
                    logging.error(f"Failed to move rejected file: {move_error}")

            return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Generic BigQuery auto-ingestion utility',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--source',
        required=True,
        help='Source file or directory (GCS URI or local path)'
    )

    parser.add_argument(
        '--dataset',
        required=True,
        help='BigQuery dataset name'
    )

    parser.add_argument(
        '--table',
        required=True,
        help='BigQuery table name'
    )

    parser.add_argument(
        '--project-id',
        help='GCP project ID (defaults to GCP_PROJECT_ID env var)'
    )

    parser.add_argument(
        '--partition-field',
        help='Field to partition by (e.g., _dp_inserted_at)'
    )

    parser.add_argument(
        '--partition-type',
        choices=['DAY', 'MONTH', 'YEAR'],
        default='DAY',
        help='Partition type (default: DAY)'
    )

    parser.add_argument(
        '--clustering',
        help='Comma-separated list of fields to cluster by (e.g., activity_id,user_id)'
    )

    parser.add_argument(
        '--archive-path',
        help='GCS path to archive successful files (e.g., gs://bucket/archive/)'
    )

    parser.add_argument(
        '--rejected-path',
        help='GCS path for failed files (e.g., gs://bucket/rejected/)'
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

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Parse clustering fields
    clustering_fields = None
    if args.clustering:
        clustering_fields = [f.strip() for f in args.clustering.split(',')]

    # Initialize ingestor
    ingestor = BigQueryAutoIngestor(
        project_id=args.project_id,
        dry_run=args.dry_run
    )

    # Check if source is directory or file
    if ingestor.is_gcs_path(args.source) and args.source.endswith('/'):
        # Batch mode: process all files in directory
        logging.info(f"📂 Batch mode: processing directory {args.source}")
        
        files = ingestor.list_gcs_files(args.source)
        
        success_count = 0
        failed_count = 0

        for file_uri in files:
            logging.info(f"\n{'='*80}")
            logging.info(f"Processing: {file_uri}")
            logging.info(f"{'='*80}")

            success = ingestor.ingest_file(
                source=file_uri,
                dataset=args.dataset,
                table=args.table,
                partition_field=args.partition_field,
                partition_type=args.partition_type,
                clustering_fields=clustering_fields,
                archive_path=args.archive_path,
                rejected_path=args.rejected_path
            )

            if success:
                success_count += 1
            else:
                failed_count += 1

        # Summary
        logging.info(f"\n{'='*80}")
        logging.info(f"📊 SUMMARY")
        logging.info(f"{'='*80}")
        logging.info(f"✅ Successful: {success_count}")
        logging.info(f"❌ Failed: {failed_count}")
        logging.info(f"Total: {len(files)}")

        sys.exit(0 if failed_count == 0 else 1)

    else:
        # Single file mode
        logging.info(f"📄 Single file mode: {args.source}")

        success = ingestor.ingest_file(
            source=args.source,
            dataset=args.dataset,
            table=args.table,
            partition_field=args.partition_field,
            partition_type=args.partition_type,
            clustering_fields=clustering_fields,
            archive_path=args.archive_path,
            rejected_path=args.rejected_path
        )

        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
