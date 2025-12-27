#!/usr/bin/env python3
"""
Generic Spotify Data Ingestion Engine - Configuration-Driven Approach
======================================================================

This script provides a declarative, configuration-driven ingestion pipeline
for Spotify data with support for hybrid parsing (typed columns + raw JSON).

Features:
- YAML-based configuration (single source of truth)
- Hybrid parsing: core fields typed, extended fields in JSON
- Automatic schema generation from config
- Data quality validation
- Error handling and retry logic
- Performance optimization (batching, parallelization)
- Comprehensive logging and metrics
- Idempotent operations

Architecture:
    GCS Landing → Parse (YAML config) → Validate → BigQuery → Archive

Usage:
    # Basic ingestion
    python -m src.connectors.spotify.spotify_ingest --config recently_played --env dev

    # With options
    python -m src.connectors.spotify.spotify_ingest \\
        --config recently_played \\
        --env prd \\
        --log-level DEBUG \\
        --dry-run

    # Process specific file
    python -m src.connectors.spotify.spotify_ingest \\
        --config recently_played \\
        --env dev \\
        --file gs://bucket/path/to/file.jsonl

Author: Data Platform Team
Version: 2.0.0
Last Updated: 2025-11-01
"""

import argparse
import json
import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

try:
    from google.cloud import bigquery, storage
    from google.cloud.exceptions import GoogleCloudError
except ImportError:
    print("ERROR: Missing dependencies. Install with:")
    print("  pip install google-cloud-bigquery google-cloud-storage pyyaml")
    sys.exit(1)


# =============================================================================
# CONFIGURATION MODELS
# =============================================================================


@dataclass
class FieldConfig:
    """Configuration for a single field (supports nested RECORD)"""

    name: str
    json_path: str = ""
    bq_type: str = "STRING"
    mode: str = "NULLABLE"
    description: str = ""
    transform: Optional[str] = None
    validations: List[Dict[str, Any]] = field(default_factory=list)
    is_unique_key: bool = False
    max_length: Optional[int] = None

    # RECORD support (nested fields)
    fields: Optional[List["FieldConfig"]] = None

    # Wildcard support (for dynamic maps like deviceId)
    json_path_base: Optional[str] = None

    # Array index support (for REPEATED RECORD with sub-arrays)
    array_index: Optional[int] = None


@dataclass
class IngestionMetrics:
    """Metrics tracked during ingestion"""

    files_found: int = 0
    files_processed: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    records_read: int = 0
    records_parsed: int = 0
    records_validated: int = 0
    records_inserted: int = 0
    records_rejected: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def duration_seconds(self) -> float:
        """Calculate duration"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.files_processed == 0:
            return 0.0
        return (self.files_succeeded / self.files_processed) * 100

    def rejection_rate(self) -> float:
        """Calculate rejection rate"""
        if self.records_read == 0:
            return 0.0
        return (self.records_rejected / self.records_read) * 100


class IngestionConfig:
    """Parsed YAML configuration"""

    def __init__(self, config_path: Path):
        """Load and parse YAML configuration"""
        self.config_path = config_path

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration not found: {config_path}")

        with open(config_path, "r") as f:
            self.raw_config = yaml.safe_load(f)

        # Parse sections
        self.data_type = self.raw_config["data_type"]
        self.description = self.raw_config.get("description", "")
        self.version = self.raw_config.get("version", "1.0.0")

        self.source = self.raw_config["source"]
        self.destination = self.raw_config["destination"]
        self.parsing = self.raw_config["parsing"]
        self.metadata_fields = self.raw_config.get("metadata_fields", [])
        self.quality_checks = self.raw_config.get("quality_checks", {})
        self.performance = self.raw_config.get("performance", {})
        self.logging_config = self.raw_config.get("logging", {})
        self.array_expansion = self.parsing.get("array_expansion")

        logging.info(f"Loaded config: {self.data_type} v{self.version}")
        logging.info(f"Description: {self.description}")
        if self.array_expansion:
            logging.info(
                f"Array expansion enabled: {self.array_expansion['array_path']}"
            )

    def get_core_fields(self) -> List[FieldConfig]:
        """Parse core fields into FieldConfig objects (recursive for RECORD)"""
        fields = []
        for field_dict in self.parsing.get("core_fields", []):
            fields.append(self._parse_field_config(field_dict))
        return fields

    def _parse_field_config(self, field_dict: Dict[str, Any]) -> FieldConfig:
        """
        Parse single field config (handles nested fields recursively)

        Args:
            field_dict: Field configuration dictionary from YAML

        Returns:
            FieldConfig object with nested fields if RECORD
        """
        # Valid fields for FieldConfig
        valid_fields = {
            "name",
            "json_path",
            "bq_type",
            "mode",
            "description",
            "transform",
            "validations",
            "is_unique_key",
            "max_length",
            "json_path_base",
            "array_index",
        }
        filtered_dict = {k: v for k, v in field_dict.items() if k in valid_fields}

        # Parse nested fields recursively for RECORD
        if "fields" in field_dict:
            filtered_dict["fields"] = [
                self._parse_field_config(nested) for nested in field_dict["fields"]
            ]

        return FieldConfig(**filtered_dict)

    def get_bucket_name(self, env: str) -> str:
        """Get bucket name for environment"""
        pattern = self.source["bucket_pattern"]
        return pattern.format(env=env)

    def get_table_id(self, project_id: str, env: str) -> str:
        """Build full BigQuery table ID"""
        dataset_pattern = self.destination["dataset_pattern"]
        dataset = dataset_pattern.format(env=env)
        table = self.destination["table_name"]
        return f"{project_id}.{dataset}.{table}"

    def get_batch_size(self) -> int:
        """Get batch size for BigQuery inserts"""
        return self.performance.get("batch_size", 500)

    def get_validation_mode(self) -> str:
        """Get validation mode"""
        return self.quality_checks.get("validation_mode", "warn")


# =============================================================================
# DATA TRANSFORMATIONS
# =============================================================================


class DataTransformer:
    """Apply transformations to extracted values"""

    @staticmethod
    def transform(value: Any, transform_type: Optional[str]) -> Any:
        """
        Apply transformation to value

        Args:
            value: Raw value from JSON
            transform_type: Type of transformation to apply

        Returns:
            Transformed value
        """
        if value is None or transform_type is None:
            return value

        try:
            if transform_type == "timestamp_ms_to_timestamp":
                # Convert Garmin timestamp (milliseconds or string) to Python datetime (UTC)
                if isinstance(value, (int, float)):
                    from datetime import timezone

                    return datetime.fromtimestamp(
                        value / 1000.0, tz=timezone.utc
                    ).replace(tzinfo=None)
                elif isinstance(value, str):
                    # Parse string datetime (format: "YYYY-MM-DD HH:MM:SS")
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

            elif transform_type == "timestamp_ms_to_date":
                # Convert Garmin timestamp to date only (UTC)
                if isinstance(value, (int, float)):
                    from datetime import timezone

                    return datetime.fromtimestamp(
                        value / 1000.0, tz=timezone.utc
                    ).date()
                elif isinstance(value, str):
                    # Parse string datetime and extract date
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").date()

            elif transform_type == "string_to_date":
                # Convert string date (YYYY-MM-DD) or ISO timestamp to date
                if isinstance(value, str):
                    # Remove trailing .0 if present
                    value = value.replace(".0", "")
                    # Try ISO format first (YYYY-MM-DDTHH:MM:SS)
                    if "T" in value:
                        return datetime.fromisoformat(value).date()
                    # Try simple date format (YYYY-MM-DD)
                    else:
                        return datetime.strptime(value, "%Y-%m-%d").date()
                return value

            elif transform_type == "string_to_timestamp":
                # Convert ISO string to timestamp
                if isinstance(value, str):
                    # Remove trailing .0 if present
                    value = value.replace(".0", "")
                    return datetime.fromisoformat(value)
                return value

            elif transform_type == "string_to_int":
                return int(value)

            elif transform_type == "string_to_float":
                return float(value)

            elif transform_type == "string_to_timestamp_iso":
                # Convert ISO 8601 timestamp to Python datetime (Spotify format)
                if isinstance(value, str):
                    # Handle Spotify format: 2025-11-01T10:34:56.074Z
                    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(
                        tzinfo=None
                    )
                return value

            elif transform_type == "string_to_date_flexible":
                # Convert YYYY, YYYY-MM, or YYYY-MM-DD to date
                if isinstance(value, str):
                    if len(value) == 4:  # YYYY
                        return datetime.strptime(value, "%Y").date()
                    elif len(value) == 7:  # YYYY-MM
                        return datetime.strptime(value, "%Y-%m").date()
                    else:  # YYYY-MM-DD
                        return datetime.strptime(value, "%Y-%m-%d").date()
                return value

        except (ValueError, TypeError, OSError) as e:
            logging.warning(f"Transform failed for {transform_type}: {value} - {e}")
            return None

        return value


# =============================================================================
# SCHEMA GENERATION
# =============================================================================


class SchemaGenerator:
    """Generate BigQuery schema from configuration"""

    @staticmethod
    def generate(config: IngestionConfig) -> List[bigquery.SchemaField]:
        """
        Generate complete BigQuery schema (recursive for RECORD)

        Args:
            config: Ingestion configuration

        Returns:
            List of BigQuery SchemaField objects
        """
        schema = []

        # Core fields (recursive for RECORD)
        for field in config.get_core_fields():
            schema.append(SchemaGenerator._generate_field(field))

        # Metadata fields
        for meta_field in config.metadata_fields:
            schema.append(
                bigquery.SchemaField(
                    name=meta_field["name"],
                    field_type=meta_field["bq_type"],
                    mode=meta_field.get("mode", "NULLABLE"),
                    description=meta_field.get("description", ""),
                )
            )

        return schema

    @staticmethod
    def _generate_field(field: FieldConfig) -> bigquery.SchemaField:
        """
        Generate SchemaField (recursive for RECORD)

        Args:
            field: Field configuration

        Returns:
            BigQuery SchemaField (with nested fields if RECORD)
        """
        if field.bq_type == "RECORD" and field.fields:
            # Recursive generation for nested fields
            nested_fields = [
                SchemaGenerator._generate_field(nested) for nested in field.fields
            ]
            return bigquery.SchemaField(
                name=field.name,
                field_type="RECORD",
                mode=field.mode,
                description=field.description,
                fields=nested_fields,
            )
        else:
            # Simple field
            return bigquery.SchemaField(
                name=field.name,
                field_type=field.bq_type,
                mode=field.mode,
                description=field.description,
            )


# =============================================================================
# DATA PARSING & VALIDATION
# =============================================================================


class DataParser:
    """Parse raw JSON according to configuration"""

    def __init__(self, config: IngestionConfig):
        self.config = config
        self.transformer = DataTransformer()
        self.core_fields = config.get_core_fields()
        self.validation_mode = config.get_validation_mode()

    def extract_value(
        self, data: Dict[str, Any], json_path: str, array_index: Optional[int] = None
    ) -> Any:
        """
        Extract value from nested JSON using JSONPath

        Supports:
        - Standard paths: $.activityType.typeId
        - Wildcard paths: $.devices.*.deviceId (takes first match)
        - Array index: array_index=0 for first element of sub-array

        Args:
            data: Source JSON dictionary
            json_path: JSONPath string (e.g., "$.activityType.typeId")
            array_index: Optional index for array element extraction

        Returns:
            Extracted value or None if not found
        """
        if not json_path:
            return None

        # Remove $. prefix if present
        path = json_path.replace("$.", "")

        # Navigate nested structure
        current = data
        keys = path.split(".")

        for key in keys:
            # Wildcard support: take first match
            if key == "*":
                if isinstance(current, dict):
                    # Take first key in dict (Python 3.7+ maintains insertion order)
                    if len(current) > 0:
                        first_key = next(iter(current))
                        current = current[first_key]
                    else:
                        return None
                else:
                    return None
            # Normal key navigation
            elif isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None

        # Array index extraction (for REPEATED RECORD)
        if array_index is not None:
            if isinstance(current, list) and len(current) > array_index:
                return current[array_index]
            else:
                return None

        return current

    def cast_value(self, value: Any, bq_type: str) -> Any:
        """
        Cast value to target BigQuery type

        Args:
            value: Raw value
            bq_type: Target BigQuery type

        Returns:
            Casted value or None if cast fails
        """
        if value is None:
            return None

        try:
            if bq_type == "INT64":
                return int(value)
            elif bq_type == "FLOAT64":
                return float(value)
            elif bq_type == "STRING":
                return str(value)
            elif bq_type == "BOOL":
                return bool(value)
            elif bq_type in ["TIMESTAMP", "DATE"]:
                # Already transformed, return as-is
                return value
            else:
                return value
        except (ValueError, TypeError) as e:
            logging.debug(f"Cast failed for {bq_type}: {value} - {e}")
            return None

    def validate_field(
        self, field: FieldConfig, value: Any
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate field value against configured rules

        Args:
            field: Field configuration
            value: Value to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        for validation in field.validations:
            val_type = validation.get("type")

            if val_type == "not_null":
                if value is None:
                    return False, f"{field.name}: value is null"

            elif val_type == "positive":
                if value is not None and value <= 0:
                    return False, f"{field.name}: value must be positive, got {value}"

            elif val_type == "range":
                if value is not None:
                    min_val = validation.get("min")
                    max_val = validation.get("max")

                    if min_val is not None and value < min_val:
                        return False, f"{field.name}: value {value} below min {min_val}"
                    if max_val is not None and value > max_val:
                        return False, f"{field.name}: value {value} above max {max_val}"

            elif val_type == "date_range":
                if value is not None:
                    try:
                        min_date = datetime.strptime(
                            validation.get("min"), "%Y-%m-%d"
                        ).date()
                        max_date = datetime.strptime(
                            validation.get("max"), "%Y-%m-%d"
                        ).date()

                        if isinstance(value, datetime):
                            value = value.date()

                        if value < min_date or value > max_date:
                            return False, f"{field.name}: date {value} outside range"
                    except Exception as e:
                        logging.debug(f"Date validation error: {e}")

        # Check max length for strings
        if field.max_length and isinstance(value, str):
            if len(value) > field.max_length:
                return (
                    False,
                    f"{field.name}: length {len(value)} exceeds max {field.max_length}",
                )

        return True, None

    def parse_record(
        self, raw_data: Dict[str, Any], source_file: str
    ) -> List[Dict[str, Any]]:
        """
        Parse raw JSON record into structured format
        Supports array expansion for time-series data

        Args:
            raw_data: Raw JSON from source file
            source_file: Source filename for audit

        Returns:
            List of parsed record dictionaries (1 if no array expansion, N if array expanded)
        """
        # Check if array expansion is configured
        if self.config.array_expansion:
            return self._parse_with_array_expansion(raw_data, source_file)
        else:
            return [self._parse_single_record(raw_data, source_file)]

    def _parse_single_record(
        self, raw_data: Dict[str, Any], source_file: str
    ) -> Dict[str, Any]:
        """Parse a single record without array expansion (supports RECORD)"""
        parsed = {}

        # Extract and transform core fields (recursive for RECORD)
        for field in self.core_fields:
            parsed_value = self._parse_field_value(raw_data, field)
            parsed[field.name] = parsed_value

        # Add raw_data for extended fields
        parsed["raw_data"] = raw_data

        # Add metadata
        from datetime import timezone as tz

        parsed["dp_inserted_at"] = datetime.now(tz.utc).replace(tzinfo=None)
        parsed["source_file"] = source_file

        return parsed

    def _parse_field_value(
        self,
        data: Dict[str, Any],
        field: FieldConfig,
        base_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Parse field value (recursive for RECORD)

        Args:
            data: Source data (entire JSON)
            field: Field configuration
            base_data: Base data for relative paths (used in nested RECORD)

        Returns:
            Parsed value (primitive, dict for RECORD, or list for REPEATED)
        """
        # RECORD nested: construct dict recursively
        if field.bq_type == "RECORD" and field.fields:
            return self._parse_record_field(data, field)

        # Simple field: extract + transform + cast
        else:
            # Determine base for extraction
            extract_base = base_data if base_data is not None else data

            # Extract value (with wildcard and array_index support)
            value = self.extract_value(
                extract_base, field.json_path, array_index=field.array_index
            )

            # Apply transformation
            if field.transform:
                value = self.transformer.transform(value, field.transform)

            # Cast to target type
            value = self.cast_value(value, field.bq_type)

            return value

    def _parse_record_field(self, data: Dict[str, Any], field: FieldConfig) -> Any:
        """
        Parse RECORD field (nested dict or repeated list)

        Handles:
        1. RECORD simple: nested object
        2. RECORD with wildcard: take first match
        3. REPEATED RECORD: array of objects

        Args:
            data: Source data
            field: Field configuration (bq_type=RECORD)

        Returns:
            Dict for simple RECORD, List[Dict] for REPEATED RECORD, None if not found
        """
        # Extract base data for nested fields
        if field.json_path_base:
            # Wildcard base path (ex: $.devices.*)
            base_data = self.extract_value(data, field.json_path_base)
        elif field.json_path:
            # Standard base path (ex: $.vo2Max)
            base_data = self.extract_value(data, field.json_path)
        else:
            # No base path: use full data
            base_data = data

        if base_data is None:
            return None

        # REPEATED RECORD: array of objects
        if field.mode == "REPEATED":
            if not isinstance(base_data, list):
                logging.warning(
                    f"Expected array for REPEATED field {field.name}, got {type(base_data)}"
                )
                return None

            # Parse each array element as a record
            parsed_array = []
            for element in base_data:
                if isinstance(element, list):
                    # Array element is itself an array (time-series format)
                    # Ex: [timestamp, value]
                    record = {}
                    for nested_field in field.fields:
                        # Use array_index to extract from sub-array
                        if nested_field.array_index is not None:
                            if len(element) > nested_field.array_index:
                                value = element[nested_field.array_index]
                            else:
                                value = None
                        else:
                            value = None

                        # Transform and cast
                        if nested_field.transform:
                            value = self.transformer.transform(
                                value, nested_field.transform
                            )
                        value = self.cast_value(value, nested_field.bq_type)

                        record[nested_field.name] = value

                    parsed_array.append(record)

                elif isinstance(element, dict):
                    # Array element is a dict (standard object format)
                    record = {}
                    for nested_field in field.fields:
                        value = self._parse_field_value(
                            data, nested_field, base_data=element
                        )
                        record[nested_field.name] = value
                    parsed_array.append(record)

            return parsed_array

        # Simple RECORD: single nested object
        else:
            if not isinstance(base_data, dict):
                logging.warning(
                    f"Expected dict for RECORD field {field.name}, got {type(base_data)}"
                )
                return None

            # Parse nested fields
            record = {}
            for nested_field in field.fields:
                # Parse with relative path from base_data
                value = self._parse_field_value(data, nested_field, base_data=base_data)
                record[nested_field.name] = value

            return record

    def _parse_with_array_expansion(
        self, raw_data: Dict[str, Any], source_file: str
    ) -> List[Dict[str, Any]]:
        """
        Parse record with array expansion for time-series data

        Example config:
        array_expansion:
          array_path: "$.floorValuesArray"
          descriptor_path: "$.floorsValueDescriptorDTOList"
          descriptor_key_field: "key"
          descriptor_index_field: "index"

        Args:
            raw_data: Raw JSON with array data
            source_file: Source filename

        Returns:
            List of expanded records
        """
        expansion_config = self.config.array_expansion

        # Extract array data
        array_data = self.extract_value(raw_data, expansion_config["array_path"])
        if not array_data or not isinstance(array_data, list):
            logging.warning(
                f"Array path {expansion_config['array_path']} is empty or not a list"
            )
            return []

        # Extract descriptors (field name to array index mapping)
        descriptor_data = self.extract_value(
            raw_data, expansion_config["descriptor_path"]
        )
        if not descriptor_data:
            logging.warning(
                f"Descriptor path {expansion_config['descriptor_path']} not found"
            )
            return []

        # Build field mapping: {index: field_name}
        index_to_field = {}
        key_field = expansion_config.get("descriptor_key_field", "key")
        index_field = expansion_config.get("descriptor_index_field", "index")

        for descriptor in descriptor_data:
            field_name = descriptor.get(key_field)
            index = descriptor.get(index_field)
            if field_name and index is not None:
                index_to_field[index] = field_name

        # Expand array into records
        expanded_records = []
        from datetime import timezone as tz

        current_timestamp = datetime.now(tz.utc).replace(tzinfo=None)

        for array_element in array_data:
            if not isinstance(array_element, list):
                continue

            # Convert array element to dict using descriptors
            element_dict = {}
            for index, value in enumerate(array_element):
                field_name = index_to_field.get(index)
                if field_name:
                    element_dict[field_name] = value

            # Parse this element as a record
            parsed = {}
            for field in self.core_fields:
                # Extract from element_dict instead of raw_data
                value = element_dict.get(field.json_path.replace("$.", ""))

                # Apply transformation
                if field.transform:
                    value = self.transformer.transform(value, field.transform)

                # Cast to target type
                value = self.cast_value(value, field.bq_type)

                parsed[field.name] = value

            # Add raw_data (store the element data)
            parsed["raw_data"] = element_dict

            # Add metadata
            parsed["dp_inserted_at"] = current_timestamp
            parsed["source_file"] = source_file

            expanded_records.append(parsed)

        logging.info(
            f"Expanded {len(array_data)} array elements into {len(expanded_records)} records"
        )
        return expanded_records

    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate parsed record

        Args:
            record: Parsed record

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Check required fields
        required_fields = self.config.quality_checks.get("required_fields", [])
        for field_name in required_fields:
            if record.get(field_name) is None:
                errors.append(f"Missing required field: {field_name}")

        # Validate each core field
        for field in self.core_fields:
            value = record.get(field.name)
            is_valid, error = self.validate_field(field, value)
            if not is_valid:
                errors.append(error)

        is_valid = len(errors) == 0

        # Handle validation mode
        if not is_valid:
            if self.validation_mode == "strict":
                # Strict mode: reject record
                return False, errors
            elif self.validation_mode == "warn":
                # Warn mode: log but continue
                for error in errors:
                    logging.warning(f"Validation warning: {error}")
                return True, []  # Consider valid but logged
            else:
                # Skip mode: no validation
                return True, []

        return is_valid, errors


# =============================================================================
# INGESTION ORCHESTRATOR
# =============================================================================


class SpotifyIngestor:
    """Main ingestion orchestrator"""

    def __init__(self, config_path: Path, env: str, dry_run: bool = False):
        """
        Initialize ingestor

        Args:
            config_path: Path to YAML configuration file
            env: Environment (dev/prd)
            dry_run: If True, validate but don't write to BigQuery
        """
        self.config = IngestionConfig(config_path)
        self.env = env
        self.dry_run = dry_run
        self.parser = DataParser(self.config)
        self.metrics = IngestionMetrics()

        # Initialize GCP clients (auto-detect project if not set)
        self.project_id = os.getenv("GCP_PROJECT_ID")

        if self.project_id:
            self.bq_client = bigquery.Client(project=self.project_id)
            self.storage_client = storage.Client(project=self.project_id)
        else:
            # Auto-detect project from GCP environment (Cloud Run, GCE, etc.)
            self.bq_client = bigquery.Client()
            self.storage_client = storage.Client()
            self.project_id = self.bq_client.project
            logging.info(f"Auto-detected GCP project: {self.project_id}")

        self.bucket_name = self.config.get_bucket_name(env)
        self.table_id = self.config.get_table_id(self.project_id, env)

        logging.info(f"Initialized ingestor for: {self.config.data_type}")
        logging.info(f"Environment: {env}")
        logging.info(f"Bucket: {self.bucket_name}")
        logging.info(f"Table: {self.table_id}")
        logging.info(f"Dry run: {dry_run}")

    def list_source_files(self, specific_file: Optional[str] = None) -> List[str]:
        """
        List JSONL files to process

        Args:
            specific_file: If provided, process only this file

        Returns:
            List of GCS URIs
        """
        if specific_file:
            logging.info(f"Processing specific file: {specific_file}")
            return [specific_file]

        # List files from GCS
        landing_path = self.config.source["landing_path"]
        file_pattern = self.config.source["file_pattern"]

        prefix = f"{landing_path}/"
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)

        files = []
        pattern_suffix = file_pattern.replace("*", "")

        for blob in blobs:
            if blob.name.endswith(".jsonl") and pattern_suffix in blob.name:
                gcs_uri = f"gs://{self.bucket_name}/{blob.name}"
                files.append(gcs_uri)

        self.metrics.files_found = len(files)
        logging.info(f"Found {len(files)} files matching pattern: {file_pattern}")

        return files

    def download_file(self, gcs_uri: str) -> Tuple[List[str], str]:
        """
        Download JSONL file from GCS

        Args:
            gcs_uri: GCS URI (gs://bucket/path/file.jsonl)

        Returns:
            Tuple of (list of lines, filename)
        """
        # Parse URI
        parts = gcs_uri.replace("gs://", "").split("/")
        bucket_name = parts[0]
        blob_path = "/".join(parts[1:])
        filename = parts[-1]

        # Download
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        timeout = self.config.performance.get("gcs_download_timeout", 300)
        content = blob.download_as_text(timeout=timeout)
        lines = content.splitlines()

        logging.info(f"Downloaded {filename}: {len(lines)} lines")
        return lines, filename

    def process_file(self, gcs_uri: str) -> Tuple[bool, List[Dict[str, Any]], int]:
        """
        Process a single file (parse only, no insertion)

        Args:
            gcs_uri: GCS URI to process

        Returns:
            Tuple of (success, parsed_records, rejected_count)
        """
        logging.info(f"\n{'='*80}")
        logging.info(f"Processing: {gcs_uri}")
        logging.info(f"{'='*80}")

        try:
            # Download file
            lines, filename = self.download_file(gcs_uri)
            self.metrics.records_read += len(lines)

            # Parse records
            parsed_records = []
            rejected_count = 0

            for line_num, line in enumerate(lines, 1):
                try:
                    # Parse JSON
                    raw_data = json.loads(line)

                    # Parse according to config (returns list of records)
                    parsed_list = self.parser.parse_record(raw_data, filename)
                    self.metrics.records_parsed += len(parsed_list)

                    # Validate each record
                    for parsed in parsed_list:
                        is_valid, errors = self.parser.validate_record(parsed)

                        if is_valid:
                            parsed_records.append(parsed)
                            self.metrics.records_validated += 1
                        else:
                            rejected_count += 1
                            self.metrics.records_rejected += 1
                            if self.config.logging_config.get("log_rejected_records"):
                                logging.warning(
                                    f"Rejected record from line {line_num}: {errors}"
                                )

                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON on line {line_num}: {e}")
                    rejected_count += 1
                    self.metrics.records_rejected += 1

                except Exception as e:
                    logging.error(f"Error parsing line {line_num}: {e}")
                    rejected_count += 1
                    self.metrics.records_rejected += 1

            # Log sample records
            if self.config.logging_config.get("include_sample_records"):
                max_samples = self.config.logging_config.get("max_sample_records", 3)
                logging.info(f"\nSample records (showing first {max_samples}):")
                for i, record in enumerate(parsed_records[:max_samples], 1):
                    # Log without raw_data (too verbose)
                    sample = {k: v for k, v in record.items() if k != "raw_data"}
                    logging.info(f"  Record {i}: {sample}")

            logging.info(f"\n✅ File parsed successfully:")
            logging.info(f"   - Parsed: {len(parsed_records)}")
            logging.info(f"   - Rejected: {rejected_count}")

            return True, parsed_records, rejected_count

        except Exception as e:
            logging.error(f"❌ Failed to process file: {e}")
            logging.debug(traceback.format_exc())
            return False, [], 0

    def serialize_for_bigquery(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize record for BigQuery (convert datetime to ISO strings, recursive for RECORD)

        Args:
            record: Parsed record with Python objects

        Returns:
            Record with JSON-serializable values
        """
        import datetime as dt

        serialized = {}
        for key, value in record.items():
            serialized[key] = self._serialize_value(value)
        return serialized

    def _serialize_value(self, value: Any) -> Any:
        """
        Serialize a single value recursively

        Args:
            value: Value to serialize (can be dict, list, datetime, etc.)

        Returns:
            JSON-serializable value
        """
        import datetime as dt

        # Order matters: check datetime BEFORE date (datetime is subclass of date)
        if isinstance(value, dt.datetime):
            return value.isoformat()
        elif isinstance(value, dt.date):
            return value.isoformat()
        elif isinstance(value, dict):
            # Recursive for nested RECORD
            return {k: self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            # Recursive for REPEATED RECORD
            return [self._serialize_value(item) for item in value]
        else:
            return value

    def insert_to_bigquery(self, records: List[Dict[str, Any]]) -> None:
        """
        Insert records to BigQuery with partitioning and clustering

        Args:
            records: List of parsed records
        """
        if not records:
            return

        # Serialize records for BigQuery
        serialized_records = [self.serialize_for_bigquery(r) for r in records]

        # Generate schema
        schema = SchemaGenerator.generate(self.config)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
        )

        # Configure partitioning if enabled
        partition_config = self.config.destination.get("partition", {})
        if partition_config.get("enabled", False):
            partition_field = partition_config.get("field")
            partition_type = partition_config.get("type", "DAY")
            expiration_days = partition_config.get("expiration_days")

            if partition_field:
                # Time-based partitioning
                if partition_type == "DAY":
                    job_config.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=partition_field,
                        expiration_ms=(
                            expiration_days * 86400000 if expiration_days else None
                        ),
                    )
                elif partition_type == "MONTH":
                    job_config.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH,
                        field=partition_field,
                        expiration_ms=(
                            expiration_days * 86400000 if expiration_days else None
                        ),
                    )
                elif partition_type == "YEAR":
                    job_config.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.YEAR,
                        field=partition_field,
                        expiration_ms=(
                            expiration_days * 86400000 if expiration_days else None
                        ),
                    )

                logging.info(
                    f"Partitioning configured: {partition_type} on {partition_field}"
                )

        # Configure clustering if enabled
        clustering_config = self.config.destination.get("clustering", {})
        if clustering_config.get("enabled", False):
            clustering_fields = clustering_config.get("fields", [])
            if clustering_fields:
                job_config.clustering_fields = clustering_fields
                logging.info(
                    f"Clustering configured on: {', '.join(clustering_fields)}"
                )

        # Single insert for all records
        timeout = self.config.performance.get("bq_job_timeout_seconds", 600)

        try:
            job = self.bq_client.load_table_from_json(
                serialized_records, self.table_id, job_config=job_config
            )

            job.result(timeout=timeout)
            logging.info(
                f"  Inserted {len(serialized_records)} records in single batch"
            )

        except GoogleCloudError as e:
            logging.error(f"BigQuery insert failed: {e}")
            raise

    def move_file(self, gcs_uri: str, destination: str) -> None:
        """
        Move file to archive or rejected folder (atomic operation)

        Args:
            gcs_uri: Source GCS URI
            destination: 'archive' or 'rejected'
        """
        # Parse URI
        parts = gcs_uri.replace("gs://", "").split("/")
        bucket_name = parts[0]
        source_path = "/".join(parts[1:])
        filename = parts[-1]

        # Determine destination path
        if destination == "archive":
            dest_path = self.config.source["archive_path"]
        else:
            dest_path = self.config.source["rejected_path"]

        dest_blob_path = f"{dest_path}/{filename}"

        # Move file (atomic operation within same bucket)
        bucket = self.storage_client.bucket(bucket_name)
        source_blob = bucket.blob(source_path)

        bucket.rename_blob(source_blob, dest_blob_path)

        logging.info(f"📁 Moved to {destination}: {filename}")

    def run(self, specific_file: Optional[str] = None) -> int:
        """
        Run ingestion pipeline with batch processing

        Args:
            specific_file: Optional specific file to process

        Returns:
            Exit code (0 = success, 1 = error)
        """
        self.metrics.start_time = time.time()

        logging.info(f"\n{'='*80}")
        logging.info(f"🚀 STARTING INGESTION: {self.config.data_type}")
        logging.info(f"{'='*80}")
        logging.info(f"Environment: {self.env}")
        logging.info(f"Version: {self.config.version}")
        logging.info(f"Dry run: {self.dry_run}")
        logging.info(f"{'='*80}\n")

        try:
            # List files
            files = self.list_source_files(specific_file)

            if not files:
                logging.warning("⚠️  No files found to process")
                return 0

            # Phase 1: Parse all files and accumulate records
            all_parsed_records = []
            successfully_parsed_files = []
            failed_files = []

            logging.info(f"\n{'='*80}")
            logging.info(f"PHASE 1: PARSING {len(files)} FILES")
            logging.info(f"{'='*80}\n")

            for gcs_uri in files:
                self.metrics.files_processed += 1

                success, parsed_records, _rejected_count = self.process_file(gcs_uri)

                if success:
                    self.metrics.files_succeeded += 1
                    all_parsed_records.extend(parsed_records)
                    successfully_parsed_files.append(gcs_uri)
                else:
                    self.metrics.files_failed += 1
                    failed_files.append(gcs_uri)

            # Phase 2: Insert all records to BigQuery in one batch
            if all_parsed_records:
                logging.info(f"\n{'='*80}")
                logging.info(
                    f"PHASE 2: INSERTING {len(all_parsed_records)} RECORDS TO BIGQUERY"
                )
                logging.info(f"{'='*80}\n")

                if not self.dry_run:
                    try:
                        self.insert_to_bigquery(all_parsed_records)
                        self.metrics.records_inserted += len(all_parsed_records)
                        logging.info(
                            f"✅ Successfully inserted {len(all_parsed_records)} records"
                        )
                    except Exception as e:
                        logging.error(f"❌ BigQuery insertion failed: {e}")
                        logging.error(
                            "Files will NOT be archived due to insertion failure"
                        )
                        return 1
                else:
                    logging.info(
                        f"DRY RUN: Would insert {len(all_parsed_records)} records"
                    )

            # Phase 3: Archive successfully processed files
            if successfully_parsed_files and not self.dry_run:
                logging.info(f"\n{'='*80}")
                logging.info(
                    f"PHASE 3: ARCHIVING {len(successfully_parsed_files)} FILES"
                )
                logging.info(f"{'='*80}\n")

                for gcs_uri in successfully_parsed_files:
                    try:
                        self.move_file(gcs_uri, "archive")
                    except Exception as e:
                        logging.error(f"Failed to archive {gcs_uri}: {e}")

            # Phase 4: Move failed files to rejected
            if failed_files and not self.dry_run:
                logging.info(f"\n{'='*80}")
                logging.info(
                    f"PHASE 4: MOVING {len(failed_files)} FAILED FILES TO REJECTED"
                )
                logging.info(f"{'='*80}\n")

                for gcs_uri in failed_files:
                    try:
                        self.move_file(gcs_uri, "rejected")
                    except Exception as e:
                        logging.error(f"Failed to move {gcs_uri} to rejected: {e}")

            # Final metrics
            self.metrics.end_time = time.time()
            self.print_summary()

            # Check for failures
            if self.metrics.files_failed > 0:
                logging.warning(f"⚠️  {self.metrics.files_failed} files failed")
                return 1

            return 0

        except Exception as e:
            logging.error(f"❌ Ingestion failed: {e}")
            logging.debug(traceback.format_exc())
            return 1

    def print_summary(self) -> None:
        """Print ingestion summary"""
        logging.info(f"\n{'='*80}")
        logging.info(f"📊 INGESTION SUMMARY")
        logging.info(f"{'='*80}")
        logging.info(f"Duration: {self.metrics.duration_seconds():.2f} seconds")
        logging.info(f"\nFiles:")
        logging.info(f"  Found:      {self.metrics.files_found}")
        logging.info(f"  Processed:  {self.metrics.files_processed}")
        logging.info(f"  Succeeded:  {self.metrics.files_succeeded}")
        logging.info(f"  Failed:     {self.metrics.files_failed}")
        logging.info(f"  Success rate: {self.metrics.success_rate():.1f}%")
        logging.info(f"\nRecords:")
        logging.info(f"  Read:       {self.metrics.records_read}")
        logging.info(f"  Parsed:     {self.metrics.records_parsed}")
        logging.info(f"  Validated:  {self.metrics.records_validated}")
        logging.info(f"  Inserted:   {self.metrics.records_inserted}")
        logging.info(f"  Rejected:   {self.metrics.records_rejected}")
        logging.info(f"  Rejection rate: {self.metrics.rejection_rate():.1f}%")
        logging.info(f"{'='*80}\n")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================


def setup_logging(level: str) -> None:
    """Configure logging"""
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Generic Spotify data ingestion with YAML configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest recently_played to dev
  python -m src.connectors.spotify.spotify_ingest --config recently_played --env dev

  # Ingest to prod with debug logging
  python -m src.connectors.spotify.spotify_ingest --config recently_played --env prd --log-level DEBUG

  # Dry run (validate only)
  python -m src.connectors.spotify.spotify_ingest --config recently_played --env dev --dry-run

  # Process specific file
  python -m src.connectors.spotify.spotify_ingest --config recently_played --env dev \\
      --file gs://ela-dp-dev/spotify/landing/2025_11_01_recently_played.jsonl
        """,
    )

    parser.add_argument(
        "--config",
        required=True,
        help="Configuration name (e.g., recently_played, saved_tracks)",
    )

    parser.add_argument(
        "--env", choices=["dev", "prd"], required=True, help="Environment"
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and parse data without writing to BigQuery",
    )

    parser.add_argument(
        "--file", help="Process specific GCS file (gs://bucket/path/file.jsonl)"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    # Load configuration
    config_path = Path(__file__).parent / "configs" / f"{args.config}.yaml"

    if not config_path.exists():
        logging.error(f"❌ Configuration not found: {config_path}")
        logging.error(f"Available configs should be in: {config_path.parent}")
        return 1

    try:
        # Run ingestion
        ingestor = SpotifyIngestor(config_path, args.env, args.dry_run)
        exit_code = ingestor.run(args.file)

        if exit_code == 0:
            logging.info("✅ Ingestion completed successfully")
        else:
            logging.error("❌ Ingestion completed with errors")

        return exit_code

    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
        logging.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
