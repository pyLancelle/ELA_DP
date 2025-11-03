#!/usr/bin/env python3
"""
Test script for Garmin Activities ingestion with YAML configuration

This script validates:
1. YAML configuration is valid and complete
2. Sample data can be parsed correctly
3. All core fields are extracted properly
4. Transformations work correctly
5. Validation rules are applied
6. Schema generation works

Usage:
    python tests/connectors/garmin/test_activities_ingestion.py

    # With verbose output
    python tests/connectors/garmin/test_activities_ingestion.py --verbose
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.connectors.garmin.garmin_ingest_v2 import (
    IngestionConfig,
    DataParser,
    DataTransformer,
    SchemaGenerator
)


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_test(name: str):
    """Print test name"""
    print(f"\n{Colors.BOLD}TEST: {name}{Colors.RESET}")
    print("-" * 80)


def print_success(message: str):
    """Print success message"""
    print(f"{Colors.GREEN}✓{Colors.RESET} {message}")


def print_warning(message: str):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠{Colors.RESET} {message}")


def print_error(message: str):
    """Print error message"""
    print(f"{Colors.RED}✗{Colors.RESET} {message}")


def print_info(message: str):
    """Print info message"""
    print(f"{Colors.BLUE}ℹ{Colors.RESET} {message}")


# =============================================================================
# SAMPLE DATA
# =============================================================================

SAMPLE_ACTIVITY = {
    "activityId": 123456789,
    "activityName": "Morning Run",
    "startTimeGMT": 1704067200000,  # 2024-01-01 00:00:00 GMT
    "startTimeLocal": 1704070800000,  # 2024-01-01 01:00:00 Local
    "endTimeGMT": 1704069000000,
    "activityType": {
        "typeId": 1,
        "typeKey": "running",
        "parentTypeId": 17,
        "isHidden": False,
        "restricted": False,
        "trimmable": False
    },
    "sportTypeId": 1,
    "distance": 5000.0,
    "duration": 1800.0,
    "elapsedDuration": 1900.0,
    "movingDuration": 1750.0,
    "elevationGain": 50.0,
    "elevationLoss": 45.0,
    "minElevation": 100.0,
    "maxElevation": 150.0,
    "averageHR": 150,
    "maxHR": 175,
    "averageSpeed": 2.78,  # ~5:00/km pace
    "maxSpeed": 4.5,
    "calories": 350.5,
    "startLatitude": 48.8566,  # Paris
    "startLongitude": 2.3522,
    "locationName": "Paris, France",
    # Extended fields (not in core)
    "hrTimeInZone_1": 120.0,
    "hrTimeInZone_2": 600.0,
    "hrTimeInZone_3": 720.0,
    "hrTimeInZone_4": 300.0,
    "hrTimeInZone_5": 60.0,
    "aerobicTrainingEffect": 3.5,
    "anaerobicTrainingEffect": 2.1,
    "vO2MaxValue": 52.0,
    "averageRunningCadenceInStepsPerMinute": 175.0,
    "steps": 3500,
    "hasPolyline": True,
    "hasSplits": True,
    "favorite": False,
    "pr": True,
}


# =============================================================================
# TESTS
# =============================================================================

def test_config_loading():
    """Test that configuration can be loaded"""
    print_test("Configuration Loading")

    config_path = Path("src/connectors/garmin/configs/activities.yaml")

    if not config_path.exists():
        print_error(f"Configuration not found: {config_path}")
        return False

    try:
        config = IngestionConfig(config_path)
        print_success(f"Loaded config: {config.data_type} v{config.version}")
        print_info(f"Description: {config.description}")

        # Check sections
        assert config.source is not None, "Source config missing"
        assert config.destination is not None, "Destination config missing"
        assert config.parsing is not None, "Parsing config missing"

        print_success("All config sections present")

        # Check core fields
        core_fields = config.get_core_fields()
        print_success(f"Core fields defined: {len(core_fields)}")

        # List first 5 core fields
        print_info("Sample core fields:")
        for field in core_fields[:5]:
            print(f"   - {field.name}: {field.bq_type} ({field.json_path})")

        return True

    except Exception as e:
        print_error(f"Failed to load config: {e}")
        return False


def test_transformations():
    """Test data transformations"""
    print_test("Data Transformations")

    transformer = DataTransformer()

    # Test timestamp to datetime
    timestamp_ms = 1704067200000  # 2024-01-01 00:00:00
    result = transformer.transform(timestamp_ms, "timestamp_ms_to_timestamp")
    expected = datetime(2024, 1, 1, 0, 0, 0)

    if result == expected:
        print_success(f"Timestamp conversion: {timestamp_ms} → {result}")
    else:
        print_error(f"Timestamp conversion failed: expected {expected}, got {result}")
        return False

    # Test timestamp to date
    result_date = transformer.transform(timestamp_ms, "timestamp_ms_to_date")
    expected_date = expected.date()

    if result_date == expected_date:
        print_success(f"Date conversion: {timestamp_ms} → {result_date}")
    else:
        print_error(f"Date conversion failed: expected {expected_date}, got {result_date}")
        return False

    # Test null handling
    result_null = transformer.transform(None, "timestamp_ms_to_timestamp")
    if result_null is None:
        print_success("Null handling works correctly")
    else:
        print_error(f"Null handling failed: got {result_null}")
        return False

    return True


def test_field_extraction():
    """Test JSON field extraction"""
    print_test("Field Extraction")

    config_path = Path("src/connectors/garmin/configs/activities.yaml")
    config = IngestionConfig(config_path)
    parser = DataParser(config)

    tests = [
        ("$.activityId", 123456789),
        ("$.activityName", "Morning Run"),
        ("$.activityType.typeKey", "running"),
        ("$.distance", 5000.0),
        ("$.averageHR", 150),
    ]

    all_passed = True
    for json_path, expected_value in tests:
        result = parser.extract_value(SAMPLE_ACTIVITY, json_path)
        if result == expected_value:
            print_success(f"Extracted {json_path}: {result}")
        else:
            print_error(f"Failed {json_path}: expected {expected_value}, got {result}")
            all_passed = False

    return all_passed


def test_record_parsing():
    """Test complete record parsing"""
    print_test("Record Parsing")

    config_path = Path("src/connectors/garmin/configs/activities.yaml")
    config = IngestionConfig(config_path)
    parser = DataParser(config)

    try:
        parsed = parser.parse_record(SAMPLE_ACTIVITY, "test_file.jsonl")

        # Check core fields
        core_fields = config.get_core_fields()
        missing_fields = []

        for field in core_fields:
            if field.name not in parsed:
                missing_fields.append(field.name)

        if missing_fields:
            print_error(f"Missing fields in parsed record: {missing_fields}")
            return False

        print_success(f"All {len(core_fields)} core fields present in parsed record")

        # Check specific values
        assert parsed['activity_id'] == 123456789, "activity_id mismatch"
        assert parsed['activity_name'] == "Morning Run", "activity_name mismatch"
        assert parsed['activity_type_key'] == "running", "activity_type_key mismatch"
        assert parsed['distance_meters'] == 5000.0, "distance_meters mismatch"
        assert parsed['duration_seconds'] == 1800.0, "duration_seconds mismatch"
        assert parsed['average_hr_bpm'] == 150, "average_hr_bpm mismatch"
        assert parsed['calories'] == 350.5, "calories mismatch"

        print_success("All field values correct")

        # Check date transformation
        assert isinstance(parsed['activity_date'], type(datetime.now().date())), "activity_date not a date"
        print_success(f"Date transformation correct: {parsed['activity_date']}")

        # Check timestamp transformation
        assert isinstance(parsed['start_time_gmt'], datetime), "start_time_gmt not a datetime"
        print_success(f"Timestamp transformation correct: {parsed['start_time_gmt']}")

        # Check raw_data preserved
        assert 'raw_data' in parsed, "raw_data missing"
        assert parsed['raw_data'] == SAMPLE_ACTIVITY, "raw_data not preserved"
        print_success("raw_data preserved correctly")

        # Check extended fields available in raw_data
        extended_fields = ['hrTimeInZone_1', 'aerobicTrainingEffect', 'vO2MaxValue']
        for field in extended_fields:
            assert field in parsed['raw_data'], f"Extended field {field} missing from raw_data"
        print_success(f"Extended fields available in raw_data: {len(extended_fields)} checked")

        # Check metadata
        assert 'dp_inserted_at' in parsed, "dp_inserted_at missing"
        assert 'source_file' in parsed, "source_file missing"
        assert parsed['source_file'] == "test_file.jsonl", "source_file incorrect"
        print_success("Metadata fields correct")

        return True

    except AssertionError as e:
        print_error(f"Assertion failed: {e}")
        return False
    except Exception as e:
        print_error(f"Parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_validation():
    """Test record validation"""
    print_test("Record Validation")

    config_path = Path("src/connectors/garmin/configs/activities.yaml")
    config = IngestionConfig(config_path)
    parser = DataParser(config)

    # Parse valid record
    valid_record = parser.parse_record(SAMPLE_ACTIVITY, "test.jsonl")
    is_valid, errors = parser.validate_record(valid_record)

    if is_valid:
        print_success("Valid record passed validation")
    else:
        print_error(f"Valid record failed validation: {errors}")
        return False

    # Test invalid record (missing required field)
    invalid_data = SAMPLE_ACTIVITY.copy()
    invalid_data['activityId'] = None
    invalid_record = parser.parse_record(invalid_data, "test.jsonl")

    is_valid, errors = parser.validate_record(invalid_record)
    if not is_valid or len(errors) > 0:
        print_success(f"Invalid record correctly rejected")
        print_info(f"Validation errors: {errors}")
    else:
        print_error("Invalid record was not rejected")
        return False

    # Test out of range value
    invalid_data = SAMPLE_ACTIVITY.copy()
    invalid_data['distance'] = 999999999  # Unrealistic distance
    invalid_record = parser.parse_record(invalid_data, "test.jsonl")

    is_valid, errors = parser.validate_record(invalid_record)
    # Note: might pass in 'warn' mode
    print_info(f"Out of range validation: is_valid={is_valid}, errors={errors}")

    return True


def test_schema_generation():
    """Test BigQuery schema generation"""
    print_test("Schema Generation")

    try:
        from google.cloud import bigquery
    except ImportError:
        print_warning("google-cloud-bigquery not installed, skipping schema test")
        return True

    config_path = Path("src/connectors/garmin/configs/activities.yaml")
    config = IngestionConfig(config_path)

    schema = SchemaGenerator.generate(config)

    print_success(f"Generated schema with {len(schema)} fields")

    # Check expected fields present
    field_names = [field.name for field in schema]

    expected_fields = [
        'activity_id',
        'activity_name',
        'activity_date',
        'distance_meters',
        'duration_seconds',
        'raw_data',
        'dp_inserted_at',
        'source_file'
    ]

    missing = []
    for expected in expected_fields:
        if expected not in field_names:
            missing.append(expected)

    if missing:
        print_error(f"Missing expected fields in schema: {missing}")
        return False

    print_success("All expected fields present in schema")

    # Show sample of schema
    print_info("Sample schema fields:")
    for field in schema[:5]:
        print(f"   - {field.name}: {field.field_type} ({field.mode})")

    return True


def test_full_pipeline():
    """Test full parsing pipeline with multiple records"""
    print_test("Full Pipeline (Multiple Records)")

    config_path = Path("src/connectors/garmin/configs/activities.yaml")
    config = IngestionConfig(config_path)
    parser = DataParser(config)

    # Create variations of sample data
    activities = []
    for i in range(5):
        activity = SAMPLE_ACTIVITY.copy()
        activity['activityId'] = 123456789 + i
        activity['activityName'] = f"Activity {i+1}"
        activity['distance'] = 5000.0 + (i * 1000)
        activities.append(activity)

    parsed_records = []
    for i, activity in enumerate(activities):
        try:
            parsed = parser.parse_record(activity, f"test_{i}.jsonl")
            is_valid, errors = parser.validate_record(parsed)

            if is_valid:
                parsed_records.append(parsed)
            else:
                print_warning(f"Record {i} invalid: {errors}")
        except Exception as e:
            print_error(f"Failed to parse record {i}: {e}")
            return False

    print_success(f"Parsed {len(parsed_records)}/{len(activities)} records")

    # Check uniqueness
    activity_ids = [r['activity_id'] for r in parsed_records]
    if len(activity_ids) == len(set(activity_ids)):
        print_success("All activity IDs unique")
    else:
        print_error("Duplicate activity IDs found")
        return False

    return True


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Run all tests"""
    print(f"\n{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}GARMIN ACTIVITIES INGESTION - TEST SUITE{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*80}{Colors.RESET}")

    tests = [
        ("Configuration Loading", test_config_loading),
        ("Data Transformations", test_transformations),
        ("Field Extraction", test_field_extraction),
        ("Record Parsing", test_record_parsing),
        ("Record Validation", test_validation),
        ("Schema Generation", test_schema_generation),
        ("Full Pipeline", test_full_pipeline),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print_error(f"Test crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))

    # Print summary
    print(f"\n{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}TEST SUMMARY{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*80}{Colors.RESET}")

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = f"{Colors.GREEN}PASS{Colors.RESET}" if result else f"{Colors.RED}FAIL{Colors.RESET}"
        print(f"{status} - {test_name}")

    print(f"\n{Colors.BOLD}Result: {passed}/{total} tests passed{Colors.RESET}")

    if passed == total:
        print(f"{Colors.GREEN}✓ All tests passed!{Colors.RESET}\n")
        return 0
    else:
        print(f"{Colors.RED}✗ Some tests failed{Colors.RESET}\n")
        return 1


if __name__ == '__main__':
    sys.exit(main())
