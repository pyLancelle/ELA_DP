import json
import logging
import sys
from pathlib import Path
from google.cloud.bigquery import SchemaField

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.bq_auto_ingest import BigQueryAutoIngestor

from unittest.mock import MagicMock, patch

import argparse

def test_schema_evolution():
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='Path to JSONL file to test')
    parser.add_argument('--target-field', help='Field to search for in schema', default='dailyAcuteChronicWorkloadRatio')
    args = parser.parse_args()

    if args.file:
        logging.info(f"Testing with real file: {args.file}")
        with open(args.file, 'r') as f:
            records = [json.loads(line) for line in f]
    else:
        # 1. Create test data with late-arriving fields
        records = [
            {"id": 1, "common": "A"},
            {"id": 2, "common": "B"},
            {"id": 3, "common": "C", "late_field": "I am new"},
            {"id": 4, "common": "D", "nested": {"a": 1}},
            {"id": 5, "common": "E", "nested": {"b": 2}}, # Nested evolution
        ]
    
    # Mock the clients to avoid credential errors
    with patch('google.cloud.bigquery.Client'), \
         patch('google.cloud.storage.Client'):
        ingestor = BigQueryAutoIngestor(project_id="test-project", dry_run=True)
    
    # 2. Run schema detection
    logging.info("Running schema detection...")
    schema = ingestor.detect_schema(records)
    
    # 3. Verify results
    schema_map = {f.name: f for f in schema}
    logging.info(f"Detected {len(schema)} top-level fields")
    
    if args.file:
        # Recursive search for specific field
        target_field = args.target_field
        found = False
        
        def search_field(fields, path=""):
            nonlocal found
            for f in fields:
                current_path = f"{path}.{f.name}" if path else f.name
                if f.name == target_field:
                    logging.info(f"✅ Found '{target_field}' at {current_path}")
                    found = True
                if f.field_type == 'RECORD':
                    search_field(f.fields, current_path)

        search_field(schema)
        
        if not found:
            logging.error(f"❌ '{target_field}' NOT found in schema")
            sys.exit(1)
            
    else:
        # Check late_field
        if "late_field" in schema_map:
            logging.info("✅ 'late_field' detected successfully")
        else:
            logging.error("❌ 'late_field' NOT detected")
            sys.exit(1)
            
        # Check nested evolution
        if "nested" in schema_map:
            nested_field = schema_map["nested"]
            nested_sub_map = {f.name: f for f in nested_field.fields}
            if "a" in nested_sub_map and "b" in nested_sub_map:
                 logging.info("✅ Nested fields 'a' and 'b' detected successfully")
            else:
                 logging.error(f"❌ Nested fields missing. Found: {nested_sub_map.keys()}")
                 sys.exit(1)
        else:
            logging.error("❌ 'nested' field NOT detected")
            sys.exit(1)

    logging.info("Schema detection test passed!")

if __name__ == "__main__":
    test_schema_evolution()
