import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Add src to python path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from src.connectors.garmin.utils import flatten_nested_arrays, setup_logging

def process_file(input_path: Path, output_path: Path) -> bool:
    """
    Process a single JSONL file: read, transform, write.
    Returns True if successful, False otherwise.
    """
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:
            
            line_count = 0
            for line in infile:
                if not line.strip():
                    continue
                
                try:
                    data = json.loads(line)
                    # Apply transformation
                    cleaned = flatten_nested_arrays(data)
                    outfile.write(json.dumps(cleaned, default=str) + '\n')
                    line_count += 1
                except json.JSONDecodeError as e:
                    logging.warning(f"Skipping invalid JSON line in {input_path}: {e}")
                    continue
                    
        logging.info(f"✅ Processed {input_path.name}: {line_count} items")
        return True
        
    except Exception as e:
        logging.error(f"❌ Failed to process {input_path}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Batch process legacy Garmin JSONL files.")
    parser.add_argument("--input-dir", required=True, type=Path, help="Directory containing raw JSONL files")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory to save transformed files")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    
    if not args.input_dir.exists():
        logging.error(f"Input directory not found: {args.input_dir}")
        sys.exit(1)
        
    logging.info(f"🚀 Starting batch processing from {args.input_dir} to {args.output_dir}")
    
    files = list(args.input_dir.rglob("*.jsonl"))
    logging.info(f"Found {len(files)} JSONL files to process.")
    
    success_count = 0
    failure_count = 0
    
    for input_path in files:
        # Determine output path (maintain relative structure if possible, else flat)
        # Here we just mirror the structure relative to input_dir
        rel_path = input_path.relative_to(args.input_dir)
        output_path = args.output_dir / rel_path
        
        if process_file(input_path, output_path):
            success_count += 1
        else:
            failure_count += 1
            
    logging.info(f"🏁 Batch processing complete.")
    logging.info(f"✅ Success: {success_count}")
    logging.info(f"❌ Failure: {failure_count}")

if __name__ == "__main__":
    main()
