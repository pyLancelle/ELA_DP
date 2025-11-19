"""
Garmin Connector Utilities
--------------------------
Helper functions for date handling, file I/O, and logging.
"""
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from zoneinfo import ZoneInfo

from .config import DEFAULT_TIMEZONE

def setup_logging(level: str = "INFO") -> None:
    """Configure logging format and level."""
    fmt = "%(asctime)s %(levelname)s: %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True # Ensure we override any existing config
    )

def to_jsonl(data: List[Dict[str, Any]], jsonl_output_path: str) -> None:
    """Write list of dicts to JSONL file."""
    with open(jsonl_output_path, 'w', encoding='utf-8') as f:
        for entry in data:
            f.write(json.dumps(entry, default=str) + '\n')

def write_jsonl(data: List[Dict[str, Any]], output_path: Path) -> None:
    """Write a list of dicts to a JSONL file with directory creation."""
    try:
        if not data:
            logging.warning(f"No data to write for {output_path}")
            return

        output_path.parent.mkdir(parents=True, exist_ok=True)
        to_jsonl(data, jsonl_output_path=str(output_path))
        logging.info(f"📁 Dump saved to: {output_path} ({len(data)} items)")
    except Exception as e:
        raise IOError(f"Failed to write JSONL file: {e}") from e

def generate_output_filename(
    output_dir: Path, data_type: str, timezone: str = DEFAULT_TIMEZONE
) -> Path:
    """Generate timestamped output filename."""
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        logging.warning(f"Invalid timezone {timezone}, falling back to UTC")
        from datetime import timezone as dt_timezone
        tz = dt_timezone.utc
        
    timestamp = datetime.now(tz=tz).strftime("%Y_%m_%d_%H_%M")
    return output_dir / f"{timestamp}_garmin_{data_type}.jsonl"
