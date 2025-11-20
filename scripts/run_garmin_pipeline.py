#!/usr/bin/env python3
"""
Garmin Pipeline Orchestrator
----------------------------
Runs the complete Garmin data pipeline:
1. Fetch data from Garmin Connect
2. Ingest data to BigQuery
3. Run DBT transformations
"""
import subprocess
import logging
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_command(command: str, description: str) -> None:
    """Run a shell command with logging and error handling."""
    logger.info(f"🚀 Starting: {description}")
    logger.info(f"   Command: {command}")
    
    start_time = datetime.now()
    try:
        # Run command and stream output
        process = subprocess.run(
            command,
            shell=True,
            check=True,
            text=True,
            capture_output=False  # Let output flow to stdout
        )
        duration = datetime.now() - start_time
        logger.info(f"✅ Completed: {description} (Duration: {duration})")
        
    except subprocess.CalledProcessError as e:
        duration = datetime.now() - start_time
        logger.error(f"❌ Failed: {description} (Duration: {duration})")
        logger.error(f"   Exit Code: {e.returncode}")
        sys.exit(e.returncode)

def main():
    parser = argparse.ArgumentParser(description="Run the Garmin Data Pipeline")
    parser.add_argument("--env", choices=["dev", "prd"], default="prd", help="Environment to run in")
    parser.add_argument("--days", type=int, default=2, help="Number of days to fetch")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip the fetch step")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip the ingestion step")
    parser.add_argument("--skip-dbt", action="store_true", help="Skip the DBT step")
    
    args = parser.parse_args()
    
    logger.info(f"Starting Garmin Pipeline in {args.env.upper()} environment")
    
    # 1. Fetch Data
    if not args.skip_fetch:
        # Note: Fetch is environment agnostic in terms of script arguments usually, 
        # but we might want to pass env if the script supports it. 
        # The current garmin connector uses .env file, so we rely on that or args.
        # We'll use the command structure from ingestion-config-prd.yaml
        fetch_cmd = f"python -m src.connectors.garmin --days {args.days}"
        run_command(fetch_cmd, "Fetch Garmin Data")
    else:
        logger.info("⏭️  Skipping Fetch step")

    # 2. Ingest Data
    if not args.skip_ingest:
        ingest_cmd = f"python -m src.connectors.garmin.garmin_ingest --env {args.env}"
        run_command(ingest_cmd, "Ingest to BigQuery")
    else:
        logger.info("⏭️  Skipping Ingest step")

    # 3. Run DBT
    if not args.skip_dbt:
        dbt_cmd = f"python -m src.connectors.garmin.garmin_dbt_run --env {args.env}"
        run_command(dbt_cmd, "Run DBT Transformations")
    else:
        logger.info("⏭️  Skipping DBT step")
        
    logger.info("🎉 Pipeline completed successfully!")

if __name__ == "__main__":
    main()
