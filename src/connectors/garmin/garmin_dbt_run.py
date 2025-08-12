#!/usr/bin/env python3
"""
Garmin DBT Lake Models Automation
---------------------------------
Automates the execution of dbt run for Garmin lake models.

This script runs the dbt transformations to load data from 
lake_garmin__stg_garmin_raw to the dedicated lake service tables.

Usage:
    python -m src.connectors.garmin.garmin_dbt_run --env dev
    python -m src.connectors.garmin.garmin_dbt_run --env prd
    python -m src.connectors.garmin.garmin_dbt_run --env dev --models lake_garmin__svc_activities
"""

import argparse
import subprocess
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path


def setup_logging():
    """Configure logging for DBT operations."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


def get_dbt_directory():
    """Get the DBT project directory."""
    # Assume we're running from project root
    dbt_dir = Path(__file__).parent.parent.parent / "dbt_dataplatform"

    if not dbt_dir.exists():
        raise FileNotFoundError(f"DBT directory not found at {dbt_dir}")

    return str(dbt_dir.absolute())


def validate_environment(env: str):
    """Validate the target environment."""
    if env not in ["dev", "prd"]:
        raise ValueError("Environment must be 'dev' or 'prd'")


def run_dbt_command(dbt_dir: str, env: str, models: str = None) -> bool:
    """
    Execute dbt run command for Garmin lake models.

    Args:
        dbt_dir: Path to DBT project directory
        env: Target environment (dev/prd)
        models: Optional specific models to run

    Returns:
        bool: True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)

    # Build dbt command
    if models:
        # Run specific models
        cmd = [
            "uv",
            "run",
            "dbt",
            "run",
            "--target",
            env,
            "--select",
            models,
            "--project-dir",
            dbt_dir,
        ]
    else:
        # Run all Garmin lake models
        cmd = [
            "uv",
            "run",
            "dbt",
            "run",
            "--target",
            env,
            "--select",
            "tag:garmin,tag:lake",
            "--project-dir",
            dbt_dir,
        ]

    logger.info(f"Executing DBT command: {' '.join(cmd)}")
    logger.info(f"Target environment: {env.upper()}")
    logger.info(f"Working directory: {dbt_dir}")

    try:
        # Execute the command
        result = subprocess.run(
            cmd,
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            check=False,  # Don't raise on non-zero exit
        )

        # Log output
        if result.stdout:
            logger.info("DBT Output:")
            for line in result.stdout.split("\n"):
                if line.strip():
                    logger.info(f"  {line}")

        if result.stderr:
            logger.warning("DBT Errors/Warnings:")
            for line in result.stderr.split("\n"):
                if line.strip():
                    logger.warning(f"  {line}")

        # Check result
        if result.returncode == 0:
            logger.info("✅ DBT run completed successfully")
            return True
        else:
            logger.error(f"❌ DBT run failed with return code {result.returncode}")
            return False

    except FileNotFoundError:
        logger.error("❌ dbt command not found. Make sure dbt is installed and in PATH")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error running DBT: {e}")
        return False


def get_models_summary(dbt_dir: str, env: str) -> dict:
    """Get a summary of models that would be run."""
    logger = logging.getLogger(__name__)

    cmd = [
        "uv",
        "run",
        "dbt",
        "list",
        "--target",
        env,
        "--select",
        "tag:garmin,tag:lake",
        "--project-dir",
        dbt_dir,
    ]

    try:
        result = subprocess.run(
            cmd, cwd=dbt_dir, capture_output=True, text=True, check=False
        )

        if result.returncode == 0 and result.stdout:
            models = [
                line.strip() for line in result.stdout.split("\n") if line.strip()
            ]
            return {"total_models": len(models), "models": models}
        else:
            logger.warning("Could not get models list")
            return {"total_models": 0, "models": []}

    except Exception as e:
        logger.warning(f"Error getting models summary: {e}")
        return {"total_models": 0, "models": []}


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Run DBT transformations for Garmin lake models"
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "prd"],
        help="Target environment (dev or prd)",
    )
    parser.add_argument(
        "--models",
        help="Specific models to run (optional, defaults to all Garmin lake models)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what models would be run without executing",
    )

    args = parser.parse_args()

    # Setup
    logger = setup_logging()
    start_time = datetime.now(timezone.utc)

    logger.info("🚀 Starting Garmin DBT Lake Models automation")
    logger.info(f"📅 Execution time: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    try:
        # Validate inputs
        validate_environment(args.env)
        dbt_dir = get_dbt_directory()

        logger.info(f"🎯 Environment: {args.env.upper()}")
        logger.info(f"📁 DBT directory: {dbt_dir}")

        # Get models summary
        summary = get_models_summary(dbt_dir, args.env)
        logger.info(f"📊 Found {summary['total_models']} Garmin lake models:")
        for model in summary["models"]:
            logger.info(f"  - {model}")

        if args.dry_run:
            logger.info("🔍 Dry run mode - no actual execution")
            return True

        if summary["total_models"] == 0:
            logger.warning("⚠️ No models found to run")
            return True

        # Execute DBT run
        success = run_dbt_command(dbt_dir, args.env, args.models)

        # Final summary
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        if success:
            logger.info(f"✅ DBT automation completed successfully in {duration:.1f}s")
            return True
        else:
            logger.error(f"❌ DBT automation failed after {duration:.1f}s")
            return False

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
