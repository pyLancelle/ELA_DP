#!/usr/bin/env python3
"""
Generic DBT Run Script
======================

Universal script for running DBT transformations with flexible model selection.
All configuration is passed via command-line arguments.

Usage:
    # Run specific models/paths
    python -m src.services.dbt.dbt_run --env dev --select "models/lake/service/spotify/"

    # Run with tags
    python -m src.services.dbt.dbt_run --env dev --select "tag:garmin,tag:lake"

    # Run multiple selectors
    python -m src.services.dbt.dbt_run --env dev --select "models/lake/service/spotify/ models/hub/service/music/"

    # Dry run to see what would be executed
    python -m src.services.dbt.dbt_run --env dev --select "tag:spotify" --dry-run
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


def run_dbt_command(dbt_dir: str, env: str, select: str) -> bool:
    """
    Execute dbt run command with provided selectors.

    Args:
        dbt_dir: Path to DBT project directory
        env: Target environment (dev/prd)
        select: DBT select argument (models, tags, paths, etc.)

    Returns:
        bool: True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)

    # Build dbt command
    cmd = [
        "uv",
        "run",
        "dbt",
        "run",
        "--target",
        env,
        "--select",
        select,
        "--project-dir",
        dbt_dir,
    ]

    logger.info(f"Executing DBT command: {' '.join(cmd)}")
    logger.info(f"Target environment: {env.upper()}")
    logger.info(f"Working directory: {dbt_dir}")
    logger.info(f"Model selector: {select}")

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


def get_models_summary(dbt_dir: str, env: str, select: str) -> dict:
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
        select,
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
        description="Generic DBT run script with flexible model selection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run Spotify lake models
  python -m src.services.dbt.dbt_run --env dev --select "models/lake/service/spotify/"

  # Run Garmin models by tags
  python -m src.services.dbt.dbt_run --env dev --select "tag:garmin,tag:lake"

  # Run Spotify lake + Hub music
  python -m src.services.dbt.dbt_run --env dev --select "models/lake/service/spotify/ models/hub/service/music/"

  # Dry run to see what would be executed
  python -m src.services.dbt.dbt_run --env dev --select "tag:spotify" --dry-run
        """
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "prd"],
        help="Target environment (dev or prd)",
    )
    parser.add_argument(
        "--select",
        required=True,
        help="DBT select argument (models, tags, paths, etc.)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what models would be run without executing",
    )
    parser.add_argument(
        "--description",
        default="DBT transformations",
        help="Description of what this run does (for logging)",
    )

    args = parser.parse_args()

    # Setup
    logger = setup_logging()
    start_time = datetime.now(timezone.utc)

    logger.info(f"🚀 Starting DBT run: {args.description}")
    logger.info(f"📅 Execution time: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    try:
        # Validate inputs
        validate_environment(args.env)
        dbt_dir = get_dbt_directory()

        logger.info(f"🎯 Environment: {args.env.upper()}")
        logger.info(f"📁 DBT directory: {dbt_dir}")

        # Get models summary
        summary = get_models_summary(dbt_dir, args.env, args.select)
        logger.info(f"📊 Found {summary['total_models']} models:")
        for model in summary["models"]:
            logger.info(f"  - {model}")

        if args.dry_run:
            logger.info("🔍 Dry run mode - no actual execution")
            return True

        if summary["total_models"] == 0:
            logger.warning("⚠️ No models found to run")
            return True

        # Execute DBT run
        success = run_dbt_command(dbt_dir, args.env, args.select)

        # Final summary
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        if success:
            logger.info(
                f"✅ DBT run completed successfully in {duration:.1f}s"
            )
            return True
        else:
            logger.error(f"❌ DBT run failed after {duration:.1f}s")
            return False

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
