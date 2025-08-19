#!/usr/bin/env python3
"""
Chess.com dbt Runner
-------------------
Run dbt models specifically for Chess.com data processing.

Usage:
    python -m src.connectors.chess.chess_dbt_run --env dev
    python -m src.connectors.chess.chess_dbt_run --env prd
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_dbt_command(command: list, env: str) -> bool:
    """
    Run dbt command with proper environment configuration.

    Args:
        command: List of dbt command arguments
        env: Environment (dev/prd)

    Returns:
        True if successful, False otherwise
    """
    # Change to dbt project directory
    dbt_dir = Path(__file__).parent.parent.parent.parent / "src" / "dbt_dataplatform"

    # Set environment variables
    env_vars = os.environ.copy()
    env_vars["DBT_PROFILES_DIR"] = str(dbt_dir)

    try:
        print(f"🚀 Running dbt command: {' '.join(command)}")
        print(f"📁 Working directory: {dbt_dir}")

        result = subprocess.run(
            command, cwd=dbt_dir, env=env_vars, capture_output=False, text=True
        )

        if result.returncode == 0:
            print(f"✅ dbt command completed successfully")
            return True
        else:
            print(f"❌ dbt command failed with return code {result.returncode}")
            return False

    except Exception as e:
        print(f"❌ Error running dbt command: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run dbt models for Chess.com data processing"
    )
    parser.add_argument(
        "--env", choices=["dev", "prd"], required=True, help="Environment (dev or prd)"
    )
    parser.add_argument(
        "--models",
        default="chess",
        help="dbt models to run (default: chess for all Chess.com models)",
    )
    parser.add_argument(
        "--full-refresh", action="store_true", help="Run with full refresh"
    )

    args = parser.parse_args()

    # Build dbt command
    dbt_cmd = ["dbt", "run", "--target", args.env]

    if args.models:
        dbt_cmd.extend(["--models", args.models])

    if args.full_refresh:
        dbt_cmd.append("--full-refresh")

    print(f"🚀 Starting Chess.com dbt processing for {args.env} environment")

    success = run_dbt_command(dbt_cmd, args.env)

    if success:
        print(f"✅ Chess.com dbt processing completed successfully for {args.env}")
        sys.exit(0)
    else:
        print(f"❌ Chess.com dbt processing failed for {args.env}")
        sys.exit(1)


if __name__ == "__main__":
    main()
