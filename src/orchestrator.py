#!/usr/bin/env python3
"""
YAML Pipeline Orchestrator
--------------------------
Executes a pipeline of jobs defined in ingestion-config-*.yaml.
The pipeline job must define a 'steps' list containing the names of other jobs to run.
"""
import argparse
import logging
import sys
import yaml
import subprocess
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def load_config(env: str) -> Dict[str, Any]:
    """Load the ingestion configuration for the specified environment."""
    # Determine project root (assuming this script is in src/)
    project_root = Path(__file__).parent.parent
    config_path = project_root / f"ingestion-config-{env}.yaml"
    
    if not config_path.exists():
        logger.error(f"❌ Configuration file not found: {config_path}")
        sys.exit(1)
        
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"❌ Failed to parse configuration file: {e}")
        sys.exit(1)

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
            capture_output=False,
            env=os.environ.copy()
        )
        duration = datetime.now() - start_time
        logger.info(f"✅ Completed: {description} (Duration: {duration})")
        
    except subprocess.CalledProcessError as e:
        duration = datetime.now() - start_time
        logger.error(f"❌ Failed: {description} (Duration: {duration})")
        logger.error(f"   Exit Code: {e.returncode}")
        sys.exit(e.returncode)

def main():
    parser = argparse.ArgumentParser(description="Run a pipeline from configuration")
    parser.add_argument("--pipeline", required=True, help="Name of the pipeline job to run")
    parser.add_argument("--env", choices=["dev", "prd"], required=True, help="Environment (dev/prd)")
    
    args = parser.parse_args()
    
    logger.info(f"Starting Pipeline Job: {args.pipeline} (Env: {args.env})")
    
    # Load configuration
    config = load_config(args.env)
    all_jobs = config.get("jobs", {})
    
    # Find pipeline job definition
    if args.pipeline not in all_jobs:
        logger.error(f"❌ Pipeline job '{args.pipeline}' not found in 'jobs' section")
        sys.exit(1)
        
    pipeline_job = all_jobs[args.pipeline]
    steps = pipeline_job.get("steps", [])
    
    if not steps:
        logger.error(f"❌ No 'steps' defined for pipeline job '{args.pipeline}'")
        sys.exit(1)
        
    logger.info(f"📋 Pipeline contains {len(steps)} steps:")
    for step in steps:
        logger.info(f"   - {step}")
    
    # Execute steps
    for i, step_name in enumerate(steps, 1):
        logger.info(f"\n[{i}/{len(steps)}] Executing step: {step_name}")
        
        if step_name not in all_jobs:
            logger.error(f"❌ Job definition for step '{step_name}' not found in 'jobs' section")
            sys.exit(1)
            
        job_def = all_jobs[step_name]
        command = job_def.get("command")
        description = job_def.get("description", step_name)
        
        if not command:
            logger.error(f"❌ No command defined for job '{step_name}'")
            sys.exit(1)
            
        run_command(command, description)
        
    logger.info(f"\n🎉 Pipeline '{args.pipeline}' completed successfully!")

if __name__ == "__main__":
    main()
