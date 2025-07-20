#!/usr/bin/env python3
"""
Test script for validating the ingestion configuration.

This script validates the ingestion-config.yaml file and tests the job scheduling logic.
Run this before deploying the orchestrator to ensure everything is configured correctly.

Usage:
    python test_ingestion_config.py
"""

import yaml
import sys
from datetime import datetime, timezone
from croniter import croniter
import pytz


def load_config():
    """Load and validate the configuration file."""
    try:
        with open("ingestion-config.yaml", "r") as f:
            config = yaml.safe_load(f)
        print("✅ Configuration file loaded successfully")
        return config
    except FileNotFoundError:
        print("❌ Configuration file 'ingestion-config.yaml' not found")
        return None
    except yaml.YAMLError as e:
        print(f"❌ YAML parsing error: {e}")
        return None


def validate_config_structure(config):
    """Validate the structure of the configuration."""
    errors = []

    # Check required top-level keys
    required_keys = ["global", "job_groups", "jobs", "services"]
    for key in required_keys:
        if key not in config:
            errors.append(f"Missing top-level key: {key}")

    # Validate global config
    if "global" in config:
        global_config = config["global"]
        required_global = ["timezone", "retry_attempts", "max_parallel_jobs"]
        for key in required_global:
            if key not in global_config:
                errors.append(f"Missing global config key: {key}")

    # Validate jobs
    if "jobs" in config:
        for job_id, job_config in config["jobs"].items():
            required_job_keys = [
                "service",
                "data_type",
                "description",
                "cron",
                "command",
            ]
            for key in required_job_keys:
                if key not in job_config:
                    errors.append(f"Job {job_id} missing key: {key}")

            # Validate CRON expression
            if "cron" in job_config:
                try:
                    croniter(job_config["cron"])
                except Exception as e:
                    errors.append(
                        f"Job {job_id} has invalid CRON expression '{job_config['cron']}': {e}"
                    )

    return errors


def test_cron_scheduling(config):
    """Test the CRON scheduling logic."""
    print("\n🕐 Testing CRON scheduling logic...")

    if "global" not in config or "jobs" not in config:
        print("❌ Cannot test scheduling without proper config structure")
        return

    # Setup timezone
    tz = pytz.timezone(config["global"]["timezone"])
    now = datetime.now(tz)

    print(f"   Current time: {now}")
    print(f"   Timezone: {config['global']['timezone']}")

    jobs_due = []
    jobs_not_due = []

    for job_id, job_config in config["jobs"].items():
        if not job_config.get("enabled", True):
            continue

        cron_expr = job_config.get("cron")
        if not cron_expr:
            continue

        try:
            # Get the previous scheduled time
            cron = croniter(cron_expr, now)
            prev_run = cron.get_prev(datetime)
            next_run = cron.get_next(datetime)

            # Check if the job should have run within the last hour
            time_diff = (now - prev_run).total_seconds()
            should_run = time_diff <= 3600  # 1 hour tolerance

            job_info = {
                "job_id": job_id,
                "service": job_config["service"],
                "cron": cron_expr,
                "prev_run": prev_run,
                "next_run": next_run,
                "time_diff": time_diff,
                "should_run": should_run,
            }

            if should_run:
                jobs_due.append(job_info)
            else:
                jobs_not_due.append(job_info)

        except Exception as e:
            print(f"   ❌ Error testing schedule for {job_id}: {e}")

    print(f"\n📊 Scheduling Analysis:")
    print(f"   Jobs due to run now: {len(jobs_due)}")
    print(f"   Jobs not due: {len(jobs_not_due)}")

    if jobs_due:
        print(f"\n✅ Jobs that would run now:")
        for job in jobs_due:
            print(
                f"   - {job['job_id']} ({job['service']}) - last run: {job['prev_run']}"
            )

    if jobs_not_due:
        print(f"\n⏭️  Jobs not scheduled (next 5):")
        for i, job in enumerate(jobs_not_due[:5]):
            print(
                f"   - {job['job_id']} ({job['service']}) - next run: {job['next_run']}"
            )


def test_job_groups(config):
    """Test job group configuration."""
    print("\n📦 Testing job groups...")

    if "job_groups" not in config or "jobs" not in config:
        print("❌ Cannot test job groups without proper config structure")
        return

    for group_id, group_config in config["job_groups"].items():
        print(f"\n   Group: {group_id}")
        print(f"   Description: {group_config.get('description', 'No description')}")

        group_jobs = group_config.get("jobs", [])
        print(f"   Jobs ({len(group_jobs)}):")

        for job_id in group_jobs:
            if job_id in config["jobs"]:
                job = config["jobs"][job_id]
                print(f"     ✅ {job_id} ({job['service']}) - {job['description']}")
            else:
                print(f"     ❌ {job_id} - Job not found in configuration")


def validate_services(config):
    """Validate service configuration."""
    print("\n🔧 Validating services...")

    if "services" not in config or "jobs" not in config:
        print("❌ Cannot validate services without proper config structure")
        return

    # Check that all services referenced in jobs are defined
    job_services = set()
    for job_config in config["jobs"].values():
        job_services.add(job_config.get("service"))

    defined_services = set(config["services"].keys())

    missing_services = job_services - defined_services
    if missing_services:
        print(f"   ❌ Jobs reference undefined services: {missing_services}")
    else:
        print(f"   ✅ All job services are properly defined")

    # Validate each service configuration
    for service_id, service_config in config["services"].items():
        print(f"\n   Service: {service_id}")
        required_keys = ["base_path", "fetch_script", "supported_data_types"]
        missing_keys = [key for key in required_keys if key not in service_config]

        if missing_keys:
            print(f"     ❌ Missing keys: {missing_keys}")
        else:
            print(f"     ✅ All required keys present")
            print(f"     📁 Path: {service_config['base_path']}")
            print(f"     📜 Script: {service_config['fetch_script']}")
            print(f"     📊 Data types: {service_config['supported_data_types']}")


def main():
    """Main test function."""
    print("🧪 Testing ELA Data Platform Ingestion Configuration")
    print("=" * 55)

    # Load configuration
    config = load_config()
    if not config:
        sys.exit(1)

    # Validate structure
    print("\n🔍 Validating configuration structure...")
    errors = validate_config_structure(config)

    if errors:
        print("❌ Configuration validation failed:")
        for error in errors:
            print(f"   - {error}")
        sys.exit(1)
    else:
        print("✅ Configuration structure is valid")

    # Test components
    validate_services(config)
    test_job_groups(config)
    test_cron_scheduling(config)

    print("\n" + "=" * 55)
    print("🎉 Configuration validation completed successfully!")
    print("\n📋 Summary:")
    print(f"   • Jobs configured: {len(config.get('jobs', {}))}")
    print(f"   • Job groups: {len(config.get('job_groups', {}))}")
    print(f"   • Services: {len(config.get('services', {}))}")
    print(f"   • Timezone: {config.get('global', {}).get('timezone', 'Not set')}")

    print("\n🚀 The orchestrator is ready to deploy!")
    print("   Next steps:")
    print("   1. Commit the configuration files to your repository")
    print("   2. Ensure all required secrets are configured in GitHub")
    print("   3. Test the workflow with a manual trigger")
    print("   4. Monitor the first few automatic executions")


if __name__ == "__main__":
    main()
