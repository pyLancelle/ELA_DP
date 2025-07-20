#!/usr/bin/env python3
"""
Multi-environment test script for validating ingestion configurations.

This script validates both DEV and PRD ingestion configuration files and tests 
the job scheduling logic for both environments.

Usage:
    python test_ingestion_config_multi.py
    python test_ingestion_config_multi.py --config ingestion-config-dev.yaml
    python test_ingestion_config_multi.py --config ingestion-config-prd.yaml
"""

import yaml
import sys
import argparse
from datetime import datetime, timezone
from croniter import croniter
import pytz


def load_config(config_file):
    """Load and validate the configuration file."""
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        print(f"✅ Configuration file '{config_file}' loaded successfully")
        return config
    except FileNotFoundError:
        print(f"❌ Configuration file '{config_file}' not found")
        return None
    except yaml.YAMLError as e:
        print(f"❌ YAML parsing error in '{config_file}': {e}")
        return None


def validate_config_structure(config, config_file):
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
        required_global = [
            "timezone",
            "retry_attempts",
            "max_parallel_jobs",
            "environment",
        ]
        for key in required_global:
            if key not in global_config:
                errors.append(f"Missing global config key: {key}")

        # Check environment consistency
        config_env = global_config.get("environment")
        if "dev" in config_file.lower() and config_env != "dev":
            errors.append(
                f"DEV config file should have environment: 'dev', found: '{config_env}'"
            )
        elif "prd" in config_file.lower() and config_env != "prd":
            errors.append(
                f"PRD config file should have environment: 'prd', found: '{config_env}'"
            )

    # Validate jobs
    if "jobs" in config:
        for job_id, job_config in config["jobs"].items():
            required_job_keys = [
                "service",
                "data_type",
                "description",
                "cron",
                "command",
                "environment",
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

            # Validate environment consistency
            job_env = job_config.get("environment")
            expected_env = (
                global_config.get("environment") if "global" in config else None
            )
            if job_env != expected_env:
                errors.append(
                    f"Job {job_id} environment '{job_env}' doesn't match global environment '{expected_env}'"
                )

    return errors


def analyze_scheduling_differences(dev_config, prd_config):
    """Compare scheduling differences between DEV and PRD environments."""
    print("\n🔄 Analyzing DEV vs PRD Scheduling Differences...")

    dev_jobs = dev_config.get("jobs", {})
    prd_jobs = prd_config.get("jobs", {})

    common_jobs = set(dev_jobs.keys()) & set(prd_jobs.keys())
    dev_only = set(dev_jobs.keys()) - set(prd_jobs.keys())
    prd_only = set(prd_jobs.keys()) - set(dev_jobs.keys())

    print(f"   📊 Job Distribution:")
    print(f"     • Common jobs: {len(common_jobs)}")
    print(f"     • DEV only: {len(dev_only)}")
    print(f"     • PRD only: {len(prd_only)}")

    if dev_only:
        print(f"     • DEV-only jobs: {', '.join(dev_only)}")
    if prd_only:
        print(f"     • PRD-only jobs: {', '.join(prd_only)}")

    print(f"\n   ⏰ Schedule Comparison for Common Jobs:")
    for job_id in sorted(common_jobs):
        dev_cron = dev_jobs[job_id].get("cron", "N/A")
        prd_cron = prd_jobs[job_id].get("cron", "N/A")
        dev_limit = extract_limit_from_command(dev_jobs[job_id].get("command", ""))
        prd_limit = extract_limit_from_command(prd_jobs[job_id].get("command", ""))

        same_schedule = dev_cron == prd_cron
        same_limit = dev_limit == prd_limit

        status = "🟰" if same_schedule and same_limit else "🔄"
        print(f"     {status} {job_id}:")
        print(f"       DEV: {dev_cron} (limit: {dev_limit})")
        print(f"       PRD: {prd_cron} (limit: {prd_limit})")


def extract_limit_from_command(command):
    """Extract limit parameter from command string."""
    if "--limit" in command:
        parts = command.split("--limit")
        if len(parts) > 1:
            limit_part = parts[1].strip().split()[0]
            return limit_part
    return "default"


def test_cron_scheduling(config, config_name):
    """Test the CRON scheduling logic."""
    print(f"\n🕐 Testing CRON scheduling logic for {config_name}...")

    if "global" not in config or "jobs" not in config:
        print("❌ Cannot test scheduling without proper config structure")
        return

    # Setup timezone
    tz = pytz.timezone(config["global"]["timezone"])
    now = datetime.now(tz)

    print(f"   Current time: {now}")
    print(f"   Timezone: {config['global']['timezone']}")
    print(f"   Environment: {config['global']['environment']}")

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

    print(f"\n📊 Scheduling Analysis for {config_name}:")
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


def test_job_groups(config, config_name):
    """Test job group configuration."""
    print(f"\n📦 Testing job groups for {config_name}...")

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


def validate_multi_environment():
    """Validate both DEV and PRD configurations and compare them."""
    print("🧪 Testing ELA Data Platform Multi-Environment Configuration")
    print("=" * 65)

    # Load both configurations
    dev_config = load_config("ingestion-config-dev.yaml")
    prd_config = load_config("ingestion-config-prd.yaml")

    if not dev_config or not prd_config:
        print("❌ Cannot proceed without both DEV and PRD configurations")
        return False

    # Validate both configurations
    print("\n🔍 Validating DEV configuration structure...")
    dev_errors = validate_config_structure(dev_config, "ingestion-config-dev.yaml")

    print("\n🔍 Validating PRD configuration structure...")
    prd_errors = validate_config_structure(prd_config, "ingestion-config-prd.yaml")

    if dev_errors or prd_errors:
        print("❌ Configuration validation failed:")
        if dev_errors:
            print("  DEV errors:")
            for error in dev_errors:
                print(f"   - {error}")
        if prd_errors:
            print("  PRD errors:")
            for error in prd_errors:
                print(f"   - {error}")
        return False
    else:
        print("✅ Both configurations are structurally valid")

    # Compare environments
    analyze_scheduling_differences(dev_config, prd_config)

    # Test components for both environments
    test_job_groups(dev_config, "DEV")
    test_job_groups(prd_config, "PRD")
    test_cron_scheduling(dev_config, "DEV")
    test_cron_scheduling(prd_config, "PRD")

    print("\n" + "=" * 65)
    print("🎉 Multi-environment configuration validation completed successfully!")
    print("\n📋 Summary:")
    print(f"   • DEV jobs configured: {len(dev_config.get('jobs', {}))}")
    print(f"   • PRD jobs configured: {len(prd_config.get('jobs', {}))}")
    print(f"   • DEV job groups: {len(dev_config.get('job_groups', {}))}")
    print(f"   • PRD job groups: {len(prd_config.get('job_groups', {}))}")
    print(
        f"   • DEV timezone: {dev_config.get('global', {}).get('timezone', 'Not set')}"
    )
    print(
        f"   • PRD timezone: {prd_config.get('global', {}).get('timezone', 'Not set')}"
    )

    print("\n🚀 Both environments are ready to deploy!")
    print("   Next steps:")
    print("   1. Test DEV environment first with manual triggers")
    print("   2. Validate DEV data output in GCS bucket")
    print("   3. Enable PRD environment for production workloads")
    print("   4. Monitor both environments independently")

    return True


def validate_single_environment(config_file):
    """Validate a single environment configuration."""
    config = load_config(config_file)
    if not config:
        return False

    env_name = (
        "DEV"
        if "dev" in config_file.lower()
        else "PRD" if "prd" in config_file.lower() else "UNKNOWN"
    )

    print(f"\n🔍 Validating {env_name} configuration structure...")
    errors = validate_config_structure(config, config_file)

    if errors:
        print(f"❌ {env_name} configuration validation failed:")
        for error in errors:
            print(f"   - {error}")
        return False
    else:
        print(f"✅ {env_name} configuration structure is valid")

    # Test components
    test_job_groups(config, env_name)
    test_cron_scheduling(config, env_name)

    print(f"\n🎉 {env_name} configuration validation completed successfully!")
    return True


def main():
    """Main test function."""
    parser = argparse.ArgumentParser(description="Test ingestion configuration(s)")
    parser.add_argument(
        "--config",
        help="Specific config file to test (default: test both DEV and PRD)",
        default=None,
    )

    args = parser.parse_args()

    if args.config:
        # Test single configuration
        success = validate_single_environment(args.config)
        sys.exit(0 if success else 1)
    else:
        # Test both configurations
        success = validate_multi_environment()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
