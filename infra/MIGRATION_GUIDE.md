# Migration Guide: From Manual Setup to Terraform

This guide helps you migrate from manually created GCP resources to Terraform-managed infrastructure.

## Overview

You likely have existing resources created via:
- Shell scripts ([create_jobs.sh](../scripts/create_jobs.sh), [create_dbt_jobs.sh](../scripts/create_dbt_jobs.sh))
- Manual GCP Console operations
- GitHub Actions workflows

This guide will help you import those resources into Terraform without destroying and recreating them.

## Migration Strategy

We'll use Terraform's `import` command to bring existing resources under Terraform management.

### Phase 1: Inventory Your Resources

First, list what you have:

```bash
# List Cloud Run Jobs
gcloud run jobs list --region=europe-west1 --format="table(name,region)"

# List BigQuery datasets
bq ls --project_id=polar-scene-465223-f7

# List GCS buckets
gsutil ls -p polar-scene-465223-f7

# List secrets
gcloud secrets list --format="table(name)"

# List Artifact Registry repositories
gcloud artifacts repositories list --location=europe-west1
```

### Phase 2: Deploy Terraform Configuration (Without Creating Resources)

1. **Deploy shared resources first** (may create new service account):
   ```bash
   cd terraform/shared
   terraform init
   ```

2. **Import existing Artifact Registry** (if it exists):
   ```bash
   terraform import module.artifact_registry.google_artifact_registry_repository.repository \
     projects/polar-scene-465223-f7/locations/europe-west1/repositories/ela-dataplatform
   ```

3. **Apply shared resources**:
   ```bash
   terraform apply
   ```

### Phase 3: Import Existing Resources for Dev Environment

Navigate to dev environment:
```bash
cd ../environments/dev
terraform init
```

#### Import Storage Bucket

```bash
terraform import module.storage.google_storage_bucket.bucket ela-dp-dev
```

#### Import BigQuery Datasets

```bash
terraform import 'module.bigquery.google_bigquery_dataset.dataset["raw"]' \
  projects/polar-scene-465223-f7/datasets/raw

terraform import 'module.bigquery.google_bigquery_dataset.dataset["lake"]' \
  projects/polar-scene-465223-f7/datasets/lake

terraform import 'module.bigquery.google_bigquery_dataset.dataset["hub"]' \
  projects/polar-scene-465223-f7/datasets/hub

terraform import 'module.bigquery.google_bigquery_dataset.dataset["product"]' \
  projects/polar-scene-465223-f7/datasets/product
```

#### Import Secrets

```bash
terraform import 'module.secrets.google_secret_manager_secret.secrets["SPOTIFY_CLIENT_ID"]' \
  projects/polar-scene-465223-f7/secrets/SPOTIFY_CLIENT_ID

terraform import 'module.secrets.google_secret_manager_secret.secrets["SPOTIFY_CLIENT_SECRET"]' \
  projects/polar-scene-465223-f7/secrets/SPOTIFY_CLIENT_SECRET

terraform import 'module.secrets.google_secret_manager_secret.secrets["SPOTIFY_REFRESH_TOKEN"]' \
  projects/polar-scene-465223-f7/secrets/SPOTIFY_REFRESH_TOKEN

terraform import 'module.secrets.google_secret_manager_secret.secrets["SPOTIFY_REDIRECT_URI"]' \
  projects/polar-scene-465223-f7/secrets/SPOTIFY_REDIRECT_URI

terraform import 'module.secrets.google_secret_manager_secret.secrets["GARMIN_USERNAME"]' \
  projects/polar-scene-465223-f7/secrets/GARMIN_USERNAME

terraform import 'module.secrets.google_secret_manager_secret.secrets["GARMIN_PASSWORD"]' \
  projects/polar-scene-465223-f7/secrets/GARMIN_PASSWORD
```

#### Import Cloud Run Jobs

For each existing job:

```bash
# Example for spotify-recently-played
terraform import 'module.cloud_run_jobs.google_cloud_run_v2_job.jobs["spotify-recently-played"]' \
  projects/polar-scene-465223-f7/locations/europe-west1/jobs/spotify-recently-played

# Example for garmin-fetch
terraform import 'module.cloud_run_jobs.google_cloud_run_v2_job.jobs["garmin-fetch"]' \
  projects/polar-scene-465223-f7/locations/europe-west1/jobs/garmin-fetch

# Add more as needed...
```

#### Import Cloud Scheduler Jobs

```bash
terraform import 'module.cloud_run_jobs.google_cloud_scheduler_job.schedulers["spotify-recently-played"]' \
  projects/polar-scene-465223-f7/locations/europe-west1/jobs/spotify-recently-played-scheduler
```

### Phase 4: Verify Import

After importing, run:

```bash
terraform plan
```

**Expected output**: "No changes. Your infrastructure matches the configuration."

If you see changes:
1. Review what Terraform wants to change
2. Adjust your `.tfvars` file to match existing resources
3. Run `terraform plan` again until no changes are shown

### Phase 5: Migrate Production Environment

Repeat the same process for production:

```bash
cd ../prd
terraform init

# Import resources similar to dev environment
# Use ela-dp-prd instead of ela-dp-dev for bucket names
# Adjust job names and other identifiers accordingly
```

## Migration Script (Semi-Automated)

Create a script to automate imports:

```bash
#!/bin/bash
# infra/scripts/migrate.sh

ENV=$1  # dev or prd

if [ "$ENV" != "dev" ] && [ "$ENV" != "prd" ]; then
    echo "Usage: ./migrate.sh [dev|prd]"
    exit 1
fi

cd terraform/environments/$ENV

echo "Importing resources for $ENV environment..."

# Import bucket
terraform import module.storage.google_storage_bucket.bucket ela-dp-$ENV

# Import datasets
for dataset in raw lake hub product; do
    terraform import "module.bigquery.google_bigquery_dataset.dataset[\"$dataset\"]" \
        "projects/polar-scene-465223-f7/datasets/$dataset"
done

# Import secrets
for secret in SPOTIFY_CLIENT_ID SPOTIFY_CLIENT_SECRET SPOTIFY_REFRESH_TOKEN SPOTIFY_REDIRECT_URI GARMIN_USERNAME GARMIN_PASSWORD; do
    terraform import "module.secrets.google_secret_manager_secret.secrets[\"$secret\"]" \
        "projects/polar-scene-465223-f7/secrets/$secret"
done

# List and import Cloud Run jobs
gcloud run jobs list --region=europe-west1 --format="value(name)" | while read job; do
    # Convert job name to terraform key (replace - with _)
    tf_key=$(echo $job | tr '-' '_')

    echo "Importing job: $job"
    terraform import "module.cloud_run_jobs.google_cloud_run_v2_job.jobs[\"$job\"]" \
        "projects/polar-scene-465223-f7/locations/europe-west1/jobs/$job" || true
done

echo "Import complete. Run 'terraform plan' to verify."
```

## Post-Migration

### 1. Decommission Old Scripts

Once migration is complete and verified:

1. Move old scripts to an archive folder:
   ```bash
   mkdir -p scripts/archive
   mv scripts/create_jobs.sh scripts/archive/
   mv scripts/create_dbt_jobs.sh scripts/archive/
   ```

2. Update documentation to point to Terraform

### 2. Update CI/CD

Modify GitHub Actions workflows to use Terraform instead of direct `gcloud` commands.

### 3. Set Up State Locking (Optional)

Enable state locking in your backend configuration for team collaboration.

## Troubleshooting

### Import Fails with "Resource Not Found"

The resource might not exist or the ID format is wrong. Verify:
```bash
gcloud run jobs describe JOB_NAME --region=europe-west1 --format="value(name)"
```

### Import Succeeds but Plan Shows Changes

Common causes:
- Labels or metadata differ
- Environment variables order
- Default values in Terraform vs actual resource

Solution: Adjust your Terraform configuration to match the actual resource state.

### Can't Import Cloud Scheduler Jobs

Cloud Scheduler jobs created by GitHub Actions might have different names. List them:
```bash
gcloud scheduler jobs list --location=europe-west1
```

## Rollback Plan

If something goes wrong during migration:

1. Delete the Terraform state:
   ```bash
   rm terraform.tfstate*
   ```

2. Re-run your old scripts:
   ```bash
   bash scripts/create_jobs.sh
   ```

3. Resources remain unchanged in GCP

## Best Practices After Migration

1. **Never manually modify resources** - always use Terraform
2. **Review plans carefully** before applying
3. **Use workspaces** for different configurations if needed
4. **Enable state versioning** in GCS bucket (already done by setup script)
5. **Document any manual changes** immediately

## Support

If you encounter issues during migration:
1. Check Terraform state: `terraform state list`
2. Verify resource in GCP Console
3. Review Terraform documentation for the specific resource type
4. Consider using `terraform state rm` to remove problematic imports and retry
