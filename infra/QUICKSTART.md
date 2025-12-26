# Quick Start Guide

This guide will help you get started with the ELA Data Platform infrastructure.

## Prerequisites Check

Before you begin, ensure you have:

1. Terraform installed (>= 1.5.0)
   ```bash
   terraform version
   ```

2. Google Cloud SDK installed
   ```bash
   gcloud version
   ```

3. Authenticated with GCP
   ```bash
   gcloud auth application-default login
   gcloud config set project polar-scene-465223-f7
   ```

## Step-by-Step Setup

### 1. Run Initial Setup

This creates the Terraform state bucket and enables required APIs:

```bash
cd infra
./scripts/setup.sh
```

### 2. Deploy Shared Resources

Shared resources include Artifact Registry and service accounts:

```bash
cd terraform/shared
terraform init
terraform plan
terraform apply
```

Expected output:
- Artifact Registry repository URL
- Service account email for Cloud Run

### 3. Deploy Development Environment

```bash
cd ../environments/dev
terraform init
terraform plan
terraform apply
```

This will create:
- GCS bucket: `ela-dp-dev`
- BigQuery datasets: `raw`, `lake`, `hub`, `product`
- Secret Manager secrets (empty - need to be populated)
- Cloud Run Jobs (fetchers and DBT)
- Cloud Scheduler jobs (cron triggers)

### 4. Populate Secrets

After applying, you need to populate the secrets manually:

```bash
# Spotify secrets
echo -n "your-client-id" | gcloud secrets versions add SPOTIFY_CLIENT_ID --data-file=-
echo -n "your-client-secret" | gcloud secrets versions add SPOTIFY_CLIENT_SECRET --data-file=-
echo -n "your-refresh-token" | gcloud secrets versions add SPOTIFY_REFRESH_TOKEN --data-file=-
echo -n "your-redirect-uri" | gcloud secrets versions add SPOTIFY_REDIRECT_URI --data-file=-

# Garmin secrets
echo -n "your-username" | gcloud secrets versions add GARMIN_USERNAME --data-file=-
echo -n "your-password" | gcloud secrets versions add GARMIN_PASSWORD --data-file=-
```

### 5. Test Cloud Run Jobs

Execute a job manually to test:

```bash
gcloud run jobs execute spotify-recently-played --region=europe-west1 --wait
```

### 6. Deploy Production Environment (Optional)

Once dev is validated:

```bash
cd ../prd
terraform init
terraform plan
terraform apply
```

## Common Commands

### View Resources

```bash
# List all resources in current state
terraform state list

# Show details of a specific resource
terraform state show module.storage.google_storage_bucket.bucket
```

### Update Infrastructure

After modifying `.tf` files:

```bash
terraform plan
terraform apply
```

### Add a New Cloud Run Job

1. Edit `terraform.tfvars` in the environment directory
2. Add a new job configuration
3. Apply changes:
   ```bash
   terraform plan
   terraform apply
   ```

### Import Existing Resources

If you have existing resources:

```bash
# Import a Cloud Run Job
terraform import 'module.cloud_run_jobs.google_cloud_run_v2_job.jobs["spotify-recently-played"]' projects/polar-scene-465223-f7/locations/europe-west1/jobs/spotify-recently-played

# Import a BigQuery dataset
terraform import 'module.bigquery.google_bigquery_dataset.dataset["raw"]' projects/polar-scene-465223-f7/datasets/raw

# Import a GCS bucket
terraform import module.storage.google_storage_bucket.bucket ela-dp-dev
```

## Troubleshooting

### Error: Backend initialization required

```bash
terraform init -reconfigure
```

### Error: Resource already exists

Import the existing resource:
```bash
terraform import <resource_address> <resource_id>
```

### Error: Permission denied

Check your authentication:
```bash
gcloud auth application-default login
gcloud projects get-iam-policy polar-scene-465223-f7 \
  --flatten="bindings[].members" \
  --filter="bindings.members:user:$(gcloud config get-value account)"
```

### View Cloud Run Job Logs

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=spotify-recently-played" \
  --limit 50 \
  --format json
```

## Next Steps

1. Review the generated resources in GCP Console
2. Configure monitoring and alerts
3. Set up CI/CD for automated deployments
4. Consider adding Cloud Monitoring dashboards

## Useful Links

- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Cloud Run Jobs Documentation](https://cloud.google.com/run/docs/create-jobs)
- [GCP Console](https://console.cloud.google.com/)
