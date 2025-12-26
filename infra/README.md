# Infrastructure as Code - ELA Data Platform

This directory contains the Terraform Infrastructure as Code (IaC) configuration for the ELA Data Platform on Google Cloud Platform.

## Architecture Overview

The ELA Data Platform infrastructure consists of:

- **Cloud Run Jobs**: Containerized data fetching and processing jobs
- **BigQuery**: Data warehouse for analytics
- **Cloud Storage**: Data lake for raw and processed data
- **Artifact Registry**: Docker image storage
- **Secret Manager**: Secure credential storage
- **IAM**: Service accounts and permissions

## Directory Structure

```
infra/
├── terraform/
│   ├── modules/                    # Reusable Terraform modules
│   │   ├── cloud-run-jobs/        # Cloud Run Jobs configuration
│   │   ├── bigquery/              # BigQuery datasets and tables
│   │   ├── storage/               # GCS buckets
│   │   ├── artifact-registry/     # Docker registry
│   │   └── secret-manager/        # Secret management
│   ├── environments/              # Environment-specific configs
│   │   ├── dev/                   # Development environment
│   │   └── prd/                   # Production environment
│   └── shared/                    # Shared resources (Artifact Registry, etc.)
└── scripts/                       # Helper scripts
    └── setup.sh                   # Initial setup script
```

## Prerequisites

1. **Terraform** >= 1.5.0
   ```bash
   brew install terraform
   ```

2. **Google Cloud SDK**
   ```bash
   brew install google-cloud-sdk
   ```

3. **GCP Authentication**
   ```bash
   gcloud auth application-default login
   gcloud config set project polar-scene-465223-f7
   ```

4. **GCS Bucket for Terraform State** (will be created by setup script)

## Quick Start

### 1. Initial Setup

Run the setup script to create the GCS bucket for Terraform state:

```bash
cd infra
./scripts/setup.sh
```

### 2. Deploy Shared Resources

Deploy shared resources like Artifact Registry first:

```bash
cd terraform/shared
terraform init
terraform plan
terraform apply
```

### 3. Deploy Development Environment

```bash
cd terraform/environments/dev
terraform init
terraform plan
terraform apply
```

### 4. Deploy Production Environment

```bash
cd terraform/environments/prd
terraform init
terraform plan
terraform apply
```

## Environment Management

### Development (dev)

- **Project**: polar-scene-465223-f7
- **Region**: europe-west1
- **Bucket**: ela-dp-dev
- **Usage**: Testing and development

### Production (prd)

- **Project**: polar-scene-465223-f7
- **Region**: europe-west1
- **Bucket**: ela-dp-prd
- **Usage**: Production workloads

## Managing Resources

### View Current State

```bash
cd terraform/environments/dev
terraform state list
```

### Import Existing Resources

If you have existing resources created manually, you can import them:

```bash
terraform import module.storage.google_storage_bucket.data_bucket ela-dp-dev
terraform import module.bigquery.google_bigquery_dataset.raw raw
```

### Update Infrastructure

```bash
cd terraform/environments/dev
terraform plan
terraform apply
```

### Destroy Resources (CAUTION)

```bash
cd terraform/environments/dev
terraform destroy
```

## Module Documentation

### Cloud Run Jobs

Creates and manages Cloud Run Jobs for data fetching and processing.

Variables:
- `project_id`: GCP project ID
- `region`: GCP region
- `jobs`: Map of job configurations
- `image`: Docker image URL

### BigQuery

Manages BigQuery datasets and tables.

Variables:
- `project_id`: GCP project ID
- `datasets`: List of dataset configurations
- `location`: BigQuery location

### Storage

Manages GCS buckets for data storage.

Variables:
- `project_id`: GCP project ID
- `bucket_name`: Name of the bucket
- `location`: Bucket location
- `lifecycle_rules`: Optional lifecycle rules

### Artifact Registry

Manages Docker repositories for container images.

Variables:
- `project_id`: GCP project ID
- `region`: Registry region
- `repository_id`: Repository name

### Secret Manager

Manages secrets for API keys and credentials.

Variables:
- `project_id`: GCP project ID
- `secrets`: Map of secret configurations

## Best Practices

1. **Always run `terraform plan` before `apply`**
2. **Use workspaces or separate state files for environments**
3. **Store sensitive data in Secret Manager, never in code**
4. **Use variables for configuration, outputs for cross-module references**
5. **Tag resources appropriately for cost tracking**
6. **Enable version control for Terraform state (GCS versioning)**
7. **Use remote backend (GCS) for team collaboration**

## Troubleshooting

### State Lock Issues

If you encounter state lock issues:

```bash
terraform force-unlock LOCK_ID
```

### Authentication Issues

Refresh your credentials:

```bash
gcloud auth application-default login
gcloud auth configure-docker europe-west1-docker.pkg.dev
```

### Import Existing Resources

To avoid destroying existing resources, import them first:

```bash
# List existing resources
gcloud run jobs list --region=europe-west1

# Import them
terraform import module.cloud_run_jobs.google_cloud_run_v2_job.job["spotify-recently-played"] projects/polar-scene-465223-f7/locations/europe-west1/jobs/spotify-recently-played
```

## CI/CD Integration

Future enhancement: Integrate Terraform with GitHub Actions for automated deployments.

## Support

For questions or issues, contact the platform team or refer to:
- [Terraform GCP Provider Docs](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [GCP Documentation](https://cloud.google.com/docs)
