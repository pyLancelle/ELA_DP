# Terraform Modules

This directory contains reusable Terraform modules for the ELA Data Platform infrastructure.

## Available Modules

### [artifact-registry](./artifact-registry)

Manages Google Artifact Registry repositories for Docker images.

**Features:**
- Docker repository creation
- Cleanup policies for image retention
- IAM permissions management

**Usage:**
```hcl
module "artifact_registry" {
  source = "../../modules/artifact-registry"

  project_id    = "my-project"
  region        = "europe-west1"
  repository_id = "my-repo"

  cleanup_policies = [
    {
      id     = "keep-recent"
      action = "KEEP"
      condition = {
        tag_state = "TAGGED"
      }
      most_recent_versions = {
        keep_count = 10
      }
    }
  ]
}
```

### [bigquery](./bigquery)

Manages BigQuery datasets and optionally tables.

**Features:**
- Dataset creation with labels
- Access control configuration
- Optional table creation with partitioning and clustering

**Usage:**
```hcl
module "bigquery" {
  source = "../../modules/bigquery"

  project_id  = "my-project"
  location    = "EU"
  environment = "dev"

  datasets = {
    raw = {
      description = "Raw data layer"
      labels = {
        layer = "raw"
      }
    }
  }
}
```

### [cloud-run-jobs](./cloud-run-jobs)

Manages Cloud Run Jobs and their schedulers.

**Features:**
- Cloud Run Job creation
- Environment variables and secrets configuration
- Cloud Scheduler integration for cron scheduling
- Resource limits (CPU, memory, timeout)

**Usage:**
```hcl
module "cloud_run_jobs" {
  source = "../../modules/cloud-run-jobs"

  project_id            = "my-project"
  region                = "europe-west1"
  environment           = "dev"
  default_image         = "gcr.io/my-project/my-image:latest"
  service_account_email = "my-sa@my-project.iam.gserviceaccount.com"

  jobs = {
    "my-job" = {
      description = "My job description"
      cpu         = "1"
      memory      = "512Mi"
      timeout     = "600s"
      schedule    = "0 * * * *"  # Hourly
      env_vars = {
        KEY = "value"
      }
      secrets = {
        SECRET_KEY = {
          secret = "my-secret"
        }
      }
    }
  }
}
```

### [secret-manager](./secret-manager)

Manages Google Secret Manager secrets.

**Features:**
- Secret creation
- IAM permissions management
- Optional secret value setting (use with caution)

**Usage:**
```hcl
module "secrets" {
  source = "../../modules/secret-manager"

  project_id  = "my-project"
  environment = "dev"

  secrets = {
    API_KEY = {
      labels = {
        service = "my-service"
      }
    }
  }

  iam_members = {
    api_key_access = {
      secret_id = "API_KEY"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:my-sa@my-project.iam.gserviceaccount.com"
    }
  }
}
```

### [storage](./storage)

Manages Google Cloud Storage buckets.

**Features:**
- Bucket creation with versioning
- Lifecycle rules for cost optimization
- IAM permissions management
- Uniform bucket-level access

**Usage:**
```hcl
module "storage" {
  source = "../../modules/storage"

  project_id  = "my-project"
  bucket_name = "my-bucket"
  location    = "EU"
  environment = "dev"

  versioning_enabled = true

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 90
      }
    }
  ]
}
```

## Module Development Guidelines

When creating or modifying modules:

1. **Use semantic versioning** for module changes
2. **Document all variables** with clear descriptions
3. **Provide examples** in module README
4. **Use sensible defaults** where possible
5. **Follow Google Cloud best practices**
6. **Add validation** for input variables when appropriate
7. **Keep modules focused** on a single resource type or logical grouping
8. **Use consistent naming** across modules

## Testing Modules

Before using modules in production:

1. Test in dev environment first
2. Run `terraform validate` to check syntax
3. Run `terraform plan` to preview changes
4. Check for drift: `terraform plan -refresh-only`

## Module Updates

To update a module:

1. Make changes in the module directory
2. Update version documentation
3. Test in dev environment
4. Apply to production after validation

## Common Patterns

### Using Remote State

Access shared resources from environment configs:

```hcl
data "terraform_remote_state" "shared" {
  backend = "gcs"
  config = {
    bucket = "my-terraform-state"
    prefix = "shared"
  }
}

# Use shared outputs
service_account_email = data.terraform_remote_state.shared.outputs.cloud_run_sa_email
```

### Conditional Resource Creation

```hcl
# In module
resource "google_resource" "optional" {
  count = var.create_resource ? 1 : 0
  # ...
}
```

### Dynamic Blocks

Used extensively for optional nested blocks:

```hcl
dynamic "lifecycle_rule" {
  for_each = var.lifecycle_rules
  content {
    # ...
  }
}
```

## Troubleshooting

### Module not found

Ensure you're using relative paths correctly:
```hcl
source = "../../modules/module-name"  # From environments/dev
source = "../modules/module-name"     # From shared
```

### Changes not applying

Clear module cache:
```bash
rm -rf .terraform/modules
terraform init
```

## Further Reading

- [Terraform Module Documentation](https://www.terraform.io/docs/language/modules/index.html)
- [Google Cloud Provider Documentation](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Best Practices for Terraform Modules](https://www.terraform.io/docs/cloud/guides/recommended-practices/index.html)
