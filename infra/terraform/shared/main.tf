# Shared Resources Configuration
# Resources that are shared across all environments (dev, prd)

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "polar-scene-465223-f7-terraform-state"
    prefix = "shared"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Artifact Registry for Docker images (shared across environments)
module "artifact_registry" {
  source = "../modules/artifact-registry"

  project_id    = var.project_id
  region        = var.region
  repository_id = var.artifact_registry_repository

  description = "Docker repository for ELA Data Platform fetcher images"

  labels = {
    project = "ela-dataplatform"
    purpose = "docker-images"
  }

  # Cleanup policy to keep only recent images
  cleanup_policies = [
    {
      id     = "keep-recent-versions"
      action = "KEEP"
      condition = {
        tag_state = "TAGGED"
      }
      most_recent_versions = {
        keep_count = 10
      }
    },
    {
      id     = "delete-untagged"
      action = "DELETE"
      condition = {
        tag_state  = "UNTAGGED"
        older_than = "2592000s" # 30 days
      }
    }
  ]

  # Grant GitHub Actions service account push access
  iam_members = {
    github_actions = {
      role   = "roles/artifactregistry.writer"
      member = "serviceAccount:github-actions@${var.project_id}.iam.gserviceaccount.com"
    }
  }
}

# Service Account for Cloud Run Jobs (shared)
resource "google_service_account" "cloud_run_sa" {
  account_id   = "cloud-run-jobs"
  display_name = "Cloud Run Jobs Service Account"
  description  = "Service account for Cloud Run Jobs across all environments"
  project      = var.project_id
}

# Grant necessary permissions to the service account
resource "google_project_iam_member" "cloud_run_sa_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/secretmanager.secretAccessor",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}
