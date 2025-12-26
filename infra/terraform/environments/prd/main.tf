# Production Environment Configuration

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
    prefix = "environments/prd"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Data source to get shared resources outputs
data "terraform_remote_state" "shared" {
  backend = "gcs"
  config = {
    bucket = "polar-scene-465223-f7-terraform-state"
    prefix = "shared"
  }
}

# Cloud Storage buckets for prd environment
module "storage" {
  source = "../../modules/storage"

  project_id  = var.project_id
  bucket_name = "ela-dp-prd"
  location    = "EU"
  environment = "prd"

  versioning_enabled = true
  force_destroy      = false

  # Lifecycle rules for cost optimization
  lifecycle_rules = [
    {
      action = {
        type          = "SetStorageClass"
        storage_class = "NEARLINE"
      }
      condition = {
        age = 30 # Move to Nearline after 30 days
      }
    },
    {
      action = {
        type          = "SetStorageClass"
        storage_class = "COLDLINE"
      }
      condition = {
        age = 90 # Move to Coldline after 90 days
      }
    },
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 365 # Delete after 1 year
      }
    }
  ]

  labels = {
    environment = "prd"
    project     = "ela-dataplatform"
  }
}

# BigQuery datasets for prd environment
module "bigquery" {
  source = "../../modules/bigquery"

  project_id  = var.project_id
  location    = "EU"
  environment = "prd"

  datasets = {
    raw = {
      description = "Raw data ingestion layer (prd)"
      labels = {
        layer = "raw"
      }
    }
    lake = {
      description = "Cleaned and structured data layer (prd)"
      labels = {
        layer = "lake"
      }
    }
    hub = {
      description = "Business-oriented data models layer (prd)"
      labels = {
        layer = "hub"
      }
    }
    product = {
      description = "Product-ready data and metrics layer (prd)"
      labels = {
        layer = "product"
      }
    }
  }
}

# Secret Manager secrets for prd environment
module "secrets" {
  source = "../../modules/secret-manager"

  project_id  = var.project_id
  environment = "prd"

  secrets = {
    SPOTIFY_CLIENT_ID = {
      labels = {
        service = "spotify"
      }
    }
    SPOTIFY_CLIENT_SECRET = {
      labels = {
        service = "spotify"
      }
    }
    SPOTIFY_REFRESH_TOKEN = {
      labels = {
        service = "spotify"
      }
    }
    SPOTIFY_REDIRECT_URI = {
      labels = {
        service = "spotify"
      }
    }
    GARMIN_USERNAME = {
      labels = {
        service = "garmin"
      }
    }
    GARMIN_PASSWORD = {
      labels = {
        service = "garmin"
      }
    }
    GCP_SERVICE_ACCOUNT_KEY = {
      labels = {
        service = "gcp"
      }
    }
  }

  # Grant Cloud Run service account access to secrets
  iam_members = {
    spotify_client_id = {
      secret_id = "SPOTIFY_CLIENT_ID"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:${data.terraform_remote_state.shared.outputs.cloud_run_sa_email}"
    }
    spotify_client_secret = {
      secret_id = "SPOTIFY_CLIENT_SECRET"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:${data.terraform_remote_state.shared.outputs.cloud_run_sa_email}"
    }
    spotify_refresh_token = {
      secret_id = "SPOTIFY_REFRESH_TOKEN"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:${data.terraform_remote_state.shared.outputs.cloud_run_sa_email}"
    }
    spotify_redirect_uri = {
      secret_id = "SPOTIFY_REDIRECT_URI"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:${data.terraform_remote_state.shared.outputs.cloud_run_sa_email}"
    }
    garmin_username = {
      secret_id = "GARMIN_USERNAME"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:${data.terraform_remote_state.shared.outputs.cloud_run_sa_email}"
    }
    garmin_password = {
      secret_id = "GARMIN_PASSWORD"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:${data.terraform_remote_state.shared.outputs.cloud_run_sa_email}"
    }
    gcp_sa_key = {
      secret_id = "GCP_SERVICE_ACCOUNT_KEY"
      role      = "roles/secretmanager.secretAccessor"
      member    = "serviceAccount:${data.terraform_remote_state.shared.outputs.cloud_run_sa_email}"
    }
  }
}

# Cloud Run Jobs for prd environment
module "cloud_run_jobs" {
  source = "../../modules/cloud-run-jobs"

  project_id            = var.project_id
  region                = var.region
  environment           = "prd"
  default_image         = "${data.terraform_remote_state.shared.outputs.artifact_registry_url}/fetcher:latest"
  service_account_email = data.terraform_remote_state.shared.outputs.cloud_run_sa_email
  schedule_timezone     = "Europe/Paris"

  jobs = var.cloud_run_jobs
}
