# Cloud Run Jobs Module
# Creates and manages Cloud Run Jobs for data fetching and processing

resource "google_cloud_run_v2_job" "jobs" {
  for_each = var.jobs

  name     = each.key
  location = var.region
  project  = var.project_id

  template {
    template {
      containers {
        image = lookup(each.value, "image", var.default_image)

        # Environment variables
        dynamic "env" {
          for_each = lookup(each.value, "env_vars", {})
          content {
            name  = env.key
            value = env.value
          }
        }

        # Secrets from Secret Manager
        dynamic "env" {
          for_each = lookup(each.value, "secrets", {})
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = env.value.secret
                version = lookup(env.value, "version", "latest")
              }
            }
          }
        }

        resources {
          limits = {
            cpu    = lookup(each.value, "cpu", "1")
            memory = lookup(each.value, "memory", "512Mi")
          }
        }
      }

      max_retries = lookup(each.value, "max_retries", 0)
      timeout     = lookup(each.value, "timeout", "600s")

      service_account = var.service_account_email
    }
  }

  labels = merge(
    lookup(each.value, "labels", {}),
    {
      managed_by  = "terraform"
      environment = var.environment
      service     = lookup(each.value, "service", "unknown")
    }
  )
}

# Cloud Scheduler jobs for cron scheduling
resource "google_cloud_scheduler_job" "schedulers" {
  for_each = {
    for k, v in var.jobs : k => v
    if lookup(v, "schedule", null) != null
  }

  name             = "${each.key}-scheduler"
  region           = var.region
  project          = var.project_id
  description      = lookup(each.value, "description", "Scheduled job for ${each.key}")
  schedule         = each.value.schedule
  time_zone        = var.schedule_timezone
  attempt_deadline = lookup(each.value, "attempt_deadline", "320s")

  retry_config {
    retry_count = lookup(each.value, "retry_count", 0)
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.jobs[each.key].name}:run"

    oauth_token {
      service_account_email = var.service_account_email
    }
  }
}
