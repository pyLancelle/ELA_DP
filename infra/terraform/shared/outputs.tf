# Shared Resources Outputs

output "artifact_registry_url" {
  description = "URL for the Artifact Registry"
  value       = module.artifact_registry.repository_url
}

output "artifact_registry_id" {
  description = "ID of the Artifact Registry repository"
  value       = module.artifact_registry.repository_id
}

output "cloud_run_sa_email" {
  description = "Email of the Cloud Run service account"
  value       = google_service_account.cloud_run_sa.email
}

output "cloud_run_sa_name" {
  description = "Name of the Cloud Run service account"
  value       = google_service_account.cloud_run_sa.name
}
