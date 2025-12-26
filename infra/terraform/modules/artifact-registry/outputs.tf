# Artifact Registry Module Outputs

output "repository_id" {
  description = "ID of the repository"
  value       = google_artifact_registry_repository.repository.repository_id
}

output "repository_name" {
  description = "Full name of the repository"
  value       = google_artifact_registry_repository.repository.name
}

output "repository_url" {
  description = "URL for pushing images to the repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.repository.repository_id}"
}
