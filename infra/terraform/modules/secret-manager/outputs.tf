# Secret Manager Module Outputs

output "secret_ids" {
  description = "Map of secret names to secret IDs"
  value       = { for k, v in google_secret_manager_secret.secrets : k => v.secret_id }
}

output "secret_names" {
  description = "Map of secret names to full resource names"
  value       = { for k, v in google_secret_manager_secret.secrets : k => v.name }
}
