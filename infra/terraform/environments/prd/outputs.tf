# Production Environment Outputs

output "bucket_name" {
  description = "Name of the prd data bucket"
  value       = module.storage.bucket_name
}

output "bucket_url" {
  description = "URL of the prd data bucket"
  value       = module.storage.bucket_url
}

output "bigquery_datasets" {
  description = "BigQuery dataset IDs"
  value       = module.bigquery.dataset_ids
}

output "cloud_run_jobs" {
  description = "Cloud Run job names"
  value       = module.cloud_run_jobs.job_names
}

output "cloud_run_schedulers" {
  description = "Cloud Scheduler job names"
  value       = module.cloud_run_jobs.scheduler_names
}

output "secrets" {
  description = "Secret Manager secret IDs"
  value       = module.secrets.secret_ids
}
