# Cloud Run Jobs Module Outputs

output "job_names" {
  description = "Map of job keys to job names"
  value       = { for k, v in google_cloud_run_v2_job.jobs : k => v.name }
}

output "job_ids" {
  description = "Map of job keys to job IDs"
  value       = { for k, v in google_cloud_run_v2_job.jobs : k => v.id }
}

output "scheduler_names" {
  description = "Map of scheduler keys to scheduler names"
  value       = { for k, v in google_cloud_scheduler_job.schedulers : k => v.name }
}
