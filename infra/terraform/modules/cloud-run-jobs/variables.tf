# Cloud Run Jobs Module Variables

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Run Jobs"
  type        = string
  default     = "europe-west1"
}

variable "environment" {
  description = "Environment name (dev, prd)"
  type        = string
}

variable "default_image" {
  description = "Default Docker image to use for jobs"
  type        = string
}

variable "service_account_email" {
  description = "Service account email for running jobs"
  type        = string
}

variable "schedule_timezone" {
  description = "Timezone for Cloud Scheduler"
  type        = string
  default     = "Europe/Paris"
}

variable "jobs" {
  description = "Map of Cloud Run Jobs to create"
  type = map(object({
    image            = optional(string)
    description      = optional(string)
    service          = optional(string)
    cpu              = optional(string)
    memory           = optional(string)
    max_retries      = optional(number)
    timeout          = optional(string)
    schedule         = optional(string)  # Cron expression
    retry_count      = optional(number)
    attempt_deadline = optional(string)
    env_vars         = optional(map(string))
    secrets = optional(map(object({
      secret  = string
      version = optional(string)
    })))
    labels = optional(map(string))
  }))
  default = {}
}
