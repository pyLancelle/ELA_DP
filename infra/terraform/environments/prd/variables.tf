# Production Environment Variables

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "polar-scene-465223-f7"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "cloud_run_jobs" {
  description = "Map of Cloud Run Jobs to create"
  type = map(object({
    image            = optional(string)
    description      = optional(string)
    service          = optional(string)
    cpu              = optional(string)
    memory           = optional(string)
    max_retries      = optional(number)
    timeout          = optional(string)
    schedule         = optional(string)
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
