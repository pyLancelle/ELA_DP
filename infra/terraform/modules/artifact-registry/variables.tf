# Artifact Registry Module Variables

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Region for the repository"
  type        = string
  default     = "europe-west1"
}

variable "repository_id" {
  description = "ID of the repository"
  type        = string
}

variable "description" {
  description = "Description of the repository"
  type        = string
  default     = "Docker image repository"
}

variable "labels" {
  description = "Labels to apply to the repository"
  type        = map(string)
  default     = {}
}

variable "cleanup_policy_dry_run" {
  description = "If true, cleanup policies will not delete images"
  type        = bool
  default     = false
}

variable "cleanup_policies" {
  description = "Cleanup policies for the repository"
  type = list(object({
    id     = string
    action = string
    condition = object({
      tag_state             = optional(string)
      tag_prefixes          = optional(list(string))
      older_than            = optional(string)
      newer_than            = optional(string)
      package_name_prefixes = optional(list(string))
    })
    most_recent_versions = optional(object({
      package_name_prefixes = optional(list(string))
      keep_count            = optional(number)
    }))
  }))
  default = []
}

variable "iam_members" {
  description = "IAM members to grant access to the repository"
  type = map(object({
    role   = string
    member = string
  }))
  default = {}
}
