# Secret Manager Module Variables

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, prd)"
  type        = string
}

variable "secrets" {
  description = "Map of secrets to create"
  type = map(object({
    secret_data = optional(string)  # Only use for non-sensitive defaults, prefer manual creation
    labels      = optional(map(string))
  }))
  default = {}
}

variable "iam_members" {
  description = "IAM members to grant access to secrets"
  type = map(object({
    secret_id = string
    role      = string
    member    = string
  }))
  default = {}
}
