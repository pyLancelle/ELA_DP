# Storage Module Variables

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "location" {
  description = "Location for the bucket"
  type        = string
  default     = "EU"
}

variable "environment" {
  description = "Environment name (dev, prd)"
  type        = string
}

variable "force_destroy" {
  description = "Allow deletion of non-empty bucket"
  type        = bool
  default     = false
}

variable "versioning_enabled" {
  description = "Enable versioning for the bucket"
  type        = bool
  default     = true
}

variable "lifecycle_rules" {
  description = "List of lifecycle rules for the bucket"
  type = list(object({
    action = object({
      type          = string
      storage_class = optional(string)
    })
    condition = object({
      age                = optional(number)
      matches_prefix     = optional(list(string))
      matches_suffix     = optional(list(string))
      num_newer_versions = optional(number)
      with_state         = optional(string)
    })
  }))
  default = []
}

variable "labels" {
  description = "Labels to apply to the bucket"
  type        = map(string)
  default     = {}
}

variable "iam_members" {
  description = "IAM members to grant access to the bucket"
  type = map(object({
    role   = string
    member = string
  }))
  default = {}
}
