# BigQuery Module Variables

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "location" {
  description = "Location for BigQuery datasets"
  type        = string
  default     = "EU"
}

variable "environment" {
  description = "Environment name (dev, prd)"
  type        = string
}

variable "datasets" {
  description = "Map of BigQuery datasets to create"
  type = map(object({
    description                 = optional(string)
    default_table_expiration_ms = optional(number)
    labels                      = optional(map(string))
    access = optional(list(object({
      role           = string
      user_by_email  = optional(string)
      group_by_email = optional(string)
      special_group  = optional(string)
    })))
  }))
  default = {}
}

variable "tables" {
  description = "Map of BigQuery tables to create"
  type = map(object({
    dataset_id  = string
    description = optional(string)
    schema      = optional(string)
    time_partitioning = optional(object({
      type                     = string
      field                    = optional(string)
      expiration_ms            = optional(number)
      require_partition_filter = optional(bool)
    }))
    clustering = optional(object({
      fields = list(string)
    }))
    labels = optional(map(string))
  }))
  default = {}
}
