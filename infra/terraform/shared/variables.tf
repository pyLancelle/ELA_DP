# Shared Resources Variables

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

variable "artifact_registry_repository" {
  description = "Name of the Artifact Registry repository"
  type        = string
  default     = "ela-dataplatform"
}
