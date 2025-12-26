# Artifact Registry Module
# Creates and manages Docker repositories

resource "google_artifact_registry_repository" "repository" {
  location      = var.region
  repository_id = var.repository_id
  project       = var.project_id
  description   = var.description
  format        = "DOCKER"

  labels = merge(
    var.labels,
    {
      managed_by  = "terraform"
    }
  )

  cleanup_policy_dry_run = var.cleanup_policy_dry_run

  dynamic "cleanup_policies" {
    for_each = var.cleanup_policies
    content {
      id     = cleanup_policies.value.id
      action = cleanup_policies.value.action

      dynamic "condition" {
        for_each = [cleanup_policies.value.condition]
        content {
          tag_state             = lookup(condition.value, "tag_state", null)
          tag_prefixes          = lookup(condition.value, "tag_prefixes", null)
          older_than            = lookup(condition.value, "older_than", null)
          newer_than            = lookup(condition.value, "newer_than", null)
          package_name_prefixes = lookup(condition.value, "package_name_prefixes", null)
        }
      }

      dynamic "most_recent_versions" {
        for_each = lookup(cleanup_policies.value, "most_recent_versions", null) != null ? [cleanup_policies.value.most_recent_versions] : []
        content {
          package_name_prefixes = lookup(most_recent_versions.value, "package_name_prefixes", null)
          keep_count            = lookup(most_recent_versions.value, "keep_count", null)
        }
      }
    }
  }
}

# IAM binding for repository access
resource "google_artifact_registry_repository_iam_member" "members" {
  for_each = var.iam_members

  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.repository.name
  role       = each.value.role
  member     = each.value.member
}
