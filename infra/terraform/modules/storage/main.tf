# Cloud Storage Bucket Module
# Creates and manages GCS buckets for data storage

resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.location
  project       = var.project_id
  force_destroy = var.force_destroy

  uniform_bucket_level_access = true

  versioning {
    enabled = var.versioning_enabled
  }

  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      action {
        type          = lifecycle_rule.value.action.type
        storage_class = lookup(lifecycle_rule.value.action, "storage_class", null)
      }
      condition {
        age                   = lookup(lifecycle_rule.value.condition, "age", null)
        matches_prefix        = lookup(lifecycle_rule.value.condition, "matches_prefix", null)
        matches_suffix        = lookup(lifecycle_rule.value.condition, "matches_suffix", null)
        num_newer_versions    = lookup(lifecycle_rule.value.condition, "num_newer_versions", null)
        with_state            = lookup(lifecycle_rule.value.condition, "with_state", null)
      }
    }
  }

  labels = merge(
    var.labels,
    {
      managed_by  = "terraform"
      environment = var.environment
    }
  )
}

# IAM binding for bucket access
resource "google_storage_bucket_iam_member" "members" {
  for_each = var.iam_members

  bucket = google_storage_bucket.bucket.name
  role   = each.value.role
  member = each.value.member
}
