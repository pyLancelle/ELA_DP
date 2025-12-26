# Secret Manager Module
# Creates and manages secrets for credentials and API keys

resource "google_secret_manager_secret" "secrets" {
  for_each = var.secrets

  secret_id = each.key
  project   = var.project_id

  labels = merge(
    lookup(each.value, "labels", {}),
    {
      managed_by  = "terraform"
      environment = var.environment
    }
  )

  replication {
    auto {}
  }
}

# Optional: Create secret versions if values are provided
# Note: In production, secrets should be created manually or via CI/CD
resource "google_secret_manager_secret_version" "secret_versions" {
  for_each = {
    for k, v in var.secrets : k => v
    if lookup(v, "secret_data", null) != null
  }

  secret      = google_secret_manager_secret.secrets[each.key].id
  secret_data = each.value.secret_data
}

# IAM binding for secret access
resource "google_secret_manager_secret_iam_member" "members" {
  for_each = var.iam_members

  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.value.secret_id].secret_id
  role      = each.value.role
  member    = each.value.member
}
