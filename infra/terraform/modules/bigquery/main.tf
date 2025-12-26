# BigQuery Module
# Creates and manages BigQuery datasets

resource "google_bigquery_dataset" "dataset" {
  for_each = var.datasets

  dataset_id                 = each.key
  project                    = var.project_id
  location                   = var.location
  description                = each.value.description
  default_table_expiration_ms = lookup(each.value, "default_table_expiration_ms", null)

  labels = merge(
    lookup(each.value, "labels", {}),
    {
      managed_by  = "terraform"
      environment = var.environment
    }
  )

  dynamic "access" {
    for_each = lookup(each.value, "access", [])
    content {
      role          = access.value.role
      user_by_email = lookup(access.value, "user_by_email", null)
      group_by_email = lookup(access.value, "group_by_email", null)
      special_group = lookup(access.value, "special_group", null)
    }
  }
}

# Optional: Create specific tables (if needed)
resource "google_bigquery_table" "tables" {
  for_each = var.tables

  dataset_id = google_bigquery_dataset.dataset[each.value.dataset_id].dataset_id
  table_id   = each.key
  project    = var.project_id

  description = lookup(each.value, "description", null)

  dynamic "time_partitioning" {
    for_each = lookup(each.value, "time_partitioning", null) != null ? [each.value.time_partitioning] : []
    content {
      type                     = time_partitioning.value.type
      field                    = lookup(time_partitioning.value, "field", null)
      expiration_ms            = lookup(time_partitioning.value, "expiration_ms", null)
      require_partition_filter = lookup(time_partitioning.value, "require_partition_filter", false)
    }
  }

  dynamic "clustering" {
    for_each = lookup(each.value, "clustering", null) != null ? [each.value.clustering] : []
    content {
      fields = clustering.value.fields
    }
  }

  schema = lookup(each.value, "schema", null)

  labels = merge(
    lookup(each.value, "labels", {}),
    {
      managed_by  = "terraform"
      environment = var.environment
    }
  )
}
