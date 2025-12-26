# BigQuery Module Outputs

output "dataset_ids" {
  description = "Map of dataset names to dataset IDs"
  value       = { for k, v in google_bigquery_dataset.dataset : k => v.dataset_id }
}

output "dataset_self_links" {
  description = "Map of dataset names to self links"
  value       = { for k, v in google_bigquery_dataset.dataset : k => v.self_link }
}

output "table_ids" {
  description = "Map of table names to table IDs"
  value       = { for k, v in google_bigquery_table.tables : k => v.table_id }
}
