resource "google_storage_bucket" "batch_amazon_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  storage_class = "STANDARD"
}

resource "google_bigquery_dataset" "kindle_reviews_dataset" {
  dataset_id = "kindle_reviews_dataset"
  project    = var.gcp_project_id
  location   = var.region
  description = "Dataset for storing Kindle reviews"
  labels = {
    environment = "dev"
  }
}

