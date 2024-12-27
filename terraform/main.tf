resource "google_storage_bucket" "storage_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  storage_class = "STANDARD"
}

resource "google_bigquery_dataset" "reviews_dataset" {
  dataset_id  = var.BQ_DATASET_NAME
  project     = var.gcp_project_id
  location    = var.region
  description = "Dataset for storing Kindle reviews"
  labels = {
    environment = "dev"
  }
}
