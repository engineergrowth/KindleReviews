provider "google" {
  credentials = file("../gcp_credentials.json")
  project     = var.gcp_project_id
  region      = var.region
}

resource "google_storage_bucket" "batch_amazon_bucket" {
  name          = var.gcs_bucket_name
  location      = "US"
  storage_class = "STANDARD"
}

resource "google_bigquery_dataset" "kindle_reviews_dataset" {
  dataset_id = "kindle_reviews_dataset"
  project    = var.gcp_project_id
  location   = "US"
  description = "Dataset for storing Kindle reviews"
  labels = {
    environment = "dev"
  }
}

variable "gcp_project_id" {
  description = "Google Cloud project ID"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name"
}

variable "region" {
  description = "Google Cloud region"
}
