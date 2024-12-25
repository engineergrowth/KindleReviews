provider "google" {
  credentials = file("../gcp_credentials.json")
  project     = "batch-amazon"
  region      = "us-east1"
}

resource "google_storage_bucket" "batch_amazon_bucket" {
  name          = "batch-amazon-data"
  location      = "US"
  storage_class = "STANDARD"
}
