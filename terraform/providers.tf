provider "google" {
  credentials = file("../gcp_credentials.json")
  project     = var.gcp_project_id
  region      = var.region
}