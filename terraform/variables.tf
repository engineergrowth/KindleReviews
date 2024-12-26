variable "gcp_project_id" {
  description = "Google Cloud project ID"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name"
}

variable "region" {
  description = "Google Cloud region"
  default     = "us-east2"
}
