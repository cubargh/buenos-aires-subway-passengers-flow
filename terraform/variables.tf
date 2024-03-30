variable "project_id" {
  type        = string
  description = "The GCP project ID"
  default     = "zoomcamp-0"
}

variable "region" {
  type        = string
  description = "The region where resources will be created"
  default     = "us-central1"
}

variable "bigquery_dataset_name" {
  type        = string
  description = "The name of the BigQuery dataset"
  default     = "caba_flow"
}

variable "location" {
  default     = "US"
}

variable "gcs_bucket_name" {
  default     = "caba_flow"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}