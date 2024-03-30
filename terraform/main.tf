terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "flow-caba-dataset" {
  dataset_id = var.bigquery_dataset_name
  location   = var.location
}

resource "google_storage_bucket" "flow-caba-bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  storage_class = var.gcs_storage_class

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

}
