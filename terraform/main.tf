terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "project-data-talk-club-terraform-state"
    prefix = "terraform/state"
  }
}

# Bucket para guardar el Terraform state
resource "google_storage_bucket" "terraform_state" {
  name          = "${var.project_id}-terraform-state"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }
}

# Bucket para el data lake (raw y processed)
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }
}

# Dataset principal
resource "google_bigquery_dataset" "australian_rentals" {
  dataset_id = "australian_rentals"
  location   = var.region
}

# Dataset de dbt
resource "google_bigquery_dataset" "australian_rentals_dbt" {
  dataset_id = "australian_rentals_dbt"
  location   = var.region
}