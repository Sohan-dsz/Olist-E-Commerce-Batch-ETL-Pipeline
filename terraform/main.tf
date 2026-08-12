terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "gcp_project" {
  description = "GCP project ID to deploy into"
  type        = string
}

variable "gcp_region" {
  description = "Region for the dataset (BigQuery uses multi-regions like US/EU or specific regions)"
  type        = string
  default     = "US"
}

variable "dataset_id" {
  description = "BigQuery dataset name"
  type        = string
  default     = "olist_analytics"
}

provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}

resource "google_bigquery_dataset" "olist_analytics" {
  dataset_id  = var.dataset_id
  location    = var.gcp_region
  description = "Analytics-ready tables produced by the Olist batch ETL pipeline"

  # 90-day default expiry on tables in this dataset -- this is a portfolio/demo
  # project, not production, so there's no reason to let tables accumulate
  # indefinitely against the free tier storage quota if I forget to clean up.
  default_table_expiration_ms = 90 * 24 * 60 * 60 * 1000

  labels = {
    project = "olist-etl-pipeline"
    env     = "dev"
  }
}

# The three tables the pipeline writes are created by load.py itself via
# autodetect=True on load_table_from_dataframe, so they're intentionally not
# declared as google_bigquery_table resources here -- Terraform owns the dataset
# (the durable infra piece); the pipeline owns table schema (which changes
# whenever the aggregation logic changes, and shouldn't require a terraform apply
# every time a column gets added).

output "dataset_self_link" {
  value = google_bigquery_dataset.olist_analytics.self_link
}
