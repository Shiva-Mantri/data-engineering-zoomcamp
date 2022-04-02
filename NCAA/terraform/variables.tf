locals {
  data_lake_bucket = "ncaa-datalake-bucket"
}

variable "project" {
  description = "use your project: ncaa-data-eng-zc-2022"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-east4"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "ncaa_datawarehouse_bigquery"
}