variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "australia-southeast1"
}

variable "credentials_file" {
  description = "Path to GCP service account JSON"
  type        = string
}