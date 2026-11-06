variable "name_prefix" {
  description = "Prefix for the environment, cluster and service accounts."
  type        = string
  default     = "orders-dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,30}[a-z0-9]$", var.name_prefix))
    error_message = "name_prefix must be 3-32 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "cloud" {
  description = "Cloud provider hosting the cluster."
  type        = string
  default     = "GCP"

  validation {
    condition     = contains(["AWS", "AZURE", "GCP"], var.cloud)
    error_message = "cloud must be AWS, AZURE or GCP."
  }
}

variable "region" {
  description = "Cloud region hosting the cluster."
  type        = string
  default     = "us-central1"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.region))
    error_message = "region must be a cloud region ID, such as us-central1."
  }
}
