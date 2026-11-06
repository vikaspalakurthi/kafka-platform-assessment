terraform {
  required_version = ">= 1.5.0, < 2.0.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "= 2.93.0" # Exact pin: a provider upgrade should be a reviewed change.
    }
  }
}

# No credentials here on purpose. The provider reads them from the environment:
#   CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET
provider "confluent" {}
