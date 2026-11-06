locals {
  topic_name = "orders.v1"
}

resource "confluent_environment" "this" {
  display_name = var.name_prefix

  # Essentials is the lowest-cost package that includes Schema Registry.
  stream_governance {
    package = "ESSENTIALS"
  }
}

# Basic: the assessment does not need Dedicated, and the first eCKU is free.
resource "confluent_kafka_cluster" "this" {
  display_name = var.name_prefix
  availability = "SINGLE_ZONE" # Basic clusters are single-zone.
  cloud        = var.cloud
  region       = var.region
  basic {}

  environment {
    id = confluent_environment.this.id
  }

  lifecycle {
    # Deleting the cluster deletes its data, so it takes a deliberate code change.
    prevent_destroy = true
  }
}

# Schema Registry is provisioned with the environment, so it is read, not created.
# It only becomes available once the environment has a cluster.
data "confluent_schema_registry_cluster" "this" {
  environment {
    id = confluent_environment.this.id
  }

  depends_on = [confluent_kafka_cluster.this]
}

# Topics and ACLs are managed through the cluster's data plane, which needs a
# Kafka API key with admin rights. This account exists for that reason, so the
# producer below can stay least-privilege.
resource "confluent_service_account" "manager" {
  display_name = "${var.name_prefix}-manager"
  description  = "Terraform-only: manages topics and ACLs on ${var.name_prefix}."
}

resource "confluent_role_binding" "manager_cluster_admin" {
  principal   = "User:${confluent_service_account.manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.this.rbac_crn
}

resource "confluent_api_key" "manager" {
  display_name = "${var.name_prefix}-manager-kafka"
  description  = "Kafka API key used by Terraform for topics and ACLs."

  owner {
    id          = confluent_service_account.manager.id
    api_version = confluent_service_account.manager.api_version
    kind        = confluent_service_account.manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }

  # The key must not be used until the role that makes it useful exists.
  depends_on = [confluent_role_binding.manager_cluster_admin]
}

resource "confluent_kafka_topic" "orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  topic_name = local.topic_name

  # 6: enough parallelism for up to 6 consumers; partitions can grow but never shrink.
  partitions_count = 6

  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint

  config = {
    # 3: survives a broker loss; Confluent Cloud's fixed value, stated to be explicit.
    "replication.factor" = "3"

    # 2: with acks=all, each write is on 2 replicas, and one replica can be down.
    "min.insync.replicas" = "2"
  }

  credentials {
    key    = confluent_api_key.manager.id
    secret = confluent_api_key.manager.secret
  }

  lifecycle {
    # Deleting a topic deletes its data, so it takes a deliberate code change.
    prevent_destroy = true

    # replication.factor is applied at creation but not returned on read, so
    # without this every later plan shows a false diff.
    ignore_changes = [config["replication.factor"]]
  }
}

resource "confluent_service_account" "producer" {
  display_name = "${var.name_prefix}-producer"
  description  = "Application identity that may only produce to ${local.topic_name}."
}

resource "confluent_api_key" "producer" {
  display_name = "${var.name_prefix}-producer-kafka"
  description  = "Kafka API key for the ${local.topic_name} producer."

  owner {
    id          = confluent_service_account.producer.id
    api_version = confluent_service_account.producer.api_version
    kind        = confluent_service_account.producer.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }
}

# Least privilege: LITERAL scopes both ACLs to exactly orders.v1, not a prefix.
# WRITE allows producing; DESCRIBE lets the client fetch the topic's metadata.
resource "confluent_kafka_acl" "producer" {
  for_each = toset(["WRITE", "DESCRIBE"])

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.orders.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.producer.id}"
  host          = "*"
  operation     = each.key
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.manager.id
    secret = confluent_api_key.manager.secret
  }
}
