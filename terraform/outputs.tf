output "bootstrap_endpoint" {
  description = "Kafka bootstrap endpoint for clients."
  value       = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "schema_registry_endpoint" {
  description = "Schema Registry REST endpoint."
  value       = data.confluent_schema_registry_cluster.this.rest_endpoint
}

output "producer_api_key" {
  description = "API key ID for the orders.v1 producer."
  value       = confluent_api_key.producer.id
}

output "producer_api_secret" {
  description = "API secret for the orders.v1 producer."
  value       = confluent_api_key.producer.secret
  sensitive   = true
}
