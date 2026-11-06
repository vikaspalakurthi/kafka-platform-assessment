#!/bin/sh
# First boot writes a starting config with one replica fetcher and formats
# storage. Later boots reuse the file, so a restart picks up what Ansible wrote.
set -eu

config=/etc/kafka/server.properties

if [ ! -f "$config" ]; then
  cat > "$config" <<EOF
node.id=${NODE_ID}
process.roles=broker,controller
controller.quorum.voters=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka${NODE_ID}:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/var/lib/kafka/data
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
num.replica.fetchers=1
EOF
fi

if [ ! -f /var/lib/kafka/data/meta.properties ]; then
  /opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$config"
fi

# exec makes Kafka PID 1, so it receives SIGTERM and shuts down cleanly.
exec /opt/kafka/bin/kafka-server-start.sh "$config"
