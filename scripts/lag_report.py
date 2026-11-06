#!/usr/bin/env python3
"""Print per-partition consumer lag for one consumer group as JSON.

Credentials come from KAFKA_API_KEY and KAFKA_API_SECRET, never the command line.
Leave both unset for a plaintext cluster. Only partitions with a committed offset
are listed, so a group that has never committed prints [].

Exit codes: 0 success, 1 unreachable or failed, 2 group not found or bad usage.
"""
import argparse
import json
import os
import sys
import time

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

ATTEMPTS = 3  # The first try plus the two retries the brief asks for.
RETRY_DELAY = 2
TIMEOUT = 10
# Only transient failures are retried. Bad credentials will not fix themselves.
RETRIABLE = {KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN, KafkaError._TIMED_OUT, KafkaError._RESOLVE,
             KafkaError.REQUEST_TIMED_OUT, KafkaError._NOENT, KafkaError.COORDINATOR_NOT_AVAILABLE,
             KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS}

def client_config(bootstrap: str, on_error, key: str | None, secret: str | None) -> dict:
    config = {"bootstrap.servers": bootstrap, "client.id": "lag-report", "error_cb": on_error}
    if key and secret:
        config.update({"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
                       "sasl.username": key, "sasl.password": secret})
    return config

def fetch_lag(admin: AdminClient, group: str) -> list[dict] | None:
    """Return one row per committed partition, or None if the group does not exist."""
    try:
        state = admin.describe_consumer_groups([group], request_timeout=TIMEOUT)[group].result().state
    except KafkaException as exc:
        if exc.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND:
            return None
        raise
    if state.name == "DEAD":  # An unknown classic group is reported as DEAD, not as an error.
        return None
    offsets = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)], request_timeout=TIMEOUT)
    committed = offsets[group].result().topic_partitions
    if not committed:
        return []  # The group exists but has never committed an offset.

    # Read after the commits, so a busy topic cannot produce a negative lag.
    wanted = {TopicPartition(tp.topic, tp.partition): OffsetSpec.latest() for tp in committed}
    ends = admin.list_offsets(wanted, request_timeout=TIMEOUT)
    end_offsets = {(tp.topic, tp.partition): f.result().offset for tp, f in ends.items()}
    if min(end_offsets.values()) < 0:  # Leader down: -1001 and no error, which must not print as lag 0.
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT, "partition leader unavailable"))
    return [{"topic": tp.topic, "partition": tp.partition, "current_offset": tp.offset,
             "log_end_offset": end_offsets[(tp.topic, tp.partition)],
             "lag": max(end_offsets[(tp.topic, tp.partition)] - tp.offset, 0)}
            for tp in sorted(committed, key=lambda t: (t.topic, t.partition))]

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--group", required=True)
    args = parser.parse_args()
    key, secret = os.environ.get("KAFKA_API_KEY"), os.environ.get("KAFKA_API_SECRET")
    if bool(key) != bool(secret):
        parser.error("set both KAFKA_API_KEY and KAFKA_API_SECRET, or neither")

    for attempt in range(1, ATTEMPTS + 1):
        client_errors: list[KafkaError] = []  # A failed login surfaces here, not on the request.
        # A new client each time: reusing one after a timeout can crash librdkafka.
        admin = AdminClient(client_config(args.bootstrap_servers, client_errors.append, key, secret))
        try:
            rows = fetch_lag(admin, args.group)
            if rows is None:
                print(f"error: consumer group '{args.group}' does not exist", file=sys.stderr)
                return 2
            print(json.dumps(rows, indent=2))
            return 0
        except KafkaException as exc:
            error = exc.args[0]
            admin.poll(0)  # Delivers any queued client errors to client_errors.
            if any(e.code() == KafkaError._AUTHENTICATION for e in client_errors):
                print("error: authentication failed, check the API key and secret", file=sys.stderr)
                return 1
            if error.code() not in RETRIABLE:
                print(f"error: {error.str()}", file=sys.stderr)
                return 1
            print(f"attempt {attempt}/{ATTEMPTS} failed: {error.str()}", file=sys.stderr)
            if attempt < ATTEMPTS:
                time.sleep(RETRY_DELAY)
    print(f"error: could not get offsets from {args.bootstrap_servers}", file=sys.stderr)
    return 1

if __name__ == "__main__":
    sys.exit(main())
