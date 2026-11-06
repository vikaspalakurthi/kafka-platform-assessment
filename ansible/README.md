# Rolling Kafka broker config change

This role sets a `server.properties` value (`num.replica.fetchers=4` by default) and restarts brokers one at a time. It restarts a broker only if its config changed, and it stops the rollout if the cluster is unhealthy.

## What it does on each broker

1. Fails the play if `kafka_properties` is missing a required setting, because the template owns the whole file.
2. Compares the rendered config with the file on disk, without writing. If they match and no restart is owed, it moves on.
3. Fails the play if there is no active controller, if under-replicated partitions (URP) are above 0, or if a check can't reach the cluster.
4. Records that a restart is owed, writes the config, and restarts the broker.
5. Waits for the port, then for URP to return to 0, before the next broker starts.

## Design decisions

| Decision | Reason |
|---|---|
| `serial: 1` with `any_errors_fatal` | RF=3 with `min.insync.replicas=2` tolerates one broker down, and any failure stops the rollout. |
| Health check before the config is written | A refused rollout leaves the broker unchanged. |
| Tasks instead of a handler | With a handler, the file is written first. After a failed check, the next run sees no change and the broker never restarts. |
| Restart marker file | If a restart fails after the write, the next run retries it, even though the config already matches. |
| URP wait after each restart | An open port only means the process is up, not that the broker has caught up. |
| Failed URP command fails the task | "Can't reach the cluster" must never count as 0. |
| Required settings check | An empty `kafka_properties` would otherwise replace the whole file with one line. |
| Diff off by default | `server.properties` can hold passwords, and a diff prints them. The lab turns it on because it has no secrets. |
| `become` per task | Only the config and restart tasks use root. The health checks run unprivileged. |

## Dry run

```bash
ansible-playbook -i inventory/lab.yml rolling_config.yml --check --diff
```

This runs the real health checks, because they are read-only. It writes nothing and restarts nothing. Each broker is previewed against its current state, and a dry run still fails if the cluster is unhealthy.

The lab inventory turns the diff on. Elsewhere, add `-e kafka_show_diff=true` only if the config has no secrets, because the output may end up in CI logs.

## Try it in the local lab

The lab is three KRaft nodes in Docker. It needs Docker Compose and `ansible-core`, and the `docker` CLI must work where Ansible runs. In WSL, run from a Linux path, because Ansible ignores `ansible.cfg` in a world-writable directory such as `/mnt/c`.

```bash
ansible-galaxy collection install -r requirements.yml
(cd lab && docker compose up -d --build)

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --topic orders.v1 --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2

ansible-playbook -i inventory/lab.yml rolling_config.yml --check --diff
ansible-playbook -i inventory/lab.yml rolling_config.yml
ansible-playbook -i inventory/lab.yml rolling_config.yml   # changed=0, no restarts
```

The lab can stay up between runs. `docker compose stop` and `docker compose start` keep its data, and `docker compose down -v` deletes it.

## Test results

Run against the lab with Kafka 4.1.0 and ansible-core 2.20.1. `ansible-lint` passes on the production profile.

| Test | Result |
|---|---|
| Dry run | Config unchanged, no marker left, no restarts |
| Empty `kafka_properties` | Failed on the first task, nothing rendered |
| Fake password in the config | Appeared 0 times with the diff off, 2 times with it on |
| Cluster unreachable | Failed at the first health check, no config written |
| Controller quorum lost (2 of 3 stopped) | Failed at the controller check, no config written |
| One broker stopped (URP = 6) | Failed on the first broker: no config written, no restarts |
| Restart command fails | Stopped at that broker, later brokers untouched, marker kept |
| Rerun after the failed restart, with a live `acks=all` producer | Restarted the owed broker, then finished the rollout. All 40,000 records present, p99 109 ms, max 753 ms |
| Second run | `changed=0`, container start times unchanged |

During the rollout the producer logged 7 retries, all `NOT_LEADER_OR_FOLLOWER`: the client found the new partition leader and carried on. No send failed.

## Using it on real brokers

Create an inventory with a `kafka_brokers` group and set these variables:

- `kafka_properties`: the broker's full configuration, because the template owns the whole file.
- `kafka_bootstrap_servers`: more than one broker.
- `kafka_client_config`: a client properties file, if the cluster uses SASL or TLS.
- `kafka_config_owner`, `kafka_config_group`, `kafka_bin_dir`: to match the install. The defaults restart with `systemctl restart kafka`.
- `kafka_port_host`: if the broker doesn't listen on `127.0.0.1`.
- `kafka_required_properties`: the settings every broker must have.

Config and restart tasks use `become`. The health checks don't, so the Ansible user needs read access to `kafka_client_config`.

## Not tested

- The default `systemctl` restart. The lab containers have no systemd, so it overrides the restart command.
- `become`. The lab containers have no sudo, so it sets `kafka_become: false`.
- `kafka_client_config` against a secured cluster.
- The assert for a quorum that answers with no leader. In the lab test, the quorum command itself failed first.

## Known limits

- The URP check covers the whole cluster, so an unrelated URP blocks the rollout. That is deliberate.
- The controller check uses `kafka-metadata-quorum.sh`, so the role only supports KRaft.
- Order follows the inventory. Listing the active controller last avoids an extra failover.
- There is no automatic rollback. `backup: true` leaves the previous config beside the file.
- The recap shows `changed=1` on a refused broker. That comes from the preview task, which writes nothing.
