# Notes

## How this was validated

| Part | Tested against | Result |
|---|---|---|
| Terraform | Real Confluent Cloud resources: an environment with Schema Registry Essentials, a Basic cluster on GCP `us-central1`, `orders.v1`, two service accounts and the producer ACLs | `terraform apply` succeeded, and `terraform plan` came back clean afterwards |
| Ansible | Kafka 4.1.0 in a local 3-node Docker cluster, not production servers | Real rolling restarts under a live `acks=all` producer, with all 40,000 records present. Failure tests are in `ansible/README.md` |
| `lag_report.py` | The same Docker cluster, plus real Confluent Cloud for the login failure path | Lag matched `kafka-consumer-groups`, all 8 broker kill tests gave correct results, and a wrong key on Confluent Cloud exited 1 with a clear message |

## Terraform decisions

- **Second service account.** The brief asks for one service account, and `producer` is that account. Creating topics and ACLs needs a Kafka API key with admin rights, so I added `manager` for Terraform only. Without it, the producer would need admin rights, which defeats least privilege.
- **`CloudClusterAdmin` on `manager`.** It is broad, but managing ACLs needs cluster-level rights. It is scoped to this one cluster, and no application uses that key.
- **`replication.factor` finding.** I ran a real apply, although the brief doesn't require one. The provider accepts `replication.factor` at creation but doesn't return it on read, so every later plan showed a false diff. `ignore_changes` on that key fixes it, and the value stays in the code because the brief asks for it to be explicit.
- **`prevent_destroy` on the cluster and topic.** Deleting either one deletes data, so that should take a deliberate code change. It has to be removed before an intentional teardown.
- **Least privilege.** The producer has only `WRITE` and `DESCRIBE`, as `LITERAL` ACLs on `orders.v1`. On the live cluster, its key was denied group access.

## Deliberately skipped

| Skipped | Why | In production |
|---|---|---|
| Remote state backend | Local state keeps the assessment simple, and it is gitignored | Encrypted remote backend with locking, such as GCS, because state holds API secrets in plain text |
| API key rotation and a secret store | Out of scope | Rotate keys on a schedule, and hand the producer key over through a secret manager rather than a Terraform output |
| Dedicated identity for Terraform | The brief only needs env vars | The Cloud API key belongs to a service account with only the roles it needs |
| Multi-zone cluster | Basic clusters are single-zone, and the brief allows Basic | Standard or Dedicated with `MULTI_ZONE`, plus private networking |
| Schema Registry access for the producer | The brief asks for produce only | Add a scoped role if the application uses schemas |
| Automatic Ansible rollback | A bad config stops the rollout on the first broker, and `backup: true` keeps the previous file | Decide per change, because restarting into an old config can also be risky |
| Molecule tests | The Docker lab covers the same behaviour for one role | Add them if the role grows |

## Ansible lab and what was not tested

The role was tested on a 3-node KRaft cluster in Docker, not on 6 real brokers. Test results are in `ansible/README.md`.

Not tested, because the lab can't exercise them:

- The default `systemctl restart kafka` path. The containers have no systemd, so the lab overrides the restart command.
- `become`. The containers run as a normal user with no sudo, so the lab sets `kafka_become: false`.
- `kafka_client_config` against a secured cluster. The lab uses plaintext with no authentication, which is for local use only.

The health checks run without privilege, so the Ansible user needs read access to `kafka_client_config`. The controller check uses `kafka-metadata-quorum.sh`, so the role only supports KRaft.

## Section 3 decisions

- **Negative end offsets are a failed attempt.** With a partition leader down, `list_offsets` returns `-1001` with no exception. The first version turned that into `lag: 0` with exit 0, a false "healthy" during an outage. I found this by killing brokers in the lab, and the script now retries and fails with exit 1 if the leader stays down.
- **New client on each attempt.** Retrying on the same `AdminClient` after a timeout crashed inside librdkafka (exit 139). It crashed more often on 2.16.1 than on 2.12.2, so upgrading doesn't fix it, and I stayed on 2.12.2.
- **Failover errors are retried.** During a broker failure the client can return `_NOENT` ("No leaders found") or a coordinator error, so these are retried along with connection errors.
- **Half-set credentials are refused.** Otherwise the script would silently connect without authentication.

Left out to stay under the brief's ~100 lines:

- Bad usage exits 2 (the `argparse` default), the same code as "group not found". The message on stderr tells them apart.
- A blank `--group` or `--bootstrap-servers` isn't checked up front, so it fails with a poor message.
- Ctrl-C prints a traceback, and a closed pipe prints a `BrokenPipeError` message.
- Per-partition errors on committed offsets aren't checked. I couldn't trigger one.

Known limit: only partitions with a committed offset are listed. A group that committed 2 of 6 partitions shows 2 rows, so its total lag is understated, and a group that never committed prints `[]`. Covering this would need member assignments and topic metadata.

## Security and quality checks

| Tool | Covers |
|---|---|
| `terraform fmt` and `validate`, `tflint` | Terraform style and correctness |
| `checkov`, `trivy config` | Misconfiguration in Terraform, Dockerfile, Ansible and CI |
| `ansible-lint` (production profile), `yamllint` | Ansible and YAML |
| `gitleaks` | Secrets in files and git history |
| `ruff`, `mypy`, `bandit`, `pip-audit`, `pytest` | The Python script and its dependency |
| `actionlint` | The CI workflow (run locally) |

All of these except `actionlint` run in `.github/workflows/ci.yml`, and `.pre-commit-config.yaml` runs a faster subset before each commit. Tools, images and actions are pinned to exact versions, and actions are pinned to commit SHAs.

Limits of these results:

- `checkov` and `trivy` appear to have no rules for Confluent resources, so a clean Terraform scan proves little. Least privilege was checked by reviewing the ACLs and by the live group access check.
- One `checkov` finding is skipped in `.checkov.yaml`, with the reason there: the rollout block has no `rescue` on purpose.
- The lab image has known CVEs from its base image, reported by `trivy image`. It is a local test image and is never deployed.

## AI usage

I used Claude Code as a pair throughout: for drafting code and answers, checking facts against vendor docs, running scanners and running the lab tests. I reviewed the output and can explain the decisions. The real apply and the lab runs produced the findings above.

Answer 1.2 (Confluent Cloud on GCP over Private Service Connect) was generated with AI, including the concepts as well as the wording. It was not tested against a real PSC setup.
