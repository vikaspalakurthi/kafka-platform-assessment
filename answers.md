# Section 1

## 1.1 Durability basics

A producer writes with `acks=all`. Topic has RF=3 and `min.insync.replicas=2`.

**What does `acks=all` actually wait for?**
acks=all actually wait for acknowledgement from all the replicas that are part of ISR. and min.insync.replicas is the minimum required replicas for the writes to go through. (leader will refuse the write if minimum is not satisfied.)


**What happens when one broker holding a replica goes offline?**
as long as the min.insync.replicas are there, writes continues. 
when a broker holding a follower goes down, after the lag timeout, leader sends a request to controller to update the ISR, controller validates the request and provides approval (epochs), 
when a broker holding a leader goes down, 
controller elects a new leader. 
No acknowledged data is lost (for a 3 broker 3 replica scenario)
Partitions does show as under-replicated. 


**What happens when two brokers go offline?**
since min insync is 2 and we have 3 brokers, when 2 brokers go offline, the ISR count drops to 1. So the writes get retries until delivery.timeout.ms, 
reads still work but only leader serves the requests. leader will send code 19 NOT_ENOUGH_REPLICAS until the ISR count is back to 2. producer can handle after the delivery timeout based on the app design. 

when the older leader broker comes back, it decides if it needs to truncate (based on diverging leader epoch) and then tries to catch after getting the new leader info and once ISR gets updated, writes can resume. 

when the older follower comes back, same determines if its consistent with new leader, mostly just catches up and then the ISR request by leader goes to controller. (this is where partition epoch gets used to validate the latest version of the partitions' metadata)

**Why is `min.insync.replicas=2` (not 3) a common choice?**
We aim for availability and also fault tolerance, if we have 3, any 1 broker going down will affect the writes. hence 2. 


## 1.2 Confluent Cloud on GCP, network path

You have a Confluent Cloud Dedicated cluster in us-central1 reached via Private Service Connect (PSC).

**Draw the path from a GCE client in your VPC to the cluster (PSC endpoint, DNS resolution, bootstrap, per-broker connection).**


```text
GCE client (my VPC)
   |
   | 1. resolve bootstrap hostname under <dns-domain>
   v
Cloud DNS private zone (attached to my VPC)
   |    * A record -> all 3 PSC endpoint IPs
   |
   | 2. TCP + TLS to port 9092 on one endpoint IP
   v
PSC endpoint (private IP in my subnet, one per zone)
   |
   | 3. forwarded to Confluent's zonal service attachment
   v
Confluent network
   |
   | 4. SASL auth (API key), then Metadata request
   | 5. metadata returns broker hostnames and partition leaders
   v
client resolves each broker hostname
   |    *.<zone-id> A record -> that zone's PSC endpoint IP
   |
   | 6. client connects to each partition leader it needs,
   |    through that zone's PSC endpoint
   v
broker in that zone
```

Bootstrap is only used to discover the brokers. Produce and fetch traffic then goes to individual brokers through their zonal endpoints, so a working bootstrap doesn't prove the whole path works. A multi-zone cluster needs 3 PSC endpoints, one per zonal service attachment, and my GCP project has to be registered on the Confluent network.

**Name the private DNS zone convention Confluent uses and which VPC it must be attached to.**

The Cloud DNS private zone is named after the Confluent network's DNS domain, which has the form `<network-id>.<region>.gcp.confluent.cloud`, for example `abcdef.us-central1.gcp.confluent.cloud`. It has one `*` A record with all 3 endpoint IPs, and one `*.<zone-id>` A record per zone (like `*.us-central1-a`) with only that zone's IP. It must be attached to the VPC where the clients and PSC endpoints live, because a private zone only answers queries from VPCs it's attached to.

**Name one common misconfiguration and how you would detect it.**

A zonal record is missing or points to the wrong zone's IP, or a zone's endpoint was never created. Bootstrap still works through the `*` record, but connections to some brokers then fail, so only some partitions are affected. The exact client error depends on the fault. It can be a connection failure or timeout, and Confluent lists `SSL handshake failed: Disconnected: connecting to a PLAINTEXT broker listener?` for a wrong-zone endpoint or bad private zone.

To detect it, I would run this from a VM in that VPC for each zone:

```bash
dig +short test.us-central1-a.<dns-domain>
```

It should return only that zone's endpoint IP. Getting an answer isn't enough, because if the zonal record is missing the `*` record may still answer with all 3 IPs. I would also test TLS with `openssl s_client -connect <bootstrap>:9092`. Ping is no use here because broker hosts don't respond to it.

## 1.3 Producer latency triage

Producer p99 latency jumped from 30 ms to ~400 ms. Throughput is unchanged. Broker CPU is flat.

**Give 4 likely causes, ranked most-to-least likely. For each cause, name one metric or one log line you would check.**

CPU is flat, Throughput unchanged, -> so no extra work, but still latency increased for p99. That means requests are waiting on something, and only p99 moving points to one slow component or intermittent stalls. I would start with the Produce time breakdown in `kafka.network:type=RequestMetrics`, because it shows which stage the time is going to, and check whether it's one broker or all of them.

1. **Slow follower or replication path.** `acks=all` is only as fast as the slowest ISR member, and waiting uses no CPU. Check `RemoteTimeMs` for request=Produce.
2. **Disk latency on a broker.** Blocked writes use no CPU and hit the tail first. Check `LocalTimeMs` for request=Produce, and `await` in `iostat -x 1`.
3. **Network problems, such as packet loss or retransmits.** TCP retransmit timeouts land in the hundreds of milliseconds, which fits 400 ms. Check retransmits with `netstat -s` or `ss -ti`, and `ResponseSendTimeMs`.
4. **Client-side change, such as `linger.ms`, `batch.size`, a full buffer or quota throttling.** Broker metrics look normal in this case. Check the producer's `record-queue-time-avg` and `produce-throttle-time-avg`.

GC pauses are a fair fifth candidate, because a short stop-the-world pause barely shows in average CPU.

**On the broker host, what shell command would you use to check for recent GC pauses? Show the actual command.**

```bash
# last 20 pauses over 100 ms (log path depends on the install)
awk '/Pause/ && $NF+0 > 100' /var/log/kafka/kafkaServer-gc.log | tail -20

# live view, every second for 10 seconds (needs a JDK)
jstat -gcutil "$(jps | awk '/Kafka/ {print $1}')" 1000 10
```

## 1.4 Ansible change gone wrong

Someone ran an Ansible play that restarted 3 of 6 brokers at the same time (`serial: 3`) to change a `server.properties` value. There were no health checks. After the play, several partitions show `UnderReplicatedPartitions > 0`.

**Why is `serial: 3` unsafe here?**
serial: 3 is unsafe when Replication factor is 3 and min.insync.replicas=2 (assumed from 1.1). For this scenario, with serial: 3, ansible tries to update exactly 3 brokers at the same time. so there could be a scenario, the whole partition goes down. which causes downtime and write failures. and also 2 replicas go down and still < min.insync replicas. with min.insync.replicas 2 and replication factor 3, our safe bet is serial: 1 no matter the number of brokers in the cluster. 


**Name 3 safety checks the play should include.**

1. First, before each restart, play needs to check if there are no under-replicated partitions and fail the play if not. say ISR count is 2, (under replicated), then the one broker goes down -> thats less than min.insync.replicas. 

2. After each restart, wait for the broker to rejoin and for under replicated partitions to go back to 0 before moving on. -> basically wait for everything to be good. 

3. for our 6 broker and min.insync.replicas: 2 scenario, use serial: 1 with any_errors_fatal: true, so one failed check can stop the whole rollout. (rf:3, min.insync.replicas: 2, affordable fault tolerance is 1, hence serial: 1)

**List the recovery steps in order.**

1. **Stop the change.** 

2. **Assess the damage.** Confirm there is an active controller (`ActiveControllerCount`), then check how many partitions are affected:
   ```bash
   kafka-topics --bootstrap-server $B --describe --under-replicated-partitions
   kafka-topics --bootstrap-server $B --describe --under-min-isr-partitions
   kafka-topics --bootstrap-server $B --describe --unavailable-partitions
   ```
3. **Get the 3 restarted brokers back up.** If one failed to start, read its logs and revert the config if that's the cause. Log recovery can take time after an unclean stop, so I wouldn't restart it again partway through. Confirm all 3 rejoined so all 6 are online.

4. **Fix offline partitions first,** because those are the actual outage. Bring back the brokers holding their replicas, and don't use unclean leader election unless the business accepts the data loss.

5. **Let replicas catch up.** Make sure the URP count keeps falling to 0, with no more restarts during this time.

6. **Rebalance leaders.** The restarted brokers came back leading nothing, so run:
   ```bash
   kafka-leader-election --bootstrap-server $B --election-type preferred --all-topic-partitions
   ```
7. **Validate.** Confirm the new config is active on the 3 restarted brokers, and that producer errors and consumer lag are back to normal.
8. **Finish the rollout.** Apply the change to the other 3 brokers with the fixed play (`serial: 1` with health checks), then write the postmortem.

## 1.5 ZooKeeper vs KRaft

**What role does ZooKeeper play in a Kafka cluster that uses it?**
Zookeeper is like a separate service that used for holding the cluster metadata, (broker membership, topic configs, ACLs...) and its used to elect the controller. -> main purpose. 


**What replaces ZooKeeper in a KRaft cluster?**
KRaft makes a quorum of controllers and these take over the cluster metadata. An active controller (leader) and standby controllers (followers). this active controller acts as the single source of truth for the cluster's metadata, it writes the metadata to __cluster_metadata log. 


**If you were building a new cluster today, which mode would you pick and why?**
I'd pick KRaft and at the same time, Kafka 4.0 onwards no Zookeeper. 
why ? -> one less component and controller failover is faster and it handles more partitions. 


