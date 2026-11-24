# Ledger

Every failure the map produced was checked against the harness, the client
and Apache Kafka before AutoMQ. Each line below was a harness, client or
environment problem, found and fixed. None was an AutoMQ bug.

1. **A health check on the client port deadlocked startup.** A server opens
   port 9092 only after the controller quorum forms, and the quorum needs
   all three servers started. The health check now probes the controller
   port, 9093.
2. **The bucket command needed a shell.** The MinIO client image's entrypoint
   is `mc`, so a scenario could not run a script in it. Its entrypoint is now
   `/bin/sh -c`.
3. **Duplicates under S3 cuts were the client's.** With a 30 s message
   timeout, half_dead wrote 3,569 values more than once, 3,358 of them
   acknowledged. The cause is librdkafka, the client library: when a send
   times out, it starts a new producer epoch and re-sends writes of unknown
   outcome under it, and the broker cannot tell the two copies apart. Apache
   Kafka 3.9.1 shows the same with the same client (below). This is known
   upstream as confluentinc/librdkafka#4953. The producers now time out a
   send after 120 s, longer than any fault here, so a duplicate left is the
   broker's. `MESSAGE_TIMEOUT_MS=30000` brings the client's behavior back.
4. **A slow client edge does not time a send out.** librdkafka's
   `request.timeout.ms` is how long the broker waits for replicas. The
   client's own network timeout is `socket.timeout.ms` (60 s). Responses
   delayed 12 s never failed a send. The differential freezes a follower
   instead.
5. **A frozen leader does not time a send out either.** A 15 s freeze is
   shorter than the 30 s message timeout, so neither cluster bumped an epoch.
6. **The host slept.** On battery, a sleeping Mac froze the Docker VM for up
   to 25 minutes, even under `caffeinate`, and stretched fault windows. One
   broker_crash run reported a 100 s gap between acknowledgements, which was
   the stall. The probes now flag any wait that took far longer than asked.
7. **A stale oracle image.** The differential ran an old build of the oracle.
   It now builds the image first.
8. **Shell word splitting.** Probe scripts written for zsh did not split
   variables, which wasted runs. Every script is now POSIX `sh`.

## The Kafka differential

`compose-kafka.yml` runs three Apache Kafka 3.9.1 brokers, replication factor
3, `min.insync.replicas` 2, with the same oracle, workload and client
(confluent-kafka 2.12.2, librdkafka 2.12.1). Client traffic goes through
Toxiproxy on each broker's client listener.

To match half_dead, one follower of partition 0 froze for 45 s. The broker
session timeout and `replica.lag.time.max.ms` were raised to 60 s, so the
follower stayed in the in-sync set and the leader held every acks=all write
for the whole freeze. With the 30 s message timeout, Kafka wrote 9,031 values
more than once, 5,649 of them acknowledged, up to 10 copies each, after 95
epoch bumps. AutoMQ under the same client showed the same pattern, smaller.
