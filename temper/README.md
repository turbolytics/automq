# Temper map for AutoMQ

This map checks the reliability claims in AutoMQ's README: "zero RPO, RTO in
seconds" and "100% Kafka compatible". It runs AutoMQ 1.7.5
(`automqinc/automq`) as three servers, each a broker and a KRaft controller,
on MinIO, in Docker. Toxiproxy sits on every broker-to-S3 edge, so a scenario
can cut, slow or reset one broker's access to S3 while everything else keeps
working.

Each check is a Temper scenario in `temper/scenarios`. Each claim is an
invariant in `temper/invariants.yml`, quoting where it comes from.
`MATRIX.md` shows the result of every check.

## How a check works

1. Two idempotent producers (acks=all) send numbered values to every
   partition. Each producer writes a ledger line when a send's outcome
   arrives: the offset it was acknowledged at, or the error.
2. The scenario injects its faults while the producers are still sending. A
   check before the faults fails the run if they have already finished.
3. The oracle (`temper/recipe/oracle/oracle/verdict.py`) reads every
   partition back and compares it with the ledger. It looks for acknowledged
   writes that are missing or moved, values written twice, a producer's
   values out of order, failed writes that came back after a later
   acknowledged write, records nobody sent, and the longest gap between
   acknowledgements on each partition.

## Accelerated timeouts

Brokers heartbeat every 1 s and are fenced after 4 s without one (2 s and 9 s
by default). Producers fail a send after 120 s (300 s by default). That is
longer than any fault here, on purpose: see the ledger. `temper/compose.yml`
lists every setting with its default.

## Scenarios

| Scenario | Faults |
|---|---|
| `baseline` | none |
| `broker_crash` | a server killed every 10-20 s for a minute, started again 0.5-2.5 s later |
| `half_dead` | the S3 edge of partition 0's leader cut for 30 s, while its heartbeats to the controllers stay healthy |
| `s3_slow` | every S3 edge slowed by 2 s for 30 s, then by 5 s for 20 s |
| `s3_outage` | every S3 edge cut for 20 s, then every connection reset |

## Results

The recorded run passed 38 checks, and the matrix shows all 9 claims covered.
AutoMQ held every claim we tested. Across the five scenarios the producers
sent 156,157 values. Every one was acknowledged, and every acknowledged value
was read back once, in order, at the offset it was acknowledged at.

- **A broker without S3 acknowledges nothing it did not store.** In
  half_dead, the leader's writes waited for S3 and were acknowledged after the
  edge came back.
- **Crashes recover in seconds.** In broker_crash, the longest gap between
  acknowledgements on any partition was 15.7 s, under the 30 s bound.
- **No phantoms.** Every record in every partition was sent by a producer.

One claim held only trivially: `failed.stays_failed`. With a 120 s timeout,
no send failed, so there was no failed write to check.

## What the map does not cover yet

- A zombie broker: a leader cut off from the controllers but still reaching
  S3 and its clients. This needs an iptables sidecar on the raft edges.
- Partition reassignment and the auto-balancer moving partitions under load.
- Transactions.
- The Java client. Every check uses librdkafka.

## The duplicates were the client's

With a 30 s message timeout, half_dead wrote thousands of values twice, most
of them acknowledged. Apache Kafka shows the same with the same client:
librdkafka starts a new producer epoch when a send times out and re-sends
writes of unknown outcome under it (confluentinc/librdkafka#4953). The ledger
(`temper/LEDGER.md`) has the details, and the Kafka differential reproduces
it:

    sh temper/recipe/kafka_freeze_diff.sh

## Running it

    temper run --level faults -- temper scenarios run --build
    temper matrix --all-levels

One scenario at a time takes about 2 minutes. Keep the host awake: a
sleeping Mac freezes the Docker VM and stretches every fault window.
