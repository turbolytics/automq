#!/bin/sh
# The Kafka differential for half_dead. On AutoMQ, the leader of partition 0
# loses S3 for 30 s: it takes writes it cannot make durable. Here, one
# follower of partition 0 freezes for 45 s, and neither the session timeout
# nor the replica lag drops it from the in-sync set, so the leader holds every
# acks=all write for 45 s, past the client's 30 s message timeout. If the
# client re-sends writes of unknown outcome under a new epoch and the leader
# later commits the originals, Kafka shows the same duplicates AutoMQ did.
# Run from the AutoMQ checkout's root.
p=temper-automq-fdiff
export MESSAGE_TIMEOUT_MS=${MESSAGE_TIMEOUT_MS:-30000}
export SESSION_MS=60000 REPLICA_LAG_MS=60000
C="docker compose -f temper/compose-kafka.yml -p $p"
O="$C run --rm --no-deps -T"
q() { grep -v -E 'config.json|^ *Container|WARN|orphan|Image|Built'; }
stalled() { [ "$2" -le "$3" ] || echo "HOST STALLED: a $1 s wait took $2 s; this run is invalid"; }
$C down -v >/dev/null 2>&1
$C build -q oracle 2>&1 | q
$C up -d toxiproxy kafka1 kafka2 kafka3 2>&1 | q | tail -1
for i in $(seq 1 90); do
  b=$($O oracle python -m oracle ready 2>/dev/null | tail -1)
  [ "$b" = 3 ] && break
  sleep 2
done
echo "kafka brokers: $b"
$O oracle python -m oracle setup automq 2>&1 | q | tail -1
sleep 5
leader=$($O oracle python -m oracle leader x 0 2>/dev/null | tail -1)
for k in kafka1 kafka2 kafka3; do [ "$k" != "$leader" ] && follower=$k; done
echo "partition 0: leader $leader, freezing follower $follower"
$C run -d --no-deps -e DEBUG_EOS=1 -e MESSAGE_TIMEOUT_MS oracle python -m oracle write automq 120 2>&1 | q | tail -1
t0=$(date +%s); sleep 20; stalled 20 $(( $(date +%s) - t0 )) 35
docker pause "$p-$follower-1" >/dev/null
echo "$(date -u +%T) froze $follower"
t0=$(date +%s); sleep 45; stalled 45 $(( $(date +%s) - t0 )) 60
docker unpause "$p-$follower-1" >/dev/null
echo "$(date -u +%T) thawed $follower"
for i in $(seq 1 60); do
  w=$($O oracle python -m oracle writing automq 2>/dev/null | tail -1)
  [ "$w" = 1 ] && break
  sleep 5
done
echo "$(date -u +%T) writer done: $w"
echo "leaders after: $($O oracle python -c 'from oracle.workload import leaders; print(leaders())' 2>&1 | q | tail -1)"
$O oracle python -m oracle dupinfo automq 2>&1 | q
$O oracle sh -c 'cd /ledger/automq && for f in rdkafka-*.log; do echo "$f: $(grep -c "bumped epoch" $f) epoch bumps, $(grep -c "drain before retrying" $f) retry drains"; done; echo "produce errors:"; cat rdkafka-*.log | grep -i -o -E "(Produce|MessageSet)[^:]*: [^(]*error[^(]*|Err[A-Za-z_]+|_?[A-Z]{3,}_[A-Z_]+" | sed -E "s/[0-9]+//g" | sort | uniq -c | sort -rn | head -15' 2>&1 | q
$C down -v >/dev/null 2>&1
