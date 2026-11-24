"""The verdicts: read every partition back and compare it with the ledger of
every send's outcome.

- An acknowledged send must be at the offset it was acknowledged at.
- No value may appear twice: the producers are idempotent, and the workload
  never resends a value, so a second copy is the broker's or the client's.
- Within a partition, one producer's values appear in the order it sent
  them (idempotent producers keep order).
- A send that failed may still have been written (a timeout leaves the
  outcome unknown), but it may not appear after a later send of the same
  producer that was acknowledged: that is a failed write coming back.
- Every value in the log was sent.
"""
import glob
import json
import os
import time
from collections import defaultdict

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from .common import BOOTSTRAP, PARTITIONS, TOPIC, ledger_dir, log

SHOW = 10


def read_log():
    """Every record of every partition, read committed, as {p: {offset: value}}.
    A read that stops short of a partition's end raises."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "oracle-reader", "enable.auto.commit": False,
                  "isolation.level": "read_committed", "auto.offset.reset": "earliest"})
    try:
        tps = [TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in range(PARTITIONS)]
        c.assign(tps)
        ends = {p: c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10, cached=False)[1]
                for p in range(PARTITIONS)}
        out = {p: {} for p in range(PARTITIONS)}
        todo = {p for p, e in ends.items() if e > 0}
        stop = time.time() + 120
        while todo and time.time() < stop:
            msg = c.poll(0.5)
            if msg is not None and not msg.error():
                out[msg.partition()][msg.offset()] = msg.value().decode()
            for tp in c.position([TopicPartition(TOPIC, p) for p in todo]):
                if tp.offset >= ends[tp.partition]:
                    todo.discard(tp.partition)
        if todo:
            raise SystemExit(f"{TOPIC}: partitions {sorted(todo)} not read to their end in 120 s")
        log(f"read {sum(len(v) for v in out.values())} records; partition ends {ends}")
        return out
    finally:
        c.close()


class Run:
    def __init__(self, subject):
        self.sends = {}
        for f in glob.glob(os.path.join(ledger_dir(subject), "acks-*.jsonl")):
            for line in open(f):
                s = json.loads(line)
                self.sends[s["v"]] = s
        if not self.sends:
            raise SystemExit("the ledger is empty: the writer did not run")
        self.log = read_log()
        self.seen = defaultdict(list)  # value -> [(partition, offset)]
        for p, recs in self.log.items():
            for o, v in recs.items():
                self.seen[v].append((p, o))
        acked = sum(1 for s in self.sends.values() if "o" in s)
        failed = [s for s in self.sends.values() if "err" in s]
        kinds = defaultdict(int)
        for s in failed:
            kinds[s["err"]] += 1
        log(f"ledger: {len(self.sends)} sends, {acked} acknowledged, {len(failed)} failed {dict(kinds)}; "
            f"{sum(1 for s in failed if s['v'] in self.seen)} failed sends are in the log")

    def lost(self):
        bad = [s for s in self.sends.values() if "o" in s and self.log[s["p"]].get(s["o"]) != s["v"]]
        for s in bad[:SHOW]:
            log(f"lost: {s['v']} acknowledged at partition {s['p']} offset {s['o']}, "
                f"which holds {self.log[s['p']].get(s['o'])!r}; found at {self.seen.get(s['v'])}")
        return len(bad)

    def dups(self):
        d = {v: at for v, at in self.seen.items() if len(at) > 1}
        for v, at in list(d.items())[:SHOW]:
            log(f"duplicate: {v} at {at} ({self.sends.get(v, {}).get('err', 'acknowledged')})")
        return len(d)

    def order(self):
        bad = 0
        for p, recs in self.log.items():
            top = {}  # pid -> (highest seq so far, its offset)
            for o in sorted(recs):
                v = recs[o]
                if ":" not in v:
                    continue
                pid, seq = v.split(":")[0], int(v.split(":")[1])
                if pid in top and seq < top[pid][0]:
                    bad += 1
                    if bad <= SHOW:
                        log(f"order: partition {p} offset {o} holds {v} after {pid}:{top[pid][0]} at offset {top[pid][1]}")
                else:
                    top[pid] = (seq, o)
        return bad

    def resurrected(self):
        """Failed sends that appear after a later acknowledged send of the
        same producer in the same partition."""
        acked_at = defaultdict(list)  # (pid, partition) -> [(seq, offset)]
        for s in self.sends.values():
            if "o" in s:
                acked_at[(s["pid"], s["p"])].append((s["seq"], s["o"]))
        bad = []
        for s in self.sends.values():
            if "err" not in s or s["v"] not in self.seen:
                continue
            for p, o in self.seen[s["v"]]:
                later = [(q, ao) for q, ao in acked_at[(s["pid"], p)] if q > s["seq"] and ao < o]
                if later:
                    bad.append((s, p, o, min(later)))
        for s, p, o, (q, ao) in bad[:SHOW]:
            log(f"resurrected: {s['v']} failed ({s['err']}) but is at partition {p} offset {o}, "
                f"after {s['pid']}:{q} acknowledged at offset {ao}")
        return len(bad)

    def unexpected(self):
        bad = [v for v in self.seen if v not in self.sends]
        for v in bad[:SHOW]:
            log(f"unexpected: {v!r} at {self.seen[v]} was never sent")
        return len(bad)

    def rto(self, limit=30.0):
        """1 when some partition went more than `limit` seconds without an
        acknowledgement while the writer ran, else 0."""
        by_p = defaultdict(list)
        for s in self.sends.values():
            if "o" in s:
                by_p[s["p"]].append(s["t"])
        worst = 0.0
        for p, ts in by_p.items():
            ts.sort()
            gap = max((b - a for a, b in zip(ts, ts[1:])), default=0.0)
            log(f"partition {p}: longest gap between acknowledgements {gap:.1f} s")
            worst = max(worst, gap)
        missing = [p for p in range(PARTITIONS) if p not in by_p]
        if missing:
            log(f"partitions {missing} acknowledged nothing")
            return 1
        return 1 if worst > limit else 0


def dupinfo(subject):
    """Describe the duplicates: how many copies, which partitions, whether
    the send was acknowledged, and where the copies sit."""
    r = Run(subject)
    from collections import Counter
    d = {v: sorted(at) for v, at in r.seen.items() if len(at) > 1}
    print(f"{len(d)} values duplicated; copies: {dict(Counter(len(a) for a in d.values()))}")
    print(f"by partition: {dict(Counter(a[0][0] for a in d.values()))}")
    print(f"by outcome: {dict(Counter(r.sends.get(v, {}).get('err', 'acked') for v in d))}")
    acked = [v for v in d if "o" in r.sends.get(v, {})]
    print(f"acknowledged and duplicated: {len(acked)}; ack offset is the first copy in "
          f"{sum(1 for v in acked if r.sends[v]['o'] == d[v][0][1])} of them")
    for p in range(PARTITIONS):
        offs = sorted(o for a in d.values() for (pp, o) in a if pp == p)
        if offs:
            firsts = sorted(a[0][1] for a in d.values() if a[0][0] == p)
            seconds = sorted(a[1][1] for a in d.values() if a[0][0] == p)
            print(f"partition {p}: first copies at offsets {firsts[0]}-{firsts[-1]}, "
                  f"second copies at {seconds[0]}-{seconds[-1]}")
    sample = sorted(d.items(), key=lambda kv: kv[1][0][1])[:5]
    for v, at in sample:
        print(f"  {v} at {at}: {r.sends.get(v)}")


METRICS = {"lost", "dups", "order", "resurrected", "unexpected", "rto"}


def check(subject, *pairs):
    """Print a results object: each argument ref=metric1+metric2 maps a
    reference to the sum of those violation counts."""
    r = Run(subject)
    cache, out = {}, {}
    for pair in pairs:
        ref, _, metrics = pair.partition("=")
        total = 0
        for m in metrics.split("+"):
            if m not in METRICS:
                raise SystemExit(f"unknown metric {m!r}; known: {sorted(METRICS)}")
            if m not in cache:
                cache[m] = getattr(r, m)()
            total += cache[m]
        out[ref] = total
    print(json.dumps(out))
