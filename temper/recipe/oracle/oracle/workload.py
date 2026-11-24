"""The workload: create the topic, then producers send numbered values and
write a ledger line for every send's outcome."""
import json
import os
import random
import shutil
import signal
import threading
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .common import BOOTSTRAP, PARTITIONS, TOPIC, ledger_dir, log, wait


def admin():
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def ready(subject=None):
    """Print how many brokers the cluster reports (3 when it formed)."""
    try:
        print(len(admin().list_topics(timeout=5).brokers))
    except Exception as e:
        log(f"not ready: {e}")
        print(0)


def setup(subject):
    """Empty the subject's ledger and create the topic: one partition per
    broker, so each broker leads one."""
    d = ledger_dir(subject)
    shutil.rmtree(d)
    ledger_dir(subject)
    a = admin()
    fs = a.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS)])
    for _, f in fs.items():
        f.result(30)

    def led():
        t = a.list_topics(TOPIC, timeout=5).topics[TOPIC]
        return len(t.partitions) == PARTITIONS and all(p.leader >= 0 for p in t.partitions.values())
    wait("a leader for every partition", led, 60)
    log(f"topic {TOPIC}: {PARTITIONS} partitions, leaders {leaders()}")


def leaders():
    t = admin().list_topics(TOPIC, timeout=5).topics[TOPIC]
    return {p: t.partitions[p].leader for p in sorted(t.partitions)}


def leader(subject, partition="0"):
    """Print the Compose service that leads a partition (node n is server n+1)."""
    node = leaders()[int(partition)]
    # AutoMQ's node ids start at 0 (server1); the Kafka differential's at 1 (kafka1).
    print(f"kafka{node}" if "920" in BOOTSTRAP or "kafka" in BOOTSTRAP else f"server{node + 1}")


class _Logger:
    """A logging.Logger stand-in for librdkafka's log callback."""
    def __init__(self, f):
        self.f = f

    def log(self, level, fmt, *args):
        line = fmt % args if args else fmt
        if "Produce" in line and "eos" not in line.lower() and "Retry" not in line and "rror" not in line:
            return  # the protocol trace of every request: too much
        self.f.write(f"{time.time():.3f} {line}\n")


def write(subject, seconds="60", seed="1", producers="2"):
    """Each producer sends values pid:seq to partition seq % PARTITIONS for
    `seconds`, idempotent with acks=all, and writes one ledger line per send
    when its outcome arrives: the offset it was acknowledged at, or the error.
    A send whose outcome never arrived is recorded as unknown. Marks the
    ledger done when every producer finished."""
    d = ledger_dir(subject)
    until = time.time() + float(seconds)
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    counts = {}

    def run(pid):
        rng = random.Random(f"{seed}-{pid}")
        out = open(os.path.join(d, f"acks-{pid}.jsonl"), "w", buffering=1)
        pending = {}
        conf = {
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            # A send fails after 120 s (300 s by default), longer than any
            # fault here. When a send times out, librdkafka bumps the
            # producer epoch and re-sends writes of unknown outcome under it,
            # which duplicates them on Apache Kafka too (librdkafka #4953;
            # the follower-freeze differential shows it at 30 s). A timeout
            # past every fault keeps each duplicate the broker's.
            "message.timeout.ms": int(os.environ.get("MESSAGE_TIMEOUT_MS", "120000")),
            "request.timeout.ms": 10000,
        }
        if os.environ.get("DEBUG_EOS"):
            # librdkafka's idempotence log: producer ids, epoch bumps, retries.
            dbg = open(os.path.join(d, f"rdkafka-{pid}.log"), "w", buffering=1)
            conf["debug"] = "eos,protocol"
            conf["logger"] = _Logger(dbg)
        p = Producer(conf)
        c = counts[pid] = {"acked": 0, "failed": 0}

        def done(err, msg):
            v = msg.value().decode()
            pending.pop(v, None)
            line = {"v": v, "pid": pid, "seq": int(v.split(":")[1]), "p": msg.partition(), "t": time.time()}
            if err is None:
                line["o"] = msg.offset()
                c["acked"] += 1
            else:
                line["err"] = err.name()
                line["retriable"] = err.retriable()
                c["failed"] += 1
            out.write(json.dumps(line) + "\n")

        seq = 0
        while not stop and time.time() < until:
            v = f"{pid}:{seq}"
            try:
                p.produce(TOPIC, value=v.encode(), partition=seq % PARTITIONS, on_delivery=done)
                pending[v] = seq % PARTITIONS
                seq += 1
            except BufferError:
                p.poll(0.1)
                continue
            p.poll(0)
            time.sleep(rng.uniform(0.002, 0.008))
        p.flush(60)
        for v, part in pending.items():
            out.write(json.dumps({"v": v, "pid": pid, "seq": int(v.split(":")[1]), "p": part, "err": "unknown"}) + "\n")
        c["sent"] = seq
        out.close()

    threads = [threading.Thread(target=run, args=(f"p{i}",)) for i in range(int(producers))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    open(os.path.join(d, "done"), "w").close()
    log(f"writer finished: {counts}")


def writing(subject):
    """Print 1 once the writer finished, else 0: a check before the faults,
    because faults after the last write test nothing."""
    print(1 if os.path.exists(os.path.join(ledger_dir(subject), "done")) else 0)
