import os
import sys
import time

# The cluster under test: AutoMQ by default; Apache Kafka for the differential.
BOOTSTRAP = os.environ.get("BOOTSTRAP", "server1:9092,server2:9092,server3:9092")
TOPIC = "events"
PARTITIONS = 3
LEDGER = "/ledger"


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def ledger_dir(subject):
    d = os.path.join(LEDGER, subject)
    os.makedirs(d, exist_ok=True)
    return d


def wait(what, ok, timeout, every=1.0):
    deadline = time.time() + timeout
    while True:
        try:
            if ok():
                return
        except Exception as e:  # a broker that is starting refuses connections
            log(f"{what}: {e}")
        if time.time() > deadline:
            raise SystemExit(f"timed out after {timeout} s waiting for {what}")
        time.sleep(every)
