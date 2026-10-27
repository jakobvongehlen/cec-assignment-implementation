"""Scales the `consumer` service of this compose project based on Prometheus metrics.

Every INTERVAL seconds it looks at:
- delay: Kafka lag divided by the consume rate, i.e. how many seconds behind
  the consumers are. Notifications have to arrive within 10 s.
- busy: the share of time an average consumer spends processing messages. A
  consumer is a single Python thread, so 1.0 means it is saturated.
- VM CPU: adding consumers doesn't help once the VM itself is saturated.

It adds a consumer by copying the configuration of a running one (same image,
command, environment, mounts and compose labels, so `docker compose` treats it
as one of its replicas) and removes one by stopping it, which lets the consumer
commit its offsets before its partitions move to the others.
"""
import json
import logging
import math
import os
import socket
import time
import urllib.parse
import urllib.request

import docker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("autoscaler")

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
SERVICE = os.environ.get("SERVICE", "consumer")
MIN_CONSUMERS = int(os.environ.get("MIN_CONSUMERS", "1"))
MAX_CONSUMERS = int(os.environ.get("MAX_CONSUMERS", "6"))
# seconds between decisions
INTERVAL = float(os.environ.get("INTERVAL", "5"))
# seconds to wait after a change before the next one: a new consumer needs a few
# seconds to start, join the group and get partitions; scaling down waits longer
UP_COOLDOWN = float(os.environ.get("UP_COOLDOWN", "20"))
DOWN_COOLDOWN = float(os.environ.get("DOWN_COOLDOWN", "60"))
# scale up when consumers are this many seconds behind, or this busy
UP_DELAY = float(os.environ.get("UP_DELAY", "2"))
# below saturation, so the existing consumers have headroom while partitions move
UP_BUSY = float(os.environ.get("UP_BUSY", "0.5"))
# this far behind, the 10 s notification limit is at risk: scale up right away
# and at least double the consumers
URGENT_DELAY = float(os.environ.get("URGENT_DELAY", "5"))
# busy share to aim for when scaling up
TARGET_BUSY = float(os.environ.get("TARGET_BUSY", "0.5"))
# scale down when one consumer less would still be below this, and nothing is behind
DOWN_DELAY = float(os.environ.get("DOWN_DELAY", "0.5"))
DOWN_BUSY = float(os.environ.get("DOWN_BUSY", "0.4"))
# consecutive checks needed, scaling down is slower than up
UP_CHECKS = int(os.environ.get("UP_CHECKS", "2"))
DOWN_CHECKS = int(os.environ.get("DOWN_CHECKS", "24"))
# no consumers are added above this VM CPU share
MAX_VM_CPU = float(os.environ.get("MAX_VM_CPU", "0.9"))

QUERIES = {
    "lag": "sum(consumer_lag_messages)",
    "rate": "sum(rate(consumer_messages_total[30s]))",
    "busy": "avg(rate(consumer_message_processing_seconds_sum[30s]))",
    "partitions": "max(consumer_topic_partitions)",
    "vm_cpu": "cec:node:vcpus_used / cec:node:vcpus",
}


def query(promql):
    url = f"{PROMETHEUS_URL}/api/v1/query?" + urllib.parse.urlencode({"query": promql})
    with urllib.request.urlopen(url, timeout=5) as resp:
        result = json.load(resp)["data"]["result"]
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if math.isnan(value) else value


class Replicas:
    """The containers of the consumer service in this compose project."""

    def __init__(self):
        self.client = docker.from_env()
        # our own container tells us the compose project we belong to
        me = self.client.containers.get(socket.gethostname())
        self.project = me.labels["com.docker.compose.project"]

    def list(self):
        containers = self.client.containers.list(filters={"label": [
            f"com.docker.compose.project={self.project}",
            f"com.docker.compose.service={SERVICE}",
        ]})
        return sorted(containers, key=lambda c: int(c.labels["com.docker.compose.container-number"]))

    def add(self, template):
        number = max(int(c.labels["com.docker.compose.container-number"]) for c in self.list()) + 1
        attrs = template.attrs
        config = attrs["Config"]
        labels = dict(config["Labels"])
        labels["com.docker.compose.container-number"] = str(number)
        volumes = [
            f"{m['Source'] if m['Type'] == 'bind' else m['Name']}:{m['Destination']}:{'rw' if m['RW'] else 'ro'}"
            for m in attrs["Mounts"]
        ]
        container = self.client.containers.run(
            image=config["Image"],
            command=config["Cmd"],
            entrypoint=config["Entrypoint"],
            environment=config["Env"],
            working_dir=config["WorkingDir"],
            labels=labels,
            volumes=volumes,
            network=next(iter(attrs["NetworkSettings"]["Networks"])),
            restart_policy=attrs["HostConfig"]["RestartPolicy"],
            name=f"{self.project}-{SERVICE}-{number}",
            detach=True,
        )
        logger.info("Started %s", container.name)

    def remove(self, container):
        # SIGTERM, the consumer commits and leaves the group before it exits
        container.stop(timeout=30)
        container.remove()
        logger.info("Removed %s", container.name)


def main():
    replicas = Replicas()
    logger.info("Scaling %s of project %s between %d and %d", SERVICE, replicas.project, MIN_CONSUMERS, MAX_CONSUMERS)
    up_checks = down_checks = checks = 0
    last_change = 0.0
    while True:
        time.sleep(INTERVAL)
        try:
            containers = replicas.list()
            n = len(containers)
            if n == 0:
                logger.warning("No running %s containers, nothing to copy", SERVICE)
                continue
            m = {name: query(q) for name, q in QUERIES.items()}
        except Exception as e:
            logger.warning("Skipping this check: %s", e)
            continue

        lag = m["lag"] or 0.0
        rate = m["rate"] or 0.0
        busy = m["busy"] or 0.0
        # seconds the consumers are behind; lag without progress counts as far behind
        delay = lag / rate if rate > 1 else (0.0 if lag < 10 else float("inf"))
        # more consumers than partitions would sit idle
        limit = min(MAX_CONSUMERS, int(m["partitions"])) if m["partitions"] else MAX_CONSUMERS
        vm_cpu = m["vm_cpu"]

        urgent = delay > URGENT_DELAY and n < limit
        up = (delay > UP_DELAY or busy > UP_BUSY) and n < limit
        down = n > MIN_CONSUMERS and delay < DOWN_DELAY and busy * n / (n - 1) < DOWN_BUSY
        up_checks = up_checks + 1 if up else 0
        down_checks = down_checks + 1 if down else 0
        # every check while something happens, otherwise once a minute
        checks += 1
        if up or down_checks or delay > DOWN_DELAY or checks % max(1, int(60 / INTERVAL)) == 0:
            logger.info(
                "consumers=%d/%d lag=%.0f rate=%.0f/s delay=%.1fs busy=%.2f vm_cpu=%s up=%d/%d down=%d/%d",
                n, limit, lag, rate, delay, busy, "?" if vm_cpu is None else f"{vm_cpu:.2f}",
                up_checks, UP_CHECKS, down_checks, DOWN_CHECKS,
            )

        since_change = time.monotonic() - last_change
        if (urgent or up_checks >= UP_CHECKS) and since_change >= UP_COOLDOWN:
            if vm_cpu is not None and vm_cpu > MAX_VM_CPU:
                logger.warning("VM CPU at %.0f%%, another consumer would not be faster", vm_cpu * 100)
                continue
            # enough consumers to bring the busy share down to the target, at least one more;
            # when far behind at least twice as many, the backlog has to be worked off as well
            target = max(n + 1, math.ceil(n * busy / TARGET_BUSY))
            if urgent:
                target = max(target, 2 * n)
            target = min(limit, target)
            logger.info("Scaling up from %d to %d consumers%s", n, target, " (urgent)" if urgent else "")
            for _ in range(target - n):
                replicas.add(containers[0])
        elif down_checks >= DOWN_CHECKS and since_change >= DOWN_COOLDOWN:
            logger.info("Scaling down from %d to %d consumers", n, n - 1)
            replicas.remove(containers[-1])
        else:
            continue
        up_checks = down_checks = 0
        last_change = time.monotonic()

if __name__ == "__main__":
    main()
