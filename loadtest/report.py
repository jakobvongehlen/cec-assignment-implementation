#!/usr/bin/env python3
"""Summarizes a load test run from Prometheus, one row per stage, see README.md.

Usage: report.py <run-dir> [--prometheus URL] [--http-prometheus URL]

--http-prometheus is the Prometheus that scrapes the load generator, when it
runs somewhere else than the system under test (run-vm.sh).

Reads <run-dir>/plan.json and <run-dir>/t0 (start of the producer, epoch
seconds) and writes <run-dir>/report.md and <run-dir>/results.json. Each stage is
measured over its steady window, after the experiments of the previous stage
have finished.
"""
import argparse
import json
import math
import os
import sys
import urllib.parse
import urllib.request

# a stage passes if ...
SLA_SECONDS = 10.0          # every notification arrives within 10 s
MIN_RATE_SHARE = 0.95       # the API answers at least 95 % of the requested rate
MAX_HTTP_ERROR_SHARE = 0.01  # and at most 1 % of the responses are errors or wrong

SUT_GROUPS = {
    "consumers": r"consumer_.*",
    "api": r"api",
    "postgres": r"mypg",
    "monitoring": r"prometheus|cadvisor|grafana|node-exporter",
}
# not part of the system, but on the same machine when it runs on the VM
TOOLS = r"experiment-producer|notifications-service|http-load-generator"


class Prometheus:
    def __init__(self, url):
        self.url = url.rstrip("/")

    def query(self, expr, at):
        params = urllib.parse.urlencode({"query": expr, "time": f"{at:.3f}"})
        with urllib.request.urlopen(f"{self.url}/api/v1/query?{params}", timeout=30) as response:
            body = json.load(response)
        if body["status"] != "success":
            raise RuntimeError(f"{expr}: {body}")
        return body["data"]["result"]

    def scalar(self, expr, at):
        """First value of the result, None if there is none (no data) or it is NaN."""
        result = self.query(expr, at)
        if not result:
            return None
        value = float(result[0]["value"][1])
        return None if math.isnan(value) else value

    def range(self, expr, start, end, step=5):
        params = urllib.parse.urlencode({"query": expr, "start": f"{start:.3f}", "end": f"{end:.3f}", "step": step})
        with urllib.request.urlopen(f"{self.url}/api/v1/query_range?{params}", timeout=30) as response:
            body = json.load(response)
        return body["data"]["result"]

    def by_label(self, expr, label, at):
        return {r["metric"].get(label, ""): float(r["value"][1]) for r in self.query(expr, at)}


def http_window(prom, plan, stage, t0):
    """When the load generator really sent the stage's rate.

    Its stages drift from the plan (they start with the first finished
    experiment, and a stage the API can't keep up with takes longer), so the
    window comes from its target rate gauge, which it sets for every batch it
    sends. Falls back to the plan if the rate is not unique to the stage."""
    planned = (t0 + stage["steady_start"], t0 + stage["end"])
    if [s["rate"] for s in plan["stages"]].count(stage["rate"]) != 1:
        return planned
    series = prom.range("max(http_load_generator_target_request_rate)", t0, t0 + plan["duration"] + 600)
    times = [float(t) for t, v in (series[0]["values"] if series else []) if float(v) == stage["rate"]]
    if len(times) < 4:
        return planned
    # the first scrape may still count responses of the previous stage
    return times[0] + 10, times[-1]


def measure_stage(prom, http_prom, plan, stage, t0):
    start = t0 + stage["steady_start"]
    end = t0 + stage["end"]
    w = int(end - start)
    r = f"[{w}s]"
    q = lambda expr: prom.scalar(expr, end)
    http_start, http_end = http_window(http_prom, plan, stage, t0)
    hw = max(int(http_end - http_start), 5)
    hr = f"[{hw}s]"
    hq = lambda expr: http_prom.scalar(expr, http_end)

    lat = lambda quantile: q(
        f"histogram_quantile({quantile}, sum by (le) (increase(notification_latency_seconds_bucket{r})))")
    notified = q(f"sum(increase(notification_latency_seconds_count{r}))") or 0.0
    in_sla = q(f'sum(increase(notification_latency_seconds_bucket{{le=~"10(\\\\.0)?"}}{r}))') or 0.0
    rtt = lambda quantile: hq(
        f'histogram_quantile({quantile}, sum by (le) (increase(http_load_generator_response_rtt_histogram_bucket{hr})))')
    responses = http_prom.by_label(
        f"sum by (response_type) (increase(http_load_generator_response_count_total{hr}))", "response_type", http_end)
    total = sum(responses.values())
    ok = responses.get("Ok", 0.0)
    cpu = {
        group: q(f'sum(rate(container_cpu_usage_seconds_total{{container_label_com_docker_compose_service=~"{regex}"}}{r}))')
        for group, regex in SUT_GROUPS.items()
    }
    tools_cpu = q(f'sum(rate(container_cpu_usage_seconds_total{{container_label_com_docker_compose_service=~"{TOOLS}"}}{r}))')
    # the whole machine (node exporter): share of CPU time busy, and stolen by the
    # hypervisor (a t3a out of CPU credits is throttled through steal)
    busy = q(f'1 - avg(rate(node_cpu_seconds_total{{mode="idle"}}{r}))')
    steal = q(f'avg(rate(node_cpu_seconds_total{{mode="steal"}}{r}))')
    memory = q('sum(max_over_time(container_memory_working_set_bytes{container_label_com_docker_compose_service=~"'
               + "|".join(SUT_GROUPS.values()) + f'"}}{r}))')

    m = {
        "stage": stage["stage"],
        "experiments": stage["experiments"],
        "target_rate": stage["rate"],
        "window_seconds": w,
        # consumers
        "events_per_s": (q(f"sum(increase(consumer_messages_total{r}))") or 0.0) / w,
        "lag_max": q(f"max_over_time(sum(consumer_lag_messages)[{w}s:5s])"),
        "processing_p99_ms": (q(f"histogram_quantile(0.99, sum by (le) (increase(consumer_message_processing_seconds_bucket{r})))") or 0) * 1000,
        "notifications": notified,
        "notification_errors": q(f'sum(increase(notifications_total{{outcome="error"}}{r}))') or 0.0,
        "latency_p50": lat(0.5),
        "latency_p99": lat(0.99),
        "late": max(notified - in_sla, 0.0),
        # API
        "http_window_seconds": hw,
        "http_window_offset": http_start - t0,
        "achieved_rate": total / hw,
        "http_errors": {k: v for k, v in responses.items() if k != "Ok" and v > 0},
        "http_error_share": (total - ok) / total if total else None,
        "rtt_p50_ms": (rtt(0.5) or 0) * 1000,
        "rtt_p99_ms": (rtt(0.99) or 0) * 1000,
        # resources of the system under test, in cores
        "cpu": cpu,
        "cpu_total": sum(v for v in cpu.values() if v is not None),
        "cpu_tools": tools_cpu,
        "machine_busy": busy,
        "machine_steal": steal,
        "memory_mb": memory / 2**20 if memory is not None else None,
    }
    # round(x) with x < 0.5 errors would hide single wrong notifications
    m["consumer_ok"] = (
        notified > 0
        and m["late"] < 0.5
        and m["notification_errors"] < 0.5
        and m["latency_p99"] is not None and m["latency_p99"] < SLA_SECONDS
    )
    m["api_ok"] = (
        total > 0
        and m["achieved_rate"] >= MIN_RATE_SHARE * stage["rate"]
        and m["http_error_share"] <= MAX_HTTP_ERROR_SHARE
    )
    return m


def fmt(value, digits=0, unit=""):
    if value is None:
        return "–"
    return f"{value:,.{digits}f}{unit}"


def render(plan, results, run_dir):
    lines = [
        f"# Load test {os.path.basename(os.path.abspath(run_dir))}",
        "",
        f"{len(plan['stages'])} stages of {plan['stage_seconds']} s, experiments of "
        f"{plan['experiment_seconds']} s with {plan['sensors']} sensors at 1 sample/s, "
        f"{plan['experiments_total']} experiments in total. Each stage is measured over its steady "
        "window, after the previous stage's experiments have ended.",
        "",
        "## Consumers (Kafka → Postgres → notifications)",
        "",
        "| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for m in results:
        expected = m["experiments"] * plan["sensors"]
        lines.append(
            f"| {m['stage']} | {m['experiments']} | {fmt(m['events_per_s'])} ({expected}) | {fmt(m['lag_max'])} "
            f"| {fmt(m['processing_p99_ms'], 1, ' ms')} | {fmt(m['notifications'])} | {fmt(m['latency_p50'], 2, ' s')} "
            f"| {fmt(m['latency_p99'], 2, ' s')} | {fmt(m['late'])} | {fmt(m['notification_errors'])} "
            f"| {'✅' if m['consumer_ok'] else '❌'} |")
    lines += [
        "",
        "## HTTP API (finished experiments only)",
        "",
        "| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for m in results:
        errors = ", ".join(f"{k}: {v:,.0f}" for k, v in m["http_errors"].items()) or "–"
        share = fmt(m["http_error_share"] * 100, 2, " %") if m["http_error_share"] is not None else "–"
        lines.append(
            f"| {m['stage']} | {m['http_window_offset']:.0f}–{m['http_window_offset'] + m['http_window_seconds']:.0f} "
            f"| {m['target_rate']} | {fmt(m['achieved_rate'])} | {fmt(m['rtt_p50_ms'], 1, ' ms')} "
            f"| {fmt(m['rtt_p99_ms'], 1, ' ms')} | {share} | {errors} | {'✅' if m['api_ok'] else '❌'} |")
    lines += [
        "",
        "## CPU of the system under test (cores)",
        "",
        "Load tools: producer, notifications service and load generator, where they run on the same machine. "
        "Machine busy and steal: all CPUs of the machine (node exporter).",
        "",
        "| Stage | " + " | ".join(SUT_GROUPS) + " | Total | Load tools | Machine busy | Steal | Memory |",
        "|---|" + "---|" * (len(SUT_GROUPS) + 5),
    ]
    for m in results:
        lines.append(
            f"| {m['stage']} | " + " | ".join(fmt(m["cpu"][g], 2) for g in SUT_GROUPS)
            + f" | {fmt(m['cpu_total'], 2)} | {fmt(m['cpu_tools'], 2)} "
            f"| {fmt(m['machine_busy'] * 100 if m['machine_busy'] is not None else None, 0, ' %')} "
            f"| {fmt(m['machine_steal'] * 100 if m['machine_steal'] is not None else None, 1, ' %')} "
            f"| {fmt(m['memory_mb'], 0, ' MB')} |")

    passed = [m for m in results if m["consumer_ok"] and m["api_ok"]]
    first_fail = next((m for m in results if not (m["consumer_ok"] and m["api_ok"])), None)
    lines += ["", "## Result", ""]
    if first_fail is None:
        lines.append(f"All stages passed, up to {results[-1]['experiments']} experiments and "
                     f"{results[-1]['target_rate']} req/s. The limit is above this load.")
    else:
        last = passed[-1] if passed and passed[-1]["stage"] < first_fail["stage"] else None
        broke = [name for name, ok in (("consumers", first_fail["consumer_ok"]), ("API", first_fail["api_ok"])) if not ok]
        lines.append(
            (f"Last stage that passed: {last['stage']} ({last['experiments']} experiments, {last['target_rate']} req/s). "
             if last else "No stage passed. ")
            + f"Stage {first_fail['stage']} ({first_fail['experiments']} experiments, {first_fail['target_rate']} req/s) "
            f"failed on the {' and '.join(broke)}.")
    for m in results:
        expected = m["experiments"] * plan["sensors"]
        if m["events_per_s"] < 0.8 * expected and (m["lag_max"] or 0) < expected:
            lines.append(
                f"\n⚠️ Stage {m['stage']}: the consumers got {m['events_per_s']:,.0f} of {expected} events/s without "
                "falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). "
                "The stage says nothing about the consumers.")
    lines += ["", f"Pass criteria: no notification later than {SLA_SECONDS:.0f} s and none failed; the API answers "
              f"≥ {MIN_RATE_SHARE:.0%} of the target rate with ≤ {MAX_HTTP_ERROR_SHARE:.0%} errors or wrong answers."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir")
    parser.add_argument("--prometheus", default=os.environ.get("PROMETHEUS_URL", "http://localhost:3008"))
    parser.add_argument("--http-prometheus", help="Prometheus with the load generator's metrics (default: --prometheus)")
    args = parser.parse_args()

    with open(os.path.join(args.run_dir, "plan.json")) as f:
        plan = json.load(f)
    with open(os.path.join(args.run_dir, "t0")) as f:
        t0 = float(f.read())

    prom = Prometheus(args.prometheus)
    http_prom = Prometheus(args.http_prometheus) if args.http_prometheus else prom
    results = [measure_stage(prom, http_prom, plan, stage, t0) for stage in plan["stages"]]
    with open(os.path.join(args.run_dir, "results.json"), "w") as f:
        json.dump({"t0": t0, "plan": plan, "stages": results}, f, indent=2)
    report = render(plan, results, args.run_dir)
    with open(os.path.join(args.run_dir, "report.md"), "w") as f:
        f.write(report)
    sys.stdout.write(report)


if __name__ == "__main__":
    main()
