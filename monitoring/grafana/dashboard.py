"""Generates dashboards/temperature-observability.json.

Edit the panels here and run `python3 monitoring/grafana/dashboard.py`, then
restart grafana (`docker compose restart grafana`).
"""
import json
import os

DS = {"type": "prometheus", "uid": "prometheus"}
panels, y = [], 0
pid = 0

def nid():
    global pid
    pid += 1
    return pid

def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
    y += 1

def target(expr, legend="", ref="A", instant=False):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}
    if instant:
        t["instant"] = True
        t["range"] = False
    return t

def panel(kind, title, targets, x, w, h, unit=None, desc=None, stack=False, thresholds=None, decimals=None, min0=True, options=None):
    fc = {"defaults": {"custom": {}}, "overrides": []}
    d = fc["defaults"]
    if unit: d["unit"] = unit
    if decimals is not None: d["decimals"] = decimals
    if kind == "timeseries":
        d["custom"] = {"fillOpacity": 30 if stack else 10, "lineWidth": 1, "showPoints": "never",
                       "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
        if min0: d["min"] = 0
    if thresholds:
        d["thresholds"] = {"mode": "absolute", "steps": [{"color": "green", "value": None}] +
                           [{"color": c, "value": v} for v, c in thresholds]}
        if kind == "timeseries":
            d["custom"]["thresholdsStyle"] = {"mode": "line+area"}
    p = {"type": kind, "title": title, "id": nid(), "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets, "fieldConfig": fc}
    if desc: p["description"] = desc
    if kind == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}}
    if options: p["options"] = options
    panels.append(p)

STAT = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "colorMode": "value", "graphMode": "none", "textMode": "auto"}

# --- cost -------------------------------------------------------------------
row("Estimated cost (AWS t3a.medium, eu-west-1)")
panel("stat", "Cost right now", [target("sum(cec:cost_usd_per_hour)", instant=True)], 0, 6, 4,
      unit="currencyUSD", decimals=4, options=STAT, desc="Billed cost per hour: instance, public IPv4, EBS, CPU credits and egress")
panel("stat", "Cost in the selected time range", [target("sum(avg_over_time(cec:cost_usd_per_hour[$__range])) * $__range_s / 3600", instant=True)], 6, 6, 4,
      unit="currencyUSD", decimals=4, options=STAT)
panel("stat", "Projected per month", [target("sum(cec:cost_usd_per_hour) * 730", instant=True)], 12, 6, 4,
      unit="currencyUSD", decimals=2, options=STAT, desc="Current hourly cost x 730 hours")
panel("stat", "CPU credit surcharge in range", [target("sum(avg_over_time(cec:cost_usd_per_hour{component=\"cpu_credits\"}[$__range])) * $__range_s / 3600", instant=True)], 18, 6, 4,
      unit="currencyUSD", decimals=4, options=STAT,
      desc="Extra cost when the VM runs above the t3a baseline of 20% per vCPU (unlimited mode, worst case: ignores earned credits)")
y += 4
panel("timeseries", "Billed cost per component", [target("cec:cost_usd_per_hour", "{{component}}")], 0, 12, 8,
      unit="currencyUSD", stack=True, desc="USD per hour. Only cpu_credits and egress depend on the load.")
panel("timeseries", "Showback: instance cost per container", [
        target("cec:container:cost_usd_per_hour", "{{service}}"),
        target("cec:unused_cost_usd_per_hour", "unused / OS", "B")], 12, 12, 8,
      unit="currencyUSD", stack=True,
      desc="The instance price split over the containers by CPU and memory use (Fargate vCPU:GiB price ratio). 'unused' is idle capacity plus the OS and dockerd.")
y += 8
panel("bargauge", "Cost per container in the selected time range",
      [target("sort_desc(sum by (service) (avg_over_time(cec:container:cost_usd_per_hour[$__range])) * $__range_s / 3600)", "{{service}}", instant=True)],
      0, 24, 6, unit="currencyUSD", decimals=5,
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}})
y += 6

# --- bottlenecks ------------------------------------------------------------
row("Where is the bottleneck?")
panel("timeseries", "CPU per container", [
        target("cec:container:vcpus_used", "{{service}}"),
        target("cec:node:vcpus", "VM vCPUs", "B")], 0, 12, 8, unit="none", decimals=2,
      desc="vCPUs in use. A container at 1.0 that runs a single-threaded process (a Python consumer) is saturated.")
panel("timeseries", "Memory per container", [
        target("cec:container:memory_gib", "{{service}}"),
        target("cec:node:memory_gib", "VM memory", "B")], 12, 12, 8, unit="gbytes", decimals=2)
y += 8
panel("timeseries", "Kafka consumer lag", [target("sum by (instance) (consumer_lag_messages)", "{{instance}}")], 0, 8, 8,
      unit="short", desc="Messages waiting per consumer. Growing lag means the consumers cannot keep up, and notification latency grows with it.")
panel("timeseries", "Messages consumed", [target("sum by (instance) (rate(consumer_messages_total[1m]))", "{{instance}}")], 8, 8, 8,
      unit="reqps", stack=True)
panel("timeseries", "Consumer time per message (p95)", [
        target("histogram_quantile(0.95, sum by (le) (rate(consumer_message_processing_seconds_bucket[1m])))", "decode + process"),
        target("histogram_quantile(0.95, sum by (le) (rate(consumer_db_write_seconds_bucket[1m])))", "DB insert", "B")], 16, 8, 8,
      unit="s", desc="If the DB insert is most of the time, Postgres or the round trip to it is the bottleneck.")
y += 8
panel("timeseries", "VM CPU and memory", [
        target("cec:node:vcpus_used / cec:node:vcpus", "CPU"),
        target("cec:node:memory_used_gib / cec:node:memory_gib", "memory", "B"),
        target("cec:instance:baseline_vcpu_fraction", "t3a CPU baseline", "C")], 0, 12, 8, unit="percentunit")
panel("timeseries", "VM network", [
        target("cec:node:egress_bytes_per_second", "out"),
        target("sum(rate(node_network_receive_bytes_total{job=\"node\", device!~\"lo|veth.*|docker.*|br-.*\"}[1m]))", "in", "B")], 12, 12, 8, unit="Bps")
y += 8

# --- scaling ------------------------------------------------------------------
row("Scaling (autoscaler)")
panel("timeseries", "Consumers", [
        target("count(up{job=\"consumer\"} == 1)", "consumers"),
        target("max(consumer_topic_partitions)", "partitions (most useful consumers)", "B")], 0, 8, 8,
      unit="short", decimals=0, desc="Running consumer containers. More consumers than partitions would sit idle.")
panel("timeseries", "Busy share per consumer", [target("rate(consumer_message_processing_seconds_sum[1m])", "{{instance}}")], 8, 8, 8,
      unit="percentunit", thresholds=[(0.5, "orange")],
      desc="Share of time spent processing messages. A consumer is one Python thread, so 100% is saturated. The autoscaler adds consumers above 50%.")
panel("timeseries", "Skipped messages", [target("sum by (reason) (rate(consumer_skipped_messages_total[1m]))", "{{reason}}")], 16, 8, 8,
      unit="reqps", desc="already_processed: read again after a partition moved to another consumer, has no effect. unknown_experiment: configured before any consumer was running. incomplete_measurement: dropped after 30 s.")
y += 8

# --- notifications ------------------------------------------------------------
row("Notifications (SLA: < 10 s)")
panel("timeseries", "Notification latency", [
        target("histogram_quantile(0.5, sum by (le) (rate(notification_latency_seconds_bucket[1m])))", "p50"),
        target("histogram_quantile(0.95, sum by (le) (rate(notification_latency_seconds_bucket[1m])))", "p95", "B"),
        target("histogram_quantile(0.99, sum by (le) (rate(notification_latency_seconds_bucket[1m])))", "p99", "C")], 0, 12, 8,
      unit="s", thresholds=[(10, "red")], desc="Latency reported by the notifications service: measurement produced -> notification received")
panel("timeseries", "Notifications sent", [target("sum by (notification_type, outcome) (rate(notifications_total[1m]))", "{{notification_type}} {{outcome}}")], 12, 6, 8,
      unit="reqps")
panel("timeseries", "Notifications waiting in the thread pool", [target("sum by (instance) (notifications_pending)", "{{instance}}")], 18, 6, 8,
      unit="short", desc="Queued or in flight. Growing means the notifier pool or the notifications service is the bottleneck.")
y += 8
panel("stat", "Notifications over 10 s in range", [target(
        "sum(increase(notification_latency_seconds_count[$__range])) - sum(increase(notification_latency_seconds_bucket{le=\"10.0\"}[$__range]))", instant=True)],
      0, 6, 4, unit="short", decimals=0, thresholds=[(1, "red")], options=STAT)
panel("stat", "Failed notifications in range", [target("sum(increase(notifications_total{outcome=\"error\"}[$__range])) or vector(0)", instant=True)],
      6, 6, 4, unit="short", decimals=0, thresholds=[(1, "red")], options=STAT)
panel("stat", "Request time to notifications service (p99)", [target(
        "histogram_quantile(0.99, sum by (le) (rate(notification_request_duration_seconds_bucket[$__range])))", instant=True)],
      12, 6, 4, unit="s", options=STAT)
panel("stat", "Active experiments", [target("sum(consumer_active_experiments)", instant=True)], 18, 6, 4, unit="short", options=STAT)

dash = {"title": "Temperature Observability", "uid": "temperature-observability", "editable": False,
        "refresh": "10s", "schemaVersion": 39, "tags": ["cec"], "time": {"from": "now-1h", "to": "now"},
        "panels": panels, "templating": {"list": []}, "annotations": {"list": []}}
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards", "temperature-observability.json")
with open(out, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print(f"wrote {len(panels)} panels to {out}")
