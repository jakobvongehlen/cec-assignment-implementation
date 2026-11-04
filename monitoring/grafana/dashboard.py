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
panel("stat", "Projected per month", [target("sum(cec:cost_usd_per_hour) * 730", instant=True)], 0, 6, 8,
      unit="currencyUSD", decimals=2, options=STAT,
      desc="Current billed cost per hour (instance, public IPv4, EBS, CPU credits and egress) x 730 hours")
panel("timeseries", "Cost over time per component", [target("cec:cost_usd_per_hour", "{{component}}")], 6, 18, 8,
      unit="currencyUSD", stack=True,
      desc="USD per hour. Only cpu_credits (CPU above the t3a baseline of 20% per vCPU, worst case) and egress depend on the load.")
y += 8

# --- load ---------------------------------------------------------------------
row("Load")
# by compose service, with consumer_1 ... consumer_16 added up as "consumers"
panel("timeseries", "CPU per container", [
        target('sum by (service) (label_replace(label_replace(rate(container_cpu_usage_seconds_total{job="cadvisor"}[1m]), '
               '"service", "$1", "container_label_com_docker_compose_service", "(.*)"), '
               '"service", "consumers", "service", "consumer_[0-9]+"))', "{{service}}"),
        target("cec:node:vcpus", "VM vCPUs", "B")], 0, 12, 8, unit="none", decimals=2,
      desc="vCPUs in use per compose service. 'consumers' adds up the 16 consumer containers. The VM has 2 vCPUs.")
panel("timeseries", "Kafka consumer lag", [target("sum(consumer_lag_messages)", "messages waiting")], 12, 12, 8,
      unit="short", decimals=0,
      desc="Messages in Kafka that the 16 consumers have not read yet. Near 0: they keep up. Growing: they fall behind, and notifications arrive later and later.")
y += 8

dash = {"title": "Temperature Observability", "uid": "temperature-observability", "editable": False,
        "refresh": "10s", "schemaVersion": 39, "tags": ["cec"], "time": {"from": "now-1h", "to": "now"},
        "panels": panels, "templating": {"list": []}, "annotations": {"list": []}}
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards", "temperature-observability.json")
with open(out, "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print(f"wrote {len(panels)} panels to {out}")
