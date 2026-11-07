#!/usr/bin/env python3
"""Checks that the API still answers while more clients are connected than it may
open files: holds idle connections, then sends a request that needs the database.
Run on the API's machine: ulimit -n 8192; python3 fd-repro.py [connections]"""
import socket, sys, time, urllib.request

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1100
URL = "http://localhost:3003/temperature/out-of-range?experiment-id=fd-repro"

def probe(label):
    start = time.time()
    try:
        with urllib.request.urlopen(URL, timeout=15) as r:
            print(f"{label}: {r.status} after {time.time() - start:.2f} s")
    except Exception as e:
        print(f"{label}: failed after {time.time() - start:.2f} s ({e})")

held = []
for _ in range(N):
    s = socket.socket()
    s.settimeout(2)
    try:
        s.connect(("localhost", 3003))
        held.append(s)
    except OSError:
        s.close()
time.sleep(3)  # the API accepts them
print(f"holding {len(held)} idle connections")
probe("request while held")
for s in held:
    s.close()
time.sleep(3)
probe("request after closing them")
