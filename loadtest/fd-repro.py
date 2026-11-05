#!/usr/bin/env python3
"""Checks that the API survives more client connections than it may open
files (see README.md, "the API hung"). Run on the API's machine with a raised
limit for itself: ulimit -n 8192; python3 fd-repro.py [connections]

Holds the connections open without sending anything, then sends a request that
needs a database connection (an unknown experiment is not in the cache), and
again after closing them."""
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
