#!/usr/bin/env python3
import asyncio
import logging
import os
from array import array
from bisect import bisect_left, bisect_right
from contextlib import asynccontextmanager, suppress

import asyncpg
import uvicorn
from fastapi import FastAPI, Query, Response

logger = logging.getLogger("uvicorn.error")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:cec@mypg/temp_db",
)
# connections per worker process; total = WORKERS * POOL_SIZE
POOL_SIZE = int(os.environ.get("POOL_SIZE", "10"))
WORKERS = int(os.environ.get("WORKERS", "2"))
# cached experiments per worker process, the most recently terminated ones are kept
CACHE_SIZE = int(os.environ.get("CACHE_SIZE", "8000"))
# seconds between checks for newly terminated experiments
CACHE_REFRESH = float(os.environ.get("CACHE_REFRESH", "1"))
# experiments loaded per query
CACHE_LOAD_BATCH = 100

# Postgres builds the JSON array, so Python only passes the text through
TEMPERATURE_SQL = (
    "SELECT COALESCE(json_agg(json_build_object('timestamp', timestamp, 'temperature', temperature) "
    "ORDER BY timestamp), '[]')::text FROM measurements "
    "WHERE experiment_id = $1 AND timestamp >= $2 AND timestamp <= $3"
)
OUT_OF_RANGE_SQL = (
    "SELECT COALESCE(json_agg(json_build_object('timestamp', timestamp, 'temperature', temperature) "
    "ORDER BY timestamp), '[]')::text FROM measurements "
    "WHERE experiment_id = $1 AND out_of_range = TRUE"
)
TERMINATED_SQL = (
    "SELECT experiment_id FROM experiments_terminated ORDER BY terminated_at DESC LIMIT $1"
)
# every measurement of the given experiments, as JSON fragments, to fill the cache
EXPERIMENTS_SQL = (
    "SELECT experiment_id, timestamp, out_of_range, "
    "json_build_object('timestamp', timestamp, 'temperature', temperature)::text "
    "FROM measurements WHERE experiment_id = ANY($1) ORDER BY experiment_id, timestamp"
)

pool = None
# Terminated experiments never receive new measurements, so all of their rows
# can be kept in memory and any time range is answered without the database.
# A background task loads them as they terminate; everything else (running
# experiments, ones not loaded yet, old evicted ones) goes to the database.
# experiment_id -> (sorted timestamps, all rows as one comma separated JSON
# string, where each row starts in that string, out-of-range body). Flat arrays
# and one bytes object per experiment keep the memory per row small.
cache = {}


async def load_experiments(experiment_ids):
    rows = await pool.fetch(EXPERIMENTS_SQL, experiment_ids)
    loaded = {experiment_id: ([], [], []) for experiment_id in experiment_ids}
    for experiment_id, timestamp, out_of_range, fragment in rows:
        timestamps, fragments, out_of_range_fragments = loaded[experiment_id]
        timestamps.append(timestamp)
        fragments.append(fragment)
        if out_of_range:
            out_of_range_fragments.append(fragment)
    for experiment_id in experiment_ids:
        timestamps, fragments, out_of_range_fragments = loaded[experiment_id]
        rows_json = ",".join(fragments).encode()
        # one extra start past the end, as if every row was followed by a comma
        starts = array("q", [0])
        for fragment in fragments:
            starts.append(starts[-1] + len(fragment) + 1)
        out_of_range = ("[" + ",".join(out_of_range_fragments) + "]").encode()
        cache[experiment_id] = (array("d", timestamps), rows_json, starts, out_of_range)


async def refresh_cache():
    # The cache always holds the CACHE_SIZE most recently terminated experiments.
    # Comparing the whole list doesn't depend on rows arriving in timestamp order.
    while True:
        try:
            newest = {row[0] for row in await pool.fetch(TERMINATED_SQL, CACHE_SIZE)}
            for experiment_id in cache.keys() - newest:
                del cache[experiment_id]
            missing = list(newest - cache.keys())
            # in chunks, so filling an empty cache at startup doesn't fetch everything at once
            for i in range(0, len(missing), CACHE_LOAD_BATCH):
                await load_experiments(missing[i : i + CACHE_LOAD_BATCH])
        except Exception:
            logger.exception("cache refresh failed")
        await asyncio.sleep(CACHE_REFRESH)


@asynccontextmanager
async def lifespan(_app):
    # created per worker process, after uvicorn has started it
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=POOL_SIZE, max_size=POOL_SIZE)
    refresher = asyncio.create_task(refresh_cache())
    yield
    refresher.cancel()
    with suppress(asyncio.CancelledError):
        await refresher
    await pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/temperature")
async def temperature(
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
):
    entry = cache.get(experiment_id)
    if entry is None:
        body = await pool.fetchval(TEMPERATURE_SQL, experiment_id, start_time, end_time)
    else:
        timestamps, rows_json, starts, _ = entry
        # inclusive on both ends, like the SQL query
        lo = bisect_left(timestamps, start_time)
        hi = bisect_right(timestamps, end_time)
        body = b"[" + rows_json[starts[lo] : starts[hi] - 1] + b"]" if lo < hi else b"[]"
    return Response(content=body, media_type="application/json")


@app.get("/temperature/out-of-range")
async def out_of_range(experiment_id: str = Query(alias="experiment-id")):
    entry = cache.get(experiment_id)
    if entry is None:
        body = await pool.fetchval(OUT_OF_RANGE_SQL, experiment_id)
    else:
        body = entry[3]
    return Response(content=body, media_type="application/json")


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=3003, workers=WORKERS)
