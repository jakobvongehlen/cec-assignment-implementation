#!/usr/bin/env python3
import os
from contextlib import asynccontextmanager

import asyncpg
import uvicorn
from fastapi import FastAPI, Query, Response

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:cec@mypg/temp_db",
)
# connections per worker process; total = WORKERS * POOL_SIZE
POOL_SIZE = int(os.environ.get("POOL_SIZE", "10"))
WORKERS = int(os.environ.get("WORKERS", "2"))

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

pool = None


@asynccontextmanager
async def lifespan(_app):
    # created per worker process, after uvicorn has started it
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=POOL_SIZE, max_size=POOL_SIZE)
    yield
    await pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/temperature")
async def temperature(
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
):
    body = await pool.fetchval(TEMPERATURE_SQL, experiment_id, start_time, end_time)
    return Response(content=body, media_type="application/json")


@app.get("/temperature/out-of-range")
async def out_of_range(experiment_id: str = Query(alias="experiment-id")):
    body = await pool.fetchval(OUT_OF_RANGE_SQL, experiment_id)
    return Response(content=body, media_type="application/json")


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=3003, workers=WORKERS)
