#!/usr/bin/env python3
import os
import threading
from contextlib import asynccontextmanager, contextmanager

import uvicorn
from fastapi import FastAPI, Query, Response
from psycopg2.pool import ThreadedConnectionPool

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "dbname=temp_db user=postgres password=cec host=mypg",
)
# connections per worker process; total = WORKERS * POOL_SIZE
POOL_SIZE = int(os.environ.get("POOL_SIZE", "10"))
WORKERS = int(os.environ.get("WORKERS", "2"))

pool = None
# ThreadedConnectionPool raises instead of blocking when exhausted,
# so request threads wait here for a free connection
pool_slots = threading.BoundedSemaphore(POOL_SIZE)


@asynccontextmanager
async def lifespan(_app):
    # created per worker process, after uvicorn has started it
    global pool
    pool = ThreadedConnectionPool(1, POOL_SIZE, DATABASE_URL)
    yield
    pool.closeall()


app = FastAPI(lifespan=lifespan)


@contextmanager
def connection():
    with pool_slots:
        conn = pool.getconn()
        try:
            yield conn
        finally:
            conn.rollback()
            pool.putconn(conn)


def query_json(sql, params):
    # Postgres builds the JSON array, so Python only passes the text through
    with connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return Response(content=cur.fetchone()[0], media_type="application/json")


@app.get("/temperature")
def temperature(
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
):
    return query_json(
        "SELECT COALESCE(json_agg(json_build_object('timestamp', timestamp, 'temperature', temperature) "
        "ORDER BY timestamp), '[]')::text FROM measurements "
        "WHERE experiment_id = %s AND timestamp >= %s AND timestamp <= %s",
        (experiment_id, start_time, end_time),
    )


@app.get("/temperature/out-of-range")
def out_of_range(experiment_id: str = Query(alias="experiment-id")):
    return query_json(
        "SELECT COALESCE(json_agg(json_build_object('timestamp', timestamp, 'temperature', temperature) "
        "ORDER BY timestamp), '[]')::text FROM measurements "
        "WHERE experiment_id = %s AND out_of_range = TRUE",
        (experiment_id,),
    )


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=3003, workers=WORKERS)
