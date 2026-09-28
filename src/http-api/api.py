#!/usr/bin/env python3
import os

import psycopg2
import uvicorn
from fastapi import FastAPI, Query
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "dbname=temp_db user=postgres password=cec host=mypg",
)

app = FastAPI()


def connect():
    return psycopg2.connect(DATABASE_URL)


@app.get("/temperature")
def temperature(
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
):
    conn = connect()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT timestamp, temperature FROM measurements "
            "WHERE experiment_id = %s AND timestamp >= %s AND timestamp <= %s "
            "ORDER BY timestamp",
            (experiment_id, start_time, end_time),
        )
        return cur.fetchall()
    finally:
        conn.close()


@app.get("/temperature/out-of-range")
def out_of_range(experiment_id: str = Query(alias="experiment-id")):
    conn = connect()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT timestamp, temperature FROM measurements "
            "WHERE experiment_id = %s AND out_of_range = TRUE "
            "ORDER BY timestamp",
            (experiment_id,),
        )
        return cur.fetchall()
    finally:
        conn.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3003)
