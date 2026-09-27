#!/usr/bin/env python3
import os

import psycopg2
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "dbname=temp_db user=postgres password=cec host=mypg",
)

app = FastAPI()


def connect():
    return psycopg2.connect(DATABASE_URL)


def get_experiment(cur, experiment_id):
    # TODO: experiment config is saved by the consumer, rename table/columns here to match
    cur.execute(
        "SELECT upper_threshold, lower_threshold, started_at "
        "FROM experiments WHERE experiment_id = %s",
        (experiment_id,),
    )
    return cur.fetchone()


@app.get("/temperature")
def temperature(
    experiment_id: str = Query(alias="experiment-id"),
    start_time: float = Query(alias="start-time"),
    end_time: float = Query(alias="end-time"),
):
    conn = connect()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        experiment = get_experiment(cur, experiment_id)
        if experiment is None:
            raise HTTPException(404, "unknown experiment {}".format(experiment_id))

        # measurements only exist for the carry-out phase, so start from started_at
        cur.execute(
            "SELECT timestamp, temperature FROM measurements "
            "WHERE experiment_id = %s AND timestamp >= %s AND timestamp <= %s "
            "ORDER BY timestamp",
            (experiment_id, max(start_time, experiment["started_at"]), end_time),
        )
        return cur.fetchall()
    finally:
        conn.close()


@app.get("/temperature/out-of-range")
def out_of_range(experiment_id: str = Query(alias="experiment-id")):
    conn = connect()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        experiment = get_experiment(cur, experiment_id)
        if experiment is None:
            raise HTTPException(404, "unknown experiment {}".format(experiment_id))

        cur.execute(
            "SELECT timestamp, temperature FROM measurements "
            "WHERE experiment_id = %s AND (temperature > %s OR temperature < %s) "
            "ORDER BY timestamp",
            (experiment_id, experiment["upper_threshold"], experiment["lower_threshold"]),
        )
        return cur.fetchall()
    finally:
        conn.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3003)
