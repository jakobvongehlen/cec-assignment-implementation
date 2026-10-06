# HTTP API

A small FastAPI service for reading measurements back out of the `measurements` table that the consumer fills. It listens on port `3003`.

## Endpoints

**`GET /temperature`** returns every measurement of an experiment between two timestamps, sorted by time.

```
curl "localhost:3003/temperature?experiment-id=<id>&start-time=<unix ts>&end-time=<unix ts>"
```

**`GET /temperature/out-of-range`** returns every measurement of an experiment that was flagged as out of range.

```
curl "localhost:3003/temperature/out-of-range?experiment-id=<id>"
```

Both endpoints return a JSON list like this one, or `[]` if nothing matches:

```json
[{"timestamp": 1727512345.0, "temperature": 21.4}]
```

## Running it

The simplest option is the compose file in the repo root, which also starts the database and the consumer:

```
docker compose up -d --build
```

To run only the API, point it at a reachable Postgres:

```
docker build -t http-api .
docker run -d --network cec-net -p 3003:3003 http-api
```

## Configuration

| Variable       | Default                                   | What it does                                  |
|----------------|-------------------------------------------|-----------------------------------------------|
| `DATABASE_URL` | `postgresql://postgres:cec@mypg/temp_db`  | Postgres connection string                    |
| `WORKERS`      | `2`                                       | Number of uvicorn worker processes            |
| `POOL_SIZE`    | `10`                                      | DB connections per worker (total = workers × pool size) |
| `CACHE_SIZE`   | `10000`                                   | Cached responses per worker, `0` turns the cache off |

## Why it's fast

Postgres builds the JSON response itself (`json_agg`), so Python just passes the text through without parsing rows. Each worker also keeps its own asyncpg connection pool, so requests don't have to open a new database connection.

Once an experiment shows up in `experiments_terminated`, its data can't change anymore, so the worker keeps the response in memory and answers repeat requests without touching the database. Running experiments are never cached. The termination check runs in the same SQL statement as the data query, so a cached response is always complete.
