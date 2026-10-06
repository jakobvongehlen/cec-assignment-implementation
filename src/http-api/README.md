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
| `CACHE_SIZE`   | `8000`                                    | Cached experiments per worker, `0` turns the cache off |
| `CACHE_REFRESH`| `1`                                       | Seconds between checks for newly terminated experiments |

## Why it's fast

Postgres builds the JSON response itself (`json_agg`), so Python just passes the text through without parsing rows. Each worker also keeps its own asyncpg connection pool, so requests don't have to open a new database connection.

Once an experiment shows up in `experiments_terminated`, its data can't change anymore. Every worker runs a background task that checks that table every `CACHE_REFRESH` seconds and loads each newly terminated experiment into memory: the sorted timestamps, each row as a ready-made JSON fragment, and the finished out-of-range response. A request for a cached experiment is then answered with a binary search and a string join, without touching the database. Requests never fill the cache themselves; running experiments and anything not loaded yet go to the database. The cache keeps the `CACHE_SIZE` most recently terminated experiments (about 25 KB each) and drops the oldest when it's full.
