# HTTP API (Rust)

A Rust port of the Python API in [`../http-api`](../http-api). It serves the same endpoints with the same SQL, the same cache of terminated experiments and byte-identical responses, including the 422 bodies for missing or invalid parameters. See the Python README for the endpoints and how the cache works.

## Choosing the implementation

`compose.yml` builds the Python API by default. To deploy this one instead, set `API_IMPL`:

```
API_IMPL=rust docker compose up -d --build api
```

Leave `API_IMPL` unset (or empty) to go back to Python. Each implementation gets its own image (`http-api` / `http-api-rust`), so switching recreates the container.

## Differences to the Python version

- One process with `WORKERS` threads instead of `WORKERS` processes. All threads share one cache and one connection pool of `WORKERS × POOL_SIZE` connections, so the cache is only filled once.
- No `/docs` or `/openapi.json`, and no access log.
- `DATABASE_URL` can be a URL or libpq's `key=value` form. TLS to Postgres is not supported.

## Configuration

| Variable       | Default                                   | What it does                                  |
|----------------|-------------------------------------------|-----------------------------------------------|
| `DATABASE_URL` | `postgresql://postgres:cec@mypg/temp_db`  | Postgres connection string                    |
| `WORKERS`      | number of cores                           | Worker threads                                |
| `POOL_SIZE`    | `10`                                      | DB connections per worker (total = workers × pool size) |
| `CACHE_SIZE`   | `8000`                                    | Cached experiments, `0` turns the cache off   |
| `CACHE_REFRESH`| `1`                                       | Seconds between checks for newly terminated experiments |
| `PORT`         | `3003`                                    | Port to listen on                             |

## Running it without Docker

```
cargo build --release
DATABASE_URL=postgresql://postgres:cec@localhost/temp_db ./target/release/http-api
```

## Performance

Measured locally with `oha` (64 connections, both versions with 2 workers, load generator on the same machine):

| Request                  | Python      | Rust         |
|--------------------------|-------------|--------------|
| cached experiment        | 8,455 req/s | 35,224 req/s |
| running experiment (DB)  | 2,693 req/s | 7,236 req/s  |
