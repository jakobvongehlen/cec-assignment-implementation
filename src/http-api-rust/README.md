# HTTP API (Rust)

Port of the FastAPI version in [`../http-api`](../http-api) with the same endpoints, SQL and responses, including the 422 bodies. `compose.yml` builds it because `.env` sets `API_IMPL=rust`.

## Configuration

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:cec@mypg/temp_db` | URL or libpq `key=value` form, no TLS |
| `WORKERS` | number of cores | worker threads |
| `POOL_SIZE` | `10` | DB connections per worker |
| `DB_TIMEOUT` | `5` | seconds a request may spend on the database before it returns 500 |
| `CACHE_SIZE` | `8000` | cached experiments, `0` disables the cache |
| `CACHE_REFRESH` | `1` | seconds between cache refreshes |
| `PORT` | `3003` | |

## Running without Docker

```
cargo build --release
DATABASE_URL=postgresql://postgres:cec@localhost/temp_db ./target/release/http-api
```
