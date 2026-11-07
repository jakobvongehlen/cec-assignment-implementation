# HTTP API (FastAPI)

Our first version of the API, replaced by the Rust port in [`../http-api-rust`](../http-api-rust). Same endpoints and cache, but one uvicorn process per worker, each with its own cache and connection pool. To deploy it instead of the Rust version:

```
API_IMPL= docker compose up -d --build api
```

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:cec@mypg/temp_db` | |
| `WORKERS` | `2` | uvicorn worker processes |
| `POOL_SIZE` | `10` | DB connections per worker |
| `CACHE_SIZE` | `8000` | cached experiments per worker, `0` disables the cache |
| `CACHE_REFRESH` | `1` | seconds between cache refreshes |
