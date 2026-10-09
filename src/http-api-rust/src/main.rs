//! Rust port of `src/http-api/api.py`: same endpoints, same SQL, same response
//! bodies, same cache of terminated experiments. It runs as one process with a
//! multithreaded runtime, so all threads share one cache and one connection pool.

use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde_json::{json, Value};
use tokio_postgres::NoTls;
use tracing::{error, info};

// Postgres builds the JSON array, so the service only passes the text through
const TEMPERATURE_SQL: &str = "SELECT COALESCE(json_agg(json_build_object('timestamp', timestamp, 'temperature', temperature) \
    ORDER BY timestamp), '[]')::text FROM measurements \
    WHERE experiment_id = $1 AND timestamp >= $2 AND timestamp <= $3";
const OUT_OF_RANGE_SQL: &str = "SELECT COALESCE(json_agg(json_build_object('timestamp', timestamp, 'temperature', temperature) \
    ORDER BY timestamp), '[]')::text FROM measurements \
    WHERE experiment_id = $1 AND out_of_range = TRUE";
const TERMINATED_SQL: &str =
    "SELECT experiment_id FROM experiments_terminated ORDER BY terminated_at DESC LIMIT $1";
// every measurement of the given experiments, as JSON fragments, to fill the cache
const EXPERIMENTS_SQL: &str = "SELECT experiment_id, timestamp, out_of_range, \
    json_build_object('timestamp', timestamp, 'temperature', temperature)::text \
    FROM measurements WHERE experiment_id = ANY($1) ORDER BY experiment_id, timestamp";

// experiments loaded per query
const CACHE_LOAD_BATCH: usize = 100;

/// All measurements of one terminated experiment. Terminated experiments never
/// receive new measurements, so any time range is answered from these.
struct Entry {
    /// sorted timestamps
    timestamps: Vec<f64>,
    /// all rows as one comma separated JSON string
    rows_json: Bytes,
    /// where each row starts in `rows_json`, plus one start past the end, as if
    /// every row was followed by a comma
    starts: Vec<u32>,
    /// the finished out-of-range response
    out_of_range: Bytes,
}

type Cache = RwLock<HashMap<String, Arc<Entry>>>;

struct AppState {
    pool: Pool,
    cache: Cache,
}

fn env_or<T: FromStr>(name: &str, default: T) -> T {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("invalid value for {name}: {value}")),
        Err(_) => default,
    }
}

fn main() {
    tracing_subscriber::fmt().init();

    // worker threads, default one per core
    let workers: usize = env_or(
        "WORKERS",
        std::thread::available_parallelism().map_or(1, |n| n.get()),
    );
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(serve(workers));
}

async fn serve(workers: usize) {
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:cec@mypg/temp_db".to_owned());
    // connections per worker thread, so the total matches the Python version's
    // WORKERS * POOL_SIZE; all threads share one pool
    let pool_size: usize = workers * env_or::<usize>("POOL_SIZE", 10);
    // cached experiments, the most recently terminated ones are kept
    let cache_size: i64 = env_or("CACHE_SIZE", 8000);
    // seconds between checks for newly terminated experiments
    let cache_refresh: f64 = env_or("CACHE_REFRESH", 1.0);
    let port: u16 = env_or("PORT", 3003);

    // accepts URLs as well as libpq's "key=value" form
    let pg_config = tokio_postgres::Config::from_str(&database_url).expect("invalid DATABASE_URL");
    let manager = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig { recycling_method: RecyclingMethod::Fast },
    );
    let pool = Pool::builder(manager).max_size(pool_size).build().expect("pool");

    let state = Arc::new(AppState { pool, cache: RwLock::new(HashMap::new()) });
    let refresher = tokio::spawn(refresh_cache(
        state.clone(),
        cache_size,
        Duration::from_secs_f64(cache_refresh),
    ));

    let app = Router::new()
        .route("/temperature", get(temperature))
        .route("/temperature/out-of-range", get(out_of_range))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("bind");
    info!("listening on 0.0.0.0:{port} with {workers} threads, {pool_size} connections");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server");
    refresher.abort();
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn load_experiments(state: &AppState, experiment_ids: &[String]) -> Result<(), BoxError> {
    let client = state.pool.get().await?;
    let statement = client.prepare_cached(EXPERIMENTS_SQL).await?;
    let rows = client.query(&statement, &[&experiment_ids]).await?;

    // experiments without measurements are cached too, they answer with []
    // experiment_id -> (timestamps, rows_json, starts, out_of_range)
    #[allow(clippy::type_complexity)]
    let mut loaded: HashMap<&str, (Vec<f64>, String, Vec<u32>, String)> = experiment_ids
        .iter()
        .map(|id| (id.as_str(), (Vec::new(), String::new(), vec![0], String::from("["))))
        .collect();
    for row in &rows {
        let experiment_id: &str = row.get(0);
        let timestamp: f64 = row.get(1);
        let is_out_of_range: bool = row.get(2);
        let fragment: &str = row.get(3);
        let Some((timestamps, rows_json, starts, out_of_range)) = loaded.get_mut(experiment_id)
        else {
            continue;
        };
        timestamps.push(timestamp);
        if !rows_json.is_empty() {
            rows_json.push(',');
        }
        rows_json.push_str(fragment);
        starts.push(starts.last().unwrap() + fragment.len() as u32 + 1);
        if is_out_of_range {
            if out_of_range.len() > 1 {
                out_of_range.push(',');
            }
            out_of_range.push_str(fragment);
        }
    }

    let mut cache = state.cache.write().unwrap();
    for (experiment_id, (timestamps, rows_json, starts, mut out_of_range)) in loaded {
        out_of_range.push(']');
        cache.insert(
            experiment_id.to_owned(),
            Arc::new(Entry {
                timestamps,
                rows_json: Bytes::from(rows_json),
                starts,
                out_of_range: Bytes::from(out_of_range),
            }),
        );
    }
    Ok(())
}

async fn refresh_once(state: &AppState, cache_size: i64) -> Result<(), BoxError> {
    let newest: HashSet<String> = {
        let client = state.pool.get().await?;
        let statement = client.prepare_cached(TERMINATED_SQL).await?;
        client
            .query(&statement, &[&cache_size])
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect()
    };
    let missing: Vec<String> = {
        let mut cache = state.cache.write().unwrap();
        cache.retain(|experiment_id, _| newest.contains(experiment_id));
        newest.into_iter().filter(|id| !cache.contains_key(id)).collect()
    };
    // in chunks, so filling an empty cache at startup doesn't fetch everything at once
    for chunk in missing.chunks(CACHE_LOAD_BATCH) {
        load_experiments(state, chunk).await?;
    }
    Ok(())
}

async fn refresh_cache(state: Arc<AppState>, cache_size: i64, interval: Duration) {
    // The cache always holds the CACHE_SIZE most recently terminated experiments.
    // Comparing the whole list doesn't depend on rows arriving in timestamp order.
    loop {
        if let Err(e) = refresh_once(&state, cache_size).await {
            error!("cache refresh failed: {e}");
        }
        tokio::time::sleep(interval).await;
    }
}

fn json_response(body: impl Into<axum::body::Body>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body.into()).into_response()
}

fn internal_error(e: impl std::fmt::Display) -> Response {
    error!("request failed: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "Internal Server Error",
    )
        .into_response()
}

/// Query parameter parsing with FastAPI's 422 error bodies. As in Starlette,
/// the last value of a repeated parameter wins.
struct Params {
    values: Vec<(String, String)>,
    errors: Vec<Value>,
}

impl Params {
    fn new(values: Vec<(String, String)>) -> Self {
        Self { values, errors: Vec::new() }
    }

    fn string(&mut self, name: &str) -> Option<String> {
        let value = self.values.iter().rev().find(|(key, _)| key == name).map(|(_, v)| v.clone());
        if value.is_none() {
            self.errors.push(json!({
                "type": "missing", "loc": ["query", name], "msg": "Field required", "input": null,
            }));
        }
        value
    }

    fn float(&mut self, name: &str) -> Option<f64> {
        let raw = self.string(name)?;
        match raw.trim().parse::<f64>() {
            Ok(value) => Some(value),
            Err(_) => {
                self.errors.push(json!({
                    "type": "float_parsing", "loc": ["query", name],
                    "msg": "Input should be a valid number, unable to parse string as a number",
                    "input": raw,
                }));
                None
            }
        }
    }

    fn rejection(self) -> Response {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            [(header::CONTENT_TYPE, "application/json")],
            json!({ "detail": self.errors }).to_string(),
        )
            .into_response()
    }
}

async fn temperature(
    State(state): State<Arc<AppState>>,
    Query(query): Query<Vec<(String, String)>>,
) -> Response {
    let mut params = Params::new(query);
    let experiment_id = params.string("experiment-id");
    let start_time = params.float("start-time");
    let end_time = params.float("end-time");
    let (Some(experiment_id), Some(start_time), Some(end_time)) =
        (experiment_id, start_time, end_time)
    else {
        return params.rejection();
    };

    let entry = state.cache.read().unwrap().get(&experiment_id).cloned();
    match entry {
        Some(entry) => {
            // inclusive on both ends, like the SQL query. Same comparisons as
            // Python's bisect_left/bisect_right, so NaN bounds behave the same.
            let lo = entry.timestamps.partition_point(|&t| t < start_time);
            let hi = entry.timestamps.partition_point(|&t| !(end_time < t));
            if lo >= hi {
                return json_response("[]");
            }
            let rows = &entry.rows_json[entry.starts[lo] as usize..entry.starts[hi] as usize - 1];
            let mut body = Vec::with_capacity(rows.len() + 2);
            body.push(b'[');
            body.extend_from_slice(rows);
            body.push(b']');
            json_response(body)
        }
        None => {
            let result = async {
                let client = state.pool.get().await?;
                let statement = client.prepare_cached(TEMPERATURE_SQL).await?;
                let row = client
                    .query_one(&statement, &[&experiment_id, &start_time, &end_time])
                    .await?;
                Ok::<String, BoxError>(row.get(0))
            };
            match result.await {
                Ok(body) => json_response(body),
                Err(e) => internal_error(e),
            }
        }
    }
}

async fn out_of_range(
    State(state): State<Arc<AppState>>,
    Query(query): Query<Vec<(String, String)>>,
) -> Response {
    let mut params = Params::new(query);
    let Some(experiment_id) = params.string("experiment-id") else {
        return params.rejection();
    };

    let entry = state.cache.read().unwrap().get(&experiment_id).cloned();
    match entry {
        Some(entry) => json_response(entry.out_of_range.clone()),
        None => {
            let result = async {
                let client = state.pool.get().await?;
                let statement = client.prepare_cached(OUT_OF_RANGE_SQL).await?;
                let row = client.query_one(&statement, &[&experiment_id]).await?;
                Ok::<String, BoxError>(row.get(0))
            };
            match result.await {
                Ok(body) => json_response(body),
                Err(e) => internal_error(e),
            }
        }
    }
}
