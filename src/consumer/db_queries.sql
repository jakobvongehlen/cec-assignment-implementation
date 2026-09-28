-- For creating measurements table
CREATE TABLE measurements (
    measurement_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    measurement_hash TEXT,
    out_of_range BOOLEAN NOT NULL DEFAULT FALSE
);

-- Supports /temperature
CREATE INDEX idx_measurements_experiment_timestamp
ON measurements (experiment_id, timestamp);

-- Supports /temperature/out-of-range
CREATE INDEX idx_measurements_out_of_range
ON measurements (experiment_id, timestamp)
WHERE out_of_range = TRUE;