-- For creating measurements table
CREATE TABLE measurements (
    measurement_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    measurement_hash TEXT,
    out_of_range BOOLEAN NOT NULL DEFAULT FALSE
);

-- For creating experiments_terminated table, supports fast lookup of terminated experiments
CREATE TABLE experiments_terminated (
    experiment_id TEXT PRIMARY KEY,
    terminated_at DOUBLE PRECISION NOT NULL
);

-- Supports /temperature
CREATE INDEX idx_measurements_experiment_timestamp
ON measurements (experiment_id, timestamp);

-- Supports /temperature/out-of-range
CREATE INDEX idx_measurements_out_of_range
ON measurements (experiment_id, timestamp)
WHERE out_of_range = TRUE;

-- State of every experiment, so another consumer can take over its partition.
-- The consumer also creates it, for databases that already existed.
CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    researcher TEXT,
    lower_threshold DOUBLE PRECISION NOT NULL,
    upper_threshold DOUBLE PRECISION NOT NULL,
    num_sensors INTEGER NOT NULL,
    phase TEXT NOT NULL,
    -- whether the last measurement was out of range
    out_of_range BOOLEAN NOT NULL,
    -- Kafka offset of the last event that changed the experiment
    last_offset BIGINT NOT NULL
);
