-- For creating measurements table
CREATE TABLE measurements (
    measurement_id TEXT PRIMARY KEY,
    experiment_id TEXT,
    timestamp DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    measurement_hash TEXT,
    out_of_range BOOLEAN
);