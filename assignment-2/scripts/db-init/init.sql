CREATE TABLE IF NOT EXISTS silver_data (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR,
    temperature FLOAT,
    humidity FLOAT,
    location VARCHAR,
    timestamp TIMESTAMP,
    status VARCHAR,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
