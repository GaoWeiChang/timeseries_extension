DROP TABLE IF EXISTS sensor_data CASCADE;

CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    device_name TEXT,          -- encode by dictionary
    location TEXT,             -- encode by dictionary
    sensor_id INTEGER,
    value DOUBLE PRECISION
);

INSERT INTO sensor_data
SELECT 
    '2024-01-01'::timestamptz + (i || ' minutes')::interval,
    'Device_' || ((i % 10) + 1),           -- 10 unique devices
    'Location_' || ((i % 5) + 1),          -- 5 unique locations
    (i % 20) + 1,
    random() * 100
FROM generate_series(1, 1000) i;

SELECT show_compression_info('sensor_data');

SELECT test_compress_chunk_column('sensor_data', 'device_name');

SELECT test_compress_chunk_column('sensor_data', 'location');