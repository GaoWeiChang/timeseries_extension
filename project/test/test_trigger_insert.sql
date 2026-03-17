DROP TABLE IF EXISTS sensor_data CASCADE;
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

\echo 'Created table: sensor_data'

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');

INSERT INTO sensor_data VALUES ('2024-01-01 00:00:00+00', 1, 25.5, 60.0);

INSERT INTO sensor_data VALUES 
    ('2024-01-01 06:00:00+00', 1, 26.0, 61.0),
    ('2024-01-01 12:00:00+00', 2, 27.5, 62.0),
    ('2024-01-01 18:00:00+00', 1, 25.0, 59.0);

-- parent table must return 0
SELECT COUNT(*) FROM ONLY sensor_data; 

-- return 4 (child included)
SELECT COUNT(*) FROM sensor_data;

SELECT * FROM _hyper_1_1_chunk ORDER BY time;


INSERT INTO sensor_data VALUES ('2024-01-02 10:00:00+00', 3, 28.0, 63.0);

\echo 'Chunks created:'
SELECT 
    id,
    table_name,
    TO_TIMESTAMP((start_time::double precision / 1000000) + 946684800) AT TIME ZONE 'UTC' as start_time
FROM _timeseries_catalog.chunk
ORDER BY id;


\echo 'Insert across multiple days...'

INSERT INTO sensor_data VALUES 
    ('2024-01-03 08:00:00+00', 1, 24.0, 58.0),
    ('2024-01-03 16:00:00+00', 2, 26.5, 60.5),
    ('2024-01-04 09:00:00+00', 3, 27.0, 61.5),
    ('2024-01-04 15:00:00+00', 1, 25.5, 59.5),
    ('2024-01-05 11:00:00+00', 2, 28.5, 63.5);

SELECT COUNT(*) as total_rows FROM sensor_data;

\echo 'Testing second hypertable with hourly chunks...'

DROP TABLE IF EXISTS metrics CASCADE;
CREATE TABLE metrics (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);

SELECT create_hypertable('metrics', 'time', INTERVAL '1 hour');

\echo 'Inserting data spanning 3 hours...'

INSERT INTO metrics VALUES
    ('2024-01-01 00:30:00+00', 100.0),
    ('2024-01-01 01:30:00+00', 150.0),
    ('2024-01-01 02:30:00+00', 200.0);

SELECT * FROM metrics ORDER BY time;


\echo 'Display all triggers:'
SELECT * FROM display_all_triggers();