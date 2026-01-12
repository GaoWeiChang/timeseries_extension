DROP TABLE IF EXISTS sensor_data CASCADE;
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

\echo 'Created table: sensor_data'

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');

INSERT INTO sensor_data VALUES ('2024-01-01 00:00:00+00'::timestamptz, 1, 25.5, 60.0);

INSERT INTO sensor_data VALUES 
    ('2024-01-01 06:00:00+00', 1, 26.0, 61.0),
    ('2024-01-01 12:00:00+00', 2, 27.5, 62.0),
    ('2024-01-01 18:00:00+00', 1, 25.0, 59.0);

SELECT COUNT(*) FROM sensor_data;

SELECT * FROM _hyper_1_1_chunk ORDER BY time;


INSERT INTO sensor_data VALUES ('2024-01-02 10:00:00+00', 3, 28.0, 63.0);

\echo 'Chunks created:'
SELECT 
    id,
    table_name,
    TO_TIMESTAMP(start_time::double precision / 1000000) AT TIME ZONE 'UTC' as start_time
FROM _timeseries_catalog.chunk
ORDER BY id;


\echo 'Insert across multiple days...'

INSERT INTO sensor_data VALUES ('2024-01-03 08:00:00+00'::timestamptz, 1, 24.0, 58.0);
INSERT INTO sensor_data VALUES ('2024-01-03 16:00:00+00'::timestamptz, 2, 26.5, 60.5);
INSERT INTO sensor_data VALUES ('2024-01-04 09:00:00+00'::timestamptz, 3, 27.0, 61.5);
INSERT INTO sensor_data VALUES ('2024-01-04 15:00:00+00'::timestamptz, 1, 25.5, 59.5);
INSERT INTO sensor_data VALUES ('2024-01-05 11:00:00+00'::timestamptz, 2, 28.5, 63.5);

SELECT COUNT(*) as total_rows FROM sensor_data;


\echo 'Test batch insert 100 rows...'

\timing on

INSERT INTO sensor_data 
SELECT 
    '2024-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval as time,
    (i % 5) + 1 as sensor_id,
    20 + (random() * 10) as temperature,
    50 + (random() * 20) as humidity
FROM generate_series(1, 100) i;

\timing off

\echo 'Total rows after batch insert:'
SELECT COUNT(*) FROM sensor_data;


\echo 'Testing second hypertable with hourly chunks...'

DROP TABLE IF EXISTS metrics CASCADE;
CREATE TABLE metrics (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);

SELECT create_hypertable('metrics', 'time', INTERVAL '1 hour');

\echo 'Inserting data spanning 3 hours...'
INSERT INTO metrics VALUES ('2024-01-01 00:30:00+00', 100.0);
INSERT INTO metrics VALUES ('2024-01-01 01:30:00+00', 150.0);
INSERT INTO metrics VALUES ('2024-01-01 02:30:00+00', 200.0);


SELECT * FROM metrics ORDER BY time;


\echo 'Display all triggers:'
SELECT * FROM display_all_triggers();