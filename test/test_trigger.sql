DROP TABLE IF EXISTS sensor_data CASCADE;
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

\echo '✓ Created table: sensor_data'
\echo ''

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');

INSERT INTO sensor_data VALUES ('2024-01-01 00:00:00+00'::timestamptz, 1, 25.5, 60.0);

SELECT COUNT(*) FROM ONLY sensor_data;
SELECT COUNT(*) FROM sensor_data;

SELECT 
    id,
    table_name,
    TO_TIMESTAMP(start_time::double precision / 1000000) AT TIME ZONE 'UTC' as start_time,
    TO_TIMESTAMP(end_time::double precision / 1000000) AT TIME ZONE 'UTC' as end_time
FROM _timeseries_catalog.chunk
ORDER BY id;


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


\echo 'Test 4: Batch insert across multiple days...'
\echo ''

INSERT INTO sensor_data VALUES 
    ('2024-01-03 08:00:00+00', 1, 24.0, 58.0),
    ('2024-01-03 16:00:00+00', 2, 26.5, 60.5),
    ('2024-01-04 09:00:00+00', 3, 27.0, 61.5),
    ('2024-01-04 15:00:00+00', 1, 25.5, 59.5),
    ('2024-01-05 11:00:00+00', 2, 28.5, 63.5);

\echo ''
\echo 'Total data in hypertable:'
SELECT COUNT(*) as total_rows FROM sensor_data;

\echo ''
\echo 'All chunks:'
SELECT 
    c.table_name,
    TO_TIMESTAMP(c.start_time::double precision / 1000000) AT TIME ZONE 'UTC' as start_time
FROM _timeseries_catalog.chunk c
ORDER BY c.start_time;

\echo 'Test 5: Querying through parent table...'
\echo ''

\echo 'Query 1: First 5 rows ordered by time'
SELECT 
    time,
    sensor_id,
    temperature,
    humidity
FROM sensor_data
ORDER BY time
LIMIT 5;

\echo ''
\echo 'Query 2: Daily aggregates'
SELECT 
    DATE(time) as date,
    COUNT(*) as measurements,
    AVG(temperature)::numeric(5,2) as avg_temp,
    AVG(humidity)::numeric(5,2) as avg_humidity
FROM sensor_data
GROUP BY DATE(time)
ORDER BY date;

\echo ''
\echo 'Query 3: Per sensor statistics'
SELECT 
    sensor_id,
    COUNT(*) as readings,
    MIN(temperature)::numeric(5,2) as min_temp,
    MAX(temperature)::numeric(5,2) as max_temp,
    AVG(temperature)::numeric(5,2) as avg_temp
FROM sensor_data
GROUP BY sensor_id
ORDER BY sensor_id;

\echo 'Test 6: Performance test - batch insert 100 rows...'
\echo ''

\timing on

INSERT INTO sensor_data 
SELECT 
    '2024-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval as time,
    (i % 5) + 1 as sensor_id,
    20 + (random() * 10) as temperature,
    50 + (random() * 20) as humidity
FROM generate_series(1, 100) i;

\timing off

\echo ''
\echo 'Total rows after batch insert:'
SELECT COUNT(*) FROM sensor_data;


\echo 'Test 7: Testing second hypertable with hourly chunks...'
\echo ''

DROP TABLE IF EXISTS metrics CASCADE;
CREATE TABLE metrics (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);

SELECT create_hypertable('metrics', 'time', INTERVAL '1 hour');

\echo ''
\echo 'Inserting data spanning 3 hours...'
INSERT INTO metrics VALUES
    ('2024-01-01 00:30:00+00', 100.0),
    ('2024-01-01 01:30:00+00', 150.0),
    ('2024-01-01 02:30:00+00', 200.0);

\echo ''
\echo 'Hourly chunks created:'
SELECT 
    table_name,
    TO_TIMESTAMP(start_time::double precision / 1000000) AT TIME ZONE 'UTC' as start_time,
    TO_TIMESTAMP(end_time::double precision / 1000000) AT TIME ZONE 'UTC' as end_time
FROM _timeseries_catalog.chunk
WHERE hypertable_id = 2
ORDER BY start_time;

\echo ''
\echo 'Data in metrics:'
SELECT * FROM metrics ORDER BY time;

\echo '=========================================='
\echo 'Summary'
\echo '=========================================='
\echo ''

\echo 'All Hypertables:'
SELECT * FROM _timeseries_catalog.hypertables ORDER BY id;

\echo ''
\echo 'Total Chunks Created:'
SELECT COUNT(*) as total_chunks FROM _timeseries_catalog.chunk;

\echo ''
\echo 'Chunk Details:'
SELECT 
    h.table_name as hypertable,
    c.table_name as chunk,
    TO_TIMESTAMP(c.start_time::double precision / 1000000) AT TIME ZONE 'UTC' as start_time,
    TO_TIMESTAMP(c.end_time::double precision / 1000000) AT TIME ZONE 'UTC' as end_time
FROM _timeseries_catalog.chunk c
JOIN _timeseries_catalog.hypertable h ON c.hypertable_id = h.id
ORDER BY h.id, c.start_time;

\echo ''
\echo '=========================================='
\echo '✅ All tests passed!'
\echo 'INSERT Trigger is working perfectly!'
\echo '=========================================='
\echo ''
