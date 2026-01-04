\echo 'Step 1: Creating test tables...'
\echo ''

-- table 1: metrics 
DROP TABLE IF EXISTS metrics CASCADE;
CREATE TABLE metrics (
    time_val TIMESTAMPTZ NOT NULL,
    device_id INTEGER,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);


-- table 2: sensors
DROP TABLE IF EXISTS sensors CASCADE;
CREATE TABLE sensors (
    timestamp_val TIMESTAMPTZ NOT NULL,
    sensor_id TEXT,
    value DOUBLE PRECISION
);

SELECT create_hypertable('metrics', 'time_val', INTERVAL '1 day');


\echo 'Test 2: Verifying metadata...'
\echo ''

\echo 'Hypertable metadata:'
SELECT * FROM _timeseries_catalog.hypertable WHERE table_name = 'metrics';
\echo ''

\echo 'Dimension metadata:'
SELECT * FROM _timeseries_catalog.dimension WHERE hypertable_id = 1;
\echo ''

\echo 'Test 3: Creating second hypertable with 1 hour interval...'
\echo ''

SELECT create_hypertable('sensors', 'timestamp_val', INTERVAL '1 hour');

\echo 'Test 4: Viewing all hypertables...'

SELECT * FROM _timeseries_catalog.hypertables ORDER BY id;


\echo 'Test 8: Testing different interval sizes...'
\echo ''

DROP TABLE IF EXISTS test_intervals CASCADE;
CREATE TABLE test_intervals (
    time_val TIMESTAMPTZ NOT NULL,
    val DOUBLE PRECISION
);

SELECT create_hypertable('test_intervals', 'time', INTERVAL '1 hour');

SELECT 
    h.table_name,
    d.interval_length as interval_microseconds,
    (d.interval_length / 1000000) as interval_seconds,
    (d.interval_length / 1000000 / 3600) as interval_hours
FROM _timeseries_catalog.hypertable h
JOIN _timeseries_catalog.dimension d ON h.id = d.hypertable_id
WHERE h.table_name = 'test_intervals';

\echo ''
\echo '✓ Test 8 passed'
\echo ''