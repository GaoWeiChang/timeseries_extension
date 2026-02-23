DROP TABLE IF EXISTS sensor_readings CASCADE;

CREATE TABLE sensor_readings (
    time       TIMESTAMPTZ NOT NULL,
    sensor_id  INTEGER,
    temperature DOUBLE PRECISION,
    humidity    DOUBLE PRECISION
);

SELECT create_hypertable('sensor_readings', 'time', INTERVAL '1 day');

-- insert 7 days of chunk
INSERT INTO sensor_readings
SELECT
    '2026-02-13'::timestamptz + (i || ' hours')::interval AS time,
    ((i % 5) + 1) AS sensor_id,
    20 + random() * 15 AS temperature,
    40 + random() * 30 AS humidity
FROM generate_series(0, 167) i;

-- check time bucket
SELECT
    time_bucket('1 hour', '2026-02-13 14:35:22+00'::timestamptz) AS bucketed_hour,
    time_bucket('1 day',  '2026-02-17 17:30:00+00'::timestamptz) AS bucketed_day;

-- create_continuous_aggregate() — Hourly
SELECT create_continuous_aggregate(
    'sensor_hourly',                                     -- view_name
    'sensor_readings',                                   -- hypertable
    'SELECT
        time_bucket(''1 hour'', time) AS bucket,
        sensor_id,
        AVG(temperature) AS avg_temp,
        AVG(humidity) AS avg_humidity,
        COUNT(*) AS sample_count
     FROM sensor_readings
     GROUP BY bucket, sensor_id',                        -- view_sql
    INTERVAL '1 hour',                                   -- bucket_width
    INTERVAL '1 minute'                                  -- refresh_interval (auto)
);


-- manual refresh
SELECT refresh_continuous_aggregate(
    'sensor_hourly',
    '2026-02-13 00:00:00+00'::timestamptz,
    '2026-02-20 00:00:00+00'::timestamptz
);

-- check whether data inserted or not
SELECT
    bucket,
    sensor_id,
    avg_temp::numeric(5,2),
    avg_humidity::numeric(5,2),
    sample_count
FROM _timeseries_catalog.sensor_hourly
WHERE bucket >= '2026-02-13' AND bucket < '2026-02-13 05:00:00'
ORDER BY bucket, sensor_id;


-- check amount cagg
SELECT *
FROM _timeseries_catalog.continuous_aggregate;



-- create_continuous_aggregate() — daily
SELECT create_continuous_aggregate(
    'sensor_daily',
    'sensor_readings',
    'SELECT
        time_bucket(''1 day'', time) AS bucket,
        AVG(temperature) AS avg_temp,
        MIN(temperature) AS min_temp,
        MAX(temperature) AS max_temp,
        COUNT(*) AS sample_count
     FROM sensor_readings
     GROUP BY bucket',
    INTERVAL '1 day',
    INTERVAL '1 hour'      -- auto refresh every 1 hour
);

SELECT refresh_continuous_aggregate(
    'sensor_daily',
    '2026-02-13'::timestamptz,
    '2026-02-20'::timestamptz
);

-- Query
\echo ''
\echo '    Daily summary:'
SELECT
    bucket::date AS day,
    avg_temp::numeric(5,2),
    min_temp::numeric(5,2),
    max_temp::numeric(5,2),
    sample_count
FROM _timeseries_catalog.sensor_daily
ORDER BY bucket;

/*
Expected output:
    day     | avg_temp | min_temp | max_temp | sample_count
------------+----------+----------+----------+--------------
 2026-02-13 |    27.34 |    20.12 |    34.89 |           24
 2026-02-14 |    28.56 |    21.45 |    33.67 |           24
 2026-02-15 |    26.89 |    20.78 |    35.12 |           24
 2026-02-16 |    29.12 |    22.34 |    34.56 |           24
 2026-02-17 |    27.78 |    21.89 |    33.45 |           24
 2026-02-18 |    28.23 |    20.56 |    35.89 |           24
 2026-02-19 |    27.45 |    21.23 |    34.12 |           24
(7 rows)
*/


-- Insert new data
INSERT INTO sensor_readings VALUES
    ('2026-02-20 00:00:00+00', 1, 30.0, 60.0),
    ('2026-02-20 01:00:00+00', 2, 31.0, 62.0),
    ('2026-02-20 02:00:00+00', 3, 29.0, 58.0);

\echo '    Materialized data (ยังไม่มี 2026-02-20):'
SELECT COUNT(*) AS materialized_count
FROM _timeseries_catalog.sensor_hourly
WHERE bucket >= '2026-02-20';
-- Expected: 0

-- manual refresh 
SELECT refresh_continuous_aggregate(
    'sensor_hourly',
    '2026-02-20 00:00:00+00',
    '2026-02-21 00:00:00+00'
);

-- after refresh
SELECT COUNT(*) AS materialized_count
FROM _timeseries_catalog.sensor_hourly
WHERE bucket >= '2026-02-20';
-- Expected: 3


-- drop cagg
SELECT drop_continuous_aggregate('sensor_daily');


-- check background worker in database (must have 1 cagg bgw in that table))
SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'test_db';

-- check which chunk need to refresh
SELECT id, view_name, watermark, bucket_width, refresh_interval
FROM _timeseries_catalog.continuous_aggregate
WHERE (refresh_interval > 0) AND
    ((updated_at IS NULL) OR (NOW() >= (updated_at + CONCAT(refresh_interval, ' microseconds')::interval)));
    

