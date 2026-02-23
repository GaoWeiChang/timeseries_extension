DROP TABLE IF EXISTS sensor_data CASCADE;

CREATE TABLE sensor_data (
    time        TIMESTAMPTZ NOT NULL,
    sensor_id   INTEGER,
    temperature DOUBLE PRECISION,
    humidity    DOUBLE PRECISION,
    location    TEXT
);

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');

-- insert 1440 rows (1 day = 1440 minutes)
INSERT INTO sensor_data
SELECT
    '2024-01-01'::timestamptz + (i || ' minutes')::interval AS time,
    (i % 10) + 1 AS sensor_id,
    20 + random() * 15 AS temperature,
    40 + random() * 30 AS humidity,
    CASE (i % 5)
        WHEN 0 THEN 'Building A, Floor 1'
        WHEN 1 THEN 'Building A, Floor 2'
        WHEN 2 THEN 'Building B, Floor 1'
        WHEN 3 THEN 'Building B, Floor 2'
        ELSE 'Building C, Floor 1'
    END AS location
FROM generate_series(0, 1439) i; 

-- day 2
INSERT INTO sensor_data
SELECT
    '2024-01-02'::timestamptz + (i || ' minutes')::interval,
    (i % 10) + 1,
    20 + random() * 15,
    40 + random() * 30,
    CASE (i % 5)
        WHEN 0 THEN 'Building A, Floor 1'
        WHEN 1 THEN 'Building A, Floor 2'
        WHEN 2 THEN 'Building B, Floor 1'
        WHEN 3 THEN 'Building B, Floor 2'
        ELSE 'Building C, Floor 1'
    END
FROM generate_series(0, 1439) i;

-- insert every second in chunk
INSERT INTO sensor_data
SELECT
    '2024-01-03'::timestamptz + (i || ' seconds')::interval AS time,
    (i % 10) + 1,
    20 + random() * 15,
    40 + random() * 30,
    CASE (i % 5)
        WHEN 0 THEN 'Building A, Floor 1'
        WHEN 1 THEN 'Building A, Floor 2'
        WHEN 2 THEN 'Building B, Floor 1'
        WHEN 3 THEN 'Building B, Floor 2'
        ELSE 'Building C, Floor 1'
    END
FROM generate_series(0, 86399) i;

-- check uncompressed chunk size
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(
        quote_ident(schema_name) || '.' || quote_ident(table_name)
    )) AS size,
    (SELECT COUNT(*) FROM public._hyper_1_1_chunk) AS rows
FROM _timeseries_catalog.chunk
WHERE id = 1
ORDER BY table_name;

-- compress chunk
SELECT compress_chunk('_hyper_1_1_chunk');

-- check whether chunk compressed or not
SELECT COUNT(*) FROM _timeseries_catalog.compressed_chunk WHERE chunk_id=1;
-- =========================
--  count 
-- -------
--      5
-- (1 row)
-- =========================

-- total compressed chunk size 
SELECT
    pg_size_pretty(SUM(pg_column_size(column_data)::bigint)) AS compressed_size
FROM _timeseries_catalog.compressed_chunk
WHERE chunk_id = 1;