DROP DATABASE IF EXISTS test_metadata;
CREATE DATABASE test_metadata;
\c test_metadata

CREATE EXTENSION simple_timeseries;

\echo '==========================================';
\echo 'Test 1: Create Hypertable Metadata';
\echo '==========================================';

CREATE TABLE metrics (
    time TIMESTAMPTZ NOT NULL,
    device_id INTEGER,
    value DOUBLE PRECISION
);

SELECT test_create_hypertable_metadata(
    'public',
    'metrics',
    'time',
    'timestamptz'::regtype,
    86400000000  -- 1 day
);

\echo '';
\echo 'Test 2: Check if Hypertable Exists';
\echo '==========================================';

SELECT check_hypertable_exists('public', 'metrics');
SELECT check_hypertable_exists('public', 'nonexistent');

\echo '';
\echo 'Test 3: Get Hypertable Info';
\echo '==========================================';

SELECT get_hypertable_info('public', 'metrics');

\echo '';
\echo 'Test 4: View Metadata Tables Directly';
\echo '==========================================';

SELECT * FROM _timeseries_catalog.hypertable;
SELECT * FROM _timeseries_catalog.dimension;
SELECT * FROM _timeseries_catalog.chunk;

\echo '';
\echo '==========================================';
\echo 'Test 5: Create Chunk Metadata';
\echo '==========================================';

SELECT test_create_chunk_metadata(
    1,                       -- hypertable_id
    'public',
    '_hyper_1_1_chunk',
    0,                       -- start_time
    86400000000             -- end_time (1 day later)
);
SELECT test_create_chunk_metadata(
    1,
    'public',
    '_hyper_1_2_chunk',
    86400000000,            -- 1 day
    172800000000            -- 2 days
);

-- Chunk 3: Day 2
SELECT test_create_chunk_metadata(
    1,
    'public',
    '_hyper_1_3_chunk',
    172800000000,           -- 2 days
    259200000000            -- 3 days
);

\echo '';
\echo 'Test 6: View Chunk Metadata';
\echo '==========================================';

SELECT * FROM _timeseries_catalog.chunk;

\echo '';
\echo 'Test 7: Find Chunk by Time';
\echo '==========================================';

SELECT test_find_chunk(1, 43200000000) AS chunk_id_for_day0_noon;
SELECT test_find_chunk(1, 129600000000) AS chunk_id_for_day1_noon;
SELECT test_find_chunk(1, 999999999999999) AS chunk_id_for_future;

\echo '';
\echo 'Test 8: View Complete Hypertable Info';
\echo '==========================================';

SELECT * FROM _timeseries_catalog.hypertables;

\echo '';
\echo '==========================================';
\echo 'Summary';
\echo '==========================================';

SELECT 
    'Hypertables' AS object_type,
    COUNT(*) AS count
FROM _timeseries_catalog.hypertable
UNION ALL
SELECT 
    'Chunks',
    COUNT(*)
FROM _timeseries_catalog.chunk;

\c postgres
DROP DATABASE test_metadata;

\echo '';
\echo '✅ All metadata tests completed!';
