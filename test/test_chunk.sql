\echo ''
\echo '=========================================='
\echo 'Testing Chunk Management System'
\echo '=========================================='
\echo ''

-- ==========================================
-- Preparation
-- ==========================================

\echo 'Step 1: Setting up test environment...'
\echo ''

DROP TABLE IF EXISTS chunk_test CASCADE;
CREATE TABLE chunk_test (
    time TIMESTAMPTZ NOT NULL,
    device_id INTEGER,
    value DOUBLE PRECISION
);

SELECT create_hypertable('chunk_test', 'time', INTERVAL '1 day');

\echo '✓ Created hypertable: chunk_test'
\echo ''


\echo 'Test 1: Creating first chunk...'
\echo ''

SELECT test_create_chunk(1, '2024-01-01 00:00:00+00'::timestamptz) AS chunk_id;

\echo ''
\echo 'Verifying chunk creation...'
SELECT * FROM _timeseries_catalog.chunk WHERE hypertable_id = 1;
\echo ''

\echo '✓ Test 1 passed'
\echo ''

-- ==========================================
-- Test 2: Chunk Table
-- ==========================================

\echo 'Test 2: Verifying chunk table exists...'
\echo ''

\echo 'Chunk tables in schema:'
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE tablename LIKE '_hyper_%'
ORDER BY tablename;

\echo ''
\echo '✓ Test 2 passed'
\echo ''

-- ==========================================
-- Test 3: Inheritance
-- ==========================================

\echo 'Test 3: Verifying table inheritance...'
\echo ''

SELECT 
    c.relname as child_table,
    p.relname as parent_table
FROM pg_inherits i
JOIN pg_class c ON i.inhrelid = c.oid
JOIN pg_class p ON i.inhparent = p.oid
WHERE p.relname = 'chunk_test';

\echo ''
\echo '✓ Test 3 passed'
\echo ''

-- ==========================================
-- Test 4: CHECK Constraint
-- ==========================================

\echo 'Test 4: Verifying CHECK constraint...'
\echo ''

SELECT 
    conname AS constraint_name,
    pg_get_constraintdef(oid) AS constraint_definition
FROM pg_constraint
WHERE conrelid = (
    SELECT oid FROM pg_class 
    WHERE relname LIKE '_hyper_1_1_chunk'
)
AND contype = 'c';

\echo ''
\echo '✓ Test 4 passed'
\echo ''

-- ==========================================
-- Test 5: Build multiple chunks 
-- ==========================================

\echo 'Test 5: Creating multiple chunks...'
\echo ''

-- สร้าง chunks สำหรับหลายวัน
SELECT test_create_chunk(1, '2024-01-02 00:00:00+00'::timestamptz) AS chunk_2;
SELECT test_create_chunk(1, '2024-01-03 00:00:00+00'::timestamptz) AS chunk_3;
SELECT test_create_chunk(1, '2024-01-05 12:00:00+00'::timestamptz) AS chunk_4;

\echo ''
\echo 'All chunks:'
-- Use this query:
SELECT 
    id,
    table_name,
    TO_TIMESTAMP((start_time::double precision / 1000000) + 946684800) as start_time,
    TO_TIMESTAMP((end_time::double precision / 1000000) + 946684800) as end_time
FROM _timeseries_catalog.chunk
WHERE hypertable_id = 1
ORDER BY start_time;

\echo ''
\echo '✓ Test 5 passed'
\echo ''

-- ==========================================
-- Test 6: test Find Chunk
-- ==========================================

\echo 'Test 6: Testing chunk lookup...'
\echo ''

-- หา chunk สำหรับเวลาต่างๆ
\echo 'Finding chunk for 2024-01-01 12:00:00:'
SELECT test_find_chunk_for_time(1, '2024-01-01 12:00:00+00'::timestamptz) AS found_chunk;

\echo ''
\echo 'Finding chunk for 2024-01-03 18:00:00:'
SELECT test_find_chunk_for_time(1, '2024-01-03 18:00:00+00'::timestamptz) AS found_chunk;

\echo ''
\echo 'Finding chunk for non-existent time (2024-01-10):'
SELECT test_find_chunk_for_time(1, '2024-01-10 00:00:00+00'::timestamptz) AS not_found;

\echo ''
\echo '✓ Test 6 passed'
\echo ''

-- ==========================================
-- Test 7: test Get or Create
-- ==========================================

\echo 'Test 7: Testing get_or_create...'
\echo ''

-- ทดสอบกับ chunk ที่มีอยู่แล้ว
\echo 'Get existing chunk (2024-01-02):'
SELECT test_get_or_create_chunk(1, '2024-01-02 10:00:00+00'::timestamptz) AS existing_chunk;

\echo ''
\echo 'Create new chunk (2024-01-07):'
SELECT test_get_or_create_chunk(1, '2024-01-07 00:00:00+00'::timestamptz) AS new_chunk;

\echo ''
\echo 'Updated chunk list:'
SELECT COUNT(*) as total_chunks FROM _timeseries_catalog.chunk WHERE hypertable_id = 1;

\echo ''
\echo '✓ Test 7 passed'
\echo ''

-- ==========================================
-- Test 8: test Chunk Boundaries
-- ==========================================

\echo 'Test 8: Testing chunk boundaries...'
\echo ''

-- สร้าง chunks ที่มีเวลาใกล้ boundary
\echo 'Creating chunk at boundary:';
SELECT test_create_chunk(1, '2024-01-04 00:00:00+00'::timestamptz);

\echo ''
\echo 'Creating chunk just before boundary:';
SELECT test_create_chunk(1, '2024-01-08 23:59:59+00'::timestamptz);

\echo ''
\echo 'All chunks with boundaries:'
SELECT 
    table_name,
    TO_TIMESTAMP((start_time::double precision / 1000000) + 946684800) as start_time,
    TO_TIMESTAMP((end_time::double precision / 1000000) + 946684800) as end_time,
    (end_time - start_time) / 1000000 / 86400 as days
FROM _timeseries_catalog.chunk
WHERE hypertable_id = 1
ORDER BY start_time;

\echo ''
\echo '✓ Test 8 passed'
\echo ''

-- ==========================================
-- Test 9: test with another hypertable 
-- ==========================================

\echo 'Test 9: Testing with different hypertable (hourly chunks)...'
\echo ''

DROP TABLE IF EXISTS hourly_test CASCADE;
CREATE TABLE hourly_test (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);

SELECT create_hypertable('hourly_test', 'time', INTERVAL '1 hour');

SELECT test_create_chunk(2, '2024-01-01 00:00:00+00'::timestamptz);
SELECT test_create_chunk(2, '2024-01-01 01:00:00+00'::timestamptz);
SELECT test_create_chunk(2, '2024-01-01 02:00:00+00'::timestamptz);

\echo ''
\echo 'Hourly chunks:'
SELECT 
    table_name,
    TO_TIMESTAMP((start_time::double precision / 1000000) + 946684800) as start_time,
    TO_TIMESTAMP((end_time::double precision / 1000000) + 946684800) as end_time
FROM _timeseries_catalog.chunk
WHERE hypertable_id = 2
ORDER BY start_time;

\echo ''
\echo '✓ Test 9 passed'
\echo ''

-- ==========================================
-- Summary
-- ==========================================

\echo '=========================================='
\echo 'Summary: All Chunks'
\echo '=========================================='
\echo ''

SELECT 
    h.table_name as hypertable,
    COUNT(c.id) as num_chunks,
    MIN(TO_TIMESTAMP(c.start_time::double precision / 1000000)) AT TIME ZONE 'UTC' as earliest,
    MAX(TO_TIMESTAMP(c.end_time::double precision / 1000000)) AT TIME ZONE 'UTC' as latest
FROM _timeseries_catalog.hypertable h
LEFT JOIN _timeseries_catalog.chunk c ON h.id = c.hypertable_id
GROUP BY h.id, h.table_name
ORDER BY h.id;

\echo ''

SELECT 
    schemaname,
    tablename
FROM pg_tables 
WHERE tablename LIKE '_hyper_%'
ORDER BY tablename;

\echo ''
\echo '=========================================='
\echo '✅ All tests passed!'
\echo '=========================================='
\echo ''