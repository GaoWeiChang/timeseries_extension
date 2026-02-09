-- test_compress_chunk_integrated.sql
-- Phase 8.4: ทดสอบ compress_chunk() ที่ integrate 3 algorithms

\echo '=========================================='
\echo 'Phase 8.4: Integrated compress_chunk()'
\echo '=========================================='
\echo ''

-- ==========================================
-- Setup: สร้าง hypertable + insert data
-- ==========================================

\echo 'Setup: create hypertable + data…'

DROP TABLE IF EXISTS metrics CASCADE;

CREATE TABLE metrics (
    time        TIMESTAMPTZ,        -- เวลาที่เก็บข้อมูล
    device_name TEXT,               -- ชื่ออุปกรณ์
    location    TEXT,               -- สถานที่ติดตั้ง
    sensor_id   INTEGER,            -- ID ของ sensor
    value       DOUBLE PRECISION    -- ค่าที่วัดได้
);

-- สร้าง hypertable ผ่าน extension
SELECT create_hypertable('metrics', 'time', INTERVAL '1 day');

-- Insert 3 วัน (= 3 chunks)
INSERT INTO metrics
SELECT
    '2024-01-01'::timestamptz + (i || ' minutes')::interval,   -- time        → DoD
    'device_' || ((i % 5) + 1),                                  -- device_name → Dictionary
    'location_' || ((i % 3) + 1),                                -- location    → Dictionary
    (i % 100) + 1,                                               -- sensor_id   → Delta
    random() * 100                                               -- value       → None (float8)
FROM generate_series(0, 4319) i;   -- 4320 rows = 3 days × 1440 min/day

\echo ''
\echo '✓ Inserted 4320 rows across 3 chunks'
\echo ''

-- ==========================================
-- Test 1: Show chunk info before compress
-- ==========================================

\echo '=========================================='
\echo 'Test 1: Before compression'
\echo '=========================================='
\echo ''

SELECT show_compression_info('metrics');

\echo ''
SELECT show_chunk_compression_stats('metrics');

\echo ''

-- ==========================================
-- Test 2: Compress chunk 1
-- ==========================================

\echo '=========================================='
\echo 'Test 2: compress_chunk(1)'
\echo '=========================================='
\echo ''

SELECT compress_chunk(1);

\echo ''

-- ==========================================
-- Test 3: Verify compressed_columns metadata
-- ==========================================

\echo '=========================================='
\echo 'Test 3: compressed_columns metadata'
\echo '=========================================='
\echo ''

SELECT chunk_id, column_name, algorithm, pg_size_pretty(octet_length(compressed_data)) as payload_size
FROM _timeseries_catalog.compressed_columns
WHERE chunk_id = 1
ORDER BY column_name;

\echo ''

-- ==========================================
-- Test 4: Decompress chunk 1 → verify data
-- ==========================================

\echo '=========================================='
\echo 'Test 4: decompress_chunk(1) + verify'
\echo '=========================================='
\echo ''

SELECT decompress_chunk(1);

\echo ''
\echo 'Row count after decompress:'
SELECT COUNT(*) FROM metrics;

\echo ''
\echo 'Sample rows from chunk 1:'
SELECT time, device_name, location, sensor_id
FROM metrics
WHERE time >= '2024-01-01' AND time < '2024-01-02'
ORDER BY time
LIMIT 5;

\echo ''

-- ==========================================
-- Test 5: Compress all chunks
-- ==========================================

\echo '=========================================='
\echo 'Test 5: compress_chunks_older_than'
\echo '=========================================='
\echo ''

SELECT compress_chunks_older_than('metrics', INTERVAL '0 days');

\echo ''
SELECT show_chunk_compression_stats('metrics');

\echo ''

-- ==========================================
-- Test 6: Decompress all + final verify
-- ==========================================

\echo '=========================================='
\echo 'Test 6: Decompress all + verify'
\echo '=========================================='
\echo ''

-- decompress each chunk
SELECT decompress_chunk(1);
SELECT decompress_chunk(2);
SELECT decompress_chunk(3);

\echo ''
\echo 'Final row count (should be 4320):'
SELECT COUNT(*) FROM metrics;

\echo ''
\echo 'Final stats:'
SELECT show_chunk_compression_stats('metrics');

\echo ''
\echo '=========================================='
\echo '✅ Phase 8.4 Complete!'
\echo '=========================================='
\echo ''
\echo 'Algorithm mapping:'
\echo '  time        (timestamptz) → delta_of_delta'
\echo '  device_name (text)        → dictionary'
\echo '  location    (text)        → dictionary'
\echo '  sensor_id   (integer)     → delta'
\echo '  value       (float8)      → none (skipped)'