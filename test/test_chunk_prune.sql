DROP TABLE IF EXISTS sensor_data CASCADE;
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');

INSERT INTO sensor_data VALUES ('2024-01-01 00:00:00+00'::timestamptz, 1, 25.5, 60.0);
INSERT INTO sensor_data VALUES 
    ('2024-01-01 06:00:00+00', 1, 26.0, 61.0),
    ('2024-01-01 12:00:00+00', 2, 27.5, 62.0),
    ('2024-01-01 18:00:00+00', 1, 25.0, 59.0);

INSERT INTO sensor_data VALUES ('2024-01-02 10:00:00+00', 3, 28.0, 63.0);
INSERT INTO sensor_data VALUES ('2024-01-03 08:00:00+00'::timestamptz, 1, 24.0, 58.0);
INSERT INTO sensor_data VALUES ('2024-01-03 16:00:00+00'::timestamptz, 2, 26.5, 60.5);
INSERT INTO sensor_data VALUES ('2024-01-04 09:00:00+00'::timestamptz, 3, 27.0, 61.5);
INSERT INTO sensor_data VALUES ('2024-01-04 15:00:00+00'::timestamptz, 1, 25.5, 59.5);
INSERT INTO sensor_data VALUES ('2024-01-05 11:00:00+00'::timestamptz, 2, 28.5, 63.5);

-- ==========================================
-- Test Pruning
-- ==========================================
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM sensor_data
WHERE time >= '2024-01-01' AND time < '2024-01-03';
-- output must scan only _hyper_1_1_chunk and _hyper_1_2_chunk

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM sensor_data
WHERE time >= '2024-01-01';
-- output must scan all chunks