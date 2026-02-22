DROP TABLE IF EXISTS sensor_data CASCADE;

CREATE TABLE sensor_data (
    time        TIMESTAMPTZ NOT NULL,
    sensor_id   INTEGER,
    temperature DOUBLE PRECISION,
    humidity    DOUBLE PRECISION,
    location    TEXT
);

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');

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

-- comrpess
SELECT compress_chunk('_hyper_1_1_chunk');
