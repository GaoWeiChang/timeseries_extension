CREATE TABLE sensor_logs (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INT,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    status TEXT,                    
    message TEXT           
);

SELECT create_hypertable('sensor_logs', 'time', INTERVAL '1 day');


INSERT INTO sensor_logs (time, sensor_id, temperature, humidity, pressure, status, message)
SELECT 
    timestamp '2024-01-01 00:00:00',
    (random() * 100)::int + 1,
    (random() * 20 + 15)::numeric(5,2),
    (random() * 40 + 40)::numeric(5,2),
    (random() * 100 + 980)::numeric(7,2),
    CASE 
        WHEN random() < 0.7 THEN 'normal'
        WHEN random() < 0.85 THEN 'warning'
        WHEN random() < 0.95 THEN 'error'
        ELSE 'critical'
    END,
    CASE 
        WHEN random() < 0.6 THEN 'All systems operational'
        WHEN random() < 0.75 THEN 'Temperature slightly elevated'
        WHEN random() < 0.85 THEN 'Humidity above threshold'
        WHEN random() < 0.92 THEN 'Pressure anomaly detected'
        WHEN random() < 0.97 THEN 'Sensor calibration required'
        ELSE 'Connection unstable'
    END
FROM generate_series(1, 500000) AS i;

-- ====================== or =========================
INSERT INTO sensor_logs (time, sensor_id, temperature, humidity, pressure, status, message)
SELECT 
    timestamp '2024-01-01 00:00:00' + (i || ' seconds')::interval,
    (i % 100) + 1,  -- ✅ sensor_id 1-100 กระจายสม่ำเสมอ
    (random() * 20 + 15)::numeric(5,2),
    (random() * 40 + 40)::numeric(5,2),
    (random() * 100 + 980)::numeric(7,2),
    CASE (i % 100)  -- ✅ ใช้ modulo แทน random
        WHEN 0 THEN 'critical'   -- 1%
        WHEN 1 THEN 'error'       -- 1%
        WHEN 2 THEN 'error'       -- 1%
        WHEN 3 THEN 'error'       -- 1%
        WHEN 4 THEN 'error'       -- 1%
        WHEN 5 THEN 'warning'     -- 1%
        WHEN 6 THEN 'warning'     -- 1%
        WHEN 7 THEN 'warning'     -- 1%
        WHEN 8 THEN 'warning'     -- 1%
        WHEN 9 THEN 'warning'     -- 1%
        WHEN 10 THEN 'warning'    -- 1%
        WHEN 11 THEN 'warning'    -- 1%
        WHEN 12 THEN 'warning'    -- 1%
        WHEN 13 THEN 'warning'    -- 1%
        WHEN 14 THEN 'warning'    -- 1%
        ELSE 'normal'             -- 85%
    END,
    CASE (i % 20)  -- ✅ 20 แบบกระจายสม่ำเสมอ
        WHEN 0 THEN 'All systems operational'
        WHEN 1 THEN 'Temperature slightly elevated'
        WHEN 2 THEN 'Humidity above threshold'
        WHEN 3 THEN 'Pressure anomaly detected'
        WHEN 4 THEN 'Sensor calibration required'
        WHEN 5 THEN 'Connection unstable'
        WHEN 6 THEN 'Battery low'
        WHEN 7 THEN 'Signal strength weak'
        WHEN 8 THEN 'Data sync pending'
        WHEN 9 THEN 'Firmware update available'
        WHEN 10 THEN 'Maintenance scheduled'
        WHEN 11 THEN 'Network latency high'
        WHEN 12 THEN 'Storage capacity warning'
        WHEN 13 THEN 'Power supply fluctuation'
        WHEN 14 THEN 'Temperature sensor drift'
        WHEN 15 THEN 'Humidity sensor recalibration needed'
        WHEN 16 THEN 'Pressure reading unstable'
        WHEN 17 THEN 'Communication timeout'
        WHEN 18 THEN 'Configuration mismatch'
        ELSE 'System check in progress'
    END
FROM generate_series(1, 50000) AS i;


-- size
SELECT 
    'Total Size' as description,
    pg_size_pretty(SUM(pg_total_relation_size('public.' || c.table_name))) as size
FROM _timeseries_catalog.chunk c
WHERE c.hypertable_id = 1;


-- compress
SELECT compress_chunk(1);