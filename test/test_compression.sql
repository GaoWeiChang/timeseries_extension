CREATE TABLE sensor_logs (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INT,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    status TEXT,                    
    message TEXT,                   
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