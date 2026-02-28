DROP TABLE IF EXISTS sensor_data CASCADE;

CREATE TABLE sensor_data (
    time      TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    value     DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');

INSERT INTO sensor_data VALUES
    ('2024-01-01 7:00', 1, 10.0),
    ('2024-01-02 4:00', 2, 20.0),
    ('2024-01-03 13:00', 1, 15.0),
    ('2024-01-04 2:00', 3, 30.0),
    ('2024-01-05 21:00', 2, 25.0),
    ('2024-01-06 23:00', 1, 12.0),
    ('2024-01-07 11:00', 3, 35.0),
    ('2024-01-08 9:00', 2, 22.0),
    ('2024-01-09 2:00', 1, 18.0),
    ('2024-01-10 5:00', 3, 40.0);

-- =============================================
-- Manual Retention
-- =============================================

-- remove chunk lower than 365 days(1 year)
SELECT drop_chunks('sensor_data', INTERVAL '365 days');

-- all 
SELECT count(*) AS remaining_chunks FROM _timeseries_catalog.chunk;


-- manual apply retention
INSERT INTO sensor_data VALUES
    ('2024-01-01 12:00', 1, 10.0),
    ('2024-01-02 12:00', 2, 20.0),
    ('2024-01-03 12:00', 1, 15.0),
    ('2024-01-04 12:00', 3, 30.0),
    ('2024-01-05 12:00', 2, 25.0),
    ('2024-01-06 12:00', 1, 12.0),
    ('2024-01-07 12:00', 3, 35.0),
    ('2024-01-08 12:00', 2, 22.0),
    ('2024-01-09 12:00', 1, 18.0),
    ('2024-01-10 12:00', 3, 40.0);

-- set policy (drop partial chunks)
SELECT set_retention_policy('sensor_data', INTERVAL '365 days'); 

-- check policy
SELECT * FROM _timeseries_catalog.retention_policies;

-- apply policy
SELECT apply_retention_policies();

-- remove policy
SELECT remove_retention_policy('sensor_data');


-- =============================================
-- Auto Retention
-- =============================================

-- insert data
INSERT INTO sensor_data VALUES
    ('2025-02-01 13:00', 1, 15.0),
    ('2025-02-08 2:00', 3, 30.0),
    ('2025-02-09 21:00', 2, 25.0),
    ('2025-02-10 23:00', 1, 12.0),
    ('2025-02-11 11:00', 3, 35.0),
    ('2025-02-12 9:00', 2, 22.0),
    ('2025-02-14 2:00', 1, 18.0),
    ('2025-02-15 5:00', 3, 40.0);

-- output 10 rows
select * from sensor_data;

-- set policy (remove chunk older than 365 days)
SELECT set_retention_policy('sensor_data', INTERVAL '365 days'); 

-- output 6 rows, since some of chunks get remove due to policy
select * from sensor_data;

-- check background worker in database (must have 1 retention bgw in that table))
SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'test_db';