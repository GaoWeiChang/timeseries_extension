-- every 1 minutes
DROP TABLE IF EXISTS regular_ts CASCADE;

CREATE TABLE regular_ts (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);

INSERT INTO regular_ts
SELECT
    '2024-01-01 00:00:00'::timestamptz + (i || ' minutes')::interval,
    random() * 100
FROM generate_series(0, 19) i;

SELECT test_compress_dod('regular_ts', 'time');


-- irregular pattern
DROP TABLE IF EXISTS irregular_ts CASCADE;

CREATE TABLE irregular_ts (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);

INSERT INTO irregular_ts (time, value) VALUES
    ('2024-01-01 00:00:00+00', 1),   -- base
    ('2024-01-01 00:01:00+00', 2),   -- +60s
    ('2024-01-01 00:02:00+00', 3),   -- +60s  → dod = 0
    ('2024-01-01 00:02:30+00', 4),   -- +30s  → dod = -30
    ('2024-01-01 00:03:30+00', 5),   -- +60s  → dod = +30
    ('2024-01-01 00:05:30+00', 6),   -- +120s → dod = +60
    ('2024-01-01 00:06:30+00', 7),   -- +60s  → dod = -60
    ('2024-01-01 00:06:45+00', 8),   -- +15s  → dod = -45
    ('2024-01-01 00:07:45+00', 9),   -- +60s  → dod = +45
    ('2024-01-01 00:08:45+00', 10);  -- +60s  → dod = 0

SELECT test_compress_dod('irregular_ts', 'time');

-- every 1 hour
DROP TABLE IF EXISTS hourly_ts CASCADE;

CREATE TABLE hourly_ts (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);

INSERT INTO hourly_ts
SELECT
    '2024-01-01 00:00:00'::timestamptz + (i || ' hours')::interval,
    random() * 100
FROM generate_series(0, 23) i;

SELECT test_compress_dod('hourly_ts', 'time');


-- single row
DROP TABLE IF EXISTS single_ts CASCADE;
CREATE TABLE single_ts (time TIMESTAMPTZ NOT NULL);
INSERT INTO single_ts VALUES ('2024-01-01 00:00:00+00');

SELECT test_compress_dod('single_ts', 'time');

-- two rows
DROP TABLE IF EXISTS two_ts CASCADE;
CREATE TABLE two_ts (time TIMESTAMPTZ NOT NULL);
INSERT INTO two_ts VALUES
    ('2024-01-01 00:00:00+00'),
    ('2024-01-01 01:00:00+00');

SELECT test_compress_dod('two_ts', 'time');

-- burst pattern
DROP TABLE IF EXISTS burst_ts CASCADE;
CREATE TABLE burst_ts (time TIMESTAMPTZ NOT NULL);

INSERT INTO burst_ts VALUES
    ('2024-01-01 00:00:00+00'),    -- regular
    ('2024-01-01 00:01:00+00'),    -- +60s
    ('2024-01-01 00:02:00+00'),    -- +60s
    ('2024-01-01 00:03:00+00'),    -- +60s
    ('2024-01-01 00:04:00+00'),    -- +60s
    ('2024-01-01 00:05:00+00'),    -- +60s  ← burst starts
    ('2024-01-01 00:05:01+00'),    -- +1s   ← dod = -59
    ('2024-01-01 00:05:02+00'),    -- +1s   ← dod = 0
    ('2024-01-01 00:05:03+00'),    -- +1s   ← dod = 0
    ('2024-01-01 00:05:04+00'),    -- +1s   ← dod = 0
    ('2024-01-01 00:06:04+00'),    -- +60s  ← dod = +59 (back to regular)
    ('2024-01-01 00:07:04+00'),    -- +60s  ← dod = 0
    ('2024-01-01 00:08:04+00');    -- +60s  ← dod = 0

SELECT test_compress_dod('burst_ts', 'time');